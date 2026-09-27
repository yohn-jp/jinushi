package guardian

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
)

const (
	defaultMaxOutputBytes = int64(8 << 20)
	startupWait           = 15 * time.Second
	outputPollInterval    = 100 * time.Millisecond
)

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type rpcRequest struct {
	Version int    `json:"version"`
	Token   string `json:"token"`
	Op      string `json:"op"`
	Signal  string `json:"signal,omitempty"`
	Data    []byte `json:"data,omitempty"`
	Rows    uint16 `json:"rows,omitempty"`
	Cols    uint16 `json:"cols,omitempty"`
	GraceMS int64  `json:"graceMs,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Stream  string `json:"stream,omitempty"`
	Offset  int64  `json:"offset,omitempty"`
	Limit   int64  `json:"limit,omitempty"`
}

type rpcResponse struct {
	Version     int                        `json:"version"`
	Snapshot    *Snapshot                  `json:"snapshot,omitempty"`
	Termination *backend.TerminationResult `json:"termination,omitempty"`
	Chunk       *Chunk                     `json:"chunk,omitempty"`
	Error       string                     `json:"error,omitempty"`
}

func startHelper(ctx context.Context, executable string, config Config) (*Handle, error) {
	if !runIDPattern.MatchString(config.RunID) {
		return nil, ErrInvalidIdentity
	}
	if strings.TrimSpace(executable) == "" {
		return nil, errors.New("guardian: executable path is required")
	}
	dir, err := filepath.Abs(config.Dir)
	if err != nil {
		return nil, fmt.Errorf("guardian: resolve state dir: %w", err)
	}
	if config.Dir == "" {
		return nil, errors.New("guardian: state dir is required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("guardian: create run directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("guardian: run directory is not a private directory")
	}
	if err := secureDir(dir); err != nil {
		return nil, fmt.Errorf("guardian: secure run directory: %w", err)
	}
	if config.MaxOutputBytes < 0 {
		return nil, errors.New("guardian: output limit cannot be negative")
	}
	if config.MaxOutputBytes == 0 {
		config.MaxOutputBytes = defaultMaxOutputBytes
	}
	if config.Spec.Limits.OutputBytes > 0 && config.Spec.Limits.OutputBytes < config.MaxOutputBytes {
		config.MaxOutputBytes = config.Spec.Limits.OutputBytes
	}
	if _, err := os.Stat(filepath.Join(dir, descriptorName)); err == nil {
		return nil, ErrAlreadyStarted
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("guardian: inspect run descriptor: %w", err)
	}
	token, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("guardian: create control credential: %w", err)
	}
	descriptor := privateDescriptor{
		Version: ProtocolVersion,
		RunID:   config.RunID,
		Dir:     dir,
		Token:   token,
	}
	if err := writeDescriptor(descriptor); err != nil {
		return nil, err
	}
	initial := Snapshot{Version: ProtocolVersion, RunID: config.RunID, State: model.Starting, Resources: unavailableResources()}
	if err := writeSnapshot(dir, initial); err != nil {
		_ = os.Remove(filepath.Join(dir, descriptorName))
		return nil, err
	}
	lcfg := launchConfig{
		Version: ProtocolVersion, RunID: config.RunID, Dir: dir, Spec: config.Spec,
		MaxOutputBytes: config.MaxOutputBytes, Token: token,
	}
	configBytes, err := json.Marshal(lcfg)
	if err != nil {
		_ = os.Remove(filepath.Join(dir, descriptorName))
		_ = os.Remove(filepath.Join(dir, statusName))
		return nil, fmt.Errorf("guardian: encode transient launch config: %w", err)
	}
	if len(configBytes) > maxConfigBytes {
		_ = os.Remove(filepath.Join(dir, descriptorName))
		_ = os.Remove(filepath.Join(dir, statusName))
		return nil, errors.New("guardian: launch config exceeds bounded helper input")
	}
	cmd := exec.Command(executable, "__guardian")
	configureHelperCommand(cmd)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = os.Remove(filepath.Join(dir, descriptorName))
		_ = os.Remove(filepath.Join(dir, statusName))
		return nil, fmt.Errorf("guardian: create helper input pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = os.Remove(filepath.Join(dir, descriptorName))
		_ = os.Remove(filepath.Join(dir, statusName))
		return nil, fmt.Errorf("guardian: start helper: %w", err)
	}
	h := &Handle{descriptor: descriptor, started: true}
	done := make(chan error, 1)
	h.helperDone = done
	go func() { done <- cmd.Wait() }()
	data := append(configBytes, '\n')
	n, writeErr := stdin.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	closeErr := stdin.Close()
	if writeErr != nil || closeErr != nil {
		// The helper may already have consumed a complete config and launched
		// the backend. Keep the descriptor and require reconciliation.
		return h, fmt.Errorf("%w: transient helper setup did not complete", ErrUncertain)
	}
	startupCtx, cancel := context.WithTimeout(ctx, startupWait)
	defer cancel()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-startupCtx.Done():
			return h, fmt.Errorf("%w: helper did not establish ownership before deadline", ErrUncertain)
		default:
		}
		if snapshot, err := h.probe(startupCtx); err == nil {
			if snapshot.State == model.Running {
				return h, nil
			}
			if snapshot.State == model.Terminal {
				return h, nil
			}
			if snapshot.State == model.Uncertain {
				return h, fmt.Errorf("%w: %s", ErrUncertain, safeReason(snapshot.Reason))
			}
		}
		if snapshot, err := readSnapshot(dir); err == nil {
			if snapshot.State == model.Terminal {
				return h, nil
			}
			if snapshot.State == model.Uncertain {
				return h, fmt.Errorf("%w: %s", ErrUncertain, safeReason(snapshot.Reason))
			}
		}
		select {
		case helperErr := <-done:
			if snapshot, err := readSnapshot(dir); err == nil && snapshot.State == model.Terminal {
				return h, nil
			}
			if helperErr != nil {
				return h, fmt.Errorf("%w: helper exited before ownership was reported", ErrUncertain)
			}
			return h, fmt.Errorf("%w: helper exited before ownership was reported", ErrUncertain)
		case <-tick.C:
		case <-startupCtx.Done():
			return h, fmt.Errorf("%w: helper did not establish ownership before deadline", ErrUncertain)
		}
	}
}

func reattach(ctx context.Context, dir, runID string) (*Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !runIDPattern.MatchString(runID) {
		return nil, ErrInvalidIdentity
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(absDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("guardian: run directory is not a private directory")
	}
	if err := secureDir(absDir); err != nil {
		return nil, fmt.Errorf("guardian: secure run directory: %w", err)
	}
	d, err := readDescriptor(absDir, runID)
	if err != nil {
		return nil, err
	}
	return &Handle{descriptor: d}, nil
}

func randomToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

func unavailableResources() model.Resources {
	metric := model.Metric{Status: "unavailable"}
	return model.Resources{MemoryBytes: metric, PeakMemoryBytes: metric, CPUTimeNs: metric, ProcessCount: metric, PeakProcessCount: metric}
}

func safeReason(reason string) string {
	if reason == "" {
		return "ownership was not proven"
	}
	if len(reason) > 256 {
		return reason[:256]
	}
	return reason
}

func (h *Handle) probe(ctx context.Context) (Snapshot, error) {
	response, err := h.call(ctx, rpcRequest{Op: "observe"})
	if err != nil {
		return Snapshot{}, err
	}
	if response.Snapshot == nil {
		return Snapshot{}, errors.New("guardian: helper returned no observation")
	}
	snapshot := *response.Snapshot
	snapshot.Live = true
	return snapshot, nil
}

func (h *Handle) observe(ctx context.Context) (Snapshot, error) {
	if snapshot, err := h.probe(ctx); err == nil {
		return snapshot, nil
	}
	snapshot, err := readSnapshot(h.descriptor.Dir)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: no durable helper evidence is available", ErrUncertain)
	}
	if snapshot.State == model.Running || snapshot.State == model.Starting || snapshot.State == model.Terminating {
		return snapshot, ErrUncertain
	}
	return snapshot, nil
}

func (h *Handle) wait(ctx context.Context) (Evidence, error) {
	ticker := time.NewTicker(outputPollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := h.observe(ctx)
		if err == nil && snapshot.State == model.Terminal && snapshot.Receipt != nil {
			exit := backend.Exit{ExitCode: snapshot.Receipt.ExitCode, Signal: snapshot.Receipt.Signal, Outcome: snapshot.Receipt.Outcome}
			if snapshot.Receipt.StartedAt != nil {
				exit.StartedAt = *snapshot.Receipt.StartedAt
			}
			exit.FinishedAt = snapshot.Receipt.FinishedAt
			return Evidence{Snapshot: snapshot, Exit: exit, Receipt: *snapshot.Receipt}, nil
		}
		if err != nil || snapshot.State == model.Uncertain {
			return Evidence{}, ErrUncertain
		}
		select {
		case <-ctx.Done():
			return Evidence{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (h *Handle) signal(ctx context.Context, name string) error {
	_, err := h.call(ctx, rpcRequest{Op: "signal", Signal: name})
	return err
}

func (h *Handle) terminate(ctx context.Context, grace time.Duration, reason string) (backend.TerminationResult, error) {
	snapshot, err := readSnapshot(h.descriptor.Dir)
	if err == nil && snapshot.State == model.Terminal {
		return snapshot.Termination, nil
	}
	response, err := h.call(ctx, rpcRequest{Op: "terminate", GraceMS: grace.Milliseconds(), Reason: reason})
	if err != nil {
		if snapshot, readErr := readSnapshot(h.descriptor.Dir); readErr == nil && snapshot.State == model.Terminal {
			return snapshot.Termination, nil
		}
		return backend.TerminationResult{}, err
	}
	if response.Termination == nil {
		return backend.TerminationResult{}, errors.New("guardian: helper returned no termination result")
	}
	return *response.Termination, nil
}

func (h *Handle) writeInput(ctx context.Context, data []byte) error {
	if len(data) > maxRPCBytes/2 {
		return errors.New("guardian: input write exceeds bounded frame")
	}
	_, err := h.call(ctx, rpcRequest{Op: "input", Data: data})
	return err
}

func (h *Handle) closeInput(ctx context.Context) error {
	_, err := h.call(ctx, rpcRequest{Op: "close-input"})
	return err
}

func (h *Handle) resize(ctx context.Context, rows, cols uint16) error {
	_, err := h.call(ctx, rpcRequest{Op: "resize", Rows: rows, Cols: cols})
	return err
}

func (h *Handle) readOutput(stream string, offset, limit int64) (Chunk, error) {
	if offset < 0 || limit <= 0 || limit > maxSpoolRead {
		return Chunk{}, errors.New("guardian: invalid output range")
	}
	if stream != "stdout" && stream != "stderr" && stream != "pty" {
		return Chunk{}, fmt.Errorf("guardian: unknown output stream %q", stream)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	snapshot, _ := h.observe(ctx)
	stat := outputStream(snapshot.Output, stream)
	path := filepath.Join(h.descriptor.Dir, stream+".out")
	response, callErr := h.call(ctx, rpcRequest{Op: "output", Stream: stream, Offset: offset, Limit: limit})
	if callErr == nil && response.Chunk != nil {
		return *response.Chunk, nil
	}
	if callErr != nil {
		if snapshot.State != model.Terminal && snapshot.State != model.Uncertain {
			return Chunk{}, ErrUncertain
		}
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Chunk{Stream: stream, Offset: offset, ObservedBytes: stat.ObservedBytes, RetainedBytes: stat.RetainedBytes, RetainedFrom: stat.RetainedFrom, Truncated: stat.Truncated, Gap: offset < stat.ObservedBytes}, nil
		}
		return Chunk{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Chunk{}, err
	}
	if !info.Mode().IsRegular() {
		return Chunk{}, errors.New("guardian: output spool is not a regular file")
	}
	chunk := Chunk{Stream: stream, Offset: offset, RetainedFrom: stat.RetainedFrom, RetainedBytes: stat.RetainedBytes, ObservedBytes: stat.ObservedBytes, Truncated: stat.Truncated}
	retainedEnd := stat.RetainedFrom + stat.RetainedBytes
	if offset < stat.RetainedFrom {
		chunk.Gap = true
		return chunk, nil
	}
	if offset >= retainedEnd {
		chunk.Gap = offset < stat.ObservedBytes
		return chunk, nil
	}
	if end := retainedEnd; offset+limit > end {
		limit = end - offset
	}
	chunk.Data = make([]byte, int(limit))
	capacity := stat.RetainedBytes
	if capacity == 0 {
		chunk.Data = nil
		chunk.Gap = offset < stat.ObservedBytes
		return chunk, nil
	}
	position := offset
	if stat.RetainedFrom > 0 {
		position %= capacity
	}
	firstCount := int64(len(chunk.Data))
	if remain := capacity - position; firstCount > remain {
		firstCount = remain
	}
	n, err := f.ReadAt(chunk.Data[:int(firstCount)], position)
	if err != nil && err != io.EOF {
		return Chunk{}, err
	}
	if int64(n) != firstCount {
		chunk.Data = chunk.Data[:n]
		chunk.Gap = true
		return chunk, nil
	}
	if firstCount < int64(len(chunk.Data)) {
		secondCount := int64(len(chunk.Data)) - firstCount
		n, err = f.ReadAt(chunk.Data[int(firstCount):], 0)
		if err != nil && err != io.EOF {
			return Chunk{}, err
		}
		if int64(n) != secondCount {
			chunk.Data = chunk.Data[:int(firstCount)+n]
			chunk.Gap = true
			return chunk, nil
		}
	}
	return chunk, nil
}

func outputStream(output model.Output, name string) model.OutputStream {
	switch name {
	case "stdout":
		return output.Stdout
	case "stderr":
		return output.Stderr
	default:
		return output.PTY
	}
}

func (h *Handle) call(ctx context.Context, request rpcRequest) (rpcResponse, error) {
	request.Version = ProtocolVersion
	request.Token = h.descriptor.Token
	data, err := json.Marshal(request)
	if err != nil {
		return rpcResponse{}, err
	}
	if len(data) > maxRPCBytes {
		return rpcResponse{}, errors.New("guardian: request exceeds bounded frame")
	}
	conn, err := ipc.Dial(ctx, h.descriptor.Dir)
	if err != nil {
		return rpcResponse{}, ErrUnavailable
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := conn.Write(append(data, '\n')); err != nil {
		return rpcResponse{}, ErrUnavailable
	}
	decoder := json.NewDecoder(io.LimitReader(conn, maxRPCBytes))
	var response rpcResponse
	if err := decoder.Decode(&response); err != nil {
		return rpcResponse{}, ErrUnavailable
	}
	if response.Version != ProtocolVersion {
		return rpcResponse{}, errors.New("guardian: unsupported response version")
	}
	if response.Error != "" {
		return rpcResponse{}, errors.New("guardian: " + response.Error)
	}
	return response, nil
}

func equalToken(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
