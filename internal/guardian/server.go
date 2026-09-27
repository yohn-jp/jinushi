package guardian

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
)

func serveFromArgs(args []string, factory BackendFactory) error {
	if len(args) != 0 {
		return errors.New("guardian: hidden helper accepts no arguments")
	}
	if factory == nil {
		return errors.New("guardian: backend factory is required")
	}
	var config launchConfig
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, maxConfigBytes))
	if err := decoder.Decode(&config); err != nil {
		return errors.New("guardian: invalid transient launch config")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("guardian: helper input must contain one config")
	}
	if config.Version != ProtocolVersion || !runIDPattern.MatchString(config.RunID) || config.Dir == "" || config.Token == "" {
		return errors.New("guardian: incomplete launch config")
	}
	if err := validateLaunchConfig(config); err != nil {
		return err
	}
	return serveConfig(config, factory)
}

func validateLaunchConfig(config launchConfig) error {
	d, err := readDescriptor(config.Dir, config.RunID)
	if err != nil || !equalToken(d.Token, config.Token) {
		return errors.New("guardian: launch config does not match private descriptor")
	}
	if config.MaxOutputBytes < 0 || config.MaxOutputBytes > 1<<40 {
		return errors.New("guardian: invalid output retention limit")
	}
	return nil
}

func serveConfig(config launchConfig, factory BackendFactory) error {
	spool, err := openSpool(config.Dir, config.MaxOutputBytes)
	if err != nil {
		return err
	}
	defer spool.close()
	state := &runState{
		descriptor: config,
		spool:      spool,
		terminal:   make(chan struct{}),
	}
	state.snapshot = Snapshot{Version: ProtocolVersion, RunID: config.RunID, State: model.Starting, Resources: unavailableResources()}
	if err := state.persist(); err != nil {
		return err
	}
	listener, err := ipc.Listen(config.Dir)
	if err != nil {
		state.markStartupFailure("guardian-control-endpoint-failed")
		return err
	}
	defer listener.Close()
	go state.serve(listener)

	selected := factory()
	if selected == nil {
		state.markStartupFailure("backend-factory-unavailable")
		return nil
	}
	stdout, stderr := spool.writer("stdout"), spool.writer("stderr")
	if config.Spec.Interactive {
		stdout, stderr = spool.writer("pty"), spool.writer("pty")
	}
	process, err := selected.Start(config.Spec, stdout, stderr)
	if err != nil {
		var uncertain *backend.UncertainError
		if errors.As(err, &uncertain) {
			state.markUncertainWithOwnership(uncertain.Ownership, "backend-start-cleanup-unproven")
			return nil
		}
		state.markStartupFailure("backend-start-failed-clean")
		return nil
	}
	state.mu.Lock()
	state.process = process
	owner := process.Ownership()
	startedAt := time.Now().UTC()
	state.snapshot.State = model.Running
	state.snapshot.Ownership = &owner
	state.snapshot.StartedAt = &startedAt
	state.snapshot.Output = spool.output()
	state.snapshot.Resources = unavailableResources()
	err = state.persistLocked()
	state.mu.Unlock()
	if err != nil {
		// Ownership exists in memory and remains controlled by this helper.
		// The persisted `starting` state cannot be upgraded safely, so report
		// uncertainty over the authenticated endpoint and keep owning the Run.
		state.setMemoryUncertain(owner, "running-ownership-persist-failed")
		return state.waitForTerminal()
	}
	go state.monitor()
	return state.waitForTerminal()
}

type runState struct {
	mu         sync.Mutex
	controlMu  sync.Mutex
	descriptor launchConfig
	spool      *spool
	process    backend.Process
	snapshot   Snapshot
	terminal   chan struct{}
	termOnce   sync.Once
}

const limitTerminationGrace = 5 * time.Second

func (s *runState) serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *runState) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	decoder := json.NewDecoder(io.LimitReader(conn, maxRPCBytes))
	var request rpcRequest
	if err := decoder.Decode(&request); err != nil {
		writeResponse(conn, rpcResponse{Version: ProtocolVersion, Error: "invalid request"})
		return
	}
	if request.Version != ProtocolVersion || !constantEqual(request.Token, s.descriptor.Token) {
		writeResponse(conn, rpcResponse{Version: ProtocolVersion, Error: "authentication failed"})
		return
	}
	response := s.dispatch(request)
	response.Version = ProtocolVersion
	writeResponse(conn, response)
}

func (s *runState) dispatch(request rpcRequest) rpcResponse {
	switch request.Op {
	case "observe":
		snapshot, err := s.observe()
		if err != nil {
			return rpcResponse{Error: "observation unavailable"}
		}
		return rpcResponse{Snapshot: &snapshot}
	case "signal":
		if err := s.signal(request.Signal); err != nil {
			return rpcResponse{Error: "signal-failed"}
		}
		return rpcResponse{}
	case "terminate":
		result, err := s.terminate(time.Duration(request.GraceMS)*time.Millisecond, request.Reason)
		if err != nil {
			return rpcResponse{Error: "termination-failed"}
		}
		return rpcResponse{Termination: &result}
	case "input":
		if err := s.writeInput(request.Data); err != nil {
			return rpcResponse{Error: "input-write-failed"}
		}
		return rpcResponse{}
	case "resize":
		if err := s.resize(request.Rows, request.Cols); err != nil {
			return rpcResponse{Error: "resize-failed"}
		}
		return rpcResponse{}
	case "close-input":
		if err := s.closeInput(); err != nil {
			return rpcResponse{Error: "input-close-failed"}
		}
		return rpcResponse{}
	case "output":
		chunk, err := s.spool.read(request.Stream, request.Offset, request.Limit)
		if err != nil {
			return rpcResponse{Error: "output-read-failed"}
		}
		return rpcResponse{Chunk: &chunk}
	default:
		return rpcResponse{Error: "unsupported operation"}
	}
}

func (s *runState) observe() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.State != model.Terminal && s.process != nil {
		if resources, err := s.process.Observe(); err == nil {
			s.snapshot.Resources = mergeResources(s.snapshot.Resources, resources)
		} else {
			s.snapshot.Resources = unavailableCurrent(s.snapshot.Resources)
		}
		s.snapshot.Output = s.spool.output()
		if err := s.persistLocked(); err != nil {
			return Snapshot{}, err
		}
	}
	return s.snapshot, nil
}

func (s *runState) signal(name string) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	process := s.process
	state := s.snapshot.State
	s.mu.Unlock()
	if process == nil || state == model.Terminal {
		return errors.New("guardian: Run is not live")
	}
	return process.Signal(name)
}

func (s *runState) terminate(grace time.Duration, reason string) (backend.TerminationResult, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	if s.snapshot.State == model.Terminal {
		result := s.snapshot.Termination
		if !result.TreeEmpty {
			return backend.TerminationResult{}, errors.New("guardian: terminal receipt lacks tree-empty proof")
		}
		s.mu.Unlock()
		return result, nil
	}
	process := s.process
	if process == nil {
		s.mu.Unlock()
		return backend.TerminationResult{}, ErrUncertain
	}
	s.snapshot.State = model.Terminating
	s.snapshot.Termination.Requested = true
	s.snapshot.TerminationReason = boundedReason(reason)
	if err := s.persistLocked(); err != nil {
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = "termination-request-persist-failed"
		s.mu.Unlock()
		return backend.TerminationResult{}, ErrUncertain
	}
	s.mu.Unlock()
	if grace < 0 {
		grace = 0
	}
	result, err := process.Terminate(grace)
	if err != nil {
		return result, err
	}
	s.mu.Lock()
	s.snapshot.Termination.Requested = result.Requested || s.snapshot.Termination.Requested
	s.snapshot.Termination.Forced = result.Forced
	s.snapshot.Termination.TreeEmpty = result.TreeEmpty
	s.snapshot.Termination.Outcome = result.Outcome
	_ = s.persistLocked()
	s.mu.Unlock()
	return result, nil
}

func (s *runState) writeInput(data []byte) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if len(data) > maxRPCBytes/2 {
		return errors.New("guardian: input write exceeds bounded frame")
	}
	s.mu.Lock()
	process, state := s.process, s.snapshot.State
	s.mu.Unlock()
	if process == nil || state == model.Terminal {
		return errors.New("guardian: Run is not live")
	}
	return process.WriteInput(data)
}

func (s *runState) resize(rows, cols uint16) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	process, state := s.process, s.snapshot.State
	s.mu.Unlock()
	if process == nil || state == model.Terminal {
		return errors.New("guardian: Run is not live")
	}
	return process.Resize(rows, cols)
}

func (s *runState) closeInput() error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	process, state := s.process, s.snapshot.State
	s.mu.Unlock()
	if process == nil || state == model.Terminal {
		return errors.New("guardian: Run is not live")
	}
	return process.CloseInput()
}

func (s *runState) monitor() {
	sample := time.NewTicker(time.Second)
	defer sample.Stop()
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		for {
			exit, err := s.process.Wait()
			if err == nil {
				s.finish(exit)
				return
			}
			s.markUncertain("owned-tree-empty-unproven")
			time.Sleep(time.Second)
		}
	}()
	for {
		select {
		case <-wait:
			return
		case <-s.terminal:
			return
		case <-sample.C:
			s.sample()
		}
	}
}

func (s *runState) sample() {
	s.mu.Lock()
	if s.process == nil || s.snapshot.State == model.Terminal {
		s.mu.Unlock()
		return
	}
	process := s.process
	if resources, err := process.Observe(); err == nil {
		s.snapshot.Resources = mergeResources(s.snapshot.Resources, resources)
	} else {
		s.snapshot.Resources = unavailableCurrent(s.snapshot.Resources)
	}
	if limits, ok := process.(backend.LimitEvidence); ok {
		if outcome := normalizeLimitOutcome(limits.LimitOutcome()); outcome != "" && s.snapshot.LimitOutcome == "" {
			s.snapshot.LimitOutcome = outcome
		}
	}
	s.snapshot.Output = s.spool.output()
	err := s.persistLocked()
	triggerLimit := err == nil && s.snapshot.LimitOutcome != "" && s.snapshot.State != model.Terminal && !s.snapshot.Termination.TreeEmpty
	outcome := s.snapshot.LimitOutcome
	s.mu.Unlock()
	if triggerLimit {
		_, _ = s.terminate(limitTerminationGrace, outcome)
	}
}

func (s *runState) finish(exit backend.Exit) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if err := s.spool.sync(); err != nil {
		s.markUncertain("output-spool-sync-failed")
		return
	}
	s.mu.Lock()
	resources := s.snapshot.Resources
	limitOutcome := s.snapshot.LimitOutcome
	s.mu.Unlock()
	if observed, err := s.process.Observe(); err == nil {
		s.mu.Lock()
		resources = mergeResources(s.snapshot.Resources, observed)
		s.mu.Unlock()
	} else {
		resources = unavailableCurrent(resources)
	}
	output := s.spool.output()
	finished := exit.FinishedAt
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	started := exit.StartedAt
	if started.IsZero() {
		s.mu.Lock()
		if s.snapshot.StartedAt != nil {
			started = *s.snapshot.StartedAt
		}
		s.mu.Unlock()
	}
	outcome := exit.Outcome
	if typedOutcome := normalizeLimitOutcome(outcome); typedOutcome != "" {
		limitOutcome = typedOutcome
		outcome = "resource-limit"
	}
	if limitOutcome != "" {
		outcome = "resource-limit"
	}
	if outcome == "" {
		if exit.Signal != "" {
			outcome = "signaled"
		} else {
			outcome = "exited"
		}
	}
	s.mu.Lock()
	if s.snapshot.LimitOutcome != "" {
		limitOutcome = s.snapshot.LimitOutcome
		outcome = "resource-limit"
	}
	s.snapshot.State = model.Terminal
	s.snapshot.LimitOutcome = limitOutcome
	s.snapshot.Resources = resources
	s.snapshot.Output = output
	s.snapshot.FinishedAt = &finished
	s.snapshot.Termination.TreeEmpty = true
	receipt := model.Receipt{
		Version: 1, RunID: s.snapshot.RunID, Outcome: outcome, ExitCode: exit.ExitCode,
		Signal: exit.Signal, StartedAt: timePtrOrNil(started), FinishedAt: finished,
		Resources: resources, Output: output,
		TerminationRequested: s.snapshot.Termination.Requested,
		Forced:               s.snapshot.Termination.Forced,
		Cleanup:              "complete",
	}
	s.snapshot.Receipt = &receipt
	if err := s.persistLocked(); err != nil {
		s.snapshot.State = model.Uncertain
		s.snapshot.Receipt = nil
		s.snapshot.Reason = "terminal-receipt-persist-failed"
		_ = s.persistLocked()
		s.mu.Unlock()
		return
	}
	s.termOnce.Do(func() { close(s.terminal) })
	s.mu.Unlock()
}

func (s *runState) markStartupFailure(reason string) {
	_ = s.spool.sync()
	finished := time.Now().UTC()
	s.mu.Lock()
	s.snapshot.State = model.Terminal
	s.snapshot.Reason = boundedReason(reason)
	s.snapshot.FinishedAt = &finished
	s.snapshot.Output = s.spool.output()
	receipt := model.Receipt{Version: 1, RunID: s.snapshot.RunID, Outcome: "startup-failed", FinishedAt: finished, Resources: unavailableResources(), Output: s.snapshot.Output, Cleanup: "complete"}
	s.snapshot.Receipt = &receipt
	_ = s.persistLocked()
	s.termOnce.Do(func() { close(s.terminal) })
	s.mu.Unlock()
}

func (s *runState) markUncertain(reason string) {
	s.mu.Lock()
	if s.snapshot.State != model.Terminal {
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = boundedReason(reason)
		s.snapshot.Output = s.spool.output()
		_ = s.persistLocked()
	}
	s.mu.Unlock()
}

func (s *runState) markUncertainWithOwnership(owner model.Ownership, reason string) {
	s.mu.Lock()
	s.snapshot.State = model.Uncertain
	s.snapshot.Ownership = &owner
	s.snapshot.Reason = boundedReason(reason)
	s.snapshot.Output = s.spool.output()
	_ = s.persistLocked()
	s.mu.Unlock()
}

func (s *runState) setMemoryUncertain(owner model.Ownership, reason string) {
	s.mu.Lock()
	s.snapshot.State = model.Uncertain
	s.snapshot.Ownership = &owner
	s.snapshot.Reason = boundedReason(reason)
	s.mu.Unlock()
}

func (s *runState) persist() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistLocked()
}

func (s *runState) persistLocked() error { return writeSnapshot(s.descriptor.Dir, s.snapshot) }

func (s *runState) waitForTerminal() error {
	<-s.terminal
	return nil
}

func writeResponse(conn net.Conn, response rpcResponse) {
	data, err := json.Marshal(response)
	if err != nil || len(data) > maxRPCBytes {
		data = []byte(`{"version":1,"error":"response unavailable"}`)
	}
	_, _ = conn.Write(append(data, '\n'))
}

func constantEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func boundedReason(reason string) string {
	if len(reason) == 0 {
		return ""
	}
	if len(reason) > 64 {
		return "requested"
	}
	for _, char := range reason {
		if !(char >= 'a' && char <= 'z') && !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') && char != '_' && char != '-' && char != '.' && char != ':' {
			return "requested"
		}
	}
	return reason
}

func normalizeLimitOutcome(outcome string) string {
	if outcome == "" || len(outcome) > 96 {
		return ""
	}
	for _, char := range outcome {
		if !(char >= 'a' && char <= 'z') && !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') && char != '_' && char != '-' && char != '.' && char != ':' {
			return ""
		}
	}
	if outcome == "resource-limit" || len(outcome) > len("resource-limit:") && outcome[:len("resource-limit:")] == "resource-limit:" {
		return outcome
	}
	return ""
}

func timePtrOrNil(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func mergeResources(previous, current model.Resources) model.Resources {
	out := current
	out.PeakMemoryBytes = maxMetric(previous.PeakMemoryBytes, current.PeakMemoryBytes, current.MemoryBytes)
	out.PeakProcessCount = maxMetric(previous.PeakProcessCount, current.PeakProcessCount, current.ProcessCount)
	if current.CPUTimeNs.Status == "measured" || current.CPUTimeNs.Status == "unsupported" {
		out.CPUTimeNs = current.CPUTimeNs
	} else if previous.CPUTimeNs.Status == "measured" {
		out.CPUTimeNs = previous.CPUTimeNs
	}
	out.SampleIntervalMs = 1000
	return out
}

func maxMetric(previous, reported, current model.Metric) model.Metric {
	best := previous
	for _, candidate := range []model.Metric{reported, current} {
		if candidate.Status == "unsupported" && best.Status != "measured" {
			best = candidate
		}
		if candidate.Status == "measured" && (best.Status != "measured" || candidate.Value > best.Value) {
			best = candidate
		}
	}
	if best.Status == "" {
		best.Status = "unavailable"
	}
	return best
}

func unavailableCurrent(previous model.Resources) model.Resources {
	out := previous
	out.MemoryBytes = model.Metric{Status: "unavailable"}
	out.ProcessCount = model.Metric{Status: "unavailable"}
	return out
}
