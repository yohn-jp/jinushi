//go:build linux

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

var (
	jinushiBinary    string
	helperBinary     string
	buildRoot        string
	submissionSerial atomic.Uint64
)

func TestMain(m *testing.M) {
	var err error
	buildRoot, err = moduleRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "locate Jinushi module root:", err)
		os.Exit(1)
	}
	buildDir, err := os.MkdirTemp("", "jinushi-blackbox-build-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create blackbox build directory:", err)
		os.Exit(1)
	}
	jinushiBinary = filepath.Join(buildDir, "jinushi")
	helperBinary = filepath.Join(buildDir, "process-helper")
	if err := buildBinary(buildRoot, jinushiBinary, "./cmd/jinushi"); err != nil {
		fmt.Fprintln(os.Stderr, "build production Jinushi CLI:", err)
		_ = os.RemoveAll(buildDir)
		os.Exit(1)
	}
	if err := buildBinary(buildRoot, helperBinary, "./integration/testdata/helper"); err != nil {
		fmt.Fprintln(os.Stderr, "build process helper:", err)
		_ = os.RemoveAll(buildDir)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(buildDir)
	os.Exit(code)
}

func moduleRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for directory := current; ; directory = filepath.Dir(directory) {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
	}
	return "", fmt.Errorf("no go.mod found above %s", current)
}

func buildBinary(dir, output, packagePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", output, packagePath)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go build %s: %w: %s", packagePath, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

type metric struct {
	Status string `json:"status"`
	Value  int64  `json:"value"`
}

type outputStream struct {
	ObservedBytes int64 `json:"observedBytes"`
	RetainedBytes int64 `json:"retainedBytes"`
	RetainedFrom  int64 `json:"retainedFrom"`
	Truncated     bool  `json:"truncated"`
}

type runOutput struct {
	Stdout          outputStream `json:"stdout"`
	Stderr          outputStream `json:"stderr"`
	PTY             outputStream `json:"pty"`
	HistoryComplete bool         `json:"historyComplete"`
}

type receipt struct {
	Outcome               string        `json:"outcome"`
	ExitCode              *int          `json:"exitCode"`
	Cleanup               string        `json:"cleanup"`
	TerminationRequested  bool          `json:"terminationRequested"`
	AcceptedArgvSHA256    string        `json:"acceptedArgvSha256"`
	Resources             resources     `json:"resources"`
	Capabilities          capabilities  `json:"capabilities"`
	EffectiveCapabilities *capabilities `json:"effectiveCapabilities"`
	Output                runOutput     `json:"output"`
	EventFirstSeq         uint64        `json:"eventFirstSeq"`
	EventLastSeq          uint64        `json:"eventLastSeq"`
	EventHistoryComplete  bool          `json:"eventHistoryComplete"`
	EvidenceIncomplete    bool          `json:"evidenceIncomplete"`
}

type resources struct {
	MemoryBytes      metric `json:"memoryBytes"`
	PeakMemoryBytes  metric `json:"peakMemoryBytes"`
	CPUTimeNs        metric `json:"cpuTimeNs"`
	ProcessCount     metric `json:"processCount"`
	PeakProcessCount metric `json:"peakProcessCount"`
	TaskCount        metric `json:"taskCount"`
	PeakTaskCount    metric `json:"peakTaskCount"`
	SampleIntervalMs int64  `json:"sampleIntervalMs"`
}

type run struct {
	ID                    string        `json:"runId"`
	State                 string        `json:"state"`
	Generation            uint64        `json:"generation"`
	Output                runOutput     `json:"output"`
	Resources             resources     `json:"resources"`
	EffectiveCapabilities *capabilities `json:"effectiveCapabilities"`
	LastOutputAt          *time.Time    `json:"lastOutputAt"`
	LastResourceSampleAt  *time.Time    `json:"lastResourceSampleAt"`
	ResourceGap           bool          `json:"resourceGap"`
	Receipt               *receipt      `json:"receipt"`
	LeaseGeneration       uint64        `json:"leaseGeneration"`
	LeaseExpiry           *time.Time    `json:"leaseExpiry"`
}

type capabilities struct {
	Backend                 string   `json:"backend"`
	PTY                     bool     `json:"pty"`
	MemoryEnforcement       bool     `json:"memoryEnforcement"`
	CPUQuotaEnforcement     bool     `json:"cpuQuotaEnforcement"`
	ProcessCountEnforcement bool     `json:"processCountEnforcement"`
	TaskCountEnforcement    bool     `json:"taskCountEnforcement"`
	RestartReconciliation   string   `json:"restartReconciliation"`
	Signals                 []string `json:"signals"`
}

type event struct {
	Seq        uint64                    `json:"seq"`
	Kind       string                    `json:"kind"`
	ObservedAt time.Time                 `json:"observedAt"`
	Payload    map[string]map[string]any `json:"payload"`
	Body       map[string]any            `json:"body"`
}

func (e event) data() map[string]any {
	if e.Body != nil {
		return e.Body
	}
	for _, key := range []string{"run", "control", "signal", "limit", "lease", "pty", "output", "resource", "resourceGap"} {
		if body := e.Payload[key]; body != nil {
			return body
		}
	}
	return nil
}

type followedEvent struct {
	RunID string `json:"runId"`
	Seq   uint64 `json:"seq"`
	Kind  string `json:"kind"`
}

type followRecord struct {
	Type  string        `json:"type"`
	RunID string        `json:"runId"`
	State string        `json:"state"`
	Event followedEvent `json:"event"`
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type response struct {
	Run          *run          `json:"run"`
	Runs         []run         `json:"runs"`
	Events       []event       `json:"events"`
	Capabilities *capabilities `json:"capabilities"`
	Error        *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Data         string `json:"data"`
	Gap          bool   `json:"gap"`
	RetainedFrom uint64 `json:"retainedFrom"`
}

type supervisorProcess struct {
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
}

type harness struct {
	t        *testing.T
	stateDir string
	sup      *supervisorProcess
	runs     []string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	tempRoot, err := os.MkdirTemp("", "jinushi-")
	if err != nil {
		t.Fatalf("create short supervisor state root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempRoot) })
	h := &harness{t: t, stateDir: filepath.Join(tempRoot, "state")}
	t.Cleanup(h.cleanup)
	h.startSupervisor()
	return h
}

func (h *harness) startSupervisor() {
	h.t.Helper()
	if h.sup != nil {
		h.t.Fatal("supervisor already running")
	}
	cmd := exec.Command(jinushiBinary, "supervisor", "--state-dir", h.stateDir)
	sup := &supervisorProcess{cmd: cmd}
	cmd.Stdout = &sup.stdout
	cmd.Stderr = &sup.stderr
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("start real Jinushi supervisor: %v", err)
	}
	h.sup = sup
	h.waitUntil("supervisor IPC ready", 10*time.Second, func() bool {
		_, _, result := h.invoke(2*time.Second, "status", "--state-dir", h.stateDir)
		return result.Error == nil
	})
}

func (h *harness) stopSupervisor(force bool) {
	h.t.Helper()
	if h.sup == nil {
		return
	}
	sup := h.sup
	h.sup = nil
	if force {
		_ = sup.cmd.Process.Kill()
	} else {
		_ = sup.cmd.Process.Signal(os.Interrupt)
	}
	done := make(chan error, 1)
	go func() { done <- sup.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		_ = sup.cmd.Process.Kill()
		<-done
		h.t.Errorf("supervisor did not stop after signal")
	}
}

func (h *harness) restartAfterCrash() {
	h.t.Helper()
	h.stopSupervisor(true)
	h.startSupervisor()
}

func (h *harness) cleanup() {
	if h.sup == nil {
		return
	}
	for i := len(h.runs) - 1; i >= 0; i-- {
		id := h.runs[i]
		_, _, current := h.invoke(3*time.Second, "inspect", "--state-dir", h.stateDir, id)
		if current.Run != nil && current.Run.State != "terminal" && current.Run.State != "uncertain" {
			_, _, _ = h.invoke(3*time.Second, "cancel", "--state-dir", h.stateDir, "--request-id", fmt.Sprintf("cleanup-%d", submissionSerial.Add(1)), "--expected-generation", strconv.FormatUint(current.Run.Generation, 10), id)
		}
		_, _, _ = h.invoke(8*time.Second, "await", "--state-dir", h.stateDir, id)
	}
	h.stopSupervisor(false)
}

func (h *harness) invoke(timeout time.Duration, args ...string) (int, []byte, response) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, jinushiBinary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else if ctx.Err() != nil {
			return -1, append(stdout.Bytes(), stderr.Bytes()...), response{Error: &struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}{Code: "client-timeout", Message: ctx.Err().Error()}}
		} else {
			code = -1
		}
	}
	var result response
	if len(bytes.TrimSpace(stdout.Bytes())) > 0 {
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			h.t.Fatalf("decode JSON response from `jinushi %s`: %v; stdout=%q stderr=%q", strings.Join(args, " "), err, stdout.String(), stderr.String())
		}
	}
	if code < 0 {
		h.t.Fatalf("run `jinushi %s`: %v; stderr=%q", strings.Join(args, " "), err, stderr.String())
	}
	return code, append([]byte(nil), stdout.Bytes()...), result
}

func (h *harness) run(args ...string) run {
	h.t.Helper()
	full := []string{"run", "--state-dir", h.stateDir, "--submission-id", fmt.Sprintf("integration-%d", submissionSerial.Add(1)), "--cwd", h.t.TempDir()}
	full = append(full, args...)
	code, _, result := h.invoke(20*time.Second, full...)
	if code != 0 || result.Error != nil || result.Run == nil || result.Run.ID == "" {
		h.t.Fatalf("accept Run: exit=%d response=%+v", code, result)
	}
	h.runs = append(h.runs, result.Run.ID)
	return *result.Run
}

func (h *harness) await(id string, timeout time.Duration) (int, run) {
	h.t.Helper()
	code, _, result := h.invoke(timeout, "await", "--state-dir", h.stateDir, id)
	if result.Error != nil || result.Run == nil || result.Run.Receipt == nil {
		h.t.Fatalf("await Run %s: exit=%d response=%+v", id, code, result)
	}
	return code, *result.Run
}

func (h *harness) inspect(id string) run {
	h.t.Helper()
	code, _, result := h.invoke(5*time.Second, "inspect", "--state-dir", h.stateDir, id)
	if code != 0 || result.Error != nil || result.Run == nil {
		h.t.Fatalf("inspect Run %s: exit=%d response=%+v", id, code, result)
	}
	return *result.Run
}

func (h *harness) getCapabilities() capabilities {
	h.t.Helper()
	code, _, result := h.invoke(5*time.Second, "capabilities", "--state-dir", h.stateDir)
	if code != 0 || result.Error != nil || result.Capabilities == nil {
		h.t.Fatalf("discover capabilities: exit=%d response=%+v", code, result)
	}
	return *result.Capabilities
}

func (h *harness) waitUntil(description string, timeout time.Duration, predicate func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", description)
}

func TestRunLifecycleStartupFailureAndExitReceipt(t *testing.T) {
	h := newHarness(t)

	normal := h.run("--", "/bin/sh", "-c", "printf 'normal-output'")
	code, completed := h.await(normal.ID, 15*time.Second)
	if code != 0 || completed.State != "terminal" || completed.Receipt.Outcome != "exited" || completed.Receipt.ExitCode == nil || *completed.Receipt.ExitCode != 0 {
		t.Fatalf("normal Run did not produce a clean physical receipt: exit=%d run=%+v", code, completed)
	}
	if completed.Receipt.EventFirstSeq != 1 || !completed.Receipt.EventHistoryComplete {
		t.Fatalf("short Run receipt did not preserve complete event history from sequence 1: %+v", completed.Receipt)
	}
	if completed.Receipt.AcceptedArgvSHA256 == "" || completed.Receipt.Capabilities.Backend == "" || len(completed.Receipt.Capabilities.Signals) == 0 {
		t.Fatalf("terminal receipt omitted accepted argv identity or backend capability evidence: %+v", completed.Receipt)
	}
	if completed.EffectiveCapabilities == nil || completed.Receipt.EffectiveCapabilities == nil || !reflect.DeepEqual(*completed.EffectiveCapabilities, *completed.Receipt.EffectiveCapabilities) || !reflect.DeepEqual(*completed.EffectiveCapabilities, completed.Receipt.Capabilities) {
		t.Fatalf("terminal receipt did not preserve the Run's effective capabilities: run=%+v receipt=%+v", completed.EffectiveCapabilities, completed.Receipt)
	}
	_, _, eventResult := h.invoke(5*time.Second, "events", "--state-dir", h.stateDir, normal.ID)
	if eventResult.Error != nil {
		t.Fatalf("read normal Run event journal: %+v", eventResult)
	}
	var terminalSeq uint64
	for _, ev := range eventResult.Events {
		if ev.Kind == "run.terminal" {
			terminalSeq = ev.Seq
			break
		}
	}
	if terminalSeq == 0 || terminalSeq < completed.Receipt.EventFirstSeq || terminalSeq > completed.Receipt.EventLastSeq {
		t.Fatalf("receipt journal range [%d,%d] does not contain terminal event sequence %d: events=%+v", completed.Receipt.EventFirstSeq, completed.Receipt.EventLastSeq, terminalSeq, eventResult.Events)
	}
	unavailableTelemetry := false
	for _, sampled := range []metric{
		completed.Receipt.Resources.MemoryBytes,
		completed.Receipt.Resources.PeakMemoryBytes,
		completed.Receipt.Resources.CPUTimeNs,
		completed.Receipt.Resources.ProcessCount,
		completed.Receipt.Resources.PeakProcessCount,
	} {
		if sampled.Status == "unavailable" || sampled.Status == "" {
			unavailableTelemetry = true
		}
	}
	wantIncomplete := unavailableTelemetry || !completed.Receipt.Output.HistoryComplete || !completed.Receipt.EventHistoryComplete
	if completed.Receipt.EvidenceIncomplete != wantIncomplete {
		t.Fatalf("receipt evidenceIncomplete=%v; expected %v from telemetry/output/event evidence: resources=%+v output=%+v eventsComplete=%v", completed.Receipt.EvidenceIncomplete, wantIncomplete, completed.Receipt.Resources, completed.Receipt.Output, completed.Receipt.EventHistoryComplete)
	}
	_, _, result := h.invoke(5*time.Second, "output", "--state-dir", h.stateDir, "--stream", "stdout", "--limit", "128", "--json", normal.ID)
	output, err := base64.StdEncoding.DecodeString(result.Data)
	if result.Error != nil || err != nil || string(output) != "normal-output" {
		t.Fatalf("read normal stdout: output=%q decodeErr=%v response=%+v", output, err, result)
	}

	nonzero := h.run("--", "/bin/sh", "-c", "exit 23")
	code, completed = h.await(nonzero.ID, 15*time.Second)
	if code != 23 || completed.Receipt.Outcome != "exited" || completed.Receipt.ExitCode == nil || *completed.Receipt.ExitCode != 23 {
		t.Fatalf("nonzero Run receipt lost the process exit code: exit=%d run=%+v", code, completed)
	}

	missing := h.run("--", filepath.Join(t.TempDir(), "does-not-exist"))
	code, completed = h.await(missing.ID, 15*time.Second)
	if code == 0 || completed.State != "terminal" || completed.Receipt.Outcome != "startup-failed" || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("startup failure was not represented as a proven terminal receipt: exit=%d run=%+v", code, completed)
	}
}

func TestEventsFollowStreamsLiveRunThroughTerminalMarker(t *testing.T) {
	h := newHarness(t)
	releasePath := filepath.Join(t.TempDir(), "release-run")
	started := h.run("--", "/bin/sh", "-c", `printf 'before-follow'; while [ ! -e "$1" ]; do sleep 0.02; done; printf 'after-follow'`, "jinushi-follow", releasePath)
	h.waitUntil("held Run first output", 8*time.Second, func() bool {
		current := h.inspect(started.ID)
		return current.State == "running" && current.Output.Stdout.ObservedBytes >= int64(len("before-follow"))
	})

	_, _, prior := h.invoke(5*time.Second, "events", "--state-dir", h.stateDir, "--after", "0", started.ID)
	if prior.Error != nil || len(prior.Events) == 0 {
		t.Fatalf("read journal before starting follow: %+v", prior)
	}
	var after uint64
	var sawFirstOutput bool
	for _, item := range prior.Events {
		if item.Seq > after {
			after = item.Seq
		}
		if item.Kind == "output.chunk" {
			sawFirstOutput = true
		}
	}
	if !sawFirstOutput || after == 0 {
		t.Fatalf("Run journal did not contain the first output before follow: %+v", prior.Events)
	}

	command := exec.Command(jinushiBinary, "events", "--state-dir", h.stateDir, "--after", strconv.FormatUint(after, 10), "--follow", started.ID)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("open follow stdout: %v", err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start production events --follow CLI: %v", err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	type scannedLine struct {
		data []byte
		err  error
	}
	lines := make(chan scannedLine, 16)
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			lines <- scannedLine{data: append([]byte(nil), scanner.Bytes()...)}
		}
		if err := scanner.Err(); err != nil {
			lines <- scannedLine{err: err}
		}
		close(lines)
	}()
	readLine := func(timeout time.Duration) []byte {
		t.Helper()
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("event follower closed before terminal marker")
			}
			if line.err != nil {
				t.Fatalf("read event follower output: %v", line.err)
			}
			return line.data
		case <-time.After(timeout):
			t.Fatalf("timed out reading event follower output")
			return nil
		}
	}
	decodeRecord := func(line []byte) followRecord {
		t.Helper()
		var record followRecord
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode follow NDJSON record %q: %v", line, err)
		}
		return record
	}
	first := decodeRecord(readLine(8 * time.Second))
	if first.Type != "event" || first.Event.RunID != started.ID || first.Event.Seq <= after ||
		(first.Event.Kind != "resource.sample" && first.Event.Kind != "resource.unavailable") {
		t.Fatalf("follow did not stream a new live Run event after sequence %d: %+v", after, first)
	}
	if current := h.inspect(started.ID); current.State != "running" {
		t.Fatalf("follow did not attach while the Run was still active: state=%s", current.State)
	}
	if err := os.WriteFile(releasePath, []byte("release"), 0600); err != nil {
		t.Fatalf("release held Run: %v", err)
	}

	lastSeq := first.Event.Seq
	sawTerminalEvent := first.Event.Kind == "run.terminal"
	sawTerminalMarker := false
	for !sawTerminalMarker {
		record := decodeRecord(readLine(8 * time.Second))
		switch record.Type {
		case "event":
			if sawTerminalEvent || record.Event.RunID != started.ID || record.Event.Seq <= lastSeq {
				t.Fatalf("follow event stream was not strictly ordered for Run %s: last=%d record=%+v", started.ID, lastSeq, record)
			}
			lastSeq = record.Event.Seq
			if record.Event.Kind == "run.terminal" {
				sawTerminalEvent = true
			}
		case "terminal":
			if !sawTerminalEvent || record.RunID != started.ID || record.State != "terminal" {
				t.Fatalf("follow terminal marker did not follow run.terminal: %+v (saw event=%v)", record, sawTerminalEvent)
			}
			sawTerminalMarker = true
		default:
			t.Fatalf("unexpected event follow record %q: %+v", record.Type, record)
		}
	}
	select {
	case line, ok := <-lines:
		if ok {
			t.Fatalf("event follower wrote a record after its final terminal marker: %q", line.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("event follower did not close after its final terminal marker")
	}
	if err := command.Wait(); err != nil {
		waited = true
		t.Fatalf("production events --follow exited unsuccessfully: %v; stderr=%q", err, stderr.String())
	}
	waited = true
	code, completed := h.await(started.ID, 8*time.Second)
	if code != 0 || completed.Receipt.Outcome != "exited" || completed.Receipt.ExitCode == nil || *completed.Receipt.ExitCode != 0 {
		t.Fatalf("followed Run did not finish with a clean physical receipt: exit=%d receipt=%+v", code, completed.Receipt)
	}
}

func TestGrandchildCancellationProvesTreeGone(t *testing.T) {
	h := newHarness(t)
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")
	started := h.run("--", helperBinary, "spawn-grandchild", pidFile)
	pid := waitForPIDFile(t, pidFile, 10*time.Second)
	if !linuxProcessExecuting(pid) {
		t.Fatalf("grandchild PID %d was not executing before cancellation", pid)
	}
	code, _, canceled := h.invoke(5*time.Second, "cancel", "--state-dir", h.stateDir, "--request-id", "grandchild-cancel", "--expected-generation", strconv.FormatUint(h.inspect(started.ID).Generation, 10), started.ID)
	if code != 0 || canceled.Error != nil {
		t.Fatalf("cancel Run: exit=%d response=%+v", code, canceled)
	}
	_, finished := h.await(started.ID, 15*time.Second)
	if finished.Receipt.Outcome != "cancelled" || finished.Receipt.Cleanup != "complete" || !finished.Receipt.TerminationRequested {
		t.Fatalf("cancellation receipt did not prove tree cleanup: %+v", finished.Receipt)
	}
	waitProcessGone(t, pid, 8*time.Second)
}

func TestOutputFloodIsBoundedAndReportsOutputGap(t *testing.T) {
	h := newHarness(t)
	// Output on both streams exceeds the requested retention ceiling while the
	// real workload continues writing, exercising the production pipe drainers.
	const floodBytes = 80 << 20
	started := h.run("--output-bytes", "8192", "--", helperBinary, "flood", strconv.Itoa(floodBytes))
	code, finished := h.await(started.ID, 3*time.Minute)
	if code != 0 || finished.Receipt.Outcome != "exited" || finished.Receipt.ExitCode == nil || *finished.Receipt.ExitCode != 0 {
		t.Fatalf("output flood did not complete: exit=%d receipt=%+v", code, finished.Receipt)
	}
	if finished.Output.Stdout.ObservedBytes != floodBytes || finished.Output.Stderr.ObservedBytes != floodBytes {
		t.Fatalf("flood byte counters are wrong: stdout=%+v stderr=%+v", finished.Output.Stdout, finished.Output.Stderr)
	}
	if finished.Output.Stdout.RetainedBytes > 8192 || finished.Output.Stderr.RetainedBytes > 8192 || finished.Output.HistoryComplete {
		t.Fatalf("bounded spool metadata is inconsistent: %+v", finished.Output)
	}
	_, _, outputResult := h.invoke(5*time.Second, "output", "--state-dir", h.stateDir, "--stream", "stdout", "--offset", "0", "--limit", "128", "--json", started.ID)
	if outputResult.Error != nil || !outputResult.Gap || outputResult.RetainedFrom == 0 {
		t.Fatalf("compacted stdout did not expose a machine-readable gap: %+v", outputResult)
	}
}

func TestContinuousStdoutFloodDoesNotStarveStderrAndCancelsTree(t *testing.T) {
	h := newHarness(t)
	pidFile := filepath.Join(t.TempDir(), "stdout-flood.pid")
	const marker = "stderr-fairness-marker"
	const outputRetention = int64(12 << 10)
	const perStreamRetention = outputRetention / 3
	script := `dd if=/dev/zero bs=4096 2>/dev/null & flood_pid=$!; printf '%s\n' "$flood_pid" > "$1"; sleep 0.1; printf '%s' "$2" >&2; wait "$flood_pid"`
	started := h.run("--output-bytes", strconv.FormatInt(outputRetention, 10), "--", "/bin/sh", "-c", script, "jinushi-output-fairness", pidFile, marker)
	floodPID := waitForPIDFile(t, pidFile, 10*time.Second)
	h.waitUntil("stderr marker to be observed while stdout floods and Run stays live", 20*time.Second, func() bool {
		current := h.inspect(started.ID)
		return current.State == "running" && current.Output.Stdout.ObservedBytes > 2*perStreamRetention && current.Output.Stderr.ObservedBytes >= int64(len(marker))
	})
	whileFlooding := h.inspect(started.ID)
	if !linuxProcessExecuting(floodPID) {
		t.Fatalf("continuous stdout producer PID %d stopped before cancellation", floodPID)
	}
	if whileFlooding.State != "running" || whileFlooding.Output.Stdout.RetainedBytes > perStreamRetention || !whileFlooding.Output.Stdout.Truncated {
		t.Fatalf("stdout flood was not both live and bounded by its retention limit: state=%s output=%+v", whileFlooding.State, whileFlooding.Output.Stdout)
	}
	if whileFlooding.Output.Stderr.ObservedBytes != int64(len(marker)) || whileFlooding.Output.Stderr.RetainedBytes > perStreamRetention {
		t.Fatalf("stderr marker was not fully observed within the bounded stream: %+v", whileFlooding.Output.Stderr)
	}

	code, _, canceled := h.invoke(5*time.Second, "cancel", "--state-dir", h.stateDir, "--request-id", "stdout-flood-cancel", "--expected-generation", strconv.FormatUint(h.inspect(started.ID).Generation, 10), started.ID)
	if code != 0 || canceled.Error != nil {
		t.Fatalf("cancel output flood Run: exit=%d response=%+v", code, canceled)
	}
	_, completed := h.await(started.ID, 20*time.Second)
	if completed.Receipt.Outcome != "cancelled" || completed.Receipt.Cleanup != "complete" || !completed.Receipt.TerminationRequested {
		t.Fatalf("output flood cancellation did not prove physical tree cleanup: %+v", completed.Receipt)
	}
	waitProcessGone(t, floodPID, 8*time.Second)
}

func TestEventJournalReportsCompactionGap(t *testing.T) {
	h := newHarness(t)
	caps := h.getCapabilities()
	supported := false
	for _, name := range caps.Signals {
		if name == "SIGUSR1" {
			supported = true
		}
	}
	if !supported {
		t.Skipf("Linux backend does not advertise SIGUSR1 for this real event load; capabilities: %+v", caps)
	}
	started := h.run("--", helperBinary, "signal-sink")
	h.waitUntil("signal sink to enter running state and install its handler", 10*time.Second, func() bool {
		current := h.inspect(started.ID)
		return current.State == "running" && current.Output.Stdout.ObservedBytes > 0
	})

	// Signals are delivered by Run identity through production local IPC and
	// the OS backend. Each accepted delivery appends one non-critical event;
	// enough events force bounded journal compaction without injecting records
	// into the store or replacing the real process.
	const workers = 16
	const deliveries = 4600
	errorsFound := make(chan error, workers)
	var wg sync.WaitGroup
	var controlMu sync.Mutex
	generation := h.inspect(started.ID).Generation
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < deliveries/workers; i++ {
				controlMu.Lock()
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				result, err := ipc.Call(ctx, h.stateDir, protocol.Request{Version: 1, Op: "signal", RunID: started.ID, Signal: "SIGUSR1", RequestID: fmt.Sprintf("signal-%d", generation), ExpectedGeneration: generation})
				cancel()
				if err == nil && result.Error == nil && result.Run != nil {
					generation = result.Run.Generation
				}
				controlMu.Unlock()
				if err != nil {
					errorsFound <- err
					return
				}
				if result.Error != nil || result.Run == nil {
					errorsFound <- fmt.Errorf("signal response: %+v", result)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("deliver a real SIGUSR1 through local IPC: %v", err)
	}
	_, _, journal := h.invoke(10*time.Second, "events", "--state-dir", h.stateDir, "--after", "1", started.ID)
	if journal.Error != nil || !journal.Gap || len(journal.Events) == 0 {
		t.Fatalf("compacted event history did not expose its sequence gap: %+v", journal)
	}
}

func TestPTYAttachReconnectResizeAndInput(t *testing.T) {
	h := newHarness(t)
	caps := h.getCapabilities()
	if !caps.PTY {
		t.Skipf("Linux backend explicitly reports PTY unsupported; capability response: %+v", caps)
	}
	started := h.run("--interactive", "--", "/bin/sh", "-c", `printf READY; IFS= read -r line; stty size; printf 'GOT:%s\n' "$line"`)
	h.waitUntil("interactive Run to enter running state", 10*time.Second, func() bool { return h.inspect(started.ID).State == "running" })

	first := startAttach(t, h, started.ID)
	if err := waitEventCount(h, started.ID, "pty.attached", 1, 8*time.Second); err != nil {
		first.stop()
		t.Fatalf("first attachment was not accepted: %v; stdout=%q stderr=%q", err, first.stdout.String(), first.stderr.String())
	}
	if _, err := first.input.Write([]byte{0x1d}); err != nil {
		t.Fatalf("send Ctrl+] to detach: %v", err)
	}
	_ = first.input.Close()
	if err := first.wait(8 * time.Second); err != nil {
		t.Fatalf("first attach did not detach cleanly: %v stderr=%q", err, first.stderr.String())
	}
	if err := waitEventCount(h, started.ID, "pty.detached", 1, 8*time.Second); err != nil {
		t.Fatalf("first attachment did not record detach: %v; stdout=%q stderr=%q", err, first.stdout.String(), first.stderr.String())
	}
	if inspected := h.inspect(started.ID); inspected.State != "running" {
		t.Fatalf("PTY detach changed Run lifetime: state=%s", inspected.State)
	}

	code, _, resized := h.invoke(5*time.Second, "resize", "--state-dir", h.stateDir, "--rows", "40", "--cols", "100", "--request-id", "pty-resize", "--expected-generation", strconv.FormatUint(h.inspect(started.ID).Generation, 10), started.ID)
	if code != 0 || resized.Error != nil {
		t.Fatalf("resize PTY: exit=%d response=%+v", code, resized)
	}
	reconnected := startAttach(t, h, started.ID)
	if err := waitEventCount(h, started.ID, "pty.attached", 2, 8*time.Second); err != nil {
		reconnected.stop()
		t.Fatalf("reconnected attachment was not accepted: %v; stdout=%q stderr=%q", err, reconnected.stdout.String(), reconnected.stderr.String())
	}
	if _, err := reconnected.input.Write([]byte("hello from reconnected client\n")); err != nil {
		t.Fatalf("write PTY input: %v", err)
	}
	_ = reconnected.input.Close()
	code, finished := h.await(started.ID, 20*time.Second)
	if code != 0 || finished.Receipt.Outcome != "exited" || finished.Receipt.ExitCode == nil || *finished.Receipt.ExitCode != 0 {
		t.Fatalf("interactive Run failed: exit=%d receipt=%+v", code, finished.Receipt)
	}
	if err := reconnected.wait(8 * time.Second); err != nil {
		t.Fatalf("reconnected attach did not finish: %v stderr=%q", err, reconnected.stderr.String())
	}
	attached := reconnected.stdout.String()
	if !strings.Contains(attached, "READY") || !strings.Contains(attached, "40 100") || !strings.Contains(attached, "GOT:hello from reconnected client") {
		t.Fatalf("attach did not carry PTY output, resize, and input; output=%q", attached)
	}
}

type attachClient struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	stdout bytes.Buffer
	stderr bytes.Buffer
	waited bool
}

func startAttach(t *testing.T, h *harness, id string) *attachClient {
	t.Helper()
	cmd := exec.Command(jinushiBinary, "attach", "--state-dir", h.stateDir, id)
	client := &attachClient{cmd: cmd}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	client.input = stdin
	cmd.Stdout = &client.stdout
	cmd.Stderr = &client.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start real attach client: %v", err)
	}
	t.Cleanup(func() {
		client.stop()
	})
	return client
}

func (client *attachClient) stop() {
	_ = client.input.Close()
	if !client.waited && client.cmd.Process != nil {
		_ = client.cmd.Process.Kill()
		_ = client.cmd.Wait()
		client.waited = true
	}
}

func (client *attachClient) wait(timeout time.Duration) error {
	err := waitCommand(client.cmd, timeout)
	client.waited = true
	return err
}

func waitCommand(cmd *exec.Cmd, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
		return fmt.Errorf("command exceeded %s", timeout)
	}
}

func TestDetachedRunSurvivesClientDisconnect(t *testing.T) {
	h := newHarness(t)
	started := h.run("--", "/bin/sh", "-c", "sleep 1; printf detached-after-client-exit")
	if inspected := h.inspect(started.ID); inspected.State == "terminal" {
		t.Fatalf("short detached process unexpectedly finished before the run client exited: %+v", inspected.Receipt)
	}
	code, completed := h.await(started.ID, 15*time.Second)
	if code != 0 || completed.Receipt.Outcome != "exited" || completed.Output.Stdout.ObservedBytes != int64(len("detached-after-client-exit")) {
		t.Fatalf("detached Run did not outlive its requesting client: exit=%d receipt=%+v output=%+v", code, completed.Receipt, completed.Output)
	}
}

func TestCloseInputDeliversEOFToRealRun(t *testing.T) {
	h := newHarness(t)
	started := h.run("--", "/bin/sh", "-c", "cat; printf EOF")
	h.waitUntil("stdin-reading Run to enter running state", 10*time.Second, func() bool { return h.inspect(started.ID).State == "running" })

	code, _, input := h.invoke(5*time.Second, "input", "--state-dir", h.stateDir, "--request-id", "stdin-payload", "--expected-generation", strconv.FormatUint(h.inspect(started.ID).Generation, 10), started.ID, "payload-before-eof")
	if code != 0 || input.Error != nil {
		t.Fatalf("write stdin through production CLI: exit=%d response=%+v", code, input)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	writer, err := ipc.Call(ctx, h.stateDir, protocol.Request{Version: 1, Op: "writer-acquire", RunID: started.ID, AttachID: "stdin-close-owner"})
	if err != nil || writer.Error != nil || writer.WriterToken == "" {
		cancel()
		t.Fatalf("acquire stdin writer lease: response=%+v err=%v", writer, err)
	}
	closed, err := ipc.Call(ctx, h.stateDir, protocol.Request{Version: 1, Op: "close-input", RunID: started.ID, AttachID: "stdin-close-owner", WriterToken: writer.WriterToken, RequestID: "stdin-close", ExpectedGeneration: input.Run.Generation})
	_, _ = ipc.Call(ctx, h.stateDir, protocol.Request{Version: 1, Op: "writer-release", RunID: started.ID, AttachID: "stdin-close-owner", WriterToken: writer.WriterToken})
	cancel()
	if err != nil || closed.Error != nil {
		t.Fatalf("close stdin through production local protocol: response=%+v err=%v", closed, err)
	}

	code, completed := h.await(started.ID, 15*time.Second)
	if code != 0 || completed.Receipt.Outcome != "exited" || completed.Receipt.ExitCode == nil || *completed.Receipt.ExitCode != 0 {
		t.Fatalf("stdin EOF did not let the Run finish normally: exit=%d receipt=%+v", code, completed.Receipt)
	}
	_, _, output := h.invoke(5*time.Second, "output", "--state-dir", h.stateDir, "--stream", "stdout", "--limit", "128", "--json", started.ID)
	data, decodeErr := base64.StdEncoding.DecodeString(output.Data)
	if output.Error != nil || decodeErr != nil || string(data) != "payload-before-eofEOF" {
		t.Fatalf("Run did not preserve input bytes through EOF: output=%q decodeErr=%v response=%+v", data, decodeErr, output)
	}
}

func TestSupervisorCrashRestartReconcilesLiveRun(t *testing.T) {
	h := newHarness(t)
	started := h.run("--", helperBinary, "wait", "60000")
	h.waitUntil("owned Run to enter running state", 10*time.Second, func() bool { return h.inspect(started.ID).State == "running" })
	h.stopSupervisor(true)
	h.startSupervisor()
	reconciled := h.inspect(started.ID)
	if reconciled.State != "running" {
		t.Fatalf("restart failed to reconnect the still-owned execution: %+v", reconciled)
	}
	code, _, canceled := h.invoke(5*time.Second, "cancel", "--state-dir", h.stateDir, "--request-id", "reconciled-cancel", "--expected-generation", strconv.FormatUint(h.inspect(started.ID).Generation, 10), started.ID)
	if code != 0 || canceled.Error != nil {
		t.Fatalf("cancel reconciled Run: exit=%d response=%+v", code, canceled)
	}
	_, completed := h.await(started.ID, 15*time.Second)
	if completed.Receipt.Outcome != "cancelled" || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("reconciled Run did not produce a terminal cleanup receipt: %+v", completed.Receipt)
	}
}

func TestSupervisorRestartDoesNotDuplicateRunOutput(t *testing.T) {
	h := newHarness(t)
	gate := filepath.Join(t.TempDir(), "write-suffix")
	prefix := "output-before-supervisor-restart:"
	suffix := "output-after-supervisor-restart"
	script := `printf '%s' "$2"; while [ ! -e "$1" ]; do sleep 0.02; done; printf '%s' "$3"`
	started := h.run("--", "/bin/sh", "-c", script, "jinushi-output-cursor", gate, prefix, suffix)
	h.waitUntil("pre-restart prefix to enter the durable Run output", 10*time.Second, func() bool {
		current := h.inspect(started.ID)
		return current.State == "running" && current.Output.Stdout.ObservedBytes >= int64(len(prefix))
	})
	beforeRestart := h.inspect(started.ID)
	if beforeRestart.Output.Stdout.ObservedBytes != int64(len(prefix)) {
		t.Fatalf("held workload wrote beyond its pre-restart prefix: %+v", beforeRestart.Output.Stdout)
	}
	preRestartBytes, preRestartOutput := readRunStdout(t, h, started.ID, 1024)
	if preRestartOutput.Gap || !bytes.Equal(preRestartBytes, []byte(prefix)) {
		t.Fatalf("Store did not contain the exact prefix before Supervisor restart: data=%q metadata=%+v", preRestartBytes, preRestartOutput)
	}

	supervisorDown := false
	t.Cleanup(func() {
		if supervisorDown && h.sup == nil {
			h.startSupervisor()
			supervisorDown = false
		}
	})
	h.stopSupervisor(true)
	supervisorDown = true
	h.startSupervisor()
	supervisorDown = false

	afterRestart := h.inspect(started.ID)
	if afterRestart.State != "running" || afterRestart.Output.Stdout.ObservedBytes != int64(len(prefix)) {
		t.Fatalf("Supervisor reconciliation changed the persisted output cursor: state=%s output=%+v", afterRestart.State, afterRestart.Output.Stdout)
	}
	resumedBytes, resumedOutput := readRunStdout(t, h, started.ID, 1024)
	if resumedOutput.Gap || !bytes.Equal(resumedBytes, []byte(prefix)) {
		t.Fatalf("reconciled Store output duplicated or lost the pre-restart prefix: data=%q metadata=%+v", resumedBytes, resumedOutput)
	}
	if err := os.WriteFile(gate, []byte("continue"), 0600); err != nil {
		t.Fatalf("release post-restart output suffix: %v", err)
	}

	code, completed := h.await(started.ID, 15*time.Second)
	if code != 0 || completed.Receipt.Outcome != "exited" || completed.Receipt.ExitCode == nil || *completed.Receipt.ExitCode != 0 {
		t.Fatalf("output cursor Run did not exit cleanly: code=%d receipt=%+v", code, completed.Receipt)
	}
	expected := []byte(prefix + suffix)
	if completed.Output.Stdout.ObservedBytes != int64(len(expected)) || completed.Output.Stdout.Truncated {
		t.Fatalf("output byte accounting changed across restart: %+v, expected %d bytes", completed.Output.Stdout, len(expected))
	}
	finalBytes, finalOutput := readRunStdout(t, h, started.ID, 1024)
	if finalOutput.Gap || !bytes.Equal(finalBytes, expected) {
		t.Fatalf("final stdout differs from the exact prefix+suffix once: got=%q want=%q metadata=%+v", finalBytes, expected, finalOutput)
	}
}

func TestSupervisorRestartPreservesTruncatedOutputCursor(t *testing.T) {
	h := newHarness(t)
	gate := filepath.Join(t.TempDir(), "write-suffix")
	const retention = int64(64)
	const outputLimit = retention
	prefix := strings.Repeat("P", 160)
	suffix := strings.Repeat("S", 32)
	script := `printf '%s' "$2"; while [ ! -e "$1" ]; do sleep 0.02; done; printf '%s' "$3"`
	started := h.run("--output-bytes", strconv.FormatInt(outputLimit, 10), "--", "/bin/sh", "-c", script, "jinushi-truncated-output", gate, prefix, suffix)
	h.waitUntil("truncated pre-restart prefix to enter the durable Run output", 10*time.Second, func() bool {
		current := h.inspect(started.ID)
		return current.State == "running" && current.Output.Stdout.ObservedBytes >= int64(len(prefix))
	})
	beforeRestart := h.inspect(started.ID)
	if beforeRestart.Output.Stdout.ObservedBytes != int64(len(prefix)) || beforeRestart.Output.Stdout.RetainedBytes != retention || !beforeRestart.Output.Stdout.Truncated {
		t.Fatalf("pre-restart output did not reach its expected retention bound: %+v", beforeRestart.Output.Stdout)
	}
	preRestartBytes, preRestartOutput := readRunStdout(t, h, started.ID, 1024)
	if !preRestartOutput.Gap || preRestartOutput.RetainedFrom != uint64(len(prefix))-uint64(retention) || string(preRestartBytes) != strings.Repeat("P", int(retention)) {
		t.Fatalf("pre-restart truncation metadata/data were inconsistent: data=%q response=%+v", preRestartBytes, preRestartOutput)
	}

	supervisorDown := false
	t.Cleanup(func() {
		if supervisorDown && h.sup == nil {
			h.startSupervisor()
			supervisorDown = false
		}
	})
	h.stopSupervisor(true)
	supervisorDown = true
	h.startSupervisor()
	supervisorDown = false

	afterRestart := h.inspect(started.ID)
	if afterRestart.State != "running" || afterRestart.Output.Stdout.ObservedBytes != int64(len(prefix)) || afterRestart.Output.Stdout.RetainedBytes != retention || !afterRestart.Output.Stdout.Truncated {
		t.Fatalf("Supervisor reconciliation changed truncated output metadata: state=%s output=%+v", afterRestart.State, afterRestart.Output.Stdout)
	}
	resumedBytes, resumedOutput := readRunStdout(t, h, started.ID, 1024)
	if !resumedOutput.Gap || resumedOutput.RetainedFrom != preRestartOutput.RetainedFrom || !bytes.Equal(resumedBytes, preRestartBytes) {
		t.Fatalf("reconciled Store changed pre-restart truncated output: data=%q response=%+v", resumedBytes, resumedOutput)
	}
	if err := os.WriteFile(gate, []byte("continue"), 0600); err != nil {
		t.Fatalf("release post-restart truncated output suffix: %v", err)
	}

	code, completed := h.await(started.ID, 15*time.Second)
	if code != 0 || completed.Receipt.Outcome != "exited" || completed.Receipt.ExitCode == nil || *completed.Receipt.ExitCode != 0 {
		t.Fatalf("truncation output Run did not exit cleanly: code=%d receipt=%+v", code, completed.Receipt)
	}
	expected := []byte(prefix + suffix)
	if completed.Output.Stdout.ObservedBytes != int64(len(expected)) || completed.Output.Stdout.RetainedBytes != retention || !completed.Output.Stdout.Truncated || completed.Output.Stdout.RetainedFrom != int64(len(expected))-retention {
		t.Fatalf("final truncation metadata changed across restart: %+v, expected %d observed / %d retained", completed.Output.Stdout, len(expected), retention)
	}
	finalBytes, finalOutput := readRunStdout(t, h, started.ID, 1024)
	wantTail := expected[len(expected)-int(retention):]
	if !finalOutput.Gap || finalOutput.RetainedFrom != uint64(len(expected))-uint64(retention) || !bytes.Equal(finalBytes, wantTail) {
		t.Fatalf("final retained output is not the exact truncated tail: got=%q want=%q response=%+v", finalBytes, wantTail, finalOutput)
	}
}

func TestSupervisorRestartPreservesPhysicalTelemetryAndOutputTime(t *testing.T) {
	h := newHarness(t)
	writeGate := filepath.Join(t.TempDir(), "write-while-supervisor-down")
	writtenMarker := filepath.Join(t.TempDir(), "prefix-written")
	finishGate := filepath.Join(t.TempDir(), "finish-run")
	const prefix = "written-during-supervisor-outage"
	const suffix = "written-after-restart"
	script := `while [ ! -e "$1" ]; do sleep 0.02; done; printf '%s' "$2"; : > "$3"; while [ ! -e "$4" ]; do sleep 0.02; done; printf '%s' "$5"`
	started := h.run("--", "/bin/sh", "-c", script, "jinushi-physical-evidence", writeGate, prefix, writtenMarker, finishGate, suffix)
	h.waitUntil("held Run to have a persisted resource sample", 10*time.Second, func() bool {
		current := h.inspect(started.ID)
		return current.State == "running" && current.LastResourceSampleAt != nil && current.Resources.SampleIntervalMs > 0
	})
	baseline := h.inspect(started.ID)
	if baseline.LastResourceSampleAt == nil || baseline.Resources.SampleIntervalMs <= 0 {
		t.Fatalf("running Run omitted baseline resource sample evidence: %+v", baseline)
	}
	baselineSampleAt := *baseline.LastResourceSampleAt
	sampleInterval := time.Duration(baseline.Resources.SampleIntervalMs) * time.Millisecond

	supervisorDown := false
	t.Cleanup(func() {
		if supervisorDown && h.sup == nil {
			h.startSupervisor()
			supervisorDown = false
		}
	})
	h.stopSupervisor(true)
	supervisorDown = true
	supervisorDownAt := time.Now().UTC()
	if err := os.WriteFile(writeGate, []byte("write"), 0600); err != nil {
		t.Fatalf("release prefix while Supervisor is down: %v", err)
	}
	h.waitUntil("workload to write its prefix while Supervisor is down", 10*time.Second, func() bool {
		_, err := os.Stat(writtenMarker)
		return err == nil
	})
	// Keep the Supervisor unavailable across several backend sampling intervals.
	time.Sleep(5*sampleInterval + 100*time.Millisecond)
	restartStartedAt := time.Now().UTC()
	h.startSupervisor()
	supervisorDown = false

	reconciled := h.inspect(started.ID)
	if reconciled.State != "running" || !reconciled.ResourceGap {
		t.Fatalf("restart did not retain the live Run and record a resource gap: state=%s resourceGap=%v", reconciled.State, reconciled.ResourceGap)
	}
	if reconciled.LastResourceSampleAt == nil || reconciled.LastOutputAt == nil {
		t.Fatalf("restart omitted physical observation timestamps: sample=%v output=%v", reconciled.LastResourceSampleAt, reconciled.LastOutputAt)
	}
	if reconciled.LastOutputAt.Before(supervisorDownAt) || !reconciled.LastOutputAt.Before(restartStartedAt) {
		t.Fatalf("LastOutputAt does not identify the physical write during Supervisor downtime: down=%s restart=%s LastOutputAt=%s", supervisorDownAt, restartStartedAt, *reconciled.LastOutputAt)
	}

	_, _, journal := h.invoke(5*time.Second, "events", "--state-dir", h.stateDir, started.ID)
	if journal.Error != nil {
		t.Fatalf("read reconciled lifecycle journal: %+v", journal.Error)
	}
	var gapEvent *event
	var resourceSamples []*event
	var lastPhysicalOutput *event
	for i := range journal.Events {
		current := &journal.Events[i]
		switch current.Kind {
		case "resource.gap":
			gapEvent = current
		case "resource.sample":
			resourceSamples = append(resourceSamples, current)
		case "output.chunk":
			if current.data()["stream"] == "stdout" {
				var body struct {
					TimestampBasis string `json:"timestampBasis"`
				}
				encoded, err := json.Marshal(current.data())
				if err != nil || json.Unmarshal(encoded, &body) != nil {
					t.Fatalf("decode output.chunk evidence: body=%v err=%v", current.data(), err)
				}
				if body.TimestampBasis != "stream-last-write" {
					t.Fatalf("output.chunk used %q instead of physical stream write time: %+v", body.TimestampBasis, current)
				}
				if lastPhysicalOutput == nil || current.ObservedAt.After(lastPhysicalOutput.ObservedAt) {
					lastPhysicalOutput = current
				}
			}
		}
	}
	if gapEvent == nil {
		t.Fatalf("reconciliation journal omitted resource.gap evidence: %+v", journal.Events)
	}
	var gapBody struct {
		From            time.Time `json:"from"`
		To              time.Time `json:"to"`
		Status          string    `json:"status"`
		Reason          string    `json:"reason"`
		LatestResources resources `json:"latestResources"`
	}
	encodedGap, err := json.Marshal(gapEvent.data())
	if err != nil || json.Unmarshal(encodedGap, &gapBody) != nil {
		t.Fatalf("decode resource.gap body: body=%v err=%v", gapEvent.data(), err)
	}
	gapDuration := gapBody.To.Sub(gapBody.From)
	if gapBody.From.Before(baselineSampleAt) || gapBody.From.After(baselineSampleAt.Add(2*sampleInterval)) || reconciled.LastResourceSampleAt.Before(gapBody.To) || gapDuration <= 2*sampleInterval || gapDuration > 30*sampleInterval {
		t.Fatalf("resource.gap interval is not bounded by physical samples: body=%+v baseline=%s interval=%s", gapBody, baselineSampleAt, sampleInterval)
	}
	if gapBody.Status != "unavailable" || gapBody.Reason != "supervisor-unavailable" {
		t.Fatalf("resource.gap did not explain unavailable Supervisor sampling: %+v", gapBody)
	}
	if gapBody.LatestResources.SampleIntervalMs != baseline.Resources.SampleIntervalMs {
		t.Fatalf("resource.gap latest metrics lost the sample interval: %+v", gapBody.LatestResources)
	}
	for name, sampled := range map[string]metric{
		"memoryBytes":      gapBody.LatestResources.MemoryBytes,
		"peakMemoryBytes":  gapBody.LatestResources.PeakMemoryBytes,
		"cpuTimeNs":        gapBody.LatestResources.CPUTimeNs,
		"processCount":     gapBody.LatestResources.ProcessCount,
		"peakProcessCount": gapBody.LatestResources.PeakProcessCount,
	} {
		if sampled.Status != "measured" && sampled.Status != "unavailable" && sampled.Status != "unsupported" {
			t.Fatalf("resource.gap latest metric %s has no explicit evidence status: %+v", name, sampled)
		}
	}
	var sampleBody struct {
		Resources resources `json:"resources"`
	}
	var sampleEvent *event
	for _, current := range resourceSamples {
		if current.ObservedAt.Equal(gapBody.To) {
			sampleEvent = current
			break
		}
	}
	if sampleEvent == nil || gapEvent.Seq >= sampleEvent.Seq {
		t.Fatalf("resource.gap was not followed by its matching resource.sample event: gap=%+v samples=%+v", gapEvent, resourceSamples)
	}
	encodedSample, err := json.Marshal(sampleEvent.data())
	if err != nil || json.Unmarshal(encodedSample, &sampleBody) != nil {
		t.Fatalf("decode resource.sample body: body=%v err=%v", sampleEvent.data(), err)
	}
	encodedLatest, _ := json.Marshal(gapBody.LatestResources)
	encodedSampled, _ := json.Marshal(sampleBody.Resources)
	if !sampleEvent.ObservedAt.Equal(gapBody.To) || !bytes.Equal(encodedLatest, encodedSampled) {
		t.Fatalf("resource.sample did not carry the latest gap snapshot at its physical timestamp: sample=%+v gap=%+v", sampleEvent, gapBody)
	}
	if lastPhysicalOutput == nil || !lastPhysicalOutput.ObservedAt.Equal(*reconciled.LastOutputAt) || lastPhysicalOutput.ObservedAt.Before(supervisorDownAt) || !lastPhysicalOutput.ObservedAt.Before(restartStartedAt) {
		t.Fatalf("output.chunk timestamp does not match the physical pre-restart write: event=%+v LastOutputAt=%v downtime=[%s,%s)", lastPhysicalOutput, reconciled.LastOutputAt, supervisorDownAt, restartStartedAt)
	}
	storedPrefix, storedOutput := readRunStdout(t, h, started.ID, 1024)
	if storedOutput.Error != nil || storedOutput.Gap || !bytes.Equal(storedPrefix, []byte(prefix)) || reconciled.Output.Stdout.ObservedBytes != int64(len(prefix)) {
		t.Fatalf("reconciliation did not import the exact pre-restart prefix once: bytes=%q response=%+v", storedPrefix, storedOutput)
	}

	if err := os.WriteFile(finishGate, []byte("finish"), 0600); err != nil {
		t.Fatalf("release post-restart suffix: %v", err)
	}
	code, completed := h.await(started.ID, 15*time.Second)
	if code != 0 || completed.Receipt.Outcome != "exited" || completed.Receipt.ExitCode == nil || *completed.Receipt.ExitCode != 0 {
		t.Fatalf("physical evidence Run did not exit normally: code=%d receipt=%+v", code, completed.Receipt)
	}
	if !completed.ResourceGap || !completed.Receipt.EvidenceIncomplete {
		t.Fatalf("terminal receipt failed to report incomplete evidence after a Supervisor resource gap: %+v", completed.Receipt)
	}
}

func readRunStdout(t *testing.T, h *harness, id string, limit int64) ([]byte, response) {
	t.Helper()
	code, _, result := h.invoke(5*time.Second, "output", "--state-dir", h.stateDir, "--stream", "stdout", "--offset", "0", "--limit", strconv.FormatInt(limit, 10), "--json", id)
	if code != 0 || result.Error != nil {
		t.Fatalf("read stdout for Run %s: exit=%d response=%+v", id, code, result)
	}
	data, err := base64.StdEncoding.DecodeString(result.Data)
	if err != nil {
		t.Fatalf("decode stdout for Run %s: %v; response=%+v", id, err, result)
	}
	return data, result
}

func TestConcurrentRunsRemainIsolated(t *testing.T) {
	h := newHarness(t)
	const count = 12
	started := make([]run, count)
	for i := range started {
		started[i] = h.run("--", "/bin/sh", "-c", fmt.Sprintf("sleep 0.05; printf run-%02d", i))
	}
	type result struct {
		code int
		run  run
		err  error
	}
	results := make(chan result, count)
	var wg sync.WaitGroup
	for _, accepted := range started {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, jinushiBinary, "await", "--state-dir", h.stateDir, id)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			code := 0
			if err != nil {
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					code = exitErr.ExitCode()
				}
			}
			var response response
			if decodeErr := json.Unmarshal(stdout.Bytes(), &response); decodeErr != nil {
				results <- result{code: code, err: fmt.Errorf("decode concurrent await: %w; stderr=%q", decodeErr, stderr.String())}
				return
			}
			if response.Run == nil {
				results <- result{code: code, err: fmt.Errorf("concurrent await omitted Run: %s", stdout.String())}
				return
			}
			results <- result{code: code, run: *response.Run}
		}(accepted.ID)
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil || result.code != 0 || result.run.Receipt == nil || result.run.Receipt.Outcome != "exited" || result.run.Receipt.ExitCode == nil || *result.run.Receipt.ExitCode != 0 {
			t.Errorf("concurrent Run did not remain isolated: exit=%d run=%+v err=%v", result.code, result.run, result.err)
		}
	}
}

func TestWallTimeLimitEnforcement(t *testing.T) {
	h := newHarness(t)
	started := h.run("--wall-time-ms", "750", "--", "/bin/sh", "-c", "sleep 30")
	code, completed := h.await(started.ID, 15*time.Second)
	if code == 0 || completed.Receipt.Outcome != "timed-out" || !completed.Receipt.TerminationRequested || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("wall-time budget did not terminate and prove cleanup: exit=%d receipt=%+v", code, completed.Receipt)
	}
}

func TestMemoryLimitEnforcementWhenKernelDelegatesIt(t *testing.T) {
	h := newHarness(t)
	caps := h.getCapabilities()
	if !caps.MemoryEnforcement {
		t.Skipf("Linux host does not delegate enforceable cgroup v2 memory limits; capabilities: %+v", caps)
	}
	started := h.run("--memory-bytes", strconv.Itoa(64<<20), "--", helperBinary, "allocate", strconv.Itoa(256<<20))
	code, completed := h.await(started.ID, 30*time.Second)
	if code == 0 || completed.Receipt.Outcome != "resource-limit" || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("kernel memory limit did not terminate the owned Run: exit=%d receipt=%+v", code, completed.Receipt)
	}
}

func TestWritableCgroupTerminalReceiptRetainsFinalResourceCounters(t *testing.T) {
	h := newHarness(t)
	if caps := h.getCapabilities(); !strings.Contains(caps.RestartReconciliation, "cgroup-v2") {
		t.Skipf("writable cgroup v2 is unavailable on this host; capabilities: %+v", caps)
	}
	started := h.run("--", "/bin/sh", "-c", "sleep 0.3")
	code, completed := h.await(started.ID, 15*time.Second)
	if code != 0 || completed.Receipt == nil || completed.Receipt.Outcome != "exited" || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("cgroup Run did not exit cleanly: code=%d run=%+v", code, completed)
	}
	final := completed.Receipt.Resources
	if final.CPUTimeNs.Status != "measured" || final.PeakMemoryBytes.Status != "measured" || final.ProcessCount.Status != "measured" || completed.Receipt.EvidenceIncomplete {
		t.Fatalf("terminal receipt lost final cgroup counters: resources=%+v evidenceIncomplete=%v", final, completed.Receipt.EvidenceIncomplete)
	}
}

func TestLeaseExpiryTerminatesOwnedRun(t *testing.T) {
	h := newHarness(t)
	started := h.run("--lifetime", "lease-bound", "--lease-ms", "1500", "--", helperBinary, "wait", "60000")
	code, completed := h.await(started.ID, 15*time.Second)
	if code == 0 || completed.Receipt.Outcome != "cancelled" || !completed.Receipt.TerminationRequested || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("lease expiry did not terminate and prove cleanup: exit=%d receipt=%+v", code, completed.Receipt)
	}
}

func TestWallTimeLimitExpiresWhileSupervisorIsStopped(t *testing.T) {
	h := newHarness(t)
	pidFile := filepath.Join(t.TempDir(), "wall-time-descendant.pid")
	started := h.run("--wall-time-ms", "1000", "--", helperBinary, "spawn-grandchild", pidFile)
	pid := waitForPIDFile(t, pidFile, 10*time.Second)
	h.waitUntil("wall-limited Run to enter running state", 10*time.Second, func() bool {
		return h.inspect(started.ID).State == "running"
	})

	supervisorDown := false
	t.Cleanup(func() {
		if supervisorDown {
			h.startSupervisor()
			supervisorDown = false
		}
	})
	h.stopSupervisor(true)
	supervisorDown = true
	// The wall deadline plus guardian termination grace must elapse without a
	// Supervisor monitor available to drive termination.
	time.Sleep(4 * time.Second)
	waitProcessGone(t, pid, 3*time.Second)

	h.startSupervisor()
	supervisorDown = false
	code, completed := h.await(started.ID, 15*time.Second)
	if code == 0 || completed.Receipt.Outcome != "timed-out" || !completed.Receipt.TerminationRequested || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("guardian wall deadline did not survive Supervisor downtime with a terminal receipt: exit=%d receipt=%+v", code, completed.Receipt)
	}
	_, _, events := h.invoke(5*time.Second, "events", "--state-dir", h.stateDir, started.ID)
	if events.Error != nil || !containsLimitReachedEvent(events.Events, "wall-time") {
		t.Fatalf("wall-time timeout did not record a typed limit.reached event: %+v", events)
	}
	waitProcessGone(t, pid, 2*time.Second)
}

func TestLeaseExpiresWhileSupervisorIsStopped(t *testing.T) {
	h := newHarness(t)
	pidFile := filepath.Join(t.TempDir(), "lease-descendant.pid")
	started := h.run("--lifetime", "lease-bound", "--lease-ms", "1000", "--", helperBinary, "spawn-grandchild", pidFile)
	pid := waitForPIDFile(t, pidFile, 10*time.Second)
	h.waitUntil("lease-bound Run to enter running state", 10*time.Second, func() bool {
		return h.inspect(started.ID).State == "running"
	})

	supervisorDown := false
	t.Cleanup(func() {
		if supervisorDown {
			h.startSupervisor()
			supervisorDown = false
		}
	})
	h.stopSupervisor(true)
	supervisorDown = true
	// Verify the guardian enforces lease expiry and cleans the real process tree
	// before a replacement Supervisor can reconcile the Run.
	time.Sleep(4 * time.Second)
	waitProcessGone(t, pid, 3*time.Second)

	h.startSupervisor()
	supervisorDown = false
	code, completed := h.await(started.ID, 15*time.Second)
	if code == 0 || completed.Receipt.Outcome != "cancelled" || !completed.Receipt.TerminationRequested || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("guardian lease deadline did not survive Supervisor downtime with a terminal receipt: exit=%d receipt=%+v", code, completed.Receipt)
	}
	_, _, events := h.invoke(5*time.Second, "events", "--state-dir", h.stateDir, started.ID)
	if events.Error != nil || !containsEvent(events.Events, "lease.expired") {
		t.Fatalf("lease expiry event was not durable across Supervisor restart: %+v", events)
	}
	waitProcessGone(t, pid, 2*time.Second)
}

func TestLeaseRenewalAfterRestartSurvivesOriginalExpiry(t *testing.T) {
	h := newHarness(t)
	pidFile := filepath.Join(t.TempDir(), "renewed-lease-descendant.pid")
	const initialLeaseMs = int64(5000)
	const renewedLeaseMs = int64(10000)
	started := h.run("--lifetime", "lease-bound", "--lease-ms", strconv.FormatInt(initialLeaseMs, 10), "--", helperBinary, "spawn-grandchild", pidFile)
	pid := waitForPIDFile(t, pidFile, 10*time.Second)
	h.waitUntil("renewal Run to enter running state", 10*time.Second, func() bool {
		return h.inspect(started.ID).State == "running"
	})
	if started.LeaseGeneration == 0 || started.LeaseExpiry == nil {
		t.Fatalf("accepted lease-bound Run omitted initial lease evidence: %+v", started)
	}
	originalGeneration := started.LeaseGeneration
	originalExpiry := *started.LeaseExpiry

	supervisorDown := false
	t.Cleanup(func() {
		if supervisorDown {
			h.startSupervisor()
			supervisorDown = false
		}
	})
	h.stopSupervisor(true)
	supervisorDown = true
	time.Sleep(200 * time.Millisecond)
	h.startSupervisor()
	supervisorDown = false

	reconciled := h.inspect(started.ID)
	if reconciled.State != "running" || reconciled.LeaseGeneration != originalGeneration || reconciled.LeaseExpiry == nil || !reconciled.LeaseExpiry.Equal(originalExpiry) {
		t.Fatalf("Supervisor restart did not recover the original live lease: %+v", reconciled)
	}
	code, _, renewedResponse := h.invoke(8*time.Second, "lease", "renew", "--state-dir", h.stateDir, "--generation", strconv.FormatUint(reconciled.LeaseGeneration, 10), "--lease-ms", strconv.FormatInt(renewedLeaseMs, 10), started.ID)
	if code != 0 || renewedResponse.Error != nil || renewedResponse.Run == nil {
		t.Fatalf("renew lease through production CLI after restart: exit=%d response=%+v", code, renewedResponse)
	}
	renewed := *renewedResponse.Run
	if renewed.LeaseGeneration != originalGeneration+1 || renewed.LeaseExpiry == nil || !renewed.LeaseExpiry.After(originalExpiry) {
		t.Fatalf("lease renewal did not advance durable generation/expiry: old generation=%d expiry=%s, renewed=%+v", originalGeneration, originalExpiry, renewed)
	}
	code, _, duplicateRenewal := h.invoke(8*time.Second, "lease", "renew", "--state-dir", h.stateDir, "--generation", strconv.FormatUint(reconciled.LeaseGeneration, 10), "--lease-ms", strconv.FormatInt(renewedLeaseMs, 10), started.ID)
	if code != 0 || duplicateRenewal.Error != nil || duplicateRenewal.Run == nil || duplicateRenewal.Run.LeaseGeneration != renewed.LeaseGeneration || duplicateRenewal.Run.LeaseExpiry == nil || !duplicateRenewal.Run.LeaseExpiry.Equal(*renewed.LeaseExpiry) {
		t.Fatalf("identical renewal retry was not idempotent: exit=%d response=%+v original=%+v", code, duplicateRenewal, renewed)
	}
	code, _, changedRenewal := h.invoke(8*time.Second, "lease", "renew", "--state-dir", h.stateDir, "--generation", strconv.FormatUint(reconciled.LeaseGeneration, 10), "--lease-ms", strconv.FormatInt(renewedLeaseMs+1000, 10), started.ID)
	if code == 0 || changedRenewal.Error == nil || changedRenewal.Error.Code != "stale-generation" {
		t.Fatalf("stale generation with a different duration was not rejected: exit=%d response=%+v", code, changedRenewal)
	}
	afterRejectedRenewal := h.inspect(started.ID)
	if afterRejectedRenewal.LeaseGeneration != renewed.LeaseGeneration || afterRejectedRenewal.LeaseExpiry == nil || !afterRejectedRenewal.LeaseExpiry.Equal(*renewed.LeaseExpiry) {
		t.Fatalf("rejected renewal changed durable lease state: got %+v, want generation=%d expiry=%s", afterRejectedRenewal, renewed.LeaseGeneration, renewed.LeaseExpiry)
	}

	// Stop the Supervisor again and cross the original deadline. If renewal was
	// not persisted into Guardian ownership, the actual descendant will die at
	// the old expiry while the Supervisor is unavailable.
	h.stopSupervisor(true)
	supervisorDown = true
	oldExpiryProofTime := originalExpiry.Add(300 * time.Millisecond)
	if !renewed.LeaseExpiry.After(oldExpiryProofTime.Add(time.Second)) {
		t.Fatalf("renewed lease leaves too little proof window beyond the original expiry: renewed=%s proof=%s", renewed.LeaseExpiry, oldExpiryProofTime)
	}
	if wait := time.Until(oldExpiryProofTime); wait > 0 {
		time.Sleep(wait)
	}
	if !linuxProcessExecuting(pid) {
		t.Fatalf("real descendant PID %d did not survive beyond the original lease expiry", pid)
	}

	h.startSupervisor()
	supervisorDown = false
	recovered := h.inspect(started.ID)
	if recovered.State != "running" || recovered.LeaseGeneration != renewed.LeaseGeneration || recovered.LeaseExpiry == nil || !recovered.LeaseExpiry.Equal(*renewed.LeaseExpiry) {
		t.Fatalf("renewed lease evidence did not survive a second Supervisor restart: %+v, want generation=%d expiry=%s", recovered, renewed.LeaseGeneration, renewed.LeaseExpiry)
	}
	code, _, canceled := h.invoke(5*time.Second, "cancel", "--state-dir", h.stateDir, "--request-id", "renewed-lease-cancel", "--expected-generation", strconv.FormatUint(h.inspect(started.ID).Generation, 10), started.ID)
	if code != 0 || canceled.Error != nil {
		t.Fatalf("cancel renewed lease Run: exit=%d response=%+v", code, canceled)
	}
	code, completed := h.await(started.ID, 15*time.Second)
	if code == 0 || completed.Receipt.Outcome != "cancelled" || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("cancel renewed Run did not prove physical cleanup: exit=%d receipt=%+v", code, completed.Receipt)
	}
	waitProcessGone(t, pid, 3*time.Second)
}

func containsEvent(events []event, kind string) bool {
	for _, item := range events {
		if item.Kind == kind {
			return true
		}
	}
	return false
}

func containsLimitReachedEvent(events []event, limit string) bool {
	for _, item := range events {
		if item.Kind == "limit.reached" && item.data()["limit"] == limit {
			return true
		}
	}
	return false
}

func waitForPIDFile(t *testing.T, path string, timeout time.Duration) int {
	t.Helper()
	var pid int
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			value, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr == nil && value > 0 {
				pid = value
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatalf("timed out waiting for descendant PID in %s", path)
	}
	return pid
}

func linuxProcessExecuting(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return false
	}
	fields := strings.Fields(string(data[end+2:]))
	return len(fields) > 0 && fields[0] != "Z" && fields[0] != "X"
}

func waitProcessGone(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !linuxProcessExecuting(pid) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("descendant PID %d remained executable after Jinushi reported terminal cleanup", pid)
}

func waitEventCount(h *harness, id, kind string, count int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, result := h.invoke(5*time.Second, "events", "--state-dir", h.stateDir, id)
		if result.Error == nil {
			found := 0
			for _, item := range result.Events {
				if item.Kind == kind {
					found++
				}
			}
			if found >= count {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %d event(s) of kind %s", count, kind)
}
