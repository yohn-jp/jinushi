package guardian

import (
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

type deadlineProcess struct {
	graceCalls        []time.Duration
	terminationResult *backend.TerminationResult
	terminationErr    error
	observeErr        error
}

type capabilityProcess struct {
	deadlineProcess
	effective model.Capabilities
}

func (p *capabilityProcess) EffectiveCapabilities() model.Capabilities { return p.effective }

type capabilityBackend struct{ capabilities model.Capabilities }

func (b *capabilityBackend) Capabilities() model.Capabilities { return b.capabilities }
func (*capabilityBackend) Start(model.RunSpec, io.Writer, io.Writer) (backend.Process, error) {
	return nil, errors.New("not used")
}
func (*capabilityBackend) Reconcile(model.Ownership) (backend.ReconcileResult, error) {
	return backend.ReconcileResult{}, errors.New("not used")
}

func (*deadlineProcess) Ownership() model.Ownership  { return model.Ownership{Backend: "test"} }
func (*deadlineProcess) Wait() (backend.Exit, error) { return backend.Exit{}, nil }
func (p *deadlineProcess) Observe() (model.Resources, error) {
	return unavailableResources(), p.observeErr
}
func (*deadlineProcess) Signal(string) error { return nil }
func (p *deadlineProcess) Terminate(grace time.Duration) (backend.TerminationResult, error) {
	p.graceCalls = append(p.graceCalls, grace)
	if p.terminationResult != nil {
		return *p.terminationResult, p.terminationErr
	}
	if p.terminationErr != nil {
		return backend.TerminationResult{}, p.terminationErr
	}
	return backend.TerminationResult{Requested: true, TreeEmpty: true}, nil
}
func (*deadlineProcess) WriteInput([]byte) error     { return nil }
func (*deadlineProcess) Resize(uint16, uint16) error { return nil }
func (*deadlineProcess) CloseInput() error           { return nil }

func TestGuardianDeadlinesUseConfiguredTerminationGrace(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		setup  func(*runState)
	}{
		{
			name:   "wall-time",
			reason: "timed-out",
			setup: func(s *runState) {
				s.descriptor.Spec.Limits.WallTimeMs = 1000
				s.startedMono = time.Now().Add(-2 * time.Second)
				now := time.Now().Add(-2 * time.Second).UTC()
				s.snapshot.StartedAt = &now
			},
		},
		{
			name:   "lease-expiry",
			reason: "lease-expired",
			setup: func(s *runState) {
				expiry := time.Now().Add(-time.Second).UTC()
				s.snapshot.LeaseExpiry = &expiry
				s.snapshot.LeaseGeneration = 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			spool, err := openSpool(filepath.Join(dir, "spool"), 3<<10)
			if err != nil {
				t.Fatal(err)
			}
			defer spool.close()
			process := &deadlineProcess{}
			state := &runState{
				descriptor: launchConfig{Dir: dir, TerminationGraceMs: 321},
				spool:      spool,
				process:    process,
				terminal:   make(chan struct{}),
				snapshot: Snapshot{
					Version: ProtocolVersion, RunID: "run_deadline", State: model.Running,
					Resources: unavailableResources(),
				},
			}
			test.setup(state)
			if err := state.persist(); err != nil {
				t.Fatal(err)
			}
			state.sample()
			if len(process.graceCalls) != 0 {
				t.Fatalf("sample path enforced a deadline: calls=%v", process.graceCalls)
			}

			state.enforceDeadlines()
			if len(process.graceCalls) != 1 || process.graceCalls[0] != 321*time.Millisecond {
				t.Fatalf("termination grace calls = %v, want [321ms]", process.graceCalls)
			}
			snapshot, err := readSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.TerminationReason != test.reason || snapshot.State != model.Terminating {
				t.Fatalf("persisted deadline transition = %s/%s, want terminating/%s", snapshot.State, snapshot.TerminationReason, test.reason)
			}
		})
	}
}

func TestFailedTerminationPersistsUncertainWithOwnership(t *testing.T) {
	backendFailure := errors.New("backend failed with private diagnostic")
	tests := []struct {
		name         string
		result       backend.TerminationResult
		terminateErr error
	}{
		{
			name: "backend error",
			result: backend.TerminationResult{
				Requested: true, Forced: true, TreeEmpty: true,
			},
			terminateErr: backendFailure,
		},
		{
			name: "tree emptiness unproven",
			result: backend.TerminationResult{
				Requested: true, Forced: true, TreeEmpty: false,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			spool, err := openSpool(filepath.Join(dir, "spool"), 3<<10)
			if err != nil {
				t.Fatal(err)
			}
			defer spool.close()
			owner := model.Ownership{Backend: "test", Token: "owned"}
			process := &deadlineProcess{terminationResult: &test.result, terminationErr: test.terminateErr}
			state := &runState{
				descriptor: launchConfig{Dir: dir},
				spool:      spool,
				process:    process,
				terminal:   make(chan struct{}),
				snapshot: Snapshot{
					Version: ProtocolVersion, RunID: "run_termination", State: model.Running,
					Ownership: &owner, Resources: unavailableResources(),
				},
			}
			if err := state.persist(); err != nil {
				t.Fatal(err)
			}

			_, err = state.terminate(250*time.Millisecond, "lease-expired")
			if !errors.Is(err, ErrUncertain) {
				t.Fatalf("terminate() error = %v, want ErrUncertain", err)
			}
			snapshot, err := readSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.State != model.Uncertain || snapshot.Reason != "termination-outcome-unproven" {
				t.Fatalf("persisted failed termination = %s/%q", snapshot.State, snapshot.Reason)
			}
			if snapshot.Ownership == nil || snapshot.Ownership.Token != owner.Token {
				t.Fatalf("ownership evidence was lost: %+v", snapshot.Ownership)
			}
			if !snapshot.Termination.Requested || snapshot.Termination.TreeEmpty {
				t.Fatalf("failed termination proof = %+v", snapshot.Termination)
			}
			if snapshot.TerminationReason != "lease-expired" || snapshot.Receipt != nil {
				t.Fatalf("failed termination was not kept nonterminal: reason=%q receipt=%+v", snapshot.TerminationReason, snapshot.Receipt)
			}
			if strings.Contains(snapshot.Reason, "private diagnostic") {
				t.Fatalf("backend diagnostic leaked into durable reason: %q", snapshot.Reason)
			}
		})
	}
}

func TestUncertainTerminationCanBeRetriedThroughLiveHelper(t *testing.T) {
	dir := t.TempDir()
	spool, err := openSpool(filepath.Join(dir, "spool"), 3<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	owner := model.Ownership{Backend: "test", Token: "owned"}
	failed := backend.TerminationResult{Requested: true}
	process := &deadlineProcess{terminationResult: &failed}
	state := &runState{
		descriptor: launchConfig{Dir: dir},
		spool:      spool,
		process:    process,
		terminal:   make(chan struct{}),
		snapshot: Snapshot{
			Version: ProtocolVersion, RunID: "run_retry", State: model.Running,
			Ownership: &owner, Resources: unavailableResources(),
		},
	}
	if err := state.persist(); err != nil {
		t.Fatal(err)
	}
	if _, err := state.terminate(250*time.Millisecond, "timed-out"); !errors.Is(err, ErrUncertain) {
		t.Fatalf("first terminate() error = %v, want ErrUncertain", err)
	}

	proven := backend.TerminationResult{Requested: true, TreeEmpty: true}
	process.terminationResult = &proven
	result, err := state.terminate(250*time.Millisecond, "timed-out")
	if err != nil || !result.TreeEmpty {
		t.Fatalf("retry terminate() = %+v, %v; want tree-empty proof", result, err)
	}
	snapshot, err := readSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != model.Terminating || !snapshot.Termination.TreeEmpty || snapshot.Receipt != nil {
		t.Fatalf("termination retry state = %s/%+v receipt=%+v", snapshot.State, snapshot.Termination, snapshot.Receipt)
	}
}

func TestResourceObservationTimestampAndIntervalPersist(t *testing.T) {
	dir := t.TempDir()
	spool, err := openSpool(filepath.Join(dir, "spool"), 3<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	if _, err := spool.writer("stdout").Write([]byte("out")); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.writer("stderr").Write([]byte("err")); err != nil {
		t.Fatal(err)
	}
	state := &runState{
		descriptor: launchConfig{Dir: dir, SampleIntervalMs: 321},
		spool:      spool,
		process:    &deadlineProcess{observeErr: errors.New("temporary observation failure")},
		terminal:   make(chan struct{}),
		snapshot: Snapshot{
			Version: ProtocolVersion, RunID: "run_sample", State: model.Uncertain,
			Resources: unavailableResources(),
		},
	}
	if err := state.persist(); err != nil {
		t.Fatal(err)
	}

	state.sample()
	first, err := readSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first.LastResourceSampleAt == nil || first.LastResourceSampleAt.IsZero() {
		t.Fatal("sample() did not persist the observation timestamp")
	}
	if first.Resources.SampleIntervalMs != 321 {
		t.Fatalf("sample interval = %dms, want 321ms", first.Resources.SampleIntervalMs)
	}
	if first.Resources.MemoryBytes.Status != "unavailable" {
		t.Fatalf("failed observation was not preserved as unavailable: %+v", first.Resources.MemoryBytes)
	}
	if first.LastOutputAt == nil || len(first.OutputLastWriteAt) != 2 {
		t.Fatalf("sample() did not persist per-stream output times: latest=%v streams=%v", first.LastOutputAt, first.OutputLastWriteAt)
	}
	if first.OutputLastWriteAt["stdout"].IsZero() || first.OutputLastWriteAt["stderr"].IsZero() {
		t.Fatalf("sample() persisted invalid output times: %v", first.OutputLastWriteAt)
	}

	observed, err := state.observe()
	if err != nil {
		t.Fatal(err)
	}
	if observed.LastResourceSampleAt == nil || observed.LastResourceSampleAt.Before(*first.LastResourceSampleAt) {
		t.Fatalf("observe() did not advance/preserve observation time: first=%v observed=%v", first.LastResourceSampleAt, observed.LastResourceSampleAt)
	}
	if observed.Resources.SampleIntervalMs != 321 {
		t.Fatalf("observed sample interval = %dms, want 321ms", observed.Resources.SampleIntervalMs)
	}
	if observed.LastOutputAt == nil || !observed.LastOutputAt.Equal(*first.LastOutputAt) {
		t.Fatalf("observe() changed the last physical output time: first=%v observed=%v", first.LastOutputAt, observed.LastOutputAt)
	}

	handle := &Handle{descriptor: privateDescriptor{Dir: dir}}
	chunk, err := handle.readOutput("stdout", 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if chunk.LastWriteAt == nil || !chunk.LastWriteAt.Equal(first.OutputLastWriteAt["stdout"]) {
		t.Fatalf("offline output replay timestamp = %v, snapshot timestamp = %v", chunk.LastWriteAt, first.OutputLastWriteAt["stdout"])
	}
}

func TestTerminalReceiptUsesPerExecutionCapabilities(t *testing.T) {
	dir := t.TempDir()
	spool, err := openSpool(filepath.Join(dir, "spool"), 3<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	host := model.Capabilities{Backend: "linux", PTY: true, MemoryEnforcement: true, ProcessCountEnforcement: true, Signals: []string{"TERM", "KILL"}}
	effective := model.Capabilities{Backend: "linux", PTY: false, MemoryEnforcement: false, ProcessCountEnforcement: false, Signals: []string{"TERM", "KILL"}}
	process := &capabilityProcess{effective: effective}
	state := &runState{
		descriptor: launchConfig{Dir: dir, SampleIntervalMs: 100},
		spool:      spool,
		process:    process,
		terminal:   make(chan struct{}),
		snapshot: Snapshot{
			Version: ProtocolVersion, RunID: "run_effective_caps", State: model.Running,
			EffectiveCapabilities: ptrCapabilities(effective),
			Resources:             unavailableResources(),
		},
	}
	selected := &capabilityBackend{capabilities: host}
	derived := effectiveCapabilities(selected, process, false)
	if derived.Backend != effective.Backend || derived.PTY || derived.MemoryEnforcement || derived.ProcessCountEnforcement {
		t.Fatalf("derived capabilities = %+v, want per-Run capabilities %+v", derived, effective)
	}
	if !state.finish(backend.Exit{Outcome: "exited", FinishedAt: time.Now().UTC()}) {
		t.Fatal("finish failed to persist terminal receipt")
	}
	snapshot, err := readSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Receipt == nil || snapshot.Receipt.Capabilities.Backend != "linux" || snapshot.Receipt.Capabilities.PTY || snapshot.Receipt.Capabilities.MemoryEnforcement || snapshot.Receipt.Capabilities.ProcessCountEnforcement {
		t.Fatalf("receipt did not preserve actual capabilities: %+v", snapshot.Receipt)
	}
}

func ptrCapabilities(value model.Capabilities) *model.Capabilities { return &value }

func TestUnavailableCurrentPreservesUnsupportedAndDoesNotReuseMeasuredCPU(t *testing.T) {
	previous := model.Resources{
		MemoryBytes:     model.Metric{Status: "unsupported"},
		CPUTimeNs:       model.Metric{Status: "measured", Value: 42},
		ProcessCount:    model.Metric{Status: "measured", Value: 2},
		PeakMemoryBytes: model.Metric{Status: "measured", Value: 99},
	}
	got := unavailableCurrent(previous)
	if got.MemoryBytes.Status != "unsupported" || got.CPUTimeNs.Status != "unavailable" || got.ProcessCount.Status != "unavailable" {
		t.Fatalf("failed current observation status = %+v", got)
	}
	if got.PeakMemoryBytes != previous.PeakMemoryBytes {
		t.Fatalf("historical peak changed: %+v", got.PeakMemoryBytes)
	}
}

var _ backend.Process = (*deadlineProcess)(nil)
