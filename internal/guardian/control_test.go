package guardian

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

type controlTestProcess struct {
	mu              sync.Mutex
	owner           model.Ownership
	capabilities    model.Capabilities
	paused          bool
	memoryHigh      int64
	memoryUnlimited bool
	cpuQuota        int64
	cpuUnlimited    bool
	calls           int
	failNext        bool
	beforeEffect    func()
}

func (p *controlTestProcess) Ownership() model.Ownership      { return p.owner }
func (*controlTestProcess) Wait() (backend.Exit, error)       { return backend.Exit{}, nil }
func (*controlTestProcess) Observe() (model.Resources, error) { return model.Resources{}, nil }
func (*controlTestProcess) Signal(string) error               { return nil }
func (*controlTestProcess) Terminate(time.Duration) (backend.TerminationResult, error) {
	return backend.TerminationResult{Requested: true, TreeEmpty: true}, nil
}
func (*controlTestProcess) WriteInput([]byte) error                     { return nil }
func (*controlTestProcess) Resize(uint16, uint16) error                 { return nil }
func (*controlTestProcess) CloseInput() error                           { return nil }
func (p *controlTestProcess) EffectiveCapabilities() model.Capabilities { return p.capabilities }
func (p *controlTestProcess) SupportsCgroupFreeze() bool                { return p.capabilities.CgroupFreeze }
func (p *controlTestProcess) SupportsMemoryHighControl() bool {
	return p.capabilities.MemoryHighControl
}
func (p *controlTestProcess) SupportsCPUQuotaControl() bool { return p.capabilities.CPUQuotaControl }

func (p *controlTestProcess) Pause() error  { p.runBeforeEffect(); return p.setPaused(true) }
func (p *controlTestProcess) Resume() error { p.runBeforeEffect(); return p.setPaused(false) }
func (p *controlTestProcess) runBeforeEffect() {
	p.mu.Lock()
	callback := p.beforeEffect
	p.mu.Unlock()
	if callback != nil {
		callback()
	}
}
func (p *controlTestProcess) setPaused(paused bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.paused = paused
	if p.failNext {
		p.failNext = false
		return errors.New("simulated write uncertainty")
	}
	return nil
}
func (p *controlTestProcess) SetMemoryHigh(value int64) error {
	p.runBeforeEffect()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.memoryHigh = value
	p.memoryUnlimited = false
	if p.failNext {
		p.failNext = false
		return errors.New("simulated write uncertainty")
	}
	return nil
}
func (p *controlTestProcess) SetCPUQuotaPercent(value int64) error {
	p.runBeforeEffect()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.cpuQuota = value
	p.cpuUnlimited = false
	if p.failNext {
		p.failNext = false
		return errors.New("simulated write uncertainty")
	}
	return nil
}
func (p *controlTestProcess) CurrentPauseState() (bool, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused, true, nil
}
func (p *controlTestProcess) CurrentMemoryHigh() (int64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.memoryHigh, p.memoryUnlimited, nil
}
func (p *controlTestProcess) CurrentCPUQuotaPercent() (int64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cpuQuota, p.cpuUnlimited, nil
}

func newControlTestState(t *testing.T, process *controlTestProcess) *runState {
	t.Helper()
	dir := t.TempDir()
	spool, err := openSpool(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = spool.close() })
	capabilities := process.capabilities
	return &runState{
		descriptor: launchConfig{Version: ProtocolVersion, RunID: "run_control", Dir: dir},
		spool:      spool,
		process:    process,
		snapshot: Snapshot{
			Version: ProtocolVersion, RunID: "run_control", State: model.Running,
			EffectiveCapabilities: &capabilities,
		},
		terminal: make(chan struct{}),
	}
}

func TestGuardianControlPersistsTypedEvidenceAndReplaysRequest(t *testing.T) {
	process := &controlTestProcess{
		owner:        model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10},
		capabilities: model.Capabilities{Backend: "linux", CgroupFreeze: true, MemoryHighControl: true, CPUQuotaControl: true},
		memoryHigh:   128 << 20, cpuQuota: 300,
	}
	state := newControlTestState(t, process)
	request := rpcRequest{Op: "control-pause", RequestID: "ctl-1"}
	evidence := state.control(request)
	if evidence.Outcome != "applied" || evidence.Control != "cgroup.freeze" || evidence.Action != "pause" || evidence.OperationID != "ctl-1" {
		t.Fatalf("pause evidence is incomplete: %+v", evidence)
	}
	if evidence.PreviousValue == nil || *evidence.PreviousValue != 0 || evidence.Value == nil || *evidence.Value != 1 {
		t.Fatalf("pause evidence does not preserve requested and prior values: %+v", evidence)
	}
	if got := state.dispatch(request).Control; got == nil || got.Outcome != "applied" {
		t.Fatalf("duplicate control request was not replayed: %+v", got)
	}
	process.mu.Lock()
	calls, paused := process.calls, process.paused
	process.mu.Unlock()
	if calls != 1 || !paused {
		t.Fatalf("duplicate request repeated physical control: calls=%d paused=%t", calls, paused)
	}
	stored, err := readSnapshot(state.descriptor.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Controls == nil || len(stored.Controls.Operations) != 1 || stored.Controls.Operations[0].RequestID != "ctl-1" || stored.Controls.Paused == nil || !*stored.Controls.Paused {
		t.Fatalf("durable control state was not persisted: %+v", stored.Controls)
	}
	conflict := state.control(rpcRequest{Op: "control-resume", RequestID: "ctl-1"})
	if conflict.Outcome != "failed" || conflict.Reason != "request-id-conflict" {
		t.Fatalf("reused request identity did not conflict: %+v", conflict)
	}
}

func TestGuardianMutableControlPersistsEffectiveValues(t *testing.T) {
	process := &controlTestProcess{
		owner:        model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10},
		capabilities: model.Capabilities{Backend: "linux", MemoryHighControl: true, CPUQuotaControl: true},
		memoryHigh:   0, memoryUnlimited: true, cpuQuota: 0, cpuUnlimited: true,
	}
	state := newControlTestState(t, process)
	for _, test := range []struct {
		request rpcRequest
		control string
	}{{request: rpcRequest{Op: "control-memory-high", RequestID: "ctl-mem", ControlValue: 64 << 20}, control: "memory.high"},
		{request: rpcRequest{Op: "control-cpu-quota", RequestID: "ctl-cpu", ControlValue: 200}, control: "cpu.max"}} {
		response := state.dispatch(test.request)
		if response.Control == nil || response.Control.Outcome != "applied" {
			t.Fatalf("control response for %s: %+v", test.request.Op, response)
		}
		if response.Control.Control != test.control || response.Control.Unlimited == nil || *response.Control.Unlimited || response.Control.PreviousUnlimited == nil || !*response.Control.PreviousUnlimited {
			t.Fatalf("control evidence lost finite/unlimited semantics: %+v", response.Control)
		}
	}
	stored, err := readSnapshot(state.descriptor.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Controls == nil || stored.Controls.MemoryHighBytes == nil || *stored.Controls.MemoryHighBytes != 64<<20 || stored.Controls.CPUQuotaPercent == nil || *stored.Controls.CPUQuotaPercent != 200 {
		t.Fatalf("effective mutable controls were not retained: %+v", stored.Controls)
	}
}

func TestGuardianControlFailureIsDurablyUncertainWithoutRetry(t *testing.T) {
	process := &controlTestProcess{
		owner:        model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10},
		capabilities: model.Capabilities{Backend: "linux", CgroupFreeze: true},
		failNext:     true,
	}
	state := newControlTestState(t, process)
	request := rpcRequest{Op: "control-pause", RequestID: "ctl-uncertain"}
	first := state.control(request)
	if first.Outcome != "uncertain" || first.Reason != "control-effect-unverified" || state.snapshot.State != model.Uncertain {
		t.Fatalf("failed physical control was not classified uncertain: %+v state=%s", first, state.snapshot.State)
	}
	second := state.control(request)
	if second.Outcome != "uncertain" {
		t.Fatalf("retry did not replay uncertain result: %+v", second)
	}
	process.mu.Lock()
	calls := process.calls
	process.mu.Unlock()
	if calls != 1 {
		t.Fatalf("ambiguous control was applied %d times", calls)
	}
	stored, err := readSnapshot(state.descriptor.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != model.Uncertain || stored.Controls == nil || len(stored.Controls.Operations) != 1 || stored.Controls.Operations[0].Evidence.Outcome != "uncertain" {
		t.Fatalf("uncertain control evidence was not durable: %+v", stored)
	}
}

func TestGuardianControlIntentIsDurableBeforePhysicalEffect(t *testing.T) {
	var state *runState
	process := &controlTestProcess{
		owner:        model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10},
		capabilities: model.Capabilities{Backend: "linux", CgroupFreeze: true},
		beforeEffect: func() {
			snapshot, err := readSnapshot(state.descriptor.Dir)
			if err != nil {
				t.Errorf("read pre-effect snapshot: %v", err)
				return
			}
			if snapshot.Controls == nil || len(snapshot.Controls.Operations) != 1 {
				t.Errorf("control intent was not durable before physical effect: %+v", snapshot.Controls)
				return
			}
			operation := snapshot.Controls.Operations[0]
			if operation.RequestID != "ctl-before-effect" || operation.Status != "pending" {
				t.Errorf("persisted intent has wrong identity/state: %+v", operation)
			}
		},
	}
	state = newControlTestState(t, process)
	if evidence := state.control(rpcRequest{Op: "control-pause", RequestID: "ctl-before-effect"}); evidence.Outcome != "applied" {
		t.Fatalf("pause control: %+v", evidence)
	}
}

func TestGuardianRetainsRequestEvidenceAcrossLaterControls(t *testing.T) {
	process := &controlTestProcess{
		owner:        model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10},
		capabilities: model.Capabilities{Backend: "linux", CgroupFreeze: true},
	}
	state := newControlTestState(t, process)
	firstRequest := rpcRequest{Op: "control-pause", RequestID: "ctl-history-1"}
	if got := state.control(firstRequest); got.Outcome != "applied" {
		t.Fatalf("first control: %+v", got)
	}
	if got := state.control(rpcRequest{Op: "control-resume", RequestID: "ctl-history-2"}); got.Outcome != "applied" {
		t.Fatalf("second control: %+v", got)
	}
	if got := state.control(firstRequest); got.Outcome != "applied" || got.Action != "pause" {
		t.Fatalf("older operation evidence was not replayed: %+v", got)
	}
	process.mu.Lock()
	calls := process.calls
	process.mu.Unlock()
	if calls != 2 {
		t.Fatalf("later retry repeated physical effect: calls=%d", calls)
	}
}

func TestGuardianControlEvidenceExpiryReportsMachineReadableGap(t *testing.T) {
	process := &controlTestProcess{
		owner:        model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10},
		capabilities: model.Capabilities{Backend: "linux", CgroupFreeze: true},
	}
	state := newControlTestState(t, process)
	old := time.Now().UTC().Add(-25 * time.Hour)
	state.snapshot.Controls = &ControlState{Operations: []ControlOperation{{
		RequestID: "ctl-expired", Digest: controlRequestDigest("control-pause", 1), Status: "completed",
		Evidence:  model.ControlEventPayload{OperationID: "ctl-expired", Control: "cgroup.freeze", Action: "pause", Outcome: "applied"},
		CreatedAt: old, UpdatedAt: old,
	}}}
	process.mu.Lock()
	process.paused = true
	process.mu.Unlock()
	if got := state.control(rpcRequest{Op: "control-resume", RequestID: "ctl-retained"}); got.Outcome != "applied" {
		t.Fatalf("retained control: %+v", got)
	}
	record, found, gap := state.lookupControl("ctl-expired")
	if found || record.RequestID != "" || gap.ExpiredCount != 1 || gap.ExpiredFrom == nil || gap.ExpiredThrough == nil || gap.RetainedFrom == nil {
		t.Fatalf("expired operation gap was not explicit: found=%t record=%+v gap=%+v", found, record, gap)
	}
	stored, err := readSnapshot(state.descriptor.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Controls == nil || stored.Controls.EvidenceGap.ExpiredCount != 1 {
		t.Fatalf("evidence gap was not durable: %+v", stored.Controls)
	}
}

func TestGuardianObserveRevalidatesPersistedControlState(t *testing.T) {
	process := &controlTestProcess{
		owner:        model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10},
		capabilities: model.Capabilities{Backend: "linux", CgroupFreeze: true},
	}
	state := newControlTestState(t, process)
	if evidence := state.control(rpcRequest{Op: "control-pause", RequestID: "ctl-drift"}); evidence.Outcome != "applied" {
		t.Fatalf("pause control: %+v", evidence)
	}
	process.mu.Lock()
	process.paused = false
	process.mu.Unlock()
	snapshot, err := state.observe()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != model.Uncertain || snapshot.Reason != "physical-control-state-unavailable-or-changed" {
		t.Fatalf("kernel/control-state drift was not explicit: %+v", snapshot)
	}
}

func TestGuardianControlRejectsUnsupportedCapabilityBeforePhysicalCall(t *testing.T) {
	process := &controlTestProcess{owner: model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10}}
	state := newControlTestState(t, process)
	evidence := state.control(rpcRequest{Op: "control-resume", RequestID: "ctl-no-cap"})
	if evidence.Outcome != "failed" || evidence.Reason != "unsupported" {
		t.Fatalf("unsupported control evidence: %+v", evidence)
	}
	process.mu.Lock()
	calls := process.calls
	process.mu.Unlock()
	if calls != 0 {
		t.Fatalf("unsupported capability invoked physical control %d times", calls)
	}
}

func TestGuardianConcurrentControlRetryHasSinglePhysicalEffect(t *testing.T) {
	process := &controlTestProcess{
		owner:        model.Ownership{Backend: "linux", PID: 10, StartTime: 11, ProcessGroup: 10},
		capabilities: model.Capabilities{Backend: "linux", CgroupFreeze: true},
	}
	state := newControlTestState(t, process)
	request := rpcRequest{Op: "control-pause", RequestID: "ctl-race"}
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response := state.dispatch(request)
			if response.Control == nil || response.Control.Outcome != "applied" {
				t.Errorf("concurrent duplicate returned %+v", response.Control)
			}
		}()
	}
	wait.Wait()
	process.mu.Lock()
	calls := process.calls
	process.mu.Unlock()
	if calls != 1 {
		t.Fatalf("concurrent duplicate applied control %d times", calls)
	}
}

func TestGuardianHostEnvelopeConfigPersistsInPrivateDescriptor(t *testing.T) {
	dir := t.TempDir()
	want := HostEnvelopeConfig{MemoryBytes: 1 << 30, TaskCount: 512, MaxActiveRuns: 8}
	descriptor := privateDescriptor{Version: ProtocolVersion, RunID: "run_host", Dir: dir, Token: "private-token", HostEnvelope: want}
	if err := writeDescriptor(descriptor); err != nil {
		t.Fatal(err)
	}
	got, err := readDescriptor(dir, "run_host")
	if err != nil {
		t.Fatal(err)
	}
	if got.HostEnvelope != want {
		t.Fatalf("Guardian descriptor lost host envelope: got %+v want %+v", got.HostEnvelope, want)
	}
	launch := launchConfig{Version: ProtocolVersion, RunID: "run_host", Dir: dir, HostEnvelope: want, MaxOutputBytes: 1024, TerminationGraceMs: 100, SampleIntervalMs: 50, Token: "private-token"}
	if err := validateLaunchConfig(launch); err != nil {
		t.Fatalf("matching host envelope launch config was rejected: %v", err)
	}
	launch.HostEnvelope.TaskCount++
	if err := validateLaunchConfig(launch); err == nil {
		t.Fatal("launch config with replaced host envelope did not fail closed")
	}
	if err := validateHostEnvelope(HostEnvelopeConfig{MemoryBytes: -1}); err == nil {
		t.Fatal("negative host memory ceiling was accepted")
	}
}
