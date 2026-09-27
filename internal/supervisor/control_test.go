package supervisor

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/guardian"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

type controlEvidencePhysical struct {
	*controlTestPhysical
	controlMu   sync.Mutex
	calls       map[string]int
	operations  map[string]guardian.ControlOperation
	responseErr bool
	gap         guardian.ControlEvidenceGap
	snapshot    guardian.Snapshot
	snapshotErr error
}

func newControlEvidencePhysical() *controlEvidencePhysical {
	return &controlEvidencePhysical{
		controlTestPhysical: &controlTestPhysical{},
		calls:               make(map[string]int),
		operations:          make(map[string]guardian.ControlOperation),
	}
}

func (p *controlEvidencePhysical) control(requestID, op string, value int64, responseErr bool) (model.ControlEventPayload, error) {
	publicOp := map[string]string{
		"control-pause": "pause", "control-resume": "resume",
		"control-memory-high": "memory-high", "control-cpu-quota": "cpu-quota",
	}[op]
	req := protocol.Request{Op: publicOp, RequestID: requestID}
	if publicOp == "memory-high" {
		req.MemoryHighBytes = value
	}
	if publicOp == "cpu-quota" {
		req.CPUQuotaPercent = value
	}
	guardianDigest, _ := guardianControlDigestFor(req)
	control, action := "cgroup.freeze", "pause"
	effective := value
	var unlimited *bool
	switch op {
	case "control-pause":
		effective = 1
	case "control-resume":
		action, effective = "resume", 0
	case "control-memory-high":
		control, action = "memory.high", "set"
		unlimited = boolPointer(false)
	case "control-cpu-quota":
		control, action = "cpu.max", "set"
		unlimited = boolPointer(false)
	}
	evidence := model.ControlEventPayload{
		OperationID: requestID, Control: control, Action: action,
		Value: int64Pointer(effective), Unlimited: unlimited, Outcome: "applied",
	}
	p.controlMu.Lock()
	p.calls[op]++
	p.operations[requestID] = guardian.ControlOperation{
		RequestID: requestID, Digest: guardianDigest, Status: "completed", Evidence: evidence,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	p.controlMu.Unlock()
	if responseErr || p.responseErr {
		return model.ControlEventPayload{}, errors.New("Guardian response lost after durable control completion")
	}
	return evidence, nil
}

func (p *controlEvidencePhysical) Pause(requestID string) (model.ControlEventPayload, error) {
	return p.control(requestID, "control-pause", 0, false)
}
func (p *controlEvidencePhysical) Resume(requestID string) (model.ControlEventPayload, error) {
	return p.control(requestID, "control-resume", 0, false)
}
func (p *controlEvidencePhysical) SetMemoryHigh(requestID string, value int64) (model.ControlEventPayload, error) {
	return p.control(requestID, "control-memory-high", value, false)
}
func (p *controlEvidencePhysical) SetCPUQuotaPercent(requestID string, value int64) (model.ControlEventPayload, error) {
	return p.control(requestID, "control-cpu-quota", value, false)
}
func (p *controlEvidencePhysical) LookupControl(requestID string) (guardian.ControlOperation, bool, guardian.ControlEvidenceGap, error) {
	p.controlMu.Lock()
	defer p.controlMu.Unlock()
	operation, found := p.operations[requestID]
	return operation, found, p.gap, nil
}

func (p *controlEvidencePhysical) ControlSnapshot() (guardian.Snapshot, error) {
	return p.snapshot, p.snapshotErr
}

func boolPointer(value bool) *bool { return &value }

func int64Pointer(value int64) *int64 { return &value }

func configureControlRun(t *testing.T, svc *Service, db *store.Store, capabilities model.Capabilities) {
	t.Helper()
	run, err := db.Get("run_control_test")
	if err != nil {
		t.Fatal(err)
	}
	run.EffectiveCapabilities = &capabilities
	run.Generation++
	if _, err := db.UpdateWithEvents(run, nil); err != nil {
		t.Fatal(err)
	}
	svc.active[run.ID].run = run
}

func controlEvents(t *testing.T, db *store.Store, runID string) []model.Event {
	t.Helper()
	events, _, _, err := db.Events(runID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var controls []model.Event
	for _, event := range events {
		if event.Kind == model.EventControlChanged || event.Kind == model.EventControlRecovered {
			controls = append(controls, event)
		}
	}
	return controls
}

func TestCapabilityGatedControlsRecordTypedEvidenceAndRetryOnce(t *testing.T) {
	tests := []struct {
		name   string
		op     string
		caps   model.Capabilities
		set    func(*protocol.Request)
		change func(*protocol.Request)
	}{
		{"pause", "pause", model.Capabilities{Backend: "linux", CgroupFreeze: true}, func(*protocol.Request) {}, nil},
		{"resume", "resume", model.Capabilities{Backend: "linux", CgroupFreeze: true}, func(*protocol.Request) {}, nil},
		{"memory-high", "memory-high", model.Capabilities{Backend: "linux", MemoryHighControl: true}, func(req *protocol.Request) { req.MemoryHighBytes = 64 << 20 }, func(req *protocol.Request) { req.MemoryHighBytes++ }},
		{"cpu-quota", "cpu-quota", model.Capabilities{Backend: "linux", CPUQuotaControl: true}, func(req *protocol.Request) { req.CPUQuotaPercent = 175 }, func(req *protocol.Request) { req.CPUQuotaPercent++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := newControlEvidencePhysical()
			svc, db := newControlTestService(t, p)
			configureControlRun(t, svc, db, test.caps)
			req := protocol.Request{Version: model.ProtocolVersion, Op: test.op, RunID: "run_control_test", RequestID: "control-once", ExpectedGeneration: 2}
			test.set(&req)
			first := svc.Handle(context.Background(), req)
			if first.Error != nil || first.Run == nil || first.Run.Generation != 3 {
				t.Fatalf("first control response = %+v", first.Error)
			}
			second := svc.Handle(context.Background(), req)
			if second.Error != nil || second.Run == nil || second.Run.Generation != 3 {
				t.Fatalf("replayed control response = %+v", second.Error)
			}
			if test.change != nil {
				conflict := req
				test.change(&conflict)
				result := svc.Handle(context.Background(), conflict)
				if result.Error == nil || result.Error.Code != "request-conflict" {
					t.Fatalf("changed control value response = %+v, want request-conflict", result.Error)
				}
			}
			stale := req
			stale.RequestID = "stale-" + test.name
			if result := svc.Handle(context.Background(), stale); result.Error == nil || result.Error.Code != "stale-generation" {
				t.Fatalf("stale-generation response = %+v", result.Error)
			}
			if len(controlEvents(t, db, req.RunID)) != 1 {
				t.Fatalf("control journal events = %+v, want one", controlEvents(t, db, req.RunID))
			}
			p.controlMu.Lock()
			callCount := 0
			for _, count := range p.calls {
				callCount += count
			}
			p.controlMu.Unlock()
			if callCount != 1 {
				t.Fatalf("physical control calls = %d, want one", callCount)
			}
		})
	}
}

func TestUnsupportedControlDoesNotClaimRequestOrAdvanceGeneration(t *testing.T) {
	p := newControlEvidencePhysical()
	svc, db := newControlTestService(t, p)
	configureControlRun(t, svc, db, model.Capabilities{Backend: "linux"})
	req := protocol.Request{Version: model.ProtocolVersion, Op: "pause", RunID: "run_control_test", RequestID: "unsupported-pause", ExpectedGeneration: 2}
	response := svc.Handle(context.Background(), req)
	if response.Error == nil || response.Error.Code != "unsupported-capability" {
		t.Fatalf("unsupported pause response = %+v", response.Error)
	}
	run, err := db.Get(req.RunID)
	if err != nil || run.Generation != 2 {
		t.Fatalf("unsupported control Run generation=%d err=%v", run.Generation, err)
	}
	digest, _ := mutationDigest(controlIdentityFor(req))
	if _, found, err := db.FindControlRequest(req.RunID, req.RequestID, digest, time.Now().UTC()); err != nil || found {
		t.Fatalf("unsupported control request found=%t err=%v", found, err)
	}
	if len(controlEvents(t, db, req.RunID)) != 0 {
		t.Fatal("unsupported control wrote lifecycle evidence")
	}
}

func TestControlResponseLossRecoversFromGuardianEvidence(t *testing.T) {
	p := newControlEvidencePhysical()
	p.responseErr = true
	svc, db := newControlTestService(t, p)
	configureControlRun(t, svc, db, model.Capabilities{Backend: "linux", MemoryHighControl: true})
	req := protocol.Request{Version: model.ProtocolVersion, Op: "memory-high", RunID: "run_control_test", RequestID: "lost-response", ExpectedGeneration: 2, MemoryHighBytes: 96 << 20}
	response := svc.Handle(context.Background(), req)
	if response.Error != nil || response.Run == nil || response.Run.Generation != 3 {
		t.Fatalf("response-loss replay = %+v", response.Error)
	}
	events := controlEvents(t, db, req.RunID)
	if len(events) != 1 || events[0].Kind != model.EventControlRecovered || events[0].Payload.Control == nil || events[0].Payload.Control.OperationID != req.RequestID {
		t.Fatalf("recovered control events = %+v", events)
	}
	response = svc.Handle(context.Background(), req)
	if response.Error != nil {
		t.Fatalf("idempotent replay = %+v", response.Error)
	}
	if len(controlEvents(t, db, req.RunID)) != 1 {
		t.Fatal("retry duplicated recovered control evidence")
	}
	if p.calls["control-memory-high"] != 1 {
		t.Fatalf("physical memory control calls = %d", p.calls["control-memory-high"])
	}
}

type controlLookupExecutor struct {
	controlTestExecutor
	operation guardian.ControlOperation
	found     bool
	gap       guardian.ControlEvidenceGap
	err       error
}

func (e controlLookupExecutor) LookupControl(string, string) (guardian.ControlOperation, bool, guardian.ControlEvidenceGap, error) {
	return e.operation, e.found, e.gap, e.err
}

func TestPendingControlReplayUsesExactGuardianEvidence(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	caps := model.Capabilities{Backend: "linux", CPUQuotaControl: true}
	run := model.Run{ID: "run_pending_control", State: model.Running, Generation: 1, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}, EffectiveCapabilities: &caps}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	req := protocol.Request{Version: model.ProtocolVersion, Op: "cpu-quota", RunID: run.ID, RequestID: "pending-cpu", ExpectedGeneration: 1, CPUQuotaPercent: 125}
	digest, err := mutationDigest(controlIdentityFor(req))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginControlRequest(run.ID, req.RequestID, digest, req.ExpectedGeneration, now); err != nil {
		t.Fatal(err)
	}
	operationDigest, _ := guardianControlDigestFor(req)
	evidence := model.ControlEventPayload{OperationID: req.RequestID, Control: "cpu.max", Action: "set", Value: int64Pointer(125), Unlimited: boolPointer(false), Outcome: "applied"}
	executor := controlLookupExecutor{operation: guardian.ControlOperation{RequestID: req.RequestID, Digest: operationDigest, Status: "completed", Evidence: evidence}, found: true}
	svc := newService(root, db, executor, defaultConfig())
	t.Cleanup(func() { _ = svc.Close() })
	response := svc.Handle(context.Background(), req)
	if response.Error != nil {
		t.Fatalf("pending retry response = %+v", response.Error)
	}
	stored, found, err := db.FindControlRequest(run.ID, req.RequestID, digest, time.Now().UTC())
	if err != nil || !found || stored.Status != store.ControlRequestSucceeded {
		t.Fatalf("stored retry disposition=%+v found=%t err=%v", stored, found, err)
	}
	events := controlEvents(t, db, run.ID)
	if len(events) != 1 || events[0].Kind != model.EventControlRecovered || events[0].Payload.Control == nil || events[0].Payload.Control.OperationID != req.RequestID {
		t.Fatalf("recovered journal evidence=%+v", events)
	}
}

func TestExpiredGuardianControlEvidenceKeepsPendingMutationUncertain(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	caps := model.Capabilities{Backend: "linux", CgroupFreeze: true}
	run := model.Run{ID: "run_expired_control", State: model.Running, Generation: 1, CreatedAt: time.Now().UTC(), Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}, EffectiveCapabilities: &caps}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	run, err = db.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	req := protocol.Request{Version: model.ProtocolVersion, Op: "pause", RunID: run.ID, RequestID: "expired-pause", ExpectedGeneration: 1}
	digest, err := mutationDigest(controlIdentityFor(req))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginControlRequest(run.ID, req.RequestID, digest, req.ExpectedGeneration, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	executor := controlLookupExecutor{gap: guardian.ControlEvidenceGap{ExpiredCount: 1}}
	svc := newService(root, db, executor, defaultConfig())
	t.Cleanup(func() { _ = svc.Close() })
	response := svc.Handle(context.Background(), req)
	if response.Error == nil || response.Error.Code != "control-uncertain" {
		t.Fatalf("expired evidence response = %+v", response.Error)
	}
	stored, found, err := db.FindControlRequest(run.ID, req.RequestID, digest, time.Now().UTC())
	if err != nil || !found || stored.Status != store.ControlRequestPending {
		t.Fatalf("expired evidence disposition=%+v found=%t err=%v", stored, found, err)
	}
	events := controlEvents(t, db, run.ID)
	if len(events) != 1 || events[0].Kind != model.EventControlRecovered || events[0].Payload.Control == nil || events[0].Payload.Control.Control != "guardian-control-evidence" || events[0].Payload.Control.Outcome != "expired" {
		t.Fatalf("expired Guardian evidence gap = %+v", events)
	}
}

type guardianRecoveryExecutor struct {
	controlTestExecutor
	result backend.TerminationResult
	err    error
	calls  int
	owner  model.Ownership
}

func (e *guardianRecoveryExecutor) RecoverGuardianLoss(owner model.Ownership, _ time.Duration) (backend.TerminationResult, error) {
	e.calls++
	e.owner = owner
	return e.result, e.err
}

func TestActiveGuardianLossRecordsRecoveryAndRemainsUncertain(t *testing.T) {
	p := newControlEvidencePhysical()
	executor := &guardianRecoveryExecutor{result: backend.TerminationResult{Requested: true, TreeEmpty: true, Outcome: "guardian-loss-recovery"}}
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	run := model.Run{ID: "run_control_test", State: model.Running, Generation: 1, CreatedAt: time.Now().UTC(), Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}}
	owner := model.Ownership{Backend: "linux", PID: 43210, StartTime: 99, ProcessGroup: 43210, Token: "persisted-ownership"}
	run.Ownership = &owner
	p.snapshot = guardian.Snapshot{RunID: run.ID}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	run, err = db.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	svc := newService(root, db, executor, defaultConfig())
	a := &active{run: run, spec: run.Spec, process: p, done: make(chan struct{})}
	svc.active[run.ID] = a
	if !svc.recoverActiveGuardianLoss(a) {
		t.Fatal("active Guardian loss was not handled")
	}
	if executor.calls != 1 || executor.owner != owner {
		t.Fatalf("recovery calls=%d owner=%+v", executor.calls, executor.owner)
	}
	stored, err := db.Get(run.ID)
	if err != nil || stored.State != model.Uncertain {
		t.Fatalf("Guardian loss state=%s err=%v", stored.State, err)
	}
	events, _, _, err := db.Events(run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var recovered, uncertain bool
	for _, event := range events {
		if event.Kind == model.EventControlRecovered && event.Payload.Control != nil {
			recovered = event.Payload.Control.Control == "guardian-loss" && event.Payload.Control.Outcome == "tree-terminated"
		}
		if event.Kind == model.EventRunUncertain && event.Payload.Run != nil {
			uncertain = event.Payload.Run.Reason == "guardian lost; process tree is empty but terminal receipt is unavailable"
		}
	}
	if !recovered || !uncertain {
		t.Fatalf("recovery event=%t uncertain event=%t; events=%+v", recovered, uncertain, events)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
}

type waitReceiptTerminateFailure struct {
	*controlEvidencePhysical
	exit exitResult
}

func (p *waitReceiptTerminateFailure) Wait() (exitResult, error) { return p.exit, nil }
func (*waitReceiptTerminateFailure) Terminate(time.Duration) (terminationResult, error) {
	return terminationResult{}, errors.New("Guardian became unavailable after its terminal receipt")
}

func TestMonitorUsesLinuxRecoveryAfterTerminalWaitButFailedCleanupRPC(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	caps := model.Capabilities{Backend: "linux", CgroupFreeze: true}
	run := model.Run{ID: "run_terminal_wait_recovery", State: model.Running, Generation: 1, CreatedAt: now, StartedAt: &now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}, EffectiveCapabilities: &caps}
	owner := model.Ownership{Backend: "linux", PID: 56789, StartTime: 1234, ProcessGroup: 56789, Token: "persisted-owner"}
	run.Ownership = &owner
	run, _, err = db.Create(run, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &model.Receipt{Version: model.ProtocolVersion, RunID: run.ID, Outcome: "exited", FinishedAt: now, Resources: model.Resources{}, Output: model.Output{HistoryComplete: true}, Cleanup: "complete", EffectiveCapabilities: &caps, Capabilities: caps}
	physical := &waitReceiptTerminateFailure{controlEvidencePhysical: newControlEvidencePhysical(), exit: exitResult{outcome: "exited", startedAt: now, finishedAt: now, receipt: receipt}}
	physical.snapshot = guardian.Snapshot{RunID: run.ID}
	executor := &guardianRecoveryExecutor{result: backend.TerminationResult{Requested: true, TreeEmpty: true, Forced: true, Outcome: "guardian-loss-recovery"}}
	svc := newService(root, db, executor, defaultConfig())
	t.Cleanup(func() { _ = svc.Close() })
	a := &active{run: run, spec: run.Spec, process: physical, done: make(chan struct{})}
	svc.active[run.ID] = a
	svc.monitor(a)
	stored, err := db.Get(run.ID)
	if err != nil || stored.State != model.Terminal || stored.Receipt == nil || stored.Receipt.Cleanup != "complete" || stored.Receipt.Outcome != "exited" {
		t.Fatalf("terminal recovery Run=%+v active=%+v err=%v", stored, a.run, err)
	}
	if executor.calls != 1 || executor.owner != owner {
		t.Fatalf("independent recovery calls=%d owner=%+v", executor.calls, executor.owner)
	}
	events, _, _, err := db.Events(run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var recoverySeq, terminalSeq uint64
	for _, event := range events {
		if event.Kind == model.EventControlRecovered && event.Payload.Control != nil && event.Payload.Control.Control == "guardian-loss" {
			recoverySeq = event.Seq
		}
		if event.Kind == model.EventRunTerminal {
			terminalSeq = event.Seq
		}
	}
	if recoverySeq == 0 || terminalSeq <= recoverySeq {
		t.Fatalf("recovery sequence=%d terminal sequence=%d events=%+v", recoverySeq, terminalSeq, events)
	}
}

func TestMonitorPreservesGuardianReceiptWhenIndependentCleanupIsUnproven(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	caps := model.Capabilities{Backend: "linux", CgroupFreeze: true}
	run := model.Run{ID: "run_receipt_cleanup_uncertain", State: model.Running, Generation: 1, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}, EffectiveCapabilities: &caps}
	owner := model.Ownership{Backend: "linux", PID: 56790, StartTime: 1236, ProcessGroup: 56790, Token: "persisted-owner"}
	run.Ownership = &owner
	run, _, err = db.Create(run, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &model.Receipt{Version: model.ProtocolVersion, RunID: run.ID, Outcome: "exited", FinishedAt: now, Resources: model.Resources{}, Output: run.Output, Cleanup: "complete", EffectiveCapabilities: &caps, Capabilities: caps}
	physical := &waitReceiptTerminateFailure{controlEvidencePhysical: newControlEvidencePhysical(), exit: exitResult{outcome: "exited", startedAt: now, finishedAt: now, receipt: receipt}}
	physical.snapshot = guardian.Snapshot{RunID: run.ID}
	executor := &guardianRecoveryExecutor{err: errors.New("native ownership proof unavailable")}
	svc := newService(root, db, executor, defaultConfig())
	t.Cleanup(func() { _ = svc.Close() })
	a := &active{run: run, spec: run.Spec, process: physical, done: make(chan struct{})}
	svc.active[run.ID] = a
	svc.monitor(a)
	stored, err := db.Get(run.ID)
	if err != nil || stored.State != model.Uncertain || stored.Receipt == nil || stored.Receipt.Outcome != "exited" || stored.Receipt.Cleanup != "complete" {
		t.Fatalf("uncertain Run lost terminal receipt evidence: run=%+v err=%v", stored, err)
	}
	if executor.calls != 1 || executor.owner != owner {
		t.Fatalf("independent recovery calls=%d owner=%+v", executor.calls, executor.owner)
	}
	events, _, _, err := db.Events(run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Kind == model.EventRunTerminal {
			t.Fatalf("unproven cleanup was reported terminal: %+v", events)
		}
	}
}

type waitErrorGuardianPhysical struct{ *controlEvidencePhysical }

func (*waitErrorGuardianPhysical) Wait() (exitResult, error) {
	return exitResult{}, errors.New("Guardian wait lost")
}
func (*waitErrorGuardianPhysical) Observe() (model.Resources, error) {
	return model.Resources{}, errors.New("Guardian observation lost")
}

func TestMonitorWaitErrorUsesGuardianLossRecovery(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	run := model.Run{ID: "run_monitor_wait_loss", State: model.Running, Generation: 1, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}}
	owner := model.Ownership{Backend: "linux", PID: 56890, StartTime: 1235, ProcessGroup: 56890, Token: "persisted-owner"}
	run.Ownership = &owner
	run, _, err = db.Create(run, nil)
	if err != nil {
		t.Fatal(err)
	}
	physical := &waitErrorGuardianPhysical{controlEvidencePhysical: newControlEvidencePhysical()}
	physical.snapshot = guardian.Snapshot{RunID: run.ID}
	executor := &guardianRecoveryExecutor{result: backend.TerminationResult{Requested: true, TreeEmpty: true}}
	svc := newService(root, db, executor, defaultConfig())
	t.Cleanup(func() { _ = svc.Close() })
	a := &active{run: run, spec: run.Spec, process: physical, done: make(chan struct{})}
	svc.active[run.ID] = a
	svc.monitor(a)
	stored, err := db.Get(run.ID)
	if err != nil || stored.State != model.Uncertain {
		t.Fatalf("Run state after lost Wait=%s err=%v", stored.State, err)
	}
	if executor.calls != 1 || executor.owner != owner {
		t.Fatalf("Wait loss recovery calls=%d owner=%+v", executor.calls, executor.owner)
	}
}

type testNativeRecoveryBackend struct {
	result backend.TerminationResult
	err    error
	calls  int
	owner  model.Ownership
}

func (*testNativeRecoveryBackend) Capabilities() model.Capabilities {
	return model.Capabilities{Backend: "linux"}
}
func (*testNativeRecoveryBackend) Start(model.RunSpec, io.Writer, io.Writer) (backend.Process, error) {
	return nil, errors.New("unexpected native Start")
}
func (*testNativeRecoveryBackend) Reconcile(model.Ownership) (backend.ReconcileResult, error) {
	return backend.ReconcileResult{}, errors.New("unexpected native Reconcile")
}
func (b *testNativeRecoveryBackend) RecoverGuardianLoss(owner model.Ownership, _ time.Duration) (backend.TerminationResult, error) {
	b.calls++
	b.owner = owner
	return b.result, b.err
}

func TestStartupGuardianLossRecoveryPersistsEvidenceBeforeUncertain(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	owner := model.Ownership{Backend: "linux", PID: 98765, StartTime: 444, ProcessGroup: 98765, Token: "persisted-ownership"}
	run := model.Run{ID: "run_startup_guardian_loss", State: model.Running, Generation: 1, CreatedAt: time.Now().UTC(), Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}, Ownership: &owner}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	native := &testNativeRecoveryBackend{result: backend.TerminationResult{Requested: true, TreeEmpty: true}}
	config := defaultConfig()
	executor := &guardedExecutor{root: root, native: native, config: config}
	svc := newService(root, db, executor, config)
	t.Cleanup(func() { _ = svc.Close() })
	if err := svc.reconcile(); err != nil {
		t.Fatal(err)
	}
	if native.calls != 1 || native.owner != owner {
		t.Fatalf("native recovery calls=%d owner=%+v", native.calls, native.owner)
	}
	stored, err := db.Get(run.ID)
	if err != nil || stored.State != model.Uncertain {
		t.Fatalf("startup Guardian loss state=%s err=%v", stored.State, err)
	}
	events, _, _, err := db.Events(run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var recoveredSeq, uncertainSeq uint64
	for _, event := range events {
		if event.Kind == model.EventControlRecovered && event.Payload.Control != nil && event.Payload.Control.Control == "guardian-loss" {
			recoveredSeq = event.Seq
		}
		if event.Kind == model.EventRunUncertain && event.Payload.Run != nil {
			uncertainSeq = event.Seq
		}
	}
	if recoveredSeq == 0 || uncertainSeq <= recoveredSeq {
		t.Fatalf("Guardian recovery evidence sequence=%d uncertain sequence=%d events=%+v", recoveredSeq, uncertainSeq, events)
	}
}

type reconciledControlExecutor struct {
	controlTestExecutor
	result reconcileResult
	err    error
}

func (e reconciledControlExecutor) Reconcile(model.Run, io.Writer, io.Writer) (reconcileResult, error) {
	return e.result, e.err
}

func TestReconcileImportsGuardianControlBeforeTerminalAndResolvesClaim(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	caps := model.Capabilities{Backend: "linux", CgroupFreeze: true}
	run := model.Run{ID: "run_reconcile_control", State: model.Running, Generation: 1, CreatedAt: now, StartedAt: &now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}, EffectiveCapabilities: &caps}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	request := protocol.Request{Op: "pause", RunID: run.ID, RequestID: "restart-pause", ExpectedGeneration: 1}
	digest, err := mutationDigest(controlIdentityFor(request))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginControlRequest(run.ID, request.RequestID, digest, request.ExpectedGeneration, now); err != nil {
		t.Fatal(err)
	}
	guardianDigest, _ := guardianControlDigestFor(request)
	operation := guardian.ControlOperation{
		RequestID: request.RequestID, Digest: guardianDigest, Status: "completed",
		Evidence:  model.ControlEventPayload{OperationID: request.RequestID, Control: "cgroup.freeze", Action: "pause", Value: int64Pointer(1), Outcome: "applied"},
		CreatedAt: now, UpdatedAt: now,
	}
	executor := reconciledControlExecutor{result: reconcileResult{terminal: true, exit: exitResult{outcome: "exited"}, controlEvidence: []guardian.ControlOperation{operation}}}
	svc := newService(root, db, executor, defaultConfig())
	t.Cleanup(func() { _ = svc.Close() })
	if err := svc.reconcile(); err != nil {
		t.Fatal(err)
	}
	stored, err := db.Get(run.ID)
	if err != nil || stored.State != model.Terminal {
		t.Fatalf("reconciled Run state=%s err=%v", stored.State, err)
	}
	controlRequest, found, err := db.FindControlRequest(run.ID, request.RequestID, digest, time.Now().UTC())
	if err != nil || !found || controlRequest.Status != store.ControlRequestSucceeded {
		t.Fatalf("pending control disposition=%+v found=%t err=%v", controlRequest, found, err)
	}
	events, _, _, err := db.Events(run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var recoveredSeq, terminalSeq uint64
	for _, event := range events {
		if event.Kind == model.EventControlRecovered && event.Payload.Control != nil && event.Payload.Control.OperationID == request.RequestID {
			recoveredSeq = event.Seq
		}
		if event.Kind == model.EventRunTerminal {
			terminalSeq = event.Seq
		}
	}
	if recoveredSeq == 0 || terminalSeq <= recoveredSeq {
		t.Fatalf("Guardian control event sequence=%d terminal sequence=%d events=%+v", recoveredSeq, terminalSeq, events)
	}
}

func TestReconcileGuardianUncertainControlNeverBecomesSuccess(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	caps := model.Capabilities{Backend: "linux", CgroupFreeze: true}
	run := model.Run{ID: "run_uncertain_reconcile_control", State: model.Running, Generation: 1, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}, EffectiveCapabilities: &caps}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	request := protocol.Request{Op: "pause", RunID: run.ID, RequestID: "uncertain-pause", ExpectedGeneration: 1}
	digest, err := mutationDigest(controlIdentityFor(request))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginControlRequest(run.ID, request.RequestID, digest, request.ExpectedGeneration, now); err != nil {
		t.Fatal(err)
	}
	guardianDigest, _ := guardianControlDigestFor(request)
	operation := guardian.ControlOperation{
		RequestID: request.RequestID, Digest: guardianDigest, Status: "pending",
		Evidence:  model.ControlEventPayload{OperationID: request.RequestID, Control: "cgroup.freeze", Action: "pause", Value: int64Pointer(1), Outcome: "uncertain", Reason: "control-effect-pending"},
		CreatedAt: now, UpdatedAt: now,
	}
	executor := reconciledControlExecutor{result: reconcileResult{controlUncertain: []guardian.ControlOperation{operation}}, err: errors.New("Guardian has no provable live Run state")}
	svc := newService(root, db, executor, defaultConfig())
	t.Cleanup(func() { _ = svc.Close() })
	if err := svc.reconcile(); err != nil {
		t.Fatal(err)
	}
	stored, err := db.Get(run.ID)
	if err != nil || stored.State != model.Uncertain {
		t.Fatalf("reconciled Run state=%s err=%v", stored.State, err)
	}
	controlRequest, found, err := db.FindControlRequest(run.ID, request.RequestID, digest, time.Now().UTC())
	if err != nil || !found || controlRequest.Status != store.ControlRequestUncertain {
		t.Fatalf("uncertain control disposition=%+v found=%t err=%v", controlRequest, found, err)
	}
	events := controlEvents(t, db, run.ID)
	if len(events) != 1 || events[0].Kind != model.EventControlRecovered || events[0].Payload.Control == nil || events[0].Payload.Control.Outcome != "uncertain" {
		t.Fatalf("uncertain Guardian evidence = %+v", events)
	}
}

func TestReconcileControlEvidenceDistinguishesAbsentAndCompactedState(t *testing.T) {
	if operations, uncertain, gap := retainedGuardianControlEvidence("run-no-controls", nil); len(operations) != 0 || len(uncertain) != 0 || gap != nil {
		t.Fatalf("absent control state = operations %d uncertain %d gap %+v", len(operations), len(uncertain), gap)
	}
	now := time.Now().UTC()
	request := protocol.Request{Op: "pause", RequestID: "valid-pause"}
	digest, _ := guardianControlDigestFor(request)
	valid := guardian.ControlOperation{RequestID: request.RequestID, Digest: digest, Status: "completed", Evidence: model.ControlEventPayload{OperationID: request.RequestID, Control: "cgroup.freeze", Action: "pause", Value: int64Pointer(1), Outcome: "applied"}, CreatedAt: now, UpdatedAt: now}
	invalid := valid
	invalid.RequestID = "invalid-digest"
	operations, uncertain, gap := retainedGuardianControlEvidence("run-control-gap", &guardian.ControlState{Operations: []guardian.ControlOperation{valid, invalid}, EvidenceGap: guardian.ControlEvidenceGap{ExpiredCount: 2}})
	if len(operations) != 1 || operations[0].RequestID != valid.RequestID || len(uncertain) != 0 || gap == nil || gap.Outcome != "expired" || gap.Value == nil || *gap.Value != 3 {
		t.Fatalf("compacted control state = operations %+v uncertain %+v gap %+v", operations, uncertain, gap)
	}
}

func TestControlEvidenceDedupFailsClosedAcrossJournalGap(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{EventRetentionCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	run := model.Run{ID: "run_control_journal_gap", State: model.Running, Generation: 1, CreatedAt: time.Now().UTC(), Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := db.AppendEvent(run.ID, model.Event{Kind: model.EventResourceSample}); err != nil {
			t.Fatal(err)
		}
	}
	svc := newService(root, db, controlTestExecutor{}, defaultConfig())
	t.Cleanup(func() { _ = svc.Close() })
	evidence := model.ControlEventPayload{OperationID: "unseen-control", Control: "cgroup.freeze", Action: "pause", Value: int64Pointer(1), Outcome: "applied"}
	if err := svc.persistRecoveredControlEvent(run.ID, evidence); !errors.Is(err, errControlJournalGap) {
		t.Fatalf("control deduplication error = %v", err)
	}
	events, _, gap, err := db.Events(run.ID, 0, 100)
	if err != nil || !gap {
		t.Fatalf("journal gap=%t err=%v", gap, err)
	}
	for _, event := range events {
		if event.Payload != nil && event.Payload.Control != nil && event.Payload.Control.OperationID == evidence.OperationID {
			t.Fatal("unseen operation was appended despite an unproven journal history")
		}
	}
}

func TestPendingControlReplayReadsProcessUnderRunLock(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	caps := model.Capabilities{Backend: "linux", CgroupFreeze: true}
	run := model.Run{ID: "run_replay_race", State: model.Running, Generation: 1, CreatedAt: time.Now().UTC(), Spec: model.RunSpec{Argv: []string{"/bin/sleep", "2"}, Cwd: t.TempDir()}, EffectiveCapabilities: &caps}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	req := protocol.Request{Version: model.ProtocolVersion, Op: "pause", RunID: run.ID, RequestID: "race-pause", ExpectedGeneration: 1}
	digest, err := mutationDigest(controlIdentityFor(req))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginControlRequest(run.ID, req.RequestID, digest, req.ExpectedGeneration, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	guardianDigest, _ := guardianControlDigestFor(req)
	evidence := model.ControlEventPayload{OperationID: req.RequestID, Control: "cgroup.freeze", Action: "pause", Value: int64Pointer(1), Outcome: "applied"}
	operation := guardian.ControlOperation{RequestID: req.RequestID, Digest: guardianDigest, Status: "completed", Evidence: evidence}
	first, second := newControlEvidencePhysical(), newControlEvidencePhysical()
	first.operations[req.RequestID] = operation
	second.operations[req.RequestID] = operation
	executor := controlLookupExecutor{operation: operation, found: true}
	svc := newService(root, db, executor, defaultConfig())
	a := &active{run: run, spec: run.Spec, process: first, done: make(chan struct{})}
	svc.active[run.ID] = a
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 100; i++ {
			a.mu.Lock()
			if i%2 == 0 {
				a.process = first
			} else {
				a.process = second
			}
			a.mu.Unlock()
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 20; i++ {
			_ = svc.Handle(context.Background(), req)
		}
	}()
	workers.Wait()
	stored, found, err := db.FindControlRequest(run.ID, req.RequestID, digest, time.Now().UTC())
	if err != nil || !found || stored.Status != store.ControlRequestSucceeded {
		t.Fatalf("replay disposition=%+v found=%t err=%v", stored, found, err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
}
