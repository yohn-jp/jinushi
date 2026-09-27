package supervisor

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

type hostStatusExecutor struct {
	status  model.HostEnvelopeStatus
	failure *protocol.Failure
}

func (*hostStatusExecutor) Capabilities() model.Capabilities {
	return model.Capabilities{Backend: "test"}
}
func (*hostStatusExecutor) Start(model.Run, model.RunSpec, io.Writer, io.Writer) (physical, error) {
	panic("host admission failure must reject before Start")
}
func (*hostStatusExecutor) Reconcile(model.Run, io.Writer, io.Writer) (reconcileResult, error) {
	panic("status test must not reconcile")
}
func (e *hostStatusExecutor) HostEnvelopeStatus() model.HostEnvelopeStatus { return e.status }
func (e *hostStatusExecutor) ValidateHostAdmission() *protocol.Failure     { return e.failure }

type hostNativeBackend struct {
	status       model.HostEnvelopeStatus
	admissionErr error
}

func (b *hostNativeBackend) Capabilities() model.Capabilities {
	return model.Capabilities{Backend: "linux"}
}
func (*hostNativeBackend) Start(model.RunSpec, io.Writer, io.Writer) (backend.Process, error) {
	return nil, errors.New("not used")
}
func (*hostNativeBackend) Reconcile(model.Ownership) (backend.ReconcileResult, error) {
	return backend.ReconcileResult{}, errors.New("not used")
}
func (b *hostNativeBackend) HostEnvelopeStatus() model.HostEnvelopeStatus { return b.status }
func (b *hostNativeBackend) ValidateHostAdmission() error                 { return b.admissionErr }

func TestStatusAndCapabilitiesExposeHostEnvelope(t *testing.T) {
	db, err := store.Open(t.TempDir()+"/state.db", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	want := model.HostEnvelopeStatus{
		Status:      "enforced",
		Config:      model.HostEnvelopeConfig{MemoryBytes: 1 << 30, TaskCount: 1024, MaxActiveRuns: 4},
		ActiveRuns:  model.Metric{Status: "measured", Value: 2},
		MemoryBytes: model.Metric{Status: "measured", Value: 2048},
		TaskCount:   model.Metric{Status: "measured", Value: 8},
	}
	svc := newService(t.TempDir(), db, &hostStatusExecutor{status: want}, defaultConfig())

	for _, op := range []string{"status", "capabilities"} {
		response := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: op})
		if response.Error != nil || response.HostEnvelope == nil {
			t.Fatalf("%s response omitted host envelope: %+v", op, response)
		}
		if *response.HostEnvelope != want {
			t.Fatalf("%s host envelope = %+v, want %+v", op, *response.HostEnvelope, want)
		}
		if op == "capabilities" && (response.Capabilities == nil || response.Capabilities.Backend != "test") {
			t.Fatalf("capabilities response omitted backend capability: %+v", response)
		}
	}
}

func TestHostAdmissionFailureDoesNotDurablyAcceptRun(t *testing.T) {
	db, err := store.Open(t.TempDir()+"/state.db", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := newService(t.TempDir(), db, &hostStatusExecutor{failure: &protocol.Failure{Code: "host-envelope-admission", Message: "host task ceiling reached"}}, defaultConfig())
	spec := model.RunSpec{Argv: []string{"/bin/true"}, Cwd: t.TempDir()}
	response := svc.Handle(context.Background(), protocol.Request{
		Version: model.ProtocolVersion, Op: "run", SubmissionID: "host-full", Spec: &spec,
	})
	if response.Error == nil || response.Error.Code != "host-envelope-admission" {
		t.Fatalf("host admission response = %+v", response.Error)
	}
	runs, err := db.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("host admission failure durably accepted %d Runs", len(runs))
	}
}

func TestSupervisorActiveRunsReserveHostRunSlotsBeforeAcceptance(t *testing.T) {
	db, err := store.Open(t.TempDir()+"/state.db", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	config := defaultConfig()
	config.MaxActiveRuns = 1
	svc := newService(t.TempDir(), db, &hostStatusExecutor{}, config)
	svc.active["run-starting"] = &active{run: model.Run{ID: "run-starting", State: model.Starting}}
	spec := model.RunSpec{Argv: []string{"/bin/true"}, Cwd: t.TempDir()}
	response := svc.Handle(context.Background(), protocol.Request{
		Version: model.ProtocolVersion, Op: "run", SubmissionID: "host-run-slot", Spec: &spec,
	})
	if response.Error == nil || response.Error.Code != "host-envelope-admission" {
		t.Fatalf("active Run slot admission response = %+v", response.Error)
	}
	runs, err := db.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("active Run slot rejection durably accepted %d Runs", len(runs))
	}
}

func TestGuardedHostAdmissionDistinguishesUnsupportedFromCapacity(t *testing.T) {
	config := Config{HostMemoryBytes: 1 << 30}
	unsupported := &hostNativeBackend{
		status:       unavailableHostEnvelopeStatus("unsupported", "no delegated cgroup", configuredHostEnvelope(config)),
		admissionErr: errors.New("configured Linux host envelope is unavailable"),
	}
	guarded := &guardedExecutor{native: unsupported, config: config}
	if failure := guarded.ValidateHostAdmission(); failure == nil || failure.Code != "unsupported-capability" {
		t.Fatalf("unsupported envelope admission failure = %+v", failure)
	}

	full := &hostNativeBackend{
		status: model.HostEnvelopeStatus{
			Status: "enforced", Config: configuredHostEnvelope(config),
			Capabilities: model.HostEnvelopeCapabilities{WorkloadRoot: true, MemoryEnforcement: true},
		},
		admissionErr: errors.New("memory-bytes ceiling reached"),
	}
	guarded.native = full
	if failure := guarded.ValidateHostAdmission(); failure == nil || failure.Code != "host-envelope-admission" {
		t.Fatalf("full envelope admission failure = %+v", failure)
	}
}
