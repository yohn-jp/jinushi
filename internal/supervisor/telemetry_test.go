package supervisor

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

type telemetryFixtureExecutor struct{ physical *telemetryFixturePhysical }

func (*telemetryFixtureExecutor) Capabilities() model.Capabilities {
	return model.Capabilities{Backend: "fixture", MemoryTelemetry: true, CPUTelemetry: true, ProcessTelemetry: true, TaskTelemetry: true}
}

func (e *telemetryFixtureExecutor) Start(model.Run, model.RunSpec, io.Writer, io.Writer) (physical, error) {
	return e.physical, nil
}

func (*telemetryFixtureExecutor) Reconcile(model.Run, io.Writer, io.Writer) (reconcileResult, error) {
	return reconcileResult{}, errors.New("not used by telemetry fixture")
}

type telemetryFixturePhysical struct {
	sample model.TelemetrySample
	err    error
}

func (*telemetryFixturePhysical) Ownership() model.Ownership {
	return model.Ownership{Backend: "fixture"}
}
func (*telemetryFixturePhysical) Wait() (exitResult, error) { return exitResult{}, nil }
func (p *telemetryFixturePhysical) Observe() (model.Resources, error) {
	return p.sample.Resources, p.err
}
func (p *telemetryFixturePhysical) ObserveTelemetry() (model.TelemetrySample, error) {
	return p.sample, p.err
}
func (*telemetryFixturePhysical) Signal(string) error { return nil }
func (*telemetryFixturePhysical) Terminate(time.Duration) (terminationResult, error) {
	return terminationResult{complete: true}, nil
}
func (*telemetryFixturePhysical) WriteInput([]byte) error     { return nil }
func (*telemetryFixturePhysical) Resize(uint16, uint16) error { return nil }
func (*telemetryFixturePhysical) CloseInput() error           { return nil }

func measuredTelemetryFixture(runID string, at time.Time) model.TelemetrySample {
	caps := (&telemetryFixtureExecutor{}).Capabilities()
	resources := initialResources(caps, 250)
	resources.MemoryBytes = model.Metric{Status: string(model.EvidenceMeasured), Value: 4096}
	resources.CPUTimeNs = model.Metric{Status: string(model.EvidenceMeasured), Value: 9000}
	resources.ProcessCount = model.Metric{Status: string(model.EvidenceMeasured), Value: 1}
	resources.TaskCount = model.Metric{Status: string(model.EvidenceMeasured), Value: 1}
	sample := telemetrySampleFromResources(runID, resources, caps, at)
	sample.ProcessEvidenceStatus = model.EvidenceMeasured
	sample.ProcessEvidenceComplete = true
	sample.Processes = []model.ProcessEvidence{{
		PID: 41, StartTimeTicks: 123, ParentPID: 1, Membership: model.ProcessMembershipOwned,
		Comm: "fixture", State: "R", CPUTimeNs: model.Metric{Status: string(model.EvidenceMeasured), Value: 9000},
		RSSBytes: model.Metric{Status: string(model.EvidenceMeasured), Value: 4096},
	}}
	sample.Activity.InputBytes = model.Metric{Status: string(model.EvidenceMeasured), Value: 0}
	sample.Activity.InputWrites = model.Metric{Status: string(model.EvidenceMeasured), Value: 0}
	return sample
}

func newTelemetryFixture(t *testing.T, runID string, sample model.TelemetrySample) (*Service, *active) {
	t.Helper()
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	executor := &telemetryFixtureExecutor{physical: &telemetryFixturePhysical{sample: sample}}
	svc := newService(root, db, executor, defaultConfig())
	now := time.Now().UTC()
	run := model.Run{
		ID: runID, State: model.Running, Generation: 1, CreatedAt: now.Add(-time.Minute),
		Spec:      model.RunSpec{Argv: []string{"/bin/sleep", "1"}, Cwd: root},
		Resources: initialResources(executor.Capabilities(), 250),
	}
	run, _, err = db.Create(run, &model.Event{Kind: model.EventRunAccepted, ObservedAt: now.Add(-time.Minute)})
	if err != nil {
		_ = svc.Close()
		t.Fatal(err)
	}
	a := &active{run: run, spec: run.Spec, process: executor.physical, done: make(chan struct{})}
	t.Cleanup(func() {
		if err := svc.Close(); err != nil && !errors.Is(err, errSupervisorClosed) {
			t.Errorf("close telemetry fixture service: %v", err)
		}
	})
	return svc, a
}

func TestSupervisorPersistsHighRateTelemetryOutsideLifecycleJournal(t *testing.T) {
	runID := "run_telemetry_fixture"
	at := time.Now().UTC().Add(-time.Second)
	physical := &telemetryFixturePhysical{sample: measuredTelemetryFixture(runID, at)}
	executor := &telemetryFixtureExecutor{physical: physical}
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	svc := newService(root, db, executor, defaultConfig())
	run := model.Run{
		ID: runID, State: model.Running, Generation: 1, CreatedAt: at.Add(-time.Minute),
		Spec:      model.RunSpec{Argv: []string{"/bin/sleep", "1"}, Cwd: root},
		Resources: initialResources(executor.Capabilities(), 250),
	}
	run, _, err = db.Create(run, &model.Event{Kind: model.EventRunAccepted, ObservedAt: run.CreatedAt})
	if err != nil {
		_ = svc.Close()
		t.Fatal(err)
	}
	a := &active{run: run, spec: run.Spec, process: physical, done: make(chan struct{})}
	svc.sample(a)

	stored, err := db.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastResourceSampleAt == nil || !stored.LastResourceSampleAt.Equal(at) || stored.Resources.CPUTimeNs.Value != 9000 {
		t.Fatalf("Run summary did not import telemetry sample: %+v", stored)
	}
	telemetry, err := db.QueryTelemetry(model.TelemetryQuery{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if len(telemetry.Samples) != 1 || telemetry.Samples[0].ProcessEvidenceStatus != model.EvidenceMeasured || len(telemetry.Samples[0].Processes) != 1 {
		t.Fatalf("physical evidence was not retained separately: %+v", telemetry)
	}
	events, _, _, err := db.Events(runID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Kind != model.EventRunAccepted {
		t.Fatalf("high-rate samples polluted lifecycle journal: %+v", events)
	}
	response := svc.Handle(context.Background(), protocol.Request{
		Version: model.ProtocolVersion, Op: "telemetry", RunID: runID,
		TelemetryQuery: &model.TelemetryQuery{RunID: runID},
	})
	if response.Error != nil || response.Telemetry == nil || len(response.Telemetry.Samples) != 1 {
		if response.Error != nil {
			t.Fatalf("telemetry query response error = %s: %s", response.Error.Code, response.Error.Message)
		}
		t.Fatalf("telemetry query response = %+v", response)
	}
	missing := svc.Handle(context.Background(), protocol.Request{
		Version: model.ProtocolVersion, Op: "telemetry",
		TelemetryQuery: &model.TelemetryQuery{RunID: "run_missing"},
	})
	if missing.Error == nil || missing.Error.Code != "run-not-found" {
		t.Fatalf("missing Run telemetry query error = %+v", missing.Error)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorObservationFailurePersistsTelemetryGap(t *testing.T) {
	runID := "run_telemetry_gap"
	at := time.Now().UTC().Add(-time.Second)
	svc, a := newTelemetryFixture(t, runID, measuredTelemetryFixture(runID, at))
	a.process.(*telemetryFixturePhysical).err = errors.New("fixture observation unavailable")
	svc.sample(a)

	stored, err := svc.store.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.ResourceGap {
		t.Fatal("Run did not retain incomplete telemetry evidence")
	}
	telemetry, err := svc.store.QueryTelemetry(model.TelemetryQuery{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if len(telemetry.Gaps) != 1 || telemetry.Gaps[0].Reason != "sample-observation-unavailable" || telemetry.Gaps[0].DroppedPoints != 1 {
		t.Fatalf("observation gap was not machine-readable: %+v", telemetry.Gaps)
	}
	events, _, _, err := svc.store.Events(runID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Kind != model.EventRunAccepted {
		t.Fatalf("telemetry observation gap polluted lifecycle journal: %+v", events)
	}
}

func TestRecoveredTelemetryPersistsOneLifecycleGapAndSeparateSample(t *testing.T) {
	runID := "run_recovered_telemetry"
	at := time.Now().UTC()
	sample := measuredTelemetryFixture(runID, at)
	svc, a := newTelemetryFixture(t, runID, sample)
	previous := at.Add(-5 * time.Second)
	a.run.LastResourceSampleAt = &previous
	if _, err := svc.store.Update(a.run, nil); err != nil {
		t.Fatal(err)
	}
	events := svc.recoveredResourceEvents(&a.run, reconcileResult{telemetry: &sample})
	if len(events) != 1 || events[0].Kind != model.EventResourceGap {
		t.Fatalf("recovery lifecycle events = %+v; want one resource.gap", events)
	}
	if _, err := svc.store.UpdateWithEvents(a.run, events); err != nil {
		t.Fatal(err)
	}
	telemetry, err := svc.store.QueryTelemetry(model.TelemetryQuery{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if len(telemetry.Samples) != 1 || len(telemetry.Gaps) != 1 || telemetry.Gaps[0].Reason != "supervisor-unavailable" {
		t.Fatalf("recovered telemetry does not retain sample and gap: %+v", telemetry)
	}
	journal, _, _, err := svc.store.Events(runID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(journal) != 2 || journal[1].Kind != model.EventResourceGap {
		t.Fatalf("recovery journal contains unexpected events: %+v", journal)
	}
}
