package guardian

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

type telemetryTestProcess struct {
	inputErr  error
	resizeErr error
	inputs    [][]byte
	resizes   int
	sample    model.TelemetrySample
}

func (p *telemetryTestProcess) Ownership() model.Ownership  { return model.Ownership{Backend: "test"} }
func (p *telemetryTestProcess) Wait() (backend.Exit, error) { return backend.Exit{}, nil }
func (p *telemetryTestProcess) Observe() (model.Resources, error) {
	return p.sample.Resources, nil
}
func (p *telemetryTestProcess) Signal(string) error { return nil }
func (p *telemetryTestProcess) Terminate(time.Duration) (backend.TerminationResult, error) {
	return backend.TerminationResult{TreeEmpty: true}, nil
}
func (p *telemetryTestProcess) WriteInput(data []byte) error {
	if p.inputErr != nil {
		return p.inputErr
	}
	p.inputs = append(p.inputs, append([]byte(nil), data...))
	return nil
}
func (p *telemetryTestProcess) Resize(uint16, uint16) error {
	if p.resizeErr != nil {
		return p.resizeErr
	}
	p.resizes++
	return nil
}
func (p *telemetryTestProcess) CloseInput() error { return nil }
func (p *telemetryTestProcess) Evidence() (model.TelemetrySample, error) {
	return p.sample, nil
}

func TestBoundTelemetrySampleKeepsExplicitTruncation(t *testing.T) {
	at := time.Now().UTC()
	sample := model.TelemetrySample{
		ObservedAt: at, ProcessEvidenceStatus: model.EvidenceMeasured,
		ProcessEvidenceComplete: true,
		ProcessChanges:          make([]model.ProcessEvidenceChange, maxGuardianTelemetryChanges+1),
	}
	for i := range sample.ProcessChanges {
		sample.ProcessChanges[i] = model.ProcessEvidenceChange{
			ObservedAt: at, Kind: model.ProcessStarted,
			Process:    model.ProcessIdentity{PID: i + 1, StartTimeTicks: uint64(i + 1)},
			Membership: model.ProcessMembershipOwned,
		}
	}
	activity := initialActivity(true)
	got := boundTelemetrySample(sample, "run_test", nil, activity, 250, at)
	if len(got.ProcessChanges) != maxGuardianTelemetryChanges {
		t.Fatalf("retained %d process changes, want %d", len(got.ProcessChanges), maxGuardianTelemetryChanges)
	}
	if got.ProcessEvidenceComplete || got.ProcessEvidenceReason != "process-changes-truncated" {
		t.Fatalf("truncation is not explicit: complete=%v reason=%q", got.ProcessEvidenceComplete, got.ProcessEvidenceReason)
	}
	if got.Activity.InputBytes.Status != string(model.EvidenceMeasured) || got.Activity.ResizeCount.Status != string(model.EvidenceMeasured) {
		t.Fatalf("interactive activity status was lost: %+v", got.Activity)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxGuardianTelemetryBytes {
		t.Fatalf("telemetry sample is %d bytes, max %d", len(encoded), maxGuardianTelemetryBytes)
	}
}

func TestGuardianObservePersistsBoundedProcessEvidence(t *testing.T) {
	dir := t.TempDir()
	at := time.Now().UTC()
	process := &telemetryTestProcess{sample: model.TelemetrySample{
		ObservedAt: at, Resources: model.Resources{
			MemoryBytes: model.Metric{Status: "measured", Value: 7},
		},
		ProcessEvidenceStatus:   model.EvidenceMeasured,
		ProcessEvidenceComplete: true,
	}}
	spool, err := openSpool(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	state := &runState{
		descriptor: launchConfig{Dir: dir, RunID: "run_telemetry", Spec: model.RunSpec{Interactive: true}, SampleIntervalMs: 250},
		spool:      spool, process: process,
		snapshot: Snapshot{
			Version: ProtocolVersion, RunID: "run_telemetry", State: model.Running,
			Resources: unavailableResourcesFor(250), Activity: initialActivity(true),
			EffectiveCapabilities: &model.Capabilities{ProcessTelemetry: true},
		},
	}
	observed, err := state.observe()
	if err != nil {
		t.Fatal(err)
	}
	if observed.TelemetrySample == nil || observed.TelemetrySample.RunID != "run_telemetry" || !observed.TelemetrySample.ObservedAt.Equal(at) {
		t.Fatalf("process evidence was not attached to the Guardian snapshot: %+v", observed.TelemetrySample)
	}
	if observed.TelemetrySample.Resources.MemoryBytes.Value != 7 || observed.TelemetrySample.Activity.InputBytes.Status != "measured" {
		t.Fatalf("sample fields were not preserved: %+v", observed.TelemetrySample)
	}
	reloaded, err := readSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.TelemetrySample == nil || reloaded.TelemetrySample.ProcessEvidenceStatus != model.EvidenceMeasured {
		t.Fatalf("process evidence was not durable: %+v", reloaded.TelemetrySample)
	}
}

func TestGuardianInputAndResizeActivityContainNoInputData(t *testing.T) {
	dir := t.TempDir()
	process := &telemetryTestProcess{}
	state := &runState{
		descriptor: launchConfig{Dir: dir, RunID: "run_activity", Spec: model.RunSpec{Interactive: true}},
		process:    process,
		snapshot:   Snapshot{Version: ProtocolVersion, RunID: "run_activity", State: model.Running, Activity: initialActivity(true)},
	}
	secret := []byte("private input content")
	if err := state.writeInput(secret); err != nil {
		t.Fatal(err)
	}
	if err := state.resize(24, 80); err != nil {
		t.Fatal(err)
	}
	if len(process.inputs) != 1 || string(process.inputs[0]) != string(secret) || process.resizes != 1 {
		t.Fatalf("physical operations were not called once: inputs=%d resizes=%d", len(process.inputs), process.resizes)
	}
	if state.snapshot.Activity.InputBytes.Status != "measured" || state.snapshot.Activity.InputBytes.Value != int64(len(secret)) || state.snapshot.Activity.InputWrites.Value != 1 {
		t.Fatalf("input activity is incorrect: %+v", state.snapshot.Activity)
	}
	if state.snapshot.Activity.LastInputAt == nil || state.snapshot.Activity.ResizeCount.Value != 1 || state.snapshot.Activity.LastResizeAt == nil {
		t.Fatalf("activity timestamps/counts are missing: %+v", state.snapshot.Activity)
	}
	encoded, err := os.ReadFile(dir + "/" + statusName)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || containsBytes(encoded, secret) {
		t.Fatal("Guardian persisted raw interactive input")
	}

	process.inputErr = errors.New("partial input write is uncertain")
	if err := state.writeInput([]byte("another private value")); err == nil {
		t.Fatal("failed physical input write was reported successful")
	}
	if state.snapshot.Activity.InputBytes.Status != "unavailable" || state.snapshot.Activity.InputWrites.Status != "unavailable" {
		t.Fatalf("uncertain partial write did not invalidate counters: %+v", state.snapshot.Activity)
	}
}

func TestGuardianNoninteractiveInputActivityIsMeasured(t *testing.T) {
	dir := t.TempDir()
	process := &telemetryTestProcess{}
	state := &runState{
		descriptor: launchConfig{Dir: dir, RunID: "run_stdio_activity", Spec: model.RunSpec{Interactive: false}},
		process:    process,
		snapshot:   Snapshot{Version: ProtocolVersion, RunID: "run_stdio_activity", State: model.Running, Activity: initialActivity(false)},
	}
	input := []byte("stdio input")
	if err := state.writeInput(input); err != nil {
		t.Fatal(err)
	}
	activity := state.snapshot.Activity
	if activity.InputBytes.Status != string(model.EvidenceMeasured) || activity.InputBytes.Value != int64(len(input)) {
		t.Fatalf("stdio input byte evidence = %+v", activity.InputBytes)
	}
	if activity.InputWrites.Status != string(model.EvidenceMeasured) || activity.InputWrites.Value != 1 || activity.LastInputAt == nil {
		t.Fatalf("stdio input write evidence = %+v", activity)
	}
	if activity.ResizeCount.Status != string(model.EvidenceUnsupported) {
		t.Fatalf("noninteractive resize evidence = %+v; want unsupported", activity.ResizeCount)
	}
}

func containsBytes(haystack, needle []byte) bool {
	if len(needle) == 0 || len(haystack) < len(needle) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
