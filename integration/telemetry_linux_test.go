//go:build linux

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

const (
	telemetryRawRetentionSamples = 512
	telemetryAggregateRetention  = 2048
)

func newTelemetryHarness(t *testing.T) *harness {
	t.Helper()
	root, err := os.MkdirTemp("", "jinushi-telemetry-")
	if err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		_ = os.RemoveAll(root)
		t.Fatal(err)
	}
	config := []byte(`{"sampleIntervalMs":50,"eventRetentionCount":16,"eventRetentionBytes":262144}`)
	if err := os.WriteFile(filepath.Join(stateDir, "config.json"), config, 0600); err != nil {
		_ = os.RemoveAll(root)
		t.Fatal(err)
	}
	db, err := store.Open(filepath.Join(stateDir, "state.db"), store.Options{})
	if err != nil {
		_ = os.RemoveAll(root)
		t.Fatal(err)
	}
	if err := db.ConfigureTelemetry(store.TelemetryOptions{
		RawSamples: 32, RawBytes: 4 << 20, AggregatePoints: 64, AggregateBytes: 4 << 20,
	}); err != nil {
		_ = db.Close()
		_ = os.RemoveAll(root)
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		_ = os.RemoveAll(root)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	h := &harness{t: t, stateDir: stateDir}
	t.Cleanup(h.cleanup)
	h.startSupervisor()
	return h
}

func TestLinuxTelemetryBoundsAndAttributesShortProcessSpike(t *testing.T) {
	h := newTelemetryHarness(t)
	pidFile := filepath.Join(t.TempDir(), "spike.pid")
	script := `IFS= read -r input
"$1" allocate 67108864 &
spike=$!
printf '%s\n' "$spike" > "$2"
sleep 1.5
kill "$spike" 2>/dev/null || :
wait "$spike" 2>/dev/null || :
"$1" flood 393216
sleep 60`
	started := h.run("--", "/bin/sh", "-c", script, "jinushi-telemetry-spike", helperBinary, pidFile)
	h.waitUntil("telemetry fixture Run to start", 10*time.Second, func() bool {
		return h.inspect(started.ID).State == "running"
	})
	inputText := "bounded telemetry stdin\n"
	input, _, response := h.invoke(5*time.Second, "input", "--state-dir", h.stateDir,
		"--request-id", "telemetry-fixture-input", "--expected-generation",
		strconv.FormatUint(h.inspect(started.ID).Generation, 10), started.ID, inputText)
	if input != 0 || response.Error != nil {
		t.Fatalf("write tracked stdin input: exit=%d response=%+v", input, response)
	}
	pid := waitForPIDFile(t, pidFile, 10*time.Second)
	deadline := time.Now().Add(20 * time.Second)
	compacted := false
	for time.Now().Before(deadline) {
		response := callSupervisor(t, h, protocol.Request{
			Version: model.ProtocolVersion,
			Op:      "telemetry",
			RunID:   started.ID,
			TelemetryQuery: &model.TelemetryQuery{
				RunID: started.ID, Resolution: model.TelemetryAdaptive, Limit: 1,
			},
		})
		if response.Error != nil || response.Telemetry == nil {
			t.Fatalf("poll telemetry retention during active Run: %+v", response)
		}
		if response.Telemetry.RawRetainedFrom != nil && response.Telemetry.RetainedFrom != nil &&
			response.Telemetry.RawRetainedFrom.After(*response.Telemetry.RetainedFrom) {
			compacted = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !compacted {
		t.Fatalf("persisted telemetry policy did not compact raw history before deadline")
	}
	current := h.inspect(started.ID)
	cancelCode, _, canceled := h.invoke(5*time.Second, "cancel", "--state-dir", h.stateDir,
		"--request-id", "telemetry-bounds-cancel", "--expected-generation", strconv.FormatUint(current.Generation, 10), started.ID)
	if cancelCode != 0 || canceled.Error != nil {
		t.Fatalf("cancel telemetry fixture Run after persisted compaction: exit=%d response=%+v", cancelCode, canceled)
	}
	code, completed := h.await(started.ID, 15*time.Second)
	if completed.State != "terminal" || completed.Receipt == nil || completed.Receipt.Outcome != "cancelled" || completed.Receipt.Cleanup != "complete" {
		t.Fatalf("long-lived telemetry Run did not reach proven cancellation: exit=%d run=%+v", code, completed)
	}

	inspection := callSupervisor(t, h, protocol.Request{Version: model.ProtocolVersion, Op: "inspect", RunID: started.ID})
	if inspection.Run == nil || inspection.Run.EffectiveCapabilities == nil {
		t.Fatalf("terminal Run omitted effective capability receipt: %+v", inspection)
	}

	telemetry := queryAllTelemetry(t, h, started.ID)
	if !telemetry.HistoryComplete {
		t.Fatalf("telemetry history reported a gap despite a continuously supervised fixture: %+v", telemetry.Gaps)
	}
	if len(telemetry.Samples) == 0 || len(telemetry.Samples) > telemetryRawRetentionSamples {
		t.Fatalf("raw telemetry retention count = %d, want 1..%d", len(telemetry.Samples), telemetryRawRetentionSamples)
	}
	if len(telemetry.Aggregates) == 0 || len(telemetry.Aggregates) > telemetryAggregateRetention {
		t.Fatalf("aggregate telemetry retention count = %d, want 1..%d (samples=%d rawFrom=%v retainedFrom=%v)", len(telemetry.Aggregates), telemetryAggregateRetention,
			len(telemetry.Samples), telemetry.RawRetainedFrom, telemetry.RetainedFrom)
	}
	if len(telemetry.Samples) > 32 || len(telemetry.Aggregates) > 64 {
		t.Fatalf("persisted fixture telemetry policy bounds were exceeded: raw=%d aggregates=%d", len(telemetry.Samples), len(telemetry.Aggregates))
	}
	if telemetry.RawRetainedFrom == nil || telemetry.RetainedFrom == nil || !telemetry.RawRetainedFrom.After(*telemetry.RetainedFrom) {
		t.Fatalf("telemetry retention did not expose raw compaction watermarks: %+v", telemetry)
	}

	processEvidenceMeasured := false
	spikeAttributed := false
	inputObserved := false
	for _, sample := range telemetry.Samples {
		processEvidenceMeasured = processEvidenceMeasured || sample.ProcessEvidenceStatus == model.EvidenceMeasured
		if sample.Activity.InputBytes.Status == string(model.EvidenceMeasured) && sample.Activity.InputBytes.Value >= int64(len(inputText)) && sample.Activity.InputWrites.Status == string(model.EvidenceMeasured) && sample.Activity.LastInputAt != nil {
			inputObserved = true
		}
		for _, process := range sample.Processes {
			if process.PID == pid && process.RSSBytes.Status == string(model.EvidenceMeasured) && process.RSSBytes.Value >= 16<<20 {
				spikeAttributed = true
			}
		}
	}
	for _, aggregate := range telemetry.Aggregates {
		for _, peak := range aggregate.ProcessPeaks {
			if peak.Process.PID == pid && peak.Process.RSSBytes.Status == string(model.EvidenceMeasured) && peak.Process.RSSBytes.Value >= 16<<20 {
				spikeAttributed = true
			}
		}
	}
	if inspection.Run.EffectiveCapabilities.ProcessTelemetry {
		if !processEvidenceMeasured {
			t.Fatalf("effective process telemetry was enabled but every sample reported unavailable or unsupported process evidence")
		}
		if !spikeAttributed {
			t.Fatalf("short RSS spike was not attributed to physical process PID %d in raw samples or aggregate peaks", pid)
		}
	} else {
		for _, sample := range telemetry.Samples {
			if sample.ProcessEvidenceStatus != model.EvidenceUnavailable && sample.ProcessEvidenceStatus != model.EvidenceUnsupported {
				t.Fatalf("process telemetry capability was disabled but evidence status=%s", telemetryEvidenceStatus(sample))
			}
		}
		t.Logf("physical process attribution unsupported by effective backend %q", inspection.Run.EffectiveCapabilities.Backend)
	}
	if !inputObserved {
		t.Fatalf("stdin byte/write activity was not preserved in telemetry; expected at least %d bytes", len(inputText))
	}

	journal := callSupervisor(t, h, protocol.Request{Version: model.ProtocolVersion, Op: "events", RunID: started.ID, Limit: 1000})
	if journal.Error != nil {
		t.Fatalf("read bounded lifecycle journal: %+v", journal.Error)
	}
	if !journal.Gap || journal.RetainedFrom <= 1 || len(journal.Events) > 16 {
		t.Fatalf("lifecycle journal did not compact to its configured count bound: gap=%v retainedFrom=%d events=%d", journal.Gap, journal.RetainedFrom, len(journal.Events))
	}
	for _, event := range journal.Events {
		if event.Kind == model.EventResourceSample || event.Kind == model.EventResourceUnavailable {
			t.Fatalf("high-rate telemetry polluted lifecycle journal: %+v", event)
		}
	}
	if !inspection.Run.EffectiveCapabilities.ProcessTelemetry {
		t.Log("short process RSS spike could not be certified in this environment; stored evidence remains explicitly unavailable/unsupported")
	}
}

func callSupervisor(t *testing.T, h *harness, request protocol.Request) protocol.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := ipc.Call(ctx, h.stateDir, request)
	if err != nil {
		t.Fatalf("supervisor %s request: %v", request.Op, err)
	}
	return response
}

func queryAllTelemetry(t *testing.T, h *harness, runID string) model.TelemetryResponse {
	t.Helper()
	combined := model.TelemetryResponse{RunID: runID, Samples: []model.TelemetrySample{}, Aggregates: []model.TelemetryAggregate{}}
	pageSummary := []string{}
	cursor := ""
	for pageNumber := 0; pageNumber < 32; pageNumber++ {
		response := callSupervisor(t, h, protocol.Request{
			Version: model.ProtocolVersion,
			Op:      "telemetry",
			RunID:   runID,
			TelemetryQuery: &model.TelemetryQuery{
				RunID: runID, Resolution: model.TelemetryAdaptive, Limit: 64, Cursor: cursor,
			},
		})
		if response.Error != nil || response.Telemetry == nil {
			t.Fatalf("query telemetry page %d: response=%+v", pageNumber, response)
		}
		page := response.Telemetry
		pageSummary = append(pageSummary, fmt.Sprintf("%d:samples=%d aggregates=%d next=%t retained=%v raw=%v", pageNumber,
			len(page.Samples), len(page.Aggregates), page.NextCursor != "", page.RetainedFrom, page.RawRetainedFrom))
		if pageNumber == 0 {
			combined.Version = page.Version
			combined.RawRetainedFrom = page.RawRetainedFrom
			combined.RetainedFrom = page.RetainedFrom
			combined.Resolution = page.Resolution
			combined.HistoryComplete = page.HistoryComplete
		}
		combined.Samples = append(combined.Samples, page.Samples...)
		combined.Aggregates = append(combined.Aggregates, page.Aggregates...)
		combined.Gaps = append(combined.Gaps, page.Gaps...)
		if page.NextCursor == "" {
			t.Logf("telemetry query pages: %s", strings.Join(pageSummary, "; "))
			return combined
		}
		cursor = page.NextCursor
	}
	t.Fatalf("telemetry query exceeded bounded page count for Run %s", runID)
	return combined
}

func telemetryEvidenceStatus(sample model.TelemetrySample) string {
	return fmt.Sprintf("process=%s io=%s cpuPsi=%s memoryPsi=%s ioPsi=%s", sample.ProcessEvidenceStatus,
		sample.IO.DeviceEvidenceStatus, sample.PSI.CPU.SomeAvg10BasisPoints.Status,
		sample.PSI.Memory.SomeAvg10BasisPoints.Status, sample.PSI.IO.SomeAvg10BasisPoints.Status)
}
