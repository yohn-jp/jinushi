package store

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestTelemetryCompactsRawSamplesAndPreservesSpikeProcessAndJournal(t *testing.T) {
	path := t.TempDir() + "/state.db"
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := db.Create(testRun("run-telemetry-spike"), &model.Event{Kind: model.EventRunAccepted})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureTelemetry(TelemetryOptions{
		RawSamples:      2,
		RawBytes:        1 << 20,
		AggregatePoints: 8,
		AggregateBytes:  2 << 20,
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC)
	values := []int64{100, 9000, 200, 300}
	for i, memory := range values {
		sample := testTelemetrySample(base.Add(time.Duration(i)*time.Second), memory, int64(10+i*10))
		if i == 1 {
			sample.Processes = []model.ProcessEvidence{{
				PID:                  77,
				StartTimeTicks:       123456,
				ParentPID:            1,
				ParentStartTimeTicks: 120000,
				ParentObserved:       true,
				Membership:           model.ProcessMembershipOwned,
				Comm:                 "spiky-worker",
				State:                "R",
				CPUTimeNs:            measuredMetric(22),
				RSSBytes:             measuredMetric(memory),
			}}
		}
		appended, err := db.AppendTelemetry(run.ID, sample)
		if err != nil {
			t.Fatalf("append sample %d: %v", i, err)
		}
		if appended.Sequence != uint64(i+1) || appended.Version != model.TelemetrySchemaVersion {
			t.Fatalf("appended sample %d got sequence/version %d/%d", i, appended.Sequence, appended.Version)
		}
	}
	response, err := db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Samples) != 2 || len(response.Aggregates) != 1 {
		t.Fatalf("raw/aggregate counts = %d/%d, want 2/1", len(response.Samples), len(response.Aggregates))
	}
	aggregate := response.Aggregates[0]
	if aggregate.SampleCount != 2 || aggregate.Resources.MemoryBytes.Max.Value != 9000 || aggregate.Resources.MemoryBytes.Max.Status != "measured" {
		t.Fatalf("spike aggregate = %#v", aggregate)
	}
	if aggregate.Resources.CPUTimeNs.First.Value != 10 || aggregate.Resources.CPUTimeNs.Last.Value != 20 || aggregate.Resources.CPUTimeNs.Delta.Value != 10 || aggregate.Resources.CPUTimeNs.Delta.Status != "measured" {
		t.Fatalf("CPU counter aggregation = %#v", aggregate.Resources.CPUTimeNs)
	}
	if aggregate.Resources.MemoryBytes.Delta.Status != "unsupported" {
		t.Fatalf("gauge delta status = %q, want unsupported", aggregate.Resources.MemoryBytes.Delta.Status)
	}
	if len(aggregate.ProcessPeaks) != 1 || aggregate.ProcessPeaks[0].Process.PID != 77 || aggregate.ProcessPeaks[0].Process.StartTimeTicks != 123456 {
		t.Fatalf("process peak evidence = %#v", aggregate.ProcessPeaks)
	}
	if !response.HistoryComplete || response.RetainedFrom == nil || response.RawRetainedFrom == nil {
		t.Fatalf("retention metadata = %#v", response)
	}
	events, _, _, err := db.Events(run.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Kind != model.EventRunAccepted {
		t.Fatalf("telemetry polluted lifecycle journal: %#v", events)
	}
	usage, err := db.TelemetryStoreUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.Runs != 1 || usage.RawSamples != 2 || usage.AggregatePoints != 1 || usage.Bytes <= 0 {
		t.Fatalf("telemetry usage = %#v", usage)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	response, err = db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID})
	if err != nil || len(response.Aggregates) != 1 || response.Aggregates[0].Resources.MemoryBytes.Max.Value != 9000 {
		t.Fatalf("reopened telemetry = %#v, err=%v", response, err)
	}
	deleted, err := db.DeleteTelemetry(run.ID)
	if err != nil || deleted.RawSamples != 2 || deleted.AggregatePoints != 1 {
		t.Fatalf("delete usage = %#v, err=%v", deleted, err)
	}
	usage, err = db.TelemetryStoreUsage()
	if err != nil || usage.Runs != 0 || usage.Bytes != 0 {
		t.Fatalf("usage after delete = %#v, err=%v", usage, err)
	}
	events, _, _, err = db.Events(run.ID, 0, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("telemetry deletion affected journal: %#v, err=%v", events, err)
	}
}

func TestTelemetryAdaptiveCompactionPreservesCounterSpikeAndSequence(t *testing.T) {
	db, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	run, _, err := db.Create(testRun("run-telemetry-adaptive"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureTelemetry(TelemetryOptions{RawSamples: 1, RawBytes: 1 << 20, AggregatePoints: 2, AggregateBytes: 2 << 20}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		memory := int64(100 + i)
		if i == 4 {
			memory = 70000
		}
		sample := testTelemetrySample(base.Add(time.Duration(i)*10*time.Second), memory, int64(100+i*25))
		if i == 4 {
			sample.Processes = []model.ProcessEvidence{{
				PID: 88, StartTimeTicks: 654321, ParentPID: 1, ParentObserved: false,
				Membership: model.ProcessMembershipOwned, Comm: "memory-spike", State: "R",
				CPUTimeNs: measuredMetric(90), RSSBytes: measuredMetric(memory),
			}}
		}
		if _, err := db.AppendTelemetry(run.ID, sample); err != nil {
			t.Fatalf("append sample %d: %v", i, err)
		}
	}
	response, err := db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Aggregates) != 2 || len(response.Samples) != 1 {
		t.Fatalf("bounded query counts = %d aggregates, %d raw", len(response.Aggregates), len(response.Samples))
	}
	var samples uint64
	var spikeFound, adaptiveFound bool
	for _, aggregate := range response.Aggregates {
		samples += aggregate.SampleCount
		adaptiveFound = adaptiveFound || aggregate.Resolution == model.TelemetryAdaptive
		spikeFound = spikeFound || aggregate.Resources.MemoryBytes.Max.Value == 70000
		if aggregate.Resources.CPUTimeNs.Delta.Status != "measured" {
			t.Fatalf("counter delta status lost in aggregate: %#v", aggregate.Resources.CPUTimeNs)
		}
	}
	if samples+uint64(len(response.Samples)) != 10 || !adaptiveFound || !spikeFound || !response.HistoryComplete {
		t.Fatalf("adaptive evidence lost samples/spike/history: samples=%d adaptive=%v spike=%v response=%#v", samples, adaptiveFound, spikeFound, response)
	}
	peakFound := false
	for _, aggregate := range response.Aggregates {
		for _, peak := range aggregate.ProcessPeaks {
			if peak.Process.PID == 88 && peak.Process.StartTimeTicks == 654321 {
				peakFound = true
			}
		}
	}
	if !peakFound {
		t.Fatalf("adaptive process evidence dropped spike process: %#v", response.Aggregates)
	}
	if response.Aggregates[0].FirstSequence == 0 || response.Aggregates[0].LastSequence < response.Aggregates[0].FirstSequence {
		t.Fatalf("aggregate sequence range invalid: %#v", response.Aggregates[0])
	}
}

func TestTelemetryQueryCursorAndGapCompactionAreExplicit(t *testing.T) {
	db, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	run, _, err := db.Create(testRun("run-telemetry-gaps"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureTelemetry(TelemetryOptions{RawSamples: 8, RawBytes: 1 << 20, AggregatePoints: 8, AggregateBytes: 2 << 20, MaxGaps: 2, MaxQueryPoints: 2}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 27, 3, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		if _, err := db.AppendTelemetry(run.ID, testTelemetrySample(base.Add(time.Duration(i)*time.Second), int64(200+i), int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	first, err := db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Samples) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %#v", first)
	}
	second, err := db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID, Limit: 2, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Samples) != 2 || second.Samples[0].Sequence != 3 || second.Samples[1].Sequence != 4 || second.NextCursor != "" {
		t.Fatalf("second page = %#v", second)
	}
	for i := 0; i < 3; i++ {
		from := base.Add(time.Duration(i) * 10 * time.Second)
		gap := model.TelemetryGap{From: from, To: from.Add(time.Second), Reason: fmt.Sprintf("sample-gap-%d", i), Metrics: []string{"cpu"}, DroppedPoints: 1}
		if err := db.AppendTelemetryGap(run.ID, gap); err != nil {
			t.Fatal(err)
		}
	}
	withGaps, err := db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(withGaps.Gaps) != 2 || withGaps.HistoryComplete {
		t.Fatalf("gap history = %#v", withGaps)
	}
	compacted := false
	for _, gap := range withGaps.Gaps {
		if gap.Reason == "gap-history-compacted" && gap.DroppedPoints >= 2 && gap.Resolution == model.TelemetryAdaptive {
			compacted = true
		}
	}
	if !compacted {
		t.Fatalf("missing explicit compacted gap: %#v", withGaps.Gaps)
	}
}

func TestTelemetryCursorReportsStaleAfterCompaction(t *testing.T) {
	db, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	run, _, err := db.Create(testRun("run-telemetry-stale-cursor"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureTelemetry(TelemetryOptions{RawSamples: 2, RawBytes: 1 << 20, AggregatePoints: 8, AggregateBytes: 2 << 20, MaxQueryPoints: 1}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 27, 5, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if _, err := db.AppendTelemetry(run.ID, testTelemetrySample(base.Add(time.Duration(i)*time.Second), int64(i), int64(i))); err != nil {
			t.Fatal(err)
		}
	}
	page, err := db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID, Limit: 1})
	if err != nil || page.NextCursor == "" {
		t.Fatalf("initial page = %#v, err=%v", page, err)
	}
	if _, err := db.AppendTelemetry(run.ID, testTelemetrySample(base.Add(3*time.Second), 3, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID, Limit: 1, Cursor: page.NextCursor}); !errors.Is(err, ErrTelemetryCursorStale) {
		t.Fatalf("stale cursor error = %v, want ErrTelemetryCursorStale", err)
	}
}

func TestTelemetryRejectsMissingStatusAndMalformedProcessIdentity(t *testing.T) {
	db, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	run, _, err := db.Create(testRun("run-telemetry-validation"), nil)
	if err != nil {
		t.Fatal(err)
	}
	missingStatus := testTelemetrySample(time.Now().UTC(), 0, 0)
	missingStatus.PSI.IO.FullAvg60BasisPoints = model.Metric{}
	if _, err := db.AppendTelemetry(run.ID, missingStatus); !errors.Is(err, ErrInvalidTelemetry) {
		t.Fatalf("missing status error = %v, want ErrInvalidTelemetry", err)
	}
	badIdentity := testTelemetrySample(time.Now().UTC(), 0, 0)
	badIdentity.Processes = []model.ProcessEvidence{{
		PID: 0, StartTimeTicks: 4, Membership: model.ProcessMembershipOwned,
		CPUTimeNs: measuredMetric(0), RSSBytes: measuredMetric(0),
	}}
	if _, err := db.AppendTelemetry(run.ID, badIdentity); !errors.Is(err, ErrInvalidTelemetry) {
		t.Fatalf("bad process identity error = %v, want ErrInvalidTelemetry", err)
	}
}

func TestTelemetryAggregateRetainsUnavailableAndUnsupportedCounts(t *testing.T) {
	db, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	run, _, err := db.Create(testRun("run-telemetry-status"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ConfigureTelemetry(TelemetryOptions{RawSamples: 1, RawBytes: 1 << 20, AggregatePoints: 8, AggregateBytes: 2 << 20}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 27, 4, 0, 0, 0, time.UTC)
	first := testTelemetrySample(base, 50, 10)
	first.PSI.CPU.SomeAvg10BasisPoints = model.Metric{Status: string(model.EvidenceUnsupported)}
	second := testTelemetrySample(base.Add(time.Second), 60, 20)
	second.PSI.CPU.SomeAvg10BasisPoints = model.Metric{Status: string(model.EvidenceUnavailable)}
	second.IO.ReadBytes = model.Metric{Status: string(model.EvidenceUnavailable)}
	third := testTelemetrySample(base.Add(2*time.Second), 70, 30)
	for _, sample := range []model.TelemetrySample{first, second, third} {
		if _, err := db.AppendTelemetry(run.ID, sample); err != nil {
			t.Fatal(err)
		}
	}
	response, err := db.QueryTelemetry(model.TelemetryQuery{RunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Aggregates) != 1 {
		t.Fatalf("aggregate count = %d, want 1", len(response.Aggregates))
	}
	pressure := response.Aggregates[0].PSI.CPU.SomeAvg10BasisPoints
	if pressure.Count != 0 || pressure.UnavailableCount != 1 || pressure.UnsupportedCount != 1 || pressure.Min.Status != string(model.EvidenceUnavailable) || pressure.Max.Status != string(model.EvidenceUnavailable) {
		t.Fatalf("PSI status aggregate = %#v", pressure)
	}
	io := response.Aggregates[0].IO.ReadBytes
	if io.Count != 1 || io.UnavailableCount != 1 || io.Min.Value != 100 || io.Last.Status != string(model.EvidenceUnavailable) || io.Delta.Status != string(model.EvidenceUnavailable) {
		t.Fatalf("I/O status aggregate = %#v", io)
	}
}

func testTelemetrySample(observedAt time.Time, memory, cpu int64) model.TelemetrySample {
	measured := func(value int64) model.Metric {
		return model.Metric{Status: string(model.EvidenceMeasured), Value: value}
	}
	unsupported := func() model.Metric { return model.Metric{Status: string(model.EvidenceUnsupported)} }
	pressure := model.PressureMetrics{
		SomeAvg10BasisPoints: unsupported(), SomeAvg60BasisPoints: unsupported(), SomeAvg300BasisPoints: unsupported(), SomeTotalUS: unsupported(),
		FullAvg10BasisPoints: unsupported(), FullAvg60BasisPoints: unsupported(), FullAvg300BasisPoints: unsupported(), FullTotalUS: unsupported(),
	}
	return model.TelemetrySample{
		ObservedAt: observedAt.UTC(),
		Resources: model.Resources{
			MemoryBytes: measured(memory), PeakMemoryBytes: measured(memory), CPUTimeNs: measured(cpu),
			ProcessCount: measured(1), PeakProcessCount: measured(1), TaskCount: measured(1), PeakTaskCount: measured(1),
		},
		IO: model.IOMetrics{
			ReadBytes: measured(cpu * 10), WriteBytes: measured(cpu * 5), ReadOperations: measured(cpu), WriteOperations: measured(cpu),
			DeviceEvidenceStatus: model.EvidenceMeasured, DeviceEvidenceComplete: true,
		},
		PSI:                   model.PSIMetrics{CPU: pressure, Memory: pressure, IO: pressure},
		Activity:              model.IOActivity{InputBytes: measured(0), InputWrites: measured(0), ResizeCount: measured(0)},
		ProcessEvidenceStatus: model.EvidenceMeasured, ProcessEvidenceComplete: true,
	}
}

func measuredMetric(value int64) model.Metric {
	return model.Metric{Status: string(model.EvidenceMeasured), Value: value}
}
