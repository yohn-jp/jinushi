package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestLifecycleUpdateAndEventCommitTogether(t *testing.T) {
	path := t.TempDir() + "/state.db"
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	run, createdEvent, err := s.Create(testRun("run-atomic"), &model.Event{Kind: "run.accepted"})
	if err != nil {
		t.Fatal(err)
	}
	if createdEvent == nil || createdEvent.Seq != 1 || createdEvent.RunID != run.ID {
		t.Fatalf("created event = %#v", createdEvent)
	}

	run.State = model.Running
	run.Generation++
	started, err := s.Update(run, &model.Event{Kind: "run.started"})
	if err != nil {
		t.Fatal(err)
	}
	if started == nil || started.Seq != 2 {
		t.Fatalf("started event = %#v", started)
	}

	terminal := run
	terminal.State = model.Terminal
	terminal.Generation++
	if _, err := s.Update(terminal, &model.Event{Kind: "run.terminal"}); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("terminal Update without receipt error = %v, want ErrInvalidRun", err)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != model.Running {
		t.Fatalf("failed terminal update changed state to %q", current.State)
	}
	stale := current
	stale.Generation--
	if _, err := s.Update(stale, nil); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("stale generation error = %v, want ErrInvalidRun", err)
	}
	noGeneration := current
	noGeneration.State = model.Terminating
	if _, err := s.Update(noGeneration, nil); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("state change without generation error = %v, want ErrInvalidRun", err)
	}
	events, retainedFrom, gap, err := s.Events(run.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if retainedFrom != 1 || gap || len(events) != 2 || events[1].Kind != "run.started" {
		t.Fatalf("events after failed update = %#v, watermark=%d gap=%v", events, retainedFrom, gap)
	}

	finishedAt := time.Now().UTC()
	terminal.Receipt = &model.Receipt{
		Version:    model.ProtocolVersion,
		RunID:      terminal.ID,
		Outcome:    "exited",
		FinishedAt: finishedAt,
		Resources:  model.Resources{},
		Output:     terminal.Output,
		Cleanup:    "complete",
	}
	terminal.FinishedAt = &finishedAt
	finished, err := s.Update(terminal, &model.Event{Kind: "run.terminal"})
	if err != nil {
		t.Fatal(err)
	}
	if finished == nil || finished.Seq != 3 {
		t.Fatalf("terminal event = %#v", finished)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	current, err = s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != model.Terminal || current.Receipt == nil || current.Receipt.Outcome != "exited" {
		t.Fatalf("reopened terminal Run = %#v", current)
	}
	if current.Receipt.EventFirstSeq != 1 || current.Receipt.EventLastSeq != 3 ||
		current.Receipt.EventRetainedFrom != 1 || !current.Receipt.EventHistoryComplete || current.Receipt.EvidenceIncomplete {
		t.Fatalf("reopened receipt event range = %#v", current.Receipt)
	}
	events, retainedFrom, gap, err = s.Events(run.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if retainedFrom != 1 || gap || len(events) != 3 || events[2].Seq != 3 || events[2].Kind != "run.terminal" {
		t.Fatalf("reopened event history = %#v, watermark=%d gap=%v", events, retainedFrom, gap)
	}
}

func TestTypedEventPayloadUsesCanonicalVersionedSchema(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-typed-events"), nil); err != nil {
		t.Fatal(err)
	}
	appended, err := s.AppendEvent("run-typed-events", model.Event{
		Kind: model.EventRunAccepted,
		Payload: &model.EventPayload{Run: &model.RunEventPayload{
			State: model.Accepted, Generation: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if appended.Version != model.EventSchemaVersion || appended.Kind != model.EventRunAccepted {
		t.Fatalf("appended event header = %#v", appended)
	}
	if _, err := s.AppendEvent("run-typed-events", model.Event{
		Kind:    model.EventResourceSample,
		Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Running}},
	}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("mismatched typed payload error = %v, want ErrInvalidEvent", err)
	}
	events, _, _, err := s.Events("run-typed-events", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Payload == nil || events[0].Payload.Run == nil || events[0].Payload.Run.State != model.Accepted {
		t.Fatalf("stored typed event = %#v", events)
	}
}

func TestEffectiveCapabilitiesAreFrozenIntoReceipt(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run, _, err := s.Create(testRun("run-effective-backend"), nil)
	if err != nil {
		t.Fatal(err)
	}
	effective := model.Capabilities{Backend: "linux-session"}
	run.State = model.Running
	run.Generation++
	run.EffectiveCapabilities = &effective
	if _, err := s.Update(run, nil); err != nil {
		t.Fatal(err)
	}
	changed := run
	changedCaps := effective
	changedCaps.Backend = "linux-cgroup-v2"
	changed.EffectiveCapabilities = &changedCaps
	if _, err := s.Update(changed, nil); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("effective capability mutation error = %v, want ErrInvalidRun", err)
	}

	finishedAt := time.Now().UTC()
	run.State = model.Terminal
	run.Generation++
	run.FinishedAt = &finishedAt
	run.Receipt = &model.Receipt{
		Version:               model.ProtocolVersion,
		RunID:                 run.ID,
		Outcome:               "exited",
		FinishedAt:            finishedAt,
		EffectiveCapabilities: &effective,
		Capabilities:          effective,
	}
	if _, err := s.Update(run, nil); err != nil {
		t.Fatal(err)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.EffectiveCapabilities == nil || current.Receipt == nil || current.Receipt.EffectiveCapabilities == nil ||
		current.EffectiveCapabilities.Backend != "linux-session" || current.Receipt.Capabilities.Backend != "linux-session" {
		t.Fatalf("effective backend was not frozen into the receipt: %#v", current)
	}
}

func TestFailedMultiEventLifecycleUpdateRollsBackEntireTransaction(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{
		EventRetentionCount: 8,
		EventRetentionBytes: 512,
		MaxEventBytes:       256,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run, _, err := s.Create(testRun("run-rollback"), nil)
	if err != nil {
		t.Fatal(err)
	}
	run.State = model.Running
	run.Generation++
	_, err = s.UpdateWithEvents(run, []model.Event{
		{Kind: "run.started"},
		{Kind: "resource.sample", Body: map[string]any{"payload": bytes.Repeat([]byte("x"), 2048)}},
	})
	if !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("Update error = %v, want ErrEventTooLarge", err)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != model.Accepted {
		t.Fatalf("failed update persisted state %q", current.State)
	}
	events, retainedFrom, gap, err := s.Events(run.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 || retainedFrom != 1 || gap {
		t.Fatalf("failed update persisted journal: events=%#v retainedFrom=%d gap=%v", events, retainedFrom, gap)
	}
}

func TestUpdateWithEventsStampsFinalSequence(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run, _, err := s.Create(testRun("run-multiple-terminal-events"), &model.Event{Kind: "run.accepted"})
	if err != nil {
		t.Fatal(err)
	}
	run.State = model.Running
	run.Generation++
	if _, err := s.UpdateWithEvents(run, []model.Event{{Kind: "run.started"}}); err != nil {
		t.Fatal(err)
	}
	run.State = model.Terminal
	run.Generation++
	finishedAt := time.Now().UTC()
	run.FinishedAt = &finishedAt
	run.Receipt = &model.Receipt{
		Version:    model.ProtocolVersion,
		RunID:      run.ID,
		Outcome:    "resource-limit",
		FinishedAt: finishedAt,
		Cleanup:    "complete",
	}
	appended, err := s.UpdateWithEvents(run, []model.Event{
		{Kind: "limit.reached"},
		{Kind: "lease.expired"},
		{Kind: "run.terminal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(appended) != 3 || appended[0].Seq != 3 || appended[1].Seq != 4 || appended[2].Seq != 5 {
		t.Fatalf("multi-event append result = %#v", appended)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Receipt == nil || current.Receipt.EventFirstSeq != 1 || current.Receipt.EventLastSeq != 5 ||
		current.Receipt.EventRetainedFrom != 1 || !current.Receipt.EventHistoryComplete || current.Receipt.EvidenceIncomplete {
		t.Fatalf("terminal receipt range = %#v", current.Receipt)
	}
	events, retainedFrom, gap, err := s.Events(run.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if gap || retainedFrom != 1 || len(events) != 5 || events[4].Seq != 5 || events[4].Kind != "run.terminal" {
		t.Fatalf("multi-event journal = %#v retainedFrom=%d gap=%v", events, retainedFrom, gap)
	}
}

func TestEventCompactionReportsWatermarkAndBoundsPage(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{
		EventRetentionCount: 3,
		EventRetentionBytes: 4096,
		MaxEventBytes:       1024,
		MaxEventPage:        2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-events"), nil); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 6; i++ {
		if _, err := s.AppendEvent("run-events", model.Event{Kind: model.EventResourceSample}); err != nil {
			t.Fatal(err)
		}
	}
	events, retainedFrom, gap, err := s.Events("run-events", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if retainedFrom != 4 || !gap || len(events) != 2 || events[0].Seq != 4 || events[1].Seq != 5 {
		t.Fatalf("bounded events = %#v, retainedFrom=%d gap=%v", events, retainedFrom, gap)
	}
	events, retainedFrom, gap, err = s.Events("run-events", 3, 2)
	if err != nil {
		t.Fatal(err)
	}
	if retainedFrom != 4 || gap || len(events) != 2 || events[0].Seq != 4 || events[1].Seq != 5 {
		t.Fatalf("events from immediately before watermark = %#v, retainedFrom=%d gap=%v", events, retainedFrom, gap)
	}
}

func TestLifecycleEventsSurviveTelemetryCompactionUntilTerminal(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{
		EventRetentionCount: 3,
		EventRetentionBytes: 4096,
		MaxEventBytes:       1024,
		MaxEventPage:        16,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run, accepted, err := s.Create(testRun("run-critical-events"), &model.Event{Kind: "run.accepted"})
	if err != nil {
		t.Fatal(err)
	}
	if accepted == nil || accepted.Seq != 1 {
		t.Fatalf("accepted event = %#v", accepted)
	}
	run.State = model.Running
	run.Generation++
	if _, err := s.Update(run, &model.Event{Kind: model.EventRunRunning}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := s.AppendEvent(run.ID, model.Event{Kind: "resource.sample"}); err != nil {
			t.Fatal(err)
		}
	}
	events, retainedFrom, gap, err := s.Events(run.ID, 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if !gap || retainedFrom != 1 || len(events) != 3 || events[0].Kind != model.EventRunAccepted || events[1].Kind != model.EventRunRunning {
		t.Fatalf("events after telemetry compaction = %#v retainedFrom=%d gap=%v", events, retainedFrom, gap)
	}

	finishedAt := time.Now().UTC()
	run.State = model.Terminal
	run.Generation++
	run.FinishedAt = &finishedAt
	run.Receipt = &model.Receipt{
		Version:    model.ProtocolVersion,
		RunID:      run.ID,
		Outcome:    "exited",
		FinishedAt: finishedAt,
		Cleanup:    "complete",
	}
	if _, err := s.Update(run, &model.Event{Kind: "run.terminal"}); err != nil {
		t.Fatal(err)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Receipt == nil || current.Receipt.EventFirstSeq != 1 || current.Receipt.EventLastSeq != 23 ||
		current.Receipt.EventRetainedFrom != 2 || current.Receipt.EventHistoryComplete || !current.Receipt.EvidenceIncomplete {
		t.Fatalf("receipt did not stamp compacted event history: %#v", current.Receipt)
	}
	events, _, _, err = s.Events(run.ID, 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[len(events)-1].Kind != "run.terminal" {
		t.Fatalf("bounded post-terminal events = %#v", events)
	}
}

func TestProtectedLifecycleEventsCanRejectAnUnboundedJournal(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{
		EventRetentionCount: 1,
		EventRetentionBytes: 4096,
		MaxEventBytes:       1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run, _, err := s.Create(testRun("run-journal-full"), &model.Event{Kind: "run.accepted"})
	if err != nil {
		t.Fatal(err)
	}
	run.State = model.Running
	run.Generation++
	if _, err := s.Update(run, &model.Event{Kind: model.EventRunRunning}); !errors.Is(err, ErrJournalFull) {
		t.Fatalf("second protected event error = %v, want ErrJournalFull", err)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != model.Accepted || current.Generation != 1 {
		t.Fatalf("full journal partially committed lifecycle update: %#v", current)
	}
}

func TestOutputFloodKeepsBoundedTailAndReportsGap(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{
		OutputChunkBytes:    7,
		OutputReadBytes:     16,
		OutputRetainedBytes: 96,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-output"), nil); err != nil {
		t.Fatal(err)
	}
	var all bytes.Buffer
	for i := 0; i < 1000; i++ {
		chunk := []byte(fmt.Sprintf("chunk-%08d\n", i))
		all.Write(chunk)
		if _, err := s.AppendOutput("run-output", "stdout", chunk, 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	wantTail := all.Bytes()[all.Len()-96:]
	got, err := s.Get("run-output")
	if err != nil {
		t.Fatal(err)
	}
	if got.Output.Stdout.ObservedBytes != int64(all.Len()) || got.Output.Stdout.RetainedBytes != 96 ||
		got.Output.Stdout.RetainedFrom != int64(all.Len()-96) || !got.Output.Stdout.Truncated || got.Output.HistoryComplete {
		t.Fatalf("Run output metadata = %#v", got.Output)
	}
	data, retainedFrom, observed, gap, err := s.ReadOutput("run-output", "stdout", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if retainedFrom != int64(all.Len()-96) || observed != int64(all.Len()) || !gap || !bytes.Equal(data, wantTail[:16]) {
		t.Fatalf("first output read = %q retainedFrom=%d observed=%d gap=%v", data, retainedFrom, observed, gap)
	}
	collected := append([]byte(nil), data...)
	cursor := retainedFrom + int64(len(data))
	for cursor < observed {
		data, retainedFrom, observed, gap, err = s.ReadOutput("run-output", "stdout", cursor, 100)
		if err != nil {
			t.Fatal(err)
		}
		if retainedFrom != int64(all.Len()-96) || observed != int64(all.Len()) || gap || len(data) == 0 {
			t.Fatalf("continued output read = %q retainedFrom=%d observed=%d gap=%v", data, retainedFrom, observed, gap)
		}
		collected = append(collected, data...)
		cursor += int64(len(data))
	}
	if !bytes.Equal(collected, wantTail) {
		t.Fatalf("retained output = %q, want %q", collected, wantTail)
	}
}

func TestOutputAppendAndStreamMetadataSurviveReopen(t *testing.T) {
	path := t.TempDir() + "/state.db"
	s, err := Open(path, Options{OutputRetainedBytes: 24})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(testRun("run-reopen"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendOutput("run-reopen", "stderr", []byte("0123456789"), 24); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Options{OutputRetainedBytes: 24})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	data, retainedFrom, observed, gap, err := s.ReadOutput("run-reopen", "stderr", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "0123456789" || retainedFrom != 0 || observed != 10 || gap {
		t.Fatalf("reopened output = %q retainedFrom=%d observed=%d gap=%v", data, retainedFrom, observed, gap)
	}
}

func TestOutputAggregateCeilingAndPartialChunkTrim(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{
		OutputChunkBytes:    10,
		OutputReadBytes:     64,
		OutputRetainedBytes: 96,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run := testRun("run-output-quota")
	run.Spec.Limits.OutputBytes = 15
	if _, _, err := s.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{"stdout", "stderr", "pty"} {
		if _, err := s.AppendOutput(run.ID, stream, []byte("0123456789"), 100); err != nil {
			t.Fatal(err)
		}
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	retainedTotal := current.Output.Stdout.RetainedBytes + current.Output.Stderr.RetainedBytes + current.Output.PTY.RetainedBytes
	if retainedTotal != 15 {
		t.Fatalf("aggregate retained output = %d, want 15 (metadata=%#v)", retainedTotal, current.Output)
	}
	for _, stream := range []string{"stdout", "stderr", "pty"} {
		data, retainedFrom, observed, gap, err := s.ReadOutput(run.ID, stream, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]struct {
			data         string
			retainedFrom int64
			gap          bool
		}{
			"stdout": {data: "", retainedFrom: 10, gap: true},
			"stderr": {data: "56789", retainedFrom: 5, gap: true},
			"pty":    {data: "0123456789", retainedFrom: 0, gap: false},
		}[stream]
		if string(data) != want.data || retainedFrom != want.retainedFrom || observed != 10 || gap != want.gap {
			t.Fatalf("partial output trim for %s = %q retainedFrom=%d observed=%d gap=%v", stream, data, retainedFrom, observed, gap)
		}
	}
}

func TestPTYOutputUsesSharedAggregateRetention(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{OutputRetainedBytes: 24})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run := testRun("run-pty-output-quota")
	run.Spec.Interactive = true
	if _, _, err := s.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendOutput(run.ID, "pty", []byte("012345678901234567890123456789"), 0); err != nil {
		t.Fatal(err)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Output.PTY.ObservedBytes != 30 || current.Output.PTY.RetainedBytes != 24 || current.Output.PTY.RetainedFrom != 6 {
		t.Fatalf("PTY output metadata = %#v", current.Output.PTY)
	}
	data, retainedFrom, observed, gap, err := s.ReadOutput(run.ID, "pty", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "678901234567890123456789" || retainedFrom != 6 || observed != 30 || !gap {
		t.Fatalf("PTY output = %q retainedFrom=%d observed=%d gap=%v", data, retainedFrom, observed, gap)
	}
}

func TestOutputFloodKeepsDatabaseBounded(t *testing.T) {
	path := t.TempDir() + "/state.db"
	s, err := Open(path, Options{
		OutputChunkBytes:    1024,
		OutputReadBytes:     64,
		OutputRetainedBytes: 96,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-output-disk-bound"), nil); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 64<<10)
	var last byte
	for i := 0; i < 512; i++ {
		last = byte(i % 251)
		for j := range payload {
			payload[j] = last
		}
		if _, err := s.AppendOutput("run-output-disk-bound", "stdout", payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	data, retainedFrom, observed, gap, err := s.ReadOutput("run-output-disk-bound", "stdout", 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 64 || !bytes.Equal(data, bytes.Repeat([]byte{last}, 64)) ||
		retainedFrom != (32<<20)-96 || observed != 32<<20 || !gap {
		t.Fatalf("flood output len=%d retainedFrom=%d observed=%d gap=%v", len(data), retainedFrom, observed, gap)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 1<<20 {
		t.Fatalf("database grew to %d bytes while retaining only 32 output bytes", info.Size())
	}
}

func TestRecordOutputGapPreservesAbsoluteOffsets(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{OutputRetainedBytes: 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-output-gap"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendOutput("run-output-gap", "stdout", []byte("before-gap"), 30); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordOutputGap("run-output-gap", "stdout", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendOutput("run-output-gap", "stdout", []byte("future-output"), 30); err != nil {
		t.Fatal(err)
	}
	data, retainedFrom, observed, gap, err := s.ReadOutput("run-output-gap", "stdout", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "future-output" || retainedFrom != 100 || observed != 113 || !gap {
		t.Fatalf("output after gap = %q retainedFrom=%d observed=%d gap=%v", data, retainedFrom, observed, gap)
	}
}

func TestAtomicOutputWritesPersistMetadataAndJournalEvent(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{
		OutputChunkBytes:    3,
		OutputReadBytes:     64,
		OutputRetainedBytes: 96,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run, accepted, err := s.Create(testRun("run-output-events"), &model.Event{Kind: "run.accepted"})
	if err != nil {
		t.Fatal(err)
	}
	if accepted == nil || accepted.Seq != 1 {
		t.Fatalf("accepted event = %#v", accepted)
	}

	chunkAt := time.Date(2026, 9, 27, 1, 2, 3, 0, time.UTC)
	chunkMeta, err := s.AppendOutputWithEvent(run.ID, "stdout", []byte("0123456789"), 24, model.Event{
		Kind:       "output.chunk",
		ObservedAt: chunkAt,
		Body:       map[string]any{"stream": "stdout", "observedBytes": 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if chunkMeta.ObservedBytes != 10 || chunkMeta.RetainedBytes != 10 || chunkMeta.RetainedFrom != 0 || chunkMeta.Truncated {
		t.Fatalf("appended output metadata = %#v", chunkMeta)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Output.Stdout != chunkMeta || current.LastOutputAt == nil || !current.LastOutputAt.Equal(chunkAt) {
		t.Fatalf("Run after output chunk = output %#v lastOutputAt %v", current.Output, current.LastOutputAt)
	}
	data, retainedFrom, observed, gap, err := s.ReadOutput(run.ID, "stdout", 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "0123456789" || retainedFrom != 0 || observed != 10 || gap {
		t.Fatalf("read output chunk = %q retainedFrom=%d observed=%d gap=%v", data, retainedFrom, observed, gap)
	}

	gapAt := chunkAt.Add(-time.Second)
	gapMeta, err := s.RecordOutputGapWithEvent(run.ID, "stdout", 100, model.Event{
		Kind:       "output.gap",
		ObservedAt: gapAt,
		Body:       map[string]any{"stream": "stdout", "observedBytes": 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gapMeta.ObservedBytes != 100 || gapMeta.RetainedBytes != 0 || gapMeta.RetainedFrom != 100 || !gapMeta.Truncated {
		t.Fatalf("gap output metadata = %#v", gapMeta)
	}
	current, err = s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Output.Stdout != gapMeta || current.LastOutputAt == nil || !current.LastOutputAt.Equal(chunkAt) {
		t.Fatalf("Run after output gap = output %#v lastOutputAt %v", current.Output, current.LastOutputAt)
	}
	stderrMeta, err := s.AppendOutputWithEvent(run.ID, "stderr", []byte("x"), 24, model.Event{Kind: "output.chunk"})
	if err != nil {
		t.Fatal(err)
	}
	if stderrMeta.ObservedBytes != 1 {
		t.Fatalf("stderr output metadata = %#v", stderrMeta)
	}
	events, eventRetainedFrom, eventGap, err := s.Events(run.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if eventRetainedFrom != 1 || eventGap || len(events) != 4 || events[1].Seq != 2 || events[1].Kind != "output.chunk" ||
		!events[1].ObservedAt.Equal(chunkAt) || events[2].Seq != 3 || events[2].Kind != "output.gap" || !events[2].ObservedAt.Equal(gapAt) ||
		events[3].Seq != 4 || events[3].Kind != "output.chunk" || events[3].ObservedAt.IsZero() {
		t.Fatalf("output event sequence = %#v retainedFrom=%d gap=%v", events, eventRetainedFrom, eventGap)
	}
	current, err = s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantLastOutputAt := chunkAt
	if events[3].ObservedAt.After(wantLastOutputAt) {
		wantLastOutputAt = events[3].ObservedAt
	}
	if current.LastOutputAt == nil || !current.LastOutputAt.Equal(wantLastOutputAt) {
		t.Fatalf("LastOutputAt = %v, want maximum event timestamp %v", current.LastOutputAt, wantLastOutputAt)
	}
}

func TestAtomicOutputWriteRollsBackWhenEventCannotBePersisted(t *testing.T) {
	s, err := Open(t.TempDir()+"/state.db", Options{
		EventRetentionBytes: 512,
		MaxEventBytes:       256,
		OutputChunkBytes:    4,
		OutputReadBytes:     64,
		OutputRetainedBytes: 96,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run, _, err := s.Create(testRun("run-output-event-rollback"), nil)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := s.AppendOutput(run.ID, "stdout", []byte("baseline"), 24)
	if err != nil {
		t.Fatal(err)
	}
	largeEvent := func(kind model.EventKind) model.Event {
		return model.Event{Kind: kind, Body: map[string]any{"payload": bytes.Repeat([]byte("x"), 1024)}}
	}
	if _, err := s.AppendOutputWithEvent(run.ID, "stdout", []byte("lost"), 24, largeEvent("output.chunk")); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversized chunk event error = %v, want ErrEventTooLarge", err)
	}
	if _, err := s.RecordOutputGapWithEvent(run.ID, "stdout", 100, largeEvent("output.gap")); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("oversized gap event error = %v, want ErrEventTooLarge", err)
	}
	current, err := s.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Output.Stdout != baseline || current.LastOutputAt != nil {
		t.Fatalf("failed atomic write changed Run output/timestamp: %#v lastOutputAt=%v", current.Output, current.LastOutputAt)
	}
	data, retainedFrom, observed, gap, err := s.ReadOutput(run.ID, "stdout", 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "baseline" || retainedFrom != 0 || observed != 8 || gap {
		t.Fatalf("failed atomic write changed spool: %q retainedFrom=%d observed=%d gap=%v", data, retainedFrom, observed, gap)
	}
	events, eventRetainedFrom, eventGap, err := s.Events(run.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 || eventRetainedFrom != 1 || eventGap {
		t.Fatalf("failed atomic write changed journal: events=%#v retainedFrom=%d gap=%v", events, eventRetainedFrom, eventGap)
	}
}

func testRun(id string) model.Run {
	return model.Run{
		ID:         id,
		Spec:       model.RunSpec{Argv: []string{"true"}, Cwd: "/"},
		State:      model.Accepted,
		Generation: 1,
		CreatedAt:  time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC),
	}
}
