package supervisor

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

func TestRetentionCollectionKeepsQueryableIncompleteTombstone(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	finished := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	run := model.Run{
		ID: "run-retention-supervisor", State: model.Accepted, Generation: 1,
		CreatedAt: finished.Add(-time.Minute),
		Spec:      model.RunSpec{Argv: []string{"/bin/true"}, Cwd: t.TempDir()},
	}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	run, err = db.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	run.State = model.Running
	run.Generation++
	if _, err := db.Update(run, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AppendEvent(run.ID, model.Event{Kind: model.EventRunRunning, ObservedAt: finished.Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}
	run.State = model.Terminal
	run.Generation++
	run.FinishedAt = &finished
	run.Receipt = &model.Receipt{
		Version: model.ProtocolVersion, RunID: run.ID, Outcome: "exited",
		FinishedAt: finished, EventHistoryComplete: true,
		Resources: run.Resources, Output: run.Output, Cleanup: "complete",
	}
	if _, err := db.Update(run, nil); err != nil {
		t.Fatal(err)
	}

	config := defaultConfig()
	config.Retention = RetentionConfig{
		MaxAgeMs: 0, MaxTerminalRuns: 0, MaxStateBytes: 1 << 20,
		PreserveTombstones: true, MaxTombstones: 8, MaxTombstoneAgeMs: 24 * 60 * 60 * 1000,
	}
	svc := newService(t.TempDir(), db, nil, config)
	result, err := svc.runRetentionPass(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedRuns != 1 || result.TerminalRunsRemaining != 0 {
		t.Fatalf("retention result = %+v", result)
	}

	for _, op := range []string{"inspect", "await"} {
		response := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: op, RunID: run.ID})
		if response.Error != nil || response.Run == nil || response.Tombstone == nil {
			t.Fatalf("%s response = %+v", op, response)
		}
		if response.Run.ID != run.ID || response.Run.State != model.Terminal ||
			response.Run.Spec.Argv != nil || response.Run.Receipt == nil || !response.Run.Receipt.EvidenceIncomplete ||
			response.Run.Receipt.EventHistoryComplete {
			t.Fatalf("%s did not return the compact incomplete Run tombstone: %+v", op, response.Run)
		}
		if response.Tombstone.RunID != run.ID || response.Tombstone.ReceiptSHA256 == "" || len(response.Tombstone.Reasons) != 1 {
			t.Fatalf("%s tombstone summary = %+v", op, response.Tombstone)
		}
	}

	for _, request := range []protocol.Request{
		{Version: model.ProtocolVersion, Op: "events", RunID: run.ID},
		{Version: model.ProtocolVersion, Op: "output", RunID: run.ID, Stream: "stdout"},
	} {
		response := svc.Handle(context.Background(), request)
		if response.Error == nil || response.Error.Code != "evidence-collected" || response.Tombstone == nil {
			t.Fatalf("%s after collection returned %+v", request.Op, response)
		}
	}

	status := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "status"})
	if status.Error != nil || status.Status == nil || status.Status.Store.Status != "available" ||
		status.Status.Store.RunCount != 0 || status.Status.Store.TombstoneCount != 1 ||
		status.Status.Retention.Status != "available" || status.Status.Retention.EvictedRunsTotal != 1 ||
		status.Status.Retention.CompactionStatus != "not-attempted" || status.Status.Retention.CompactionAttemptedAt != nil {
		t.Fatalf("runtime retention status = %+v", status)
	}
}

func TestCompactionFailureClassificationKeepsUncertaintyMachineReadable(t *testing.T) {
	for _, test := range []struct {
		err       error
		status    string
		code      string
		uncertain bool
	}{
		{store.ErrCompactionUnsupported, "unsupported", "compaction-unsupported", false},
		{store.ErrCompactionArtifactsPending, "blocked", "compaction-artifacts-pending", false},
		{store.ErrCompactionPathUnsafe, "blocked", "compaction-path-unsafe", false},
		{store.ErrCompactionUncertain, "uncertain", "compaction-uncertain", true},
	} {
		status, code, uncertain := compactionFailure(test.err)
		if status != test.status || code != test.code || uncertain != test.uncertain {
			t.Errorf("compactionFailure(%v) = (%q, %q, %t), want (%q, %q, %t)", test.err, status, code, uncertain, test.status, test.code, test.uncertain)
		}
	}

	svc := &Service{}
	attempted := time.Now().UTC()
	svc.recordCompaction("uncertain", "compaction-uncertain", attempted, true)
	state := svc.compactionSnapshot()
	if !state.uncertain || state.status != "uncertain" || state.errorCode != "compaction-uncertain" ||
		state.attemptedAt == nil || !state.attemptedAt.Equal(attempted) {
		t.Fatalf("uncertain compaction state was not preserved: %+v", state)
	}
}
