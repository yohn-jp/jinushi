package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestSubmissionBindingIsAtomicDurableAndProtected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	now := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	run := model.Run{ID: "run_submission", State: model.Accepted, Generation: 1, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/true"}, Cwd: "/"}}
	first, err := s.AcceptSubmission(run, &model.Event{Kind: model.EventRunAccepted, ObservedAt: now}, "caller-1", testDigest('a'), now)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.Run.ID != run.ID || first.Event == nil {
		t.Fatalf("first acceptance = %+v", first)
	}

	duplicate, found, err := s.ResolveSubmission("caller-1", testDigest('a'), now.Add(time.Hour))
	if err != nil || !found || duplicate.ID != run.ID {
		t.Fatalf("ResolveSubmission = run=%+v found=%t err=%v", duplicate, found, err)
	}
	if _, err := s.AcceptSubmission(model.Run{ID: "run_duplicate", State: model.Accepted, Generation: 1, CreatedAt: now, Spec: run.Spec}, nil, "caller-1", testDigest('b'), now.Add(2*time.Hour)); !errors.Is(err, ErrSubmissionConflict) {
		t.Fatalf("conflicting submission error = %v, want ErrSubmissionConflict", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		protected, err := submissionProtectsRunTx(tx, run.ID, now.Add(24*time.Hour))
		if err != nil {
			return err
		}
		if !protected {
			t.Fatal("live submission did not protect Run from terminal GC")
		}
		protected, err = submissionProtectsRunTx(tx, run.ID, now.Add(SubmissionRetentionWindow))
		if err != nil {
			return err
		}
		if protected {
			t.Fatal("expired submission still protects Run")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	duplicate, found, err = s.ResolveSubmission("caller-1", testDigest('a'), now.Add(24*time.Hour))
	if err != nil || !found || duplicate.ID != run.ID {
		t.Fatalf("reopened ResolveSubmission = run=%+v found=%t err=%v", duplicate, found, err)
	}
}

func TestCollectedSubmissionRetainsCompactReplayTombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	now := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	finished := now.Add(time.Second)
	started := now.Add(-time.Second)
	exitCode := 4
	run := model.Run{
		ID: "run_collected", State: model.Terminal, Generation: 3, CreatedAt: now,
		StartedAt: &started, FinishedAt: &finished,
		Spec:      model.RunSpec{Argv: []string{"/bin/echo", "large details"}, Cwd: "/"},
		Resources: model.Resources{MemoryBytes: model.Metric{Status: "measured", Value: 99}},
		Receipt: &model.Receipt{
			Version: 1, RunID: "run_collected", Outcome: "exited", ExitCode: &exitCode,
			StartedAt: &started, FinishedAt: finished,
			Resources:     model.Resources{MemoryBytes: model.Metric{Status: "measured", Value: 99}},
			Output:        model.Output{HistoryComplete: true},
			EventFirstSeq: 1, EventLastSeq: 9, EventRetainedFrom: 1, EventHistoryComplete: true,
			Cleanup: "complete",
		},
	}
	if _, err := s.AcceptSubmission(run, nil, "collected-key", testDigest('d'), now); err != nil {
		t.Fatal(err)
	}
	stub := submissionRunTombstone(run)
	err = s.db.Update(func(tx *bolt.Tx) error {
		marked, err := markSubmissionCollectedTx(tx, run.ID, stub, now.Add(time.Hour))
		if err != nil {
			return err
		}
		if !marked {
			t.Fatal("active submission binding was not marked collected")
		}
		return tx.Bucket([]byte(runsBucketName)).Delete([]byte(run.ID))
	})
	if err != nil {
		t.Fatal(err)
	}
	resolved, found, err := s.ResolveSubmission("collected-key", testDigest('d'), now.Add(2*time.Hour))
	if err != nil || !found {
		t.Fatalf("collected retry found=%t err=%v", found, err)
	}
	if resolved.ID != run.ID || resolved.State != model.Terminal || resolved.Receipt == nil || resolved.Receipt.Outcome != "exited" || resolved.Receipt.ExitCode == nil || *resolved.Receipt.ExitCode != exitCode {
		t.Fatalf("tombstone lost physical terminal identity: %+v", resolved)
	}
	if !resolved.Receipt.EvidenceIncomplete || resolved.Receipt.EventHistoryComplete || resolved.Receipt.Output.HistoryComplete || resolved.Receipt.Resources.MemoryBytes.Status != "unavailable" || resolved.Output.HistoryComplete {
		t.Fatalf("tombstone did not state evidence loss explicitly: %+v", resolved)
	}
	if _, _, err := s.ResolveSubmission("collected-key", testDigest('e'), now.Add(2*time.Hour)); !errors.Is(err, ErrSubmissionConflict) {
		t.Fatalf("collected submission conflict error = %v", err)
	}
}

func TestExpiredSubmissionIdentityCanBeReused(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	first := model.Run{ID: "run_expired_1", State: model.Accepted, Generation: 1, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/true"}, Cwd: "/"}}
	if _, err := s.AcceptSubmission(first, nil, "reused-key", testDigest('a'), now); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.ResolveSubmission("reused-key", testDigest('b'), now.Add(SubmissionRetentionWindow)); err != nil || found {
		t.Fatalf("expired conflicting identity found=%t err=%v", found, err)
	}
	second := first
	second.ID = "run_expired_2"
	second.CreatedAt = now.Add(SubmissionRetentionWindow)
	accepted, err := s.AcceptSubmission(second, nil, "reused-key", testDigest('b'), second.CreatedAt)
	if err != nil || !accepted.Created || accepted.Run.ID != second.ID {
		t.Fatalf("reused submission identity result=%+v err=%v", accepted, err)
	}
}

func TestControlRequestClaimsSideEffectOnceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	now := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	run := model.Run{ID: "run_control", State: model.Running, Generation: 7, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "1"}, Cwd: "/"}}
	if _, _, err := s.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	first, err := s.BeginControlRequest(run.ID, "input-1", testDigest('a'), run.Generation, now)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || first.Run.Generation != run.Generation+1 || first.Request.Status != ControlRequestPending {
		t.Fatalf("first control claim = %+v", first)
	}
	if err := s.CompleteControlRequest(run.ID, "input-1", ControlRequestSucceeded, "", "", now); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	replay, found, err := s.FindControlRequest(run.ID, "input-1", testDigest('a'), now.Add(time.Minute))
	if err != nil || !found || replay.Status != ControlRequestSucceeded {
		t.Fatalf("reopened control replay = %+v found=%t err=%v", replay, found, err)
	}
	second, err := s.BeginControlRequest(run.ID, "input-1", testDigest('a'), run.Generation, now.Add(time.Minute))
	if err != nil || second.Created || second.Request.Status != ControlRequestSucceeded {
		t.Fatalf("duplicate BeginControlRequest = %+v err=%v", second, err)
	}
	if _, err := s.BeginControlRequest(run.ID, "input-1", testDigest('b'), run.Generation, now.Add(time.Minute)); !errors.Is(err, ErrControlRequestConflict) {
		t.Fatalf("mutated request ID error = %v, want ErrControlRequestConflict", err)
	}
	if _, err := s.BeginControlRequest(run.ID, "input-2", testDigest('b'), run.Generation, now.Add(time.Minute)); !errors.Is(err, ErrStaleControlGeneration) {
		t.Fatalf("stale control error = %v, want ErrStaleControlGeneration", err)
	}
	stored, err := s.Get(run.ID)
	if err != nil || stored.Generation != run.Generation+1 {
		t.Fatalf("generation after replay = %d err=%v", stored.Generation, err)
	}
}

func TestPendingControlRequestRemainsUncertain(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Now().UTC()
	run := model.Run{ID: "run_pending", State: model.Running, Generation: 1, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "1"}, Cwd: "/"}}
	if _, _, err := s.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginControlRequest(run.ID, "request-1", testDigest('c'), 1, now); err != nil {
		t.Fatal(err)
	}
	replay, found, err := s.FindControlRequest(run.ID, "request-1", testDigest('c'), now.Add(time.Second))
	if err != nil || !found || replay.Status != ControlRequestPending {
		t.Fatalf("pending replay = %+v found=%t err=%v", replay, found, err)
	}
	if _, err := s.BeginControlRequest(run.ID, "request-2", testDigest('d'), 1, now.Add(time.Second)); !errors.Is(err, ErrStaleControlGeneration) {
		t.Fatalf("stale generation after pending claim = %v", err)
	}
}

func TestExpiredControlRequestIdentityCanBeReused(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	now := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	run := model.Run{ID: "run_expired_control", State: model.Running, Generation: 4, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/sleep", "1"}, Cwd: "/"}}
	if _, _, err := s.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginControlRequest(run.ID, "reused-request", testDigest('a'), run.Generation, now); err != nil {
		t.Fatal(err)
	}
	expiredAt := now.Add(ControlRequestWindow)
	if _, found, err := s.FindControlRequest(run.ID, "reused-request", testDigest('b'), expiredAt); err != nil || found {
		t.Fatalf("expired conflicting request found=%t err=%v", found, err)
	}
	reused, err := s.BeginControlRequest(run.ID, "reused-request", testDigest('b'), run.Generation+1, expiredAt)
	if err != nil || !reused.Created || reused.Request.MutationDigest != testDigest('b') {
		t.Fatalf("reused control identity result=%+v err=%v", reused, err)
	}
}

func testDigest(ch byte) string {
	const hexChars = "0123456789abcdef"
	var digest [64]byte
	for i := range digest {
		digest[i] = hexChars[ch&0x0f]
	}
	return string(digest[:])
}
