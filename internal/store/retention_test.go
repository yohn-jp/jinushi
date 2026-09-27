package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestCollectTerminalBoundsCountAndLeavesExplicitTombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	oldFinished := now.Add(-time.Hour)
	newFinished := now.Add(-time.Minute)
	makeTerminalForRetention(t, s, "run-retention-old", oldFinished)
	makeTerminalForRetention(t, s, "run-retention-new", newFinished)
	if _, _, err := s.Create(testRun("run-retention-live"), nil); err != nil {
		t.Fatal(err)
	}

	policy := RetentionPolicy{
		MaxAge: 0, MaxTerminalRuns: 1, MaxStateBytes: 1 << 30,
		PreserveTombstones: true, MaxTombstones: 10,
		MaxTombstoneAge: 24 * time.Hour,
	}
	result, err := s.CollectTerminal(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedRuns != 1 || result.TerminalRunsRemaining != 1 || result.BudgetExceeded {
		t.Fatalf("GC result = %#v", result)
	}
	if _, err := s.Get("run-retention-old"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("evicted Run Get error = %v, want ErrRunNotFound", err)
	}
	if _, err := s.Get("run-retention-new"); err != nil {
		t.Fatalf("newest terminal Run was evicted: %v", err)
	}
	live, err := s.Get("run-retention-live")
	if err != nil || live.State != model.Accepted {
		t.Fatalf("live Run after GC = %#v, err=%v", live, err)
	}
	tombstone, err := s.GetTombstone("run-retention-old")
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.Outcome != "exited" || !tombstone.EvidenceIncomplete || tombstone.ReceiptSHA256 == "" ||
		len(tombstone.Reasons) != 1 || tombstone.Reasons[0] != EvictionTerminalCount {
		t.Fatalf("eviction tombstone = %#v", tombstone)
	}
	if tombstone.Receipt.Outcome != "exited" || !tombstone.Receipt.EvidenceIncomplete ||
		tombstone.Receipt.EventHistoryComplete || tombstone.Receipt.Output.HistoryComplete ||
		tombstone.Receipt.Resources.MemoryBytes.Status != "unavailable" ||
		tombstone.Receipt.Resources.TaskCount.Status != "unsupported" {
		t.Fatalf("compact tombstone receipt = %#v", tombstone.Receipt)
	}
	snapshot := tombstone.RunSnapshot()
	if snapshot.ID != tombstone.RunID || snapshot.State != model.Terminal || snapshot.Receipt == nil || snapshot.Spec.Argv != nil || snapshot.Ownership != nil {
		t.Fatalf("tombstone Run snapshot = %#v", snapshot)
	}
	if _, err := s.GetTombstone("run-retention-new"); !errors.Is(err, ErrTombstoneNotFound) {
		t.Fatalf("un-evicted Run tombstone error = %v", err)
	}
	checkpoint, err := s.RetentionState()
	if err != nil || checkpoint.EvictedRunsTotal != 1 || checkpoint.LastEvictedRunID != "run-retention-old" || checkpoint.BudgetExceeded {
		t.Fatalf("retention checkpoint = %#v, err=%v", checkpoint, err)
	}
}

func TestCollectTerminalDeletesAllPerRunEvidenceAtomically(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	makeTerminalForRetention(t, s, "run-retention-evidence", now.Add(-time.Hour))
	if _, err := s.AppendTelemetry("run-retention-evidence", testTelemetrySample(now, 1024, 42)); err != nil {
		t.Fatal(err)
	}
	beforeTelemetry, err := s.TelemetryStoreUsage()
	if err != nil || beforeTelemetry.Runs != 1 || beforeTelemetry.Bytes == 0 {
		t.Fatalf("telemetry before collection = %#v, err=%v", beforeTelemetry, err)
	}
	policy := RetentionPolicy{MaxAge: time.Minute, MaxTerminalRuns: 100, MaxStateBytes: 1 << 30,
		PreserveTombstones: true, MaxTombstones: 10, MaxTombstoneAge: 24 * time.Hour}
	result, err := s.CollectTerminal(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedRuns != 1 || result.EvidenceBytesRemoved == 0 {
		t.Fatalf("GC result = %#v", result)
	}
	if _, _, _, err := s.Events("run-retention-evidence", 0, 10); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("events after collection = %v, want ErrRunNotFound", err)
	}
	if _, _, _, _, err := s.ReadOutput("run-retention-evidence", "stdout", 0, 100); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("output after collection = %v, want ErrRunNotFound", err)
	}
	afterTelemetry, err := s.TelemetryStoreUsage()
	if err != nil || afterTelemetry.Runs != 0 || afterTelemetry.Bytes != 0 {
		t.Fatalf("telemetry after collection = %#v, err=%v", afterTelemetry, err)
	}
}

func TestRetentionAgeZeroDisablesAgeTriggerAndByteBudgetEvicts(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	makeTerminalForRetention(t, s, "run-retention-age", now.Add(-365*24*time.Hour))
	zeroAge := RetentionPolicy{MaxAge: 0, MaxTerminalRuns: 10, MaxStateBytes: 1 << 30, MaxTombstones: 0}
	if result, err := s.CollectTerminal(zeroAge, now); err != nil || result.EvictedRuns != 0 {
		t.Fatalf("MaxAge=0 collection = %#v, err=%v; zero disables age eviction", result, err)
	}
	byteLimit := RetentionPolicy{MaxAge: 0, MaxTerminalRuns: 10, MaxStateBytes: 0, MaxTombstones: 0}
	result, err := s.CollectTerminal(byteLimit, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedRuns != 1 || result.TerminalEvidenceBytes != 0 || result.BudgetExceeded {
		t.Fatalf("zero-byte collection = %#v", result)
	}
	if _, err := s.GetTombstone("run-retention-age"); !errors.Is(err, ErrTombstoneNotFound) {
		t.Fatalf("non-preserved tombstone lookup error = %v", err)
	}
}

func TestCollectTerminalRetainsRetryIdentityAndPrunesExpiredSubmissionBinding(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	digest := strings.Repeat("a", 64)
	makeSubmissionBoundTerminalForRetention(t, s, "run-bound-active", "submission-active", digest,
		now.Add(-time.Hour), now.Add(-time.Minute))
	makeSubmissionBoundTerminalForRetention(t, s, "run-bound-expired", "submission-expired", strings.Repeat("b", 64),
		now.Add(-31*24*time.Hour), now.Add(-31*24*time.Hour+time.Hour))
	policy := RetentionPolicy{MaxAge: 0, MaxTerminalRuns: 0, MaxStateBytes: 0, MaxTombstones: 0}
	result, err := s.CollectTerminal(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedRuns != 2 || result.SubmissionReplayStubs != 1 || result.ExpiredSubmissionBindings != 1 || result.BudgetExceeded {
		t.Fatalf("bound submission retention result = %#v", result)
	}
	if _, err := s.Get("run-bound-active"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("active binding Run detail remains: %v", err)
	}
	if _, err := s.Get("run-bound-expired"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("expired binding Run Get error = %v", err)
	}
	stub, found, err := s.ResolveSubmission("submission-active", digest, now)
	if err != nil || !found || stub.ID != "run-bound-active" || stub.State != model.Terminal ||
		stub.Receipt == nil || stub.Receipt.Outcome != "exited" || !stub.Receipt.EvidenceIncomplete ||
		stub.Receipt.EventHistoryComplete || stub.Receipt.Output.HistoryComplete ||
		stub.Receipt.EffectiveCapabilities == nil || stub.Receipt.EffectiveCapabilities.Backend != "retention-fixture" ||
		stub.Spec.Argv != nil || stub.Ownership != nil || stub.Receipt.Resources.MemoryBytes.Status != "unavailable" ||
		stub.Receipt.Resources.MemoryBytes.Value != 0 || stub.Receipt.Resources.TaskCount.Status != "unsupported" {
		t.Fatalf("resolved collected submission = %#v, found=%v, err=%v", stub, found, err)
	}
	retry, err := s.AcceptSubmission(testRun("run-submission-duplicate"), nil, "submission-active", digest, now.Add(time.Second))
	if err != nil || retry.Created || retry.Run.ID != "run-bound-active" {
		t.Fatalf("same submission retry = %#v, err=%v; it must resolve without creating a Run", retry, err)
	}
	if _, found, err := s.ResolveSubmission("submission-expired", strings.Repeat("b", 64), now); err != nil || found {
		t.Fatalf("expired submission resolve = found %v, err %v", found, err)
	}
	usage, err := s.Usage()
	if err != nil || usage.SubmissionReplayStubBytes <= 0 || usage.SubmissionBytes <= usage.SubmissionReplayStubBytes {
		t.Fatalf("submission replay stub usage = %#v, err=%v", usage, err)
	}
}

func TestCollectTerminalRejectsCorruptSubmissionExpiryIndexAtomically(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	acceptedAt := now.Add(-time.Hour)
	const submissionID = "submission-missing-expiry-index"
	makeSubmissionBoundTerminalForRetention(t, s, "run-missing-expiry-index", submissionID,
		strings.Repeat("d", 64), acceptedAt, now.Add(-time.Minute))
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(submissionExpiryBucketName)).Delete(
			expiryIndexKey(acceptedAt.Add(SubmissionRetentionWindow), []byte(submissionID)))
	}); err != nil {
		t.Fatal(err)
	}
	policy := RetentionPolicy{MaxAge: 0, MaxTerminalRuns: 0, MaxStateBytes: 0, MaxTombstones: 0}
	if result, err := s.CollectTerminal(policy, now); err == nil || result.EvictedRuns != 0 {
		t.Fatalf("collection with corrupt idempotency index = %#v, err=%v; want atomic failure", result, err)
	}
	run, err := s.Get("run-missing-expiry-index")
	if err != nil || run.State != model.Terminal || run.Receipt == nil {
		t.Fatalf("terminal Run after rejected collection = %#v, err=%v", run, err)
	}
	if _, err := s.GetTombstone(run.ID); !errors.Is(err, ErrTombstoneNotFound) {
		t.Fatalf("tombstone after rejected collection = %v", err)
	}
}

func TestCollectTerminalPreservesControlRequestsAndWatchIndex(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	run, _, err := s.Create(testRun("run-global-retention-index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	control, err := s.BeginControlRequest(run.ID, "request-retention", strings.Repeat("c", 64), run.Generation, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteControlRequest(run.ID, "request-retention", ControlRequestSucceeded, "", "", now); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		watchIndex, err := tx.CreateBucketIfNotExists([]byte("watch-index"))
		if err != nil {
			return err
		}
		return watchIndex.Put([]byte("fixture-watch-record"), []byte(control.Request.RunID))
	}); err != nil {
		t.Fatal(err)
	}
	finishRunningRunForRetention(t, s, control.Run, now.Add(-time.Minute))
	policy := RetentionPolicy{MaxAge: 0, MaxTerminalRuns: 0, MaxStateBytes: 0, MaxTombstones: 0}
	result, err := s.CollectTerminal(policy, now)
	if err != nil || result.EvictedRuns != 1 {
		t.Fatalf("terminal GC = %#v, err=%v", result, err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(controlRequestsBucketName)).Get(controlRequestKey(run.ID, "request-retention")) == nil {
			t.Fatal("terminal GC removed retained control request")
		}
		if got := string(tx.Bucket([]byte("watch-index")).Get([]byte("fixture-watch-record"))); got != run.ID {
			t.Fatalf("terminal GC changed global watch index record: %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	usage, err := s.Usage()
	if err != nil || usage.ControlRequestBytes == 0 || usage.WatchIndexBytes == 0 {
		t.Fatalf("global index usage = %#v, err=%v", usage, err)
	}
}

func TestTombstoneRetentionIsBoundedByCountAndAge(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	for _, id := range []string{"run-tomb-a", "run-tomb-b", "run-tomb-c"} {
		makeTerminalForRetention(t, s, id, now.Add(-time.Hour))
	}
	policy := RetentionPolicy{MaxAge: 0, MaxTerminalRuns: 0, MaxStateBytes: 1 << 30,
		PreserveTombstones: true, MaxTombstones: 1, MaxTombstoneAge: time.Hour}
	result, err := s.CollectTerminal(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedRuns != 3 || result.RemovedTombstones != 2 {
		t.Fatalf("tombstone count GC = %#v", result)
	}
	if _, err := s.GetTombstone("run-tomb-a"); !errors.Is(err, ErrTombstoneNotFound) {
		t.Fatalf("oldest tombstone should be dropped by count: %v", err)
	}
	if _, err := s.GetTombstone("run-tomb-c"); err != nil {
		t.Fatalf("newest tombstone missing: %v", err)
	}
	policy.MaxTombstones = 3
	policy.MaxTombstoneAge = time.Hour
	result, err = s.CollectTerminal(policy, now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.RemovedTombstones != 1 {
		t.Fatalf("tombstone age cleanup = %#v", result)
	}
}

func TestUsageReportsLogicalAndPhysicalStoreUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	makeTerminalForRetention(t, s, "run-usage-terminal", time.Now().Add(-time.Hour))
	if _, _, err := s.Create(testRun("run-usage-live"), nil); err != nil {
		t.Fatal(err)
	}
	policy := RetentionPolicy{MaxAge: time.Minute, MaxTerminalRuns: 10, MaxStateBytes: 1 << 30,
		PreserveTombstones: true, MaxTombstones: 10, MaxTombstoneAge: 24 * time.Hour}
	if _, err := s.CollectTerminal(policy, time.Now()); err != nil {
		t.Fatal(err)
	}
	usage, err := s.Usage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.RunCount != 1 || usage.ActiveRunCount != 1 || usage.TerminalRunCount != 0 || usage.TombstoneCount != 1 ||
		usage.LogicalBytes <= 0 || usage.TombstoneBytes <= 0 || usage.TerminalEvidenceBytes != usage.TombstoneBytes ||
		usage.DatabaseBytes <= 0 || usage.PageSize <= 0 || usage.FreeBytes < 0 {
		t.Fatalf("store usage = %#v", usage)
	}
}

func TestConcurrentRetentionPassesAndUsageReads(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	makeTerminalForRetention(t, s, "run-retention-concurrent", now.Add(-time.Hour))
	policy := RetentionPolicy{MaxAge: 0, MaxTerminalRuns: 0, MaxStateBytes: 0, MaxTombstones: 0}
	var workers sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			for n := 0; n < 20; n++ {
				if id%2 == 0 {
					if _, err := s.CollectTerminal(policy, now); err != nil {
						errCh <- err
						return
					}
					continue
				}
				if _, err := s.Usage(); err != nil {
					errCh <- err
					return
				}
				if _, err := s.RetentionState(); err != nil {
					errCh <- err
					return
				}
			}
		}(i)
	}
	workers.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	state, err := s.RetentionState()
	if err != nil || state.EvictedRunsTotal != 1 {
		t.Fatalf("concurrent retention checkpoint = %#v, err=%v", state, err)
	}
}

func TestStoreCompactionSnapshotAndClosedDatabaseCompaction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte("x"), 64<<10)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		fixture, err := tx.CreateBucketIfNotExists([]byte("compact-fixture"))
		if err != nil {
			return err
		}
		if err := fixture.Put([]byte("keep"), []byte("value")); err != nil {
			return err
		}
		for i := 0; i < 128; i++ {
			key := []byte(fmt.Sprintf("discard-%03d", i))
			if err := fixture.Put(key, large); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		fixture := tx.Bucket([]byte("compact-fixture"))
		for i := 0; i < 128; i++ {
			if err := fixture.Delete([]byte(fmt.Sprintf("discard-%03d", i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	needed, err := s.CompactionNeeded(RetentionPolicy{CompactMinFreeBytes: 1})
	if err != nil || !needed {
		t.Fatalf("compaction-needed = %v, err=%v", needed, err)
	}
	destination := filepath.Join(dir, "state.compacted.db")
	if err := s.CompactTo(destination); err != nil {
		t.Fatal(err)
	}
	destinationInfo, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if destinationInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("compacted snapshot size %d did not shrink source size %d", destinationInfo.Size(), beforeInfo.Size())
	}
	copyDB, err := bolt.Open(destination, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyDB.View(func(tx *bolt.Tx) error {
		if got := string(tx.Bucket([]byte("compact-fixture")).Get([]byte("keep"))); got != "value" {
			t.Fatalf("compacted snapshot lost retained data: %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := copyDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := CompactDatabase(path); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.db.View(func(tx *bolt.Tx) error {
		if got := string(tx.Bucket([]byte("compact-fixture")).Get([]byte("keep"))); got != "value" {
			t.Fatalf("offline compaction lost retained data: %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func makeTerminalForRetention(t *testing.T, s *Store, id string, finished time.Time) model.Run {
	t.Helper()
	run, _, err := s.Create(testRun(id), nil)
	if err != nil {
		t.Fatal(err)
	}
	return finishRunningRunForRetention(t, s, run, finished)
}

func makeSubmissionBoundTerminalForRetention(t *testing.T, s *Store, id, submissionID, digest string, acceptedAt, finished time.Time) model.Run {
	t.Helper()
	accepted, err := s.AcceptSubmission(testRun(id), nil, submissionID, digest, acceptedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !accepted.Created || accepted.Run.ID != id {
		t.Fatalf("initial accepted submission = %#v", accepted)
	}
	return finishRunningRunForRetention(t, s, accepted.Run, finished)
}

func finishRunningRunForRetention(t *testing.T, s *Store, run model.Run, finished time.Time) model.Run {
	t.Helper()
	id := run.ID
	run.State = model.Running
	run.Generation++
	if _, err := s.Update(run, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendEvent(id, model.Event{Kind: "retention.test-evidence"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendOutput(id, "stdout", []byte("bounded output evidence"), 1024); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	run = stored
	run.State = model.Terminal
	run.Generation++
	finished = finished.UTC()
	run.FinishedAt = &finished
	caps := model.Capabilities{Backend: "retention-fixture", Signals: []string{"TERM"}}
	run.EffectiveCapabilities = &caps
	run.Resources = model.Resources{
		MemoryBytes: model.Metric{Status: "measured", Value: 1024},
		TaskCount:   model.Metric{Status: "unsupported"},
	}
	run.Receipt = &model.Receipt{
		Version: model.ProtocolVersion, RunID: id, Outcome: "exited", FinishedAt: finished,
		Resources: run.Resources, Output: run.Output, Cleanup: "complete",
		EffectiveCapabilities: &caps, Capabilities: caps,
	}
	if _, err := s.Update(run, nil); err != nil {
		t.Fatal(err)
	}
	return run
}
