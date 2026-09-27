package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	if err := s.db.Update(func(tx *bolt.Tx) error {
		telemetry, err := tx.CreateBucketIfNotExists([]byte("telemetry"))
		if err != nil {
			return err
		}
		root, err := telemetry.CreateBucketIfNotExists([]byte("run-retention-evidence"))
		if err != nil {
			return err
		}
		raw, err := root.CreateBucketIfNotExists([]byte("raw"))
		if err != nil {
			return err
		}
		return raw.Put([]byte("sample"), []byte("telemetry evidence"))
	}); err != nil {
		t.Fatal(err)
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
	if err := s.db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte("telemetry")).Bucket([]byte("run-retention-evidence")); bucket != nil {
			t.Fatal("GC left telemetry bucket for evicted Run")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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

func TestCollectTerminalPreservesUnexpiredSubmissionBindings(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	makeTerminalForRetention(t, s, "run-bound-active", now.Add(-time.Hour))
	makeTerminalForRetention(t, s, "run-bound-expired", now.Add(-2*time.Hour))
	if err := s.db.Update(func(tx *bolt.Tx) error {
		submissions, err := tx.CreateBucketIfNotExists([]byte("submissions"))
		if err != nil {
			return err
		}
		indices, err := tx.CreateBucketIfNotExists([]byte("submissionRuns"))
		if err != nil {
			return err
		}
		for _, binding := range []struct {
			id      string
			runID   string
			expires time.Time
		}{
			{id: "submission-active", runID: "run-bound-active", expires: now.Add(time.Hour)},
			{id: "submission-expired", runID: "run-bound-expired", expires: now.Add(-time.Second)},
		} {
			record := submissionRetentionRecord{RunID: binding.runID, ExpiresAt: binding.expires}
			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := submissions.Put([]byte(binding.id), encoded); err != nil {
				return err
			}
			if err := indices.Put([]byte(binding.runID), []byte(binding.id)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	policy := RetentionPolicy{MaxAge: 0, MaxTerminalRuns: 0, MaxStateBytes: 0, MaxTombstones: 0}
	result, err := s.CollectTerminal(policy, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.EvictedRuns != 1 || result.ProtectedTerminalRuns != 1 || result.ExpiredSubmissionBindings != 1 || !result.BudgetExceeded {
		t.Fatalf("bound submission retention result = %#v", result)
	}
	if _, err := s.Get("run-bound-active"); err != nil {
		t.Fatalf("active binding Run was collected: %v", err)
	}
	if _, err := s.Get("run-bound-expired"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("expired binding Run Get error = %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("submissions")).Get([]byte("submission-active")) == nil {
			t.Fatal("GC deleted unexpired idempotency binding")
		}
		if tx.Bucket([]byte("submissions")).Get([]byte("submission-expired")) != nil ||
			tx.Bucket([]byte("submissionRuns")).Get([]byte("run-bound-expired")) != nil {
			t.Fatal("GC retained expired idempotency binding/index")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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
	run, err = s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	run.State = model.Terminal
	run.Generation++
	finished = finished.UTC()
	run.FinishedAt = &finished
	run.Receipt = &model.Receipt{
		Version:    model.ProtocolVersion,
		RunID:      id,
		Outcome:    "exited",
		FinishedAt: finished,
		Resources:  model.Resources{},
		Output:     run.Output,
		Cleanup:    "complete",
	}
	if _, err := s.Update(run, nil); err != nil {
		t.Fatal(err)
	}
	return run
}
