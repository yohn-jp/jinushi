package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestWatchPageReconnectAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	s := openWatchTestStore(t, path)
	for i := 1; i <= 5; i++ {
		appendWatchTestEvent(t, s, watchTestEvent(i, fmt.Sprintf("run-%d", i)))
	}

	first, err := s.WatchPage("", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := watchRunIDs(first.Events); got != "run-1,run-2" || !first.HasMore || first.Gap {
		t.Fatalf("first page = IDs %q, hasMore %v, gap %v", got, first.HasMore, first.Gap)
	}
	if first.Events[0].Event.Version != model.EventSchemaVersion ||
		first.Events[0].Event.Payload == nil ||
		first.Events[0].Event.Payload.Run == nil ||
		first.Events[0].Event.Payload.Run.State != model.Running {
		t.Fatalf("typed event changed in watch index: %+v", first.Events[0].Event)
	}
	watermark := first.Watermark
	cursor := first.Cursor
	if cursor == "" || cursor == watermark {
		t.Fatalf("first page cursor/watermark = %q/%q, want an intermediate continuation", cursor, watermark)
	}

	second, err := s.WatchPage(cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := watchRunIDs(second.Events); got != "run-3,run-4" || !second.HasMore || second.Gap {
		t.Fatalf("second page = IDs %q, hasMore %v, gap %v", got, second.HasMore, second.Gap)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openWatchTestStore(t, path)
	t.Cleanup(func() { _ = s.Close() })
	third, err := s.WatchPage(second.Cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := watchRunIDs(third.Events); got != "run-5" || third.HasMore || third.Gap {
		t.Fatalf("post-restart page = IDs %q, hasMore %v, gap %v", got, third.HasMore, third.Gap)
	}
	if third.Watermark != watermark {
		t.Fatalf("watermark changed across restart: got %q, want %q", third.Watermark, watermark)
	}
}

func TestStoreEventAppendAtomicallyIndexesWatchEvent(t *testing.T) {
	s := openWatchTestStore(t, filepath.Join(t.TempDir(), "store.db"))
	t.Cleanup(func() { _ = s.Close() })
	for _, id := range []string{"run-a", "run-b"} {
		run := model.Run{ID: id, State: model.Accepted}
		event := model.Event{
			Kind: model.EventRunAccepted,
			Payload: &model.EventPayload{Run: &model.RunEventPayload{
				State: model.Accepted,
			}},
		}
		if _, appended, err := s.Create(run, &event); err != nil || appended == nil {
			t.Fatalf("create %s: event=%+v err=%v", id, appended, err)
		}
	}
	page, err := s.WatchPage("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 || page.Events[0].Event.RunID != "run-a" || page.Events[1].Event.RunID != "run-b" {
		t.Fatalf("durably indexed events = %v", watchRunIDs(page.Events))
	}
	if page.Events[0].Event.Seq != 1 || page.Events[1].Event.Seq != 1 {
		t.Fatalf("per-Run sequences changed in global index: %d, %d", page.Events[0].Event.Seq, page.Events[1].Event.Seq)
	}
}

func TestWatchIndexFailureRollsBackPerRunJournalAndRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	options := Options{MaxEventBytes: 300 << 10, EventRetentionBytes: 300 << 10}
	s, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	event := model.Event{
		Kind: model.EventRunAccepted,
		Body: map[string]any{"boundedFixture": strings.Repeat("x", 270<<10)},
	}
	if _, _, err := s.Create(model.Run{ID: "run-rollback", State: model.Accepted}, &event); err == nil {
		t.Fatal("oversized watch event was accepted")
	}
	if _, err := s.Get("run-rollback"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("Run after failed atomic append: %v", err)
	}
	page, err := s.WatchPage("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 || page.Watermark != "" {
		t.Fatalf("watch index retained rolled-back event: %+v", page)
	}
}

func TestWatchPageReportsEvictionGap(t *testing.T) {
	s := openWatchTestStore(t, filepath.Join(t.TempDir(), "store.db"))
	t.Cleanup(func() { _ = s.Close() })
	var firstCursor string
	err := s.db.Update(func(tx *bolt.Tx) error {
		for i := 1; i <= watchRetentionCount+1; i++ {
			if err := appendWatchEventTx(tx, watchTestEvent(i, fmt.Sprintf("run-%d", i))); err != nil {
				return err
			}
			if i == 1 {
				meta, err := readWatchMeta(tx.Bucket([]byte(watchBucketName)))
				if err != nil {
					return err
				}
				firstCursor = encodeWatchCursor(meta.StoreID, 1)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	page, err := s.WatchPage("", 3)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Gap || len(page.Events) != 3 || page.Events[0].Event.RunID != "run-2" {
		t.Fatalf("oldest page = first Run %q, events %d, gap %v", firstRunID(page.Events), len(page.Events), page.Gap)
	}
	if page.RetainedFrom != page.Events[0].Cursor {
		t.Fatalf("retained-from cursor %q does not identify first event %q", page.RetainedFrom, page.Events[0].Cursor)
	}
	resumed, err := s.WatchPage(firstCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Gap || len(resumed.Events) != 1 || resumed.Events[0].Event.RunID != "run-2" {
		t.Fatalf("resume after consumed oldest event = first Run %q, events %d, gap %v", firstRunID(resumed.Events), len(resumed.Events), resumed.Gap)
	}
}

func TestWatchPageConcurrentTransactionsHaveOneDurableOrder(t *testing.T) {
	s := openWatchTestStore(t, filepath.Join(t.TempDir(), "store.db"))
	t.Cleanup(func() { _ = s.Close() })
	const groups = 12
	const perGroup = 20
	var wg sync.WaitGroup
	errCh := make(chan error, groups)
	for group := 0; group < groups; group++ {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.db.Update(func(tx *bolt.Tx) error {
				for i := 1; i <= perGroup; i++ {
					if err := appendWatchEventTx(tx, watchTestEvent(i, fmt.Sprintf("run-%02d", group))); err != nil {
						return err
					}
				}
				return nil
			})
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	var entries []WatchEntry
	after := ""
	for {
		page, err := s.WatchPage(after, 10000)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Events) == 0 && page.HasMore {
			t.Fatal("empty page cannot make progress")
		}
		entries = append(entries, page.Events...)
		after = page.Cursor
		if !page.HasMore {
			break
		}
	}
	if len(entries) != groups*perGroup {
		t.Fatalf("indexed %d events, want %d", len(entries), groups*perGroup)
	}
	seen := make(map[string]int, groups)
	closedGroups := make(map[string]bool, groups)
	lastGroup := ""
	for i, entry := range entries {
		if i > 0 && watchCursorSeq(t, entry.Cursor) <= watchCursorSeq(t, entries[i-1].Cursor) {
			t.Fatalf("cursor order is not increasing at entry %d", i)
		}
		group := entry.Event.RunID
		if group != lastGroup {
			if closedGroups[group] {
				t.Fatalf("transaction group %s was interleaved in global order", group)
			}
			if lastGroup != "" {
				closedGroups[lastGroup] = true
			}
			lastGroup = group
		}
		seen[group]++
	}
	if len(seen) != groups {
		t.Fatalf("observed %d transaction groups, want %d", len(seen), groups)
	}
	for group, count := range seen {
		if count != perGroup {
			t.Errorf("group %s has %d events, want %d", group, count, perGroup)
		}
	}
}

func TestWatchIndexRetentionAndPageHonorByteBounds(t *testing.T) {
	s := openWatchTestStore(t, filepath.Join(t.TempDir(), "store.db"))
	t.Cleanup(func() { _ = s.Close() })
	const count = 80
	padding := strings.Repeat("x", 210<<10)
	err := s.db.Update(func(tx *bolt.Tx) error {
		for i := 1; i <= count; i++ {
			event := watchTestEvent(i, fmt.Sprintf("run-%d", i))
			event.Body = map[string]any{"boundedFixture": padding}
			if err := appendWatchEventTx(tx, event); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var meta watchMeta
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		meta, err = readWatchMeta(tx.Bucket([]byte(watchBucketName)))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if meta.Bytes > watchRetentionBytes || meta.Count >= count || meta.Count == 0 {
		t.Fatalf("watch retention count/bytes = %d/%d, want positive count below %d and bytes <= %d", meta.Count, meta.Bytes, count, watchRetentionBytes)
	}
	page, err := s.WatchPage("", watchMaxPageRecords)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Gap || !page.HasMore || len(page.Events) == 0 {
		t.Fatalf("bounded byte page has %d entries, gap %v, hasMore %v", len(page.Events), page.Gap, page.HasMore)
	}
	var pageBytes int
	for _, entry := range page.Events {
		encoded, err := json.Marshal(entry.Event)
		if err != nil {
			t.Fatal(err)
		}
		pageBytes += len(encoded)
	}
	if pageBytes > watchMaxPageBytes {
		t.Fatalf("page returned %d event bytes, maximum %d", pageBytes, watchMaxPageBytes)
	}
}

func TestWatchCursorIsStoreBoundAndPageIsBounded(t *testing.T) {
	pathA := filepath.Join(t.TempDir(), "a.db")
	a := openWatchTestStore(t, pathA)
	t.Cleanup(func() { _ = a.Close() })
	pathB := filepath.Join(t.TempDir(), "b.db")
	b := openWatchTestStore(t, pathB)
	t.Cleanup(func() { _ = b.Close() })
	for i := 1; i <= watchMaxPageRecords+1; i++ {
		appendWatchTestEvent(t, a, watchTestEvent(i, fmt.Sprintf("run-%d", i)))
	}
	page, err := a.WatchPage("", 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != watchMaxPageRecords || !page.HasMore {
		t.Fatalf("bounded page returned %d events with hasMore=%v", len(page.Events), page.HasMore)
	}
	if _, err := b.WatchPage(page.Cursor, 1); !errors.Is(err, ErrInvalidWatchCursor) {
		t.Fatalf("foreign-store cursor error = %v, want ErrInvalidWatchCursor", err)
	}
	if _, err := a.WatchPage("invalid", 1); !errors.Is(err, ErrInvalidWatchCursor) {
		t.Fatalf("malformed cursor error = %v, want ErrInvalidWatchCursor", err)
	}
}

func openWatchTestStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func appendWatchTestEvent(t *testing.T, s *Store, event model.Event) {
	t.Helper()
	if err := s.db.Update(func(tx *bolt.Tx) error { return appendWatchEventTx(tx, event) }); err != nil {
		t.Fatal(err)
	}
}

func watchTestEvent(seq int, runID string) model.Event {
	return model.Event{
		Version:    model.EventSchemaVersion,
		RunID:      runID,
		Seq:        uint64(seq),
		Kind:       model.EventRunRunning,
		ObservedAt: time.Unix(int64(seq), 0).UTC(),
		Payload:    &model.EventPayload{Run: &model.RunEventPayload{State: model.Running}},
	}
}

func watchRunIDs(entries []WatchEntry) string {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.Event.RunID)
	}
	return strings.Join(ids, ",")
}

func firstRunID(entries []WatchEntry) string {
	if len(entries) == 0 {
		return ""
	}
	return entries[0].Event.RunID
}

func watchCursorSeq(t *testing.T, cursor string) uint64 {
	t.Helper()
	decoded, err := decodeWatchCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	return decoded.seq
}
