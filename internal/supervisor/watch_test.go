package supervisor

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

func TestHandleWatchPagesAcrossRunsAndResumesWithOpaqueCursor(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := newService(t.TempDir(), db, nil, defaultConfig())
	for _, id := range []string{"run-a", "run-b", "run-c"} {
		run := model.Run{ID: id, State: model.Accepted}
		event := model.Event{
			Kind: model.EventRunAccepted,
			Payload: &model.EventPayload{Run: &model.RunEventPayload{
				State: model.Accepted,
			}},
		}
		if _, _, err := db.Create(run, &event); err != nil {
			t.Fatal(err)
		}
	}

	first := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "watch", Limit: 1})
	if first.Error != nil || len(first.Events) != 1 || first.Events[0].RunID != "run-a" || first.NextCursor == "" || first.Gap {
		t.Fatalf("first all-Run watch page = %+v", first)
	}
	second := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "watch", Cursor: first.NextCursor, Limit: 1})
	if second.Error != nil || len(second.Events) != 1 || second.Events[0].RunID != "run-b" || second.NextCursor == first.NextCursor || second.Gap {
		t.Fatalf("resumed all-Run watch page = %+v", second)
	}
	third := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "watch", Cursor: second.NextCursor, Limit: 1})
	if third.Error != nil || len(third.Events) != 1 || third.Events[0].RunID != "run-c" || third.NextCursor == second.NextCursor || third.Gap {
		t.Fatalf("third all-Run watch page = %+v", third)
	}
	empty := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "watch", Cursor: third.NextCursor, Limit: 1})
	if empty.Error != nil || len(empty.Events) != 0 || empty.NextCursor != third.NextCursor || empty.Gap {
		t.Fatalf("empty all-Run watch page = %+v", empty)
	}
}

func TestHandleWatchRejectsRunScopedAndInvalidCursors(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := newService(t.TempDir(), db, nil, defaultConfig())

	withRunID := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "watch", RunID: "run-a"})
	if withRunID.Error == nil || withRunID.Error.Code != "invalid-request" {
		t.Fatalf("watch with Run ID returned %+v", withRunID)
	}
	withCursor := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "watch", Cursor: "not-a-watch-cursor"})
	if withCursor.Error == nil || withCursor.Error.Code != "invalid-cursor" {
		t.Fatalf("watch with invalid cursor returned %+v", withCursor)
	}
}

func TestHandleWatchBoundsFrameAndAdvancesOnlyThroughReturnedEvents(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := newService(t.TempDir(), db, nil, defaultConfig())

	observedAt := time.Date(2026, time.September, 27, 12, 0, 0, 123456789, time.UTC)
	base := model.Event{
		Version:    model.EventSchemaVersion,
		RunID:      "run-0",
		Seq:        1,
		Kind:       model.EventRunAccepted,
		ObservedAt: observedAt,
		Body:       map[string]any{"pad": ""},
	}
	encodedBase, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	// Four maximum-size entries fit the store's 1 MiB page budget, while the
	// protocol response envelope pushes them beyond its frame safety margin.
	pageEventBytes := protocol.MaxFrame - 4096 + 512
	padding := strings.Repeat("x", pageEventBytes/4-len(encodedBase))
	for i := 0; i < 4; i++ {
		id := "run-" + string(rune('0'+i))
		run := model.Run{ID: id, State: model.Accepted}
		event := model.Event{
			Kind:       model.EventRunAccepted,
			ObservedAt: observedAt,
			Body:       map[string]any{"pad": padding},
		}
		if _, _, err := db.Create(run, &event); err != nil {
			t.Fatal(err)
		}
	}
	storePage, err := db.WatchPage("", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(storePage.Events) != 4 {
		t.Fatalf("store page contains %d events, want 4", len(storePage.Events))
	}

	response := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "watch", Limit: 4})
	if response.Error != nil || len(response.Events) != 3 || response.NextCursor != storePage.Events[2].Cursor {
		t.Fatalf("frame-bounded watch page = events %d cursor %q error %+v", len(response.Events), response.NextCursor, response.Error)
	}
	encodedResponse, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(encodedResponse) >= protocol.MaxFrame-4096 {
		t.Fatalf("watch response uses %d bytes, safety limit %d", len(encodedResponse), protocol.MaxFrame-4096)
	}
	next, err := db.WatchPage(response.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Events) != 1 || next.Events[0].Event.RunID != storePage.Events[3].Event.RunID {
		t.Fatalf("cursor after trimmed frame skipped or repeated event: %+v", next.Events)
	}
}
