//go:build !windows

package supervisor

import (
	"context"
	"encoding/base64"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

func TestServiceNotifierWakesDurableEventAndOutputFollows(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := newService(root, db, nil, defaultConfig())
	now := time.Now().UTC()
	run := model.Run{
		ID: "run-subscription-test", State: model.Running, Generation: 1, CreatedAt: now,
		Spec: model.RunSpec{Argv: []string{"/bin/true"}, Cwd: root},
	}
	run, accepted, err := db.Create(run, &model.Event{Kind: model.EventRunAccepted, ObservedAt: now})
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	if accepted == nil {
		_ = s.Close()
		t.Fatal("Run acceptance event was not stored")
	}
	a := &active{run: run, spec: run.Spec, done: make(chan struct{})}
	capture := &capture{s: s, a: a, stream: "stdout"}

	listener, err := ipc.Listen(root)
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	serveCtx, stopServe := context.WithCancel(context.Background())
	queries := make(chan protocol.Request, 8)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- ipc.ServeWithNotifier(serveCtx, listener, func(ctx context.Context, request protocol.Request) protocol.Response {
			select {
			case queries <- request:
			default:
			}
			return s.Handle(ctx, request)
		}, s.notifier)
	}()
	t.Cleanup(func() {
		stopServe()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("ServeWithNotifier: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("ServeWithNotifier did not stop")
		}
		if err := s.Close(); err != nil && !errors.Is(err, errSupervisorClosed) {
			t.Errorf("Service.Close: %v", err)
		}
	})

	eventCtx, cancelEvent := context.WithCancel(context.Background())
	eventFrames := make(chan protocol.Response, 1)
	eventDone := make(chan error, 1)
	go func() {
		eventDone <- ipc.FollowEvents(eventCtx, root, protocol.Request{
			Op: "events", RunID: run.ID, After: accepted.Seq, Follow: true,
		}, func(response protocol.Response) error {
			if len(response.Events) > 0 {
				eventFrames <- response
				cancelEvent()
			}
			return nil
		})
	}()
	if request := waitNotifierQuery(t, queries, "events"); request.RunID != run.ID || request.After != accepted.Seq {
		t.Fatalf("event initial request = %#v", request)
	}
	if _, err := capture.WriteObserved([]byte("abc"), time.Now().UTC()); err != nil {
		t.Fatalf("persist output chunk: %v", err)
	}
	select {
	case frame := <-eventFrames:
		if len(frame.Events) != 1 || frame.Events[0].Kind != model.EventOutputChunk || frame.Events[0].Seq <= accepted.Seq {
			t.Fatalf("event follower frame = %#v", frame)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event follower did not wake after the durable event commit")
	}
	if err := <-eventDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("event follow exit = %v; want context.Canceled", err)
	}

	outputCtx, cancelOutput := context.WithCancel(context.Background())
	outputFrames := make(chan protocol.Response, 1)
	outputDone := make(chan error, 1)
	go func() {
		outputDone <- ipc.Follow(outputCtx, root, protocol.Request{
			Op: "output", RunID: run.ID, Stream: "stdout", Offset: 3, Follow: true,
		}, func(response protocol.Response) error {
			if response.Data != "" {
				outputFrames <- response
				cancelOutput()
			}
			return nil
		})
	}()
	if request := waitNotifierQuery(t, queries, "output"); request.RunID != run.ID || request.Offset != 3 {
		t.Fatalf("output initial request = %#v", request)
	}
	if _, err := capture.WriteObserved([]byte("def"), time.Now().UTC()); err != nil {
		t.Fatalf("persist second output chunk: %v", err)
	}
	select {
	case frame := <-outputFrames:
		data, err := base64.StdEncoding.DecodeString(frame.Data)
		if err != nil || string(data) != "def" {
			t.Fatalf("output follow data=%q err=%v", data, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("output follower did not wake after the durable output commit")
	}
	if err := <-outputDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("output follow exit = %v; want context.Canceled", err)
	}
}

func TestServiceWatchFollowReconnectsAcrossRetainedHistoryGap(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := newService(root, db, nil, defaultConfig())
	now := time.Now().UTC()
	run := model.Run{
		ID: "run-watch-gap", State: model.Running, Generation: 1, CreatedAt: now,
		Spec: model.RunSpec{Argv: []string{"/bin/true"}, Cwd: root},
	}
	run, _, err = db.Create(run, &model.Event{
		Kind: model.EventRunAccepted, ObservedAt: now,
		Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Accepted}},
	})
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	initial := s.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "watch", Limit: 1})
	if initial.Error != nil || len(initial.Events) != 1 || initial.WatchWatermark == "" || initial.WatchWatermark != initial.NextCursor || initial.WatchRetainedFrom != initial.NextCursor {
		_ = s.Close()
		t.Fatalf("initial watch watermark response = %+v", initial)
	}
	const eventCount = 4097
	events := make([]model.Event, eventCount)
	for i := range events {
		events[i] = model.Event{
			Kind: model.EventOutputGap, ObservedAt: now.Add(time.Duration(i+1) * time.Millisecond),
			Payload: &model.EventPayload{Output: &model.OutputEventPayload{Stream: "stdout", ObservedBytes: int64(i + 1)}},
		}
	}
	if _, err := db.UpdateWithEvents(run, events); err != nil {
		_ = s.Close()
		t.Fatalf("persist retained-history fixture: %v", err)
	}

	listener, err := ipc.Listen(root)
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	serveCtx, stopServe := context.WithCancel(context.Background())
	queries := make(chan protocol.Request, 8)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- ipc.ServeWithNotifier(serveCtx, listener, func(ctx context.Context, request protocol.Request) protocol.Response {
			select {
			case queries <- request:
			default:
			}
			return s.Handle(ctx, request)
		}, s.notifier)
	}()
	t.Cleanup(func() {
		stopServe()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("ServeWithNotifier: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("ServeWithNotifier did not stop")
		}
		if err := s.Close(); err != nil && !errors.Is(err, errSupervisorClosed) {
			t.Errorf("Service.Close: %v", err)
		}
	})

	gapCtx, cancelGap := context.WithCancel(context.Background())
	gapDone := make(chan error, 1)
	gapFrames := make(chan protocol.Response, 1)
	go func() {
		gapDone <- ipc.Follow(gapCtx, root, protocol.Request{Op: "watch", Cursor: initial.NextCursor, Follow: true}, func(response protocol.Response) error {
			if response.Gap {
				gapFrames <- response
				cancelGap()
			}
			return nil
		})
	}()
	if request := waitNotifierQuery(t, queries, "watch"); request.Cursor != initial.NextCursor {
		t.Fatalf("stale watch cursor request = %q, want %q", request.Cursor, initial.NextCursor)
	}
	var gap protocol.Response
	select {
	case gap = <-gapFrames:
	case <-time.After(3 * time.Second):
		t.Fatal("watch follow did not return an explicit retention gap")
	}
	if err := <-gapDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("gap Follow exit = %v; want context.Canceled", err)
	}
	if !gap.Gap || len(gap.Events) != 1 || gap.NextCursor != gap.WatchRetainedFrom || gap.WatchWatermark == "" || gap.WatchWatermark == gap.NextCursor {
		t.Fatalf("watch gap page = %+v", gap)
	}

	resumeCtx, cancelResume := context.WithCancel(context.Background())
	defer cancelResume()
	resumeDone := make(chan error, 1)
	resumeFrames := make(chan protocol.Response, 1)
	go func() {
		resumeDone <- ipc.Follow(resumeCtx, root, protocol.Request{Op: "watch", Cursor: gap.NextCursor, Follow: true}, func(response protocol.Response) error {
			if len(response.Events) > 0 {
				select {
				case resumeFrames <- response:
				default:
				}
				cancelResume()
			}
			return nil
		})
	}()
	if request := waitNotifierQuery(t, queries, "watch"); request.Cursor != gap.NextCursor {
		t.Fatalf("reconnected watch cursor = %q, want %q", request.Cursor, gap.NextCursor)
	}
	var resumed protocol.Response
	select {
	case resumed = <-resumeFrames:
	case <-time.After(3 * time.Second):
		t.Fatal("watch follow did not resume after the gap")
	}
	if err := <-resumeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("reconnected Follow exit = %v; want context.Canceled", err)
	}
	if resumed.Gap || len(resumed.Events) != 1 || resumed.NextCursor == gap.NextCursor || resumed.WatchWatermark != gap.WatchWatermark || resumed.WatchRetainedFrom != gap.WatchRetainedFrom {
		t.Fatalf("reconnected watch page = %+v", resumed)
	}
}

func waitNotifierQuery(t *testing.T, queries <-chan protocol.Request, op string) protocol.Request {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case request := <-queries:
			if request.Op == op {
				return request
			}
		case <-timer.C:
			t.Fatalf("%s follower did not read its initial durable snapshot", op)
			return protocol.Request{}
		}
	}
}
