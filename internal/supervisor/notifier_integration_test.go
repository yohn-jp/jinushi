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
