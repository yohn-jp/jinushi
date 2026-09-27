//go:build !windows

package ipc

import (
	"context"
	"encoding/base64"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

type testNotifier struct {
	subscribed chan protocol.Request
	wake       chan struct{}
	closedCh   chan struct{}
	err        error
	registered atomic.Bool
	closeOnce  sync.Once
	waits      atomic.Int32
}

func newTestNotifier() *testNotifier {
	return &testNotifier{subscribed: make(chan protocol.Request, 1), wake: make(chan struct{}, 1), closedCh: make(chan struct{})}
}

func (n *testNotifier) Subscribe(_ context.Context, request protocol.Request) (Subscription, error) {
	if n.err != nil {
		return nil, n.err
	}
	n.subscribed <- request
	n.registered.Store(true)
	return n, nil
}

func (n *testNotifier) Wait(ctx context.Context) error {
	n.waits.Add(1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.wake:
		return nil
	}
}

func (n *testNotifier) Close() {
	n.closeOnce.Do(func() { close(n.closedCh) })
}

func (n *testNotifier) notify() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func startNotifiedServer(t *testing.T, handler Handler, notifier Notifier) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	stateDir := t.TempDir()
	listener, err := Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ServeWithNotifier(ctx, listener, handler, notifier) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ServeWithNotifier: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("ServeWithNotifier did not stop")
		}
	})
	return stateDir, cancel, done
}

func TestFollowEventWakeupCannotBeLostBetweenSnapshotAndWait(t *testing.T) {
	notifier := newTestNotifier()
	var calls atomic.Int32
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, request protocol.Request) protocol.Response {
		if !notifier.registered.Load() {
			t.Error("handler ran before notifier subscription was registered")
		}
		if calls.Add(1) == 1 {
			// This change lands after subscription and during the first snapshot.
			// The buffered/coalesced notification must still wake the next query.
			notifier.notify()
			return protocol.Response{Version: model.ProtocolVersion}
		}
		return protocol.Response{Version: model.ProtocolVersion, Events: []model.Event{{Version: 1, RunID: "run_test", Seq: 1, Kind: model.EventRunRunning}}}
	}, notifier)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var received atomic.Int32
	err := Follow(ctx, stateDir, protocol.Request{Op: "events", RunID: "run_test", Follow: true}, func(response protocol.Response) error {
		if len(response.Events) > 0 {
			received.Add(1)
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow error = %v; want context.Canceled", err)
	}
	if received.Load() != 1 || calls.Load() < 2 {
		t.Fatalf("received=%d handler calls=%d; wakeup was lost", received.Load(), calls.Load())
	}
	if notifier.waits.Load() == 0 {
		t.Fatal("follow did not wait on the notifier after its empty snapshot")
	}
	select {
	case <-notifier.closedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription was not closed after disconnect")
	}
}

func TestFollowEventsResumesFromRetainedFromAfterGap(t *testing.T) {
	requests := make(chan protocol.Request, 4)
	var calls atomic.Int32
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, request protocol.Request) protocol.Response {
		requests <- request
		if calls.Add(1) == 1 {
			return protocol.Response{Version: model.ProtocolVersion, Gap: true, RetainedFrom: 10}
		}
		return protocol.Response{Version: model.ProtocolVersion, Events: []model.Event{{Version: 1, RunID: "run_test", Seq: 10, Kind: model.EventRunTerminal}}}
	}, newTestNotifier())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var frames atomic.Int32
	err := FollowEvents(ctx, stateDir, protocol.Request{Op: "events", RunID: "run_test", Follow: true}, func(response protocol.Response) error {
		frames.Add(1)
		if len(response.Events) > 0 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("FollowEvents error = %v; want context.Canceled", err)
	}
	first := <-requests
	second := <-requests
	if first.After != 0 || second.After != 9 || frames.Load() < 2 {
		t.Fatalf("gap recovery requests first=%#v second=%#v frames=%d", first, second, frames.Load())
	}
}

func TestFollowOutputAdvancesByDataLengthAfterRetentionGap(t *testing.T) {
	notifier := newTestNotifier()
	requests := make(chan protocol.Request, 4)
	var calls atomic.Int32
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, request protocol.Request) protocol.Response {
		requests <- request
		switch calls.Add(1) {
		case 1:
			return protocol.Response{Version: model.ProtocolVersion, Gap: true, RetainedFrom: 5, Data: base64.StdEncoding.EncodeToString([]byte("abc"))}
		case 2:
			return protocol.Response{Version: model.ProtocolVersion, Data: base64.StdEncoding.EncodeToString([]byte("de"))}
		default:
			return protocol.Response{Version: model.ProtocolVersion}
		}
	}, notifier)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var frames []protocol.Response
	err := Follow(ctx, stateDir, protocol.Request{Op: "output", RunID: "run_test", Stream: "pty", Offset: 3, Limit: 1 << 20, Follow: true}, func(response protocol.Response) error {
		mu.Lock()
		frames = append(frames, response)
		count := len(frames)
		mu.Unlock()
		if count == 2 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow error = %v; want context.Canceled", err)
	}
	first := <-requests
	second := <-requests
	if first.Follow || first.Offset != 3 || first.Limit != maxFollowOutputBytes {
		t.Fatalf("first output request = %#v; want Follow=false offset=3 bounded limit", first)
	}
	if second.Offset != 8 {
		t.Fatalf("second output offset = %d, want retained-from 5 + 3 decoded bytes", second.Offset)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(frames) < 2 || !frames[0].Gap {
		t.Fatalf("output follow did not preserve gap/data frames: %#v", frames)
	}
}

func TestFollowAllRunWatchCarriesOpaqueCursorAndGap(t *testing.T) {
	notifier := newTestNotifier()
	requests := make(chan protocol.Request, 4)
	var calls atomic.Int32
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, request protocol.Request) protocol.Response {
		requests <- request
		switch calls.Add(1) {
		case 1:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "watermark-a"}
		case 2:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "watermark-b", Gap: true, Events: []model.Event{{Version: 1, RunID: "run_other", Seq: 9, Kind: model.EventRunTerminal}}}
		default:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "watermark-b"}
		}
	}, notifier)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var frames atomic.Int32
	err := Follow(ctx, stateDir, protocol.Request{Op: "watch", Cursor: "", Follow: true}, func(response protocol.Response) error {
		if response.Gap {
			frames.Add(1)
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow error = %v; want context.Canceled", err)
	}
	first := <-requests
	second := <-requests
	if first.RunID != "" || first.Cursor != "" || first.Limit != maxFollowPageItems {
		t.Fatalf("initial all-Run watch request = %#v", first)
	}
	if second.Cursor != "watermark-a" || second.Follow {
		t.Fatalf("watch cursor was not advanced for the next page: %#v", second)
	}
	if frames.Load() != 1 {
		t.Fatalf("gap frame count = %d, want 1", frames.Load())
	}
}

func TestFollowSubscriptionFailureIsTyped(t *testing.T) {
	notifier := newTestNotifier()
	notifier.err = errors.New("closed")
	stateDir, _, _ := startNotifiedServer(t, func(context.Context, protocol.Request) protocol.Response {
		t.Fatal("handler called after subscription failed")
		return protocol.Response{}
	}, notifier)
	var frame protocol.Response
	err := Follow(context.Background(), stateDir, protocol.Request{Op: "watch", Follow: true}, func(response protocol.Response) error {
		frame = response
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if frame.Error == nil || frame.Error.Code != "subscription-unavailable" {
		t.Fatalf("subscription failure frame = %#v", frame)
	}
}
