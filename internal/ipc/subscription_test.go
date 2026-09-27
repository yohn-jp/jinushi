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
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "watermark-a", WatchWatermark: "watermark-a", WatchRetainedFrom: "retained-a"}
		case 2:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "watermark-b", WatchWatermark: "watermark-b", WatchRetainedFrom: "retained-b", Gap: true, Events: []model.Event{{Version: 1, RunID: "run_other", Seq: 9, Kind: model.EventRunTerminal}}}
		default:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "watermark-b", WatchWatermark: "watermark-b", WatchRetainedFrom: "retained-b"}
		}
	}, notifier)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var frames atomic.Int32
	err := Follow(ctx, stateDir, protocol.Request{Op: "watch", Cursor: "", Follow: true}, func(response protocol.Response) error {
		if response.Gap {
			if response.WatchWatermark != "watermark-b" || response.WatchRetainedFrom != "retained-b" {
				t.Errorf("watch gap watermarks = %q/%q", response.WatchWatermark, response.WatchRetainedFrom)
			}
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

func TestFollowTelemetryUsesPageCursorThenStableWatermark(t *testing.T) {
	requests := make(chan protocol.Request, 4)
	var calls atomic.Int32
	runID := "run_telemetry_follow"
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, request protocol.Request) protocol.Response {
		requests <- request
		run := &model.Run{ID: runID, State: model.Running}
		page := &model.TelemetryResponse{Version: model.TelemetrySchemaVersion, RunID: runID}
		switch calls.Add(1) {
		case 1:
			for sequence := uint64(1); sequence <= maxFollowTelemetryPoints; sequence++ {
				page.Samples = append(page.Samples, model.TelemetrySample{Sequence: sequence})
			}
			page.NextCursor = "sequence-16"
			page.Watermark = page.NextCursor
		case 2:
			page.Samples = []model.TelemetrySample{{Sequence: 17}}
			page.Watermark = "sequence-17"
		default:
			page.Watermark = "sequence-17"
		}
		return protocol.Response{Version: model.ProtocolVersion, Run: run, Telemetry: page}
	}, newTestNotifier())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var samples atomic.Int32
	err := Follow(ctx, stateDir, protocol.Request{
		Op: "telemetry", RunID: runID, Follow: true,
		TelemetryQuery: &model.TelemetryQuery{RunID: runID, Limit: 100},
	}, func(response protocol.Response) error {
		if response.Telemetry != nil && len(response.Telemetry.Samples) > 0 && samples.Add(int32(len(response.Telemetry.Samples))) >= 17 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("telemetry Follow error = %v; want context.Canceled", err)
	}
	first, second := <-requests, <-requests
	if first.TelemetryQuery == nil || first.TelemetryQuery.Limit != maxFollowTelemetryPoints || first.TelemetryQuery.Cursor != "" {
		t.Fatalf("first telemetry query = %+v", first.TelemetryQuery)
	}
	if second.TelemetryQuery == nil || second.TelemetryQuery.Cursor != "sequence-16" {
		t.Fatalf("continuation telemetry query = %+v", second.TelemetryQuery)
	}
	if samples.Load() != 17 {
		t.Fatalf("delivered sample count = %d, want 17", samples.Load())
	}
}

func TestFollowTelemetryDeduplicatesGapOnlySnapshotsAndWaits(t *testing.T) {
	notifier := newTestNotifier()
	requests := make(chan protocol.Request, 4)
	var calls atomic.Int32
	runID := "run_telemetry_gap_follow"
	firstGap := model.TelemetryGap{From: time.Unix(1, 0).UTC(), To: time.Unix(2, 0).UTC(), Reason: "first"}
	secondGap := model.TelemetryGap{From: time.Unix(2, 0).UTC(), To: time.Unix(3, 0).UTC(), Reason: "second"}
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, request protocol.Request) protocol.Response {
		requests <- request
		page := &model.TelemetryResponse{Version: model.TelemetrySchemaVersion, RunID: runID, Gaps: []model.TelemetryGap{firstGap}}
		if calls.Add(1) >= 3 {
			page.Gaps = append(page.Gaps, secondGap)
		}
		return protocol.Response{
			Version: model.ProtocolVersion, Run: &model.Run{ID: runID, State: model.Running}, Telemetry: page,
		}
	}, notifier)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := make(chan []model.TelemetryGap, 2)
	done := make(chan error, 1)
	var delivered atomic.Int32
	go func() {
		done <- Follow(ctx, stateDir, protocol.Request{
			Op: "telemetry", RunID: runID, Follow: true,
			TelemetryQuery: &model.TelemetryQuery{RunID: runID},
		}, func(response protocol.Response) error {
			if response.Telemetry != nil && len(response.Telemetry.Gaps) > 0 {
				frames <- response.Telemetry.Gaps
				if delivered.Add(1) == 2 {
					cancel()
				}
			}
			return nil
		})
	}()
	if got := <-frames; len(got) != 1 || got[0].Reason != "first" {
		t.Fatalf("initial gap frame = %+v", got)
	}
	if first, second := <-requests, <-requests; first.TelemetryQuery.Cursor != "" || second.TelemetryQuery.Cursor != "" {
		t.Fatalf("gap-only queries advanced sample cursor unexpectedly: %q / %q", first.TelemetryQuery.Cursor, second.TelemetryQuery.Cursor)
	}
	waiting := time.NewTimer(time.Second)
	defer waiting.Stop()
	for notifier.waits.Load() == 0 {
		select {
		case request := <-requests:
			t.Fatalf("retained gap caused an immediate-repeat query: %+v", request)
		case <-time.After(time.Millisecond):
		case <-waiting.C:
			t.Fatal("telemetry follow did not wait on the notifier after deduplicating the retained gap")
		}
	}
	select {
	case request := <-requests:
		t.Fatalf("telemetry follow queried again without a new gap: %+v", request)
	case <-time.After(20 * time.Millisecond):
	}
	notifier.notify()
	third := <-requests
	if third.TelemetryQuery == nil || third.TelemetryQuery.Cursor != "" {
		t.Fatalf("gap-only telemetry query cursor = %+v", third.TelemetryQuery)
	}
	if got := <-frames; len(got) != 1 || got[0].Reason != "second" {
		t.Fatalf("new gap frame = %+v", got)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("telemetry Follow exit = %v; want context.Canceled", err)
	}
}

func TestFollowTelemetryDrainsTerminalPagesBeforeClosing(t *testing.T) {
	requests := make(chan protocol.Request, 4)
	var calls atomic.Int32
	runID := "run_telemetry_terminal"
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, request protocol.Request) protocol.Response {
		requests <- request
		page := &model.TelemetryResponse{Version: model.TelemetrySchemaVersion, RunID: runID}
		switch calls.Add(1) {
		case 1:
			page.Samples = []model.TelemetrySample{{Sequence: 1}}
			page.NextCursor = "sequence-1"
			page.Watermark = page.NextCursor
		case 2:
			page.Samples = []model.TelemetrySample{{Sequence: 2}}
			page.Watermark = "sequence-2"
		default:
			page.Watermark = "sequence-2"
		}
		return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: runID, State: model.Terminal}, Telemetry: page}
	}, newTestNotifier())

	var frames int
	var samples int
	err := Follow(context.Background(), stateDir, protocol.Request{
		Op: "telemetry", RunID: runID, Follow: true,
		TelemetryQuery: &model.TelemetryQuery{RunID: runID},
	}, func(response protocol.Response) error {
		frames++
		if response.Telemetry != nil {
			samples += len(response.Telemetry.Samples)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("terminal telemetry Follow error = %v", err)
	}
	first, second, third := <-requests, <-requests, <-requests
	if first.TelemetryQuery.Cursor != "" || second.TelemetryQuery.Cursor != "sequence-1" || third.TelemetryQuery.Cursor != "sequence-2" {
		t.Fatalf("terminal telemetry cursors = %q, %q, %q", first.TelemetryQuery.Cursor, second.TelemetryQuery.Cursor, third.TelemetryQuery.Cursor)
	}
	if frames != 2 || samples != 2 {
		t.Fatalf("terminal drain delivered %d frames / %d samples, want 2 / 2", frames, samples)
	}
}

func TestFollowTelemetryPreservesStaleCursorFailure(t *testing.T) {
	runID := "run_telemetry_stale"
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, _ protocol.Request) protocol.Response {
		return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{
			Code: "stale-telemetry-cursor", Message: "telemetry cursor is stale after compaction",
		}}
	}, newTestNotifier())
	var failure *protocol.Failure
	err := Follow(context.Background(), stateDir, protocol.Request{
		Op: "telemetry", RunID: runID, Follow: true,
		TelemetryQuery: &model.TelemetryQuery{RunID: runID, Cursor: "old-cursor"},
	}, func(response protocol.Response) error {
		failure = response.Error
		return nil
	})
	if err != nil {
		t.Fatalf("stale-cursor Follow transport error = %v", err)
	}
	if failure == nil || failure.Code != "stale-telemetry-cursor" {
		t.Fatalf("stale cursor failure = %+v", failure)
	}
}

func TestFollowAllRunWatchGapWithoutEventsAdvancesToWatermarkAndReconnects(t *testing.T) {
	notifier := newTestNotifier()
	requests := make(chan protocol.Request, 4)
	var calls atomic.Int32
	stateDir, _, _ := startNotifiedServer(t, func(_ context.Context, request protocol.Request) protocol.Response {
		requests <- request
		switch calls.Add(1) {
		case 1:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "cursor-a", WatchWatermark: "cursor-a", WatchRetainedFrom: "retained-a"}
		case 2:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "cursor-b", WatchWatermark: "cursor-b", WatchRetainedFrom: "retained-b", Gap: true}
		default:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: "cursor-c", WatchWatermark: "cursor-c", WatchRetainedFrom: "retained-b", Events: []model.Event{{Version: 1, RunID: "run_after_gap", Seq: 1, Kind: model.EventRunRunning}}}
		}
	}, notifier)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	gapFrame := make(chan protocol.Response, 1)
	go func() {
		firstDone <- Follow(firstCtx, stateDir, protocol.Request{Op: "watch", Cursor: "cursor-a", Follow: true}, func(response protocol.Response) error {
			if response.Gap {
				gapFrame <- response
				cancelFirst()
			}
			return nil
		})
	}()
	initialRequest := <-requests
	if initialRequest.Cursor != "cursor-a" {
		t.Fatalf("initial watch cursor = %q", initialRequest.Cursor)
	}
	notifier.notify()
	gapRequest := <-requests
	if gapRequest.Cursor != "cursor-a" {
		t.Fatalf("gap recovery request cursor = %q, want unchanged cursor-a", gapRequest.Cursor)
	}
	var gap protocol.Response
	select {
	case gap = <-gapFrame:
	case <-time.After(2 * time.Second):
		t.Fatal("watch follow did not deliver gap-only frame")
	}
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Follow error = %v; want context.Canceled", err)
	}
	if len(gap.Events) != 0 || !gap.Gap || gap.WatchWatermark != "cursor-b" || gap.WatchRetainedFrom != "retained-b" || gap.NextCursor != gap.WatchWatermark {
		t.Fatalf("gap-only watch frame = %+v", gap)
	}
	select {
	case <-notifier.subscribed:
	default:
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	secondDone := make(chan error, 1)
	resumedFrame := make(chan protocol.Response, 1)
	go func() {
		secondDone <- Follow(secondCtx, stateDir, protocol.Request{Op: "watch", Cursor: gap.NextCursor, Follow: true}, func(response protocol.Response) error {
			if len(response.Events) > 0 {
				resumedFrame <- response
				cancelSecond()
			}
			return nil
		})
	}()
	resumeRequest := <-requests
	if resumeRequest.Cursor != "cursor-b" {
		t.Fatalf("reconnect request cursor = %q, want cursor-b", resumeRequest.Cursor)
	}
	var resumed protocol.Response
	select {
	case resumed = <-resumedFrame:
	case <-time.After(2 * time.Second):
		t.Fatal("watch follow did not reconnect after gap")
	}
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("reconnected Follow error = %v; want context.Canceled", err)
	}
	if resumed.Gap || resumed.NextCursor != "cursor-c" || resumed.WatchWatermark != "cursor-c" || len(resumed.Events) != 1 {
		t.Fatalf("reconnected watch frame = %+v", resumed)
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
