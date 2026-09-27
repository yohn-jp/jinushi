//go:build !windows

package supervisor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

func TestServiceTelemetryFollowWakesFromDurableSampleWithoutPolling(t *testing.T) {
	runID := "run_telemetry_subscription"
	physicalSample := measuredTelemetryFixture(runID, time.Now().UTC().Add(-time.Second))
	svc, a := newTelemetryFixture(t, runID, physicalSample)
	listener, err := ipc.Listen(svc.root)
	if err != nil {
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
			return svc.Handle(ctx, request)
		}, svc.notifier)
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
	})

	followCtx, cancelFollow := context.WithCancel(context.Background())
	defer cancelFollow()
	frames := make(chan protocol.Response, 1)
	followDone := make(chan error, 1)
	go func() {
		followDone <- ipc.Follow(followCtx, svc.root, protocol.Request{
			Op: "telemetry", RunID: runID, Follow: true,
			TelemetryQuery: &model.TelemetryQuery{RunID: runID},
		}, func(response protocol.Response) error {
			if response.Error != nil || response.Telemetry != nil && len(response.Telemetry.Samples) > 0 {
				frames <- response
				cancelFollow()
			}
			return nil
		})
	}()
	initial := waitNotifierQuery(t, queries, "telemetry")
	if initial.RunID != runID || initial.TelemetryQuery == nil || initial.TelemetryQuery.Limit != 16 || initial.TelemetryQuery.RunID != runID {
		t.Fatalf("initial telemetry follow query = %+v", initial)
	}
	// A production subscription blocks on the notifier. Waiting longer than the
	// legacy 100 ms fallback interval must not issue another snapshot query.
	select {
	case request := <-queries:
		t.Fatalf("telemetry follow polled without a notification: %+v", request)
	case <-time.After(250 * time.Millisecond):
	}

	physical := a.process.(*telemetryFixturePhysical)
	physical.sample = measuredTelemetryFixture(runID, time.Now().UTC())
	svc.sample(a)
	var frame protocol.Response
	select {
	case frame = <-frames:
	case <-time.After(2 * time.Second):
		t.Fatal("telemetry follow did not wake after the stored sample")
	}
	if frame.Error != nil {
		t.Fatalf("telemetry follow response error = %s: %s", frame.Error.Code, frame.Error.Message)
	}
	if frame.Run == nil || frame.Run.ID != runID || frame.Telemetry == nil || frame.Telemetry.RunID != runID || len(frame.Telemetry.Samples) != 1 {
		t.Fatalf("telemetry follow did not return current Run and stored sample: %+v", frame)
	}
	if frame.Telemetry.Watermark == "" || frame.Telemetry.NextCursor != "" {
		t.Fatalf("terminal-page watermark/continuation = %q/%q", frame.Telemetry.Watermark, frame.Telemetry.NextCursor)
	}
	if err := <-followDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("telemetry Follow exit = %v; want context.Canceled", err)
	}
}

func TestServiceTelemetryQueryReturnsTypedStaleCursorAfterCompaction(t *testing.T) {
	runID := "run_telemetry_stale_cursor"
	svc, _ := newTelemetryFixture(t, runID, measuredTelemetryFixture(runID, time.Now().UTC()))
	if err := svc.store.ConfigureTelemetry(store.TelemetryOptions{
		RawSamples: 2, RawBytes: 1 << 20, AggregatePoints: 8, AggregateBytes: 2 << 20, MaxQueryPoints: 1,
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	for index := 0; index < 3; index++ {
		if _, err := svc.store.AppendTelemetry(runID, measuredTelemetryFixture(runID, base.Add(time.Duration(index)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	page, err := svc.store.QueryTelemetry(model.TelemetryQuery{RunID: runID, Limit: 1})
	if err != nil || page.NextCursor == "" {
		t.Fatalf("initial telemetry page = %+v, err=%v", page, err)
	}
	if _, err := svc.store.AppendTelemetry(runID, measuredTelemetryFixture(runID, base.Add(3*time.Second))); err != nil {
		t.Fatal(err)
	}
	response := svc.Handle(context.Background(), protocol.Request{
		Version: model.ProtocolVersion, Op: "telemetry", RunID: runID,
		TelemetryQuery: &model.TelemetryQuery{RunID: runID, Limit: 1, Cursor: page.NextCursor},
	})
	if response.Error == nil || response.Error.Code != "stale-telemetry-cursor" {
		t.Fatalf("stale telemetry query failure = %+v", response.Error)
	}
}
