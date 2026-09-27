package supervisor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

func TestRuntimeNotifierCoalescesAndRoutesChanges(t *testing.T) {
	n := newRuntimeNotifier()
	first, err := n.Subscribe(context.Background(), protocol.Request{Op: "events", RunID: "run_1"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	other, err := n.Subscribe(context.Background(), protocol.Request{Op: "output", RunID: "run_2"})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	all, err := n.Subscribe(context.Background(), protocol.Request{Op: "watch"})
	if err != nil {
		t.Fatal(err)
	}
	defer all.Close()

	for range 100 {
		n.notify("run_1")
	}
	ready, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := first.Wait(ready); err != nil {
		t.Fatalf("Run subscription missed wake: %v", err)
	}
	if err := all.Wait(ready); err != nil {
		t.Fatalf("all-Run subscription missed wake: %v", err)
	}
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := other.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unrelated Run woke subscription: %v", err)
	}
	if err := first.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("coalesced wake grew without bound: %v", err)
	}
	n.notify("run_1")
	if err := first.Wait(ready); err != nil {
		t.Fatalf("later change was lost: %v", err)
	}
}

func TestRuntimeNotifierCloseReleasesWaiters(t *testing.T) {
	n := newRuntimeNotifier()
	sub, err := n.Subscribe(context.Background(), protocol.Request{Op: "watch"})
	if err != nil {
		t.Fatal(err)
	}
	n.close()
	if err := sub.Wait(context.Background()); !errors.Is(err, errNotifierClosed) {
		t.Fatalf("closed notifier wait = %v", err)
	}
	sub.Close()
	if _, err := n.Subscribe(context.Background(), protocol.Request{Op: "watch"}); !errors.Is(err, errNotifierClosed) {
		t.Fatalf("subscription after close = %v", err)
	}
}

func TestRuntimeNotifierRoutesTelemetryOnlyToMatchingTelemetryFollow(t *testing.T) {
	n := newRuntimeNotifier()
	telemetry, err := n.Subscribe(context.Background(), protocol.Request{
		Op: "telemetry", RunID: "run_1", TelemetryQuery: &model.TelemetryQuery{RunID: "run_1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer telemetry.Close()
	events, err := n.Subscribe(context.Background(), protocol.Request{Op: "events", RunID: "run_1"})
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	other, err := n.Subscribe(context.Background(), protocol.Request{
		Op: "telemetry", RunID: "run_2", TelemetryQuery: &model.TelemetryQuery{RunID: "run_2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	watch, err := n.Subscribe(context.Background(), protocol.Request{Op: "watch"})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()

	n.notifyTelemetry("run_1")
	ready, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := telemetry.Wait(ready); err != nil {
		t.Fatalf("matching telemetry follow missed wake: %v", err)
	}
	for _, check := range []struct {
		sub  ipc.Subscription
		name string
	}{{events, "lifecycle"}, {other, "another Run"}, {watch, "all-Run watch"}} {
		short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
		err := check.sub.Wait(short)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("telemetry sample woke %s follow: %v", check.name, err)
		}
	}
	n.notify("run_1")
	if err := telemetry.Wait(ready); err != nil {
		t.Fatalf("lifecycle change did not wake telemetry follow for terminal detection: %v", err)
	}
	if err := watch.Wait(ready); err != nil {
		t.Fatalf("lifecycle change did not wake all-Run watch: %v", err)
	}
}
