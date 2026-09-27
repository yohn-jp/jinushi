package supervisor

import (
	"context"
	"errors"
	"sync"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

var errNotifierClosed = errors.New("supervisor observation notifier is closed")

// runtimeNotifier coalesces changes for bounded local subscriptions. Registering
// before the first store read prevents a change during that read from being
// lost; the store remains the source of truth for cursors and gap detection.
type runtimeNotifier struct {
	mu      sync.Mutex
	next    uint64
	closed  bool
	waiters map[uint64]*runtimeSubscription
}

type runtimeSubscription struct {
	owner *runtimeNotifier
	id    uint64
	op    string
	runID string
	all   bool
	wake  chan struct{}
	done  chan struct{}
	once  sync.Once
}

func newRuntimeNotifier() *runtimeNotifier {
	return &runtimeNotifier{waiters: make(map[uint64]*runtimeSubscription)}
}

var _ ipc.Notifier = (*runtimeNotifier)(nil)
var _ ipc.Subscription = (*runtimeSubscription)(nil)

func (n *runtimeNotifier) Subscribe(ctx context.Context, request protocol.Request) (ipc.Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runID := request.RunID
	if request.Op == "telemetry" && request.TelemetryQuery != nil {
		if runID != "" && request.TelemetryQuery.RunID != "" && runID != request.TelemetryQuery.RunID {
			return nil, errors.New("invalid observation subscription")
		}
		if runID == "" {
			runID = request.TelemetryQuery.RunID
		}
	}
	valid := request.Op == "watch" && runID == "" ||
		(request.Op == "events" || request.Op == "output") && runID != "" ||
		request.Op == "telemetry" && request.TelemetryQuery != nil && runID != ""
	if !valid {
		return nil, errors.New("invalid observation subscription")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, errNotifierClosed
	}
	if len(n.waiters) >= 64 {
		return nil, errors.New("observation subscription limit reached")
	}
	n.next++
	sub := &runtimeSubscription{owner: n, id: n.next, op: request.Op, runID: runID, all: request.Op == "watch", wake: make(chan struct{}, 1), done: make(chan struct{})}
	n.waiters[sub.id] = sub
	return sub, nil
}

func (n *runtimeNotifier) notify(runID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	for _, sub := range n.waiters {
		if !sub.all && sub.runID != runID {
			continue
		}
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}

// notifyTelemetry wakes only telemetry followers for the Run. High-rate
// samples must not wake lifecycle, output, or all-Run watch subscriptions.
func (n *runtimeNotifier) notifyTelemetry(runID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	for _, sub := range n.waiters {
		if sub.op != "telemetry" || sub.runID != runID {
			continue
		}
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}

func (n *runtimeNotifier) close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	n.closed = true
	for _, sub := range n.waiters {
		sub.once.Do(func() { close(sub.done) })
	}
	clear(n.waiters)
}

func (s *runtimeSubscription) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return errNotifierClosed
	case <-s.wake:
		return nil
	}
}

func (s *runtimeSubscription) Close() {
	s.once.Do(func() { close(s.done) })
	s.owner.mu.Lock()
	delete(s.owner.waiters, s.id)
	s.owner.mu.Unlock()
}
