package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

type cliWakeNotifier struct {
	requests chan protocol.Request
	subs     chan *cliWakeSubscription
}

func newCLIWakeNotifier() *cliWakeNotifier {
	return &cliWakeNotifier{requests: make(chan protocol.Request, 4), subs: make(chan *cliWakeSubscription, 4)}
}

func (n *cliWakeNotifier) Subscribe(_ context.Context, request protocol.Request) (ipc.Subscription, error) {
	sub := &cliWakeSubscription{wake: make(chan struct{}, 1), waiting: make(chan struct{}, 1)}
	n.requests <- request
	n.subs <- sub
	return sub, nil
}

type cliWakeSubscription struct {
	wake    chan struct{}
	waiting chan struct{}
}

func (s *cliWakeSubscription) Wait(ctx context.Context) error {
	select {
	case s.waiting <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.wake:
		return nil
	}
}

func (s *cliWakeSubscription) Close() {}

func (s *cliWakeSubscription) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func serveNotifiedCLI(t *testing.T, notifier ipc.Notifier, handler ipc.Handler) string {
	t.Helper()
	stateDir := t.TempDir()
	listener, err := ipc.Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ipc.ServeWithNotifier(ctx, listener, handler, notifier) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("ServeWithNotifier: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("ServeWithNotifier did not stop")
		}
	})
	return stateDir
}

func waitCLIRequest(t *testing.T, requests <-chan protocol.Request) protocol.Request {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for IPC request")
		return protocol.Request{}
	}
}

func waitCLIWake(t *testing.T, waiting <-chan struct{}) {
	t.Helper()
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("follow did not wait for a notifier wakeup")
	}
}

func TestWatchSnapshotForwardsOpaqueCursorAndLimit(t *testing.T) {
	stateDir := t.TempDir()
	queries := make(chan protocol.Request, 1)
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		queries <- request
		return protocol.Response{
			Version: model.ProtocolVersion, NextCursor: "cursor_next",
			Events: []model.Event{{Version: 1, RunID: "run_watch", Seq: 3, Kind: model.EventRunRunning}},
		}
	})

	var stdout, stderr bytes.Buffer
	code := Main(context.Background(), []string{"watch", "--state-dir", stateDir, "--cursor", "cursor_before", "--limit", "7"}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("watch snapshot exit = %d; stderr=%s", code, stderr.String())
	}
	request := <-queries
	if request.Op != "watch" || request.Cursor != "cursor_before" || request.Limit != 7 || request.Follow {
		t.Fatalf("watch snapshot request = %#v", request)
	}
	var response protocol.Response
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("watch response is not JSON: %v; output=%s", err, stdout.String())
	}
	if response.NextCursor != "cursor_next" || len(response.Events) != 1 || response.Events[0].RunID != "run_watch" {
		t.Fatalf("watch response = %#v", response)
	}
}

func TestWatchFollowUsesNotifierAndReconnectsPastHistoryGap(t *testing.T) {
	notifier := newCLIWakeNotifier()
	queries := make(chan protocol.Request, 8)
	var calls atomic.Int32
	stateDir := serveNotifiedCLI(t, notifier, func(_ context.Context, request protocol.Request) protocol.Response {
		queries <- request
		switch calls.Add(1) {
		case 1:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: request.Cursor}
		case 2:
			return protocol.Response{Version: model.ProtocolVersion, Gap: true, NextCursor: "cursor_watermark"}
		default:
			return protocol.Response{Version: model.ProtocolVersion, NextCursor: request.Cursor}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Main(ctx, []string{"watch", "--state-dir", stateDir, "--cursor", "cursor_start", "--limit", "8", "--follow"}, writer, &stderr, nil)
	}()

	followRequest := waitCLIRequest(t, notifier.requests)
	if followRequest.Op != "watch" || followRequest.Cursor != "cursor_start" || followRequest.Limit != 8 || !followRequest.Follow {
		t.Fatalf("watch notifier request = %#v", followRequest)
	}
	sub := <-notifier.subs
	first := waitCLIRequest(t, queries)
	if first.Op != "watch" || first.Follow || first.Cursor != "cursor_start" || first.Limit != 1 {
		t.Fatalf("initial watch snapshot request = %#v", first)
	}
	waitCLIWake(t, sub.waiting)
	sub.notify()
	second := waitCLIRequest(t, queries)
	if second.Cursor != "cursor_start" {
		t.Fatalf("request before gap page cursor = %q, want cursor_start", second.Cursor)
	}
	third := waitCLIRequest(t, queries)
	if third.Cursor != "cursor_watermark" {
		t.Fatalf("request after gap page cursor = %q, want cursor_watermark", third.Cursor)
	}
	scanner := bufio.NewScanner(reader)
	if !scanner.Scan() {
		t.Fatalf("watch follow produced no gap page: %v", scanner.Err())
	}
	line := scanner.Text()
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("watch --follow exit = %d; stderr=%s; stdout=%s", code, stderr.String(), stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch follow did not stop after client cancellation")
	}
	var page protocol.Response
	if err := json.Unmarshal([]byte(line), &page); err != nil {
		t.Fatalf("watch follow output is not NDJSON/JSON: %v; output=%s", err, line)
	}
	if !page.Gap || page.NextCursor != "cursor_watermark" {
		t.Fatalf("watch follow gap page = %#v", page)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected watch follow stderr: %s", stderr.String())
	}
}

func TestOutputFollowUsesNotifierAndAdvancesFromGapAndData(t *testing.T) {
	notifier := newCLIWakeNotifier()
	queries := make(chan protocol.Request, 8)
	var calls atomic.Int32
	stateDir := serveNotifiedCLI(t, notifier, func(_ context.Context, request protocol.Request) protocol.Response {
		queries <- request
		switch calls.Add(1) {
		case 1:
			return protocol.Response{Version: model.ProtocolVersion}
		case 2:
			return protocol.Response{
				Version: model.ProtocolVersion, Data: base64.StdEncoding.EncodeToString([]byte("abc")),
				Gap: true, RetainedFrom: 12, Run: &model.Run{ID: "run_output", State: model.Terminal},
			}
		default:
			return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: "run_output", State: model.Terminal}}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Main(ctx, []string{"output", "--state-dir", stateDir, "--stream", "stderr", "--offset", "2", "--limit", "100000", "--follow", "--json", "run_output"}, &stdout, &stderr, nil)
	}()

	followRequest := waitCLIRequest(t, notifier.requests)
	if followRequest.Op != "output" || followRequest.RunID != "run_output" || followRequest.Stream != "stderr" || followRequest.Offset != 2 || followRequest.Limit != outputChunkSize || !followRequest.Follow {
		t.Fatalf("notifier subscription request = %#v", followRequest)
	}
	sub := <-notifier.subs
	first := waitCLIRequest(t, queries)
	if first.Op != "output" || first.Follow || first.Offset != 2 || first.Limit != outputChunkSize {
		t.Fatalf("initial bounded output snapshot request = %#v", first)
	}
	waitCLIWake(t, sub.waiting)
	sub.notify()
	second := waitCLIRequest(t, queries)
	if second.Offset != 2 {
		t.Fatalf("request after initial empty page offset = %d, want 2", second.Offset)
	}
	third := waitCLIRequest(t, queries)
	if third.Offset != 15 {
		t.Fatalf("request after gap and output data offset = %d, want 15", third.Offset)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("output --follow --json exit = %d; stderr=%s; stdout=%s", code, stderr.String(), stdout.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("output follow did not finish after terminal drain")
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("output follow NDJSON lines = %d, want output and terminal records: %s", len(lines), stdout.String())
	}
	var output outputFollowRecord
	if err := json.Unmarshal([]byte(lines[0]), &output); err != nil {
		t.Fatalf("decode output follow record: %v", err)
	}
	if output.Type != "output" || output.RunID != "run_output" || output.Stream != "stderr" || output.Offset != 12 || output.NextOffset != 15 || !output.Gap || output.RetainedFrom != 12 || output.Data != base64.StdEncoding.EncodeToString([]byte("abc")) {
		t.Fatalf("output follow record = %#v", output)
	}
	var terminal terminalRecord
	if err := json.Unmarshal([]byte(lines[1]), &terminal); err != nil || terminal.Type != "terminal" || terminal.RunID != "run_output" || terminal.State != model.Terminal {
		t.Fatalf("terminal record = %#v, err=%v", terminal, err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected output follow stderr: %s", stderr.String())
	}
}

func TestOutputFollowWritesArbitraryRawBytes(t *testing.T) {
	notifier := newCLIWakeNotifier()
	queries := make(chan protocol.Request, 4)
	var calls atomic.Int32
	stateDir := serveNotifiedCLI(t, notifier, func(_ context.Context, request protocol.Request) protocol.Response {
		queries <- request
		if calls.Add(1) == 1 {
			return protocol.Response{Version: model.ProtocolVersion}
		}
		if calls.Load() == 2 {
			return protocol.Response{Version: model.ProtocolVersion, Data: base64.StdEncoding.EncodeToString([]byte{0, 0xff, '\n'}), Run: &model.Run{ID: "run_raw", State: model.Terminal}}
		}
		return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: "run_raw", State: model.Terminal}}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Main(ctx, []string{"output", "--state-dir", stateDir, "--follow", "run_raw"}, &stdout, &stderr, nil)
	}()
	_ = waitCLIRequest(t, notifier.requests)
	sub := <-notifier.subs
	_ = waitCLIRequest(t, queries)
	waitCLIWake(t, sub.waiting)
	sub.notify()
	_ = waitCLIRequest(t, queries)
	_ = waitCLIRequest(t, queries)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("output --follow exit = %d; stderr=%s", code, stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("raw output follow did not finish after terminal drain")
	}
	if want := []byte{0, 0xff, '\n'}; !bytes.Equal(stdout.Bytes(), want) {
		t.Fatalf("raw output = %v, want %v", stdout.Bytes(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected raw output follow stderr: %s", stderr.String())
	}
}
