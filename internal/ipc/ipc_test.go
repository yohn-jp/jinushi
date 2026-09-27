//go:build !windows

package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

func TestCallAndServeRoundTrip(t *testing.T) {
	stateDir := t.TempDir()
	listener, err := Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, listener, func(_ context.Context, request protocol.Request) protocol.Response {
			if request.Op != "inspect" || request.RunID != "run_test" {
				return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: "bad-request", Message: "unexpected request"}}
			}
			return protocol.Response{Version: model.ProtocolVersion, Data: "ok"}
		})
	}()

	response, err := Call(context.Background(), stateDir, protocol.Request{Op: "inspect", RunID: "run_test"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Version != model.ProtocolVersion || response.Data != "ok" {
		t.Fatalf("unexpected response: %#v", response)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not stop after context cancellation")
	}
}

func TestListenRejectsAnActiveEndpoint(t *testing.T) {
	stateDir := t.TempDir()
	first, err := Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := Listen(stateDir); err == nil {
		second.Close()
		t.Fatal("second listener replaced the active endpoint")
	}
}

func TestReadFrameRejectsOversizedFrameBeforeAllocation(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], protocol.MaxFrame+1)
	reader := sliceReader(header[:])
	if err := readFrame(&reader, new(protocol.Request)); err == nil {
		t.Fatal("expected oversized frame error")
	}
}

func TestClientDisconnectCancelsRequestContextOnly(t *testing.T) {
	stateDir := t.TempDir()
	listener, err := Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	requestCanceled := make(chan struct{})
	go func() {
		_ = Serve(ctx, listener, func(requestCtx context.Context, _ protocol.Request) protocol.Response {
			close(started)
			<-requestCtx.Done()
			close(requestCanceled)
			return protocol.Response{Version: model.ProtocolVersion}
		})
	}()

	conn, err := Dial(context.Background(), stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(conn, protocol.Request{Version: model.ProtocolVersion, Op: "await", RunID: "run_test"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("client disconnect did not cancel the request context")
	}
	// No Run control API is invoked by this transport-level cancellation.
}

func TestFollowEventsDrainsTerminalRaceAndAdvancesCursor(t *testing.T) {
	stateDir := t.TempDir()
	listener, err := Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := make(chan protocol.Request, 4)
	var calls atomic.Int32
	go func() {
		_ = Serve(ctx, listener, func(_ context.Context, request protocol.Request) protocol.Response {
			requests <- request
			call := calls.Add(1)
			terminal := &model.Run{ID: "run_test", State: model.Terminal}
			switch call {
			case 1:
				// Run became terminal after Events read; the terminal event is late.
				return protocol.Response{Version: model.ProtocolVersion, Run: terminal}
			case 2:
				return protocol.Response{Version: model.ProtocolVersion, Run: terminal, Events: []model.Event{{Version: 1, RunID: "run_test", Seq: 1, Kind: "run.terminal"}}}
			default:
				return protocol.Response{Version: model.ProtocolVersion, Run: terminal}
			}
		})
	}()

	var responses []protocol.Response
	err = FollowEvents(context.Background(), stateDir, protocol.Request{Op: "events", RunID: "run_test", Follow: true}, func(response protocol.Response) error {
		responses = append(responses, response)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || len(responses) != 3 {
		t.Fatalf("follow made %d handler calls and received %d frames; want 3", calls.Load(), len(responses))
	}
	wantAfter := []uint64{0, 0, 1}
	for i, want := range wantAfter {
		request := <-requests
		if request.Follow || request.After != want || request.Limit != 1 {
			t.Fatalf("handler request %d = %#v; want Follow=false After=%d Limit=1", i, request, want)
		}
	}
	if len(responses[1].Events) != 1 || responses[1].Events[0].Kind != "run.terminal" {
		t.Fatalf("late terminal event was not delivered: %#v", responses)
	}
}

func TestFollowEventsStopsAfterUncertainRunSnapshot(t *testing.T) {
	stateDir := t.TempDir()
	listener, err := Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	go func() {
		_ = Serve(ctx, listener, func(_ context.Context, request protocol.Request) protocol.Response {
			calls.Add(1)
			return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: request.RunID, State: model.Uncertain}}
		})
	}()

	var responses []protocol.Response
	err = FollowEvents(context.Background(), stateDir, protocol.Request{Op: "events", RunID: "run_test", Follow: true}, func(response protocol.Response) error {
		responses = append(responses, response)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(responses) != 2 {
		t.Fatalf("follow made %d handler calls and received %d frames; want the initial uncertain snapshot plus one final query", calls.Load(), len(responses))
	}
	for i, response := range responses {
		if response.Run == nil || response.Run.State != model.Uncertain {
			t.Fatalf("frame %d did not preserve uncertain Run state: %#v", i, response)
		}
	}
}

func TestFollowDisconnectCancelsOnlySubscription(t *testing.T) {
	stateDir := t.TempDir()
	listener, err := Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, stopServe := context.WithCancel(context.Background())
	defer stopServe()
	blocked := make(chan struct{})
	var calls atomic.Int32
	go func() {
		_ = Serve(serveCtx, listener, func(ctx context.Context, request protocol.Request) protocol.Response {
			if calls.Add(1) == 1 {
				go func() {
					<-ctx.Done()
					close(blocked)
				}()
				return protocol.Response{Version: model.ProtocolVersion, Events: []model.Event{{Version: 1, RunID: request.RunID, Seq: 1, Kind: "run.started"}}}
			}
			return protocol.Response{Version: model.ProtocolVersion}
		})
	}()

	clientCtx, cancelClient := context.WithCancel(context.Background())
	err = FollowEvents(clientCtx, stateDir, protocol.Request{Op: "events", RunID: "run_test", Follow: true}, func(protocol.Response) error {
		cancelClient()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("FollowEvents error = %v; want context.Canceled", err)
	}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("client disconnect did not cancel the subscription handler")
	}
	if calls.Load() < 2 {
		t.Fatal("follow handler did not poll after its first event")
	}
}

func TestFollowFrameLimitReturnsTypedError(t *testing.T) {
	stateDir := t.TempDir()
	listener, err := Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = Serve(ctx, listener, func(_ context.Context, request protocol.Request) protocol.Response {
			return protocol.Response{Version: model.ProtocolVersion, Events: []model.Event{{
				Version: 1, RunID: request.RunID, Seq: 1, Kind: "telemetry.sample",
				Body: map[string]any{"payload": strings.Repeat("x", protocol.MaxFrame*2)},
			}}}
		})
	}()
	var response protocol.Response
	err = FollowEvents(context.Background(), stateDir, protocol.Request{Op: "events", RunID: "run_test", Follow: true}, func(frame protocol.Response) error {
		response = frame
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "invalid-response" {
		t.Fatalf("oversized response did not produce a typed error: %#v", response)
	}
}

func TestListenRejectsNonSocketPath(t *testing.T) {
	stateDir := t.TempDir()
	path := filepath.Join(stateDir, "jinushi.sock")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(stateDir); err == nil {
		t.Fatal("expected non-socket endpoint to be rejected")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep" {
		t.Fatalf("non-socket endpoint was changed: data=%q err=%v", data, err)
	}
}

// A small local reader avoids importing bytes solely for the frame test.
type sliceReader []byte

func (r *sliceReader) Read(p []byte) (int, error) {
	if len(*r) == 0 {
		return 0, errors.New("unexpected read past header")
	}
	n := copy(p, *r)
	*r = (*r)[n:]
	return n, nil
}
