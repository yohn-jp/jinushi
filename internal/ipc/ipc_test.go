//go:build !windows

package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
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
