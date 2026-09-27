// Package ipc implements the bounded local request/response transport used by
// the CLI and the resident supervisor.
package ipc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

const maxConnections = 64

var ErrFrameTooLarge = errors.New("IPC frame exceeds the protocol size limit")

// Handler handles one request on one connection. A client disconnect cancels
// only this request context; it never implies Run cancellation.
type Handler func(context.Context, protocol.Request) protocol.Response

// Listen creates the local-only supervisor endpoint for stateDir.
func Listen(stateDir string) (net.Listener, error) {
	endpoint, err := Endpoint(stateDir)
	if err != nil {
		return nil, err
	}
	return listenEndpoint(endpoint)
}

// Dial connects to the local supervisor endpoint for stateDir.
func Dial(ctx context.Context, stateDir string) (net.Conn, error) {
	endpoint, err := Endpoint(stateDir)
	if err != nil {
		return nil, err
	}
	return dialEndpoint(ctx, endpoint)
}

// Call exchanges one bounded request and response with the supervisor.
func Call(ctx context.Context, stateDir string, request protocol.Request) (protocol.Response, error) {
	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	conn, err := Dial(dialCtx, stateDir)
	cancelDial()
	if err != nil {
		return protocol.Response{}, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer func() {
		stop()
		_ = conn.Close()
	}()

	if request.Version == 0 {
		request.Version = model.ProtocolVersion
	}
	if err := writeFrame(conn, request); err != nil {
		return protocol.Response{}, fmt.Errorf("send supervisor request: %w", err)
	}
	var response protocol.Response
	if err := readFrame(conn, &response); err != nil {
		if ctx.Err() != nil {
			return protocol.Response{}, ctx.Err()
		}
		return protocol.Response{}, fmt.Errorf("read supervisor response: %w", err)
	}
	if response.Version != model.ProtocolVersion {
		return response, fmt.Errorf("supervisor returned unsupported protocol version %d", response.Version)
	}
	return response, nil
}

// FollowEvents opens one framed subscription connection. Each bounded response
// frame is passed to onResponse in order. Closing the client cancels only the
// subscription request context, never its Run.
func FollowEvents(ctx context.Context, stateDir string, request protocol.Request, onResponse func(protocol.Response) error) error {
	if request.Op != "events" || !request.Follow {
		return errors.New("event follow requires Op=events and Follow=true")
	}
	if onResponse == nil {
		return errors.New("nil event follow response handler")
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	conn, err := Dial(dialCtx, stateDir)
	cancelDial()
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer func() {
		stop()
		_ = conn.Close()
	}()

	if request.Version == 0 {
		request.Version = model.ProtocolVersion
	}
	if err := writeFrame(conn, request); err != nil {
		return fmt.Errorf("send event follow request: %w", err)
	}
	for {
		var response protocol.Response
		if err := readFrame(conn, &response); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read event follow frame: %w", err)
		}
		if response.Version != model.ProtocolVersion {
			return fmt.Errorf("supervisor returned unsupported protocol version %d", response.Version)
		}
		if err := onResponse(response); err != nil {
			return err
		}
	}
}

// Serve accepts bounded, single-request connections until ctx is canceled.
func Serve(ctx context.Context, listener net.Listener, handler Handler) error {
	if listener == nil {
		return errors.New("nil IPC listener")
	}
	if handler == nil {
		return errors.New("nil IPC handler")
	}
	defer listener.Close()
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopped:
		}
	}()
	defer close(stopped)

	sem := make(chan struct{}, maxConnections)
	var workers sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				workers.Wait()
				return nil
			}
			return fmt.Errorf("accept IPC connection: %w", err)
		}
		select {
		case sem <- struct{}{}:
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-sem }()
				serveConnection(ctx, conn, handler)
			}()
		default:
			writeFailure(conn, "busy", "supervisor connection limit reached")
			_ = conn.Close()
		}
	}
}

func serveConnection(parent context.Context, conn net.Conn, handler Handler) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var request protocol.Request
	if err := readFrame(conn, &request); err != nil {
		writeFailure(conn, "invalid-frame", err.Error())
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	if request.Version != model.ProtocolVersion {
		writeFailure(conn, "unsupported-version", fmt.Sprintf("protocol version %d is unsupported", request.Version))
		return
	}

	requestCtx, cancel := context.WithCancel(parent)
	defer cancel()
	disconnected := make(chan struct{})
	go func() {
		defer close(disconnected)
		var extra [1]byte
		_, _ = conn.Read(extra[:])
		// One request is allowed per connection. EOF means the caller stopped
		// waiting; extra data is also a protocol violation and ends the request.
		cancel()
	}()

	if request.Follow {
		if request.Op != "events" {
			writeFailure(conn, "invalid-request", "follow is supported only for events")
		} else {
			serveEventFollow(requestCtx, conn, request, handler)
		}
	} else {
		response := callHandler(requestCtx, request, handler)
		if err := writeFrame(conn, response); err != nil {
			writeFailure(conn, "invalid-response", "supervisor response could not be encoded within the protocol limit")
		}
	}
	_ = conn.Close()
	<-disconnected
}

func serveEventFollow(ctx context.Context, conn net.Conn, request protocol.Request, handler Handler) {
	request.Follow = false
	request.Limit = 1 // A single bounded event fits comfortably in one protocol frame.
	after := request.After
	finalSeen := false
	for {
		if ctx.Err() != nil {
			return
		}
		wasFinal := finalSeen
		request.After = after
		response := callHandler(ctx, request, handler)
		if response.Error != nil {
			writeFollowFrame(conn, response)
			return
		}
		for _, event := range response.Events {
			if event.RunID != request.RunID {
				writeFailure(conn, "invalid-event-stream", "event Run ID did not match the subscription")
				return
			}
			if event.Seq <= after {
				writeFailure(conn, "invalid-event-stream", "event sequence did not advance")
				return
			}
			after = event.Seq
		}
		if response.Gap && len(response.Events) == 0 && response.RetainedFrom > 0 && after < response.RetainedFrom-1 {
			after = response.RetainedFrom - 1
		}
		if response.Run != nil && isFinalRunState(response.Run.State) {
			finalSeen = true
		}

		if len(response.Events) > 0 || response.Gap || finalSeen {
			if !writeFollowFrame(conn, response) {
				return
			}
		}
		// Always make at least one additional store query after observing a
		// final state. Continue paging if that query still returned events.
		if wasFinal && len(response.Events) == 0 {
			return
		}
		if !finalSeen && len(response.Events) == 0 {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func isFinalRunState(state model.State) bool {
	return state == model.Terminal || state == model.Uncertain
}

func writeFollowFrame(conn net.Conn, response protocol.Response) bool {
	if err := writeFrame(conn, response); err != nil {
		writeFailure(conn, "invalid-response", "supervisor response could not be encoded within the protocol limit")
		return false
	}
	return true
}

func callHandler(ctx context.Context, request protocol.Request, handler Handler) (response protocol.Response) {
	defer func() {
		if recover() != nil {
			response = errorResponse("internal", "supervisor request handler failed")
		}
	}()
	response = handler(ctx, request)
	if response.Version == 0 {
		response.Version = model.ProtocolVersion
	}
	return response
}

func writeFailure(w io.Writer, code, message string) {
	_ = writeFrame(w, errorResponse(code, message))
}

func errorResponse(code, message string) protocol.Response {
	return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: code, Message: message}}
}

func writeFrame(w io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode JSON frame: %w", err)
	}
	if len(payload) == 0 || len(payload) > protocol.MaxFrame {
		return fmt.Errorf("%w: frame size %d exceeds limit %d", ErrFrameTooLarge, len(payload), protocol.MaxFrame)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func readFrame(r io.Reader, target any) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > protocol.MaxFrame {
		return fmt.Errorf("%w: frame size %d exceeds limit %d", ErrFrameTooLarge, size, protocol.MaxFrame)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("decode JSON frame: %w", err)
	}
	return nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
