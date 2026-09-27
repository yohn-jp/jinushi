//go:build windows

package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const pipeRejectRemoteClients = 0x8

type namedPipeListener struct {
	name       *uint16
	attributes windows.SecurityAttributes
	mu         sync.Mutex
	active     windows.Handle
	first      bool
	closed     bool
}

func listenNamedPipe(name string) (net.Listener, error) {
	namePtr, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	sid, err := currentUserSID()
	if err != nil {
		return nil, fmt.Errorf("read current user SID for pipe ACL: %w", err)
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;;GA;;;" + sid + ")")
	if err != nil {
		return nil, fmt.Errorf("create named-pipe access control: %w", err)
	}
	attrs := windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: sd,
	}
	return &namedPipeListener{name: namePtr, attributes: attrs, active: windows.InvalidHandle, first: true}, nil
}

func currentUserSID() (string, error) {
	process := windows.CurrentProcess()
	var token windows.Token
	if err := windows.OpenProcessToken(process, windows.TOKEN_QUERY, &token); err != nil {
		return "", err
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return "", err
	}
	return user.User.Sid.String(), nil
}

func (l *namedPipeListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, net.ErrClosed
	}
	flags := uint32(windows.PIPE_ACCESS_DUPLEX | windows.FILE_FLAG_OVERLAPPED)
	if l.first {
		flags |= windows.FILE_FLAG_FIRST_PIPE_INSTANCE
		l.first = false
	}
	pipeMode := uint32(windows.PIPE_TYPE_BYTE | windows.PIPE_READMODE_BYTE | windows.PIPE_WAIT | pipeRejectRemoteClients)
	handle, err := windows.CreateNamedPipe(l.name, flags, pipeMode, windows.PIPE_UNLIMITED_INSTANCES, 64*1024, 64*1024, 0, &l.attributes)
	if err != nil {
		l.mu.Unlock()
		return nil, fmt.Errorf("create local named pipe: %w", err)
	}
	l.active = handle
	l.mu.Unlock()

	event, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		windows.CloseHandle(handle)
		return nil, fmt.Errorf("create named-pipe connect event: %w", err)
	}
	overlapped := windows.Overlapped{HEvent: event}
	err = windows.ConnectNamedPipe(handle, &overlapped)
	if errors.Is(err, windows.ERROR_IO_PENDING) {
		_, err = windows.WaitForSingleObject(event, windows.INFINITE)
		if err == nil {
			var transferred uint32
			err = windows.GetOverlappedResult(handle, &overlapped, &transferred, false)
		}
	}
	windows.CloseHandle(event)
	if err != nil && !errors.Is(err, windows.ERROR_PIPE_CONNECTED) {
		windows.CloseHandle(handle)
		l.mu.Lock()
		if l.active == handle {
			l.active = windows.InvalidHandle
		}
		closed := l.closed
		l.mu.Unlock()
		if closed {
			return nil, net.ErrClosed
		}
		return nil, fmt.Errorf("accept named-pipe client: %w", err)
	}

	l.mu.Lock()
	if l.active == handle {
		l.active = windows.InvalidHandle
	}
	closed := l.closed
	l.mu.Unlock()
	if closed {
		windows.CloseHandle(handle)
		return nil, net.ErrClosed
	}
	return &namedPipeConn{file: os.NewFile(uintptr(handle), "jinushi-named-pipe")}, nil
}

func (l *namedPipeListener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	handle := l.active
	l.active = windows.InvalidHandle
	l.mu.Unlock()
	if handle != windows.InvalidHandle {
		_ = windows.CancelIoEx(handle, nil)
		return windows.CloseHandle(handle)
	}
	return nil
}

func (l *namedPipeListener) Addr() net.Addr { return pipeAddr("local named pipe") }

func dialNamedPipe(ctx context.Context, name string) (net.Conn, error) {
	namePtr, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	for {
		handle, openErr := windows.CreateFile(namePtr, windows.GENERIC_READ|windows.GENERIC_WRITE, 0, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_OVERLAPPED, 0)
		if openErr == nil {
			return &namedPipeConn{file: os.NewFile(uintptr(handle), "jinushi-named-pipe")}, nil
		}
		if !errors.Is(openErr, windows.ERROR_PIPE_BUSY) && !errors.Is(openErr, windows.ERROR_FILE_NOT_FOUND) {
			return nil, fmt.Errorf("connect to local supervisor pipe: %w", openErr)
		}
		timer := time.NewTimer(30 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

type namedPipeConn struct {
	file *os.File
}

func (c *namedPipeConn) Read(p []byte) (int, error)  { return c.file.Read(p) }
func (c *namedPipeConn) Write(p []byte) (int, error) { return c.file.Write(p) }
func (c *namedPipeConn) Close() error                { return c.file.Close() }

func (c *namedPipeConn) LocalAddr() net.Addr                { return pipeAddr("local named pipe") }
func (c *namedPipeConn) RemoteAddr() net.Addr               { return pipeAddr("local named pipe client") }
func (c *namedPipeConn) SetDeadline(t time.Time) error      { return c.file.SetDeadline(t) }
func (c *namedPipeConn) SetReadDeadline(t time.Time) error  { return c.file.SetReadDeadline(t) }
func (c *namedPipeConn) SetWriteDeadline(t time.Time) error { return c.file.SetWriteDeadline(t) }

type pipeAddr string

func (a pipeAddr) Network() string { return "namedpipe" }
func (a pipeAddr) String() string  { return string(a) }

var _ net.Listener = (*namedPipeListener)(nil)
var _ net.Conn = (*namedPipeConn)(nil)
