//go:build !windows

package guardian

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLongRunDirectoryUsesPrivateShortControlSocket(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), strings.Repeat("state-root-", 8), "runs", strings.Repeat("r", 40))
	originalEndpoint, err := filepath.Abs(filepath.Join(runDir, "jinushi.sock"))
	if err != nil {
		t.Fatal(err)
	}
	if len(originalEndpoint) < len(syscall.RawSockaddrUnix{}.Path) {
		t.Fatalf("test endpoint unexpectedly fits Unix socket path: %d bytes", len(originalEndpoint))
	}

	listener, err := listenControl(runDir)
	if err != nil {
		t.Fatalf("listen with long Run directory: %v", err)
	}
	endpointDir, shortened, err := controlEndpointDir(runDir)
	if err != nil || !shortened {
		t.Fatalf("controlEndpointDir() = %q, %v, %v; want a shortened path", endpointDir, shortened, err)
	}
	endpoint, err := filepath.Abs(filepath.Join(endpointDir, "jinushi.sock"))
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoint) >= len(syscall.RawSockaddrUnix{}.Path) {
		t.Fatalf("short endpoint still exceeds Unix socket path limit: %d bytes", len(endpoint))
	}
	t.Cleanup(func() {
		_ = listener.Close()
		_ = os.Remove(endpointDir)
	})
	info, err := os.Stat(endpointDir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0700 {
		t.Fatalf("control directory mode = %04o, want 0700", info.Mode().Perm())
	}

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	conn, err := dialControl(context.Background(), runDir)
	if err != nil {
		t.Fatalf("dial with long Run directory: %v", err)
	}
	defer conn.Close()
	select {
	case serverConn := <-accepted:
		_ = serverConn.Close()
	case err := <-acceptErr:
		t.Fatalf("accept shortened local control connection: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	cleanupControl(runDir)
	if _, err := os.Lstat(endpointDir); !os.IsNotExist(err) {
		t.Fatalf("shortened per-Run control directory was not removed: %v", err)
	}
}

func TestControlListenerFailsClosedWhenRunDirectoryIsReplaced(t *testing.T) {
	for _, name := range []string{"direct", "shortened"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TMPDIR", "/tmp")
			root := t.TempDir()
			if name == "shortened" {
				root = filepath.Join(root, strings.Repeat("long-state-path-", 8))
				if err := os.MkdirAll(root, 0700); err != nil {
					t.Fatal(err)
				}
			}
			runDir := filepath.Join(root, "run")
			if err := os.Mkdir(runDir, 0700); err != nil {
				t.Fatal(err)
			}
			endpointDir, shortened, err := controlEndpointDir(runDir)
			if err != nil || shortened != (name == "shortened") {
				t.Fatalf("control endpoint mode = %q, %v, %v", endpointDir, shortened, err)
			}
			listener, err := listenControl(runDir)
			if err != nil {
				t.Fatalf("listen on Run directory: %v", err)
			}
			defer listener.Close()
			moved := filepath.Join(root, "original-run")
			if err := os.Rename(runDir, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(runDir, 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if conn, err := dialControl(ctx, runDir); err == nil {
				_ = conn.Close()
				t.Fatal("dial connected through a replacement Run directory")
			}
			if name == "direct" {
				endpointDir = moved
			}
			if _, err := os.Lstat(filepath.Join(endpointDir, "jinushi.sock")); err != nil {
				t.Fatalf("pinned listener socket was lost after path replacement: %v", err)
			}
		})
	}
}

func TestControlListenerRejectsSymlinkEndpoint(t *testing.T) {
	runDir := t.TempDir()
	server, err := net.Listen("unix", filepath.Join(t.TempDir(), "other.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := os.Symlink(server.Addr().String(), filepath.Join(runDir, "jinushi.sock")); err != nil {
		t.Fatal(err)
	}
	if _, err := listenControl(runDir); err == nil {
		t.Fatal("listener accepted a symlink at the control endpoint")
	}
}
