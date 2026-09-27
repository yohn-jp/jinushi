//go:build !windows

package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Endpoint returns the Unix-domain socket path inside the supervisor state directory.
func Endpoint(stateDir string) (string, error) {
	if stateDir == "" {
		return "", fmt.Errorf("state directory is empty")
	}
	abs, err := filepath.Abs(stateDir)
	if err != nil {
		return "", fmt.Errorf("resolve state directory: %w", err)
	}
	return filepath.Join(abs, "jinushi.sock"), nil
}

func listenEndpoint(endpoint string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(endpoint), 0700); err != nil {
		return nil, fmt.Errorf("create supervisor state directory: %w", err)
	}
	if info, err := os.Lstat(endpoint); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("IPC endpoint exists and is not a socket: %s", endpoint)
		}
		probe, dialErr := net.DialTimeout("unix", endpoint, 100*time.Millisecond)
		if dialErr == nil {
			probe.Close()
			return nil, fmt.Errorf("supervisor IPC endpoint is already active: %s", endpoint)
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
			return nil, fmt.Errorf("probe existing IPC endpoint: %w", dialErr)
		}
		if err := os.Remove(endpoint); err != nil {
			return nil, fmt.Errorf("remove stale IPC socket: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect IPC endpoint: %w", err)
	}
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(endpoint, 0600); err != nil {
		listener.Close()
		os.Remove(endpoint)
		return nil, fmt.Errorf("restrict IPC socket permissions: %w", err)
	}
	return listener, nil
}

func dialEndpoint(ctx context.Context, endpoint string) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "unix", endpoint)
}
