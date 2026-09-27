//go:build windows

package ipc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"strings"
)

// Endpoint returns a stable local named-pipe name derived from the state directory.
func Endpoint(stateDir string) (string, error) {
	if strings.TrimSpace(stateDir) == "" {
		return "", fmt.Errorf("state directory is empty")
	}
	abs, err := filepath.Abs(stateDir)
	if err != nil {
		return "", fmt.Errorf("resolve state directory: %w", err)
	}
	hash := sha256.Sum256([]byte(strings.ToLower(filepath.Clean(abs))))
	return `\\.\pipe\jinushi-` + hex.EncodeToString(hash[:16]), nil
}

func listenEndpoint(endpoint string) (net.Listener, error) { return listenNamedPipe(endpoint) }

func dialEndpoint(ctx context.Context, endpoint string) (net.Conn, error) {
	return dialNamedPipe(ctx, endpoint)
}
