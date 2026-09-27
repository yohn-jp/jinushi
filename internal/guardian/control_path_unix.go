//go:build !windows

package guardian

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yohn-jp/jinushi/internal/ipc"
)

func listenControl(stateDir string) (net.Listener, error) {
	endpointDir, shortened, err := controlEndpointDir(stateDir)
	if err != nil {
		return nil, err
	}
	if shortened {
		if err := ensurePrivateControlDir(endpointDir); err != nil {
			return nil, err
		}
	}
	return ipc.Listen(endpointDir)
}

func dialControl(ctx context.Context, stateDir string) (net.Conn, error) {
	endpointDir, _, err := controlEndpointDir(stateDir)
	if err != nil {
		return nil, err
	}
	return ipc.Dial(ctx, endpointDir)
}

func cleanupControl(stateDir string) {
	endpointDir, shortened, err := controlEndpointDir(stateDir)
	if err != nil || !shortened {
		return
	}
	info, err := os.Lstat(endpointDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(owner.Uid) != os.Getuid() {
		return
	}
	_ = os.Remove(endpointDir)
}

func controlEndpointDir(stateDir string) (string, bool, error) {
	endpoint, err := ipc.Endpoint(stateDir)
	if err != nil {
		return "", false, err
	}
	if len(endpoint) < len(syscall.RawSockaddrUnix{}.Path) {
		return stateDir, false, nil
	}

	abs, err := filepath.Abs(stateDir)
	if err != nil {
		return "", false, err
	}
	hash := sha256.Sum256([]byte(filepath.Clean(abs)))
	name := fmt.Sprintf("jinushi-%d-%x", os.Getuid(), hash[:])
	for _, base := range []string{"/tmp", os.TempDir(), "/var/tmp"} {
		base = filepath.Clean(base)
		if len(filepath.Join(base, name, "jinushi.sock")) >= len(syscall.RawSockaddrUnix{}.Path) {
			continue
		}
		info, statErr := os.Stat(base)
		if statErr == nil && info.IsDir() {
			return filepath.Join(base, name), true, nil
		}
	}
	return "", false, errors.New("guardian: no short local control socket path is available")
}

func ensurePrivateControlDir(path string) error {
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return errors.New("guardian: create private control directory failed")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("guardian: control directory is not a private directory")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(owner.Uid) != os.Getuid() {
		return errors.New("guardian: control directory owner mismatch")
	}
	if info.Mode().Perm() != 0700 {
		if err := os.Chmod(path, 0700); err != nil {
			return errors.New("guardian: restrict control directory permissions failed")
		}
	}
	return nil
}
