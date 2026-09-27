//go:build linux

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

	"golang.org/x/sys/unix"

	"github.com/yohn-jp/jinushi/internal/ipc"
)

const guardianSocketName = "jinushi.sock"

type pinnedControlListener struct {
	listener  *net.UnixListener
	directory *os.File
}

func (l *pinnedControlListener) Accept() (net.Conn, error) { return l.listener.Accept() }
func (l *pinnedControlListener) Addr() net.Addr            { return l.listener.Addr() }
func (l *pinnedControlListener) Close() error {
	err := l.listener.Close()
	closeErr := l.directory.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func listenControl(stateDir string) (net.Listener, error) {
	endpointDir, shortened, err := controlEndpointDir(stateDir)
	if err != nil {
		return nil, err
	}
	if shortened {
		if err := ensurePrivateControlDir(endpointDir); err != nil {
			return nil, err
		}
	} else {
		if err := os.MkdirAll(endpointDir, 0700); err != nil {
			return nil, fmt.Errorf("guardian: create control directory: %w", err)
		}
		if err := secureDir(endpointDir); err != nil {
			return nil, err
		}
	}
	dir, err := openStateDirectory(endpointDir)
	if err != nil {
		return nil, fmt.Errorf("guardian: pin control directory: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(dir.Fd()), &stat); err != nil {
		_ = dir.Close()
		return nil, err
	}
	if stat.Mode&0777 != 0700 || int(stat.Uid) != os.Getuid() {
		_ = dir.Close()
		return nil, errors.New("guardian: control directory is not private")
	}
	if err := removeStaleControlSocket(int(dir.Fd()), controlSocketPath(dir)); err != nil {
		_ = dir.Close()
		return nil, err
	}
	path := controlSocketPath(dir)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		_ = dir.Close()
		return nil, fmt.Errorf("guardian: listen on pinned control directory: %w", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		_ = listener.Close()
		_ = dir.Close()
		return nil, fmt.Errorf("guardian: restrict control socket permissions: %w", err)
	}
	return &pinnedControlListener{listener: listener, directory: dir}, nil
}

func dialControl(ctx context.Context, stateDir string) (net.Conn, error) {
	endpointDir, _, err := controlEndpointDir(stateDir)
	if err != nil {
		return nil, err
	}
	dir, err := openStateDirectory(endpointDir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(int(dir.Fd()), &stat); err != nil {
		return nil, err
	}
	if stat.Mode&0777 != 0700 || int(stat.Uid) != os.Getuid() {
		return nil, errors.New("guardian: control directory is not private")
	}
	var dialer net.Dialer
	return dialer.DialContext(ctx, "unix", controlSocketPath(dir))
}

func controlSocketPath(dir *os.File) string {
	return fmt.Sprintf("/proc/self/fd/%d/%s", dir.Fd(), guardianSocketName)
}

func removeStaleControlSocket(dirFD int, procPath string) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(dirFD, guardianSocketName, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return fmt.Errorf("guardian: inspect control socket: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return errors.New("guardian: control endpoint exists and is not a socket")
	}
	probe, err := net.DialTimeout("unix", procPath, 100_000_000)
	if err == nil {
		_ = probe.Close()
		return errors.New("guardian: control endpoint is already active")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("guardian: probe existing control endpoint: %w", err)
	}
	if err := unix.Unlinkat(dirFD, guardianSocketName, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("guardian: remove stale control socket: %w", err)
	}
	return nil
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
	if !ok || int(owner.Uid) != os.Getuid() || info.Mode().Perm() != 0700 {
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
		if len(filepath.Join(base, name, guardianSocketName)) >= len(syscall.RawSockaddrUnix{}.Path) {
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
	if err := secureDir(path); err != nil {
		return errors.New("guardian: control directory is not a private directory")
	}
	dir, err := openStateDirectory(path)
	if err != nil {
		return errors.New("guardian: control directory is not a private directory")
	}
	defer dir.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(int(dir.Fd()), &stat); err != nil || int(stat.Uid) != os.Getuid() || stat.Mode&0777 != 0700 {
		return errors.New("guardian: control directory owner or mode mismatch")
	}
	return nil
}
