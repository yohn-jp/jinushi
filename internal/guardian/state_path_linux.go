//go:build linux

package guardian

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// openStateDirectory pins the final state directory without following a
// symlink. Guardian state directories are private to the current user.
func openStateDirectory(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || int(stat.Uid) != os.Getuid() {
		_ = unix.Close(fd)
		return nil, errors.New("guardian: state directory is not owned by the current user")
	}
	return os.NewFile(uintptr(fd), path), nil
}

func openStateFile(path string, flags int, mode os.FileMode) (*os.File, error) {
	dir, name := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	parent, err := openStateDirectory(filepath.Clean(dir))
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	fd, err := unix.Openat(int(parent.Fd()), name, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func atomicWriteState(path string, data []byte, mode os.FileMode) error {
	dirPath, name := filepath.Split(path)
	if dirPath == "" {
		dirPath = "."
	}
	dir, err := openStateDirectory(filepath.Clean(dirPath))
	if err != nil {
		return fmt.Errorf("guardian: open state directory: %w", err)
	}
	defer dir.Close()

	var temp string
	var file *os.File
	for attempt := 0; attempt < 8; attempt++ {
		var nonce [12]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return fmt.Errorf("guardian: create temporary state identity: %w", err)
		}
		temp = ".guardian-" + hex.EncodeToString(nonce[:]) + ".tmp"
		fd, openErr := unix.Openat(int(dir.Fd()), temp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(mode.Perm()))
		if errors.Is(openErr, unix.EEXIST) {
			continue
		}
		if openErr != nil {
			return fmt.Errorf("guardian: create temporary state: %w", openErr)
		}
		file = os.NewFile(uintptr(fd), temp)
		break
	}
	if file == nil {
		return errors.New("guardian: could not allocate temporary state file")
	}
	defer func() { _ = unix.Unlinkat(int(dir.Fd()), temp, 0) }()
	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		return fmt.Errorf("guardian: restrict temporary state permissions: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("guardian: write temporary state: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("guardian: sync temporary state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("guardian: close temporary state: %w", err)
	}
	if err := unix.Renameat(int(dir.Fd()), temp, int(dir.Fd()), name); err != nil {
		return fmt.Errorf("guardian: replace state: %w", err)
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("guardian: sync state directory: %w", err)
	}
	return nil
}

func secureDir(path string) error {
	dir, err := openStateDirectory(path)
	if err != nil {
		return fmt.Errorf("guardian: open private state directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Chmod(0700); err != nil {
		return fmt.Errorf("guardian: restrict state directory permissions: %w", err)
	}
	return nil
}

func syncDir(path string) error {
	dir, err := openStateDirectory(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
