//go:build linux

package supervisor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// secureStateDir rejects symlinks in the state path and existing state items.
// The directory itself is chmodded through its verified descriptor.
func secureStateDir(root string) error {
	if !filepath.IsAbs(root) || root == "/" {
		return errors.New("state directory must be an absolute non-root path")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Clean(root), "/"), "/") {
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return fmt.Errorf("state path component %q is not a plain directory: %w", part, err)
		}
		unix.Close(fd)
		fd = next
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return err
	}
	if stat.Uid != uint32(os.Getuid()) {
		return errors.New("state directory owner differs from supervisor user")
	}
	if err := unix.Fchmod(fd, 0700); err != nil {
		return err
	}
	for _, name := range []string{"config.json", "state.db"} {
		if err := checkStateEntry(fd, name, false); err != nil {
			return err
		}
	}
	return checkStateEntry(fd, "runs", true)
}

func checkStateEntry(dirfd int, name string, directory bool) error {
	var stat unix.Stat_t
	err := unix.Fstatat(dirfd, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect state item %s: %w", name, err)
	}
	if stat.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("state item %s has a different owner", name)
	}
	typeBits := stat.Mode & unix.S_IFMT
	if directory && typeBits != unix.S_IFDIR || !directory && typeBits != unix.S_IFREG {
		return fmt.Errorf("state item %s has an unsafe file type", name)
	}
	return nil
}
