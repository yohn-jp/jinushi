//go:build linux

package store

import (
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

// openDatabase pins the final database inode without following a symlink. Bolt
// opens /proc/self/fd/N, which resolves to that pinned inode even if the name
// is replaced after validation. The guard fd remains open for the Store's
// lifetime so the proc descriptor cannot be reused.
func openDatabase(path string, mode os.FileMode, options *bolt.Options) (*bolt.DB, *os.File, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, uint32(mode.Perm()))
	if err != nil {
		return nil, nil, fmt.Errorf("open database without following symlinks: %w", err)
	}
	guard := os.NewFile(uintptr(fd), path)
	if guard == nil {
		_ = unix.Close(fd)
		return nil, nil, fmt.Errorf("open database: invalid file descriptor")
	}
	closeGuard := func(cause error) (*bolt.DB, *os.File, error) {
		_ = guard.Close()
		return nil, nil, cause
	}
	stat, err := guard.Stat()
	if err != nil {
		return closeGuard(fmt.Errorf("inspect database file: %w", err))
	}
	if !stat.Mode().IsRegular() {
		return closeGuard(fmt.Errorf("database path is not a regular file"))
	}
	var raw unix.Stat_t
	if err := unix.Fstat(fd, &raw); err != nil {
		return closeGuard(fmt.Errorf("inspect database ownership: %w", err))
	}
	if raw.Uid != uint32(unix.Geteuid()) {
		return closeGuard(fmt.Errorf("database file is not owned by the current user"))
	}
	if raw.Nlink != 1 {
		return closeGuard(fmt.Errorf("database file has unexpected hard links"))
	}
	if err := guard.Chmod(mode.Perm()); err != nil {
		return closeGuard(fmt.Errorf("restrict database permissions: %w", err))
	}
	db, err := bolt.Open(fmt.Sprintf("/proc/self/fd/%d", fd), mode, options)
	if err != nil {
		return closeGuard(err)
	}
	return db, guard, nil
}
