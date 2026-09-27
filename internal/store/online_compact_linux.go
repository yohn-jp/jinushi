//go:build linux

package store

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	bbolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

const onlineCompactionPrefix = ".jinushi-online-compact-"

type fileIdentity struct {
	dev uint64
	ino uint64
}

// These indirections keep the production syscalls direct while allowing tests
// to exercise failures after a real exchange has taken place.
var (
	exchangeCompactionEntries = exchangeCompactionEntriesLinux
	syncCompactionDirectory   = unix.Fsync
)

func compactOnlineLocked(s *Store) error {
	dirFD, databaseName, err := openOwnerOnlyDatabaseDirectory(s.path)
	if err != nil {
		return err
	}
	defer unix.Close(dirFD)

	remaining, err := cleanupCompactionArtifactsAt(dirFD, maxCompactionArtifactsPerPass)
	if err != nil {
		return fmt.Errorf("clean stale online compaction artifacts: %w", err)
	}
	if remaining {
		return ErrCompactionArtifactsPending
	}
	if s.dbGuard == nil {
		return fmt.Errorf("online compaction requires a pinned source database inode")
	}
	oldIdentity, err := identityFromFile(s.dbGuard)
	if err != nil {
		return fmt.Errorf("inspect source database guard: %w", err)
	}
	if err := entryMatchesIdentity(dirFD, databaseName, oldIdentity); err != nil {
		return fmt.Errorf("source database path no longer names the pinned inode: %w", err)
	}

	tempName, err := newCompactionArtifactName(dirFD)
	if err != nil {
		return fmt.Errorf("choose online compaction path: %w", err)
	}
	tempPath := entryPath(dirFD, tempName)
	if err := compactDBToPath(s.rawDB, tempPath); err != nil {
		return err
	}
	var compactedDB *bbolt.DB
	var compactedGuard *os.File
	var newIdentity fileIdentity
	var haveNewIdentity bool
	cutover := false
	defer func() {
		if compactedGuard != nil {
			if identity, identityErr := identityFromFile(compactedGuard); identityErr == nil {
				newIdentity = identity
				haveNewIdentity = true
			}
		}
		if compactedDB != nil {
			_ = compactedDB.Close()
		}
		if compactedGuard != nil {
			_ = compactedGuard.Close()
		}
		if !cutover && haveNewIdentity {
			_ = removeCompactionArtifactAt(dirFD, tempName, &newIdentity)
		}
	}()
	compactedDB, compactedGuard, err = openDatabase(tempPath, 0o600, &bbolt.Options{Timeout: 0})
	if err != nil {
		return fmt.Errorf("reopen compacted database before cutover: %w", err)
	}
	newIdentity, err = identityFromFile(compactedGuard)
	if err != nil {
		return fmt.Errorf("inspect compacted database guard: %w", err)
	}
	haveNewIdentity = true
	if err := entryMatchesIdentity(dirFD, tempName, newIdentity); err != nil {
		return fmt.Errorf("compacted database path no longer names its pinned inode: %w", err)
	}
	if err := entryMatchesIdentity(dirFD, databaseName, oldIdentity); err != nil {
		return fmt.Errorf("source database path changed before cutover: %w", err)
	}

	if err := exchangeCompactionEntries(dirFD, databaseName, tempName); err != nil {
		return err
	}
	if newAtSource := entryMatchesIdentity(dirFD, databaseName, newIdentity); newAtSource != nil {
		rollbackErr := error(nil)
		rollbackProven := false
		if entryMatchesIdentity(dirFD, tempName, oldIdentity) == nil {
			rollbackErr = exchangeCompactionEntries(dirFD, databaseName, tempName)
			if sourceErr := entryMatchesIdentity(dirFD, databaseName, oldIdentity); sourceErr == nil {
				rollbackProven = true
			} else {
				rollbackErr = errors.Join(rollbackErr, fmt.Errorf("verify source after rollback: %w", sourceErr))
			}
		} else {
			rollbackErr = errors.New("displaced path does not name the pinned source inode")
		}
		if !rollbackProven {
			closeErr := disableStoreUnderGate(s)
			return errors.Join(
				fmt.Errorf("%w: exchange rollback was not proven", ErrCompactionUncertain),
				fmt.Errorf("%w: compacted inode did not become the database path: %v", ErrCompactionOwnershipChanged, newAtSource),
				rollbackErr,
				closeErr,
			)
		}
		return errors.Join(
			fmt.Errorf("%w: compacted inode did not become the database path and original inode was restored: %v", ErrCompactionOwnershipChanged, newAtSource),
		)
	}
	displacedMatchesSource := entryMatchesIdentity(dirFD, tempName, oldIdentity) == nil

	oldDB := s.rawDB
	oldGuard := s.dbGuard
	s.rawDB = compactedDB
	s.dbGuard = compactedGuard
	compactedDB = nil
	compactedGuard = nil
	cutover = true

	firstSyncErr := syncCompactionDirectory(dirFD)
	var maintenanceErr error
	if firstSyncErr != nil {
		maintenanceErr = errors.Join(maintenanceErr, fmt.Errorf("sync database exchange before removing rollback inode: %w", firstSyncErr))
	}
	if err := oldDB.Close(); err != nil {
		maintenanceErr = errors.Join(maintenanceErr, fmt.Errorf("close displaced database handle: %w", err))
	}
	if err := oldGuard.Close(); err != nil {
		maintenanceErr = errors.Join(maintenanceErr, fmt.Errorf("close displaced database guard: %w", err))
	}
	if firstSyncErr == nil && displacedMatchesSource {
		if err := removeCompactionArtifactAt(dirFD, tempName, &oldIdentity); err != nil {
			maintenanceErr = errors.Join(maintenanceErr, fmt.Errorf("remove displaced database: %w", err))
		}
	} else if firstSyncErr == nil {
		maintenanceErr = errors.Join(maintenanceErr, fmt.Errorf("%w: displaced path no longer names the pinned source inode", ErrCompactionOwnershipChanged))
	}
	finalSyncErr := syncCompactionDirectory(dirFD)
	if finalSyncErr != nil {
		maintenanceErr = errors.Join(maintenanceErr, fmt.Errorf("sync compacted database directory: %w", finalSyncErr))
	}
	if firstSyncErr != nil && finalSyncErr != nil {
		maintenanceErr = errors.Join(maintenanceErr, errors.New("both directory sync attempts failed after inode exchange; Store closed fail-closed"))
		maintenanceErr = errors.Join(maintenanceErr, disableStoreUnderGate(s))
	}
	if maintenanceErr != nil {
		return fmt.Errorf("%w: %w", ErrCompactionUncertain, maintenanceErr)
	}
	return nil
}

func openOwnerOnlyDatabaseDirectory(path string) (int, string, error) {
	directory := filepath.Dir(path)
	name := filepath.Base(path)
	fd, err := unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, "", fmt.Errorf("open database directory without following symlinks: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return -1, "", fmt.Errorf("inspect database directory: %w", err)
	}
	if stat.Uid != uint32(unix.Geteuid()) || stat.Mode&0o077 != 0 || stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		_ = unix.Close(fd)
		return -1, "", fmt.Errorf("%w: directory mode=%#o owner=%d euid=%d", ErrCompactionPathUnsafe, stat.Mode, stat.Uid, unix.Geteuid())
	}
	return fd, name, nil
}

func entryPath(dirFD int, name string) string {
	return fmt.Sprintf("/proc/self/fd/%d/%s", dirFD, name)
}

func identityFromFile(file *os.File) (fileIdentity, error) {
	var stat unix.Stat_t
	if file == nil {
		return fileIdentity{}, errors.New("file guard is nil")
	}
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return fileIdentity{}, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != uint32(unix.Geteuid()) || stat.Nlink != 1 || stat.Mode&0o077 != 0 {
		return fileIdentity{}, errors.New("database guard does not refer to an owner-only regular inode")
	}
	return fileIdentity{dev: uint64(stat.Dev), ino: stat.Ino}, nil
}

func entryMatchesIdentity(dirFD int, name string, expected fileIdentity) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(dirFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != uint32(unix.Geteuid()) || stat.Nlink != 1 || stat.Mode&0o077 != 0 {
		return errors.New("entry is not an owner-owned, unlinked regular file")
	}
	if uint64(stat.Dev) != expected.dev || stat.Ino != expected.ino {
		return errors.New("entry inode differs from pinned inode")
	}
	return nil
}

func newCompactionArtifactName(dirFD int) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", err
		}
		name := onlineCompactionPrefix + hex.EncodeToString(random[:])
		var stat unix.Stat_t
		err := unix.Fstatat(dirFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(err, unix.ENOENT) {
			return name, nil
		}
		if err != nil {
			return "", err
		}
	}
	return "", errors.New("could not allocate unique compaction artifact name")
}

func exchangeCompactionEntriesLinux(dirFD int, first, second string) error {
	err := unix.Renameat2(dirFD, first, dirFD, second, unix.RENAME_EXCHANGE)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
		return fmt.Errorf("%w: filesystem does not support atomic inode exchange: %v", ErrCompactionUnsupported, err)
	}
	return fmt.Errorf("exchange compacted database inode: %w", err)
}

func disableStoreUnderGate(s *Store) error {
	db, guard := s.rawDB, s.dbGuard
	s.rawDB, s.dbGuard = nil, nil
	var closeErr error
	if db != nil {
		if err := db.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close database after unproven cutover: %w", err))
		}
	}
	if guard != nil {
		if err := guard.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close database guard after unproven cutover: %w", err))
		}
	}
	return closeErr
}

func validCompactionArtifactName(name string) bool {
	if !strings.HasPrefix(name, onlineCompactionPrefix) {
		return false
	}
	suffix := strings.TrimPrefix(name, onlineCompactionPrefix)
	if len(suffix) != 32 {
		return false
	}
	for _, ch := range suffix {
		if !(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

func cleanupCompactionArtifacts(path string) {
	dirFD, _, err := openOwnerOnlyDatabaseDirectory(path)
	if err != nil {
		return
	}
	defer unix.Close(dirFD)
	_, _ = cleanupCompactionArtifactsAt(dirFD, maxCompactionArtifactsPerPass)
}

func cleanupCompactionArtifactsAt(dirFD int, maxRemove int) (bool, error) {
	entries, err := os.Open(entryPath(dirFD, "."))
	if err != nil {
		return false, err
	}
	defer entries.Close()
	const maxScannedDirectoryEntries = 512
	scanned, removed, remaining := 0, 0, false
	for scanned < maxScannedDirectoryEntries {
		batchSize := min(64, maxScannedDirectoryEntries-scanned)
		batch, readErr := entries.ReadDir(batchSize)
		scanned += len(batch)
		for _, entry := range batch {
			if !validCompactionArtifactName(entry.Name()) {
				continue
			}
			if removed >= maxRemove {
				remaining = true
				continue
			}
			if err := removeCompactionArtifactAt(dirFD, entry.Name(), nil); err == nil {
				removed++
			} else {
				remaining = true
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return true, readErr
		}
		if len(batch) == 0 {
			break
		}
	}
	if scanned == maxScannedDirectoryEntries {
		more, readErr := entries.ReadDir(1)
		if len(more) != 0 || readErr != io.EOF {
			remaining = true
		}
	}
	return remaining, nil
}

func removeCompactionArtifactAt(dirFD int, name string, expected *fileIdentity) error {
	if !validCompactionArtifactName(name) {
		return errors.New("refusing to remove a non-compaction path")
	}
	fd, err := unix.Openat(dirFD, name, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		return err
	}
	identity := fileIdentity{dev: uint64(opened.Dev), ino: opened.Ino}
	if opened.Mode&unix.S_IFMT != unix.S_IFREG || opened.Uid != uint32(unix.Geteuid()) || opened.Nlink != 1 || opened.Mode&0o077 != 0 {
		return errors.New("refusing to remove an unsafe compaction artifact")
	}
	if expected != nil && identity != *expected {
		return errors.New("refusing to remove a different compaction inode")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("compaction artifact is still in use: %w", err)
	}
	defer unix.Flock(fd, unix.LOCK_UN)
	var current unix.Stat_t
	if err := unix.Fstatat(dirFD, name, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if current.Mode&unix.S_IFMT != unix.S_IFREG || current.Uid != uint32(unix.Geteuid()) || current.Nlink != 1 || current.Mode&0o077 != 0 || uint64(current.Dev) != identity.dev || current.Ino != identity.ino {
		return errors.New("compaction artifact changed before removal")
	}
	if err := unix.Unlinkat(dirFD, name, 0); err != nil {
		return err
	}
	return nil
}
