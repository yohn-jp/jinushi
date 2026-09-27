package store

import (
	"errors"
	"fmt"
	"os"

	bbolt "go.etcd.io/bbolt"
)

var (
	// ErrCompactionUnsupported reports that atomic live compaction is not
	// implemented on the current platform.
	ErrCompactionUnsupported = errors.New("online database compaction is unsupported on this platform")
	// ErrCompactionUncertain means compaction cutover or rollback could not be
	// proven complete. Post-cutover maintenance failures keep the compacted DB
	// active; an unproven rollback closes the Store fail-closed.
	ErrCompactionUncertain        = errors.New("online database compaction cutover has uncertain maintenance state")
	ErrCompactionOwnershipChanged = errors.New("online database compaction inode ownership changed during cutover")
	ErrCompactionPathUnsafe       = errors.New("online database compaction requires an owner-only database directory")
	ErrCompactionArtifactsPending = errors.New("online database compaction has stale or unsafe temporary artifacts")
)

const maxCompactionArtifactsPerPass = 32

// operationDB is the Store's bbolt call surface. Each operation holds a read
// lease until the transaction or metadata read finishes. CompactOnline holds
// the matching exclusive Store gate across its entire copy and inode swap.
type operationDB struct {
	store *Store
}

type databaseUsageSnapshot struct {
	stats         bbolt.Stats
	pageSize      int
	databaseBytes int64
}

func (db *operationDB) View(fn func(*bbolt.Tx) error) error {
	s := db.store
	s.opGate.RLock()
	defer s.opGate.RUnlock()
	if s.rawDB == nil {
		return ErrStoreClosed
	}
	return s.rawDB.View(fn)
}

func (db *operationDB) Update(fn func(*bbolt.Tx) error) error {
	s := db.store
	s.opGate.RLock()
	defer s.opGate.RUnlock()
	if s.rawDB == nil {
		return ErrStoreClosed
	}
	return s.rawDB.Update(fn)
}

func (db *operationDB) Stats() bbolt.Stats {
	s := db.store
	s.opGate.RLock()
	defer s.opGate.RUnlock()
	if s.rawDB == nil {
		return bbolt.Stats{}
	}
	return s.rawDB.Stats()
}

func (db *operationDB) Info() *bbolt.Info {
	s := db.store
	// bbolt.Info reads mmap state without its own lock. Exclude transactions,
	// which can remap the database while extending it.
	s.opGate.Lock()
	defer s.opGate.Unlock()
	if s.rawDB == nil {
		return &bbolt.Info{}
	}
	info := *s.rawDB.Info()
	return &info
}

func (db *operationDB) Path() string {
	s := db.store
	s.opGate.RLock()
	defer s.opGate.RUnlock()
	return s.path
}

// databaseUsageSnapshot holds the exclusive gate across all physical metadata
// reads. bbolt.Info is not synchronized with mmap changes, and separating the
// file stat from Stats could mix the source and compacted inodes.
func (db *operationDB) databaseUsageSnapshot() (databaseUsageSnapshot, error) {
	s := db.store
	s.opGate.Lock()
	defer s.opGate.Unlock()
	if s.rawDB == nil {
		return databaseUsageSnapshot{}, ErrStoreClosed
	}
	stats := s.rawDB.Stats()
	pageSize := s.rawDB.Info().PageSize
	var fileInfo os.FileInfo
	var err error
	if s.dbGuard == nil {
		fileInfo, err = os.Stat(s.path)
		if err != nil {
			return databaseUsageSnapshot{}, fmt.Errorf("stat active bbolt database: %w", err)
		}
	} else {
		fileInfo, err = s.dbGuard.Stat()
		if err != nil {
			return databaseUsageSnapshot{}, fmt.Errorf("stat active bbolt database: %w", err)
		}
		pathInfo, err := os.Lstat(s.path)
		if err != nil {
			return databaseUsageSnapshot{}, fmt.Errorf("inspect active bbolt database path: %w", err)
		}
		if pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(fileInfo, pathInfo) {
			return databaseUsageSnapshot{}, errors.New("active bbolt path no longer names the Store's database inode")
		}
	}
	return databaseUsageSnapshot{stats: stats, pageSize: pageSize, databaseBytes: fileInfo.Size()}, nil
}

// CompactOnline compacts the live database and atomically hands the Store to
// the compacted inode. The operation gate excludes every Store transaction and
// database metadata read until the compacted file is active and reopened.
// Linux filesystems without RENAME_EXCHANGE and non-Linux platforms return
// ErrCompactionUnsupported; no non-atomic replacement fallback is used.
func (s *Store) CompactOnline() error {
	if s == nil {
		return ErrStoreClosed
	}
	s.opGate.Lock()
	defer s.opGate.Unlock()
	if s.rawDB == nil {
		return ErrStoreClosed
	}
	return compactOnlineLocked(s)
}
