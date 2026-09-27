package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	bbolt "go.etcd.io/bbolt"
)

const compactTransactionBytes = 16 << 20

// CompactionNeeded reports whether reclaimed bbolt pages meet the configured
// byte and ratio thresholds. It is a signal only; callers choose a quiescent
// compaction point and must not treat logical byte counts as physical size.
func (s *Store) CompactionNeeded(policy RetentionPolicy) (bool, error) {
	if err := validateRetentionPolicy(policy); err != nil {
		return false, err
	}
	if policy.CompactMinFreeBytes == 0 {
		return false, nil
	}
	usage, err := s.Usage()
	if err != nil {
		return false, err
	}
	if usage.FreeBytes < policy.CompactMinFreeBytes {
		return false, nil
	}
	if policy.CompactMinFreeRatio <= 0 {
		return true, nil
	}
	return float64(usage.FreeBytes)/float64(max(usage.DatabaseBytes, int64(1))) >= policy.CompactMinFreeRatio, nil
}

// CompactTo writes a compacted snapshot to a new path. The destination must
// not exist; callers that will atomically replace the source must choose a
// destination in the same directory. The caller must
// hold a Store-wide exclusive operation gate for the entire copy and
// cutover: bbolt Compact reads a consistent snapshot, but writes concurrent
// with that snapshot are not copied. This method does not replace s.db or the
// source path; it is the copy primitive for a coordinated online handoff.
func (s *Store) CompactTo(destination string) error {
	if destination == "" || filepath.Clean(destination) == filepath.Clean(s.db.Path()) {
		return errors.New("compaction destination must differ from the source database")
	}
	return compactDBToPath(s.db, destination)
}

// CompactDatabase compacts a closed store in place. The caller must ensure no
// Store handle or other supervisor can access path during this operation and
// must hold the same external supervisor lock used for normal state access.
// For a resident Store, use CompactTo while holding its Store-wide exclusive
// operation gate, then atomically hand the completed file to the reopened DB.
func CompactDatabase(path string) error {
	if path == "" {
		return errors.New("database path is empty")
	}
	src, srcGuard, err := openDatabase(path, 0o600, &bbolt.Options{})
	if err != nil {
		return fmt.Errorf("open source for compaction: %w", err)
	}
	defer func() {
		_ = src.Close()
		if srcGuard != nil {
			_ = srcGuard.Close()
		}
	}()
	temp, err := os.CreateTemp(filepath.Dir(path), ".jinushi-compact-*")
	if err != nil {
		return fmt.Errorf("create compaction file: %w", err)
	}
	tempPath := temp.Name()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("restrict compaction file: %w", err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close compaction file: %w", err)
	}
	defer os.Remove(tempPath)
	if err := os.Remove(tempPath); err != nil {
		return fmt.Errorf("prepare compaction destination: %w", err)
	}
	if err := compactDBToPath(src, tempPath); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace database with compacted copy: %w", err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync compacted database directory: %w", err)
	}
	return nil
}

func compactDBToPath(src *bbolt.DB, destination string) error {
	info, err := os.Lstat(destination)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect compaction destination: %w", err)
	}
	if err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("compaction destination is not a regular file")
		}
		return errors.New("compaction destination already exists")
	}
	created, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create compaction destination: %w", err)
	}
	if err := created.Close(); err != nil {
		_ = os.Remove(destination)
		return fmt.Errorf("close compaction destination: %w", err)
	}
	dst, dstGuard, err := openDatabase(destination, 0o600, &bbolt.Options{})
	if err != nil {
		_ = os.Remove(destination)
		return fmt.Errorf("open compaction destination: %w", err)
	}
	compactErr := bbolt.Compact(dst, src, compactTransactionBytes)
	if compactErr == nil {
		compactErr = dst.Sync()
	}
	closeErr := dst.Close()
	if dstGuard != nil {
		if guardErr := dstGuard.Close(); closeErr == nil {
			closeErr = guardErr
		}
	}
	if compactErr != nil {
		_ = os.Remove(destination)
		return fmt.Errorf("compact bbolt database: %w", compactErr)
	}
	if closeErr != nil {
		_ = os.Remove(destination)
		return fmt.Errorf("close compacted database: %w", closeErr)
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
