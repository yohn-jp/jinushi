//go:build linux

package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestCompactOnlineShrinksLiveDatabaseAndPreservesStoreState(t *testing.T) {
	dir := ownerOnlyTempDir(t)
	path := filepath.Join(dir, "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, _, err := s.Create(testRun("run-before-online-compact"), nil); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), 128<<10)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("online-compact-fixture"))
		if err != nil {
			return err
		}
		for index := 0; index < 96; index++ {
			if err := bucket.Put([]byte(fmt.Sprintf("discard-%03d", index)), payload); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("online-compact-fixture"))
		for index := 0; index < 96; index++ {
			if err := bucket.Delete([]byte(fmt.Sprintf("discard-%03d", index))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompactOnline(); err != nil {
		if errors.Is(err, ErrCompactionUnsupported) {
			t.Skipf("filesystem does not support atomic online compaction: %v", err)
		}
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("online compaction did not shrink the database: before=%d after=%d", before.Size(), after.Size())
	}
	if os.SameFile(before, after) {
		t.Fatal("online compaction did not replace the physical database inode")
	}
	if got := s.db.Path(); got != path {
		t.Fatalf("Store path = %q, want %q", got, path)
	}
	if _, err := s.Get("run-before-online-compact"); err != nil {
		t.Fatalf("live Store lost Run after compaction: %v", err)
	}
	if _, _, err := s.Create(testRun("run-after-online-compact"), nil); err != nil {
		t.Fatalf("live Store could not write after compaction: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("reopen compacted Store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	for _, id := range []string{"run-before-online-compact", "run-after-online-compact"} {
		if _, err := reopened.Get(id); err != nil {
			t.Errorf("reopened Store lost %s: %v", id, err)
		}
	}
}

func TestCompactOnlineSerializesConcurrentStoreOperations(t *testing.T) {
	s, err := Open(filepath.Join(ownerOnlyTempDir(t), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const writers = 4
	const runsPerWriter = 40
	var wg sync.WaitGroup
	errCh := make(chan error, writers+2)
	for writer := 0; writer < writers; writer++ {
		writer := writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := 0; index < runsPerWriter; index++ {
				id := fmt.Sprintf("concurrent-%02d-%03d", writer, index)
				if _, _, err := s.Create(testRun(id), nil); err != nil {
					errCh <- fmt.Errorf("create %s: %w", id, err)
					return
				}
				_ = s.db.Stats()
				_ = s.db.Info()
				_ = s.db.Path()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for index := 0; index < 20; index++ {
			if _, err := s.Usage(); err != nil {
				errCh <- fmt.Errorf("usage snapshot %d: %w", index, err)
				return
			}
			if _, err := s.CompactionNeeded(RetentionPolicy{CompactMinFreeBytes: 1}); err != nil {
				errCh <- fmt.Errorf("compaction recommendation %d: %w", index, err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for index := 0; index < 5; index++ {
			if err := s.CompactOnline(); err != nil {
				errCh <- fmt.Errorf("compaction %d: %w", index, err)
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if errors.Is(err, ErrCompactionUnsupported) {
			continue
		}
		t.Error(err)
	}

	for writer := 0; writer < writers; writer++ {
		for index := 0; index < runsPerWriter; index++ {
			id := fmt.Sprintf("concurrent-%02d-%03d", writer, index)
			if _, err := s.Get(id); err != nil {
				t.Errorf("Store lost accepted write %s: %v", id, err)
			}
		}
	}
}

func TestCompactOnlineRejectsSymlinkReplacement(t *testing.T) {
	dir := ownerOnlyTempDir(t)
	path := filepath.Join(dir, "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-guarded"), nil); err != nil {
		t.Fatal(err)
	}

	original := filepath.Join(dir, "state.original")
	if err := os.Rename(path, original); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "unrelated-target")
	if err := os.WriteFile(target, []byte("protected"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if err := s.CompactOnline(); err == nil || errors.Is(err, ErrCompactionUnsupported) {
		t.Fatalf("CompactOnline with a replaced symlink path error = %v", err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "protected" {
		t.Fatalf("symlink target changed: data=%q err=%v", got, err)
	}
	linkInfo, err := os.Lstat(path)
	if err != nil || linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("replacement symlink was changed: info=%v err=%v", linkInfo, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(original, path); err != nil {
		t.Fatal(err)
	}
}

func TestCompactOnlineFailsClosedWhenExchangeRollbackCannotBeProven(t *testing.T) {
	dir := ownerOnlyTempDir(t)
	path := filepath.Join(dir, "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-unproven-rollback"), nil); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "replacement-target")
	if err := os.WriteFile(target, []byte("protected"), 0o600); err != nil {
		t.Fatal(err)
	}

	realExchange := exchangeCompactionEntries
	exchangeCalls := 0
	var displacedName string
	movedCompacted := filepath.Join(dir, "moved-compacted.db")
	exchangeCompactionEntries = func(dirFD int, first, second string) error {
		exchangeCalls++
		if exchangeCalls == 2 {
			return errors.New("injected rollback exchange failure")
		}
		if err := realExchange(dirFD, first, second); err != nil {
			return err
		}
		displacedName = second
		if err := os.Rename(entryPath(dirFD, first), movedCompacted); err != nil {
			return err
		}
		return os.Symlink(target, entryPath(dirFD, first))
	}
	defer func() { exchangeCompactionEntries = realExchange }()

	err = s.CompactOnline()
	if errors.Is(err, ErrCompactionUnsupported) {
		t.Skipf("filesystem does not support atomic online compaction: %v", err)
	}
	if !errors.Is(err, ErrCompactionUncertain) {
		t.Fatalf("CompactOnline error = %v, want ErrCompactionUncertain", err)
	}
	if exchangeCalls != 2 {
		t.Fatalf("exchange calls = %d, want initial exchange and failed rollback", exchangeCalls)
	}
	if err := s.db.Update(func(*bolt.Tx) error { return nil }); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store write after unproven rollback = %v, want ErrStoreClosed", err)
	}
	if _, err := s.Get("run-unproven-rollback"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store read after unproven rollback = %v, want ErrStoreClosed", err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "protected" {
		t.Fatalf("replacement symlink target changed: data=%q err=%v", got, err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, displacedName), path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(movedCompacted); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Get("run-unproven-rollback"); err != nil {
		t.Fatalf("restored source database lost its Run: %v", err)
	}
}

func TestCompactOnlineKeepsBothInodesWhenExchangeSyncFails(t *testing.T) {
	dir := ownerOnlyTempDir(t)
	path := filepath.Join(dir, "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-sync-failure"), nil); err != nil {
		t.Fatal(err)
	}
	oldInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	realSync := syncCompactionDirectory
	syncCalls := 0
	syncCompactionDirectory = func(dirFD int) error {
		syncCalls++
		if syncCalls == 1 {
			return errors.New("injected directory sync failure")
		}
		return realSync(dirFD)
	}
	err = s.CompactOnline()
	syncCompactionDirectory = realSync
	if errors.Is(err, ErrCompactionUnsupported) {
		t.Skipf("filesystem does not support atomic online compaction: %v", err)
	}
	if !errors.Is(err, ErrCompactionUncertain) {
		t.Fatalf("CompactOnline error = %v, want ErrCompactionUncertain", err)
	}
	if syncCalls != 2 {
		t.Fatalf("directory sync calls = %d, want two attempts", syncCalls)
	}
	newInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(oldInfo, newInfo) {
		t.Fatal("new compacted inode is not active at the database path")
	}
	if _, err := s.Get("run-sync-failure"); err != nil {
		t.Fatalf("new database is not active in Store after sync failure: %v", err)
	}
	if _, _, err := s.Create(testRun("run-sync-failure-write"), nil); err != nil {
		t.Fatalf("Store write failed after uncertain sync: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var oldCopy string
	for _, entry := range entries {
		if validCompactionArtifactName(entry.Name()) {
			oldCopy = filepath.Join(dir, entry.Name())
			break
		}
	}
	if oldCopy == "" {
		t.Fatal("displaced source inode was removed after first directory sync failed")
	}
	oldCopyInfo, err := os.Stat(oldCopy)
	if err != nil || !os.SameFile(oldInfo, oldCopyInfo) {
		t.Fatalf("displaced path does not retain source inode: info=%v err=%v", oldCopyInfo, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := os.Lstat(oldCopy); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reopen did not clean verified displaced inode: err=%v", err)
	}
}

func TestCompactOnlineFailsClosedWhenBothExchangeSyncsFail(t *testing.T) {
	dir := ownerOnlyTempDir(t)
	path := filepath.Join(dir, "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, _, err := s.Create(testRun("run-double-sync-failure"), nil); err != nil {
		t.Fatal(err)
	}

	realSync := syncCompactionDirectory
	syncCalls := 0
	syncCompactionDirectory = func(int) error {
		syncCalls++
		return errors.New("injected directory sync failure")
	}
	err = s.CompactOnline()
	syncCompactionDirectory = realSync
	if errors.Is(err, ErrCompactionUnsupported) {
		t.Skipf("filesystem does not support atomic online compaction: %v", err)
	}
	if !errors.Is(err, ErrCompactionUncertain) {
		t.Fatalf("CompactOnline error = %v, want ErrCompactionUncertain", err)
	}
	if syncCalls != 2 {
		t.Fatalf("directory sync calls = %d, want two failed syncs", syncCalls)
	}
	if err := s.db.Update(func(*bolt.Tx) error { return nil }); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store write after unproven exchange durability = %v, want ErrStoreClosed", err)
	}
	if _, err := s.Get("run-double-sync-failure"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Store read after unproven exchange durability = %v, want ErrStoreClosed", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Get("run-double-sync-failure"); err != nil {
		t.Fatalf("reopened Store lost durable Run: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if validCompactionArtifactName(entry.Name()) {
			t.Errorf("reopen left verified displaced inode artifact %q", entry.Name())
		}
	}
}

func TestOpenCleansOnlyVerifiedOnlineCompactionArtifacts(t *testing.T) {
	dir := ownerOnlyTempDir(t)
	path := filepath.Join(dir, "state.db")
	validName := onlineCompactionPrefix + "0123456789abcdef0123456789abcdef"
	validPath := filepath.Join(dir, validName)
	if err := os.WriteFile(validPath, []byte("stale artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "symlink-target")
	if err := os.WriteFile(target, []byte("do not delete"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlinkName := onlineCompactionPrefix + "fedcba9876543210fedcba9876543210"
	symlinkPath := filepath.Join(dir, symlinkName)
	if err := os.Symlink(target, symlinkPath); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := os.Lstat(validPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verified stale artifact remains: err=%v", err)
	}
	if info, err := os.Lstat(symlinkPath); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("unverified symlink artifact was removed or changed: info=%v err=%v", info, err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "do not delete" {
		t.Fatalf("symlink target changed: data=%q err=%v", got, err)
	}
}

func TestCompactOnlineRequiresOwnerOnlyDirectory(t *testing.T) {
	dir := ownerOnlyTempDir(t)
	path := filepath.Join(dir, "state.db")
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o700)
		_ = s.Close()
	})
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.CompactOnline(); !errors.Is(err, ErrCompactionPathUnsafe) {
		t.Fatalf("CompactOnline with a public parent dir error = %v, want ErrCompactionPathUnsafe", err)
	}
}

func ownerOnlyTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}
