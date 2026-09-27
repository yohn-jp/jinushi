//go:build linux

package guardian

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestSecureDirDoesNotFollowSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "run")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := secureDir(link); err == nil {
		t.Fatal("secureDir followed a symlink")
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0755 {
		t.Fatalf("symlink target mode changed to %04o", info.Mode().Perm())
	}
}

func TestStateFileReadsRejectSymlinksAndAtomicWritesReplaceThemSafely(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "victim.json")
	const original = "outside-state-must-remain-unchanged"
	if err := os.WriteFile(victim, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(dir, statusName)); err != nil {
		t.Fatal(err)
	}
	if _, err := readSnapshot(dir); err == nil {
		t.Fatal("readSnapshot followed a symlink")
	}
	snapshot := Snapshot{Version: ProtocolVersion, RunID: "run_safe", State: model.Starting}
	if err := writeSnapshot(dir, snapshot); err != nil {
		t.Fatalf("atomic write replacing symlink: %v", err)
	}
	got, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != original {
		t.Fatalf("symlink target was modified: %q", got)
	}
	info, err := os.Lstat(filepath.Join(dir, statusName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatal("atomic state write left the symlink in place")
	}
}

func TestOpenSpoolRejectsSymlinkedStream(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "victim.out")
	if err := os.WriteFile(victim, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(dir, "stdout.out")); err != nil {
		t.Fatal(err)
	}
	if _, err := openSpool(dir, 3<<10); err == nil {
		t.Fatal("openSpool followed a symlinked stream")
	}
	got, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "preserve" {
		t.Fatalf("symlink target was modified: %q", got)
	}
}
