//go:build linux

package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenRejectsDatabaseSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "outside.db")
	link := filepath.Join(dir, "state.db")
	if err := os.WriteFile(target, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(link, Options{}); err == nil {
		t.Fatal("Open accepted a symlink database path")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "unchanged" {
		t.Fatalf("symlink target changed to %q", got)
	}
}

func TestOpenPinsDatabaseInodeAcrossPathReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")
	target := filepath.Join(dir, "outside.db")
	if err := os.WriteFile(target, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(testRun("run-pinned-db"), nil); err != nil {
		t.Fatalf("write through pinned database: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "unchanged" {
		t.Fatalf("replacement target changed to %q", got)
	}
	if _, err := Open(path, Options{}); err == nil {
		t.Fatal("Open followed the replacement symlink")
	}
}
