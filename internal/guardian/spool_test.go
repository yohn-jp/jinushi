package guardian

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSpoolFloodRetainsBoundedTailAndReportsGap(t *testing.T) {
	spoolDir := filepath.Join(t.TempDir(), "run")
	const aggregateLimit = int64(96 << 10)
	const retainedLimit = aggregateLimit / 3
	spool, err := openSpool(spoolDir, aggregateLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()

	flood := bytes.Repeat([]byte("x"), (2<<20)+17)
	if n, err := spool.writer("stdout").Write(flood); err != nil || n != len(flood) {
		t.Fatalf("Write() = %d, %v; want %d, nil", n, err, len(flood))
	}
	output := spool.output()
	retainedFrom := int64(len(flood)) - retainedLimit
	if output.Stdout.ObservedBytes != int64(len(flood)) {
		t.Fatalf("observed bytes = %d, want %d", output.Stdout.ObservedBytes, len(flood))
	}
	if output.Stdout.RetainedBytes != retainedLimit || !output.Stdout.Truncated || output.HistoryComplete {
		t.Fatalf("unexpected output retention metadata: %+v", output)
	}
	info, err := os.Stat(filepath.Join(spoolDir, "stdout.out"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != retainedLimit {
		t.Fatalf("spool size = %d, want %d", info.Size(), retainedLimit)
	}

	chunk, err := spool.read("stdout", retainedFrom-1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk.Data) != 0 || !chunk.Gap || chunk.RetainedFrom != retainedFrom {
		t.Fatalf("missing prefix was not reported: %+v", chunk)
	}

	chunk, err = spool.read("stdout", retainedFrom+retainedLimit-21, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk.Data) != 21 || chunk.Gap {
		t.Fatalf("retained range read = (%d bytes, gap=%v), want (21, false)", len(chunk.Data), chunk.Gap)
	}
	for _, b := range chunk.Data {
		if b != 'x' {
			t.Fatalf("ring wrap returned wrong byte %q", b)
		}
	}
}

func TestSpoolTracksPerStreamLastWriteTime(t *testing.T) {
	spool, err := openSpool(filepath.Join(t.TempDir(), "run"), 3<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()

	if _, err := spool.writer("stdout").Write([]byte("out")); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.writer("stderr").Write([]byte("err")); err != nil {
		t.Fatal(err)
	}

	_, times := spool.evidence()
	if len(times) != 2 || times["stdout"].IsZero() || times["stderr"].IsZero() {
		t.Fatalf("per-stream write timestamps = %#v", times)
	}
	stdout, err := spool.read("stdout", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if stdout.LastWriteAt == nil || !stdout.LastWriteAt.Equal(times["stdout"]) {
		t.Fatalf("chunk last write time = %v, spool evidence = %v", stdout.LastWriteAt, times["stdout"])
	}
	if stdout.LastWriteAt.Before(time.Now().Add(-time.Minute)) {
		t.Fatalf("chunk write time is stale: %v", stdout.LastWriteAt)
	}
}
