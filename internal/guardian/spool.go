package guardian

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

const maxSpoolRead = (1<<20)*3/4 - 1024

type spool struct {
	mu           sync.Mutex
	dir          string
	streamLimit  int64
	files        map[string]*os.File
	stats        map[string]model.OutputStream
	lastWriteAt  map[string]time.Time
	lastWriteSeq map[string]uint64
	writeSeq     uint64
	failed       bool
	closed       bool
}

func openSpool(dir string, maxBytes int64) (*spool, error) {
	if maxBytes < 0 {
		return nil, errors.New("guardian: output limit cannot be negative")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("guardian: create spool directory: %w", err)
	}
	s := &spool{
		dir:          dir,
		streamLimit:  maxBytes,
		files:        make(map[string]*os.File, 3),
		stats:        make(map[string]model.OutputStream, 3),
		lastWriteAt:  make(map[string]time.Time, 3),
		lastWriteSeq: make(map[string]uint64, 3),
	}
	for _, name := range []string{"stdout", "stderr", "pty"} {
		path := filepath.Join(dir, name+".out")
		f, err := openStateFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			_ = s.closeFiles()
			return nil, fmt.Errorf("guardian: open %s spool: %w", name, err)
		}
		if err := f.Chmod(0600); err != nil {
			_ = f.Close()
			_ = s.closeFiles()
			return nil, fmt.Errorf("guardian: secure %s spool: %w", name, err)
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			_ = s.closeFiles()
			return nil, fmt.Errorf("guardian: stat %s spool: %w", name, err)
		}
		if !info.Mode().IsRegular() || info.Size() > s.streamLimit {
			_ = f.Close()
			_ = s.closeFiles()
			return nil, fmt.Errorf("guardian: invalid %s spool file", name)
		}
		s.files[name] = f
		s.stats[name] = model.OutputStream{ObservedBytes: info.Size(), RetainedBytes: info.Size(), RetainedFrom: 0}
	}
	return s, nil
}

func (s *spool) writer(name string) io.Writer { return streamWriter{s: s, name: name} }

type streamWriter struct {
	s    *spool
	name string
}

func (w streamWriter) Write(p []byte) (int, error) {
	s := w.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		// Continue draining child output even if the evidence file has closed.
		return len(p), nil
	}
	f, ok := s.files[w.name]
	if !ok {
		return len(p), nil
	}
	stat := s.stats[w.name]
	if int64(len(p)) > math.MaxInt64-stat.ObservedBytes {
		stat.Truncated = true
		s.failed = true
		s.stats[w.name] = stat
		return len(p), nil
	}
	oldObserved := stat.ObservedBytes
	newObserved := oldObserved + int64(len(p))
	newFrom := max64(stat.RetainedFrom, newObserved-s.streamLimit)
	newRetained := newObserved - newFrom
	writeOffset := max64(oldObserved, newFrom)
	drop := writeOffset - oldObserved
	if drop < int64(len(p)) {
		data := p[int(drop):]
		if err := writeRing(f, data, writeOffset, s.streamLimit); err != nil {
			// A broken spool must not stop the child being drained. Evidence will
			// report the resulting hole instead of blocking an output producer.
			s.failed = true
			stat.Truncated = true
			stat.RetainedBytes = 0
			stat.RetainedFrom = newObserved
		} else {
			stat.RetainedBytes = newRetained
			stat.RetainedFrom = newFrom
		}
	}
	stat.ObservedBytes = newObserved
	if len(p) > 0 {
		s.lastWriteAt[w.name] = time.Now().UTC()
		s.writeSeq++
		s.lastWriteSeq[w.name] = s.writeSeq
	}
	if newFrom > 0 || s.failed {
		stat.Truncated = true
	}
	s.stats[w.name] = stat
	s.enforceAggregateLocked()
	return len(p), nil
}

// enforceAggregateLocked keeps the logical retained total within the Run
// budget. A stream that has not written recently gives up older evidence
// first; stream offsets remain absolute and report the resulting gap.
func (s *spool) enforceAggregateLocked() {
	var retained int64
	for _, stat := range s.stats {
		retained += stat.RetainedBytes
	}
	for retained > s.streamLimit {
		oldest := ""
		for _, name := range []string{"stdout", "stderr", "pty"} {
			if s.stats[name].RetainedBytes == 0 {
				continue
			}
			if oldest == "" || s.lastWriteSeq[name] < s.lastWriteSeq[oldest] {
				oldest = name
			}
		}
		if oldest == "" {
			s.failed = true
			return
		}
		stat := s.stats[oldest]
		drop := min64(retained-s.streamLimit, stat.RetainedBytes)
		stat.RetainedFrom += drop
		stat.RetainedBytes -= drop
		stat.Truncated = true
		if stat.RetainedBytes == 0 {
			if err := s.files[oldest].Truncate(0); err != nil {
				s.failed = true
			}
		}
		s.stats[oldest] = stat
		retained -= drop
	}
}

func writeRing(file *os.File, data []byte, absoluteOffset, capacity int64) error {
	if len(data) == 0 || capacity == 0 {
		return nil
	}
	if int64(len(data)) > capacity {
		drop := int64(len(data)) - capacity
		data = data[int(drop):]
		absoluteOffset += drop
	}
	for len(data) > 0 {
		position := absoluteOffset % capacity
		count := int64(len(data))
		if remain := capacity - position; count > remain {
			count = remain
		}
		n, err := file.WriteAt(data[:int(count)], position)
		if err != nil {
			return err
		}
		if int64(n) != count {
			return io.ErrShortWrite
		}
		data = data[n:]
		absoluteOffset += int64(n)
	}
	return nil
}

func (s *spool) sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	var first error
	for name, f := range s.files {
		if err := f.Sync(); err != nil && first == nil {
			first = fmt.Errorf("guardian: sync %s spool: %w", name, err)
		}
	}
	if err := syncDir(s.dir); err != nil && first == nil {
		first = err
	}
	if first != nil {
		s.failed = true
	}
	return first
}

func (s *spool) output() model.Output {
	output, _ := s.evidence()
	return output
}

func (s *spool) evidence() (model.Output, map[string]time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := model.Output{Stdout: s.stats["stdout"], Stderr: s.stats["stderr"], PTY: s.stats["pty"]}
	out.HistoryComplete = !s.failed && !out.Stdout.Truncated && !out.Stderr.Truncated && !out.PTY.Truncated
	if len(s.lastWriteAt) == 0 {
		return out, nil
	}
	times := make(map[string]time.Time, len(s.lastWriteAt))
	for stream, at := range s.lastWriteAt {
		times[stream] = at
	}
	return out, times
}

func (s *spool) read(stream string, offset, limit int64) (Chunk, error) {
	if offset < 0 || limit <= 0 || limit > maxSpoolRead {
		return Chunk{}, errors.New("guardian: invalid output range")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stat, ok := s.stats[stream]
	if !ok {
		return Chunk{}, fmt.Errorf("guardian: unknown output stream %q", stream)
	}
	chunk := Chunk{Stream: stream, Offset: offset, RetainedFrom: stat.RetainedFrom, ObservedBytes: stat.ObservedBytes, RetainedBytes: stat.RetainedBytes, Truncated: stat.Truncated}
	if at, exists := s.lastWriteAt[stream]; exists {
		chunk.LastWriteAt = timePtrOrNil(at)
	}
	retainedEnd := stat.RetainedFrom + stat.RetainedBytes
	if offset < stat.RetainedFrom {
		chunk.Gap = true
		return chunk, nil
	}
	if offset >= retainedEnd {
		chunk.Gap = offset < stat.ObservedBytes
		return chunk, nil
	}
	if offset+limit > retainedEnd {
		limit = retainedEnd - offset
	}
	if s.streamLimit == 0 {
		chunk.Gap = true
		return chunk, nil
	}
	chunk.Data = make([]byte, int(limit))
	f := s.files[stream]
	firstPosition := offset % s.streamLimit
	firstCount := int64(len(chunk.Data))
	if remain := s.streamLimit - firstPosition; firstCount > remain {
		firstCount = remain
	}
	n, err := f.ReadAt(chunk.Data[:int(firstCount)], firstPosition)
	if err != nil && err != io.EOF {
		return Chunk{}, err
	}
	if int64(n) != firstCount {
		chunk.Data = chunk.Data[:n]
		chunk.Gap = true
		return chunk, nil
	}
	if firstCount < int64(len(chunk.Data)) {
		secondCount := int64(len(chunk.Data)) - firstCount
		n, err = f.ReadAt(chunk.Data[int(firstCount):], 0)
		if err != nil && err != io.EOF {
			return Chunk{}, err
		}
		if int64(n) != secondCount {
			chunk.Data = chunk.Data[:int(firstCount)+n]
			chunk.Gap = true
			return chunk, nil
		}
	}
	return chunk, nil
}

func (s *spool) closeFiles() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for name, f := range s.files {
		if err := f.Close(); err != nil && first == nil {
			first = fmt.Errorf("guardian: close %s spool: %w", name, err)
		}
		delete(s.files, name)
	}
	s.closed = true
	return first
}

func (s *spool) close() error { return s.closeFiles() }

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
