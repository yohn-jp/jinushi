// Package store provides durable Run metadata, an ordered event journal, and
// bounded output retention backed by a single bbolt database.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

var (
	ErrRunNotFound       = errors.New("run not found")
	ErrRunExists         = errors.New("run already exists")
	ErrInvalidRun        = errors.New("invalid run")
	ErrInvalidEvent      = errors.New("invalid event")
	ErrEventTooLarge     = errors.New("event exceeds configured size limit")
	ErrJournalFull       = errors.New("event journal is full of protected lifecycle events")
	ErrOutputTooLarge    = errors.New("output byte counter overflow")
	ErrInvalidOutput     = errors.New("invalid output request")
	ErrTerminalImmutable = errors.New("terminal run is immutable")
	ErrStoreClosed       = errors.New("store is closed")
)

const (
	runsBucketName   = "runs"
	eventsBucketName = "events"
	outputBucketName = "outputs"

	eventMetaKey      = "\x00meta"
	eventEntryKey     = byte(1)
	outputMetaKey     = "meta"
	outputDataKey     = byte(1)
	outputSequenceKey = "\x00sequence"

	defaultEventRetentionCount = 4096
	defaultEventRetentionBytes = 16 << 20
	defaultMaxEventBytes       = 256 << 10
	defaultMaxEventPage        = 1000
	defaultOutputChunkBytes    = 64 << 10
	defaultOutputReadBytes     = 1 << 20
	defaultOutputRetainedBytes = 8 << 20
)

// Options controls the per-Run history bounds. Zero-valued fields use the
// documented package defaults. OutputRetainedBytes is an aggregate ceiling
// across stdout, stderr, and PTY output. Retention is shared by streams that
// actually produce bytes; when the aggregate is exceeded, the least recently
// written stream is compacted first.
type Options struct {
	EventRetentionCount int
	EventRetentionBytes int64
	MaxEventBytes       int
	MaxEventPage        int

	OutputChunkBytes    int
	OutputReadBytes     int
	OutputRetainedBytes int64
}

// Store is safe for concurrent use. The operation gate keeps database calls
// stable while online compaction atomically replaces and reopens the database.
type Store struct {
	opGate  sync.RWMutex
	db      *operationDB
	rawDB   *bolt.DB
	dbGuard *os.File
	path    string
	options Options
}

type eventMeta struct {
	LastSeq      uint64 `json:"lastSeq"`
	RetainedFrom uint64 `json:"retainedFrom"`
	Count        uint64 `json:"count"`
	Bytes        int64  `json:"bytes"`
}

type outputMeta struct {
	Observed      int64  `json:"observed"`
	RetainedFrom  int64  `json:"retainedFrom"`
	LastAppendSeq uint64 `json:"lastAppendSeq,omitempty"`
}

// Open opens or creates a durable store at path. The containing directory is
// created with owner-only permissions when necessary.
func Open(path string, options Options) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("open store: path is empty")
	}
	options = withDefaults(options)
	if options.EventRetentionBytes < int64(options.MaxEventBytes) {
		return nil, fmt.Errorf("event retention bytes must be at least max event bytes")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create store directory: %w", err)
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve store path: %w", err)
	}
	path = filepath.Clean(path)
	db, guard, err := openDatabase(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open store database: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{
			[]byte(runsBucketName), []byte(eventsBucketName), []byte(outputBucketName),
		} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create %q bucket: %w", name, err)
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		if guard != nil {
			_ = guard.Close()
		}
		return nil, fmt.Errorf("initialize store: %w", err)
	}
	s := &Store{rawDB: db, dbGuard: guard, path: path, options: options}
	s.db = &operationDB{store: s}
	cleanupCompactionArtifacts(path)
	return s, nil
}

func withDefaults(options Options) Options {
	if options.EventRetentionCount <= 0 {
		options.EventRetentionCount = defaultEventRetentionCount
	}
	if options.EventRetentionBytes <= 0 {
		options.EventRetentionBytes = defaultEventRetentionBytes
	}
	if options.MaxEventBytes <= 0 {
		options.MaxEventBytes = defaultMaxEventBytes
	}
	if options.MaxEventPage <= 0 {
		options.MaxEventPage = defaultMaxEventPage
	}
	if options.OutputChunkBytes <= 0 {
		options.OutputChunkBytes = defaultOutputChunkBytes
	}
	if options.OutputReadBytes <= 0 {
		options.OutputReadBytes = defaultOutputReadBytes
	}
	if options.OutputRetainedBytes <= 0 {
		options.OutputRetainedBytes = defaultOutputRetainedBytes
	}
	return options
}

// Close closes the underlying database.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.opGate.Lock()
	defer s.opGate.Unlock()
	if s.rawDB == nil {
		return nil
	}
	err := s.rawDB.Close()
	s.rawDB = nil
	if s.dbGuard != nil {
		if guardErr := s.dbGuard.Close(); err == nil {
			err = guardErr
		}
		s.dbGuard = nil
	}
	return err
}

// Create persists an accepted Run and, when event is non-nil, its initial
// lifecycle event in one transaction. The store assigns the event sequence.
func (s *Store) Create(run model.Run, event *model.Event) (model.Run, *model.Event, error) {
	if err := validateRun(run); err != nil {
		return model.Run{}, nil, err
	}
	if err := validateNewRunOutput(run.Output); err != nil {
		return model.Run{}, nil, err
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now().UTC()
	}
	run.Output.HistoryComplete = true
	var appended *model.Event
	err := s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket([]byte(runsBucketName))
		if runs.Get([]byte(run.ID)) != nil {
			return ErrRunExists
		}
		if event != nil {
			copy, err := s.appendEventTx(tx, run.ID, *event, run.State != model.Terminal)
			if err != nil {
				return err
			}
			appended = &copy
		}
		if run.State == model.Terminal {
			meta, err := readEventMetaTx(tx, run.ID)
			if err != nil {
				return err
			}
			stampReceiptEventRange(run.Receipt, meta)
		}
		encoded, err := json.Marshal(run)
		if err != nil {
			return fmt.Errorf("encode Run: %w", err)
		}
		if err := runs.Put([]byte(run.ID), encoded); err != nil {
			return fmt.Errorf("persist Run: %w", err)
		}
		return nil
	})
	if err != nil {
		return model.Run{}, nil, err
	}
	return run, appended, nil
}

// Get returns the current durable snapshot for id.
func (s *Store) Get(id string) (model.Run, error) {
	var run model.Run
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte(runsBucketName)).Get([]byte(id))
		if value == nil {
			return ErrRunNotFound
		}
		if err := json.Unmarshal(value, &run); err != nil {
			return fmt.Errorf("decode Run %q: %w", id, err)
		}
		return nil
	})
	return run, err
}

// List returns all Runs in stable Run ID order.
func (s *Store) List() ([]model.Run, error) {
	runs := make([]model.Run, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(runsBucketName)).ForEach(func(_, value []byte) error {
			var run model.Run
			if err := json.Unmarshal(value, &run); err != nil {
				return fmt.Errorf("decode Run: %w", err)
			}
			runs = append(runs, run)
			return nil
		})
	})
	return runs, err
}

// Update atomically persists a Run snapshot and an optional lifecycle event.
// Run identity, accepted specification, and creation time are immutable.
func (s *Store) Update(run model.Run, event *model.Event) (*model.Event, error) {
	var events []model.Event
	if event != nil {
		events = append(events, *event)
	}
	appended, err := s.UpdateWithEvents(run, events)
	if err != nil || len(appended) == 0 {
		return nil, err
	}
	return &appended[0], nil
}

// UpdateWithEvents atomically persists a Run snapshot and zero or more ordered
// events. Event sequences are assigned in the same transaction as the Run
// update and terminal receipt event-range stamp.
func (s *Store) UpdateWithEvents(run model.Run, events []model.Event) ([]model.Event, error) {
	if err := validateRun(run); err != nil {
		return nil, err
	}
	appended := make([]model.Event, 0, len(events))
	err := s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket([]byte(runsBucketName))
		previousBytes := runs.Get([]byte(run.ID))
		if previousBytes == nil {
			return ErrRunNotFound
		}
		var previous model.Run
		if err := json.Unmarshal(previousBytes, &previous); err != nil {
			return fmt.Errorf("decode prior Run: %w", err)
		}
		if previous.State == model.Terminal {
			return ErrTerminalImmutable
		}
		if run.Generation < previous.Generation {
			return fmt.Errorf("%w: generation %d is older than current generation %d", ErrInvalidRun, run.Generation, previous.Generation)
		}
		if run.State != previous.State && run.Generation == previous.Generation {
			return fmt.Errorf("%w: lifecycle state change requires a new generation", ErrInvalidRun)
		}
		if previous.EffectiveCapabilities == nil && run.EffectiveCapabilities != nil && run.Generation == previous.Generation {
			return fmt.Errorf("%w: establishing effective backend capabilities requires a new generation", ErrInvalidRun)
		}
		if !reflect.DeepEqual(previous.Spec, run.Spec) || !previous.CreatedAt.Equal(run.CreatedAt) {
			return fmt.Errorf("%w: Run specification and creation time are immutable", ErrInvalidRun)
		}
		if previous.EffectiveCapabilities != nil && !reflect.DeepEqual(previous.EffectiveCapabilities, run.EffectiveCapabilities) {
			return fmt.Errorf("%w: effective backend capabilities are immutable once established", ErrInvalidRun)
		}
		if !reflect.DeepEqual(previous.Output, run.Output) {
			return fmt.Errorf("%w: output metadata is managed by the output spool", ErrInvalidRun)
		}
		if previous.Receipt != nil && !reflect.DeepEqual(previous.Receipt, run.Receipt) {
			return fmt.Errorf("%w: terminal receipt is immutable", ErrInvalidRun)
		}
		for _, event := range events {
			copy, err := s.appendEventTx(tx, run.ID, event, run.State != model.Terminal)
			if err != nil {
				return err
			}
			appended = append(appended, copy)
		}
		if run.State == model.Terminal {
			meta, err := readEventMetaTx(tx, run.ID)
			if err != nil {
				return err
			}
			stampReceiptEventRange(run.Receipt, meta)
		}
		encoded, err := json.Marshal(run)
		if err != nil {
			return fmt.Errorf("encode Run: %w", err)
		}
		if err := runs.Put([]byte(run.ID), encoded); err != nil {
			return fmt.Errorf("persist Run: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return appended, nil
}

func readEventMetaTx(tx *bolt.Tx, runID string) (eventMeta, error) {
	meta := eventMeta{RetainedFrom: 1}
	root := tx.Bucket([]byte(eventsBucketName))
	journal := root.Bucket([]byte(runID))
	if journal == nil {
		return meta, nil
	}
	raw := journal.Get([]byte(eventMetaKey))
	if raw != nil {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return eventMeta{}, fmt.Errorf("decode event metadata: %w", err)
		}
	}
	if meta.RetainedFrom == 0 {
		if meta.Count == 0 {
			meta.RetainedFrom = meta.LastSeq + 1
		} else {
			key, _ := journal.Cursor().Seek([]byte{eventEntryKey})
			if key == nil || key[0] != eventEntryKey || len(key) != 9 {
				return eventMeta{}, fmt.Errorf("event metadata has retained entries but no first sequence")
			}
			meta.RetainedFrom = binary.BigEndian.Uint64(key[1:])
		}
	}
	return meta, nil
}

func stampReceiptEventRange(receipt *model.Receipt, meta eventMeta) {
	if receipt == nil {
		return
	}
	firstSeq := uint64(0)
	if meta.LastSeq > 0 {
		firstSeq = 1
	}
	if meta.RetainedFrom == 0 {
		meta.RetainedFrom = 1
	}
	complete := meta.RetainedFrom == 1 && meta.Count == meta.LastSeq
	receipt.EventFirstSeq = firstSeq
	receipt.EventLastSeq = meta.LastSeq
	receipt.EventRetainedFrom = meta.RetainedFrom
	receipt.EventHistoryComplete = complete
	if !complete {
		receipt.EvidenceIncomplete = true
	}
}

// AppendEvent appends one event and assigns its monotonic per-Run sequence.
func (s *Store) AppendEvent(runID string, event model.Event) (model.Event, error) {
	var appended model.Event
	err := s.db.Update(func(tx *bolt.Tx) error {
		runData := tx.Bucket([]byte(runsBucketName)).Get([]byte(runID))
		if runData == nil {
			return ErrRunNotFound
		}
		var run model.Run
		if err := json.Unmarshal(runData, &run); err != nil {
			return fmt.Errorf("decode Run: %w", err)
		}
		if run.State == model.Terminal {
			return ErrTerminalImmutable
		}
		var err error
		appended, err = s.appendEventTx(tx, runID, event, true)
		return err
	})
	return appended, err
}

// Events returns events with sequence greater than after. retainedFrom is the
// first retained sequence, or lastSeq+1 if no event is retained. gap reports
// compacted history before the page or holes between retained events.
func (s *Store) Events(runID string, after uint64, limit int) (events []model.Event, retainedFrom uint64, gap bool, err error) {
	if limit <= 0 || limit > s.options.MaxEventPage {
		limit = s.options.MaxEventPage
	}
	events = make([]model.Event, 0, min(limit, 64))
	err = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(runsBucketName)).Get([]byte(runID)) == nil {
			return ErrRunNotFound
		}
		journal := tx.Bucket([]byte(eventsBucketName)).Bucket([]byte(runID))
		meta := eventMeta{RetainedFrom: 1}
		if journal != nil {
			if raw := journal.Get([]byte(eventMetaKey)); raw != nil {
				if err := json.Unmarshal(raw, &meta); err != nil {
					return fmt.Errorf("decode event metadata: %w", err)
				}
			}
		}
		if meta.RetainedFrom == 0 {
			meta.RetainedFrom = 1
		}
		retainedFrom = meta.RetainedFrom
		gap = after < retainedFrom-1
		if journal == nil {
			return nil
		}
		if after == math.MaxUint64 || len(events) == limit {
			return nil
		}
		start := after + 1
		if start < retainedFrom {
			start = retainedFrom
		}
		cursor := journal.Cursor()
		expected := start
		for key, value := cursor.Seek(eventKey(start)); key != nil && key[0] == eventEntryKey && len(events) < limit; key, value = cursor.Next() {
			var event model.Event
			if err := json.Unmarshal(value, &event); err != nil {
				return fmt.Errorf("decode event: %w", err)
			}
			if event.Seq > expected {
				gap = true
			}
			events = append(events, event)
			expected = event.Seq + 1
		}
		if len(events) == 0 {
			if after < meta.LastSeq {
				gap = true
			}
		} else if len(events) < limit && events[len(events)-1].Seq < meta.LastSeq {
			gap = true
		}
		return nil
	})
	return events, retainedFrom, gap, err
}

func (s *Store) appendEventTx(tx *bolt.Tx, runID string, event model.Event, preserveCritical bool) (model.Event, error) {
	if runID == "" {
		return model.Event{}, ErrInvalidEvent
	}
	if event.Kind == "" {
		return model.Event{}, fmt.Errorf("%w: kind is empty", ErrInvalidEvent)
	}
	events := tx.Bucket([]byte(eventsBucketName))
	journal, err := events.CreateBucketIfNotExists([]byte(runID))
	if err != nil {
		return model.Event{}, fmt.Errorf("create event journal: %w", err)
	}
	meta := eventMeta{RetainedFrom: 1}
	if raw := journal.Get([]byte(eventMetaKey)); raw != nil {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return model.Event{}, fmt.Errorf("decode event metadata: %w", err)
		}
	}
	if meta.LastSeq >= math.MaxUint64-1 {
		return model.Event{}, fmt.Errorf("%w: sequence exhausted", ErrInvalidEvent)
	}
	event.Version = model.EventSchemaVersion
	event.RunID = runID
	event.Seq = meta.LastSeq + 1
	if event.ObservedAt.IsZero() {
		event.ObservedAt = time.Now().UTC()
	}
	if event.Payload != nil {
		if err := event.Payload.Validate(event.Kind); err != nil {
			return model.Event{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
		}
		if event.Body != nil {
			return model.Event{}, fmt.Errorf("%w: typed payload cannot be combined with legacy body", ErrInvalidEvent)
		}
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return model.Event{}, fmt.Errorf("encode event: %w", err)
	}
	if len(encoded) > s.options.MaxEventBytes {
		return model.Event{}, fmt.Errorf("%w: got %d bytes, maximum is %d", ErrEventTooLarge, len(encoded), s.options.MaxEventBytes)
	}
	if meta.Bytes > math.MaxInt64-int64(len(encoded)) {
		return model.Event{}, fmt.Errorf("%w: event journal byte count overflow", ErrInvalidEvent)
	}
	if err := journal.Put(eventKey(event.Seq), encoded); err != nil {
		return model.Event{}, fmt.Errorf("persist event: %w", err)
	}
	meta.LastSeq = event.Seq
	meta.Count++
	meta.Bytes += int64(len(encoded))
	if meta.RetainedFrom == 0 {
		meta.RetainedFrom = 1
	}
	if err := compactEvents(journal, &meta, s.options, preserveCritical); err != nil {
		return model.Event{}, err
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return model.Event{}, fmt.Errorf("encode event metadata: %w", err)
	}
	if err := journal.Put([]byte(eventMetaKey), metaBytes); err != nil {
		return model.Event{}, fmt.Errorf("persist event metadata: %w", err)
	}
	// Keep the global watch index in the same bbolt transaction as the
	// per-Run journal so subscribers never observe a partial event commit.
	if err := appendWatchEventTx(tx, event); err != nil {
		return model.Event{}, err
	}
	return event, nil
}

func compactEvents(journal *bolt.Bucket, meta *eventMeta, options Options, preserveCritical bool) error {
	for meta.Count > uint64(options.EventRetentionCount) || meta.Bytes > options.EventRetentionBytes {
		cursor := journal.Cursor()
		var key []byte
		var size int
		for candidate, value := cursor.Seek([]byte{eventEntryKey}); candidate != nil && candidate[0] == eventEntryKey; candidate, value = cursor.Next() {
			if preserveCritical {
				var event struct {
					Kind model.EventKind `json:"kind"`
				}
				if err := json.Unmarshal(value, &event); err != nil {
					return fmt.Errorf("decode event during compaction: %w", err)
				}
				if isCriticalEvent(event.Kind) {
					continue
				}
			}
			key = append([]byte(nil), candidate...)
			size = len(value)
			break
		}
		if key == nil {
			return ErrJournalFull
		}
		seq := binary.BigEndian.Uint64(key[1:])
		meta.Count--
		meta.Bytes -= int64(size)
		if err := journal.Delete(key); err != nil {
			return fmt.Errorf("compact event journal: %w", err)
		}
		meta.RetainedFrom = seq + 1
	}
	if meta.Count == 0 {
		meta.RetainedFrom = meta.LastSeq + 1
		return nil
	}
	key, _ := journal.Cursor().Seek([]byte{eventEntryKey})
	if key != nil && key[0] == eventEntryKey {
		meta.RetainedFrom = binary.BigEndian.Uint64(key[1:])
	}
	return nil
}

func isCriticalEvent(kind model.EventKind) bool {
	return model.IsCriticalEventKind(kind)
}

func validateRun(run model.Run) error {
	if run.ID == "" {
		return fmt.Errorf("%w: ID is empty", ErrInvalidRun)
	}
	if run.State == "" {
		return fmt.Errorf("%w: state is empty", ErrInvalidRun)
	}
	if run.State == model.Terminal {
		if run.Receipt == nil {
			return fmt.Errorf("%w: terminal Run must include its receipt", ErrInvalidRun)
		}
		if run.Receipt.RunID != run.ID {
			return fmt.Errorf("%w: terminal receipt Run ID does not match", ErrInvalidRun)
		}
	}
	if run.Receipt != nil && run.Receipt.RunID != run.ID {
		return fmt.Errorf("%w: receipt Run ID does not match", ErrInvalidRun)
	}
	if run.EffectiveCapabilities != nil {
		if run.EffectiveCapabilities.Backend == "" {
			return fmt.Errorf("%w: effective backend identity is empty", ErrInvalidRun)
		}
	}
	if run.Receipt != nil && (run.EffectiveCapabilities != nil || run.Receipt.EffectiveCapabilities != nil) {
		if run.EffectiveCapabilities == nil || run.Receipt.EffectiveCapabilities == nil ||
			!reflect.DeepEqual(run.EffectiveCapabilities, run.Receipt.EffectiveCapabilities) ||
			!reflect.DeepEqual(*run.EffectiveCapabilities, run.Receipt.Capabilities) {
			return fmt.Errorf("%w: terminal receipt does not preserve effective backend capabilities", ErrInvalidRun)
		}
	}
	return nil
}

func validateNewRunOutput(output model.Output) error {
	if output.Stdout.ObservedBytes != 0 || output.Stdout.RetainedBytes != 0 || output.Stdout.RetainedFrom != 0 || output.Stdout.Truncated ||
		output.Stderr.ObservedBytes != 0 || output.Stderr.RetainedBytes != 0 || output.Stderr.RetainedFrom != 0 || output.Stderr.Truncated ||
		output.PTY.ObservedBytes != 0 || output.PTY.RetainedBytes != 0 || output.PTY.RetainedFrom != 0 || output.PTY.Truncated {
		return fmt.Errorf("%w: new Run output metadata must be empty", ErrInvalidRun)
	}
	return nil
}

func eventKey(seq uint64) []byte {
	key := make([]byte, 9)
	key[0] = eventEntryKey
	binary.BigEndian.PutUint64(key[1:], seq)
	return key
}
