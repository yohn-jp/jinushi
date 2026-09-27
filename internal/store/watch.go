package store

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

const (
	watchBucketName = "watch-index"
	watchMetaKey    = "\x00meta"
	watchEntryKey   = byte(1)

	watchRetentionCount = 4096
	watchRetentionBytes = 16 << 20
	watchMaxEventBytes  = 256 << 10
	watchMaxPageRecords = 128
	watchMaxPageBytes   = 1 << 20
)

var ErrInvalidWatchCursor = errors.New("invalid all-Run watch cursor")

type watchMeta struct {
	StoreID      string `json:"storeId"`
	LastCursor   uint64 `json:"lastCursor"`
	RetainedFrom uint64 `json:"retainedFrom"`
	Count        uint64 `json:"count"`
	Bytes        int64  `json:"bytes"`
}

// WatchEntry pairs a durable all-Run cursor with the unchanged per-Run event.
// Cursor order is the order in which serialized bbolt write transactions
// appended the events. Event.Seq remains the ordering authority within a Run.
type WatchEntry struct {
	Cursor string      `json:"cursor"`
	Event  model.Event `json:"event"`
}

// WatchPage is a bounded page from the durable all-Run event index.
// RetainedFrom is the cursor of the oldest event still retained, Watermark is
// the latest committed cursor observed by this read, and Cursor resumes after
// the last returned event. Gap reports that events after the requested cursor
// were evicted before this page was read.
type WatchPage struct {
	Events       []WatchEntry `json:"events"`
	Cursor       string       `json:"cursor,omitempty"`
	RetainedFrom string       `json:"retainedFrom,omitempty"`
	Watermark    string       `json:"watermark,omitempty"`
	Gap          bool         `json:"gap,omitempty"`
	HasMore      bool         `json:"hasMore,omitempty"`
}

type decodedWatchCursor struct {
	storeID string
	seq     uint64
}

// appendWatchEventTx inserts one already-normalized per-Run event into the
// global index. Its caller must invoke this in the same bbolt write
// transaction as the per-Run journal append, after the event's RunID and Seq
// have been assigned. The shared transaction makes the index and journal
// commit or roll back together. bbolt serializes writers, defining global
// cursor order by transaction observation/append order.
func appendWatchEventTx(tx *bolt.Tx, event model.Event) error {
	if event.RunID == "" || event.Seq == 0 || event.Kind == "" {
		return fmt.Errorf("%w: event is not normalized", ErrInvalidEvent)
	}
	bucket, err := tx.CreateBucketIfNotExists([]byte(watchBucketName))
	if err != nil {
		return fmt.Errorf("create all-Run watch index: %w", err)
	}
	meta, err := readWatchMeta(bucket)
	if err != nil {
		return err
	}
	if meta.LastCursor >= math.MaxUint64-1 {
		return fmt.Errorf("%w: all-Run cursor exhausted", ErrInvalidEvent)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode all-Run watch event: %w", err)
	}
	if len(encoded) > watchMaxEventBytes {
		return fmt.Errorf("all-Run watch event exceeds %d bytes", watchMaxEventBytes)
	}
	key := watchEventKey(meta.LastCursor + 1)
	entryBytes := int64(len(key) + len(encoded))
	if meta.Bytes > math.MaxInt64-entryBytes {
		return fmt.Errorf("all-Run watch byte count overflow")
	}
	if err := bucket.Put(key, encoded); err != nil {
		return fmt.Errorf("persist all-Run watch event: %w", err)
	}
	meta.LastCursor++
	meta.Count++
	meta.Bytes += entryBytes
	if meta.RetainedFrom == 0 {
		meta.RetainedFrom = meta.LastCursor
	}
	for meta.Count > watchRetentionCount || meta.Bytes > watchRetentionBytes {
		oldestKey, oldestValue := bucket.Cursor().Seek([]byte{watchEntryKey})
		if oldestKey == nil || oldestKey[0] != watchEntryKey {
			return fmt.Errorf("all-Run watch retention metadata is inconsistent")
		}
		oldestSeq := binary.BigEndian.Uint64(oldestKey[1:])
		meta.Count--
		meta.Bytes -= int64(len(oldestKey) + len(oldestValue))
		if err := bucket.Delete(oldestKey); err != nil {
			return fmt.Errorf("compact all-Run watch index: %w", err)
		}
		meta.RetainedFrom = oldestSeq + 1
	}
	if err := writeWatchMeta(bucket, meta); err != nil {
		return err
	}
	return nil
}

// WatchPage reads at most watchMaxPageRecords events and watchMaxPageBytes of
// event payload after the opaque cursor. A non-positive limit uses the
// package page maximum; larger limits are clamped. An empty cursor starts at
// the oldest retained event. Clients should persist Cursor and pass it on
// reconnect; Gap means some requested history was evicted.
func (s *Store) WatchPage(after string, limit int) (WatchPage, error) {
	if limit <= 0 || limit > watchMaxPageRecords {
		limit = watchMaxPageRecords
	}
	page := WatchPage{Events: make([]WatchEntry, 0, limit)}
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(watchBucketName))
		if bucket == nil {
			if after != "" {
				return ErrInvalidWatchCursor
			}
			return nil
		}
		meta, err := readWatchMeta(bucket)
		if err != nil {
			return err
		}
		var afterSeq uint64
		if after != "" {
			decoded, err := decodeWatchCursor(after)
			if err != nil || decoded.storeID != meta.StoreID || decoded.seq > meta.LastCursor {
				return ErrInvalidWatchCursor
			}
			afterSeq = decoded.seq
		}
		if meta.LastCursor != 0 {
			page.Watermark = encodeWatchCursor(meta.StoreID, meta.LastCursor)
		}
		if meta.Count > 0 {
			page.RetainedFrom = encodeWatchCursor(meta.StoreID, meta.RetainedFrom)
			if afterSeq < meta.RetainedFrom-1 {
				page.Gap = true
			}
		}
		page.Cursor = after
		if afterSeq == math.MaxUint64 {
			return ErrInvalidWatchCursor
		}
		start := afterSeq + 1
		if meta.Count > 0 && start < meta.RetainedFrom {
			start = meta.RetainedFrom
		}
		cursor := bucket.Cursor()
		pageBytes := 0
		for key, value := cursor.Seek(watchEventKey(start)); key != nil && key[0] == watchEntryKey; key, value = cursor.Next() {
			if len(page.Events) >= limit {
				page.HasMore = true
				break
			}
			cost := len(value)
			if len(page.Events) > 0 && pageBytes+cost > watchMaxPageBytes {
				page.HasMore = true
				break
			}
			var event model.Event
			if err := json.Unmarshal(value, &event); err != nil {
				return fmt.Errorf("decode all-Run watch event: %w", err)
			}
			seq := binary.BigEndian.Uint64(key[1:])
			page.Events = append(page.Events, WatchEntry{Cursor: encodeWatchCursor(meta.StoreID, seq), Event: event})
			pageBytes += cost
			page.Cursor = encodeWatchCursor(meta.StoreID, seq)
		}
		return nil
	})
	if err != nil {
		return WatchPage{}, err
	}
	return page, nil
}

func readWatchMeta(bucket *bolt.Bucket) (watchMeta, error) {
	var meta watchMeta
	raw := bucket.Get([]byte(watchMetaKey))
	if raw == nil {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return watchMeta{}, fmt.Errorf("create all-Run watch identity: %w", err)
		}
		meta = watchMeta{StoreID: hex.EncodeToString(id), RetainedFrom: 1}
		return meta, nil
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return watchMeta{}, fmt.Errorf("decode all-Run watch metadata: %w", err)
	}
	if len(meta.StoreID) != 32 || meta.RetainedFrom == 0 || meta.Bytes < 0 {
		return watchMeta{}, fmt.Errorf("corrupt all-Run watch metadata")
	}
	if _, err := hex.DecodeString(meta.StoreID); err != nil {
		return watchMeta{}, fmt.Errorf("corrupt all-Run watch identity: %w", err)
	}
	return meta, nil
}

func writeWatchMeta(bucket *bolt.Bucket, meta watchMeta) error {
	encoded, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode all-Run watch metadata: %w", err)
	}
	if err := bucket.Put([]byte(watchMetaKey), encoded); err != nil {
		return fmt.Errorf("persist all-Run watch metadata: %w", err)
	}
	return nil
}

func encodeWatchCursor(storeID string, seq uint64) string {
	if seq == 0 {
		return ""
	}
	id, err := hex.DecodeString(storeID)
	if err != nil || len(id) != 16 {
		return ""
	}
	payload := make([]byte, 25)
	payload[0] = 1
	copy(payload[1:17], id)
	binary.BigEndian.PutUint64(payload[17:], seq)
	return "w1_" + base64.RawURLEncoding.EncodeToString(payload)
}

func decodeWatchCursor(cursor string) (decodedWatchCursor, error) {
	if len(cursor) < 4 || cursor[:3] != "w1_" {
		return decodedWatchCursor{}, ErrInvalidWatchCursor
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor[3:])
	if err != nil || len(payload) != 25 || payload[0] != 1 {
		return decodedWatchCursor{}, ErrInvalidWatchCursor
	}
	seq := binary.BigEndian.Uint64(payload[17:])
	if seq == 0 {
		return decodedWatchCursor{}, ErrInvalidWatchCursor
	}
	return decodedWatchCursor{storeID: hex.EncodeToString(payload[1:17]), seq: seq}, nil
}

func watchEventKey(seq uint64) []byte {
	key := make([]byte, 9)
	key[0] = watchEntryKey
	binary.BigEndian.PutUint64(key[1:], seq)
	return key
}
