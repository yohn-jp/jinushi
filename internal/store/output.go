package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

// AppendOutput records observed output under one aggregate Run retention
// ceiling across stdout, stderr, and PTY. Streams share the available bytes;
// on overflow, bytes from the least recently written stream are compacted
// first. A zero maxRetained uses the store-wide aggregate default. Run output
// counters and spool chunks commit atomically.
func (s *Store) AppendOutput(runID, stream string, data []byte, maxRetained int64) (model.OutputStream, error) {
	return s.appendOutput(runID, stream, data, maxRetained, nil)
}

// AppendOutputWithEvent atomically records output and its typed journal event.
// LastOutputAt is advanced to the event's observation time in the same
// transaction. The event must have kind output.chunk.
func (s *Store) AppendOutputWithEvent(runID, stream string, data []byte, maxRetained int64, event model.Event) (model.OutputStream, error) {
	if err := prepareOutputEvent(&event, model.EventOutputChunk); err != nil {
		return model.OutputStream{}, err
	}
	return s.appendOutput(runID, stream, data, maxRetained, &event)
}

func (s *Store) appendOutput(runID, stream string, data []byte, maxRetained int64, event *model.Event) (model.OutputStream, error) {
	if !validOutputStream(stream) || maxRetained < 0 {
		return model.OutputStream{}, ErrInvalidOutput
	}
	if maxRetained == 0 {
		maxRetained = s.options.OutputRetainedBytes
	}
	if maxRetained > s.options.OutputRetainedBytes {
		maxRetained = s.options.OutputRetainedBytes
	}
	var result model.OutputStream
	err := s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket([]byte(runsBucketName))
		runData := runs.Get([]byte(runID))
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
		runLimit := maxRetained
		if run.Spec.Limits.OutputBytes > 0 && run.Spec.Limits.OutputBytes < runLimit {
			runLimit = run.Spec.Limits.OutputBytes
		}
		root := tx.Bucket([]byte(outputBucketName))
		runBucket, err := root.CreateBucketIfNotExists([]byte(runID))
		if err != nil {
			return fmt.Errorf("create Run output bucket: %w", err)
		}
		streamBucket, err := runBucket.CreateBucketIfNotExists([]byte(stream))
		if err != nil {
			return fmt.Errorf("create output stream bucket: %w", err)
		}
		meta := outputMeta{}
		if raw := streamBucket.Get([]byte(outputMetaKey)); raw != nil {
			if err := json.Unmarshal(raw, &meta); err != nil {
				return fmt.Errorf("decode output metadata: %w", err)
			}
		}
		if meta.Observed < 0 || meta.RetainedFrom < 0 || meta.RetainedFrom > meta.Observed {
			return fmt.Errorf("%w: corrupt output metadata", ErrInvalidOutput)
		}
		if int64(len(data)) > math.MaxInt64-meta.Observed {
			return ErrOutputTooLarge
		}
		newObserved := meta.Observed + int64(len(data))
		newRetainedFrom := meta.RetainedFrom
		if newObserved-newRetainedFrom > runLimit {
			newRetainedFrom = newObserved - runLimit
		}
		if len(data) > 0 {
			meta.LastAppendSeq, err = nextOutputSequence(runBucket)
			if err != nil {
				return err
			}
			writeOffset := meta.Observed
			writeData := data
			if newRetainedFrom > writeOffset {
				drop := newRetainedFrom - writeOffset
				writeData = writeData[int(drop):]
				writeOffset = newRetainedFrom
			}
			for len(writeData) > 0 {
				chunkSize := min(len(writeData), s.options.OutputChunkBytes)
				chunk := writeData[:chunkSize]
				if err := streamBucket.Put(outputChunkKey(writeOffset), chunk); err != nil {
					return fmt.Errorf("persist output chunk: %w", err)
				}
				writeOffset += int64(chunkSize)
				writeData = writeData[chunkSize:]
			}
		}
		if err := trimOutput(streamBucket, newRetainedFrom); err != nil {
			return err
		}
		meta.Observed = newObserved
		meta.RetainedFrom = newRetainedFrom
		if err := persistOutputMeta(streamBucket, meta); err != nil {
			return err
		}
		setRunOutput(&run, stream, outputStream(meta))
		if err := enforceAggregateOutput(runBucket, &run, runLimit); err != nil {
			return err
		}
		meta, err = readOutputMeta(streamBucket)
		if err != nil {
			return err
		}
		result = outputStream(meta)
		if event != nil {
			appended, err := s.appendEventTx(tx, runID, *event, true)
			if err != nil {
				return err
			}
			setLastOutputAt(&run, appended.ObservedAt)
		}
		encoded, err := json.Marshal(run)
		if err != nil {
			return fmt.Errorf("encode Run output metadata: %w", err)
		}
		if err := runs.Put([]byte(runID), encoded); err != nil {
			return fmt.Errorf("persist Run output metadata: %w", err)
		}
		return nil
	})
	return result, err
}

func nextOutputSequence(runBucket *bolt.Bucket) (uint64, error) {
	var sequence uint64
	if raw := runBucket.Get([]byte(outputSequenceKey)); raw != nil {
		if len(raw) != 8 {
			return 0, fmt.Errorf("%w: corrupt output sequence", ErrInvalidOutput)
		}
		sequence = binary.BigEndian.Uint64(raw)
	}
	if sequence == math.MaxUint64 {
		return 0, fmt.Errorf("%w: output sequence exhausted", ErrInvalidOutput)
	}
	sequence++
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, sequence)
	if err := runBucket.Put([]byte(outputSequenceKey), encoded); err != nil {
		return 0, fmt.Errorf("persist output sequence: %w", err)
	}
	return sequence, nil
}

func readOutputMeta(bucket *bolt.Bucket) (outputMeta, error) {
	meta := outputMeta{}
	if bucket == nil {
		return meta, nil
	}
	if raw := bucket.Get([]byte(outputMetaKey)); raw != nil {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return outputMeta{}, fmt.Errorf("decode output metadata: %w", err)
		}
	}
	if meta.Observed < 0 || meta.RetainedFrom < 0 || meta.RetainedFrom > meta.Observed {
		return outputMeta{}, fmt.Errorf("%w: corrupt output metadata", ErrInvalidOutput)
	}
	return meta, nil
}

func persistOutputMeta(bucket *bolt.Bucket, meta outputMeta) error {
	encoded, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode output metadata: %w", err)
	}
	if err := bucket.Put([]byte(outputMetaKey), encoded); err != nil {
		return fmt.Errorf("persist output metadata: %w", err)
	}
	return nil
}

func outputStream(meta outputMeta) model.OutputStream {
	return model.OutputStream{
		ObservedBytes: meta.Observed,
		RetainedBytes: meta.Observed - meta.RetainedFrom,
		RetainedFrom:  meta.RetainedFrom,
		Truncated:     meta.RetainedFrom > 0,
	}
}

func enforceAggregateOutput(runBucket *bolt.Bucket, run *model.Run, limit int64) error {
	streams := []string{"stdout", "stderr", "pty"}
	metas := make(map[string]outputMeta, len(streams))
	var retained int64
	for _, stream := range streams {
		bucket := runBucket.Bucket([]byte(stream))
		if bucket == nil {
			continue
		}
		meta, err := readOutputMeta(bucket)
		if err != nil {
			return err
		}
		metas[stream] = meta
		streamRetained := meta.Observed - meta.RetainedFrom
		if retained > math.MaxInt64-streamRetained {
			return fmt.Errorf("%w: aggregate output counter overflow", ErrInvalidOutput)
		}
		retained += streamRetained
	}
	for retained > limit {
		oldest := ""
		var oldestSequence uint64
		for _, stream := range streams {
			meta, ok := metas[stream]
			if !ok || meta.Observed == meta.RetainedFrom {
				continue
			}
			if oldest == "" || meta.LastAppendSeq < oldestSequence {
				oldest = stream
				oldestSequence = meta.LastAppendSeq
			}
		}
		if oldest == "" {
			return fmt.Errorf("%w: aggregate output metadata is inconsistent", ErrInvalidOutput)
		}
		meta := metas[oldest]
		drop := min(retained-limit, meta.Observed-meta.RetainedFrom)
		meta.RetainedFrom += drop
		bucket := runBucket.Bucket([]byte(oldest))
		if err := trimOutput(bucket, meta.RetainedFrom); err != nil {
			return err
		}
		if err := persistOutputMeta(bucket, meta); err != nil {
			return err
		}
		metas[oldest] = meta
		setRunOutput(run, oldest, outputStream(meta))
		retained -= drop
	}
	return nil
}

// RecordOutputGap advances the absolute observed byte count while discarding
// currently retained bytes for a stream. It is used when an external spool
// reports that output was compacted or lost before it could be imported.
// Future Appends continue from observedBytes, and readers receive a gap.
func (s *Store) RecordOutputGap(runID, stream string, observedBytes int64) error {
	_, err := s.recordOutputGap(runID, stream, observedBytes, nil)
	return err
}

// RecordOutputGapWithEvent atomically advances output metadata and appends its
// typed journal event. LastOutputAt is advanced to the event's observation
// time in the same transaction. The event must have kind output.gap.
func (s *Store) RecordOutputGapWithEvent(runID, stream string, observedBytes int64, event model.Event) (model.OutputStream, error) {
	if err := prepareOutputEvent(&event, model.EventOutputGap); err != nil {
		return model.OutputStream{}, err
	}
	return s.recordOutputGap(runID, stream, observedBytes, &event)
}

func (s *Store) recordOutputGap(runID, stream string, observedBytes int64, event *model.Event) (model.OutputStream, error) {
	if !validOutputStream(stream) || observedBytes < 0 {
		return model.OutputStream{}, ErrInvalidOutput
	}
	var result model.OutputStream
	err := s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket([]byte(runsBucketName))
		raw := runs.Get([]byte(runID))
		if raw == nil {
			return ErrRunNotFound
		}
		var run model.Run
		if err := json.Unmarshal(raw, &run); err != nil {
			return fmt.Errorf("decode Run: %w", err)
		}
		if run.State == model.Terminal {
			return ErrTerminalImmutable
		}
		root := tx.Bucket([]byte(outputBucketName))
		runBucket, err := root.CreateBucketIfNotExists([]byte(runID))
		if err != nil {
			return fmt.Errorf("create Run output bucket: %w", err)
		}
		streamBucket, err := runBucket.CreateBucketIfNotExists([]byte(stream))
		if err != nil {
			return fmt.Errorf("create output stream bucket: %w", err)
		}
		meta := outputMeta{}
		if raw := streamBucket.Get([]byte(outputMetaKey)); raw != nil {
			if err := json.Unmarshal(raw, &meta); err != nil {
				return fmt.Errorf("decode output metadata: %w", err)
			}
		}
		if meta.Observed < 0 || meta.RetainedFrom < 0 || meta.RetainedFrom > meta.Observed {
			return fmt.Errorf("%w: corrupt output metadata", ErrInvalidOutput)
		}
		if observedBytes < meta.Observed {
			return fmt.Errorf("%w: observed byte count cannot move backwards", ErrInvalidOutput)
		}
		cursor := streamBucket.Cursor()
		for key, _ := cursor.Seek([]byte{outputDataKey}); key != nil && key[0] == outputDataKey; {
			if err := cursor.Delete(); err != nil {
				return fmt.Errorf("discard output after gap: %w", err)
			}
			key, _ = cursor.Next()
		}
		meta.Observed = observedBytes
		meta.RetainedFrom = observedBytes
		encodedMeta, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("encode output metadata: %w", err)
		}
		if err := streamBucket.Put([]byte(outputMetaKey), encodedMeta); err != nil {
			return fmt.Errorf("persist output metadata: %w", err)
		}
		result = model.OutputStream{
			ObservedBytes: observedBytes,
			RetainedFrom:  observedBytes,
			Truncated:     observedBytes > 0,
		}
		setRunOutput(&run, stream, result)
		if event != nil {
			appended, err := s.appendEventTx(tx, runID, *event, true)
			if err != nil {
				return err
			}
			setLastOutputAt(&run, appended.ObservedAt)
		}
		encodedRun, err := json.Marshal(run)
		if err != nil {
			return fmt.Errorf("encode Run output metadata: %w", err)
		}
		if err := runs.Put([]byte(runID), encodedRun); err != nil {
			return fmt.Errorf("persist Run output metadata: %w", err)
		}
		return nil
	})
	return result, err
}

func prepareOutputEvent(event *model.Event, kind model.EventKind) error {
	if event.Kind != kind {
		return fmt.Errorf("%w: expected kind %q", ErrInvalidEvent, kind)
	}
	if event.ObservedAt.IsZero() {
		event.ObservedAt = time.Now().UTC()
	}
	return nil
}

func setLastOutputAt(run *model.Run, observedAt time.Time) {
	if run.LastOutputAt == nil || observedAt.After(*run.LastOutputAt) {
		lastOutputAt := observedAt
		run.LastOutputAt = &lastOutputAt
	}
}

// ReadOutput reads retained bytes starting at offset. If older output has been
// compacted, the returned gap is true and data begins at retainedFrom.
func (s *Store) ReadOutput(runID, stream string, offset int64, limit int) (data []byte, retainedFrom, observedBytes int64, gap bool, err error) {
	if !validOutputStream(stream) || offset < 0 {
		return nil, 0, 0, false, ErrInvalidOutput
	}
	if limit <= 0 || limit > s.options.OutputReadBytes {
		limit = s.options.OutputReadBytes
	}
	data = make([]byte, 0, limit)
	err = s.db.View(func(tx *bolt.Tx) error {
		runData := tx.Bucket([]byte(runsBucketName)).Get([]byte(runID))
		if runData == nil {
			return ErrRunNotFound
		}
		root := tx.Bucket([]byte(outputBucketName))
		runBucket := root.Bucket([]byte(runID))
		if runBucket == nil {
			return nil
		}
		streamBucket := runBucket.Bucket([]byte(stream))
		if streamBucket == nil {
			return nil
		}
		meta := outputMeta{}
		if raw := streamBucket.Get([]byte(outputMetaKey)); raw != nil {
			if err := json.Unmarshal(raw, &meta); err != nil {
				return fmt.Errorf("decode output metadata: %w", err)
			}
		}
		if meta.Observed < 0 || meta.RetainedFrom < 0 || meta.RetainedFrom > meta.Observed {
			return fmt.Errorf("%w: corrupt output metadata", ErrInvalidOutput)
		}
		retainedFrom = meta.RetainedFrom
		observedBytes = meta.Observed
		gap = offset < retainedFrom
		readFrom := offset
		if readFrom < retainedFrom {
			readFrom = retainedFrom
		}
		if readFrom >= observedBytes || len(data) == limit {
			return nil
		}
		readUntil := readFrom + min(observedBytes-readFrom, int64(limit))
		cursor := streamBucket.Cursor()
		for key, value := cursor.Seek([]byte{outputDataKey}); key != nil && key[0] == outputDataKey && int64(len(data)) < readUntil-readFrom; key, value = cursor.Next() {
			if len(key) != 9 {
				return fmt.Errorf("%w: corrupt output chunk key", ErrInvalidOutput)
			}
			chunkOffset, err := decodeOutputChunkOffset(key)
			if err != nil {
				return err
			}
			if int64(len(value)) > math.MaxInt64-chunkOffset {
				return fmt.Errorf("%w: output chunk offset overflow", ErrInvalidOutput)
			}
			chunkEnd := chunkOffset + int64(len(value))
			if chunkEnd <= readFrom {
				continue
			}
			if chunkOffset >= readUntil {
				break
			}
			from := max(readFrom, chunkOffset)
			to := min(readUntil, chunkEnd)
			data = append(data, value[int(from-chunkOffset):int(to-chunkOffset)]...)
		}
		if int64(len(data)) != readUntil-readFrom {
			return fmt.Errorf("%w: retained output spool has a byte gap", ErrInvalidOutput)
		}
		return nil
	})
	return data, retainedFrom, observedBytes, gap, err
}

func trimOutput(bucket *bolt.Bucket, retainedFrom int64) error {
	cursor := bucket.Cursor()
	for key, value := cursor.Seek([]byte{outputDataKey}); key != nil && key[0] == outputDataKey; {
		if len(key) != 9 {
			return fmt.Errorf("%w: corrupt output chunk key", ErrInvalidOutput)
		}
		chunkOffset, err := decodeOutputChunkOffset(key)
		if err != nil {
			return err
		}
		if int64(len(value)) > math.MaxInt64-chunkOffset {
			return fmt.Errorf("%w: output chunk offset overflow", ErrInvalidOutput)
		}
		chunkEnd := chunkOffset + int64(len(value))
		if chunkEnd <= retainedFrom {
			if err := cursor.Delete(); err != nil {
				return fmt.Errorf("trim output chunk: %w", err)
			}
			key, value = cursor.Next()
			continue
		}
		if chunkOffset < retainedFrom {
			trimmed := append([]byte(nil), value[int(retainedFrom-chunkOffset):]...)
			chunkEnd := chunkOffset + int64(len(value))
			if err := cursor.Delete(); err != nil {
				return fmt.Errorf("remove partially trimmed output chunk: %w", err)
			}
			if err := bucket.Put(outputChunkKey(retainedFrom), trimmed); err != nil {
				return fmt.Errorf("persist partially trimmed output chunk: %w", err)
			}
			key, value = cursor.Seek(outputChunkKey(chunkEnd))
			continue
		}
		key, value = cursor.Next()
	}
	return nil
}

func setRunOutput(run *model.Run, stream string, output model.OutputStream) {
	switch stream {
	case "stdout":
		run.Output.Stdout = output
	case "stderr":
		run.Output.Stderr = output
	case "pty":
		run.Output.PTY = output
	}
	run.Output.HistoryComplete = !run.Output.Stdout.Truncated && !run.Output.Stderr.Truncated && !run.Output.PTY.Truncated
}

func validOutputStream(stream string) bool {
	return stream == "stdout" || stream == "stderr" || stream == "pty"
}

func outputChunkKey(offset int64) []byte {
	key := make([]byte, 9)
	key[0] = outputDataKey
	binary.BigEndian.PutUint64(key[1:], uint64(offset))
	return key
}

func decodeOutputChunkOffset(key []byte) (int64, error) {
	if len(key) != 9 {
		return 0, fmt.Errorf("%w: corrupt output chunk key", ErrInvalidOutput)
	}
	offset := binary.BigEndian.Uint64(key[1:])
	if offset > math.MaxInt64 {
		return 0, fmt.Errorf("%w: output chunk offset overflow", ErrInvalidOutput)
	}
	return int64(offset), nil
}
