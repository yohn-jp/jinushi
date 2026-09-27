package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

var ErrInvalidRetentionPolicy = errors.New("invalid terminal retention policy")

// GCResult summarizes one terminal collection pass. EvidenceBytesRemoved is
// the logical detailed Run evidence removed, before replacement tombstones.
type GCResult struct {
	StartedAt                 time.Time `json:"startedAt"`
	EvictedRuns               int       `json:"evictedRuns"`
	EvidenceBytesRemoved      int64     `json:"evidenceBytesRemoved"`
	NetLogicalBytesFreed      int64     `json:"netLogicalBytesFreed"`
	RemovedTombstones         int       `json:"removedTombstones"`
	RemovedTombstoneBytes     int64     `json:"removedTombstoneBytes"`
	ExpiredSubmissionBindings int       `json:"expiredSubmissionBindings"`
	SubmissionReplayStubs     int       `json:"submissionReplayStubs"`
	ExpiredSubmissionBytes    int64     `json:"expiredSubmissionBytes"`
	UncollectableTerminalRuns int       `json:"uncollectableTerminalRuns"`
	TerminalRunsRemaining     int       `json:"terminalRunsRemaining"`
	TerminalEvidenceBytes     int64     `json:"terminalEvidenceBytes"`
	BudgetExceeded            bool      `json:"budgetExceeded"`
	CompactionRecommended     bool      `json:"compactionRecommended"`
}

type retentionCandidate struct {
	run           model.Run
	runID         string
	finishedAt    time.Time
	detailedBytes int64
	reasons       []EvictionReason
	collectable   bool
}

// CollectTerminal applies an explicit bounded retention policy at now. Only
// model.Terminal Runs with a complete terminal receipt and finish timestamp
// can be removed. Each deletion, tombstone, expired idempotency binding prune,
// and checkpoint update commits in one bbolt write transaction.
func (s *Store) CollectTerminal(policy RetentionPolicy, now time.Time) (GCResult, error) {
	if err := validateRetentionPolicy(policy); err != nil {
		return GCResult{}, err
	}
	if now.IsZero() {
		return GCResult{}, fmt.Errorf("%w: collection time is zero", ErrInvalidRetentionPolicy)
	}
	now = now.UTC()
	result := GCResult{StartedAt: now}
	err := s.db.Update(func(tx *bolt.Tx) error {
		tombstones, err := tx.CreateBucketIfNotExists([]byte(tombstonesBucketName))
		if err != nil {
			return fmt.Errorf("create tombstones bucket: %w", err)
		}
		retention, err := tx.CreateBucketIfNotExists([]byte(retentionBucketName))
		if err != nil {
			return fmt.Errorf("create retention bucket: %w", err)
		}

		activeBindings, expiredBindings, expiredBindingBytes, err := retentionSubmissionBindings(tx, now)
		if err != nil {
			return err
		}
		result.ExpiredSubmissionBindings = expiredBindings
		result.ExpiredSubmissionBytes = expiredBindingBytes
		result.NetLogicalBytesFreed += expiredBindingBytes

		candidates, terminalCount, historicalBytes, err := readRetentionCandidates(tx)
		if err != nil {
			return err
		}
		removed, removedBytes, err := pruneTombstones(tombstones, policy, now, historicalBytes-policy.MaxStateBytes)
		if err != nil {
			return err
		}
		result.RemovedTombstones += removed
		result.RemovedTombstoneBytes += removedBytes
		result.NetLogicalBytesFreed += removedBytes
		historicalBytes -= removedBytes

		var lastEvictedRunID string
		var lastEvictedFinishedAt time.Time
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].finishedAt.Equal(candidates[j].finishedAt) {
				return candidates[i].runID < candidates[j].runID
			}
			return candidates[i].finishedAt.Before(candidates[j].finishedAt)
		})
		for _, candidate := range candidates {
			candidate.reasons = evictionReasons(candidate.finishedAt, now, policy, terminalCount, historicalBytes)
			if len(candidate.reasons) == 0 {
				continue
			}
			if !candidate.collectable {
				result.UncollectableTerminalRuns++
				continue
			}

			tombstoneBytes := int64(0)
			if policy.PreserveTombstones {
				digest, err := receiptDigest(candidate.run.Receipt)
				if err != nil {
					return err
				}
				tombstone := Tombstone{
					Version:            retentionSchema,
					RunID:              candidate.runID,
					Generation:         candidate.run.Generation,
					CreatedAt:          candidate.run.CreatedAt,
					StartedAt:          candidate.run.StartedAt,
					Outcome:            candidate.run.Receipt.Outcome,
					TerminationReason:  candidate.run.TerminationReason,
					FinishedAt:         candidate.finishedAt,
					EvictedAt:          now,
					Reasons:            append([]EvictionReason(nil), candidate.reasons...),
					ReceiptSHA256:      digest,
					Receipt:            compactTerminalReceipt(candidate.run.Receipt),
					EvidenceIncomplete: true,
				}
				encoded, err := json.Marshal(tombstone)
				if err != nil {
					return fmt.Errorf("encode Run tombstone: %w", err)
				}
				if err := tombstones.Put([]byte(candidate.runID), encoded); err != nil {
					return fmt.Errorf("persist Run tombstone: %w", err)
				}
				tombstoneBytes = int64(len(candidate.runID) + len(encoded))
			}
			stubBytes, addedStub, err := storeSubmissionReplayStub(tx, candidate.run, now, activeBindings)
			if err != nil {
				return err
			}
			if addedStub {
				result.SubmissionReplayStubs++
				result.NetLogicalBytesFreed -= stubBytes
			}

			runs := tx.Bucket([]byte(runsBucketName))
			if err := deleteRunEvidenceBuckets(tx, candidate.runID); err != nil {
				return err
			}
			if err := runs.Delete([]byte(candidate.runID)); err != nil {
				return fmt.Errorf("delete terminal Run %q: %w", candidate.runID, err)
			}
			result.EvictedRuns++
			result.EvidenceBytesRemoved += candidate.detailedBytes
			result.NetLogicalBytesFreed += candidate.detailedBytes - tombstoneBytes
			lastEvictedRunID = candidate.runID
			lastEvictedFinishedAt = candidate.finishedAt
			terminalCount--
			historicalBytes -= candidate.detailedBytes
			historicalBytes += tombstoneBytes
		}

		removed, removedBytes, err = pruneTombstones(tombstones, policy, now, historicalBytes-policy.MaxStateBytes)
		if err != nil {
			return err
		}
		result.RemovedTombstones += removed
		result.RemovedTombstoneBytes += removedBytes
		result.NetLogicalBytesFreed += removedBytes
		historicalBytes -= removedBytes

		result.TerminalRunsRemaining = terminalCount
		result.TerminalEvidenceBytes = max(int64(0), historicalBytes)
		result.BudgetExceeded = terminalCount > policy.MaxTerminalRuns || historicalBytes > policy.MaxStateBytes
		policyHash, err := retentionPolicyDigest(policy)
		if err != nil {
			return err
		}
		checkpoint, err := readRetentionCheckpoint(retention)
		if err != nil {
			return err
		}
		checkpoint.Version = retentionSchema
		checkpoint.Policy = policy
		checkpoint.PolicySHA256 = policyHash
		checkpoint.LastRunAt = now
		checkpoint.EvictedRunsTotal = satAddUint64(checkpoint.EvictedRunsTotal, uint64(result.EvictedRuns))
		checkpoint.EvictedBytesTotal = satAddInt64(checkpoint.EvictedBytesTotal, result.EvidenceBytesRemoved)
		checkpoint.RemovedTombstonesTotal = satAddUint64(checkpoint.RemovedTombstonesTotal, uint64(result.RemovedTombstones))
		checkpoint.ExpiredBindingsTotal = satAddUint64(checkpoint.ExpiredBindingsTotal, uint64(result.ExpiredSubmissionBindings))
		checkpoint.TerminalRunsRemaining = terminalCount
		checkpoint.TerminalEvidenceBytes = result.TerminalEvidenceBytes
		checkpoint.ExpiredBindingBytesTotal = satAddInt64(checkpoint.ExpiredBindingBytesTotal, result.ExpiredSubmissionBytes)
		checkpoint.SubmissionReplayStubsTotal = satAddUint64(checkpoint.SubmissionReplayStubsTotal, uint64(result.SubmissionReplayStubs))
		checkpoint.BudgetExceeded = result.BudgetExceeded
		if result.EvictedRuns > 0 {
			checkpoint.LastEvictedRunID = lastEvictedRunID
			finished := lastEvictedFinishedAt
			checkpoint.LastEvictedFinishedAt = &finished
		}
		encoded, err := json.Marshal(checkpoint)
		if err != nil {
			return fmt.Errorf("encode retention checkpoint: %w", err)
		}
		if err := retention.Put([]byte(retentionStateKey), encoded); err != nil {
			return fmt.Errorf("persist retention checkpoint: %w", err)
		}
		return nil
	})
	if err != nil {
		return GCResult{}, err
	}
	stats := s.db.Stats()
	pageSize := int64(s.db.Info().PageSize)
	freeBytes := int64(stats.FreePageN+stats.PendingPageN) * pageSize
	fileInfo, statErr := os.Stat(s.db.Path())
	if statErr == nil && policy.CompactMinFreeBytes > 0 && freeBytes >= policy.CompactMinFreeBytes {
		freeRatio := float64(freeBytes) / float64(max(fileInfo.Size(), int64(1)))
		result.CompactionRecommended = policy.CompactMinFreeRatio <= 0 || freeRatio >= policy.CompactMinFreeRatio
	}
	return result, nil
}

func validateRetentionPolicy(policy RetentionPolicy) error {
	if policy.MaxAge < 0 || policy.MaxTerminalRuns < 0 || policy.MaxStateBytes < 0 ||
		policy.MaxTombstones < 0 || policy.MaxTombstoneAge < 0 || policy.CompactMinFreeBytes < 0 ||
		math.IsNaN(policy.CompactMinFreeRatio) || math.IsInf(policy.CompactMinFreeRatio, 0) ||
		policy.CompactMinFreeRatio < 0 || policy.CompactMinFreeRatio > 1 {
		return fmt.Errorf("%w: limits must be non-negative and compact ratio must be within 0..1", ErrInvalidRetentionPolicy)
	}
	if policy.PreserveTombstones && policy.MaxTombstones == 0 {
		return fmt.Errorf("%w: preserved tombstones require MaxTombstones > 0", ErrInvalidRetentionPolicy)
	}
	return nil
}

func readRetentionCandidates(tx *bolt.Tx) ([]retentionCandidate, int, int64, error) {
	runs := tx.Bucket([]byte(runsBucketName))
	if runs == nil {
		return nil, 0, 0, fmt.Errorf("Run bucket is missing")
	}
	candidates := make([]retentionCandidate, 0)
	terminalCount := 0
	var historicalBytes int64
	err := runs.ForEach(func(key, raw []byte) error {
		if raw == nil {
			return nil
		}
		var run model.Run
		if err := json.Unmarshal(raw, &run); err != nil {
			return fmt.Errorf("decode Run %q during retention: %w", key, err)
		}
		if run.ID != string(key) {
			return fmt.Errorf("Run key %q does not match stored ID %q", key, run.ID)
		}
		if run.State != model.Terminal {
			return nil
		}
		terminalCount++
		runID := string(key)
		detailedBytes, err := runEvidenceBytesTx(tx, runID)
		if err != nil {
			return err
		}
		historicalBytes += detailedBytes
		finishedAt := time.Time{}
		if run.FinishedAt != nil {
			finishedAt = run.FinishedAt.UTC()
		}
		collectable := run.Receipt != nil && !finishedAt.IsZero()
		if collectable && (run.Receipt.RunID != run.ID || run.Receipt.Outcome == "" || !run.Receipt.FinishedAt.Equal(finishedAt)) {
			collectable = false
		}
		candidates = append(candidates, retentionCandidate{
			run:           run,
			runID:         runID,
			finishedAt:    finishedAt,
			detailedBytes: detailedBytes,
			collectable:   collectable,
		})
		return nil
	})
	if err != nil {
		return nil, 0, 0, err
	}
	tombstones := tx.Bucket([]byte(tombstonesBucketName))
	if tombstones != nil {
		bytes, err := bucketLogicalBytes(tombstones)
		if err != nil {
			return nil, 0, 0, err
		}
		historicalBytes += bytes
	}
	return candidates, terminalCount, historicalBytes, nil
}

func evictionReasons(finishedAt, now time.Time, policy RetentionPolicy, terminalCount int, historicalBytes int64) []EvictionReason {
	reasons := make([]EvictionReason, 0, 3)
	if policy.MaxAge > 0 && !finishedAt.IsZero() && !now.Before(finishedAt) && now.Sub(finishedAt) >= policy.MaxAge {
		reasons = append(reasons, EvictionAge)
	}
	if terminalCount > policy.MaxTerminalRuns {
		reasons = append(reasons, EvictionTerminalCount)
	}
	if historicalBytes > policy.MaxStateBytes {
		reasons = append(reasons, EvictionStateBytes)
	}
	return reasons
}

func retentionSubmissionBindings(tx *bolt.Tx, now time.Time) (map[string]bool, int, int64, error) {
	active := make(map[string]bool)
	submissions := tx.Bucket([]byte("submissions"))
	if submissions == nil {
		return active, 0, 0, nil
	}
	runIndex := tx.Bucket([]byte("submission-runs"))
	expiry := tx.Bucket([]byte("submission-expiry"))
	if runIndex == nil || expiry == nil {
		return nil, 0, 0, fmt.Errorf("submission idempotency indexes are missing")
	}
	var expiredKeys [][]byte
	var expiredRunIDs []string
	var expiredRawBytes []int
	var expiryKeys [][]byte
	if err := submissions.ForEach(func(key, raw []byte) error {
		if raw == nil {
			return nil
		}
		var record submissionRetentionRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return fmt.Errorf("decode submission binding during retention: %w", err)
		}
		if record.RunID == "" || record.SpecDigest == "" || record.CreatedAt.IsZero() || record.ExpiresAt.IsZero() {
			return fmt.Errorf("invalid submission binding during retention")
		}
		if record.ExpiresAt.After(now) {
			if active[record.RunID] {
				return fmt.Errorf("multiple active submission bindings for Run %q", record.RunID)
			}
			if !bytes.Equal(runIndex.Get([]byte(record.RunID)), key) {
				return fmt.Errorf("submission Run index mismatch for %q", record.RunID)
			}
			expiryKey := retentionExpiryIndexKey(record.ExpiresAt, key)
			if !bucketHasKey(expiry, expiryKey) {
				return fmt.Errorf("submission expiry index missing for %q", record.RunID)
			}
			active[record.RunID] = true
		} else {
			expiredKeys = append(expiredKeys, append([]byte(nil), key...))
			expiredRunIDs = append(expiredRunIDs, record.RunID)
			expiredRawBytes = append(expiredRawBytes, len(raw))
			expiryKeys = append(expiryKeys, retentionExpiryIndexKey(record.ExpiresAt, key))
		}
		return nil
	}); err != nil {
		return nil, 0, 0, err
	}
	var bytesRemoved int64
	for i, key := range expiredKeys {
		runID := expiredRunIDs[i]
		if err := submissions.Delete(key); err != nil {
			return nil, 0, 0, fmt.Errorf("delete expired submission binding: %w", err)
		}
		bytesRemoved += int64(len(key) + expiredRawBytes[i])
		if bytes.Equal(runIndex.Get([]byte(runID)), key) {
			if err := runIndex.Delete([]byte(runID)); err != nil {
				return nil, 0, 0, fmt.Errorf("delete expired submission index: %w", err)
			}
			bytesRemoved += int64(len(runID) + len(key))
		}
		if err := expiry.Delete(expiryKeys[i]); err != nil {
			return nil, 0, 0, fmt.Errorf("delete expired submission expiry index: %w", err)
		}
		bytesRemoved += int64(len(expiryKeys[i]))
	}
	// Remove stale indexes whose binding is gone. Active bindings were checked
	// above and remain fully indexed.
	var staleRuns [][]byte
	if err := runIndex.ForEach(func(key, value []byte) error {
		if value == nil {
			return nil
		}
		if submissions.Get(value) == nil {
			staleRuns = append(staleRuns, append([]byte(nil), key...))
		}
		return nil
	}); err != nil {
		return nil, 0, 0, err
	}
	for _, runID := range staleRuns {
		value := runIndex.Get(runID)
		if err := runIndex.Delete(runID); err != nil {
			return nil, 0, 0, fmt.Errorf("delete orphaned submission Run index: %w", err)
		}
		bytesRemoved += int64(len(runID) + len(value))
	}
	var staleExpiry [][]byte
	if err := expiry.ForEach(func(key, _ []byte) error {
		if len(key) < 8 {
			return fmt.Errorf("invalid submission expiry index key")
		}
		submissionID := key[8:]
		raw := submissions.Get(submissionID)
		if raw == nil {
			staleExpiry = append(staleExpiry, append([]byte(nil), key...))
			return nil
		}
		var record submissionRetentionRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return fmt.Errorf("decode submission expiry binding: %w", err)
		}
		if !bytes.Equal(key, retentionExpiryIndexKey(record.ExpiresAt, submissionID)) {
			staleExpiry = append(staleExpiry, append([]byte(nil), key...))
		}
		return nil
	}); err != nil {
		return nil, 0, 0, err
	}
	for _, key := range staleExpiry {
		if err := expiry.Delete(key); err != nil {
			return nil, 0, 0, fmt.Errorf("delete stale submission expiry index: %w", err)
		}
		bytesRemoved += int64(len(key))
	}
	return active, len(expiredKeys), bytesRemoved, nil
}

func bucketHasKey(bucket *bolt.Bucket, key []byte) bool {
	if bucket == nil {
		return false
	}
	found, _ := bucket.Cursor().Seek(key)
	return bytes.Equal(found, key)
}

func retentionExpiryIndexKey(expiry time.Time, key []byte) []byte {
	indexed := make([]byte, 8+len(key))
	binary.BigEndian.PutUint64(indexed[:8], uint64(expiry.UnixNano()))
	copy(indexed[8:], key)
	return indexed
}

func storeSubmissionReplayStub(tx *bolt.Tx, run model.Run, now time.Time, activeBindings map[string]bool) (int64, bool, error) {
	if !activeBindings[run.ID] {
		return 0, false, nil
	}
	submissions := tx.Bucket([]byte("submissions"))
	runIndex := tx.Bucket([]byte("submission-runs"))
	if submissions == nil || runIndex == nil {
		return 0, false, fmt.Errorf("live submission binding indexes are missing")
	}
	submissionID := runIndex.Get([]byte(run.ID))
	if submissionID == nil {
		return 0, false, fmt.Errorf("live submission Run index is missing")
	}
	raw := submissions.Get(submissionID)
	if raw == nil {
		return 0, false, fmt.Errorf("live submission binding is missing")
	}
	var record submissionRetentionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return 0, false, fmt.Errorf("decode submission binding before Run collection: %w", err)
	}
	if record.RunID != run.ID || !record.ExpiresAt.After(now) {
		return 0, false, fmt.Errorf("submission binding changed during Run collection")
	}
	if record.CollectedRun != nil {
		if record.CollectedRun.ID != run.ID || record.CollectedRun.State != model.Terminal {
			return 0, false, fmt.Errorf("invalid existing submission replay stub")
		}
		return 0, false, nil
	}
	stub := collectedRunSnapshot(run)
	record.CollectedRun = &stub
	encoded, err := json.Marshal(record)
	if err != nil {
		return 0, false, fmt.Errorf("encode submission replay stub: %w", err)
	}
	if err := submissions.Put(submissionID, encoded); err != nil {
		return 0, false, fmt.Errorf("persist submission replay stub: %w", err)
	}
	return int64(len(encoded) - len(raw)), true, nil
}

func deleteRunEvidenceBuckets(tx *bolt.Tx, runID string) error {
	names := txBucketNames(tx)
	for _, name := range names {
		if name == runsBucketName || isGlobalRetentionBucket(name) {
			continue
		}
		root := tx.Bucket([]byte(name))
		if root.Bucket([]byte(runID)) == nil {
			continue
		}
		if err := root.DeleteBucket([]byte(runID)); err != nil {
			return fmt.Errorf("delete Run %q evidence bucket %q: %w", runID, name, err)
		}
	}
	return nil
}

func pruneTombstones(bucket *bolt.Bucket, policy RetentionPolicy, now time.Time, bytesOverBudget int64) (int, int64, error) {
	tombstones := make([]Tombstone, 0)
	encodedByID := make(map[string]int64)
	if err := bucket.ForEach(func(key, raw []byte) error {
		if raw == nil {
			return nil
		}
		var tombstone Tombstone
		if err := json.Unmarshal(raw, &tombstone); err != nil {
			return fmt.Errorf("decode Run tombstone %q: %w", key, err)
		}
		if tombstone.RunID != string(key) || tombstone.Version != retentionSchema || !tombstone.EvidenceIncomplete {
			return fmt.Errorf("invalid Run tombstone %q", key)
		}
		tombstones = append(tombstones, tombstone)
		encodedByID[tombstone.RunID] = int64(len(key) + len(raw))
		return nil
	}); err != nil {
		return 0, 0, err
	}
	sort.Slice(tombstones, func(i, j int) bool {
		if tombstones[i].EvictedAt.Equal(tombstones[j].EvictedAt) {
			return tombstones[i].RunID < tombstones[j].RunID
		}
		return tombstones[i].EvictedAt.Before(tombstones[j].EvictedAt)
	})
	remove := make(map[string]bool)
	remaining := len(tombstones)
	for _, tombstone := range tombstones {
		if !policy.PreserveTombstones || (policy.MaxTombstoneAge > 0 && !now.Before(tombstone.EvictedAt) && now.Sub(tombstone.EvictedAt) >= policy.MaxTombstoneAge) {
			remove[tombstone.RunID] = true
			remaining--
		}
	}
	for _, tombstone := range tombstones {
		if remaining <= policy.MaxTombstones {
			break
		}
		if remove[tombstone.RunID] {
			continue
		}
		remove[tombstone.RunID] = true
		remaining--
	}
	for _, tombstone := range tombstones {
		if bytesOverBudget <= 0 {
			break
		}
		if remove[tombstone.RunID] {
			continue
		}
		remove[tombstone.RunID] = true
		remaining--
		bytesOverBudget -= encodedByID[tombstone.RunID]
	}
	var removed int
	var removedBytes int64
	for runID := range remove {
		if err := bucket.Delete([]byte(runID)); err != nil {
			return 0, 0, fmt.Errorf("delete expired Run tombstone %q: %w", runID, err)
		}
		removed++
		removedBytes += encodedByID[runID]
	}
	return removed, removedBytes, nil
}

func readRetentionCheckpoint(bucket *bolt.Bucket) (RetentionCheckpoint, error) {
	var state RetentionCheckpoint
	raw := bucket.Get([]byte(retentionStateKey))
	if raw == nil {
		return state, nil
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return RetentionCheckpoint{}, fmt.Errorf("decode retention checkpoint: %w", err)
	}
	return state, nil
}

func retentionPolicyDigest(policy RetentionPolicy) (string, error) {
	encoded, err := json.Marshal(policy)
	if err != nil {
		return "", err
	}
	return digestBytes(encoded), nil
}

func satAddUint64(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func satAddInt64(a, b int64) int64 {
	if b > 0 && a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}
