package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

const (
	tombstonesBucketName = "tombstones"
	retentionBucketName  = "retention"
	retentionStateKey    = "state"
	retentionSchema      = 1
)

var ErrTombstoneNotFound = errors.New("Run tombstone not found")

// RetentionPolicy bounds terminal Run detail and the tombstones left after
// detail eviction. MaxAge=0 disables age-triggered Run eviction. A zero
// MaxTerminalRuns or MaxStateBytes retains none of that category. Tombstones
// are preserved only when PreserveTombstones is true; their count remains
// bounded by MaxTombstones (zero is valid only when preservation is false).
//
// MaxStateBytes measures logical bytes for terminal Run records, their
// per-Run evidence buckets, and tombstones. It excludes live/non-terminal
// Runs, compact replay projections in the fixed-capacity submission index,
// and idempotency/control/watch indexes, which have independent hard bounds.
type RetentionPolicy struct {
	MaxAge              time.Duration `json:"maxAge"`
	MaxTerminalRuns     int           `json:"maxTerminalRuns"`
	MaxStateBytes       int64         `json:"maxStateBytes"`
	PreserveTombstones  bool          `json:"preserveTombstones"`
	MaxTombstones       int           `json:"maxTombstones"`
	MaxTombstoneAge     time.Duration `json:"maxTombstoneAge"`
	CompactMinFreeBytes int64         `json:"compactMinFreeBytes"`
	CompactMinFreeRatio float64       `json:"compactMinFreeRatio"`
}

// DefaultRetentionPolicy is intentionally finite for terminal history,
// tombstone count/age, and bbolt compaction pressure. Callers can pass an
// explicit policy to CollectTerminal to override these defaults.
func DefaultRetentionPolicy() RetentionPolicy {
	return RetentionPolicy{
		MaxAge:              30 * 24 * time.Hour,
		MaxTerminalRuns:     10_000,
		MaxStateBytes:       512 << 20,
		PreserveTombstones:  true,
		MaxTombstones:       100_000,
		MaxTombstoneAge:     30 * 24 * time.Hour,
		CompactMinFreeBytes: 64 << 20,
		CompactMinFreeRatio: 0.25,
	}
}

// Tombstone is the bounded terminal record left after detailed Run evidence
// is removed. It carries no accepted specification, output bytes, event
// bodies, telemetry, process observations, or full receipt. Its compact
// receipt preserves terminal identity/outcome and marks removed evidence
// unavailable. ReceiptSHA256 binds it to the original receipt.
type Tombstone struct {
	Version            int              `json:"version"`
	RunID              string           `json:"runId"`
	Generation         uint64           `json:"generation"`
	CreatedAt          time.Time        `json:"createdAt"`
	StartedAt          *time.Time       `json:"startedAt,omitempty"`
	Outcome            string           `json:"outcome"`
	TerminationReason  string           `json:"terminationReason,omitempty"`
	FinishedAt         time.Time        `json:"finishedAt"`
	EvictedAt          time.Time        `json:"evictedAt"`
	Reasons            []EvictionReason `json:"reasons"`
	ReceiptSHA256      string           `json:"receiptSha256"`
	Receipt            model.Receipt    `json:"receipt"`
	EvidenceIncomplete bool             `json:"evidenceIncomplete"`
}

type EvictionReason string

const (
	EvictionAge           EvictionReason = "age"
	EvictionTerminalCount EvictionReason = "terminal-count"
	EvictionStateBytes    EvictionReason = "state-byte-budget"
)

// RetentionCheckpoint is one bounded, durable summary of GC decisions. It
// remains after individual tombstones age out, so deletion policy and gaps in
// detailed evidence remain machine-readable.
type RetentionCheckpoint struct {
	Version                    int             `json:"version"`
	Policy                     RetentionPolicy `json:"policy"`
	PolicySHA256               string          `json:"policySha256"`
	LastRunAt                  time.Time       `json:"lastRunAt"`
	EvictedRunsTotal           uint64          `json:"evictedRunsTotal"`
	EvictedBytesTotal          int64           `json:"evictedBytesTotal"`
	RemovedTombstonesTotal     uint64          `json:"removedTombstonesTotal"`
	ExpiredBindingsTotal       uint64          `json:"expiredBindingsTotal"`
	ExpiredBindingBytesTotal   int64           `json:"expiredBindingBytesTotal"`
	SubmissionReplayStubsTotal uint64          `json:"submissionReplayStubsTotal"`
	LastEvictedRunID           string          `json:"lastEvictedRunId,omitempty"`
	LastEvictedFinishedAt      *time.Time      `json:"lastEvictedFinishedAt,omitempty"`
	TerminalRunsRemaining      int             `json:"terminalRunsRemaining"`
	TerminalEvidenceBytes      int64           `json:"terminalEvidenceBytes"`
	BudgetExceeded             bool            `json:"budgetExceeded"`
}

// Usage describes logical bbolt contents and the physical database footprint.
// ActiveRunCount excludes uncertain Runs, which are reported separately so
// unknown physical state is never presented as a known live workload.
// Logical byte counts omit bbolt page/allocator overhead; DatabaseBytes and
// free-page counts expose that separate physical dimension.
type Usage struct {
	RunCount                  int   `json:"runCount"`
	ActiveRunCount            int   `json:"activeRunCount"`
	TerminalRunCount          int   `json:"terminalRunCount"`
	UncertainRunCount         int   `json:"uncertainRunCount"`
	TombstoneCount            int   `json:"tombstoneCount"`
	ExpiredBindingCount       int   `json:"expiredBindingCount"`
	LogicalBytes              int64 `json:"logicalBytes"`
	RunBytes                  int64 `json:"runBytes"`
	EventBytes                int64 `json:"eventBytes"`
	OutputBytes               int64 `json:"outputBytes"`
	TelemetryBytes            int64 `json:"telemetryBytes"`
	TombstoneBytes            int64 `json:"tombstoneBytes"`
	SubmissionBytes           int64 `json:"submissionBytes"`
	SubmissionReplayStubBytes int64 `json:"submissionReplayStubBytes"`
	ControlRequestBytes       int64 `json:"controlRequestBytes"`
	WatchIndexBytes           int64 `json:"watchIndexBytes"`
	OtherBytes                int64 `json:"otherBytes"`
	TerminalEvidenceBytes     int64 `json:"terminalEvidenceBytes"`
	DatabaseBytes             int64 `json:"databaseBytes"`
	PageSize                  int   `json:"pageSize"`
	FreePages                 int   `json:"freePages"`
	PendingPages              int   `json:"pendingPages"`
	FreeBytes                 int64 `json:"freeBytes"`
}

// GetTombstone returns the compact terminal record for an evicted Run.
func (s *Store) GetTombstone(runID string) (Tombstone, error) {
	var tombstone Tombstone
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(tombstonesBucketName))
		if bucket == nil || bucket.Get([]byte(runID)) == nil {
			return ErrTombstoneNotFound
		}
		if err := json.Unmarshal(bucket.Get([]byte(runID)), &tombstone); err != nil {
			return fmt.Errorf("decode Run tombstone %q: %w", runID, err)
		}
		return nil
	})
	return tombstone, err
}

// RunSnapshot reconstructs the terminal identity and compact receipt retained
// for a retry-safe submission after its detailed Run evidence has been
// collected. The accepted specification and process ownership are omitted.
func (t Tombstone) RunSnapshot() model.Run {
	receipt := t.Receipt
	finished := t.FinishedAt
	var started *time.Time
	if t.StartedAt != nil {
		copy := *t.StartedAt
		started = &copy
	}
	return model.Run{
		ID: t.RunID, State: model.Terminal, Generation: t.Generation,
		CreatedAt: t.CreatedAt, StartedAt: started, FinishedAt: &finished,
		Resources: receipt.Resources, Output: receipt.Output,
		EffectiveCapabilities: receipt.EffectiveCapabilities,
		Receipt:               &receipt, TerminationReason: t.TerminationReason, ResourceGap: true,
	}
}

// RetentionState returns the last persisted GC summary, or a zero checkpoint
// before the first collection pass.
func (s *Store) RetentionState() (RetentionCheckpoint, error) {
	var state RetentionCheckpoint
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(retentionBucketName))
		if bucket == nil || bucket.Get([]byte(retentionStateKey)) == nil {
			return nil
		}
		if err := json.Unmarshal(bucket.Get([]byte(retentionStateKey)), &state); err != nil {
			return fmt.Errorf("decode retention checkpoint: %w", err)
		}
		return nil
	})
	return state, err
}

// Usage returns store-wide logical byte counts plus physical bbolt file and
// free-page measurements.
func (s *Store) Usage() (Usage, error) {
	var usage Usage
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		usage.LogicalBytes, err = logicalBytesTx(tx)
		if err != nil {
			return err
		}
		for _, name := range txBucketNames(tx) {
			bucket := tx.Bucket([]byte(name))
			bytes, err := bucketLogicalBytes(bucket)
			if err != nil {
				return err
			}
			switch name {
			case runsBucketName:
				usage.RunBytes = bytes
				if err := bucket.ForEach(func(_, raw []byte) error {
					if raw == nil {
						return nil
					}
					var run model.Run
					if err := json.Unmarshal(raw, &run); err != nil {
						return fmt.Errorf("decode Run for usage: %w", err)
					}
					usage.RunCount++
					if run.State == model.Terminal {
						usage.TerminalRunCount++
					} else if run.State == model.Uncertain {
						usage.UncertainRunCount++
					} else {
						usage.ActiveRunCount++
					}
					if run.State == model.Terminal {
						n, err := runEvidenceBytesTx(tx, string(run.ID))
						if err != nil {
							return err
						}
						usage.TerminalEvidenceBytes += n
					}
					return nil
				}); err != nil {
					return err
				}
			case eventsBucketName:
				usage.EventBytes = bytes
			case outputBucketName:
				usage.OutputBytes = bytes
			case tombstonesBucketName:
				usage.TombstoneBytes = bytes
				if err := bucket.ForEach(func(_, raw []byte) error {
					if raw != nil {
						usage.TombstoneCount++
					}
					return nil
				}); err != nil {
					return err
				}
				usage.TerminalEvidenceBytes += bytes
			case submissionsBucketName, submissionRunsBucketName, submissionExpiryBucketName:
				usage.SubmissionBytes += bytes
			case controlRequestsBucketName, controlRequestExpiryBucket:
				usage.ControlRequestBytes += bytes
			case "watch-index":
				usage.WatchIndexBytes = bytes
			default:
				if isTelemetryBucket(name) {
					usage.TelemetryBytes += bytes
				} else {
					usage.OtherBytes += bytes
				}
			}
		}
		if submissions := tx.Bucket([]byte(submissionsBucketName)); submissions != nil {
			if err := submissions.ForEach(func(_, raw []byte) error {
				if raw == nil {
					return nil
				}
				binding, err := decodeSubmissionBinding(raw)
				if err != nil {
					return err
				}
				if !binding.ExpiresAt.IsZero() && !binding.ExpiresAt.After(time.Now()) {
					usage.ExpiredBindingCount++
				}
				return nil
			}); err != nil {
				return err
			}
		}
		stubBytes, err := submissionReplayStubBytesTx(tx)
		if err != nil {
			return err
		}
		usage.SubmissionReplayStubBytes = stubBytes
		return nil
	})
	if err != nil {
		return Usage{}, err
	}
	stats := s.db.Stats()
	pageSize := s.db.Info().PageSize
	usage.PageSize = pageSize
	usage.FreePages = stats.FreePageN
	usage.PendingPages = stats.PendingPageN
	usage.FreeBytes = int64(stats.FreePageN+stats.PendingPageN) * int64(pageSize)
	info, err := os.Stat(s.db.Path())
	if err != nil {
		return Usage{}, fmt.Errorf("stat bbolt database: %w", err)
	}
	usage.DatabaseBytes = info.Size()
	return usage, nil
}

func txBucketNames(tx *bolt.Tx) []string {
	var names []string
	_ = tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		names = append(names, string(name))
		return nil
	})
	sort.Strings(names)
	return names
}

func logicalBytesTx(tx *bolt.Tx) (int64, error) {
	var total int64
	err := tx.ForEach(func(_ []byte, bucket *bolt.Bucket) error {
		bytes, err := bucketLogicalBytes(bucket)
		if err != nil {
			return err
		}
		total += bytes
		return nil
	})
	return total, err
}

func bucketLogicalBytes(bucket *bolt.Bucket) (int64, error) {
	var total int64
	err := bucket.ForEach(func(key, value []byte) error {
		total += int64(len(key))
		if value != nil {
			total += int64(len(value))
			return nil
		}
		child := bucket.Bucket(key)
		if child == nil {
			return fmt.Errorf("bbolt bucket %q disappeared during usage scan", key)
		}
		size, err := bucketLogicalBytes(child)
		if err != nil {
			return err
		}
		total += size
		return nil
	})
	return total, err
}

func runEvidenceBytesTx(tx *bolt.Tx, runID string) (int64, error) {
	var total int64
	for _, name := range txBucketNames(tx) {
		if isGlobalRetentionBucket(name) || name == runsBucketName {
			continue
		}
		root := tx.Bucket([]byte(name))
		perRun := root.Bucket([]byte(runID))
		if perRun == nil {
			continue
		}
		bytes, err := bucketLogicalBytes(perRun)
		if err != nil {
			return 0, err
		}
		total += int64(len(runID)) + bytes
	}
	runs := tx.Bucket([]byte(runsBucketName))
	if raw := runs.Get([]byte(runID)); raw != nil {
		total += int64(len(runID) + len(raw))
	}
	return total, nil
}

func isGlobalRetentionBucket(name string) bool {
	switch name {
	case tombstonesBucketName, retentionBucketName,
		submissionsBucketName, submissionRunsBucketName, submissionExpiryBucketName,
		controlRequestsBucketName, controlRequestExpiryBucket, "watch-index", "idempotency":
		return true
	default:
		return false
	}
}

func submissionReplayStubBytesTx(tx *bolt.Tx) (int64, error) {
	var total int64
	submissions := tx.Bucket([]byte(submissionsBucketName))
	if submissions == nil {
		return 0, nil
	}
	err := submissions.ForEach(func(key, raw []byte) error {
		if raw == nil {
			return nil
		}
		binding, err := decodeSubmissionBinding(raw)
		if err != nil {
			return err
		}
		if binding.CollectedRun == nil {
			return nil
		}
		if binding.CollectedRun.ID != binding.RunID || binding.CollectedRun.State != model.Terminal ||
			binding.CollectedRun.Receipt == nil || binding.CollectedRun.Receipt.RunID != binding.RunID ||
			!binding.CollectedRun.Receipt.EvidenceIncomplete || binding.CollectedRun.Receipt.EventHistoryComplete ||
			binding.CollectedRun.Receipt.Output.HistoryComplete {
			return fmt.Errorf("invalid submission replay stub for binding %q", key)
		}
		encoded, err := json.Marshal(binding.CollectedRun)
		if err != nil {
			return fmt.Errorf("encode submission replay stub: %w", err)
		}
		total += int64(len(encoded))
		return nil
	})
	return total, err
}

func compactTerminalReceipt(original *model.Receipt) model.Receipt {
	if original == nil {
		return model.Receipt{EvidenceIncomplete: true, EventHistoryComplete: false, Output: model.Output{HistoryComplete: false}}
	}
	receipt := *original
	receipt.Resources = unavailableResources(original.Resources)
	receipt.Output = discardOutputContent(original.Output)
	receipt.EventRetainedFrom = nextSequenceOrMax(receipt.EventLastSeq)
	receipt.EventHistoryComplete = false
	receipt.EvidenceIncomplete = true
	return receipt
}

func unavailableResources(resources model.Resources) model.Resources {
	metric := func(value model.Metric) model.Metric {
		if value.Status == "unsupported" {
			return model.Metric{Status: "unsupported"}
		}
		return model.Metric{Status: "unavailable"}
	}
	return model.Resources{
		MemoryBytes: metric(resources.MemoryBytes), PeakMemoryBytes: metric(resources.PeakMemoryBytes),
		CPUTimeNs: metric(resources.CPUTimeNs), ProcessCount: metric(resources.ProcessCount),
		PeakProcessCount: metric(resources.PeakProcessCount), TaskCount: metric(resources.TaskCount),
		PeakTaskCount: metric(resources.PeakTaskCount), SampleIntervalMs: resources.SampleIntervalMs,
	}
}

func discardOutputContent(output model.Output) model.Output {
	compact := func(stream model.OutputStream) model.OutputStream {
		stream.RetainedFrom = stream.ObservedBytes
		stream.RetainedBytes = 0
		stream.Truncated = stream.ObservedBytes > 0
		return stream
	}
	output.Stdout = compact(output.Stdout)
	output.Stderr = compact(output.Stderr)
	output.PTY = compact(output.PTY)
	output.HistoryComplete = false
	return output
}

func nextSequenceOrMax(seq uint64) uint64 {
	if seq == ^uint64(0) {
		return seq
	}
	return seq + 1
}

func isTelemetryBucket(name string) bool {
	return name == "telemetry" || name == "process-evidence" || name == "process_evidence" || name == "resource-telemetry"
}

func receiptDigest(receipt *model.Receipt) (string, error) {
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return "", fmt.Errorf("encode receipt digest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func digestBytes(encoded []byte) string {
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
