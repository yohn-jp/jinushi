package supervisor

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/store"
)

var errInvalidRetentionConfig = errors.New("invalid terminal retention configuration")

type compactionObservation struct {
	status      string
	errorCode   string
	attemptedAt *time.Time
	uncertain   bool
}

func (s *Service) recordCompaction(status, code string, attemptedAt time.Time, uncertain bool) {
	s.retentionStatusMu.Lock()
	defer s.retentionStatusMu.Unlock()
	at := attemptedAt.UTC()
	s.compaction = compactionObservation{status: status, errorCode: code, attemptedAt: &at, uncertain: uncertain}
}

func (s *Service) compactionSnapshot() compactionObservation {
	s.retentionStatusMu.RLock()
	defer s.retentionStatusMu.RUnlock()
	observation := s.compaction
	if observation.attemptedAt != nil {
		at := *observation.attemptedAt
		observation.attemptedAt = &at
	}
	if observation.status == "" {
		observation.status = "not-attempted"
	}
	return observation
}

func validateRetentionConfig(config RetentionConfig) error {
	maxDurationMs := int64(math.MaxInt64 / int64(time.Millisecond))
	if config.MaxAgeMs < 0 || config.MaxAgeMs > maxDurationMs ||
		config.MaxTerminalRuns < 0 || config.MaxStateBytes < 0 ||
		config.MaxTombstones < 0 || config.MaxTombstoneAgeMs < 0 || config.MaxTombstoneAgeMs > maxDurationMs ||
		config.CompactMinFreeBytes < 0 || math.IsNaN(config.CompactMinFreeRatio) ||
		math.IsInf(config.CompactMinFreeRatio, 0) || config.CompactMinFreeRatio < 0 || config.CompactMinFreeRatio > 1 {
		return fmt.Errorf("%w: limits must be non-negative and compact ratio must be within 0..1", errInvalidRetentionConfig)
	}
	if config.PreserveTombstones && config.MaxTombstones == 0 {
		return fmt.Errorf("%w: preserved tombstones require maxTombstones > 0", errInvalidRetentionConfig)
	}
	return nil
}

func (s *Service) runRetentionPass(now time.Time) (store.GCResult, error) {
	s.retentionPassMu.Lock()
	defer s.retentionPassMu.Unlock()
	if s.store == nil {
		return store.GCResult{}, errors.New("retention store is unavailable")
	}
	policy := s.config.Retention.policy()
	result, err := s.store.CollectTerminal(policy, now)
	if err != nil || !result.CompactionRecommended {
		return result, err
	}
	// An uncertain previous cutover is deliberately sticky for this Service
	// process. Retrying could compact a path whose durable ownership is unknown.
	if s.compactionSnapshot().uncertain {
		return result, nil
	}
	attemptedAt := now.UTC()
	s.recordCompaction("attempting", "", attemptedAt, false)
	if err := s.store.CompactOnline(); err != nil {
		status, code, uncertain := compactionFailure(err)
		s.recordCompaction(status, code, attemptedAt, uncertain)
		return result, fmt.Errorf("online database compaction %s: %w", code, err)
	}
	s.recordCompaction("completed", "", attemptedAt, false)
	needed, err := s.store.CompactionNeeded(policy)
	if err != nil {
		return result, fmt.Errorf("recheck database compaction need: %w", err)
	}
	result.CompactionRecommended = needed
	return result, nil
}

func compactionFailure(err error) (status, code string, uncertain bool) {
	switch {
	case errors.Is(err, store.ErrCompactionUncertain):
		return "uncertain", "compaction-uncertain", true
	case errors.Is(err, store.ErrCompactionUnsupported):
		return "unsupported", "compaction-unsupported", false
	case errors.Is(err, store.ErrCompactionArtifactsPending):
		return "blocked", "compaction-artifacts-pending", false
	case errors.Is(err, store.ErrCompactionPathUnsafe):
		return "blocked", "compaction-path-unsafe", false
	case errors.Is(err, store.ErrCompactionOwnershipChanged):
		return "failed", "compaction-ownership-changed", false
	default:
		return "failed", "compaction-failed", false
	}
}

func (s *Service) retentionLoop() {
	ticker := time.NewTicker(time.Duration(s.config.RetentionIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	// Run once on startup so an old store does not wait a full interval before
	// applying its configured bounds.
	_, _ = s.runRetentionPass(time.Now().UTC())
	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			_, _ = s.runRetentionPass(now.UTC())
		}
	}
}

func (s *Service) startRetentionWorker() {
	s.launch(s.retentionLoop)
}

// loadRunForInspection keeps terminal identity queryable after the detailed
// record has been collected. The store commits its tombstone with deletion,
// so the fallback cannot observe an intentional gap between those records.
func (s *Service) loadRunForInspection(id string) (model.Run, *store.Tombstone, error) {
	run, err := s.store.Get(id)
	if err == nil {
		return run, nil, nil
	}
	if !errors.Is(err, store.ErrRunNotFound) {
		return model.Run{}, nil, err
	}
	tombstone, err := s.store.GetTombstone(id)
	if err != nil {
		if errors.Is(err, store.ErrTombstoneNotFound) {
			return model.Run{}, nil, store.ErrRunNotFound
		}
		return model.Run{}, nil, err
	}
	return tombstone.RunSnapshot(), &tombstone, nil
}

func toTombstoneSummary(tombstone *store.Tombstone) *model.TombstoneSummary {
	if tombstone == nil {
		return nil
	}
	reasons := make([]string, 0, min(len(tombstone.Reasons), 3))
	for _, reason := range tombstone.Reasons {
		if len(reasons) == 3 {
			break
		}
		reasons = append(reasons, string(reason))
	}
	return &model.TombstoneSummary{
		RunID: tombstone.RunID, EvictedAt: tombstone.EvictedAt,
		Reasons: reasons, ReceiptSHA256: tombstone.ReceiptSHA256,
	}
}

func (s *Service) runtimeStatus() (model.RuntimeStatus, error) {
	policy := model.RuntimeRetentionPolicy{
		MaxAgeMs: s.config.Retention.MaxAgeMs, MaxTerminalRuns: s.config.Retention.MaxTerminalRuns,
		MaxStateBytes: s.config.Retention.MaxStateBytes, PreserveTombstones: s.config.Retention.PreserveTombstones,
		MaxTombstones: s.config.Retention.MaxTombstones, MaxTombstoneAgeMs: s.config.Retention.MaxTombstoneAgeMs,
		CompactMinFreeBytes: s.config.Retention.CompactMinFreeBytes,
		CompactMinFreeRatio: s.config.Retention.CompactMinFreeRatio,
	}
	compaction := s.compactionSnapshot()
	status := model.RuntimeStatus{
		Version: 1,
		Store:   model.RuntimeStoreUsage{Status: "unavailable"},
		Retention: model.RuntimeRetentionStatus{
			Status: "unavailable", Policy: policy, CompactionStatus: compaction.status,
			CompactionErrorCode: compaction.errorCode, CompactionAttemptedAt: compaction.attemptedAt,
		},
	}
	usage, err := s.store.Usage()
	if err != nil {
		return status, err
	}
	status.Store = model.RuntimeStoreUsage{
		Status:   "available",
		RunCount: usage.RunCount, NonterminalRunCount: usage.ActiveRunCount,
		TerminalRunCount: usage.TerminalRunCount, UncertainRunCount: usage.UncertainRunCount,
		TombstoneCount: usage.TombstoneCount, ExpiredBindingCount: usage.ExpiredBindingCount,
		LogicalBytes: usage.LogicalBytes, RunBytes: usage.RunBytes, EventBytes: usage.EventBytes,
		OutputBytes: usage.OutputBytes, TelemetryBytes: usage.TelemetryBytes,
		TombstoneBytes: usage.TombstoneBytes, SubmissionBytes: usage.SubmissionBytes,
		SubmissionReplayStubBytes: usage.SubmissionReplayStubBytes,
		ControlRequestBytes:       usage.ControlRequestBytes, WatchIndexBytes: usage.WatchIndexBytes,
		OtherBytes: usage.OtherBytes, TerminalEvidenceBytes: usage.TerminalEvidenceBytes,
		DatabaseBytes: usage.DatabaseBytes, PageSize: usage.PageSize,
		FreePages: usage.FreePages, PendingPages: usage.PendingPages, FreeBytes: usage.FreeBytes,
	}
	checkpoint, err := s.store.RetentionState()
	if err != nil {
		return status, err
	}
	status.Retention.Status = "available"
	if !checkpoint.LastRunAt.IsZero() {
		lastRun := checkpoint.LastRunAt
		status.Retention.LastRunAt = &lastRun
	}
	status.Retention.EvictedRunsTotal = checkpoint.EvictedRunsTotal
	status.Retention.EvictedBytesTotal = checkpoint.EvictedBytesTotal
	status.Retention.RemovedTombstonesTotal = checkpoint.RemovedTombstonesTotal
	status.Retention.ExpiredBindingsTotal = checkpoint.ExpiredBindingsTotal
	status.Retention.SubmissionReplayStubsTotal = checkpoint.SubmissionReplayStubsTotal
	status.Retention.TerminalRunsRemaining = checkpoint.TerminalRunsRemaining
	status.Retention.TerminalEvidenceBytes = checkpoint.TerminalEvidenceBytes
	status.Retention.BudgetExceeded = checkpoint.BudgetExceeded
	if checkpoint.LastEvictedFinishedAt != nil {
		lastFinished := *checkpoint.LastEvictedFinishedAt
		status.Retention.LastEvictedFinishedAt = &lastFinished
	}
	compactionRecommended, err := s.store.CompactionNeeded(s.config.Retention.policy())
	if err != nil {
		return status, err
	}
	status.Retention.CompactionRecommended = compactionRecommended
	return status, nil
}
