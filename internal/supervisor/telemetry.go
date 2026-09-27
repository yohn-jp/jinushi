package supervisor

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

const (
	defaultTelemetryPageLimit = 16
	maxTelemetryPageLimit     = 1000
	telemetryFrameReserve     = 4096
)

type telemetryObserver interface {
	ObserveTelemetry() (model.TelemetrySample, error)
}

type finalTelemetrySource interface {
	FinalTelemetry() *model.TelemetrySample
}

func telemetrySampleFromResources(runID string, resources model.Resources, capabilities model.Capabilities, observedAt time.Time) model.TelemetrySample {
	resources = normalizeResources(resources, capabilities, resources.SampleIntervalMs)
	unsupported := func() model.Metric { return model.Metric{Status: string(model.EvidenceUnsupported)} }
	processStatus := model.EvidenceUnsupported
	processReason := "process-evidence-unsupported"
	if capabilities.ProcessTelemetry {
		processStatus = model.EvidenceUnavailable
		processReason = "process-evidence-unavailable"
	}
	pressure := func() model.PressureMetrics {
		return model.PressureMetrics{
			SomeAvg10BasisPoints: unsupported(), SomeAvg60BasisPoints: unsupported(),
			SomeAvg300BasisPoints: unsupported(), SomeTotalUS: unsupported(),
			FullAvg10BasisPoints: unsupported(), FullAvg60BasisPoints: unsupported(),
			FullAvg300BasisPoints: unsupported(), FullTotalUS: unsupported(),
		}
	}
	return model.TelemetrySample{
		Version: model.TelemetrySchemaVersion, RunID: runID, ObservedAt: observedAt.UTC(),
		Resources: resources,
		IO: model.IOMetrics{
			ReadBytes: unsupported(), WriteBytes: unsupported(), ReadOperations: unsupported(), WriteOperations: unsupported(),
			DeviceEvidenceStatus: model.EvidenceUnsupported,
		},
		PSI: model.PSIMetrics{CPU: pressure(), Memory: pressure(), IO: pressure()},
		Activity: model.IOActivity{
			InputBytes: unsupported(), InputWrites: unsupported(), ResizeCount: unsupported(),
		},
		Processes: []model.ProcessEvidence{}, ProcessChanges: []model.ProcessEvidenceChange{},
		ProcessEvidenceStatus: processStatus, ProcessEvidenceReason: processReason,
		ProcessEvidenceComplete: false,
	}
}

func (s *Service) queryTelemetry(req protocol.Request) protocol.Response {
	if req.TelemetryQuery == nil {
		return failure("invalid-request", "telemetryQuery is required")
	}
	query := *req.TelemetryQuery
	if query.RunID == "" {
		query.RunID = req.RunID
	}
	if query.RunID == "" || req.RunID != "" && req.RunID != query.RunID {
		return failure("invalid-request", "telemetry Run ID is missing or inconsistent")
	}
	if query.Limit <= 0 {
		query.Limit = defaultTelemetryPageLimit
	}
	if query.Limit > maxTelemetryPageLimit {
		return failure("invalid-request", "telemetry query limit exceeds 1000 points")
	}
	for {
		telemetry, err := s.store.QueryTelemetry(query)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrRunNotFound):
				return s.missingRunFailure(query.RunID, "Run or telemetry history not found")
			case errors.Is(err, store.ErrTelemetryCursorStale):
				return failure("stale-telemetry-cursor", "telemetry cursor is stale after compaction")
			case errors.Is(err, store.ErrInvalidTelemetry):
				return failure("invalid-request", "telemetry query is invalid")
			default:
				return failure("storage-failure", "telemetry query failed")
			}
		}
		run, err := s.store.Get(query.RunID)
		if err != nil {
			if errors.Is(err, store.ErrRunNotFound) {
				return s.missingRunFailure(query.RunID, "Run or telemetry history not found")
			}
			return failure("storage-failure", "Run metadata could not be read")
		}
		out := response()
		out.Telemetry = &telemetry
		clean := publicRun(run)
		out.Run = &clean
		encoded, err := json.Marshal(out)
		if err != nil {
			return failure("storage-failure", "telemetry response could not be encoded")
		}
		if len(encoded) <= protocol.MaxFrame-telemetryFrameReserve {
			return out
		}
		if query.Limit <= 1 {
			return failure("response-too-large", "one telemetry point exceeds the IPC response limit")
		}
		query.Limit /= 2
	}
}

func (s *Service) appendTelemetrySample(run *model.Run, sample model.TelemetrySample) (bool, error) {
	if run == nil || sample.ObservedAt.IsZero() || sample.RunID != "" && sample.RunID != run.ID {
		return false, store.ErrInvalidTelemetry
	}
	sample.RunID = run.ID
	sample.Version = model.TelemetrySchemaVersion
	sample.Sequence = 0
	sample.ObservedAt = sample.ObservedAt.UTC()
	if run.LastResourceSampleAt != nil && !sample.ObservedAt.After(*run.LastResourceSampleAt) {
		return false, nil
	}
	if _, err := s.store.AppendTelemetry(run.ID, sample); err != nil {
		_ = s.recordTelemetryGap(run, sample.ObservedAt, "telemetry-storage-failure", true)
		run.ResourceGap = true
		return false, err
	}
	s.notifyTelemetryChange(run.ID)
	at := sample.ObservedAt
	run.LastResourceSampleAt = &at
	return true, nil
}

func (s *Service) recordTelemetryGap(run *model.Run, to time.Time, reason string, droppedPoint bool) error {
	if run == nil || to.IsZero() {
		return store.ErrInvalidTelemetry
	}
	from := run.CreatedAt
	if run.LastResourceSampleAt != nil {
		from = *run.LastResourceSampleAt
	}
	if !to.After(from) {
		return nil
	}
	gap := model.TelemetryGap{From: from, To: to, Reason: reason, Resolution: model.TelemetryRaw}
	if droppedPoint {
		gap.DroppedPoints = 1
	}
	run.ResourceGap = true
	if err := s.store.AppendTelemetryGap(run.ID, gap); err != nil {
		return err
	}
	s.notifyTelemetryChange(run.ID)
	return nil
}

func telemetryGapNeeded(last *time.Time, createdAt, observedAt time.Time, intervalMs int64) (time.Time, bool) {
	from := createdAt
	if last != nil {
		from = *last
	}
	interval := time.Duration(intervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	return from, observedAt.After(from.Add(interval + interval/10))
}

func applyTelemetrySummary(run *model.Run, sample model.TelemetrySample, capabilities model.Capabilities, intervalMs int64) {
	resources := normalizeResources(sample.Resources, capabilities, intervalMs)
	resources.SampleIntervalMs = intervalMs
	if resources.PeakMemoryBytes.Status != "measured" && resources.MemoryBytes.Status == "measured" {
		resources.PeakMemoryBytes = resources.MemoryBytes
	}
	if run.Resources.PeakMemoryBytes.Status == "measured" && resources.PeakMemoryBytes.Value < run.Resources.PeakMemoryBytes.Value {
		resources.PeakMemoryBytes = run.Resources.PeakMemoryBytes
	}
	if resources.PeakProcessCount.Status != "measured" && resources.ProcessCount.Status == "measured" {
		resources.PeakProcessCount = resources.ProcessCount
	}
	if run.Resources.PeakProcessCount.Status == "measured" && resources.PeakProcessCount.Value < run.Resources.PeakProcessCount.Value {
		resources.PeakProcessCount = run.Resources.PeakProcessCount
	}
	if resources.PeakTaskCount.Status != "measured" && resources.TaskCount.Status == "measured" {
		resources.PeakTaskCount = resources.TaskCount
	}
	if run.Resources.PeakTaskCount.Status == "measured" && resources.PeakTaskCount.Value < run.Resources.PeakTaskCount.Value {
		resources.PeakTaskCount = run.Resources.PeakTaskCount
	}
	if sample.ObservedAt.IsZero() {
		return
	}
	at := sample.ObservedAt.UTC()
	if resources.CPUTimeNs.Status == "measured" && run.Resources.CPUTimeNs.Status == "measured" && resources.CPUTimeNs.Value > run.Resources.CPUTimeNs.Value {
		run.LastCPUActivityAt = &at
	}
	if len(sample.ProcessChanges) > 0 || resources.ProcessCount.Status == "measured" && run.Resources.ProcessCount.Status == "measured" && resources.ProcessCount.Value != run.Resources.ProcessCount.Value {
		run.LastProcessChangeAt = &at
	}
	run.Resources = resources
	copy := at
	run.LastResourceSampleAt = &copy
}
