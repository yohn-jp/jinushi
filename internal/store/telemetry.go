package store

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

var (
	ErrInvalidTelemetry     = errors.New("invalid telemetry")
	ErrTelemetryTooLarge    = errors.New("telemetry record exceeds configured size limit")
	ErrTelemetryCursorStale = errors.New("telemetry cursor is stale after compaction")
)

const (
	telemetryBucketName = "telemetry"
	telemetryPolicyKey  = "\x00policy"
	telemetryMetaKey    = "\x00meta"
	telemetryRawName    = "raw"
	telemetryAggName    = "aggregates"
	telemetryGapName    = "gaps"

	defaultTelemetryRawSamples      = 512
	defaultTelemetryRawBytes        = 4 << 20
	defaultTelemetryAggregatePoints = 2048
	defaultTelemetryAggregateBytes  = 8 << 20
	defaultTelemetryMaxSampleBytes  = 512 << 10
	defaultTelemetryMaxProcesses    = 1024
	defaultTelemetryMaxChanges      = 256
	defaultTelemetryMaxDevices      = 64
	defaultTelemetryMaxGaps         = 256
	defaultTelemetryMaxQueryPoints  = 1000
	defaultTelemetryMaxProcessPeaks = 16
	defaultTelemetryMaxAggChanges   = 64
	telemetryInitialWindow          = 10 * time.Second
)

// TelemetryOptions bounds high-rate evidence independently from the lifecycle
// journal. Zero values select package defaults. ConfigureTelemetry persists
// the policy in the telemetry bucket so it survives store reopen.
type TelemetryOptions struct {
	RawSamples                    int
	RawBytes                      int64
	AggregatePoints               int
	AggregateBytes                int64
	MaxSampleBytes                int
	MaxProcessesPerSample         int
	MaxProcessChangesPerSample    int
	MaxDevicesPerSample           int
	MaxGaps                       int
	MaxQueryPoints                int
	MaxProcessPeaks               int
	MaxProcessChangesPerAggregate int
}

type TelemetryUsage struct {
	Runs            int        `json:"runs"`
	RawSamples      int        `json:"rawSamples"`
	AggregatePoints int        `json:"aggregatePoints"`
	Gaps            int        `json:"gaps"`
	Bytes           int64      `json:"bytes"`
	RawBytes        int64      `json:"rawBytes"`
	AggregateBytes  int64      `json:"aggregateBytes"`
	GapBytes        int64      `json:"gapBytes"`
	RawRetainedFrom *time.Time `json:"rawRetainedFrom,omitempty"`
	RetainedFrom    *time.Time `json:"retainedFrom,omitempty"`
	HistoryComplete bool       `json:"historyComplete"`
}

type telemetryMeta struct {
	LastSequence         uint64     `json:"lastSequence"`
	CompactionGeneration uint64     `json:"compactionGeneration"`
	LastObservedAt       *time.Time `json:"lastObservedAt,omitempty"`
	NextGapID            uint64     `json:"nextGapId"`
	RawCount             int        `json:"rawCount"`
	RawBytes             int64      `json:"rawBytes"`
	AggregateCount       int        `json:"aggregateCount"`
	AggregateBytes       int64      `json:"aggregateBytes"`
	GapCount             int        `json:"gapCount"`
	GapBytes             int64      `json:"gapBytes"`
	RawRetainedFrom      *time.Time `json:"rawRetainedFrom,omitempty"`
	RetainedFrom         *time.Time `json:"retainedFrom,omitempty"`
	HistoryComplete      bool       `json:"historyComplete"`
}

type telemetryCursor struct {
	LastSequence uint64 `json:"lastSequence"`
	Generation   uint64 `json:"generation"`
}

type telemetryQueryEntry struct {
	sequence  uint64
	sample    *model.TelemetrySample
	aggregate *model.TelemetryAggregate
}

// ConfigureTelemetry persists per-store telemetry retention bounds. Existing
// data is compacted to a newly smaller policy the next time it is appended.
func (s *Store) ConfigureTelemetry(options TelemetryOptions) error {
	options = telemetryDefaults(options)
	if err := validateTelemetryOptions(options); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		root, err := telemetryRootTx(tx, true)
		if err != nil {
			return err
		}
		data, err := json.Marshal(options)
		if err != nil {
			return err
		}
		return root.Put([]byte(telemetryPolicyKey), data)
	})
}

func telemetryDefaults(options TelemetryOptions) TelemetryOptions {
	if options.RawSamples <= 0 {
		options.RawSamples = defaultTelemetryRawSamples
	}
	if options.RawBytes <= 0 {
		options.RawBytes = defaultTelemetryRawBytes
	}
	if options.AggregatePoints <= 0 {
		options.AggregatePoints = defaultTelemetryAggregatePoints
	}
	if options.AggregateBytes <= 0 {
		options.AggregateBytes = defaultTelemetryAggregateBytes
	}
	if options.MaxSampleBytes <= 0 {
		options.MaxSampleBytes = defaultTelemetryMaxSampleBytes
	}
	if options.MaxProcessesPerSample <= 0 {
		options.MaxProcessesPerSample = defaultTelemetryMaxProcesses
	}
	if options.MaxProcessChangesPerSample <= 0 {
		options.MaxProcessChangesPerSample = defaultTelemetryMaxChanges
	}
	if options.MaxDevicesPerSample <= 0 {
		options.MaxDevicesPerSample = defaultTelemetryMaxDevices
	}
	if options.MaxGaps <= 0 {
		options.MaxGaps = defaultTelemetryMaxGaps
	}
	if options.MaxQueryPoints <= 0 {
		options.MaxQueryPoints = defaultTelemetryMaxQueryPoints
	}
	if options.MaxProcessPeaks <= 0 {
		options.MaxProcessPeaks = defaultTelemetryMaxProcessPeaks
	}
	if options.MaxProcessChangesPerAggregate <= 0 {
		options.MaxProcessChangesPerAggregate = defaultTelemetryMaxAggChanges
	}
	return options
}

func validateTelemetryOptions(options TelemetryOptions) error {
	if options.RawSamples < 1 || options.RawBytes < 1 || options.AggregatePoints < 2 ||
		options.AggregateBytes < int64(options.MaxSampleBytes) || options.MaxSampleBytes < 1024 ||
		options.MaxProcessesPerSample < 1 || options.MaxProcessChangesPerSample < 1 ||
		options.MaxDevicesPerSample < 1 || options.MaxGaps < 1 || options.MaxQueryPoints < 1 ||
		options.MaxProcessPeaks < 1 || options.MaxProcessChangesPerAggregate < 1 {
		return fmt.Errorf("%w: retention limits must be positive and aggregate bytes must cover one sample", ErrInvalidTelemetry)
	}
	return nil
}

func readTelemetryOptionsTx(tx *bolt.Tx) (TelemetryOptions, error) {
	root := tx.Bucket([]byte(telemetryBucketName))
	if root == nil || root.Get([]byte(telemetryPolicyKey)) == nil {
		return telemetryDefaults(TelemetryOptions{}), nil
	}
	var options TelemetryOptions
	if err := json.Unmarshal(root.Get([]byte(telemetryPolicyKey)), &options); err != nil {
		return TelemetryOptions{}, fmt.Errorf("decode telemetry policy: %w", err)
	}
	options = telemetryDefaults(options)
	if err := validateTelemetryOptions(options); err != nil {
		return TelemetryOptions{}, err
	}
	return options, nil
}

func telemetryRootTx(tx *bolt.Tx, create bool) (*bolt.Bucket, error) {
	if create {
		root, err := tx.CreateBucketIfNotExists([]byte(telemetryBucketName))
		if err != nil {
			return nil, fmt.Errorf("create telemetry bucket: %w", err)
		}
		return root, nil
	}
	return tx.Bucket([]byte(telemetryBucketName)), nil
}

func telemetryRunBucketTx(root *bolt.Bucket, runID string, create bool) (*bolt.Bucket, error) {
	if create {
		return root.CreateBucketIfNotExists([]byte(runID))
	}
	return root.Bucket([]byte(runID)), nil
}

func telemetryMetaFor(run *bolt.Bucket) (telemetryMeta, error) {
	meta := telemetryMeta{HistoryComplete: true}
	if raw := run.Get([]byte(telemetryMetaKey)); raw != nil {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return telemetryMeta{}, fmt.Errorf("decode telemetry metadata: %w", err)
		}
	}
	if meta.RawCount < 0 || meta.RawBytes < 0 || meta.AggregateCount < 0 || meta.AggregateBytes < 0 || meta.GapCount < 0 || meta.GapBytes < 0 {
		return telemetryMeta{}, fmt.Errorf("%w: corrupt telemetry metadata", ErrInvalidTelemetry)
	}
	return meta, nil
}

func persistTelemetryMeta(run *bolt.Bucket, meta telemetryMeta) error {
	encoded, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode telemetry metadata: %w", err)
	}
	return run.Put([]byte(telemetryMetaKey), encoded)
}

// AppendTelemetry persists a separately sequenced physical evidence sample.
// The store assigns schema version and sequence in the same durable
// transaction as raw retention and aggregate compaction.
func (s *Store) AppendTelemetry(runID string, sample model.TelemetrySample) (model.TelemetrySample, error) {
	if runID == "" || sample.RunID != "" && sample.RunID != runID || sample.Version != 0 && sample.Version != model.TelemetrySchemaVersion || sample.Sequence != 0 {
		return model.TelemetrySample{}, ErrInvalidTelemetry
	}
	var appended model.TelemetrySample
	err := s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket([]byte(runsBucketName))
		if runs == nil || runs.Get([]byte(runID)) == nil {
			return ErrRunNotFound
		}
		root, err := telemetryRootTx(tx, true)
		if err != nil {
			return err
		}
		options, err := readTelemetryOptionsTx(tx)
		if err != nil {
			return err
		}
		run, err := telemetryRunBucketTx(root, runID, true)
		if err != nil {
			return fmt.Errorf("create Run telemetry bucket: %w", err)
		}
		raw, err := run.CreateBucketIfNotExists([]byte(telemetryRawName))
		if err != nil {
			return err
		}
		aggregates, err := run.CreateBucketIfNotExists([]byte(telemetryAggName))
		if err != nil {
			return err
		}
		gaps, err := run.CreateBucketIfNotExists([]byte(telemetryGapName))
		if err != nil {
			return err
		}
		meta, err := telemetryMetaFor(run)
		if err != nil {
			return err
		}
		if meta.LastSequence == math.MaxUint64 {
			return fmt.Errorf("%w: telemetry sequence is exhausted", ErrInvalidTelemetry)
		}
		sample.RunID = runID
		sample.Version = model.TelemetrySchemaVersion
		sample.Sequence = meta.LastSequence + 1
		sample.ObservedAt = sample.ObservedAt.UTC()
		for i := range sample.ProcessChanges {
			sample.ProcessChanges[i].ObservedAt = sample.ProcessChanges[i].ObservedAt.UTC()
		}
		if err := validateTelemetrySample(sample, options); err != nil {
			return err
		}
		if meta.LastObservedAt != nil && sample.ObservedAt.Before(*meta.LastObservedAt) {
			return fmt.Errorf("%w: observation time moved backwards", ErrInvalidTelemetry)
		}
		encoded, err := json.Marshal(sample)
		if err != nil {
			return fmt.Errorf("encode telemetry sample: %w", err)
		}
		if len(encoded) > options.MaxSampleBytes {
			return ErrTelemetryTooLarge
		}
		if err := raw.Put(sequenceKey(sample.Sequence), encoded); err != nil {
			return fmt.Errorf("persist raw telemetry sample: %w", err)
		}
		meta.LastSequence = sample.Sequence
		observedAt := sample.ObservedAt.UTC()
		meta.LastObservedAt = &observedAt
		meta.RawCount++
		meta.RawBytes += int64(len(encoded) + 8)
		for meta.RawCount > options.RawSamples || meta.RawBytes > options.RawBytes {
			key, data := raw.Cursor().First()
			if key == nil || data == nil {
				return fmt.Errorf("%w: raw telemetry metadata exceeds bucket contents", ErrInvalidTelemetry)
			}
			var oldest model.TelemetrySample
			if err := json.Unmarshal(data, &oldest); err != nil {
				return fmt.Errorf("decode raw telemetry sample during compaction: %w", err)
			}
			if err := raw.Delete(key); err != nil {
				return err
			}
			if meta.CompactionGeneration == math.MaxUint64 {
				return fmt.Errorf("%w: telemetry compaction generation is exhausted", ErrInvalidTelemetry)
			}
			meta.CompactionGeneration++
			meta.RawCount--
			meta.RawBytes -= int64(len(data) + len(key))
			if err := aggregateTelemetrySample(aggregates, &meta, oldest, options); err != nil {
				return err
			}
		}
		if err := compactTelemetryAggregates(aggregates, &meta, options); err != nil {
			return err
		}
		if err := compactTelemetryGaps(gaps, &meta, options); err != nil {
			return err
		}
		if err := refreshTelemetryWatermarks(raw, aggregates, gaps, &meta); err != nil {
			return err
		}
		if err := persistTelemetryMeta(run, meta); err != nil {
			return err
		}
		appended = sample
		return nil
	})
	if err != nil {
		return model.TelemetrySample{}, err
	}
	return appended, nil
}

// AppendTelemetryGap records a known interval where sampling or process
// evidence was lost. Gaps are bounded separately and compact to one explicit
// lower-detail range when their count ceiling is reached.
func (s *Store) AppendTelemetryGap(runID string, gap model.TelemetryGap) error {
	if runID == "" {
		return ErrInvalidTelemetry
	}
	gap, err := normalizeTelemetryGap(gap)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		runs := tx.Bucket([]byte(runsBucketName))
		if runs == nil || runs.Get([]byte(runID)) == nil {
			return ErrRunNotFound
		}
		root, err := telemetryRootTx(tx, true)
		if err != nil {
			return err
		}
		options, err := readTelemetryOptionsTx(tx)
		if err != nil {
			return err
		}
		run, err := telemetryRunBucketTx(root, runID, true)
		if err != nil {
			return err
		}
		bucket, err := run.CreateBucketIfNotExists([]byte(telemetryGapName))
		if err != nil {
			return err
		}
		meta, err := telemetryMetaFor(run)
		if err != nil {
			return err
		}
		if meta.NextGapID == math.MaxUint64 {
			return fmt.Errorf("%w: telemetry gap identity is exhausted", ErrInvalidTelemetry)
		}
		meta.NextGapID++
		encoded, err := json.Marshal(gap)
		if err != nil {
			return err
		}
		key := sequenceKey(meta.NextGapID)
		if err := bucket.Put(key, encoded); err != nil {
			return err
		}
		meta.GapCount++
		meta.GapBytes += int64(len(key) + len(encoded))
		if err := compactTelemetryGaps(bucket, &meta, options); err != nil {
			return err
		}
		if err := refreshTelemetryWatermarks(run.Bucket([]byte(telemetryRawName)), run.Bucket([]byte(telemetryAggName)), bucket, &meta); err != nil {
			return err
		}
		return persistTelemetryMeta(run, meta)
	})
}

func normalizeTelemetryGap(gap model.TelemetryGap) (model.TelemetryGap, error) {
	if gap.From.IsZero() || gap.To.IsZero() || gap.To.Before(gap.From) {
		return model.TelemetryGap{}, ErrInvalidTelemetry
	}
	gap.From = gap.From.UTC()
	gap.To = gap.To.UTC()
	gap.Reason = strings.TrimSpace(gap.Reason)
	if gap.Reason == "" || len(gap.Reason) > 128 || hasControlCharacters(gap.Reason) {
		return model.TelemetryGap{}, ErrInvalidTelemetry
	}
	if gap.Resolution != "" && !validTelemetryResolution(gap.Resolution) {
		return model.TelemetryGap{}, ErrInvalidTelemetry
	}
	if len(gap.Metrics) > 32 {
		return model.TelemetryGap{}, ErrInvalidTelemetry
	}
	for _, metric := range gap.Metrics {
		if metric == "" || len(metric) > 64 || hasControlCharacters(metric) {
			return model.TelemetryGap{}, ErrInvalidTelemetry
		}
	}
	return gap, nil
}

func validateTelemetrySample(sample model.TelemetrySample, options TelemetryOptions) error {
	if sample.Version != model.TelemetrySchemaVersion || sample.RunID == "" || sample.Sequence == 0 || sample.ObservedAt.IsZero() {
		return ErrInvalidTelemetry
	}
	if len(sample.Processes) > options.MaxProcessesPerSample || len(sample.ProcessChanges) > options.MaxProcessChangesPerSample || len(sample.IO.Devices) > options.MaxDevicesPerSample {
		return ErrInvalidTelemetry
	}
	for _, metric := range resourceMetrics(sample.Resources) {
		if !validMetric(metric) {
			return ErrInvalidTelemetry
		}
	}
	for _, metric := range ioMetrics(sample.IO) {
		if !validMetric(metric) {
			return ErrInvalidTelemetry
		}
	}
	if sample.IO.DeviceEvidenceStatus != model.EvidenceMeasured && sample.IO.DeviceEvidenceStatus != model.EvidenceUnavailable && sample.IO.DeviceEvidenceStatus != model.EvidenceUnsupported {
		return ErrInvalidTelemetry
	}
	if sample.IO.DeviceEvidenceComplete && sample.IO.DeviceEvidenceStatus != model.EvidenceMeasured {
		return ErrInvalidTelemetry
	}
	for _, device := range sample.IO.Devices {
		if !validDeviceID(device.Device) || !validMetric(device.ReadBytes) || !validMetric(device.WriteBytes) || !validMetric(device.ReadOperations) || !validMetric(device.WriteOperations) {
			return ErrInvalidTelemetry
		}
	}
	for _, metric := range psiMetrics(sample.PSI) {
		if !validMetric(metric) {
			return ErrInvalidTelemetry
		}
	}
	for _, metric := range []model.Metric{sample.Activity.InputBytes, sample.Activity.InputWrites, sample.Activity.ResizeCount} {
		if !validMetric(metric) {
			return ErrInvalidTelemetry
		}
	}
	if sample.ProcessEvidenceStatus != model.EvidenceMeasured && sample.ProcessEvidenceStatus != model.EvidenceUnavailable && sample.ProcessEvidenceStatus != model.EvidenceUnsupported {
		return ErrInvalidTelemetry
	}
	if sample.ProcessEvidenceComplete && sample.ProcessEvidenceStatus != model.EvidenceMeasured {
		return ErrInvalidTelemetry
	}
	seen := make(map[model.ProcessIdentity]struct{}, len(sample.Processes))
	for _, process := range sample.Processes {
		if process.PID <= 0 || process.StartTimeTicks == 0 || process.ParentPID < 0 || !validMembership(process.Membership) || len(process.Comm) > 16 || hasControlCharacters(process.Comm) || len(process.State) > 8 || hasControlCharacters(process.State) || !validMetric(process.CPUTimeNs) || !validMetric(process.RSSBytes) {
			return ErrInvalidTelemetry
		}
		if process.ParentObserved && (process.ParentPID <= 0 || process.ParentStartTimeTicks == 0) {
			return ErrInvalidTelemetry
		}
		if !process.ParentObserved && process.ParentStartTimeTicks != 0 {
			return ErrInvalidTelemetry
		}
		identity := model.ProcessIdentity{PID: process.PID, StartTimeTicks: process.StartTimeTicks}
		if _, exists := seen[identity]; exists {
			return ErrInvalidTelemetry
		}
		seen[identity] = struct{}{}
	}
	for _, change := range sample.ProcessChanges {
		if change.ObservedAt.IsZero() || change.ObservedAt.After(sample.ObservedAt) || change.Process.PID <= 0 || change.Process.StartTimeTicks == 0 {
			return ErrInvalidTelemetry
		}
		switch change.Kind {
		case model.ProcessStarted:
			if !validMembership(change.Membership) {
				return ErrInvalidTelemetry
			}
		case model.ProcessExited:
			if change.Membership != "" || change.PreviousMembership != "" {
				return ErrInvalidTelemetry
			}
		case model.ProcessMembershipChanged:
			if !validMembership(change.Membership) || !validMembership(change.PreviousMembership) {
				return ErrInvalidTelemetry
			}
		default:
			return ErrInvalidTelemetry
		}
	}
	return nil
}

func validMetric(metric model.Metric) bool {
	switch metric.Status {
	case string(model.EvidenceMeasured):
		return metric.Value >= 0
	case string(model.EvidenceUnavailable), string(model.EvidenceUnsupported):
		return metric.Value == 0
	default:
		return false
	}
}

func validMembership(membership model.ProcessMembership) bool {
	switch membership {
	case model.ProcessMembershipOwned, model.ProcessMembershipOutside, model.ProcessMembershipUnknown, model.ProcessMembershipUnsupported:
		return true
	default:
		return false
	}
}

func validDeviceID(device string) bool {
	if len(device) > 16 || hasControlCharacters(device) || strings.Count(device, ":") != 1 {
		return false
	}
	major, minor, ok := strings.Cut(device, ":")
	if !ok || major == "" || minor == "" {
		return false
	}
	for _, value := range []string{major, minor} {
		for _, r := range value {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

func hasControlCharacters(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func resourceMetrics(resources model.Resources) []model.Metric {
	return []model.Metric{resources.MemoryBytes, resources.PeakMemoryBytes, resources.CPUTimeNs, resources.ProcessCount, resources.PeakProcessCount, resources.TaskCount, resources.PeakTaskCount}
}

func ioMetrics(io model.IOMetrics) []model.Metric {
	return []model.Metric{io.ReadBytes, io.WriteBytes, io.ReadOperations, io.WriteOperations}
}

func psiMetrics(psi model.PSIMetrics) []model.Metric {
	return []model.Metric{
		psi.CPU.SomeAvg10BasisPoints, psi.CPU.SomeAvg60BasisPoints, psi.CPU.SomeAvg300BasisPoints, psi.CPU.SomeTotalUS,
		psi.CPU.FullAvg10BasisPoints, psi.CPU.FullAvg60BasisPoints, psi.CPU.FullAvg300BasisPoints, psi.CPU.FullTotalUS,
		psi.Memory.SomeAvg10BasisPoints, psi.Memory.SomeAvg60BasisPoints, psi.Memory.SomeAvg300BasisPoints, psi.Memory.SomeTotalUS,
		psi.Memory.FullAvg10BasisPoints, psi.Memory.FullAvg60BasisPoints, psi.Memory.FullAvg300BasisPoints, psi.Memory.FullTotalUS,
		psi.IO.SomeAvg10BasisPoints, psi.IO.SomeAvg60BasisPoints, psi.IO.SomeAvg300BasisPoints, psi.IO.SomeTotalUS,
		psi.IO.FullAvg10BasisPoints, psi.IO.FullAvg60BasisPoints, psi.IO.FullAvg300BasisPoints, psi.IO.FullTotalUS,
	}
}

func validTelemetryResolution(resolution model.TelemetryResolution) bool {
	switch resolution {
	case model.TelemetryRaw, model.Telemetry10Seconds, model.TelemetryAdaptive:
		return true
	default:
		return false
	}
}

func sequenceKey(sequence uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sequence)
	return key
}

func timeBucketKey(value time.Time) []byte {
	nanos := value.UTC().UnixNano()
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(nanos)^(uint64(1)<<63))
	return key
}

func parseTimeBucketKey(key []byte) (time.Time, error) {
	if len(key) != 8 {
		return time.Time{}, ErrInvalidTelemetry
	}
	nanos := int64(binary.BigEndian.Uint64(key) ^ (uint64(1) << 63))
	return time.Unix(0, nanos).UTC(), nil
}

func telemetryWindowStart(value time.Time) time.Time {
	value = value.UTC()
	nanos := value.UnixNano()
	window := telemetryInitialWindow.Nanoseconds()
	return time.Unix(0, nanos-nanos%window).UTC()
}

func encodeTelemetryCursor(sequence, generation uint64) string {
	var raw [16]byte
	binary.BigEndian.PutUint64(raw[:8], sequence)
	binary.BigEndian.PutUint64(raw[8:], generation)
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

func decodeTelemetryCursor(cursor string) (telemetryCursor, error) {
	if cursor == "" {
		return telemetryCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(raw) != 16 {
		return telemetryCursor{}, ErrInvalidTelemetry
	}
	return telemetryCursor{LastSequence: binary.BigEndian.Uint64(raw[:8]), Generation: binary.BigEndian.Uint64(raw[8:])}, nil
}

func aggregateTelemetrySample(bucket *bolt.Bucket, meta *telemetryMeta, sample model.TelemetrySample, options TelemetryOptions) error {
	start := telemetryWindowStart(sample.ObservedAt)
	key := timeBucketKey(start)
	aggregate := model.TelemetryAggregate{
		Version:          model.TelemetrySchemaVersion,
		RunID:            sample.RunID,
		Resolution:       model.Telemetry10Seconds,
		WindowStart:      start,
		WindowEnd:        start.Add(telemetryInitialWindow),
		FirstSequence:    sample.Sequence,
		LastSequence:     sample.Sequence,
		SampleCount:      0,
		EvidenceComplete: true,
	}
	var oldBytes int64
	if data := bucket.Get(key); data != nil {
		if err := json.Unmarshal(data, &aggregate); err != nil {
			return fmt.Errorf("decode telemetry aggregate: %w", err)
		}
		oldBytes = int64(len(key) + len(data))
	}
	aggregateSample(&aggregate, sample, options)
	encoded, err := json.Marshal(aggregate)
	if err != nil {
		return err
	}
	if err := bucket.Put(key, encoded); err != nil {
		return err
	}
	meta.AggregateBytes += int64(len(key)+len(encoded)) - oldBytes
	if oldBytes == 0 {
		meta.AggregateCount++
	}
	return nil
}

func aggregateSample(aggregate *model.TelemetryAggregate, sample model.TelemetrySample, options TelemetryOptions) {
	if aggregate.SampleCount == 0 {
		aggregate.FirstSequence = sample.Sequence
		aggregate.WindowStart = telemetryWindowStart(sample.ObservedAt)
		aggregate.WindowEnd = aggregate.WindowStart.Add(telemetryInitialWindow)
		aggregate.RunID = sample.RunID
		aggregate.Version = model.TelemetrySchemaVersion
		aggregate.Resolution = model.Telemetry10Seconds
		aggregate.EvidenceComplete = true
	}
	if sample.ObservedAt.After(aggregate.WindowEnd) {
		aggregate.WindowEnd = sample.ObservedAt
	}
	if sample.Sequence < aggregate.FirstSequence {
		aggregate.FirstSequence = sample.Sequence
	}
	if sample.Sequence > aggregate.LastSequence {
		aggregate.LastSequence = sample.Sequence
	}
	addResources(&aggregate.Resources, sample.Resources)
	addIOMetrics(&aggregate.IO, sample.IO)
	addPSIMetrics(&aggregate.PSI, sample.PSI)
	addIOActivity(&aggregate.Activity, sample.Activity)
	aggregate.SampleCount++
	aggregate.ChangeCount += uint64(len(sample.ProcessChanges))
	for _, change := range sample.ProcessChanges {
		if len(aggregate.ProcessChanges) < options.MaxProcessChangesPerAggregate {
			aggregate.ProcessChanges = append(aggregate.ProcessChanges, change)
		}
	}
	aggregate.ProcessChanges = firstProcessChanges(aggregate.ProcessChanges, options.MaxProcessChangesPerAggregate)
	mergeProcessPeaks(aggregate, sample, options.MaxProcessPeaks)
	if !sample.ProcessEvidenceComplete || sample.ProcessEvidenceStatus != model.EvidenceMeasured || len(sample.Processes) > 0 || len(sample.ProcessChanges) > 0 || !sample.IO.DeviceEvidenceComplete || len(sample.IO.Devices) > 0 {
		aggregate.EvidenceComplete = false
	}
	if uint64(len(aggregate.ProcessChanges)) < aggregate.ChangeCount {
		aggregate.EvidenceComplete = false
	}
}

func mergeProcessPeaks(aggregate *model.TelemetryAggregate, sample model.TelemetrySample, max int) {
	byIdentity := make(map[model.ProcessIdentity]model.ProcessPeak, len(aggregate.ProcessPeaks)+len(sample.Processes))
	for _, peak := range aggregate.ProcessPeaks {
		id := model.ProcessIdentity{PID: peak.Process.PID, StartTimeTicks: peak.Process.StartTimeTicks}
		byIdentity[id] = peak
	}
	for _, process := range sample.Processes {
		id := model.ProcessIdentity{PID: process.PID, StartTimeTicks: process.StartTimeTicks}
		candidate := model.ProcessPeak{ObservedAt: sample.ObservedAt, Process: process}
		previous, exists := byIdentity[id]
		if !exists || processRSS(process) > processRSS(previous.Process) {
			byIdentity[id] = candidate
		}
	}
	peaks := make([]model.ProcessPeak, 0, len(byIdentity))
	for _, peak := range byIdentity {
		peaks = append(peaks, peak)
	}
	sort.Slice(peaks, func(i, j int) bool {
		left, right := processRSS(peaks[i].Process), processRSS(peaks[j].Process)
		if left != right {
			return left > right
		}
		if peaks[i].Process.PID != peaks[j].Process.PID {
			return peaks[i].Process.PID < peaks[j].Process.PID
		}
		return peaks[i].Process.StartTimeTicks < peaks[j].Process.StartTimeTicks
	})
	if len(peaks) > max {
		peaks = peaks[:max]
		aggregate.EvidenceComplete = false
	}
	aggregate.ProcessPeaks = peaks
}

func processRSS(process model.ProcessEvidence) int64 {
	if process.RSSBytes.Status != string(model.EvidenceMeasured) {
		return -1
	}
	return process.RSSBytes.Value
}

func firstProcessChanges(changes []model.ProcessEvidenceChange, limit int) []model.ProcessEvidenceChange {
	sort.Slice(changes, func(i, j int) bool {
		if !changes[i].ObservedAt.Equal(changes[j].ObservedAt) {
			return changes[i].ObservedAt.Before(changes[j].ObservedAt)
		}
		if changes[i].Process.PID != changes[j].Process.PID {
			return changes[i].Process.PID < changes[j].Process.PID
		}
		return changes[i].Process.StartTimeTicks < changes[j].Process.StartTimeTicks
	})
	if len(changes) > limit {
		changes = changes[:limit]
	}
	return changes
}

func addMetricAggregate(aggregate *model.MetricAggregate, value model.Metric, counter bool) {
	if aggregateObservationCount(*aggregate) == 0 {
		aggregate.First = value
	}
	aggregate.Last = value
	switch value.Status {
	case string(model.EvidenceMeasured):
		if aggregate.Count == 0 {
			aggregate.Min, aggregate.Max = value, value
		} else {
			if value.Value < aggregate.Min.Value {
				aggregate.Min = value
			}
			if value.Value > aggregate.Max.Value {
				aggregate.Max = value
			}
		}
		aggregate.Count++
	case string(model.EvidenceUnavailable):
		aggregate.UnavailableCount++
	case string(model.EvidenceUnsupported):
		aggregate.UnsupportedCount++
	}
	if aggregate.Count == 0 {
		status := string(model.EvidenceUnsupported)
		if aggregate.UnavailableCount > 0 {
			status = string(model.EvidenceUnavailable)
		}
		aggregate.Min = model.Metric{Status: status}
		aggregate.Max = model.Metric{Status: status}
	}
	aggregate.Delta = metricDelta(aggregate.First, aggregate.Last, counter)
}

func mergeMetricAggregate(left, right model.MetricAggregate, counter bool) model.MetricAggregate {
	if aggregateObservationCount(left) == 0 {
		return right
	}
	if aggregateObservationCount(right) == 0 {
		return left
	}
	merged := model.MetricAggregate{
		First:            left.First,
		Last:             right.Last,
		Count:            left.Count + right.Count,
		UnavailableCount: left.UnavailableCount + right.UnavailableCount,
		UnsupportedCount: left.UnsupportedCount + right.UnsupportedCount,
	}
	if left.Count == 0 {
		if right.Count == 0 {
			status := string(model.EvidenceUnsupported)
			if merged.UnavailableCount > 0 {
				status = string(model.EvidenceUnavailable)
			}
			merged.Min = model.Metric{Status: status}
			merged.Max = model.Metric{Status: status}
		} else {
			merged.Min, merged.Max = right.Min, right.Max
		}
	} else if right.Count == 0 {
		merged.Min, merged.Max = left.Min, left.Max
	} else {
		merged.Min, merged.Max = left.Min, left.Max
		if right.Min.Value < merged.Min.Value {
			merged.Min = right.Min
		}
		if right.Max.Value > merged.Max.Value {
			merged.Max = right.Max
		}
	}
	merged.Delta = metricDelta(merged.First, merged.Last, counter)
	return merged
}

func aggregateObservationCount(metric model.MetricAggregate) uint64 {
	return metric.Count + metric.UnavailableCount + metric.UnsupportedCount
}

func metricDelta(first, last model.Metric, counter bool) model.Metric {
	if !counter {
		return model.Metric{Status: string(model.EvidenceUnsupported)}
	}
	if first.Status != string(model.EvidenceMeasured) || last.Status != string(model.EvidenceMeasured) {
		if first.Status == string(model.EvidenceUnsupported) || last.Status == string(model.EvidenceUnsupported) {
			return model.Metric{Status: string(model.EvidenceUnsupported)}
		}
		return model.Metric{Status: string(model.EvidenceUnavailable)}
	}
	if last.Value < first.Value {
		return model.Metric{Status: string(model.EvidenceUnavailable)}
	}
	return model.Metric{Status: string(model.EvidenceMeasured), Value: last.Value - first.Value}
}

func mergeResources(left, right model.AggregateResources) model.AggregateResources {
	return model.AggregateResources{
		MemoryBytes:      mergeMetricAggregate(left.MemoryBytes, right.MemoryBytes, false),
		PeakMemoryBytes:  mergeMetricAggregate(left.PeakMemoryBytes, right.PeakMemoryBytes, false),
		CPUTimeNs:        mergeMetricAggregate(left.CPUTimeNs, right.CPUTimeNs, true),
		ProcessCount:     mergeMetricAggregate(left.ProcessCount, right.ProcessCount, false),
		PeakProcessCount: mergeMetricAggregate(left.PeakProcessCount, right.PeakProcessCount, false),
		TaskCount:        mergeMetricAggregate(left.TaskCount, right.TaskCount, false),
		PeakTaskCount:    mergeMetricAggregate(left.PeakTaskCount, right.PeakTaskCount, false),
	}
}

func addResources(aggregate *model.AggregateResources, value model.Resources) {
	addMetricAggregate(&aggregate.MemoryBytes, value.MemoryBytes, false)
	addMetricAggregate(&aggregate.PeakMemoryBytes, value.PeakMemoryBytes, false)
	addMetricAggregate(&aggregate.CPUTimeNs, value.CPUTimeNs, true)
	addMetricAggregate(&aggregate.ProcessCount, value.ProcessCount, false)
	addMetricAggregate(&aggregate.PeakProcessCount, value.PeakProcessCount, false)
	addMetricAggregate(&aggregate.TaskCount, value.TaskCount, false)
	addMetricAggregate(&aggregate.PeakTaskCount, value.PeakTaskCount, false)
}

func mergeAggregateIO(left, right model.AggregateIOMetrics) model.AggregateIOMetrics {
	return model.AggregateIOMetrics{
		ReadBytes:       mergeMetricAggregate(left.ReadBytes, right.ReadBytes, true),
		WriteBytes:      mergeMetricAggregate(left.WriteBytes, right.WriteBytes, true),
		ReadOperations:  mergeMetricAggregate(left.ReadOperations, right.ReadOperations, true),
		WriteOperations: mergeMetricAggregate(left.WriteOperations, right.WriteOperations, true),
	}
}

func addIOMetrics(aggregate *model.AggregateIOMetrics, value model.IOMetrics) {
	addMetricAggregate(&aggregate.ReadBytes, value.ReadBytes, true)
	addMetricAggregate(&aggregate.WriteBytes, value.WriteBytes, true)
	addMetricAggregate(&aggregate.ReadOperations, value.ReadOperations, true)
	addMetricAggregate(&aggregate.WriteOperations, value.WriteOperations, true)
}

func mergePressure(left, right model.AggregatePressureMetrics) model.AggregatePressureMetrics {
	return model.AggregatePressureMetrics{
		SomeAvg10BasisPoints:  mergeMetricAggregate(left.SomeAvg10BasisPoints, right.SomeAvg10BasisPoints, false),
		SomeAvg60BasisPoints:  mergeMetricAggregate(left.SomeAvg60BasisPoints, right.SomeAvg60BasisPoints, false),
		SomeAvg300BasisPoints: mergeMetricAggregate(left.SomeAvg300BasisPoints, right.SomeAvg300BasisPoints, false),
		SomeTotalUS:           mergeMetricAggregate(left.SomeTotalUS, right.SomeTotalUS, true),
		FullAvg10BasisPoints:  mergeMetricAggregate(left.FullAvg10BasisPoints, right.FullAvg10BasisPoints, false),
		FullAvg60BasisPoints:  mergeMetricAggregate(left.FullAvg60BasisPoints, right.FullAvg60BasisPoints, false),
		FullAvg300BasisPoints: mergeMetricAggregate(left.FullAvg300BasisPoints, right.FullAvg300BasisPoints, false),
		FullTotalUS:           mergeMetricAggregate(left.FullTotalUS, right.FullTotalUS, true),
	}
}

func addPressure(aggregate *model.AggregatePressureMetrics, value model.PressureMetrics) {
	addMetricAggregate(&aggregate.SomeAvg10BasisPoints, value.SomeAvg10BasisPoints, false)
	addMetricAggregate(&aggregate.SomeAvg60BasisPoints, value.SomeAvg60BasisPoints, false)
	addMetricAggregate(&aggregate.SomeAvg300BasisPoints, value.SomeAvg300BasisPoints, false)
	addMetricAggregate(&aggregate.SomeTotalUS, value.SomeTotalUS, true)
	addMetricAggregate(&aggregate.FullAvg10BasisPoints, value.FullAvg10BasisPoints, false)
	addMetricAggregate(&aggregate.FullAvg60BasisPoints, value.FullAvg60BasisPoints, false)
	addMetricAggregate(&aggregate.FullAvg300BasisPoints, value.FullAvg300BasisPoints, false)
	addMetricAggregate(&aggregate.FullTotalUS, value.FullTotalUS, true)
}

func mergePSI(left, right model.AggregatePSIMetrics) model.AggregatePSIMetrics {
	return model.AggregatePSIMetrics{
		CPU:    mergePressure(left.CPU, right.CPU),
		Memory: mergePressure(left.Memory, right.Memory),
		IO:     mergePressure(left.IO, right.IO),
	}
}

func addPSIMetrics(aggregate *model.AggregatePSIMetrics, value model.PSIMetrics) {
	addPressure(&aggregate.CPU, value.CPU)
	addPressure(&aggregate.Memory, value.Memory)
	addPressure(&aggregate.IO, value.IO)
}

func mergeIOActivity(left, right model.AggregateIOActivity) model.AggregateIOActivity {
	return model.AggregateIOActivity{
		InputBytes:  mergeMetricAggregate(left.InputBytes, right.InputBytes, true),
		InputWrites: mergeMetricAggregate(left.InputWrites, right.InputWrites, true),
		ResizeCount: mergeMetricAggregate(left.ResizeCount, right.ResizeCount, true),
	}
}

func addIOActivity(aggregate *model.AggregateIOActivity, value model.IOActivity) {
	addMetricAggregate(&aggregate.InputBytes, value.InputBytes, true)
	addMetricAggregate(&aggregate.InputWrites, value.InputWrites, true)
	addMetricAggregate(&aggregate.ResizeCount, value.ResizeCount, true)
}

func mergeTelemetryAggregates(left, right model.TelemetryAggregate, options TelemetryOptions) model.TelemetryAggregate {
	if right.FirstSequence < left.FirstSequence {
		left, right = right, left
	}
	merged := model.TelemetryAggregate{
		Version:          model.TelemetrySchemaVersion,
		RunID:            left.RunID,
		Resolution:       model.TelemetryAdaptive,
		WindowStart:      left.WindowStart,
		WindowEnd:        right.WindowEnd,
		FirstSequence:    left.FirstSequence,
		LastSequence:     right.LastSequence,
		SampleCount:      left.SampleCount + right.SampleCount,
		Resources:        mergeResources(left.Resources, right.Resources),
		IO:               mergeAggregateIO(left.IO, right.IO),
		PSI:              mergePSI(left.PSI, right.PSI),
		Activity:         mergeIOActivity(left.Activity, right.Activity),
		ChangeCount:      left.ChangeCount + right.ChangeCount,
		EvidenceComplete: left.EvidenceComplete && right.EvidenceComplete,
	}
	merged.ProcessChanges = append(merged.ProcessChanges, left.ProcessChanges...)
	merged.ProcessChanges = append(merged.ProcessChanges, right.ProcessChanges...)
	merged.ProcessChanges = firstProcessChanges(merged.ProcessChanges, options.MaxProcessChangesPerAggregate)
	merged.ProcessPeaks = mergeProcessPeakSets(left.ProcessPeaks, right.ProcessPeaks, options.MaxProcessPeaks)
	if uint64(len(merged.ProcessChanges)) < merged.ChangeCount || len(merged.ProcessPeaks) > options.MaxProcessPeaks {
		merged.EvidenceComplete = false
	}
	return merged
}

func mergeProcessPeakSets(left, right []model.ProcessPeak, max int) []model.ProcessPeak {
	byIdentity := make(map[model.ProcessIdentity]model.ProcessPeak, len(left)+len(right))
	for _, peak := range append(append([]model.ProcessPeak(nil), left...), right...) {
		identity := model.ProcessIdentity{PID: peak.Process.PID, StartTimeTicks: peak.Process.StartTimeTicks}
		previous, exists := byIdentity[identity]
		if !exists || processRSS(peak.Process) > processRSS(previous.Process) {
			byIdentity[identity] = peak
		}
	}
	peaks := make([]model.ProcessPeak, 0, len(byIdentity))
	for _, peak := range byIdentity {
		peaks = append(peaks, peak)
	}
	sort.Slice(peaks, func(i, j int) bool {
		if processRSS(peaks[i].Process) != processRSS(peaks[j].Process) {
			return processRSS(peaks[i].Process) > processRSS(peaks[j].Process)
		}
		return peaks[i].Process.PID < peaks[j].Process.PID
	})
	if len(peaks) > max {
		peaks = peaks[:max]
	}
	return peaks
}

func compactTelemetryAggregates(bucket *bolt.Bucket, meta *telemetryMeta, options TelemetryOptions) error {
	for meta.AggregateCount > options.AggregatePoints || meta.AggregateBytes > options.AggregateBytes {
		cursor := bucket.Cursor()
		firstKey, firstData := cursor.First()
		secondKey, secondData := cursor.Next()
		if firstKey == nil || secondKey == nil || firstData == nil || secondData == nil {
			return fmt.Errorf("%w: aggregate telemetry metadata exceeds bucket contents", ErrInvalidTelemetry)
		}
		var first, second model.TelemetryAggregate
		if err := json.Unmarshal(firstData, &first); err != nil {
			return err
		}
		if err := json.Unmarshal(secondData, &second); err != nil {
			return err
		}
		merged := mergeTelemetryAggregates(first, second, options)
		encoded, err := json.Marshal(merged)
		if err != nil {
			return err
		}
		if int64(len(firstKey)+len(encoded)) > options.AggregateBytes {
			return ErrTelemetryTooLarge
		}
		oldBytes := int64(len(firstKey) + len(firstData) + len(secondKey) + len(secondData))
		if err := bucket.Delete(firstKey); err != nil {
			return err
		}
		if err := bucket.Delete(secondKey); err != nil {
			return err
		}
		newKey := timeBucketKey(merged.WindowStart)
		if err := bucket.Put(newKey, encoded); err != nil {
			return err
		}
		if meta.CompactionGeneration == math.MaxUint64 {
			return fmt.Errorf("%w: telemetry compaction generation is exhausted", ErrInvalidTelemetry)
		}
		meta.CompactionGeneration++
		meta.AggregateCount--
		meta.AggregateBytes += int64(len(newKey)+len(encoded)) - oldBytes
	}
	return nil
}

func compactTelemetryGaps(bucket *bolt.Bucket, meta *telemetryMeta, options TelemetryOptions) error {
	for meta.GapCount > options.MaxGaps {
		cursor := bucket.Cursor()
		firstKey, firstData := cursor.First()
		secondKey, secondData := cursor.Next()
		if firstKey == nil || secondKey == nil || firstData == nil || secondData == nil {
			return fmt.Errorf("%w: gap metadata exceeds bucket contents", ErrInvalidTelemetry)
		}
		var first, second model.TelemetryGap
		if err := json.Unmarshal(firstData, &first); err != nil {
			return err
		}
		if err := json.Unmarshal(secondData, &second); err != nil {
			return err
		}
		merged := model.TelemetryGap{
			From:          first.From,
			To:            second.To,
			Reason:        "gap-history-compacted",
			Resolution:    model.TelemetryAdaptive,
			DroppedPoints: saturatingAdd(first.DroppedPoints, second.DroppedPoints),
		}
		if merged.To.Before(merged.From) {
			merged.From, merged.To = second.From, first.To
		}
		if math.MaxUint64-merged.DroppedPoints < 2 {
			merged.DroppedPoints = math.MaxUint64
		} else {
			merged.DroppedPoints += 2
		}
		metrics := make(map[string]struct{}, len(first.Metrics)+len(second.Metrics))
		for _, metric := range append(first.Metrics, second.Metrics...) {
			metrics[metric] = struct{}{}
		}
		for metric := range metrics {
			merged.Metrics = append(merged.Metrics, metric)
		}
		sort.Strings(merged.Metrics)
		if len(merged.Metrics) > 32 {
			merged.Metrics = merged.Metrics[:32]
		}
		encoded, err := json.Marshal(merged)
		if err != nil {
			return err
		}
		oldBytes := int64(len(firstKey) + len(firstData) + len(secondKey) + len(secondData))
		if err := bucket.Delete(firstKey); err != nil {
			return err
		}
		if err := bucket.Delete(secondKey); err != nil {
			return err
		}
		firstID := binary.BigEndian.Uint64(firstKey)
		if firstID == math.MaxUint64 {
			return fmt.Errorf("%w: telemetry gap identity is exhausted", ErrInvalidTelemetry)
		}
		newID := firstID + 1
		newKey := sequenceKey(newID)
		if err := bucket.Put(newKey, encoded); err != nil {
			return err
		}
		meta.GapCount--
		meta.GapBytes += int64(len(newKey)+len(encoded)) - oldBytes
		meta.HistoryComplete = false
	}
	return nil
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

func refreshTelemetryWatermarks(raw, aggregates, gaps *bolt.Bucket, meta *telemetryMeta) error {
	meta.RawRetainedFrom = nil
	if raw != nil {
		if _, data := raw.Cursor().First(); data != nil {
			var sample model.TelemetrySample
			if err := json.Unmarshal(data, &sample); err != nil {
				return err
			}
			observed := sample.ObservedAt.UTC()
			meta.RawRetainedFrom = &observed
		}
	}
	meta.RetainedFrom = nil
	if aggregates != nil {
		if _, data := aggregates.Cursor().First(); data != nil {
			var aggregate model.TelemetryAggregate
			if err := json.Unmarshal(data, &aggregate); err != nil {
				return err
			}
			retained := aggregate.WindowStart.UTC()
			meta.RetainedFrom = &retained
		}
	}
	if meta.RawRetainedFrom != nil && (meta.RetainedFrom == nil || meta.RawRetainedFrom.Before(*meta.RetainedFrom)) {
		retained := *meta.RawRetainedFrom
		meta.RetainedFrom = &retained
	}
	if gaps != nil {
		if err := gaps.ForEach(func(_, data []byte) error {
			if data == nil {
				return nil
			}
			var gap model.TelemetryGap
			if err := json.Unmarshal(data, &gap); err != nil {
				return err
			}
			if meta.RetainedFrom == nil || gap.From.Before(*meta.RetainedFrom) {
				retained := gap.From
				meta.RetainedFrom = &retained
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// QueryTelemetry returns a bounded page of raw samples and progressively
// coarser aggregates, together with every retained gap intersecting the
// requested time range. The event journal sequence is independent.
func (s *Store) QueryTelemetry(query model.TelemetryQuery) (model.TelemetryResponse, error) {
	if query.RunID == "" || query.From != nil && query.To != nil && query.To.Before(*query.From) {
		return model.TelemetryResponse{}, ErrInvalidTelemetry
	}
	if query.Resolution == "" {
		query.Resolution = model.TelemetryRaw
	}
	if !validTelemetryResolution(query.Resolution) {
		return model.TelemetryResponse{}, ErrInvalidTelemetry
	}
	cursor, err := decodeTelemetryCursor(query.Cursor)
	if err != nil {
		return model.TelemetryResponse{}, err
	}
	var response model.TelemetryResponse
	err = s.db.View(func(tx *bolt.Tx) error {
		runs := tx.Bucket([]byte(runsBucketName))
		if runs == nil || runs.Get([]byte(query.RunID)) == nil {
			return ErrRunNotFound
		}
		root := tx.Bucket([]byte(telemetryBucketName))
		response = model.TelemetryResponse{
			Version:         model.TelemetrySchemaVersion,
			RunID:           query.RunID,
			Resolution:      query.Resolution,
			Samples:         []model.TelemetrySample{},
			Aggregates:      []model.TelemetryAggregate{},
			Gaps:            []model.TelemetryGap{},
			HistoryComplete: true,
		}
		if root == nil {
			return nil
		}
		options, err := readTelemetryOptionsTx(tx)
		if err != nil {
			return err
		}
		limit := query.Limit
		if limit <= 0 {
			limit = options.MaxQueryPoints
		}
		if limit > options.MaxQueryPoints {
			return fmt.Errorf("%w: query limit exceeds configured maximum", ErrInvalidTelemetry)
		}
		run := root.Bucket([]byte(query.RunID))
		if run == nil {
			return nil
		}
		meta, err := telemetryMetaFor(run)
		if err != nil {
			return err
		}
		if query.Cursor != "" && cursor.Generation != meta.CompactionGeneration {
			return ErrTelemetryCursorStale
		}
		response.RawRetainedFrom = cloneTime(meta.RawRetainedFrom)
		response.RetainedFrom = cloneTime(meta.RetainedFrom)
		response.HistoryComplete = meta.HistoryComplete
		entries := make([]telemetryQueryEntry, 0, meta.RawCount+meta.AggregateCount)
		if raw := run.Bucket([]byte(telemetryRawName)); raw != nil {
			if err := raw.ForEach(func(key, value []byte) error {
				if value == nil || len(key) != 8 {
					return nil
				}
				sequence := binary.BigEndian.Uint64(key)
				if sequence <= cursor.LastSequence {
					return nil
				}
				var sample model.TelemetrySample
				if err := json.Unmarshal(value, &sample); err != nil {
					return fmt.Errorf("decode raw telemetry sample: %w", err)
				}
				if !telemetryTimeMatches(sample.ObservedAt, query.From, query.To) {
					return nil
				}
				entries = append(entries, telemetryQueryEntry{sequence: sequence, sample: &sample})
				return nil
			}); err != nil {
				return err
			}
		}
		if aggregates := run.Bucket([]byte(telemetryAggName)); aggregates != nil {
			if err := aggregates.ForEach(func(_, value []byte) error {
				if value == nil {
					return nil
				}
				var aggregate model.TelemetryAggregate
				if err := json.Unmarshal(value, &aggregate); err != nil {
					return fmt.Errorf("decode telemetry aggregate: %w", err)
				}
				if aggregate.LastSequence <= cursor.LastSequence || !telemetryWindowMatches(aggregate.WindowStart, aggregate.WindowEnd, query.From, query.To) {
					return nil
				}
				entries = append(entries, telemetryQueryEntry{sequence: aggregate.LastSequence, aggregate: &aggregate})
				return nil
			}); err != nil {
				return err
			}
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].sequence < entries[j].sequence })
		if len(entries) > limit {
			response.NextCursor = encodeTelemetryCursor(entries[limit-1].sequence, meta.CompactionGeneration)
			entries = entries[:limit]
		}
		for _, entry := range entries {
			if entry.sample != nil {
				response.Samples = append(response.Samples, *entry.sample)
			}
			if entry.aggregate != nil {
				response.Aggregates = append(response.Aggregates, *entry.aggregate)
			}
		}
		gaps := run.Bucket([]byte(telemetryGapName))
		if gaps != nil {
			if err := gaps.ForEach(func(_, value []byte) error {
				if value == nil {
					return nil
				}
				var gap model.TelemetryGap
				if err := json.Unmarshal(value, &gap); err != nil {
					return err
				}
				if telemetryWindowMatches(gap.From, gap.To, query.From, query.To) {
					response.Gaps = append(response.Gaps, gap)
					response.HistoryComplete = false
				}
				return nil
			}); err != nil {
				return err
			}
		}
		sort.Slice(response.Gaps, func(i, j int) bool { return response.Gaps[i].From.Before(response.Gaps[j].From) })
		return nil
	})
	if err != nil {
		return model.TelemetryResponse{}, err
	}
	return response, nil
}

func telemetryTimeMatches(value time.Time, from, to *time.Time) bool {
	if from != nil && value.Before(*from) {
		return false
	}
	if to != nil && value.After(*to) {
		return false
	}
	return true
}

func telemetryWindowMatches(start, end time.Time, from, to *time.Time) bool {
	if from != nil && end.Before(*from) {
		return false
	}
	if to != nil && start.After(*to) {
		return false
	}
	return true
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// DeleteTelemetry removes one Run's time-series evidence. The transaction
// helper is available to same-package GC paths so Run deletion and telemetry
// deletion can commit atomically.
func (s *Store) DeleteTelemetry(runID string) (TelemetryUsage, error) {
	if runID == "" {
		return TelemetryUsage{}, ErrInvalidTelemetry
	}
	var usage TelemetryUsage
	err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		usage, err = deleteTelemetryTx(tx, runID)
		return err
	})
	return usage, err
}

func deleteTelemetryTx(tx *bolt.Tx, runID string) (TelemetryUsage, error) {
	root := tx.Bucket([]byte(telemetryBucketName))
	if root == nil {
		return TelemetryUsage{}, nil
	}
	run := root.Bucket([]byte(runID))
	if run == nil {
		return TelemetryUsage{}, nil
	}
	usage, err := telemetryUsageForBucket(run)
	if err != nil {
		return TelemetryUsage{}, err
	}
	if err := root.DeleteBucket([]byte(runID)); err != nil {
		return TelemetryUsage{}, err
	}
	return usage, nil
}

// TelemetryStoreUsage reports bounded physical use across all Runs.
func (s *Store) TelemetryStoreUsage() (TelemetryUsage, error) {
	var total TelemetryUsage
	err := s.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(telemetryBucketName))
		if root == nil {
			total.HistoryComplete = true
			return nil
		}
		total.HistoryComplete = true
		return root.ForEach(func(key, value []byte) error {
			if value != nil {
				return nil
			}
			bucket := root.Bucket(key)
			usage, err := telemetryUsageForBucket(bucket)
			if err != nil {
				return err
			}
			total.Runs += usage.Runs
			total.RawSamples += usage.RawSamples
			total.AggregatePoints += usage.AggregatePoints
			total.Gaps += usage.Gaps
			total.Bytes += usage.Bytes
			total.RawBytes += usage.RawBytes
			total.AggregateBytes += usage.AggregateBytes
			total.GapBytes += usage.GapBytes
			total.HistoryComplete = total.HistoryComplete && usage.HistoryComplete
			return nil
		})
	})
	return total, err
}

func telemetryUsageForBucket(run *bolt.Bucket) (TelemetryUsage, error) {
	usage := TelemetryUsage{Runs: 1, HistoryComplete: true}
	meta, err := telemetryMetaFor(run)
	if err != nil {
		return TelemetryUsage{}, err
	}
	usage.RawRetainedFrom = cloneTime(meta.RawRetainedFrom)
	usage.RetainedFrom = cloneTime(meta.RetainedFrom)
	usage.HistoryComplete = meta.HistoryComplete
	if raw := run.Bucket([]byte(telemetryRawName)); raw != nil {
		usage.RawSamples = raw.Stats().KeyN
		usage.RawBytes = bucketDataBytes(raw)
	}
	if aggregates := run.Bucket([]byte(telemetryAggName)); aggregates != nil {
		usage.AggregatePoints = aggregates.Stats().KeyN
		usage.AggregateBytes = bucketDataBytes(aggregates)
	}
	if gaps := run.Bucket([]byte(telemetryGapName)); gaps != nil {
		usage.Gaps = gaps.Stats().KeyN
		usage.GapBytes = bucketDataBytes(gaps)
	}
	if usage.Gaps > 0 {
		usage.HistoryComplete = false
	}
	usage.Bytes = bucketDataBytes(run)
	return usage, nil
}

func bucketDataBytes(bucket *bolt.Bucket) int64 {
	var size int64
	_ = bucket.ForEach(func(key, value []byte) error {
		size += int64(len(key) + len(value))
		if value == nil {
			if child := bucket.Bucket(key); child != nil {
				size += bucketDataBytes(child)
			}
		}
		return nil
	})
	return size
}
