package model

import (
	"fmt"
	"time"
)

const ProtocolVersion = 1

type State string

const (
	Accepted    State = "accepted"
	Starting    State = "starting"
	Running     State = "running"
	Terminating State = "terminating"
	Reconciling State = "reconciling"
	Terminal    State = "terminal"
	Uncertain   State = "uncertain"
)

type Environment struct {
	Mode  string            `json:"mode,omitempty"`
	Set   map[string]string `json:"set,omitempty"`
	Unset []string          `json:"unset,omitempty"`
}

type Lifetime struct {
	Mode    string `json:"mode,omitempty"`
	LeaseMs int64  `json:"leaseMs,omitempty"`
}

// Limits distinguishes a ceiling on distinct processes from a ceiling on
// kernel tasks. On Linux, a cgroup pids limit enforces TaskCount, not
// ProcessCount.
type Limits struct {
	MemoryBytes     int64 `json:"memoryBytes,omitempty"`
	CPUQuotaPercent int64 `json:"cpuQuotaPercent,omitempty"`
	ProcessCount    int64 `json:"processCount,omitempty"`
	TaskCount       int64 `json:"taskCount,omitempty"`
	WallTimeMs      int64 `json:"wallTimeMs,omitempty"`
	OutputBytes     int64 `json:"outputBytes,omitempty"`
}

type RunSpec struct {
	Argv        []string          `json:"argv"`
	Cwd         string            `json:"cwd"`
	Environment Environment       `json:"environment,omitempty"`
	Interactive bool              `json:"interactive,omitempty"`
	Lifetime    Lifetime          `json:"lifetime,omitempty"`
	Limits      Limits            `json:"limits,omitempty"`
	ParentRunID string            `json:"parentRunId,omitempty"`
	Correlation map[string]string `json:"correlation,omitempty"`
}

// Metric distinguishes an observed zero from unavailable or unsupported data.
type Metric struct {
	Status string `json:"status"`
	Value  int64  `json:"value,omitempty"`
}

// HostEnvelopeConfig contains the aggregate physical workload ceilings
// configured by the supervisor. Zero disables an individual ceiling.
type HostEnvelopeConfig struct {
	MemoryBytes   int64 `json:"memoryBytes,omitempty"`
	TaskCount     int64 `json:"taskCount,omitempty"`
	MaxActiveRuns int64 `json:"maxActiveRuns,omitempty"`
}

// HostEnvelopeCapabilities reports the physical controls and observations
// available for the aggregate workload boundary.
type HostEnvelopeCapabilities struct {
	WorkloadRoot         bool `json:"workloadRoot"`
	MemoryEnforcement    bool `json:"memoryEnforcement"`
	TaskCountEnforcement bool `json:"taskCountEnforcement"`
	ActiveRunEnforcement bool `json:"activeRunEnforcement"`
	MemoryTelemetry      bool `json:"memoryTelemetry"`
	TaskTelemetry        bool `json:"taskTelemetry"`
	PressureTelemetry    bool `json:"pressureTelemetry"`
}

// HostPressure stores PSI averages as milli-percent and cumulative pressure
// time in microseconds. Metric status distinguishes unavailable and
// unsupported observations from a measured zero.
type HostPressure struct {
	Status                 string `json:"status"`
	SomeAvg10MilliPercent  Metric `json:"someAvg10MilliPercent"`
	SomeAvg60MilliPercent  Metric `json:"someAvg60MilliPercent"`
	SomeAvg300MilliPercent Metric `json:"someAvg300MilliPercent"`
	SomeTotalUsec          Metric `json:"someTotalUsec"`
	FullAvg10MilliPercent  Metric `json:"fullAvg10MilliPercent"`
	FullAvg60MilliPercent  Metric `json:"fullAvg60MilliPercent"`
	FullAvg300MilliPercent Metric `json:"fullAvg300MilliPercent"`
	FullTotalUsec          Metric `json:"fullTotalUsec"`
}

// HostEnvelopeStatus is a bounded point-in-time observation of aggregate
// workload usage and pressure. It carries no scheduling recommendations.
type HostEnvelopeStatus struct {
	Status         string                   `json:"status"`
	Config         HostEnvelopeConfig       `json:"config"`
	Capabilities   HostEnvelopeCapabilities `json:"capabilities"`
	ActiveRuns     Metric                   `json:"activeRuns"`
	MemoryBytes    Metric                   `json:"memoryBytes"`
	TaskCount      Metric                   `json:"taskCount"`
	MemoryPressure HostPressure             `json:"memoryPressure"`
	CPUPressure    HostPressure             `json:"cpuPressure"`
	IOPressure     HostPressure             `json:"ioPressure"`
	Reason         string                   `json:"reason,omitempty"`
}

// RuntimeStoreUsage is a bounded logical and physical store size projection.
// It deliberately contains counters and byte totals only, never Run content.
type RuntimeStoreUsage struct {
	Status   string `json:"status"`
	RunCount int    `json:"runCount"`
	// NonterminalRunCount includes accepted/starting/terminating state records;
	// it is not a count of currently executing physical processes.
	NonterminalRunCount       int   `json:"nonterminalRunCount"`
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

// RuntimeRetentionPolicy uses explicit milliseconds so configuration and
// protocol values do not depend on Go's duration serialization units.
type RuntimeRetentionPolicy struct {
	MaxAgeMs            int64   `json:"maxAgeMs"`
	MaxTerminalRuns     int     `json:"maxTerminalRuns"`
	MaxStateBytes       int64   `json:"maxStateBytes"`
	PreserveTombstones  bool    `json:"preserveTombstones"`
	MaxTombstones       int     `json:"maxTombstones"`
	MaxTombstoneAgeMs   int64   `json:"maxTombstoneAgeMs"`
	CompactMinFreeBytes int64   `json:"compactMinFreeBytes"`
	CompactMinFreeRatio float64 `json:"compactMinFreeRatio"`
}

// RuntimeRetentionStatus contains the effective policy and one bounded
// durable checkpoint of terminal evidence collection.
type RuntimeRetentionStatus struct {
	Status                     string                 `json:"status"`
	Policy                     RuntimeRetentionPolicy `json:"policy"`
	LastRunAt                  *time.Time             `json:"lastRunAt,omitempty"`
	EvictedRunsTotal           uint64                 `json:"evictedRunsTotal"`
	EvictedBytesTotal          int64                  `json:"evictedBytesTotal"`
	RemovedTombstonesTotal     uint64                 `json:"removedTombstonesTotal"`
	ExpiredBindingsTotal       uint64                 `json:"expiredBindingsTotal"`
	SubmissionReplayStubsTotal uint64                 `json:"submissionReplayStubsTotal"`
	LastEvictedFinishedAt      *time.Time             `json:"lastEvictedFinishedAt,omitempty"`
	TerminalRunsRemaining      int                    `json:"terminalRunsRemaining"`
	TerminalEvidenceBytes      int64                  `json:"terminalEvidenceBytes"`
	BudgetExceeded             bool                   `json:"budgetExceeded"`
	CompactionRecommended      bool                   `json:"compactionRecommended"`
	CompactionStatus           string                 `json:"compactionStatus"`
	CompactionErrorCode        string                 `json:"compactionErrorCode,omitempty"`
	CompactionAttemptedAt      *time.Time             `json:"compactionAttemptedAt,omitempty"`
}

// RuntimeStatus is a point-in-time summary of bounded store/retention state.
type RuntimeStatus struct {
	Version   int                    `json:"version"`
	Store     RuntimeStoreUsage      `json:"store"`
	Retention RuntimeRetentionStatus `json:"retention"`
}

// TombstoneSummary identifies which historical terminal evidence was evicted
// without returning its original spec, output, event bodies, or processes.
type TombstoneSummary struct {
	RunID         string    `json:"runId"`
	EvictedAt     time.Time `json:"evictedAt"`
	Reasons       []string  `json:"reasons"`
	ReceiptSHA256 string    `json:"receiptSha256"`
}

// Resources reports process counts independently from Linux kernel task
// counts. ProcessCount is distinct owned processes; TaskCount includes
// threads and matches cgroup v2 pids-controller accounting.
type Resources struct {
	MemoryBytes      Metric `json:"memoryBytes"`
	PeakMemoryBytes  Metric `json:"peakMemoryBytes"`
	CPUTimeNs        Metric `json:"cpuTimeNs"`
	ProcessCount     Metric `json:"processCount"`
	PeakProcessCount Metric `json:"peakProcessCount"`
	TaskCount        Metric `json:"taskCount"`
	PeakTaskCount    Metric `json:"peakTaskCount"`
	SampleIntervalMs int64  `json:"sampleIntervalMs,omitempty"`
}

// EvidenceStatus distinguishes a measured zero from an observation that was
// unavailable or a capability the backend does not support.
type EvidenceStatus string

const (
	EvidenceMeasured    EvidenceStatus = "measured"
	EvidenceUnavailable EvidenceStatus = "unavailable"
	EvidenceUnsupported EvidenceStatus = "unsupported"
)

type ProcessMembership string

const (
	ProcessMembershipOwned       ProcessMembership = "owned"
	ProcessMembershipOutside     ProcessMembership = "outside"
	ProcessMembershipUnknown     ProcessMembership = "unknown"
	ProcessMembershipUnsupported ProcessMembership = "unsupported"
)

type ProcessIdentity struct {
	PID            int    `json:"pid"`
	StartTimeTicks uint64 `json:"startTimeTicks"`
}

// ProcessEvidence is one bounded, point-in-time Linux process observation.
// Comm is the kernel's short process name; argv and environment are never
// included. StartTimeTicks disambiguates PID reuse within the current boot.
type ProcessEvidence struct {
	PID                  int               `json:"pid"`
	StartTimeTicks       uint64            `json:"startTimeTicks"`
	ParentPID            int               `json:"parentPid"`
	ParentStartTimeTicks uint64            `json:"parentStartTimeTicks,omitempty"`
	ParentObserved       bool              `json:"parentObserved"`
	Membership           ProcessMembership `json:"membership"`
	Comm                 string            `json:"comm,omitempty"`
	State                string            `json:"state,omitempty"`
	CPUTimeNs            Metric            `json:"cpuTimeNs"`
	RSSBytes             Metric            `json:"rssBytes"`
}

type ProcessChangeKind string

const (
	ProcessStarted           ProcessChangeKind = "started"
	ProcessExited            ProcessChangeKind = "exited"
	ProcessMembershipChanged ProcessChangeKind = "membership-changed"
)

type ProcessEvidenceChange struct {
	ObservedAt         time.Time         `json:"observedAt"`
	Kind               ProcessChangeKind `json:"kind"`
	Process            ProcessIdentity   `json:"process"`
	PreviousMembership ProcessMembership `json:"previousMembership,omitempty"`
	Membership         ProcessMembership `json:"membership,omitempty"`
}

// DeviceIOMetrics uses the kernel's stable major:minor device identity and
// carries no caller-controlled path or device name.
type DeviceIOMetrics struct {
	Device          string `json:"device"`
	ReadBytes       Metric `json:"readBytes"`
	WriteBytes      Metric `json:"writeBytes"`
	ReadOperations  Metric `json:"readOperations"`
	WriteOperations Metric `json:"writeOperations"`
}

type IOMetrics struct {
	ReadBytes              Metric            `json:"readBytes"`
	WriteBytes             Metric            `json:"writeBytes"`
	ReadOperations         Metric            `json:"readOperations"`
	WriteOperations        Metric            `json:"writeOperations"`
	Devices                []DeviceIOMetrics `json:"devices,omitempty"`
	DeviceEvidenceStatus   EvidenceStatus    `json:"deviceEvidenceStatus"`
	DeviceEvidenceComplete bool              `json:"deviceEvidenceComplete"`
}

// PressureMetrics stores PSI averages in basis points (100 = 1%) and the
// kernel's cumulative stall time in microseconds. Unsupported lines retain an
// explicit status instead of being projected to zero.
type PressureMetrics struct {
	SomeAvg10BasisPoints  Metric `json:"someAvg10BasisPoints"`
	SomeAvg60BasisPoints  Metric `json:"someAvg60BasisPoints"`
	SomeAvg300BasisPoints Metric `json:"someAvg300BasisPoints"`
	SomeTotalUS           Metric `json:"someTotalUs"`
	FullAvg10BasisPoints  Metric `json:"fullAvg10BasisPoints"`
	FullAvg60BasisPoints  Metric `json:"fullAvg60BasisPoints"`
	FullAvg300BasisPoints Metric `json:"fullAvg300BasisPoints"`
	FullTotalUS           Metric `json:"fullTotalUs"`
}

type PSIMetrics struct {
	CPU    PressureMetrics `json:"cpu"`
	Memory PressureMetrics `json:"memory"`
	IO     PressureMetrics `json:"io"`
}

type IOActivity struct {
	InputBytes   Metric     `json:"inputBytes"`
	InputWrites  Metric     `json:"inputWrites"`
	ResizeCount  Metric     `json:"resizeCount"`
	LastInputAt  *time.Time `json:"lastInputAt,omitempty"`
	LastResizeAt *time.Time `json:"lastResizeAt,omitempty"`
}

type TelemetrySample struct {
	Version                 int                     `json:"version"`
	RunID                   string                  `json:"runId"`
	Sequence                uint64                  `json:"sequence"`
	ObservedAt              time.Time               `json:"observedAt"`
	Resources               Resources               `json:"resources"`
	IO                      IOMetrics               `json:"io"`
	PSI                     PSIMetrics              `json:"psi"`
	Activity                IOActivity              `json:"activity"`
	Processes               []ProcessEvidence       `json:"processes,omitempty"`
	ProcessChanges          []ProcessEvidenceChange `json:"processChanges,omitempty"`
	ProcessEvidenceStatus   EvidenceStatus          `json:"processEvidenceStatus"`
	ProcessEvidenceReason   string                  `json:"processEvidenceReason,omitempty"`
	ProcessEvidenceComplete bool                    `json:"processEvidenceComplete"`
}

const TelemetrySchemaVersion = 1

type TelemetryResolution string

const (
	TelemetryRaw       TelemetryResolution = "raw"
	Telemetry10Seconds TelemetryResolution = "10s"
	TelemetryAdaptive  TelemetryResolution = "adaptive"
)

type MetricAggregate struct {
	Min              Metric `json:"min"`
	Max              Metric `json:"max"`
	First            Metric `json:"first"`
	Last             Metric `json:"last"`
	Delta            Metric `json:"delta"`
	Count            uint64 `json:"count"`
	UnavailableCount uint64 `json:"unavailableCount"`
	UnsupportedCount uint64 `json:"unsupportedCount"`
}

type AggregateResources struct {
	MemoryBytes      MetricAggregate `json:"memoryBytes"`
	PeakMemoryBytes  MetricAggregate `json:"peakMemoryBytes"`
	CPUTimeNs        MetricAggregate `json:"cpuTimeNs"`
	ProcessCount     MetricAggregate `json:"processCount"`
	PeakProcessCount MetricAggregate `json:"peakProcessCount"`
	TaskCount        MetricAggregate `json:"taskCount"`
	PeakTaskCount    MetricAggregate `json:"peakTaskCount"`
}

type AggregateIOMetrics struct {
	ReadBytes       MetricAggregate `json:"readBytes"`
	WriteBytes      MetricAggregate `json:"writeBytes"`
	ReadOperations  MetricAggregate `json:"readOperations"`
	WriteOperations MetricAggregate `json:"writeOperations"`
}

type AggregatePressureMetrics struct {
	SomeAvg10BasisPoints  MetricAggregate `json:"someAvg10BasisPoints"`
	SomeAvg60BasisPoints  MetricAggregate `json:"someAvg60BasisPoints"`
	SomeAvg300BasisPoints MetricAggregate `json:"someAvg300BasisPoints"`
	SomeTotalUS           MetricAggregate `json:"someTotalUs"`
	FullAvg10BasisPoints  MetricAggregate `json:"fullAvg10BasisPoints"`
	FullAvg60BasisPoints  MetricAggregate `json:"fullAvg60BasisPoints"`
	FullAvg300BasisPoints MetricAggregate `json:"fullAvg300BasisPoints"`
	FullTotalUS           MetricAggregate `json:"fullTotalUs"`
}

type AggregatePSIMetrics struct {
	CPU    AggregatePressureMetrics `json:"cpu"`
	Memory AggregatePressureMetrics `json:"memory"`
	IO     AggregatePressureMetrics `json:"io"`
}

type AggregateIOActivity struct {
	InputBytes  MetricAggregate `json:"inputBytes"`
	InputWrites MetricAggregate `json:"inputWrites"`
	ResizeCount MetricAggregate `json:"resizeCount"`
}

type ProcessPeak struct {
	ObservedAt time.Time       `json:"observedAt"`
	Process    ProcessEvidence `json:"process"`
}

type TelemetryAggregate struct {
	Version          int                     `json:"version"`
	RunID            string                  `json:"runId"`
	Resolution       TelemetryResolution     `json:"resolution"`
	WindowStart      time.Time               `json:"windowStart"`
	WindowEnd        time.Time               `json:"windowEnd"`
	FirstSequence    uint64                  `json:"firstSequence"`
	LastSequence     uint64                  `json:"lastSequence"`
	SampleCount      uint64                  `json:"sampleCount"`
	Resources        AggregateResources      `json:"resources"`
	IO               AggregateIOMetrics      `json:"io"`
	PSI              AggregatePSIMetrics     `json:"psi"`
	Activity         AggregateIOActivity     `json:"activity"`
	ProcessPeaks     []ProcessPeak           `json:"processPeaks,omitempty"`
	ProcessChanges   []ProcessEvidenceChange `json:"processChanges,omitempty"`
	ChangeCount      uint64                  `json:"changeCount"`
	EvidenceComplete bool                    `json:"evidenceComplete"`
}

type TelemetryGap struct {
	From          time.Time           `json:"from"`
	To            time.Time           `json:"to"`
	Reason        string              `json:"reason"`
	Metrics       []string            `json:"metrics,omitempty"`
	Resolution    TelemetryResolution `json:"resolution,omitempty"`
	DroppedPoints uint64              `json:"droppedPoints,omitempty"`
}

type TelemetryQuery struct {
	RunID      string              `json:"runId"`
	From       *time.Time          `json:"from,omitempty"`
	To         *time.Time          `json:"to,omitempty"`
	Resolution TelemetryResolution `json:"resolution,omitempty"`
	Limit      int                 `json:"limit,omitempty"`
	Cursor     string              `json:"cursor,omitempty"`
}

type TelemetryResponse struct {
	Version         int                  `json:"version"`
	RunID           string               `json:"runId"`
	Samples         []TelemetrySample    `json:"samples,omitempty"`
	Aggregates      []TelemetryAggregate `json:"aggregates,omitempty"`
	Gaps            []TelemetryGap       `json:"gaps,omitempty"`
	RawRetainedFrom *time.Time           `json:"rawRetainedFrom,omitempty"`
	RetainedFrom    *time.Time           `json:"retainedFrom,omitempty"`
	Resolution      TelemetryResolution  `json:"resolution,omitempty"`
	HistoryComplete bool                 `json:"historyComplete"`
	NextCursor      string               `json:"nextCursor,omitempty"`
}

type OutputStream struct {
	ObservedBytes int64 `json:"observedBytes"`
	RetainedBytes int64 `json:"retainedBytes"`
	RetainedFrom  int64 `json:"retainedFrom"`
	Truncated     bool  `json:"truncated"`
}

type Output struct {
	Stdout          OutputStream `json:"stdout"`
	Stderr          OutputStream `json:"stderr"`
	PTY             OutputStream `json:"pty"`
	HistoryComplete bool         `json:"historyComplete"`
}

type Ownership struct {
	Backend      string `json:"backend"`
	PID          int    `json:"pid,omitempty"`
	StartTime    uint64 `json:"startTime,omitempty"`
	ProcessGroup int    `json:"processGroup,omitempty"`
	CgroupPath   string `json:"cgroupPath,omitempty"`
	Token        string `json:"token,omitempty"`
}

type Receipt struct {
	Version            int        `json:"version"`
	RunID              string     `json:"runId"`
	Outcome            string     `json:"outcome"`
	AcceptedArgvSHA256 string     `json:"acceptedArgvSha256,omitempty"`
	ExitCode           *int       `json:"exitCode,omitempty"`
	Signal             string     `json:"signal,omitempty"`
	StartedAt          *time.Time `json:"startedAt,omitempty"`
	FinishedAt         time.Time  `json:"finishedAt"`
	Resources          Resources  `json:"resources"`
	// EffectiveCapabilities is frozen from the backend and capabilities used by
	// this Run. It is independent of capabilities currently advertised by the
	// host when the receipt is read.
	EffectiveCapabilities *Capabilities `json:"effectiveCapabilities,omitempty"`
	// Capabilities is the Protocol v1 compatibility projection of
	// EffectiveCapabilities. New consumers should read EffectiveCapabilities.
	Capabilities Capabilities `json:"capabilities"`
	Output       Output       `json:"output"`
	// EventFirstSeq and EventLastSeq cover the assigned journal range; EventRetainedFrom is its current retention watermark.
	EventFirstSeq        uint64 `json:"eventFirstSeq"`
	EventLastSeq         uint64 `json:"eventLastSeq"`
	EventRetainedFrom    uint64 `json:"eventRetainedFrom"`
	EventHistoryComplete bool   `json:"eventHistoryComplete"`
	EvidenceIncomplete   bool   `json:"evidenceIncomplete"`
	TerminationRequested bool   `json:"terminationRequested"`
	Forced               bool   `json:"forced"`
	Cleanup              string `json:"cleanup"`
}

type Run struct {
	ID         string     `json:"runId"`
	Spec       RunSpec    `json:"spec"`
	State      State      `json:"state"`
	Generation uint64     `json:"generation"`
	CreatedAt  time.Time  `json:"createdAt"`
	StartedAt  *time.Time `json:"startedAt,omitempty"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Ownership  *Ownership `json:"ownership,omitempty"`
	// EffectiveCapabilities is nil until execution ownership establishes which
	// backend mode and capability set this Run actually uses.
	EffectiveCapabilities       *Capabilities `json:"effectiveCapabilities,omitempty"`
	Resources                   Resources     `json:"resources"`
	Output                      Output        `json:"output"`
	Receipt                     *Receipt      `json:"receipt,omitempty"`
	LeaseGeneration             uint64        `json:"leaseGeneration,omitempty"`
	LeaseExpiry                 *time.Time    `json:"leaseExpiry,omitempty"`
	LastLeaseExpectedGeneration uint64        `json:"lastLeaseExpectedGeneration,omitempty"`
	LastLeaseMs                 int64         `json:"lastLeaseMs,omitempty"`
	LastOutputAt                *time.Time    `json:"lastOutputAt,omitempty"`
	LastResourceSampleAt        *time.Time    `json:"lastResourceSampleAt,omitempty"`
	ResourceGap                 bool          `json:"resourceGap,omitempty"`
	LastCPUActivityAt           *time.Time    `json:"lastCpuActivityAt,omitempty"`
	LastProcessChangeAt         *time.Time    `json:"lastProcessChangeAt,omitempty"`
	TerminationReason           string        `json:"terminationReason,omitempty"`
	Attachments                 int           `json:"attachments"`
}

// EventKind is the canonical vocabulary for lifecycle, control, output, and
// resource events. The type remains string-backed so newer protocol peers can
// preserve kinds they do not yet interpret.
type EventKind string

const (
	EventRunAccepted          EventKind = "run.accepted"
	EventRunStarting          EventKind = "run.starting"
	EventRunRunning           EventKind = "run.running"
	EventRunOwned             EventKind = "run.owned"
	EventRunTerminating       EventKind = "run.terminating"
	EventRunTerminal          EventKind = "run.terminal"
	EventRunUncertain         EventKind = "run.uncertain"
	EventRunReconciling       EventKind = "run.reconciling"
	EventRunReconciled        EventKind = "run.reconciled"
	EventLeaseExpired         EventKind = "lease.expired"
	EventLeaseRenewed         EventKind = "lease.renewed"
	EventLimitReached         EventKind = "limit.reached"
	EventResourceSample       EventKind = "resource.sample"
	EventResourceGap          EventKind = "resource.gap"
	EventResourceUnavailable  EventKind = "resource.unavailable"
	EventOutputChunk          EventKind = "output.chunk"
	EventOutputGap            EventKind = "output.gap"
	EventPTYAttached          EventKind = "pty.attached"
	EventPTYDetached          EventKind = "pty.detached"
	EventPTYAttachmentExpired EventKind = "pty.attachment-expired"
	EventSignalRequested      EventKind = "signal.requested"
	EventSignalDelivered      EventKind = "signal.delivered"

	// These kinds are reserved as protected lifecycle/control evidence for
	// producers that record the lower-level operation separately.
	EventTerminationRequested EventKind = "termination.requested"
	EventSignalSent           EventKind = "signal.sent"
	EventProcessExited        EventKind = "process.exited"
	EventCancelRequested      EventKind = "cancel.requested"
	EventControlChanged       EventKind = "control.changed"
	EventControlRecovered     EventKind = "control.recovered"
)

// EventPayload is a versioned discriminated union. Exactly one branch should
// be populated, and it must match the event kind. Body remains available for
// decoding events written by the pre-Wave-2 schema during producer migration.
type EventPayload struct {
	Run         *RunEventPayload         `json:"run,omitempty"`
	Control     *ControlEventPayload     `json:"control,omitempty"`
	Signal      *SignalEventPayload      `json:"signal,omitempty"`
	Limit       *LimitEventPayload       `json:"limit,omitempty"`
	Lease       *LeaseEventPayload       `json:"lease,omitempty"`
	PTY         *PTYEventPayload         `json:"pty,omitempty"`
	Output      *OutputEventPayload      `json:"output,omitempty"`
	Resource    *ResourceEventPayload    `json:"resource,omitempty"`
	ResourceGap *ResourceGapEventPayload `json:"resourceGap,omitempty"`
}

type RunEventPayload struct {
	State      State  `json:"state,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
	PID        int    `json:"pid,omitempty"`
	Outcome    string `json:"outcome,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type ControlEventPayload struct {
	OperationID       string `json:"operationId,omitempty"`
	Control           string `json:"control,omitempty"`
	Action            string `json:"action,omitempty"`
	Value             *int64 `json:"value,omitempty"`
	PreviousValue     *int64 `json:"previousValue,omitempty"`
	Unlimited         *bool  `json:"unlimited,omitempty"`
	PreviousUnlimited *bool  `json:"previousUnlimited,omitempty"`
	Reason            string `json:"reason,omitempty"`
	Outcome           string `json:"outcome,omitempty"`
}

type SignalEventPayload struct {
	Signal string `json:"signal"`
}

type LimitEventPayload struct {
	Limit string `json:"limit"`
}

type LeaseEventPayload struct {
	Generation uint64     `json:"generation,omitempty"`
	ExpiresAt  *time.Time `json:"expiresAt,omitempty"`
}

type PTYEventPayload struct {
	Attachments int `json:"attachments,omitempty"`
	Expired     int `json:"expired,omitempty"`
}

type OutputEventPayload struct {
	Stream         string `json:"stream"`
	Bytes          int64  `json:"bytes,omitempty"`
	ObservedBytes  int64  `json:"observedBytes,omitempty"`
	TimestampBasis string `json:"timestampBasis,omitempty"`
}

type ResourceEventPayload struct {
	Resources Resources `json:"resources"`
}

type ResourceGapEventPayload struct {
	From            *time.Time `json:"from,omitempty"`
	To              *time.Time `json:"to,omitempty"`
	Status          string     `json:"status,omitempty"`
	Reason          string     `json:"reason,omitempty"`
	LatestResources *Resources `json:"latestResources,omitempty"`
}

const EventSchemaVersion = 1

type Event struct {
	Version    int            `json:"version"`
	RunID      string         `json:"runId"`
	Seq        uint64         `json:"seq"`
	Kind       EventKind      `json:"kind"`
	ObservedAt time.Time      `json:"observedAt"`
	Payload    *EventPayload  `json:"payload,omitempty"`
	Body       map[string]any `json:"body,omitempty"`
}

// IsCriticalEventKind reports event kinds that the lifecycle journal protects
// from ordinary retention compaction.
func IsCriticalEventKind(kind EventKind) bool {
	switch kind {
	case EventRunAccepted, EventRunStarting, EventRunRunning, EventRunOwned,
		EventRunTerminating, EventRunTerminal, EventRunUncertain,
		EventRunReconciling, EventRunReconciled,
		EventLeaseExpired, EventLeaseRenewed,
		EventLimitReached, EventTerminationRequested, EventSignalSent,
		EventProcessExited, EventCancelRequested,
		EventControlChanged, EventControlRecovered:
		return true
	default:
		return false
	}
}

// Validate checks that a populated typed payload has one branch and that the
// branch belongs to the event kind. A nil payload is valid for kinds whose
// occurrence is sufficient evidence by itself.
func (p EventPayload) Validate(kind EventKind) error {
	branches := 0
	branch := ""
	for name, present := range map[string]bool{
		"run": p.Run != nil, "control": p.Control != nil, "signal": p.Signal != nil,
		"limit": p.Limit != nil, "lease": p.Lease != nil, "pty": p.PTY != nil,
		"output": p.Output != nil, "resource": p.Resource != nil,
		"resourceGap": p.ResourceGap != nil,
	} {
		if present {
			branches++
			branch = name
		}
	}
	if branches != 1 {
		return fmt.Errorf("event %q payload must contain exactly one branch", kind)
	}
	var want string
	switch kind {
	case EventRunAccepted, EventRunStarting, EventRunRunning, EventRunOwned,
		EventRunTerminating, EventRunTerminal, EventRunUncertain,
		EventRunReconciling, EventRunReconciled, EventProcessExited:
		want = "run"
	case EventTerminationRequested, EventCancelRequested,
		EventControlChanged, EventControlRecovered:
		want = "control"
	case EventSignalRequested, EventSignalDelivered, EventSignalSent:
		want = "signal"
	case EventLimitReached:
		want = "limit"
	case EventLeaseExpired, EventLeaseRenewed:
		want = "lease"
	case EventPTYAttached, EventPTYDetached, EventPTYAttachmentExpired:
		want = "pty"
	case EventOutputChunk, EventOutputGap:
		want = "output"
	case EventResourceSample, EventResourceUnavailable:
		want = "resource"
	case EventResourceGap:
		want = "resourceGap"
	default:
		return fmt.Errorf("event %q has no typed payload contract", kind)
	}
	if branch != want {
		return fmt.Errorf("event %q payload branch is %q, want %q", kind, branch, want)
	}
	return nil
}

type Capabilities struct {
	Backend             string `json:"backend"`
	PTY                 bool   `json:"pty"`
	MemoryEnforcement   bool   `json:"memoryEnforcement"`
	CPUQuotaEnforcement bool   `json:"cpuQuotaEnforcement"`
	// ProcessCountEnforcement means distinct process count can be enforced,
	// rather than only Linux kernel-task count.
	ProcessCountEnforcement bool `json:"processCountEnforcement"`
	// TaskCountEnforcement means kernel tasks, including threads, can be
	// enforced as a separate capability.
	TaskCountEnforcement  bool     `json:"taskCountEnforcement"`
	MemoryTelemetry       bool     `json:"memoryTelemetry"`
	CPUTelemetry          bool     `json:"cpuTelemetry"`
	ProcessTelemetry      bool     `json:"processTelemetry"`
	TaskTelemetry         bool     `json:"taskTelemetry"`
	CgroupFreeze          bool     `json:"cgroupFreeze"`
	MemoryHighControl     bool     `json:"memoryHighControl"`
	CPUQuotaControl       bool     `json:"cpuQuotaControl"`
	RestartReconciliation string   `json:"restartReconciliation"`
	Signals               []string `json:"signals"`
}
