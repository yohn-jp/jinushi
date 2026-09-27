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
	Reason string `json:"reason,omitempty"`
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
		EventProcessExited, EventCancelRequested:
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
	case EventTerminationRequested, EventCancelRequested:
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
	RestartReconciliation string   `json:"restartReconciliation"`
	Signals               []string `json:"signals"`
}
