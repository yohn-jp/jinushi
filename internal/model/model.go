package model

import "time"

const ProtocolVersion = 1

type State string

const (
	Accepted State = "accepted"
	Starting State = "starting"
	Running State = "running"
	Terminating State = "terminating"
	Reconciling State = "reconciling"
	Terminal State = "terminal"
	Uncertain State = "uncertain"
)

type Environment struct {
	Mode string `json:"mode,omitempty"`
	Set map[string]string `json:"set,omitempty"`
	Unset []string `json:"unset,omitempty"`
}

type Lifetime struct {
	Mode string `json:"mode,omitempty"`
	LeaseMs int64 `json:"leaseMs,omitempty"`
}

type Limits struct {
	MemoryBytes int64 `json:"memoryBytes,omitempty"`
	CPUQuotaPercent int64 `json:"cpuQuotaPercent,omitempty"`
	ProcessCount int64 `json:"processCount,omitempty"`
	WallTimeMs int64 `json:"wallTimeMs,omitempty"`
	OutputBytes int64 `json:"outputBytes,omitempty"`
}

type RunSpec struct {
	Argv []string `json:"argv"`
	Cwd string `json:"cwd"`
	Environment Environment `json:"environment,omitempty"`
	Interactive bool `json:"interactive,omitempty"`
	Lifetime Lifetime `json:"lifetime,omitempty"`
	Limits Limits `json:"limits,omitempty"`
	ParentRunID string `json:"parentRunId,omitempty"`
	Correlation map[string]string `json:"correlation,omitempty"`
}

// Metric distinguishes an observed zero from unavailable or unsupported data.
type Metric struct {
	Status string `json:"status"`
	Value int64 `json:"value,omitempty"`
}

type Resources struct {
	MemoryBytes Metric `json:"memoryBytes"`
	PeakMemoryBytes Metric `json:"peakMemoryBytes"`
	CPUTimeNs Metric `json:"cpuTimeNs"`
	ProcessCount Metric `json:"processCount"`
	PeakProcessCount Metric `json:"peakProcessCount"`
	SampleIntervalMs int64 `json:"sampleIntervalMs,omitempty"`
}

type OutputStream struct {
	ObservedBytes int64 `json:"observedBytes"`
	RetainedBytes int64 `json:"retainedBytes"`
	RetainedFrom int64 `json:"retainedFrom"`
	Truncated bool `json:"truncated"`
}

type Output struct {
	Stdout OutputStream `json:"stdout"`
	Stderr OutputStream `json:"stderr"`
	PTY OutputStream `json:"pty"`
	HistoryComplete bool `json:"historyComplete"`
}

type Ownership struct {
	Backend string `json:"backend"`
	PID int `json:"pid,omitempty"`
	StartTime uint64 `json:"startTime,omitempty"`
	ProcessGroup int `json:"processGroup,omitempty"`
	CgroupPath string `json:"cgroupPath,omitempty"`
	Token string `json:"token,omitempty"`
}

type Receipt struct {
	Version int `json:"version"`
	RunID string `json:"runId"`
	Outcome string `json:"outcome"`
	ExitCode *int `json:"exitCode,omitempty"`
	Signal string `json:"signal,omitempty"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
	FinishedAt time.Time `json:"finishedAt"`
	Resources Resources `json:"resources"`
	Output Output `json:"output"`
	TerminationRequested bool `json:"terminationRequested"`
	Forced bool `json:"forced"`
	Cleanup string `json:"cleanup"`
}

type Run struct {
	ID string `json:"runId"`
	Spec RunSpec `json:"spec"`
	State State `json:"state"`
	Generation uint64 `json:"generation"`
	CreatedAt time.Time `json:"createdAt"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Ownership *Ownership `json:"ownership,omitempty"`
	Resources Resources `json:"resources"`
	Output Output `json:"output"`
	Receipt *Receipt `json:"receipt,omitempty"`
	LeaseGeneration uint64 `json:"leaseGeneration,omitempty"`
	LeaseExpiry *time.Time `json:"leaseExpiry,omitempty"`
	LastOutputAt *time.Time `json:"lastOutputAt,omitempty"`
	LastCPUActivityAt *time.Time `json:"lastCpuActivityAt,omitempty"`
	LastProcessChangeAt *time.Time `json:"lastProcessChangeAt,omitempty"`
	TerminationReason string `json:"terminationReason,omitempty"`
}

type Event struct {
	Version int `json:"version"`
	RunID string `json:"runId"`
	Seq uint64 `json:"seq"`
	Kind string `json:"kind"`
	ObservedAt time.Time `json:"observedAt"`
	Body map[string]any `json:"body,omitempty"`
}

type Capabilities struct {
	Backend string `json:"backend"`
	PTY bool `json:"pty"`
	MemoryEnforcement bool `json:"memoryEnforcement"`
	CPUQuotaEnforcement bool `json:"cpuQuotaEnforcement"`
	ProcessCountEnforcement bool `json:"processCountEnforcement"`
	MemoryTelemetry bool `json:"memoryTelemetry"`
	CPUTelemetry bool `json:"cpuTelemetry"`
	ProcessTelemetry bool `json:"processTelemetry"`
	RestartReconciliation string `json:"restartReconciliation"`
	Signals []string `json:"signals"`
}
