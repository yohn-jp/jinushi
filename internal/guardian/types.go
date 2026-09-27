// Package guardian runs one OS backend inside a small helper process. The
// helper retains the backend handle, captures bounded output, and writes
// terminal evidence while the supervisor may be unavailable.
package guardian

import (
	"context"
	"errors"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

const (
	ProtocolVersion = 1
	maxConfigBytes  = 1 << 20
	maxRPCBytes     = 1 << 20
)

var (
	ErrUncertain       = errors.New("guardian: execution ownership or outcome is uncertain")
	ErrUnavailable     = errors.New("guardian: helper is unavailable")
	ErrAlreadyStarted  = errors.New("guardian: run directory already has a descriptor")
	ErrInvalidIdentity = errors.New("guardian: invalid run identity")
	ErrLeaseExpired    = errors.New("guardian: lease has expired")
	ErrStaleGeneration = errors.New("guardian: stale lease generation")
	ErrControlFailed   = errors.New("guardian: physical control failed")
	ErrControlConflict = errors.New("guardian: control request identity conflict")
)

// Config is transient launch data. Spec, including its environment values,
// travels to the helper over stdin and is never written to the state dir.
type Config struct {
	RunID              string
	Dir                string
	Spec               model.RunSpec
	HostEnvelope       HostEnvelopeConfig
	MaxOutputBytes     int64
	InitialLeaseExpiry *time.Time
	LeaseGeneration    uint64
	TerminationGraceMs int64
	SampleIntervalMs   int64
}

// HostEnvelopeConfig is the bounded, Linux-neutral launch projection for
// supervisor-level physical safety ceilings. The helper persists it with its
// launch evidence so the selected backend can apply the same envelope.
type HostEnvelopeConfig struct {
	MemoryBytes   int64 `json:"memoryBytes,omitempty"`
	TaskCount     int64 `json:"taskCount,omitempty"`
	MaxActiveRuns int64 `json:"maxActiveRuns,omitempty"`
}

func validateHostEnvelope(config HostEnvelopeConfig) error {
	if config.MemoryBytes < 0 || config.TaskCount < 0 || config.MaxActiveRuns < 0 {
		return errors.New("guardian: host envelope limits cannot be negative")
	}
	return nil
}

// Descriptor contains only a private state path for reconnect. Authentication
// material and the local endpoint are never exposed through this type.
type Descriptor struct {
	Version int    `json:"version"`
	RunID   string `json:"runId"`
	Dir     string `json:"dir"`
}

type privateDescriptor struct {
	Version      int                `json:"version"`
	RunID        string             `json:"runId"`
	Dir          string             `json:"dir"`
	Token        string             `json:"token"`
	HostEnvelope HostEnvelopeConfig `json:"hostEnvelope,omitempty"`
}

const maxControlEvidenceOperations = 256

// ControlEvidenceGap describes operations evicted after the request identity
// retention window. The Supervisor event journal remains the complete history
// authority; this bounded Guardian cache exists to reconcile pending calls.
type ControlEvidenceGap struct {
	ExpiredCount   uint64     `json:"expiredCount,omitempty"`
	ExpiredFrom    *time.Time `json:"expiredFrom,omitempty"`
	ExpiredThrough *time.Time `json:"expiredThrough,omitempty"`
	RetainedFrom   *time.Time `json:"retainedFrom,omitempty"`
}

// ControlOperation is a durable Guardian-side idempotency record. Pending is
// persisted before a physical effect; Completed records its verified result.
type ControlOperation struct {
	RequestID string                    `json:"requestId"`
	Digest    string                    `json:"digest"`
	Status    string                    `json:"status"`
	Evidence  model.ControlEventPayload `json:"evidence"`
	CreatedAt time.Time                 `json:"createdAt"`
	UpdatedAt time.Time                 `json:"updatedAt"`
}

// ControlState retains bounded request evidence and current effective values.
type ControlState struct {
	Paused          *bool              `json:"paused,omitempty"`
	MemoryHighBytes *int64             `json:"memoryHighBytes,omitempty"`
	CPUQuotaPercent *int64             `json:"cpuQuotaPercent,omitempty"`
	Operations      []ControlOperation `json:"operations,omitempty"`
	EvidenceGap     ControlEvidenceGap `json:"evidenceGap,omitempty"`
	UpdatedAt       time.Time          `json:"updatedAt,omitempty"`
}

// Snapshot is durable physical evidence available even if the helper is no
// longer reachable. A running snapshot does not itself prove the current OS
// state; callers must reconnect or ask the backend to reconcile ownership.
type Snapshot struct {
	Version                     int                       `json:"version"`
	RunID                       string                    `json:"runId"`
	State                       model.State               `json:"state"`
	Ownership                   *model.Ownership          `json:"ownership,omitempty"`
	EffectiveCapabilities       *model.Capabilities       `json:"effectiveCapabilities,omitempty"`
	Resources                   model.Resources           `json:"resources"`
	LastResourceSampleAt        *time.Time                `json:"lastResourceSampleAt,omitempty"`
	LastOutputAt                *time.Time                `json:"lastOutputAt,omitempty"`
	OutputLastWriteAt           map[string]time.Time      `json:"outputLastWriteAt,omitempty"`
	Output                      model.Output              `json:"output"`
	StartedAt                   *time.Time                `json:"startedAt,omitempty"`
	FinishedAt                  *time.Time                `json:"finishedAt,omitempty"`
	Receipt                     *model.Receipt            `json:"receipt,omitempty"`
	LimitOutcome                string                    `json:"limitOutcome,omitempty"`
	LeaseExpiry                 *time.Time                `json:"leaseExpiry,omitempty"`
	LeaseGeneration             uint64                    `json:"leaseGeneration"`
	LastLeaseExpectedGeneration uint64                    `json:"lastLeaseExpectedGeneration"`
	LastLeaseMs                 int64                     `json:"lastLeaseMs"`
	Termination                 backend.TerminationResult `json:"termination"`
	TerminationReason           string                    `json:"terminationReason,omitempty"`
	Reason                      string                    `json:"reason,omitempty"`
	Controls                    *ControlState             `json:"controls,omitempty"`
	Live                        bool                      `json:"-"`
}

// Evidence is an immutable completed execution receipt plus its final
// snapshot. It is produced only after the backend proves the owned tree empty.
type Evidence struct {
	Snapshot Snapshot      `json:"snapshot"`
	Exit     backend.Exit  `json:"exit"`
	Receipt  model.Receipt `json:"receipt"`
}

// Chunk is an absolute-offset output range. If Gap is true, some observed
// bytes in the requested history range were not retained.
type Chunk struct {
	Stream        string     `json:"stream"`
	Offset        int64      `json:"offset"`
	Data          []byte     `json:"data,omitempty"`
	LastWriteAt   *time.Time `json:"lastWriteAt,omitempty"`
	RetainedFrom  int64      `json:"retainedFrom"`
	RetainedBytes int64      `json:"retainedBytes"`
	ObservedBytes int64      `json:"observedBytes"`
	Gap           bool       `json:"gap"`
	Truncated     bool       `json:"truncated"`
}

// Handle reconnects to the helper with its private descriptor. Closing a
// Handle only releases client-side state; it never terminates the Run.
type Handle struct {
	descriptor privateDescriptor
	started    bool
	helperDone <-chan error
}

// BackendFactory is exposed in the helper entrypoint so the caller can select
// the platform backend without making guardian own OS process primitives.
type BackendFactory = backend.Factory

// Start launches the hidden helper and waits until backend ownership is
// durably recorded. If the helper starts but ownership cannot be proven,
// Start returns the Handle together with ErrUncertain so the caller can
// persist the descriptor and reconcile rather than report startup failure.
func Start(ctx context.Context, executable string, config Config) (*Handle, error) {
	return startHelper(ctx, executable, config)
}

// Reattach loads private helper credentials and durable evidence for an
// existing Run. It never starts or restarts a helper or workload.
func Reattach(ctx context.Context, dir, runID string) (*Handle, error) {
	return reattach(ctx, dir, runID)
}

// ServeFromArgs is called only by the hidden command mode. It reads one
// bounded transient launch config from stdin and starts one backend Run.
func ServeFromArgs(args []string, factory BackendFactory) error {
	return serveFromArgs(args, factory)
}

func (h *Handle) Descriptor() Descriptor {
	return Descriptor{Version: h.descriptor.Version, RunID: h.descriptor.RunID, Dir: h.descriptor.Dir}
}

func (h *Handle) Observe(ctx context.Context) (Snapshot, error) {
	return h.observe(ctx)
}

// Probe requires a live authenticated helper connection. A successful Probe
// is the only way to use a persisted running snapshot as current evidence.
func (h *Handle) Probe(ctx context.Context) (Snapshot, error) { return h.probe(ctx) }

func (h *Handle) Wait(ctx context.Context) (Evidence, error) { return h.wait(ctx) }

func (h *Handle) RenewLease(ctx context.Context, expectedGeneration uint64, leaseMs int64) (Snapshot, error) {
	return h.renewLease(ctx, expectedGeneration, leaseMs)
}

func (h *Handle) Signal(ctx context.Context, name string) error {
	return h.signal(ctx, name)
}

func (h *Handle) Terminate(ctx context.Context, grace time.Duration, reason string) (backend.TerminationResult, error) {
	return h.terminate(ctx, grace, reason)
}

func (h *Handle) WriteInput(ctx context.Context, data []byte) error {
	return h.writeInput(ctx, data)
}

func (h *Handle) CloseInput(ctx context.Context) error { return h.closeInput(ctx) }

func (h *Handle) Resize(ctx context.Context, rows, cols uint16) error {
	return h.resize(ctx, rows, cols)
}

// Pause requests a capability-gated physical freeze and returns durable
// evidence keyed by the caller's idempotent request identity.
func (h *Handle) Pause(ctx context.Context, requestID string) (model.ControlEventPayload, error) {
	return h.control(ctx, "control-pause", requestID, 0)
}

// Resume requests a capability-gated physical thaw.
func (h *Handle) Resume(ctx context.Context, requestID string) (model.ControlEventPayload, error) {
	return h.control(ctx, "control-resume", requestID, 0)
}

// SetMemoryHigh applies a positive finite soft memory threshold in bytes.
func (h *Handle) SetMemoryHigh(ctx context.Context, requestID string, bytes int64) (model.ControlEventPayload, error) {
	return h.control(ctx, "control-memory-high", requestID, bytes)
}

// SetCPUQuotaPercent applies a positive finite cgroup CPU rate limit.
func (h *Handle) SetCPUQuotaPercent(ctx context.Context, requestID string, percent int64) (model.ControlEventPayload, error) {
	return h.control(ctx, "control-cpu-quota", requestID, percent)
}

// LookupControl returns retained physical control evidence for a request ID.
// The gap reports whether older Guardian cache entries have expired.
func (h *Handle) LookupControl(ctx context.Context, requestID string) (ControlOperation, bool, ControlEvidenceGap, error) {
	return h.lookupControl(ctx, requestID)
}

func (h *Handle) ReadOutput(stream string, offset, limit int64) (Chunk, error) {
	return h.readOutput(stream, offset, limit)
}

func (h *Handle) Close() error { return nil }
