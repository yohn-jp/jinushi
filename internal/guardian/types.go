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
)

// Config is transient launch data. Spec, including its environment values,
// travels to the helper over stdin and is never written to the state dir.
type Config struct {
	RunID          string
	Dir            string
	Spec           model.RunSpec
	MaxOutputBytes int64
}

// Descriptor contains only a private state path for reconnect. Authentication
// material and the local endpoint are never exposed through this type.
type Descriptor struct {
	Version int    `json:"version"`
	RunID   string `json:"runId"`
	Dir     string `json:"dir"`
}

type privateDescriptor struct {
	Version int    `json:"version"`
	RunID   string `json:"runId"`
	Dir     string `json:"dir"`
	Token   string `json:"token"`
}

// Snapshot is durable physical evidence available even if the helper is no
// longer reachable. A running snapshot does not itself prove the current OS
// state; callers must reconnect or ask the backend to reconcile ownership.
type Snapshot struct {
	Version           int                       `json:"version"`
	RunID             string                    `json:"runId"`
	State             model.State               `json:"state"`
	Ownership         *model.Ownership          `json:"ownership,omitempty"`
	Resources         model.Resources           `json:"resources"`
	Output            model.Output              `json:"output"`
	StartedAt         *time.Time                `json:"startedAt,omitempty"`
	FinishedAt        *time.Time                `json:"finishedAt,omitempty"`
	Receipt           *model.Receipt            `json:"receipt,omitempty"`
	LimitOutcome      string                    `json:"limitOutcome,omitempty"`
	Termination       backend.TerminationResult `json:"termination"`
	TerminationReason string                    `json:"terminationReason,omitempty"`
	Reason            string                    `json:"reason,omitempty"`
	Live              bool                      `json:"-"`
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
	Stream        string `json:"stream"`
	Offset        int64  `json:"offset"`
	Data          []byte `json:"data,omitempty"`
	RetainedFrom  int64  `json:"retainedFrom"`
	RetainedBytes int64  `json:"retainedBytes"`
	ObservedBytes int64  `json:"observedBytes"`
	Gap           bool   `json:"gap"`
	Truncated     bool   `json:"truncated"`
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

func (h *Handle) ReadOutput(stream string, offset, limit int64) (Chunk, error) {
	return h.readOutput(stream, offset, limit)
}

func (h *Handle) Close() error { return nil }
