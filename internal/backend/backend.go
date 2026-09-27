// Package backend defines the operating-system boundary used by the
// supervisor. Implementations expose physical evidence and control only.
package backend

import (
	"io"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

// Factory creates a platform backend. Capability discovery happens on the
// returned instance so an unavailable optional OS feature does not prevent
// ordinary process execution.
type Factory func() Backend

// UncertainError means an OS child may have started, but cleanup or ownership
// could not be proven. Callers must preserve Ownership and must not treat this
// as a clean startup failure.
type UncertainError struct {
	Ownership model.Ownership `json:"ownership"`
	Err       error           `json:"-"`
}

func (e *UncertainError) Error() string {
	if e == nil || e.Err == nil {
		return "backend ownership is uncertain"
	}
	return "backend ownership is uncertain: " + e.Err.Error()
}

func (e *UncertainError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Backend starts, observes, controls, and reconciles owned executions.
type Backend interface {
	Capabilities() model.Capabilities
	Start(spec model.RunSpec, stdout, stderr io.Writer) (Process, error)
	Reconcile(ownership model.Ownership) (ReconcileResult, error)
}

// Process is a live execution handle. Wait returns only after the owned tree
// has reached a proven empty state, not merely after its original process exits.
type Process interface {
	Ownership() model.Ownership
	Wait() (Exit, error)
	Observe() (model.Resources, error)
	Signal(name string) error
	Terminate(grace time.Duration) (TerminationResult, error)
	WriteInput([]byte) error
	Resize(rows, cols uint16) error
	CloseInput() error
}

// LimitEvidence is an optional live capability for backends with native
// resource event counters. Empty means no limit trigger has been observed.
// Values use typed outcome names such as resource-limit:process-count.
type LimitEvidence interface {
	LimitOutcome() string
}

// Exit records the direct process result and the physical tree's final
// outcome. Outcome may identify an OS-enforced resource limit.
type Exit struct {
	ExitCode   *int      `json:"exitCode,omitempty"`
	Signal     string    `json:"signal,omitempty"`
	Outcome    string    `json:"outcome"`
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`
}

// TerminationResult reports the physical result of a bounded termination
// sequence. TreeEmpty is true only after the backend observed no live members.
type TerminationResult struct {
	Requested bool   `json:"requested"`
	Forced    bool   `json:"forced"`
	TreeEmpty bool   `json:"treeEmpty"`
	Outcome   string `json:"outcome,omitempty"`
}

// ReconcileResult contains OS evidence after restart. It never describes task
// success; callers combine it with persisted lifecycle evidence.
type ReconcileResult struct {
	State           model.State     `json:"state"`
	Resources       model.Resources `json:"resources"`
	OwnershipProven bool            `json:"ownershipProven"`
	Reason          string          `json:"reason,omitempty"`
}
