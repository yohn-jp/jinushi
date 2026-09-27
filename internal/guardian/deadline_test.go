package guardian

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

type deadlineProcess struct {
	graceCalls []time.Duration
}

func (*deadlineProcess) Ownership() model.Ownership        { return model.Ownership{Backend: "test"} }
func (*deadlineProcess) Wait() (backend.Exit, error)       { return backend.Exit{}, nil }
func (*deadlineProcess) Observe() (model.Resources, error) { return unavailableResources(), nil }
func (*deadlineProcess) Signal(string) error               { return nil }
func (p *deadlineProcess) Terminate(grace time.Duration) (backend.TerminationResult, error) {
	p.graceCalls = append(p.graceCalls, grace)
	return backend.TerminationResult{Requested: true, TreeEmpty: true}, nil
}
func (*deadlineProcess) WriteInput([]byte) error     { return nil }
func (*deadlineProcess) Resize(uint16, uint16) error { return nil }
func (*deadlineProcess) CloseInput() error           { return nil }

func TestGuardianDeadlinesUseConfiguredTerminationGrace(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		setup  func(*runState)
	}{
		{
			name:   "wall-time",
			reason: "timed-out",
			setup: func(s *runState) {
				s.descriptor.Spec.Limits.WallTimeMs = 1000
				s.startedMono = time.Now().Add(-2 * time.Second)
				now := time.Now().Add(-2 * time.Second).UTC()
				s.snapshot.StartedAt = &now
			},
		},
		{
			name:   "lease-expiry",
			reason: "lease-expired",
			setup: func(s *runState) {
				expiry := time.Now().Add(-time.Second).UTC()
				s.snapshot.LeaseExpiry = &expiry
				s.snapshot.LeaseGeneration = 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			spool, err := openSpool(filepath.Join(dir, "spool"), 3<<10)
			if err != nil {
				t.Fatal(err)
			}
			defer spool.close()
			process := &deadlineProcess{}
			state := &runState{
				descriptor: launchConfig{Dir: dir, TerminationGraceMs: 321},
				spool:      spool,
				process:    process,
				terminal:   make(chan struct{}),
				snapshot: Snapshot{
					Version: ProtocolVersion, RunID: "run_deadline", State: model.Running,
					Resources: unavailableResources(),
				},
			}
			test.setup(state)
			if err := state.persist(); err != nil {
				t.Fatal(err)
			}

			state.enforceDeadlines()
			if len(process.graceCalls) != 1 || process.graceCalls[0] != 321*time.Millisecond {
				t.Fatalf("termination grace calls = %v, want [321ms]", process.graceCalls)
			}
			snapshot, err := readSnapshot(dir)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.TerminationReason != test.reason || snapshot.State != model.Terminating {
				t.Fatalf("persisted deadline transition = %s/%s, want terminating/%s", snapshot.State, snapshot.TerminationReason, test.reason)
			}
		})
	}
}

var _ backend.Process = (*deadlineProcess)(nil)
