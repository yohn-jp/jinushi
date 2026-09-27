package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/guardian"
	"github.com/yohn-jp/jinushi/internal/model"
)

type guardedExecutor struct {
	root       string
	executable string
	native     backend.Backend
	config     Config
}

type cleanStartTerminal struct{ outcome string }

func (e *cleanStartTerminal) Error() string {
	return "workload finished before ownership establishment: " + e.outcome
}

func newGuardedExecutor(root string, config Config) (*guardedExecutor, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return &guardedExecutor{root: root, executable: exe, native: PlatformBackendFactory()(), config: config}, nil
}

func (g *guardedExecutor) Capabilities() model.Capabilities {
	c := g.native.Capabilities()
	c.RestartReconciliation = "guardian+" + c.RestartReconciliation
	return c
}

func (g *guardedExecutor) ValidateLimits(limits model.Limits) error {
	if validator, ok := g.native.(interface{ ValidateLimits(model.Limits) error }); ok {
		return validator.ValidateLimits(limits)
	}
	return nil
}

func (g *guardedExecutor) runDir(id string) string { return filepath.Join(g.root, "runs", id) }

func (g *guardedExecutor) Start(run model.Run, spec model.RunSpec, stdout, stderr io.Writer) (physical, error) {
	max := spec.Limits.OutputBytes
	if max == 0 {
		max = g.config.DefaultOutputBytes
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h, err := guardian.Start(ctx, g.executable, guardian.Config{
		RunID: run.ID, Dir: g.runDir(run.ID), Spec: spec, MaxOutputBytes: max,
		InitialLeaseExpiry: run.LeaseExpiry, LeaseGeneration: run.LeaseGeneration,
		TerminationGraceMs: g.config.TerminationGraceMs, SampleIntervalMs: g.config.SampleIntervalMs,
	})
	if h == nil {
		return nil, err
	}
	p := newGuardianPhysical(h, spec.Interactive, stdout, stderr)
	if snap, obErr := h.Observe(ctx); obErr == nil && snap.Ownership != nil {
		p.ownership = *snap.Ownership
	} else if obErr == nil && snap.State == model.Terminal && snap.Receipt != nil && snap.Receipt.Cleanup == "complete" {
		if snap.Receipt.Outcome == "startup-failed" {
			return nil, fmt.Errorf("workload startup failed")
		}
		return nil, &cleanStartTerminal{outcome: snap.Receipt.Outcome}
	}
	if err != nil {
		return p, err
	}
	if p.ownership.Backend == "" {
		return p, fmt.Errorf("guardian ownership unavailable after start")
	}
	return p, nil
}

func (g *guardedExecutor) Reconcile(run model.Run, stdout, stderr io.Writer) (reconcileResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := guardian.Reattach(ctx, g.runDir(run.ID), run.ID)
	if err != nil {
		return reconcileResult{}, err
	}
	snap, err := h.Observe(ctx)
	if err != nil {
		return reconcileResult{}, err
	}
	if run.Ownership != nil && (snap.Ownership == nil || *snap.Ownership != *run.Ownership) {
		return reconcileResult{}, errors.New("durable ownership mismatch")
	}
	if err := validateOutputCursor(run.Output, snap.Output, run.Spec.Interactive); err != nil {
		return reconcileResult{}, err
	}
	if snap.State == model.Uncertain && snap.Live && snap.Termination.Requested && snap.TerminationReason != "" && snap.Ownership != nil {
		osEvidence, osErr := g.native.Reconcile(*snap.Ownership)
		if osErr != nil || !osEvidence.OwnershipProven {
			return reconcileResult{}, errors.New("OS ownership is not proven for termination retry")
		}
		grace := time.Duration(g.config.TerminationGraceMs) * time.Millisecond
		retryCtx, retryCancel := context.WithTimeout(context.Background(), grace+15*time.Second)
		defer retryCancel()
		termination, retryErr := h.Terminate(retryCtx, grace, snap.TerminationReason)
		if retryErr != nil || !termination.TreeEmpty {
			return reconcileResult{}, errors.New("guardian termination retry did not prove cleanup")
		}
		evidence, waitErr := h.Wait(retryCtx)
		if waitErr != nil {
			return reconcileResult{}, errors.New("guardian termination retry lacks terminal receipt")
		}
		snap = evidence.Snapshot
	}
	if snap.State == model.Terminal && snap.Receipt != nil && snap.Receipt.Cleanup == "complete" {
		p := newReconciledGuardianPhysical(h, run, stdout, stderr)
		if err := p.syncOutput(0); err != nil {
			return reconcileResult{}, err
		}
		receipt := *snap.Receipt
		outcome := receipt.Outcome
		if snap.LimitOutcome != "" {
			outcome = snap.LimitOutcome
		}
		return reconcileResult{terminal: true, receipt: &receipt, exit: exitResult{code: receipt.ExitCode, signal: receipt.Signal, outcome: outcome}, ownership: snap.Ownership, state: snap.State, terminationReason: snap.TerminationReason, lease: leaseFromSnapshot(snap), resources: snap.Resources, lastSampleAt: snap.LastResourceSampleAt, lastOutputAt: snap.LastOutputAt}, nil
	}
	if snap.State != model.Running && snap.State != model.Terminating {
		return reconcileResult{}, errors.New("guardian has no provable live Run state")
	}
	if snap.Ownership == nil {
		return reconcileResult{}, errors.New("guardian ownership absent")
	}
	liveSnap, err := h.Probe(ctx)
	if err != nil || !liveSnap.Live {
		return reconcileResult{}, errors.New("guardian is not live")
	}
	osEvidence, err := g.native.Reconcile(*snap.Ownership)
	if err == nil && osEvidence.OwnershipProven && osEvidence.State == model.Terminal {
		// The tree can become empty between the guardian snapshot and native
		// observation. Give the live guardian a bounded chance to commit its
		// physical receipt before classifying the Run uncertain.
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer waitCancel()
		if _, waitErr := h.Wait(waitCtx); waitErr != nil {
			return reconcileResult{}, errors.New("owned tree is empty but guardian receipt is unavailable")
		}
		return g.Reconcile(run, stdout, stderr)
	}
	if err != nil || !osEvidence.OwnershipProven || (osEvidence.State != model.Running && osEvidence.State != model.Terminating) {
		return reconcileResult{}, errors.New("OS ownership not proven")
	}
	p := newReconciledGuardianPhysical(h, run, stdout, stderr)
	p.ownership = *snap.Ownership
	if err := p.syncOutput(16); err != nil {
		return reconcileResult{}, err
	}
	return reconcileResult{live: true, process: p, ownership: snap.Ownership, state: snap.State, terminationReason: snap.TerminationReason, lease: leaseFromSnapshot(snap), resources: snap.Resources, lastSampleAt: snap.LastResourceSampleAt, lastOutputAt: snap.LastOutputAt}, nil
}

func newReconciledGuardianPhysical(h *guardian.Handle, run model.Run, stdout, stderr io.Writer) *guardianPhysical {
	p := newGuardianPhysical(h, run.Spec.Interactive, stdout, stderr)
	if run.Spec.Interactive {
		p.offsets["pty"] = run.Output.PTY.ObservedBytes
	} else {
		p.offsets["stdout"] = run.Output.Stdout.ObservedBytes
		p.offsets["stderr"] = run.Output.Stderr.ObservedBytes
	}
	return p
}

func validateOutputCursor(stored, guardianOutput model.Output, interactive bool) error {
	if interactive {
		if stored.PTY.ObservedBytes > guardianOutput.PTY.ObservedBytes {
			return errors.New("durable PTY output cursor exceeds guardian observation")
		}
		return nil
	}
	if stored.Stdout.ObservedBytes > guardianOutput.Stdout.ObservedBytes || stored.Stderr.ObservedBytes > guardianOutput.Stderr.ObservedBytes {
		return errors.New("durable stdio output cursor exceeds guardian observation")
	}
	return nil
}

func leaseFromSnapshot(snap guardian.Snapshot) leaseState {
	return leaseState{expiry: snap.LeaseExpiry, generation: snap.LeaseGeneration, lastExpectedGeneration: snap.LastLeaseExpectedGeneration, lastMs: snap.LastLeaseMs}
}

type outputGapRecorder interface{ RecordGap(int64) error }

type guardianPhysical struct {
	mu          sync.Mutex
	h           *guardian.Handle
	ownership   model.Ownership
	interactive bool
	stdout      io.Writer
	stderr      io.Writer
	offsets     map[string]int64
	final       *guardian.Evidence
	reason      string
}

func newGuardianPhysical(h *guardian.Handle, interactive bool, stdout, stderr io.Writer) *guardianPhysical {
	return &guardianPhysical{h: h, interactive: interactive, stdout: stdout, stderr: stderr, offsets: map[string]int64{}}
}

func (p *guardianPhysical) Ownership() model.Ownership { return p.ownership }

// syncOutput reads one chunk per stream in each pass. Live observation uses a
// finite budget so a continuous producer cannot starve another stream or
// prevent the caller from observing the Run. Terminal reads drain the spool.
func (p *guardianPhysical) syncOutput(maxChunks int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	streams := []struct {
		name   string
		writer io.Writer
	}{{"stdout", p.stdout}, {"stderr", p.stderr}}
	if p.interactive {
		streams = []struct {
			name   string
			writer io.Writer
		}{{"pty", p.stdout}}
	}
	chunks := 0
	for {
		progress := false
		for _, stream := range streams {
			if stream.writer == nil {
				continue
			}
			if maxChunks > 0 && chunks >= maxChunks {
				return nil
			}
			offset := p.offsets[stream.name]
			chunk, err := p.h.ReadOutput(stream.name, offset, 64<<10)
			if err != nil {
				return err
			}
			var observedAt time.Time
			if chunk.LastWriteAt != nil {
				observedAt = *chunk.LastWriteAt
			}
			if chunk.Gap {
				if chunk.RetainedFrom <= offset {
					return fmt.Errorf("guardian output gap has no forward watermark")
				}
				if recorder, ok := stream.writer.(interface{ RecordGapObserved(int64, time.Time) error }); ok {
					if err := recorder.RecordGapObserved(chunk.RetainedFrom, observedAt); err != nil {
						return err
					}
				} else if recorder, ok := stream.writer.(outputGapRecorder); ok {
					if err := recorder.RecordGap(chunk.RetainedFrom); err != nil {
						return err
					}
				}
				p.offsets[stream.name] = chunk.RetainedFrom
				progress = true
				chunks++
				continue
			}
			if len(chunk.Data) > 0 {
				var n int
				if writer, ok := stream.writer.(interface {
					WriteObserved([]byte, time.Time) (int, error)
				}); ok {
					n, err = writer.WriteObserved(chunk.Data, observedAt)
				} else {
					n, err = stream.writer.Write(chunk.Data)
				}
				if err != nil {
					return err
				}
				if n != len(chunk.Data) {
					return io.ErrShortWrite
				}
				p.offsets[stream.name] = offset + int64(n)
				progress = true
				chunks++
				continue
			}
		}
		if !progress {
			break
		}
	}
	return nil
}

func (p *guardianPhysical) Wait() (exitResult, error) {
	evidence, err := p.h.Wait(context.Background())
	if err != nil {
		return exitResult{}, err
	}
	if err := p.syncOutput(0); err != nil {
		return exitResult{}, err
	}
	p.mu.Lock()
	p.final = &evidence
	p.mu.Unlock()
	outcome := evidence.Exit.Outcome
	if evidence.Snapshot.LimitOutcome != "" {
		outcome = evidence.Snapshot.LimitOutcome
	}
	return exitResult{code: evidence.Exit.ExitCode, signal: evidence.Exit.Signal, outcome: outcome, startedAt: evidence.Exit.StartedAt, finishedAt: evidence.Exit.FinishedAt, terminationRequested: evidence.Receipt.TerminationRequested, forced: evidence.Receipt.Forced}, nil
}

func (p *guardianPhysical) Observe() (model.Resources, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snap, err := p.h.Observe(ctx)
	if err != nil {
		return model.Resources{}, err
	}
	if err := p.syncOutput(16); err != nil {
		return model.Resources{}, err
	}
	return snap.Resources, nil
}

func (p *guardianPhysical) RenewLease(expectedGeneration uint64, leaseMs int64) (leaseState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	snapshot, err := p.h.RenewLease(ctx, expectedGeneration, leaseMs)
	if err != nil {
		return leaseState{}, err
	}
	return leaseFromSnapshot(snapshot), nil
}

func (p *guardianPhysical) Signal(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.h.Signal(ctx, name)
}

func (p *guardianPhysical) Terminate(grace time.Duration) (terminationResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), grace+5*time.Second)
	defer cancel()
	p.mu.Lock()
	reason := p.reason
	p.mu.Unlock()
	if reason == "" {
		reason = "supervisor"
	}
	result, err := p.h.Terminate(ctx, grace, reason)
	return terminationResult{complete: result.TreeEmpty, forced: result.Forced, outcome: result.Outcome}, err
}

func (p *guardianPhysical) SetTerminationReason(reason string) {
	p.mu.Lock()
	p.reason = reason
	p.mu.Unlock()
}

func (p *guardianPhysical) WriteInput(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.h.WriteInput(ctx, data)
}

func (p *guardianPhysical) Resize(rows, cols uint16) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.h.Resize(ctx, rows, cols)
}

func (p *guardianPhysical) CloseInput() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.h.CloseInput(ctx)
}
