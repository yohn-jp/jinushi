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

func (g *guardedExecutor) runDir(id string) string { return filepath.Join(g.root, "runs", id) }

func (g *guardedExecutor) Start(id string, spec model.RunSpec, stdout, stderr io.Writer) (physical, error) {
	max := spec.Limits.OutputBytes
	if max == 0 {
		max = g.config.DefaultOutputBytes
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h, err := guardian.Start(ctx, g.executable, guardian.Config{RunID: id, Dir: g.runDir(id), Spec: spec, MaxOutputBytes: max})
	if h == nil {
		return nil, err
	}
	p := newGuardianPhysical(h, spec.Interactive, stdout, stderr)
	if snap, obErr := h.Observe(ctx); obErr == nil && snap.Ownership != nil {
		p.ownership = *snap.Ownership
	} else if obErr == nil && snap.State == model.Terminal && snap.Receipt != nil && snap.Receipt.Outcome == "startup-failed" && snap.Receipt.Cleanup == "complete" {
		return nil, fmt.Errorf("workload startup failed")
	}
	if err != nil {
		return p, err
	}
	if p.ownership.Backend == "" {
		return p, fmt.Errorf("guardian ownership unavailable after start")
	}
	return p, nil
}

func (g *guardedExecutor) Reconcile(id string, owned *model.Ownership, interactive bool, stdout, stderr io.Writer) (reconcileResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := guardian.Reattach(ctx, g.runDir(id), id)
	if err != nil {
		return reconcileResult{}, err
	}
	snap, err := h.Observe(ctx)
	if err != nil {
		return reconcileResult{}, err
	}
	if owned != nil && (snap.Ownership == nil || *snap.Ownership != *owned) {
		return reconcileResult{}, errors.New("durable ownership mismatch")
	}
	if snap.State == model.Terminal && snap.Receipt != nil && snap.Receipt.Cleanup == "complete" {
		p := newGuardianPhysical(h, interactive, stdout, stderr)
		if err := p.syncOutput(); err != nil {
			return reconcileResult{}, err
		}
		receipt := *snap.Receipt
		outcome := receipt.Outcome
		if snap.LimitOutcome != "" {
			outcome = snap.LimitOutcome
		}
		return reconcileResult{terminal: true, receipt: &receipt, exit: exitResult{code: receipt.ExitCode, signal: receipt.Signal, outcome: outcome}, ownership: snap.Ownership, state: snap.State, terminationReason: snap.TerminationReason}, nil
	}
	if snap.Ownership == nil {
		return reconcileResult{}, errors.New("guardian ownership absent")
	}
	liveSnap, err := h.Probe(ctx)
	if err != nil || !liveSnap.Live {
		return reconcileResult{}, errors.New("guardian is not live")
	}
	osEvidence, err := g.native.Reconcile(*snap.Ownership)
	if err != nil || !osEvidence.OwnershipProven || (osEvidence.State != model.Running && osEvidence.State != model.Terminating) {
		return reconcileResult{}, errors.New("OS ownership not proven")
	}
	p := newGuardianPhysical(h, interactive, stdout, stderr)
	p.ownership = *snap.Ownership
	if err := p.syncOutput(); err != nil {
		return reconcileResult{}, err
	}
	return reconcileResult{live: true, process: p, ownership: snap.Ownership, state: snap.State, terminationReason: snap.TerminationReason}, nil
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

func (p *guardianPhysical) syncOutput() error {
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
	for _, stream := range streams {
		if stream.writer == nil {
			continue
		}
		for {
			offset := p.offsets[stream.name]
			chunk, err := p.h.ReadOutput(stream.name, offset, 64<<10)
			if err != nil {
				return err
			}
			if chunk.Gap {
				if chunk.RetainedFrom <= offset {
					return fmt.Errorf("guardian output gap has no forward watermark")
				}
				if recorder, ok := stream.writer.(outputGapRecorder); ok {
					if err := recorder.RecordGap(chunk.RetainedFrom); err != nil {
						return err
					}
				}
				p.offsets[stream.name] = chunk.RetainedFrom
				continue
			}
			if len(chunk.Data) > 0 {
				n, err := stream.writer.Write(chunk.Data)
				if err != nil {
					return err
				}
				if n != len(chunk.Data) {
					return io.ErrShortWrite
				}
				p.offsets[stream.name] = offset + int64(n)
				continue
			}
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
	if err := p.syncOutput(); err != nil {
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
	if err := p.syncOutput(); err != nil {
		return model.Resources{}, err
	}
	return snap.Resources, nil
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
