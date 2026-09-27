package supervisor

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

const (
	defaultOutputBytes = 1 << 20
	maxOutputBytes     = 64 << 20
	maxWallTime        = 7 * 24 * time.Hour
	sampleInterval     = 250 * time.Millisecond
	terminationGrace   = 2 * time.Second
)

type exitResult struct {
	code       *int
	signal     string
	outcome    string
	startedAt  time.Time
	finishedAt time.Time
}

type terminationResult struct {
	complete bool
	forced   bool
	outcome  string
}

type reconcileResult struct {
	live     bool
	terminal bool
	exit     exitResult
	process  physical
	receipt  *model.Receipt
}

type physical interface {
	Ownership() model.Ownership
	Wait() (exitResult, error)
	Observe() (model.Resources, error)
	Signal(string) error
	Terminate(time.Duration) (terminationResult, error)
	WriteInput([]byte) error
	Resize(uint16, uint16) error
	CloseInput() error
}

type executor interface {
	Capabilities() model.Capabilities
	Start(string, model.RunSpec, io.Writer, io.Writer) (physical, error)
	Reconcile(string, model.Ownership, bool, io.Writer, io.Writer) (reconcileResult, error)
}

type active struct {
	mu          sync.Mutex
	run         model.Run
	spec        model.RunSpec
	process     physical
	done        chan struct{}
	terminating bool
	forced      bool
	attachments map[string]time.Time
}

type Service struct {
	store     *store.Store
	backend   executor
	root      string
	mu        sync.RWMutex
	active    map[string]*active
	stop      chan struct{}
	closeOnce sync.Once
}

func newService(root string, db *store.Store, backend executor) *Service {
	return &Service{root: root, store: db, backend: backend, active: make(map[string]*active), stop: make(chan struct{})}
}

func newID() (string, error) {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "run_" + hex.EncodeToString(b[:]), nil
}

func failure(code, message string) protocol.Response {
	return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: code, Message: message}}
}

func response() protocol.Response { return protocol.Response{Version: model.ProtocolVersion} }

func initialResources(c model.Capabilities) model.Resources {
	metric := func(supported bool) model.Metric {
		if supported {
			return model.Metric{Status: "unavailable"}
		}
		return model.Metric{Status: "unsupported"}
	}
	return model.Resources{MemoryBytes: metric(c.MemoryTelemetry), PeakMemoryBytes: metric(c.MemoryTelemetry), CPUTimeNs: metric(c.CPUTelemetry), ProcessCount: metric(c.ProcessTelemetry), PeakProcessCount: metric(c.ProcessTelemetry), SampleIntervalMs: int64(sampleInterval / time.Millisecond)}
}

func normalizeResources(r model.Resources, c model.Capabilities) model.Resources {
	defaults := initialResources(c)
	if r.MemoryBytes.Status == "" {
		r.MemoryBytes = defaults.MemoryBytes
	}
	if r.PeakMemoryBytes.Status == "" {
		r.PeakMemoryBytes = defaults.PeakMemoryBytes
	}
	if r.CPUTimeNs.Status == "" {
		r.CPUTimeNs = defaults.CPUTimeNs
	}
	if r.ProcessCount.Status == "" {
		r.ProcessCount = defaults.ProcessCount
	}
	if r.PeakProcessCount.Status == "" {
		r.PeakProcessCount = defaults.PeakProcessCount
	}
	return r
}

func publicRun(run model.Run) model.Run {
	run.Spec.Environment.Set = nil
	run.Spec.Environment.Unset = nil
	if run.Ownership != nil {
		own := *run.Ownership
		own.Token = ""
		run.Ownership = &own
	}
	return run
}

func validSpec(spec *model.RunSpec, caps model.Capabilities) *protocol.Failure {
	if spec == nil || len(spec.Argv) == 0 || spec.Argv[0] == "" || len(spec.Argv) > 256 {
		return &protocol.Failure{"invalid-request", "argv must contain an executable and at most 256 arguments"}
	}
	for _, arg := range spec.Argv {
		if len(arg) > 32768 || strings.ContainsRune(arg, 0) {
			return &protocol.Failure{"invalid-request", "invalid argv element"}
		}
	}
	if !filepath.IsAbs(spec.Cwd) {
		return &protocol.Failure{"invalid-request", "cwd must be absolute"}
	}
	if len(spec.Cwd) > 4096 {
		return &protocol.Failure{"invalid-request", "cwd too long"}
	}
	if spec.Environment.Mode != "" && spec.Environment.Mode != "inherit-supervisor" && spec.Environment.Mode != "replace" {
		return &protocol.Failure{"invalid-request", "invalid environment mode"}
	}
	if len(spec.Environment.Set) > 128 || len(spec.Environment.Unset) > 128 {
		return &protocol.Failure{"invalid-request", "environment too large"}
	}
	for k, v := range spec.Environment.Set {
		if k == "" || strings.ContainsAny(k, "=\x00") || len(k) > 256 || len(v) > 32768 || strings.ContainsRune(v, 0) {
			return &protocol.Failure{"invalid-request", "invalid environment entry"}
		}
	}
	for _, k := range spec.Environment.Unset {
		if k == "" || strings.ContainsAny(k, "=\x00") || len(k) > 256 {
			return &protocol.Failure{"invalid-request", "invalid environment key"}
		}
	}
	if spec.Lifetime.Mode == "" {
		spec.Lifetime.Mode = "detached"
	}
	if spec.Lifetime.Mode != "detached" && spec.Lifetime.Mode != "lease-bound" {
		return &protocol.Failure{"invalid-request", "invalid lifetime mode"}
	}
	if spec.Lifetime.Mode == "lease-bound" && (spec.Lifetime.LeaseMs < 1000 || spec.Lifetime.LeaseMs > int64(maxWallTime/time.Millisecond)) {
		return &protocol.Failure{"invalid-request", "invalid lease duration"}
	}
	if spec.Limits.MemoryBytes < 0 || spec.Limits.CPUQuotaPercent < 0 || spec.Limits.ProcessCount < 0 || spec.Limits.WallTimeMs < 0 || spec.Limits.OutputBytes < 0 {
		return &protocol.Failure{"invalid-request", "negative limit"}
	}
	if spec.Limits.OutputBytes > maxOutputBytes || spec.Limits.WallTimeMs > int64(maxWallTime/time.Millisecond) {
		return &protocol.Failure{"invalid-request", "limit exceeds supervisor ceiling"}
	}
	if spec.Interactive && !caps.PTY {
		return &protocol.Failure{"unsupported-capability", "PTY unavailable"}
	}
	if spec.Limits.MemoryBytes > 0 && !caps.MemoryEnforcement {
		return &protocol.Failure{"unsupported-capability", "memory enforcement unavailable"}
	}
	if spec.Limits.CPUQuotaPercent > 0 && !caps.CPUQuotaEnforcement {
		return &protocol.Failure{"unsupported-capability", "CPU quota enforcement unavailable"}
	}
	if spec.Limits.ProcessCount > 0 && !caps.ProcessCountEnforcement {
		return &protocol.Failure{"unsupported-capability", "process-count enforcement unavailable"}
	}
	if len(spec.Correlation) > 16 {
		return &protocol.Failure{"invalid-request", "too many correlation labels"}
	}
	for k, v := range spec.Correlation {
		if len(k) == 0 || len(k) > 64 || len(v) > 256 {
			return &protocol.Failure{"invalid-request", "invalid correlation label"}
		}
	}
	return nil
}

func (s *Service) Handle(ctx context.Context, req protocol.Request) protocol.Response {
	if req.Version != model.ProtocolVersion {
		return failure("unsupported-version", "protocol version 1 required")
	}
	switch req.Op {
	case "status":
		return response()
	case "capabilities":
		caps := s.backend.Capabilities()
		out := response()
		out.Capabilities = &caps
		return out
	case "run":
		return s.create(req.Spec)
	case "list":
		runs, err := s.store.List()
		if err != nil {
			return failure("storage-failure", err.Error())
		}
		for i := range runs {
			runs[i] = publicRun(runs[i])
		}
		out := response()
		out.Runs = runs
		return out
	case "inspect":
		return s.inspect(req.RunID)
	case "await":
		return s.await(ctx, req.RunID)
	case "events":
		return s.events(req)
	case "output":
		return s.output(req)
	case "attach":
		return s.attach(req)
	case "detach":
		return s.detach(req)
	case "input":
		return s.input(req)
	case "resize":
		return s.resize(req)
	case "signal":
		return s.signal(req)
	case "cancel":
		return s.cancel(req.RunID, "cancelled")
	case "lease-renew":
		return s.renew(req)
	default:
		return failure("invalid-request", "unknown operation")
	}
}

func (s *Service) create(spec *model.RunSpec) protocol.Response {
	if f := validSpec(spec, s.backend.Capabilities()); f != nil {
		return protocol.Response{Version: model.ProtocolVersion, Error: f}
	}
	if _, err := os.Stat(spec.Cwd); err != nil {
		return failure("cwd-failure", "cwd unavailable")
	}
	if spec.ParentRunID != "" {
		if _, err := s.store.Get(spec.ParentRunID); err != nil {
			return failure("invalid-request", "parent Run not found")
		}
	}
	id, err := newID()
	if err != nil {
		return failure("backend-failure", "cannot create Run identity")
	}
	now := time.Now().UTC()
	// Environment values are needed only in memory until spawn. They are not
	// persisted or returned by normal Run observation.
	publicSpec := *spec
	publicSpec.Environment.Set = nil
	publicSpec.Environment.Unset = nil
	run := model.Run{ID: id, Spec: publicSpec, State: model.Accepted, Generation: 1, CreatedAt: now, Resources: initialResources(s.backend.Capabilities())}
	run.Output.HistoryComplete = true
	if spec.Lifetime.Mode == "lease-bound" {
		expiry := now.Add(time.Duration(spec.Lifetime.LeaseMs) * time.Millisecond)
		run.LeaseExpiry = &expiry
		run.LeaseGeneration = 1
	}
	if _, _, err = s.store.Create(run, &model.Event{Kind: "run.accepted", ObservedAt: now}); err != nil {
		return failure("storage-failure", err.Error())
	}
	a := &active{run: run, spec: *spec, done: make(chan struct{})}
	s.mu.Lock()
	s.active[id] = a
	s.mu.Unlock()
	go s.start(a)
	out := response()
	clean := publicRun(run)
	out.Run = &clean
	return out
}

func (s *Service) transition(a *active, state model.State, kind string, body map[string]any) error {
	next := a.run
	next.State = state
	next.Generation++
	if _, err := s.store.Update(next, &model.Event{Kind: kind, ObservedAt: time.Now().UTC(), Body: body}); err != nil {
		return err
	}
	a.run = next
	return nil
}

func (s *Service) start(a *active) {
	a.mu.Lock()
	if a.terminating {
		a.mu.Unlock()
		s.finish(a, "cancelled", exitResult{}, false, "complete")
		return
	}
	if err := s.transition(a, model.Starting, "run.starting", nil); err != nil {
		a.mu.Unlock()
		s.markUncertain(a, "storage failure before spawn")
		return
	}
	a.mu.Unlock()
	stdout := &capture{s: s, a: a, stream: "stdout"}
	stderr := &capture{s: s, a: a, stream: "stderr"}
	if a.spec.Interactive {
		stdout.stream = "pty"
	}
	p, err := s.backend.Start(a.run.ID, a.spec, stdout, stderr)
	if err != nil {
		if p != nil {
			a.mu.Lock()
			a.process = p
			own := p.Ownership()
			a.run.Ownership = &own
			a.mu.Unlock()
			s.markUncertain(a, "ownership unproven after spawn")
			return
		}
		s.finish(a, "startup-failed", exitResult{}, false, "complete")
		return
	}
	a.mu.Lock()
	a.process = p
	own := p.Ownership()
	a.run.Ownership = &own
	now := time.Now().UTC()
	a.run.StartedAt = &now
	state, kind := model.Running, "run.running"
	if a.terminating {
		state, kind = model.Terminating, "run.owned"
	}
	if err := s.transition(a, state, kind, map[string]any{"pid": own.PID}); err != nil {
		a.mu.Unlock()
		p.Terminate(terminationGrace)
		s.markUncertain(a, "storage failure after spawn")
		return
	}
	shouldTerminate := a.terminating
	a.mu.Unlock()
	go s.monitor(a)
	if shouldTerminate {
		go s.driveTermination(a, p)
	}
}

func (s *Service) monitor(a *active) {
	waitCh := make(chan struct {
		result exitResult
		err    error
	}, 1)
	go func() {
		x, e := a.process.Wait()
		waitCh <- struct {
			result exitResult
			err    error
		}{x, e}
	}()
	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case w := <-waitCh:
			if w.err != nil {
				s.markUncertain(a, "wait failed")
				return
			}
			s.sample(a)
			termination, err := a.process.Terminate(terminationGrace)
			if err != nil || !termination.complete {
				s.markUncertain(a, "process tree cleanup unproven")
				return
			}
			a.mu.Lock()
			reason := a.run.TerminationReason
			forced := a.forced || termination.forced
			a.mu.Unlock()
			outcome := "exited"
			if w.result.signal != "" {
				outcome = "signaled"
			}
			limitOutcome := w.result.outcome
			if limitOutcome == "" {
				limitOutcome = termination.outcome
			}
			if strings.HasPrefix(limitOutcome, "resource-limit:") {
				outcome = "resource-limit"
				_, _ = s.store.AppendEvent(a.run.ID, model.Event{Kind: "limit.reached", ObservedAt: time.Now().UTC(), Body: map[string]any{"limit": strings.TrimPrefix(limitOutcome, "resource-limit:")}})
			}
			if reason != "" {
				outcome = reason
			}
			if outcome == "lease-expired" {
				outcome = "cancelled"
			}
			s.finish(a, outcome, w.result, forced, "complete")
			return
		case <-ticker.C:
			s.sample(a)
			s.sweepAttachments(a)
			a.mu.Lock()
			run := a.run
			already := a.terminating
			a.mu.Unlock()
			if already {
				continue
			}
			if run.Spec.Limits.WallTimeMs > 0 && run.StartedAt != nil && time.Since(*run.StartedAt) >= time.Duration(run.Spec.Limits.WallTimeMs)*time.Millisecond {
				go s.terminate(a, "timed-out")
				continue
			}
			if run.LeaseExpiry != nil && time.Now().After(*run.LeaseExpiry) {
				go s.terminate(a, "lease-expired")
			}
		}
	}
}

func (s *Service) sample(a *active) {
	a.mu.Lock()
	if a.process == nil || a.run.State == model.Terminal || a.run.State == model.Uncertain {
		a.mu.Unlock()
		return
	}
	p := a.process
	a.mu.Unlock()
	r, err := p.Observe()
	if err != nil {
		a.mu.Lock()
		if a.run.State != model.Terminal && a.run.State != model.Uncertain {
			if a.run.Resources.MemoryBytes.Status != "unsupported" {
				a.run.Resources.MemoryBytes = model.Metric{Status: "unavailable"}
			}
			if a.run.Resources.CPUTimeNs.Status != "unsupported" {
				a.run.Resources.CPUTimeNs = model.Metric{Status: "unavailable"}
			}
			if a.run.Resources.ProcessCount.Status != "unsupported" {
				a.run.Resources.ProcessCount = model.Metric{Status: "unavailable"}
			}
			_, _ = s.store.Update(a.run, &model.Event{Kind: "resource.unavailable", ObservedAt: time.Now().UTC()})
		}
		a.mu.Unlock()
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.run.State == model.Terminal || a.run.State == model.Uncertain {
		return
	}
	r = normalizeResources(r, s.backend.Capabilities())
	r.SampleIntervalMs = int64(sampleInterval / time.Millisecond)
	if r.CPUTimeNs.Status == "measured" && a.run.Resources.CPUTimeNs.Status == "measured" && r.CPUTimeNs.Value > a.run.Resources.CPUTimeNs.Value {
		now := time.Now().UTC()
		a.run.LastCPUActivityAt = &now
	}
	if r.ProcessCount.Status == "measured" && a.run.Resources.ProcessCount.Status == "measured" && r.ProcessCount.Value != a.run.Resources.ProcessCount.Value {
		now := time.Now().UTC()
		a.run.LastProcessChangeAt = &now
	}
	if r.PeakMemoryBytes.Status != "measured" && r.MemoryBytes.Status == "measured" {
		r.PeakMemoryBytes = r.MemoryBytes
	}
	if a.run.Resources.PeakMemoryBytes.Status == "measured" && r.PeakMemoryBytes.Value < a.run.Resources.PeakMemoryBytes.Value {
		r.PeakMemoryBytes = a.run.Resources.PeakMemoryBytes
	}
	if r.PeakProcessCount.Status != "measured" && r.ProcessCount.Status == "measured" {
		r.PeakProcessCount = r.ProcessCount
	}
	if a.run.Resources.PeakProcessCount.Status == "measured" && r.PeakProcessCount.Value < a.run.Resources.PeakProcessCount.Value {
		r.PeakProcessCount = a.run.Resources.PeakProcessCount
	}
	a.run.Resources = r
	_, _ = s.store.Update(a.run, &model.Event{Kind: "resource.sample", ObservedAt: time.Now().UTC(), Body: map[string]any{"resources": r}})
}

func (s *Service) finish(a *active, outcome string, exit exitResult, forced bool, cleanup string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.run.State == model.Terminal || a.run.State == model.Uncertain {
		return
	}
	a.run.Attachments = 0
	now := time.Now().UTC()
	if !exit.finishedAt.IsZero() {
		now = exit.finishedAt.UTC()
	}
	if !exit.startedAt.IsZero() {
		started := exit.startedAt.UTC()
		a.run.StartedAt = &started
	}
	a.run.FinishedAt = &now
	a.run.Receipt = &model.Receipt{Version: model.ProtocolVersion, RunID: a.run.ID, Outcome: outcome, ExitCode: exit.code, Signal: exit.signal, StartedAt: a.run.StartedAt, FinishedAt: now, Resources: a.run.Resources, Output: a.run.Output, TerminationRequested: a.run.TerminationReason != "", Forced: forced, Cleanup: cleanup}
	if err := s.transition(a, model.Terminal, "run.terminal", map[string]any{"outcome": outcome}); err != nil {
		a.run.State = model.Uncertain
		close(a.done)
		s.mu.Lock()
		delete(s.active, a.run.ID)
		s.mu.Unlock()
		return
	}
	close(a.done)
	s.mu.Lock()
	delete(s.active, a.run.ID)
	s.mu.Unlock()
}

func (s *Service) markUncertain(a *active, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.run.State == model.Terminal || a.run.State == model.Uncertain {
		return
	}
	a.run.Attachments = 0
	_ = s.transition(a, model.Uncertain, "run.uncertain", map[string]any{"reason": reason})
	close(a.done)
	s.mu.Lock()
	delete(s.active, a.run.ID)
	s.mu.Unlock()
}

type capture struct {
	s      *Service
	a      *active
	stream string
}

func (w *capture) RecordGap(observed int64) error {
	w.a.mu.Lock()
	defer w.a.mu.Unlock()
	if err := w.s.store.RecordOutputGap(w.a.run.ID, w.stream, observed); err != nil {
		return err
	}
	meta := model.OutputStream{ObservedBytes: observed, RetainedFrom: observed, Truncated: true}
	if w.stream == "stdout" {
		w.a.run.Output.Stdout = meta
	} else if w.stream == "stderr" {
		w.a.run.Output.Stderr = meta
	} else {
		w.a.run.Output.PTY = meta
	}
	w.a.run.Output.HistoryComplete = false
	_, err := w.s.store.Update(w.a.run, &model.Event{Kind: "output.gap", ObservedAt: time.Now().UTC(), Body: map[string]any{"stream": w.stream, "observedBytes": observed}})
	return err
}

func (w *capture) Write(data []byte) (int, error) {
	w.a.mu.Lock()
	defer w.a.mu.Unlock()
	max := w.a.run.Spec.Limits.OutputBytes
	if max == 0 {
		max = defaultOutputBytes
	}
	meta, err := w.s.store.AppendOutput(w.a.run.ID, w.stream, data, max)
	if err != nil {
		return 0, err
	}
	if w.stream == "stdout" {
		w.a.run.Output.Stdout = meta
	} else if w.stream == "stderr" {
		w.a.run.Output.Stderr = meta
	} else {
		w.a.run.Output.PTY = meta
	}
	w.a.run.Output.HistoryComplete = !(w.a.run.Output.Stdout.Truncated || w.a.run.Output.Stderr.Truncated || w.a.run.Output.PTY.Truncated)
	now := time.Now().UTC()
	w.a.run.LastOutputAt = &now
	_, err = w.s.store.Update(w.a.run, &model.Event{Kind: "output.chunk", ObservedAt: now, Body: map[string]any{"stream": w.stream, "bytes": len(data)}})
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

func (s *Service) inspect(id string) protocol.Response {
	run, err := s.store.Get(id)
	if err != nil {
		return failure("run-not-found", "Run not found")
	}
	out := response()
	clean := publicRun(run)
	out.Run = &clean
	return out
}

func (s *Service) await(ctx context.Context, id string) protocol.Response {
	for {
		run, err := s.store.Get(id)
		if err != nil {
			return failure("run-not-found", "Run not found")
		}
		if run.State == model.Terminal || run.State == model.Uncertain {
			out := response()
			clean := publicRun(run)
			out.Run = &clean
			return out
		}
		s.mu.RLock()
		a := s.active[id]
		s.mu.RUnlock()
		if a == nil {
			return failure("ownership-uncertain", "Run has no live supervisor owner")
		}
		select {
		case <-ctx.Done():
			return failure("await-cancelled", "await cancelled")
		case <-a.done:
		}
	}
}

func (s *Service) events(req protocol.Request) protocol.Response {
	if req.Limit <= 0 || req.Limit > 1000 {
		req.Limit = 100
	}
	events, from, gap, err := s.store.Events(req.RunID, req.After, int(req.Limit))
	if err != nil {
		return failure("run-not-found", "Run not found")
	}
	out := response()
	out.Events = events
	out.RetainedFrom = from
	out.Gap = gap
	return out
}

func (s *Service) output(req protocol.Request) protocol.Response {
	if req.AttachID != "" {
		if err := s.renewAttachment(req.RunID, req.AttachID); err != nil {
			return failure("attachment-expired", err.Error())
		}
	}
	stream := req.Stream
	if stream == "" {
		stream = "stdout"
	}
	if stream != "stdout" && stream != "stderr" && stream != "pty" {
		return failure("invalid-request", "invalid stream")
	}
	if req.Limit <= 0 || req.Limit > 65536 {
		req.Limit = 65536
	}
	data, from, observed, gap, err := s.store.ReadOutput(req.RunID, stream, req.Offset, int(req.Limit))
	if err != nil {
		return failure("run-not-found", "Run or output not found")
	}
	out := response()
	out.Data = base64.StdEncoding.EncodeToString(data)
	out.Gap = gap
	out.RetainedFrom = uint64(from)
	if run, err := s.store.Get(req.RunID); err == nil {
		clean := publicRun(run)
		out.Run = &clean
	}
	_ = observed
	return out
}

func (s *Service) attach(req protocol.Request) protocol.Response {
	a, out := s.lookupActive(req.RunID)
	if a == nil {
		return out
	}
	a.mu.Lock()
	if !a.run.Spec.Interactive {
		a.mu.Unlock()
		return failure("invalid-request", "Run is not interactive")
	}
	if a.process == nil {
		a.mu.Unlock()
		return failure("backend-failure", "Run not started")
	}
	id, err := newID()
	if err != nil {
		a.mu.Unlock()
		return failure("backend-failure", "cannot create attachment")
	}
	id = "att_" + strings.TrimPrefix(id, "run_")
	next := a.run
	next.Attachments++
	if _, err := s.store.Update(next, &model.Event{Kind: "pty.attached", ObservedAt: time.Now().UTC()}); err != nil {
		a.mu.Unlock()
		return failure("storage-failure", err.Error())
	}
	a.run = next
	if a.attachments == nil {
		a.attachments = make(map[string]time.Time)
	}
	a.attachments[id] = time.Now().Add(10 * time.Second)
	a.mu.Unlock()
	req.Stream = "pty"
	out = s.output(req)
	out.AttachID = id
	return out
}

func (s *Service) detach(req protocol.Request) protocol.Response {
	if req.AttachID == "" {
		return failure("invalid-request", "attachId required")
	}
	s.mu.RLock()
	a := s.active[req.RunID]
	s.mu.RUnlock()
	if a == nil {
		return response()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.attachments[req.AttachID]; !ok {
		return response()
	}
	next := a.run
	next.Attachments--
	if _, err := s.store.Update(next, &model.Event{Kind: "pty.detached", ObservedAt: time.Now().UTC()}); err != nil {
		return failure("storage-failure", err.Error())
	}
	a.run = next
	delete(a.attachments, req.AttachID)
	return response()
}

func (s *Service) renewAttachment(runID, id string) error {
	s.mu.RLock()
	a := s.active[runID]
	s.mu.RUnlock()
	if a == nil {
		return errors.New("attachment unavailable")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	expiry, ok := a.attachments[id]
	if !ok || time.Now().After(expiry) {
		return errors.New("attachment expired")
	}
	a.attachments[id] = time.Now().Add(10 * time.Second)
	return nil
}

func (s *Service) sweepAttachments(a *active) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.attachments) == 0 {
		return
	}
	now := time.Now()
	expired := 0
	for _, expiry := range a.attachments {
		if now.After(expiry) {
			expired++
		}
	}
	if expired == 0 {
		return
	}
	next := a.run
	next.Attachments -= expired
	if _, err := s.store.Update(next, &model.Event{Kind: "pty.attachment-expired", ObservedAt: now.UTC(), Body: map[string]any{"count": expired}}); err != nil {
		return
	}
	a.run = next
	for id, expiry := range a.attachments {
		if now.After(expiry) {
			delete(a.attachments, id)
		}
	}
}

func (s *Service) lookupActive(id string) (*active, protocol.Response) {
	s.mu.RLock()
	a := s.active[id]
	s.mu.RUnlock()
	if a != nil {
		return a, response()
	}
	run, err := s.store.Get(id)
	if err != nil {
		return nil, failure("run-not-found", "Run not found")
	}
	if run.State == model.Terminal {
		return nil, failure("already-terminal", "Run is terminal")
	}
	return nil, failure("ownership-uncertain", "Run ownership unavailable")
}

func (s *Service) input(req protocol.Request) protocol.Response {
	if req.AttachID != "" {
		if err := s.renewAttachment(req.RunID, req.AttachID); err != nil {
			return failure("attachment-expired", err.Error())
		}
	}
	a, out := s.lookupActive(req.RunID)
	if a == nil {
		return out
	}
	if len(req.Data) > 90000 {
		return failure("invalid-request", "input too large")
	}
	data, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil || len(data) > 65536 {
		return failure("invalid-request", "invalid base64 input")
	}
	a.mu.Lock()
	p := a.process
	a.mu.Unlock()
	if p == nil {
		return failure("backend-failure", "Run not started")
	}
	if err := p.WriteInput(data); err != nil {
		return failure("backend-failure", err.Error())
	}
	return response()
}

func (s *Service) resize(req protocol.Request) protocol.Response {
	if req.AttachID != "" {
		if err := s.renewAttachment(req.RunID, req.AttachID); err != nil {
			return failure("attachment-expired", err.Error())
		}
	}
	a, out := s.lookupActive(req.RunID)
	if a == nil {
		return out
	}
	if req.Rows < 1 || req.Cols < 1 || req.Rows > 65535 || req.Cols > 65535 {
		return failure("invalid-request", "invalid terminal size")
	}
	a.mu.Lock()
	p := a.process
	a.mu.Unlock()
	if p == nil {
		return failure("backend-failure", "Run not started")
	}
	if err := p.Resize(uint16(req.Rows), uint16(req.Cols)); err != nil {
		return failure("unsupported-capability", err.Error())
	}
	return response()
}

func (s *Service) signal(req protocol.Request) protocol.Response {
	a, out := s.lookupActive(req.RunID)
	if a == nil {
		return out
	}
	a.mu.Lock()
	p := a.process
	a.mu.Unlock()
	if p == nil {
		return failure("backend-failure", "Run not started")
	}
	if err := p.Signal(req.Signal); err != nil {
		return failure("backend-failure", err.Error())
	}
	_, _ = s.store.AppendEvent(req.RunID, model.Event{Kind: "signal.delivered", ObservedAt: time.Now().UTC(), Body: map[string]any{"signal": req.Signal}})
	return response()
}

func (s *Service) cancel(id, reason string) protocol.Response {
	a, out := s.lookupActive(id)
	if a == nil {
		return out
	}
	p, err := s.requestTermination(a, reason)
	if err != nil {
		return failure("storage-failure", err.Error())
	}
	if p != nil {
		go s.driveTermination(a, p)
	}
	return response()
}

func (s *Service) requestTermination(a *active, reason string) (physical, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.terminating || a.run.State == model.Terminal || a.run.State == model.Uncertain {
		return nil, nil
	}
	next := a.run
	next.TerminationReason = reason
	next.State = model.Terminating
	next.Generation++
	kind := "run.terminating"
	body := map[string]any{"reason": reason}
	if reason == "timed-out" {
		kind = "limit.reached"
		body["limit"] = "wall-time"
	}
	if reason == "lease-expired" {
		kind = "lease.expired"
	}
	if _, err := s.store.Update(next, &model.Event{Kind: kind, ObservedAt: time.Now().UTC(), Body: body}); err != nil {
		return nil, err
	}
	a.run = next
	a.terminating = true
	return a.process, nil
}

func (s *Service) terminate(a *active, reason string) {
	p, err := s.requestTermination(a, reason)
	if err != nil {
		s.markUncertain(a, "termination state durability failed")
		return
	}
	if p != nil {
		s.driveTermination(a, p)
	}
}

func (s *Service) driveTermination(a *active, p physical) {
	a.mu.Lock()
	reason := a.run.TerminationReason
	a.mu.Unlock()
	if setter, ok := p.(interface{ SetTerminationReason(string) }); ok {
		setter.SetTerminationReason(reason)
	}
	result, err := p.Terminate(terminationGrace)
	if err != nil || !result.complete {
		s.markUncertain(a, "termination unproven")
		return
	}
	a.mu.Lock()
	a.forced = result.forced
	a.mu.Unlock()
}

func (s *Service) renew(req protocol.Request) protocol.Response {
	a, out := s.lookupActive(req.RunID)
	if a == nil {
		return out
	}
	if req.LeaseMs < 1000 || req.LeaseMs > int64(maxWallTime/time.Millisecond) {
		return failure("invalid-request", "invalid lease duration")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.run.Spec.Lifetime.Mode != "lease-bound" {
		return failure("invalid-request", "Run is detached")
	}
	if a.run.LeaseGeneration != req.LeaseGeneration {
		return failure("stale-generation", "stale lease generation")
	}
	if a.terminating {
		return failure("already-terminal", "Run is terminating")
	}
	next := a.run
	next.LeaseGeneration++
	expiry := time.Now().UTC().Add(time.Duration(req.LeaseMs) * time.Millisecond)
	next.LeaseExpiry = &expiry
	if _, err := s.store.Update(next, &model.Event{Kind: "lease.renewed", ObservedAt: time.Now().UTC()}); err != nil {
		return failure("storage-failure", err.Error())
	}
	a.run = next
	out = response()
	r := publicRun(a.run)
	out.Run = &r
	return out
}

func (s *Service) reconcile() error {
	runs, err := s.store.List()
	if err != nil {
		return err
	}
	for _, run := range runs {
		if run.State == model.Terminal || run.State == model.Uncertain {
			continue
		}
		run.State = model.Reconciling
		run.Generation++
		run.Attachments = 0
		if _, err := s.store.Update(run, &model.Event{Kind: "run.reconciling", ObservedAt: time.Now().UTC()}); err != nil {
			return err
		}
		if run.Ownership != nil {
			a := &active{run: run, done: make(chan struct{})}
			stdout := &capture{s: s, a: a, stream: "stdout"}
			stderr := &capture{s: s, a: a, stream: "stderr"}
			if run.Spec.Interactive {
				stdout.stream = "pty"
			}
			result, e := s.backend.Reconcile(run.ID, *run.Ownership, run.Spec.Interactive, stdout, stderr)
			if e == nil && result.terminal {
				if refreshed, readErr := s.store.Get(run.ID); readErr == nil {
					run = refreshed
				}
				now := time.Now().UTC()
				run.State = model.Terminal
				run.FinishedAt = &now
				run.Generation++
				if result.receipt != nil {
					copy := *result.receipt
					copy.Output = run.Output
					run.Receipt = &copy
					run.FinishedAt = &copy.FinishedAt
				} else {
					outcome := result.exit.outcome
					if outcome == "" {
						outcome = "exited"
					}
					run.Receipt = &model.Receipt{Version: 1, RunID: run.ID, Outcome: outcome, ExitCode: result.exit.code, Signal: result.exit.signal, StartedAt: run.StartedAt, FinishedAt: now, Resources: run.Resources, Output: run.Output, Cleanup: "complete"}
				}
				if _, err := s.store.Update(run, &model.Event{Kind: "run.terminal", ObservedAt: now}); err != nil {
					return err
				}
				continue
			}
			if e == nil && result.live && result.process != nil {
				a.process = result.process
				a.run.State = model.Running
				a.run.Generation++
				if _, err := s.store.Update(a.run, &model.Event{Kind: "run.reconciled", ObservedAt: time.Now().UTC()}); err != nil {
					return err
				}
				s.mu.Lock()
				s.active[run.ID] = a
				s.mu.Unlock()
				go s.monitor(a)
				continue
			}
		}
		run.State = model.Uncertain
		run.Generation++
		if _, err := s.store.Update(run, &model.Event{Kind: "run.uncertain", ObservedAt: time.Now().UTC(), Body: map[string]any{"reason": "ownership or outcome not proven"}}); err != nil {
			return err
		}
	}
	return nil
}

var errSupervisorClosed = errors.New("supervisor closed")

func (s *Service) Close() error {
	if s.store == nil {
		return errSupervisorClosed
	}
	s.closeOnce.Do(func() { close(s.stop) })
	return s.store.Close()
}

func statePath(root string) string { return filepath.Join(root, "state.db") }

func ensureStateDir(root string) error {
	if root == "" {
		return fmt.Errorf("state directory required")
	}
	return os.MkdirAll(root, 0700)
}
