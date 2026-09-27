package supervisor

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yohn-jp/jinushi/internal/guardian"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

type exitResult struct {
	code                 *int
	signal               string
	outcome              string
	startedAt            time.Time
	finishedAt           time.Time
	terminationRequested bool
	forced               bool
	receipt              *model.Receipt
}

type terminationResult struct {
	complete bool
	forced   bool
	outcome  string
}

type reconcileResult struct {
	live              bool
	terminal          bool
	exit              exitResult
	process           physical
	receipt           *model.Receipt
	ownership         *model.Ownership
	effective         *model.Capabilities
	state             model.State
	terminationReason string
	lease             leaseState
	resources         model.Resources
	lastSampleAt      *time.Time
	lastOutputAt      *time.Time
}

type leaseState struct {
	expiry                 *time.Time
	generation             uint64
	lastExpectedGeneration uint64
	lastMs                 int64
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
	Start(model.Run, model.RunSpec, io.Writer, io.Writer) (physical, error)
	Reconcile(model.Run, io.Writer, io.Writer) (reconcileResult, error)
}

type active struct {
	mu          sync.Mutex
	leaseMu     sync.Mutex
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
	config    Config
	mu        sync.RWMutex
	active    map[string]*active
	stop      chan struct{}
	closeOnce sync.Once
	workers   sync.WaitGroup
	closing   bool // guarded by mu; also gates workers.Add against Close.Wait
}

func newService(root string, db *store.Store, backend executor, config Config) *Service {
	return &Service{root: root, store: db, backend: backend, config: config, active: make(map[string]*active), stop: make(chan struct{})}
}

func (s *Service) launchLocked(work func()) bool {
	if s.closing {
		return false
	}
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		work()
	}()
	return true
}

func (s *Service) launch(work func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.launchLocked(work)
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

func initialResources(c model.Capabilities, sampleIntervalMs int64) model.Resources {
	metric := func(supported bool) model.Metric {
		if supported {
			return model.Metric{Status: "unavailable"}
		}
		return model.Metric{Status: "unsupported"}
	}
	return model.Resources{MemoryBytes: metric(c.MemoryTelemetry), PeakMemoryBytes: metric(c.MemoryTelemetry), CPUTimeNs: metric(c.CPUTelemetry), ProcessCount: metric(c.ProcessTelemetry), PeakProcessCount: metric(c.ProcessTelemetry), TaskCount: metric(c.ProcessTelemetry), PeakTaskCount: metric(c.ProcessTelemetry), SampleIntervalMs: sampleIntervalMs}
}

func normalizeResources(r model.Resources, c model.Capabilities, sampleIntervalMs int64) model.Resources {
	defaults := initialResources(c, sampleIntervalMs)
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
	if r.TaskCount.Status == "" {
		r.TaskCount = defaults.TaskCount
	}
	if r.PeakTaskCount.Status == "" {
		r.PeakTaskCount = defaults.PeakTaskCount
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

func (s *Service) populateReceipt(run model.Run, receipt *model.Receipt) {
	// validSpec rejects NUL in argv, making this ordered encoding unambiguous.
	identity := sha256.Sum256([]byte(strings.Join(run.Spec.Argv, "\x00")))
	receipt.AcceptedArgvSHA256 = fmt.Sprintf("%x", identity)
	if run.EffectiveCapabilities != nil {
		copy := *run.EffectiveCapabilities
		copy.Signals = append([]string(nil), copy.Signals...)
		receipt.EffectiveCapabilities = &copy
		receipt.Capabilities = copy
	}
	if !receipt.Output.HistoryComplete || run.ResourceGap {
		receipt.EvidenceIncomplete = true
	}
	for _, metric := range []model.Metric{
		receipt.Resources.MemoryBytes, receipt.Resources.PeakMemoryBytes,
		receipt.Resources.CPUTimeNs, receipt.Resources.ProcessCount,
		receipt.Resources.PeakProcessCount, receipt.Resources.TaskCount,
		receipt.Resources.PeakTaskCount,
	} {
		if metric.Status == "unavailable" || metric.Status == "" {
			receipt.EvidenceIncomplete = true
			break
		}
	}
}

func validSpec(spec *model.RunSpec, caps model.Capabilities, config Config) *protocol.Failure {
	if spec == nil || len(spec.Argv) == 0 || spec.Argv[0] == "" || len(spec.Argv) > 256 {
		return &protocol.Failure{Code: "invalid-request", Message: "argv must contain an executable and at most 256 arguments"}
	}
	for _, arg := range spec.Argv {
		if len(arg) > 32768 || strings.ContainsRune(arg, 0) {
			return &protocol.Failure{Code: "invalid-request", Message: "invalid argv element"}
		}
	}
	if !filepath.IsAbs(spec.Cwd) {
		return &protocol.Failure{Code: "invalid-request", Message: "cwd must be absolute"}
	}
	if len(spec.Cwd) > 4096 {
		return &protocol.Failure{Code: "invalid-request", Message: "cwd too long"}
	}
	if spec.Environment.Mode != "" && spec.Environment.Mode != "inherit-supervisor" && spec.Environment.Mode != "replace" {
		return &protocol.Failure{Code: "invalid-request", Message: "invalid environment mode"}
	}
	if len(spec.Environment.Set) > 128 || len(spec.Environment.Unset) > 128 {
		return &protocol.Failure{Code: "invalid-request", Message: "environment too large"}
	}
	for k, v := range spec.Environment.Set {
		if k == "" || strings.ContainsAny(k, "=\x00") || len(k) > 256 || len(v) > 32768 || strings.ContainsRune(v, 0) {
			return &protocol.Failure{Code: "invalid-request", Message: "invalid environment entry"}
		}
	}
	for _, k := range spec.Environment.Unset {
		if k == "" || strings.ContainsAny(k, "=\x00") || len(k) > 256 {
			return &protocol.Failure{Code: "invalid-request", Message: "invalid environment key"}
		}
	}
	if spec.Lifetime.Mode == "" {
		spec.Lifetime.Mode = "detached"
	}
	if spec.Lifetime.Mode != "detached" && spec.Lifetime.Mode != "lease-bound" {
		return &protocol.Failure{Code: "invalid-request", Message: "invalid lifetime mode"}
	}
	if spec.Lifetime.Mode == "lease-bound" && (spec.Lifetime.LeaseMs < 1000 || spec.Lifetime.LeaseMs > config.MaxWallTimeMs) {
		return &protocol.Failure{Code: "invalid-request", Message: "invalid lease duration"}
	}
	if spec.Limits.MemoryBytes < 0 || spec.Limits.CPUQuotaPercent < 0 || spec.Limits.ProcessCount < 0 || spec.Limits.TaskCount < 0 || spec.Limits.WallTimeMs < 0 || spec.Limits.OutputBytes < 0 {
		return &protocol.Failure{Code: "invalid-request", Message: "negative limit"}
	}
	if spec.Limits.OutputBytes > config.MaxOutputBytes || spec.Limits.WallTimeMs > config.MaxWallTimeMs || spec.Limits.MemoryBytes > config.MaxMemoryBytes || spec.Limits.ProcessCount > config.MaxProcessCount || spec.Limits.TaskCount > config.MaxTaskCount {
		return &protocol.Failure{Code: "invalid-request", Message: "limit exceeds supervisor ceiling"}
	}
	if spec.Interactive && !caps.PTY {
		return &protocol.Failure{Code: "unsupported-capability", Message: "PTY unavailable"}
	}
	if spec.Limits.MemoryBytes > 0 && !caps.MemoryEnforcement {
		return &protocol.Failure{Code: "unsupported-capability", Message: "memory enforcement unavailable"}
	}
	if spec.Limits.CPUQuotaPercent > 0 && !caps.CPUQuotaEnforcement {
		return &protocol.Failure{Code: "unsupported-capability", Message: "CPU quota enforcement unavailable"}
	}
	if spec.Limits.ProcessCount > 0 && !caps.ProcessCountEnforcement {
		return &protocol.Failure{Code: "unsupported-capability", Message: "process-count enforcement unavailable"}
	}
	if spec.Limits.TaskCount > 0 && !caps.TaskCountEnforcement {
		return &protocol.Failure{Code: "unsupported-capability", Message: "task-count enforcement unavailable"}
	}
	if len(spec.Correlation) > 16 {
		return &protocol.Failure{Code: "invalid-request", Message: "too many correlation labels"}
	}
	for k, v := range spec.Correlation {
		if len(k) == 0 || len(k) > 64 || len(v) > 256 {
			return &protocol.Failure{Code: "invalid-request", Message: "invalid correlation label"}
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
		limit := req.Limit
		if limit == 0 {
			limit = 64
		}
		if limit < 1 || limit > 128 || len(req.Cursor) > 128 {
			return failure("invalid-request", "list limit must be between 1 and 128 and cursor at most 128 bytes")
		}
		runs, nextCursor, err := s.store.ListPage(req.Cursor, int(limit))
		if err != nil {
			return failure("storage-failure", err.Error())
		}
		out := response()
		out.Runs = make([]model.Run, 0, len(runs))
		for _, run := range runs {
			out.Runs = append(out.Runs, publicRun(run))
			encoded, err := json.Marshal(out)
			if err != nil {
				return failure("storage-failure", "Run list could not be encoded")
			}
			if len(encoded) >= protocol.MaxFrame-4096 {
				out.Runs = out.Runs[:len(out.Runs)-1]
				if len(out.Runs) == 0 {
					return failure("response-too-large", "Run "+run.ID+" is too large for a list page")
				}
				nextCursor = out.Runs[len(out.Runs)-1].ID
				break
			}
		}
		out.NextCursor = nextCursor
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
	case "close-input":
		return s.closeInput(req.RunID)
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
	if f := validSpec(spec, s.backend.Capabilities(), s.config); f != nil {
		return protocol.Response{Version: model.ProtocolVersion, Error: f}
	}
	if validator, ok := s.backend.(interface{ ValidateLimits(model.Limits) error }); ok {
		if err := validator.ValidateLimits(spec.Limits); err != nil {
			return failure("unsupported-capability", err.Error())
		}
	}
	// Guardian adds launch metadata to the accepted spec before sending it
	// over its own bounded channel. Leave headroom before accepting the Run.
	launchSpec, err := json.Marshal(spec)
	if err != nil || len(launchSpec) >= protocol.MaxFrame-4096 {
		return failure("response-too-large", "Run launch data exceeds the helper input limit")
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
	run := model.Run{ID: id, Spec: publicSpec, State: model.Accepted, Generation: 1, CreatedAt: now, Resources: initialResources(s.backend.Capabilities(), s.config.SampleIntervalMs)}
	run.Output.HistoryComplete = true
	if spec.Lifetime.Mode == "lease-bound" {
		expiry := now.Add(time.Duration(spec.Lifetime.LeaseMs) * time.Millisecond)
		run.LeaseExpiry = &expiry
		run.LeaseGeneration = 1
	}
	// The accepted identity must fit in the same bounded frame that carries the
	// request. Check the actual public response before making acceptance durable.
	accepted := response()
	clean := publicRun(run)
	accepted.Run = &clean
	encoded, err := json.Marshal(accepted)
	// Reserve room for a maximum-sized journal event plus later lifecycle
	// metadata, so inspect, await, and event reads stay representable.
	if err != nil || len(encoded) >= protocol.MaxFrame-(300<<10) {
		return failure("response-too-large", "Run metadata exceeds the IPC response limit")
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return failure("supervisor-closed", "supervisor is closing")
	}
	if _, _, err = s.store.Create(run, &model.Event{Kind: model.EventRunAccepted, ObservedAt: now, Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Accepted, Generation: run.Generation}}}); err != nil {
		s.mu.Unlock()
		return failure("storage-failure", err.Error())
	}
	a := &active{run: run, spec: *spec, done: make(chan struct{})}
	s.active[id] = a
	s.launchLocked(func() { s.start(a) })
	s.mu.Unlock()
	return accepted
}

func (s *Service) transition(a *active, state model.State, kind model.EventKind, details model.RunEventPayload) error {
	next := a.run
	next.State = state
	next.Generation++
	details.State = state
	details.Generation = next.Generation
	if _, err := s.store.Update(next, &model.Event{Kind: kind, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{Run: &details}}); err != nil {
		return err
	}
	a.run = next
	return nil
}

func (s *Service) start(a *active) {
	a.leaseMu.Lock()
	defer a.leaseMu.Unlock()
	a.mu.Lock()
	if a.terminating {
		a.mu.Unlock()
		s.finish(a, "cancelled", exitResult{}, false, "complete")
		return
	}
	if err := s.transition(a, model.Starting, model.EventRunStarting, model.RunEventPayload{}); err != nil {
		a.mu.Unlock()
		s.markUncertain(a, "storage failure before spawn")
		return
	}
	run := a.run
	a.mu.Unlock()
	stdout := &capture{s: s, a: a, stream: "stdout"}
	stderr := &capture{s: s, a: a, stream: "stderr"}
	if a.spec.Interactive {
		stdout.stream = "pty"
	}
	p, err := s.backend.Start(run, a.spec, stdout, stderr)
	if err != nil {
		var cleanTerminal *cleanStartTerminal
		if errors.As(err, &cleanTerminal) {
			receipt := cleanTerminal.receipt
			a.mu.Lock()
			a.run.EffectiveCapabilities = cleanTerminal.effective
			a.mu.Unlock()
			s.finish(a, receipt.Outcome, exitResult{code: receipt.ExitCode, signal: receipt.Signal, outcome: receipt.Outcome, finishedAt: receipt.FinishedAt, terminationRequested: receipt.TerminationRequested, forced: receipt.Forced, receipt: &receipt}, receipt.Forced, receipt.Cleanup)
			return
		}
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
	if source, ok := p.(interface{ EffectiveCapabilities() *model.Capabilities }); ok {
		a.run.EffectiveCapabilities = source.EffectiveCapabilities()
	}
	now := time.Now().UTC()
	a.run.StartedAt = &now
	state, kind := model.Running, model.EventRunRunning
	if a.terminating {
		state, kind = model.Terminating, model.EventRunOwned
	}
	if err := s.transition(a, state, kind, model.RunEventPayload{PID: own.PID}); err != nil {
		a.mu.Unlock()
		p.Terminate(time.Duration(s.config.TerminationGraceMs) * time.Millisecond)
		s.markUncertain(a, "storage failure after spawn")
		return
	}
	shouldTerminate := a.terminating
	a.mu.Unlock()
	s.launch(func() { s.monitor(a) })
	if shouldTerminate {
		s.launch(func() { s.driveTermination(a, p) })
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
	ticker := time.NewTicker(time.Duration(s.config.SampleIntervalMs) * time.Millisecond)
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
			termination, err := a.process.Terminate(time.Duration(s.config.TerminationGraceMs) * time.Millisecond)
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
				w.result.outcome = limitOutcome
			} else if limitOutcome == "timed-out" || limitOutcome == "cancelled" {
				outcome = limitOutcome
			}
			if reason != "" {
				outcome = reason
			}
			if outcome == "lease-expired" {
				outcome = "cancelled"
			}
			s.finish(a, outcome, w.result, forced || w.result.forced, "complete")
			return
		case <-ticker.C:
			s.sample(a)
			s.sweepAttachments(a)
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
			now := time.Now().UTC()
			if a.run.Resources.MemoryBytes.Status != "unsupported" {
				a.run.Resources.MemoryBytes = model.Metric{Status: "unavailable"}
			}
			if a.run.Resources.CPUTimeNs.Status != "unsupported" {
				a.run.Resources.CPUTimeNs = model.Metric{Status: "unavailable"}
			}
			if a.run.Resources.ProcessCount.Status != "unsupported" {
				a.run.Resources.ProcessCount = model.Metric{Status: "unavailable"}
			}
			a.run.LastResourceSampleAt = &now
			resources := a.run.Resources
			_, _ = s.store.Update(a.run, &model.Event{Kind: model.EventResourceUnavailable, ObservedAt: now, Payload: &model.EventPayload{Resource: &model.ResourceEventPayload{Resources: resources}}})
		}
		a.mu.Unlock()
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.run.State == model.Terminal || a.run.State == model.Uncertain {
		return
	}
	r = normalizeResources(r, s.backend.Capabilities(), s.config.SampleIntervalMs)
	r.SampleIntervalMs = s.config.SampleIntervalMs
	now := time.Now().UTC()
	if r.CPUTimeNs.Status == "measured" && a.run.Resources.CPUTimeNs.Status == "measured" && r.CPUTimeNs.Value > a.run.Resources.CPUTimeNs.Value {
		a.run.LastCPUActivityAt = &now
	}
	if r.ProcessCount.Status == "measured" && a.run.Resources.ProcessCount.Status == "measured" && r.ProcessCount.Value != a.run.Resources.ProcessCount.Value {
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
	a.run.LastResourceSampleAt = &now
	_, _ = s.store.Update(a.run, &model.Event{Kind: model.EventResourceSample, ObservedAt: now, Payload: &model.EventPayload{Resource: &model.ResourceEventPayload{Resources: r}}})
}

func (s *Service) finish(a *active, outcome string, exit exitResult, forced bool, cleanup string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.run.State == model.Terminal || a.run.State == model.Uncertain {
		return
	}
	priorReason := a.run.TerminationReason
	if priorReason == "" {
		switch exit.outcome {
		case "timed-out":
			a.run.TerminationReason = "timed-out"
		case "cancelled":
			a.run.TerminationReason = "lease-expired"
		}
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
	if exit.receipt != nil {
		copy := *exit.receipt
		copy.Outcome = outcome
		copy.Output = a.run.Output
		copy.TerminationRequested = copy.TerminationRequested || a.run.TerminationReason != ""
		copy.Forced = copy.Forced || forced
		copy.Cleanup = cleanup
		a.run.Receipt = &copy
		a.run.Resources = copy.Resources
		a.run.StartedAt = copy.StartedAt
	} else {
		a.run.Receipt = &model.Receipt{Version: model.ProtocolVersion, RunID: a.run.ID, Outcome: outcome, ExitCode: exit.code, Signal: exit.signal, StartedAt: a.run.StartedAt, FinishedAt: now, Resources: a.run.Resources, Output: a.run.Output, TerminationRequested: a.run.TerminationReason != "" || exit.terminationRequested, Forced: forced, Cleanup: cleanup}
	}
	s.populateReceipt(a.run, a.run.Receipt)
	events := terminalEvents(now, exit.outcome, a.run.TerminationReason, priorReason)
	events = append(events, model.Event{Kind: model.EventRunTerminal, ObservedAt: now, Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Terminal, Generation: a.run.Generation + 1, Outcome: outcome}}})
	next := a.run
	next.State = model.Terminal
	next.Generation++
	if _, err := s.store.UpdateWithEvents(next, events); err != nil {
		a.run.State = model.Uncertain
		close(a.done)
		s.mu.Lock()
		delete(s.active, a.run.ID)
		s.mu.Unlock()
		return
	}
	a.run = next
	close(a.done)
	s.mu.Lock()
	delete(s.active, a.run.ID)
	s.mu.Unlock()
}

func terminalEvents(now time.Time, exitOutcome, reason, priorReason string) []model.Event {
	if strings.HasPrefix(exitOutcome, "resource-limit:") {
		return []model.Event{{Kind: model.EventLimitReached, ObservedAt: now, Payload: &model.EventPayload{Limit: &model.LimitEventPayload{Limit: strings.TrimPrefix(exitOutcome, "resource-limit:")}}}}
	}
	if priorReason == "" && reason == "timed-out" {
		return []model.Event{{Kind: model.EventLimitReached, ObservedAt: now, Payload: &model.EventPayload{Limit: &model.LimitEventPayload{Limit: "wall-time"}}}}
	}
	if priorReason == "" && reason == "lease-expired" {
		return []model.Event{{Kind: model.EventLeaseExpired, ObservedAt: now, Payload: &model.EventPayload{Lease: &model.LeaseEventPayload{}}}}
	}
	return nil
}

func (s *Service) markUncertain(a *active, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.run.State == model.Terminal || a.run.State == model.Uncertain {
		return
	}
	a.run.Attachments = 0
	_ = s.transition(a, model.Uncertain, model.EventRunUncertain, model.RunEventPayload{Reason: reason})
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
	return w.RecordGapObserved(observed, time.Time{})
}

func (w *capture) RecordGapObserved(observed int64, observedAt time.Time) error {
	basis := "stream-last-write"
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
		basis = "import-time-unavailable"
	}
	w.a.mu.Lock()
	defer w.a.mu.Unlock()
	meta, err := w.s.store.RecordOutputGapWithEvent(w.a.run.ID, w.stream, observed, model.Event{Kind: model.EventOutputGap, ObservedAt: observedAt, Payload: &model.EventPayload{Output: &model.OutputEventPayload{Stream: w.stream, ObservedBytes: observed, TimestampBasis: basis}}})
	if err != nil {
		return err
	}
	if w.stream == "stdout" {
		w.a.run.Output.Stdout = meta
	} else if w.stream == "stderr" {
		w.a.run.Output.Stderr = meta
	} else {
		w.a.run.Output.PTY = meta
	}
	w.a.run.Output.HistoryComplete = false
	if w.a.run.LastOutputAt == nil || observedAt.After(*w.a.run.LastOutputAt) {
		w.a.run.LastOutputAt = &observedAt
	}
	return nil
}

func (w *capture) Write(data []byte) (int, error) {
	return w.WriteObserved(data, time.Time{})
}

func (w *capture) WriteObserved(data []byte, observedAt time.Time) (int, error) {
	basis := "stream-last-write"
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
		basis = "import-time-unavailable"
	}
	w.a.mu.Lock()
	defer w.a.mu.Unlock()
	max := w.a.run.Spec.Limits.OutputBytes
	if max == 0 {
		max = w.s.config.DefaultOutputBytes
	}
	meta, err := w.s.store.AppendOutputWithEvent(w.a.run.ID, w.stream, data, max, model.Event{Kind: model.EventOutputChunk, ObservedAt: observedAt, Payload: &model.EventPayload{Output: &model.OutputEventPayload{Stream: w.stream, Bytes: int64(len(data)), TimestampBasis: basis}}})
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
	if w.a.run.LastOutputAt == nil || observedAt.After(*w.a.run.LastOutputAt) {
		w.a.run.LastOutputAt = &observedAt
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
	out.RetainedFrom = from
	out.Gap = gap
	if run, err := s.store.Get(req.RunID); err == nil {
		clean := publicRun(run)
		out.Run = &clean
	}
	base, err := json.Marshal(out)
	if err != nil {
		return failure("storage-failure", "event response could not be encoded")
	}
	used := len(base) + len(`,"events":[]`)
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return failure("storage-failure", "event could not be encoded")
		}
		if used+len(encoded)+1 >= protocol.MaxFrame-4096 {
			break
		}
		out.Events = append(out.Events, event)
		used += len(encoded) + 1
	}
	return out
}

func (s *Service) output(req protocol.Request) protocol.Response {
	if req.AttachID != "" {
		if err := s.renewAttachment(req.RunID, req.AttachID); err != nil {
			// Once a Run is final, retained output is still readable even though
			// its live attachment has been removed. The attachment cannot be
			// renewed, but no live input authority is granted by this read.
			run, readErr := s.store.Get(req.RunID)
			if readErr != nil || (run.State != model.Terminal && run.State != model.Uncertain) {
				return failure("attachment-expired", err.Error())
			}
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
	if _, err := s.store.Update(next, &model.Event{Kind: model.EventPTYAttached, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{PTY: &model.PTYEventPayload{Attachments: next.Attachments}}}); err != nil {
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
	if _, err := s.store.Update(next, &model.Event{Kind: model.EventPTYDetached, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{PTY: &model.PTYEventPayload{Attachments: next.Attachments}}}); err != nil {
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
	if _, err := s.store.Update(next, &model.Event{Kind: model.EventPTYAttachmentExpired, ObservedAt: now.UTC(), Payload: &model.EventPayload{PTY: &model.PTYEventPayload{Attachments: next.Attachments, Expired: expired}}}); err != nil {
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

func (s *Service) closeInput(id string) protocol.Response {
	a, out := s.lookupActive(id)
	if a == nil {
		return out
	}
	a.mu.Lock()
	interactive := a.run.Spec.Interactive
	p := a.process
	a.mu.Unlock()
	if interactive {
		return failure("unsupported-capability", "PTY input cannot be half-closed")
	}
	if p == nil {
		return failure("backend-failure", "Run not started")
	}
	if err := p.CloseInput(); err != nil {
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
	allowed := false
	for _, name := range s.backend.Capabilities().Signals {
		if req.Signal == name {
			allowed = true
			break
		}
	}
	if !allowed {
		return failure("invalid-request", "signal is not supported by this backend")
	}
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
	if _, err := s.store.AppendEvent(req.RunID, model.Event{Kind: model.EventSignalRequested, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{Signal: &model.SignalEventPayload{Signal: req.Signal}}}); err != nil {
		return failure("storage-failure", err.Error())
	}
	if err := p.Signal(req.Signal); err != nil {
		return failure("backend-failure", err.Error())
	}
	if _, err := s.store.AppendEvent(req.RunID, model.Event{Kind: model.EventSignalDelivered, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{Signal: &model.SignalEventPayload{Signal: req.Signal}}}); err != nil {
		return failure("storage-failure", err.Error())
	}
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
		s.launch(func() { s.driveTermination(a, p) })
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
	now := time.Now().UTC()
	events := []model.Event{
		{Kind: model.EventTerminationRequested, ObservedAt: now, Payload: &model.EventPayload{Control: &model.ControlEventPayload{Reason: reason}}},
		{Kind: model.EventRunTerminating, ObservedAt: now, Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Terminating, Generation: next.Generation, Reason: reason}}},
	}
	if reason == "timed-out" {
		events = append(events, model.Event{Kind: model.EventLimitReached, ObservedAt: now, Payload: &model.EventPayload{Limit: &model.LimitEventPayload{Limit: "wall-time"}}})
	}
	if reason == "lease-expired" {
		events = append(events, model.Event{Kind: model.EventLeaseExpired, ObservedAt: now, Payload: &model.EventPayload{Lease: &model.LeaseEventPayload{Generation: next.LeaseGeneration, ExpiresAt: next.LeaseExpiry}}})
	}
	if reason == "cancelled" {
		events = append(events, model.Event{Kind: model.EventCancelRequested, ObservedAt: now, Payload: &model.EventPayload{Control: &model.ControlEventPayload{Reason: reason}}})
	}
	if _, err := s.store.UpdateWithEvents(next, events); err != nil {
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
	result, err := p.Terminate(time.Duration(s.config.TerminationGraceMs) * time.Millisecond)
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
	if req.LeaseMs < 1000 || req.LeaseMs > s.config.MaxWallTimeMs {
		return failure("invalid-request", "invalid lease duration")
	}
	a.leaseMu.Lock()
	defer a.leaseMu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.run.Spec.Lifetime.Mode != "lease-bound" {
		return failure("invalid-request", "Run is detached")
	}
	if a.run.State == model.Terminal || a.run.State == model.Uncertain {
		return failure("already-terminal", "Run has reached a final state")
	}
	if a.run.LeaseGeneration == req.LeaseGeneration+1 && a.run.LastLeaseExpectedGeneration == req.LeaseGeneration && a.run.LastLeaseMs == req.LeaseMs {
		out = response()
		r := publicRun(a.run)
		out.Run = &r
		return out
	}
	if a.run.LeaseGeneration != req.LeaseGeneration {
		return failure("stale-generation", "stale lease generation")
	}
	if a.terminating {
		return failure("already-terminal", "Run is terminating")
	}
	renewer, ok := a.process.(interface {
		RenewLease(uint64, int64) (leaseState, error)
	})
	if !ok {
		return failure("ownership-uncertain", "Run lease guardian is unavailable")
	}
	lease, err := renewer.RenewLease(req.LeaseGeneration, req.LeaseMs)
	if err != nil {
		if errors.Is(err, guardian.ErrLeaseExpired) {
			return failure("lease-expired", "lease has expired")
		}
		if errors.Is(err, guardian.ErrStaleGeneration) {
			return failure("stale-generation", "stale lease generation")
		}
		s.launch(func() { s.markUncertain(a, "lease renewal ownership unproven") })
		return failure("ownership-uncertain", "lease renewal was not proven")
	}
	if lease.expiry == nil || lease.generation != req.LeaseGeneration+1 || lease.lastExpectedGeneration != req.LeaseGeneration || lease.lastMs != req.LeaseMs {
		s.launch(func() { s.markUncertain(a, "lease renewal evidence mismatch") })
		return failure("ownership-uncertain", "lease renewal evidence did not match the request")
	}
	next := a.run
	setLeaseState(&next, lease)
	a.run = next
	if _, err := s.store.Update(next, &model.Event{Kind: model.EventLeaseRenewed, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{Lease: &model.LeaseEventPayload{Generation: lease.generation, ExpiresAt: lease.expiry}}}); err != nil {
		s.launch(func() { s.markUncertain(a, "lease renewal durability failed") })
		return failure("storage-failure", err.Error())
	}
	out = response()
	r := publicRun(a.run)
	out.Run = &r
	return out
}

func setLeaseState(run *model.Run, lease leaseState) {
	run.LeaseExpiry = lease.expiry
	run.LeaseGeneration = lease.generation
	run.LastLeaseExpectedGeneration = lease.lastExpectedGeneration
	run.LastLeaseMs = lease.lastMs
}

func importLeaseState(run *model.Run, lease leaseState) error {
	if run.Spec.Lifetime.Mode != "lease-bound" {
		if lease.expiry != nil || lease.generation != 0 {
			return errors.New("detached Run has guardian lease evidence")
		}
		return nil
	}
	if lease.expiry == nil || lease.generation < run.LeaseGeneration {
		return errors.New("guardian lease evidence is missing or older than durable state")
	}
	if lease.generation == run.LeaseGeneration {
		if run.LeaseExpiry == nil || !lease.expiry.Equal(*run.LeaseExpiry) || lease.lastExpectedGeneration != run.LastLeaseExpectedGeneration || lease.lastMs != run.LastLeaseMs {
			return errors.New("guardian lease evidence conflicts with durable state")
		}
		return nil
	}
	if lease.lastExpectedGeneration != lease.generation-1 || lease.lastMs <= 0 {
		return errors.New("guardian lease advance is invalid")
	}
	setLeaseState(run, lease)
	return nil
}

func (s *Service) recoveredResourceEvents(run *model.Run, result reconcileResult) []model.Event {
	if result.lastSampleAt == nil || result.lastSampleAt.IsZero() {
		return nil
	}
	if run.LastResourceSampleAt != nil && !result.lastSampleAt.After(*run.LastResourceSampleAt) {
		return nil
	}
	from := run.CreatedAt
	if run.LastResourceSampleAt != nil {
		from = *run.LastResourceSampleAt
	}
	if !result.lastSampleAt.After(from) {
		return nil
	}
	latest := normalizeResources(result.resources, s.backend.Capabilities(), s.config.SampleIntervalMs)
	latest.SampleIntervalMs = s.config.SampleIntervalMs
	if latest.CPUTimeNs.Status == "measured" && run.Resources.CPUTimeNs.Status == "measured" && latest.CPUTimeNs.Value > run.Resources.CPUTimeNs.Value {
		run.LastCPUActivityAt = result.lastSampleAt
	}
	if latest.ProcessCount.Status == "measured" && run.Resources.ProcessCount.Status == "measured" && latest.ProcessCount.Value != run.Resources.ProcessCount.Value {
		run.LastProcessChangeAt = result.lastSampleAt
	}
	run.Resources = latest
	run.LastResourceSampleAt = result.lastSampleAt
	run.ResourceGap = true
	return []model.Event{
		{Kind: model.EventResourceGap, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{ResourceGap: &model.ResourceGapEventPayload{From: &from, To: result.lastSampleAt, Status: "unavailable", Reason: "supervisor-unavailable", LatestResources: &latest}}},
		{Kind: model.EventResourceSample, ObservedAt: *result.lastSampleAt, Payload: &model.EventPayload{Resource: &model.ResourceEventPayload{Resources: latest}}},
	}
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
		if _, err := s.store.Update(run, &model.Event{Kind: model.EventRunReconciling, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Reconciling, Generation: run.Generation}}}); err != nil {
			return err
		}
		{
			a := &active{run: run, done: make(chan struct{})}
			stdout := &capture{s: s, a: a, stream: "stdout"}
			stderr := &capture{s: s, a: a, stream: "stderr"}
			if run.Spec.Interactive {
				stdout.stream = "pty"
			}
			result, e := s.backend.Reconcile(run, stdout, stderr)
			refreshed, readErr := s.store.Get(run.ID)
			if readErr != nil {
				return readErr
			}
			run = refreshed
			a.run = refreshed
			var resourceEvents []model.Event
			if e == nil {
				e = importLeaseState(&run, result.lease)
				if e == nil {
					if run.EffectiveCapabilities == nil {
						run.EffectiveCapabilities = result.effective
					}
					resourceEvents = s.recoveredResourceEvents(&run, result)
					if result.lastOutputAt != nil && (run.LastOutputAt == nil || result.lastOutputAt.After(*run.LastOutputAt)) {
						run.LastOutputAt = result.lastOutputAt
					}
				}
				a.run = run
			}
			if e == nil && result.terminal {
				priorReason := run.TerminationReason
				now := time.Now().UTC()
				run.State = model.Terminal
				if run.Ownership == nil && result.ownership != nil {
					owned := *result.ownership
					run.Ownership = &owned
				}
				if result.terminationReason != "" {
					run.TerminationReason = result.terminationReason
				}
				run.FinishedAt = &now
				run.Generation++
				if result.receipt != nil {
					copy := *result.receipt
					copy.Output = run.Output
					run.Receipt = &copy
					run.FinishedAt = &copy.FinishedAt
					run.StartedAt = copy.StartedAt
					run.Resources = copy.Resources
				} else {
					outcome := result.exit.outcome
					if outcome == "" {
						outcome = "exited"
					}
					run.Receipt = &model.Receipt{Version: 1, RunID: run.ID, Outcome: outcome, ExitCode: result.exit.code, Signal: result.exit.signal, StartedAt: run.StartedAt, FinishedAt: now, Resources: run.Resources, Output: run.Output, Cleanup: "complete"}
				}
				s.populateReceipt(run, run.Receipt)
				events := append(resourceEvents, terminalEvents(now, result.exit.outcome, run.TerminationReason, priorReason)...)
				events = append(events, model.Event{Kind: model.EventRunTerminal, ObservedAt: now, Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Terminal, Generation: run.Generation, Outcome: run.Receipt.Outcome}}})
				if _, err := s.store.UpdateWithEvents(run, events); err != nil {
					return err
				}
				continue
			}
			if e == nil && result.live && result.process != nil {
				a.process = result.process
				if a.run.Ownership == nil {
					owned := result.process.Ownership()
					a.run.Ownership = &owned
				}
				if result.terminationReason != "" {
					a.run.TerminationReason = result.terminationReason
				}
				if result.state == model.Terminating || a.run.TerminationReason != "" {
					a.run.State = model.Terminating
					a.terminating = true
				} else {
					a.run.State = model.Running
				}
				a.run.Generation++
				events := append(resourceEvents, model.Event{Kind: model.EventRunReconciled, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{Run: &model.RunEventPayload{State: a.run.State, Generation: a.run.Generation}}})
				if _, err := s.store.UpdateWithEvents(a.run, events); err != nil {
					return err
				}
				s.mu.Lock()
				s.active[run.ID] = a
				s.mu.Unlock()
				s.launch(func() { s.monitor(a) })
				if a.terminating {
					s.launch(func() { s.driveTermination(a, a.process) })
				}
				continue
			}
		}
		run.State = model.Uncertain
		run.Generation++
		if _, err := s.store.Update(run, &model.Event{Kind: model.EventRunUncertain, ObservedAt: time.Now().UTC(), Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Uncertain, Generation: run.Generation, Reason: "ownership or outcome not proven"}}}); err != nil {
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
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		close(s.stop)
		s.mu.Unlock()
		s.workers.Wait()
		err = s.store.Close()
	})
	return err
}

func statePath(root string) string { return filepath.Join(root, "state.db") }

func ensureStateDir(root string) error {
	if root == "" {
		return fmt.Errorf("state directory required")
	}
	return os.MkdirAll(root, 0700)
}
