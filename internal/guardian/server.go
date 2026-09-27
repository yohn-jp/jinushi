package guardian

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

func serveFromArgs(args []string, factory BackendFactory) error {
	if len(args) != 0 {
		return errors.New("guardian: hidden helper accepts no arguments")
	}
	if factory == nil {
		return errors.New("guardian: backend factory is required")
	}
	var config launchConfig
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, maxConfigBytes))
	if err := decoder.Decode(&config); err != nil {
		return errors.New("guardian: invalid transient launch config")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("guardian: helper input must contain one config")
	}
	if config.Version != ProtocolVersion || !runIDPattern.MatchString(config.RunID) || config.Dir == "" || config.Token == "" {
		return errors.New("guardian: incomplete launch config")
	}
	if err := validateLaunchConfig(config); err != nil {
		return err
	}
	return serveConfig(config, factory)
}

func validateLaunchConfig(config launchConfig) error {
	d, err := readDescriptor(config.Dir, config.RunID)
	if err != nil || !equalToken(d.Token, config.Token) || d.HostEnvelope != config.HostEnvelope {
		return errors.New("guardian: launch config does not match private descriptor")
	}
	if config.MaxOutputBytes < 0 || config.MaxOutputBytes > 1<<40 {
		return errors.New("guardian: invalid output retention limit")
	}
	if config.TerminationGraceMs < 100 || config.TerminationGraceMs > 30000 {
		return errors.New("guardian: invalid termination grace")
	}
	if config.SampleIntervalMs < 50 || config.SampleIntervalMs > 60000 {
		return errors.New("guardian: invalid sample interval")
	}
	if err := validateHostEnvelope(config.HostEnvelope); err != nil {
		return err
	}
	if config.Spec.Lifetime.Mode == "lease-bound" {
		if config.InitialLeaseExpiry == nil || config.InitialLeaseExpiry.IsZero() || config.LeaseGeneration == 0 {
			return errors.New("guardian: lease-bound Run lacks an initial lease")
		}
	} else if config.InitialLeaseExpiry != nil || config.LeaseGeneration != 0 {
		return errors.New("guardian: detached Run has lease metadata")
	}
	return nil
}

func serveConfig(config launchConfig, factory BackendFactory) error {
	spool, err := openSpool(config.Dir, config.MaxOutputBytes)
	if err != nil {
		return err
	}
	defer spool.close()
	state := &runState{
		descriptor: config,
		spool:      spool,
		terminal:   make(chan struct{}),
	}
	state.snapshot = Snapshot{
		Version: ProtocolVersion, RunID: config.RunID, State: model.Starting,
		Resources: unavailableResourcesFor(config.SampleIntervalMs), LeaseExpiry: config.InitialLeaseExpiry,
		LeaseGeneration: config.LeaseGeneration,
	}
	if err := state.persist(); err != nil {
		return err
	}
	listener, err := listenControl(config.Dir)
	if err != nil {
		state.markStartupFailure("guardian-control-endpoint-failed")
		return err
	}
	defer func() {
		_ = listener.Close()
		cleanupControl(config.Dir)
	}()
	go state.serve(listener)

	selected := factory()
	if selected == nil {
		state.markStartupFailure("backend-factory-unavailable")
		return nil
	}
	stdout, stderr := spool.writer("stdout"), spool.writer("stderr")
	if config.Spec.Interactive {
		stdout, stderr = spool.writer("pty"), spool.writer("pty")
	}
	state.controlMu.Lock()
	state.mu.Lock()
	leaseExpired := state.snapshot.LeaseExpiry != nil && !time.Now().Before(*state.snapshot.LeaseExpiry)
	state.mu.Unlock()
	if leaseExpired {
		state.controlMu.Unlock()
		state.markNoStartTerminal("cancelled", "lease-expired")
		return nil
	}
	process, err := selected.Start(config.Spec, stdout, stderr)
	if err != nil {
		state.controlMu.Unlock()
		var uncertain *backend.UncertainError
		if errors.As(err, &uncertain) {
			state.markUncertainWithOwnership(uncertain.Ownership, "backend-start-cleanup-unproven")
			return nil
		}
		state.markStartupFailure("backend-start-failed-clean")
		return nil
	}
	if process == nil {
		state.controlMu.Unlock()
		state.markStartupFailure("backend-start-returned-no-process")
		return nil
	}
	state.mu.Lock()
	state.process = process
	owner := process.Ownership()
	effectiveCapabilities := effectiveCapabilities(selected, process, config.Spec.Interactive)
	startedMono := time.Now()
	startedAt := startedMono.UTC()
	state.snapshot.State = model.Running
	state.snapshot.Ownership = &owner
	state.snapshot.EffectiveCapabilities = &effectiveCapabilities
	state.snapshot.StartedAt = &startedAt
	state.startedMono = startedMono
	state.captureOutputEvidenceLocked()
	state.snapshot.Resources = unavailableResourcesFor(config.SampleIntervalMs)
	err = state.persistLocked()
	state.mu.Unlock()
	state.controlMu.Unlock()
	if err != nil {
		// Ownership exists in memory and remains controlled by this helper.
		// The persisted `starting` state cannot be upgraded safely, so report
		// uncertainty over the authenticated endpoint and keep owning the Run.
		state.setMemoryUncertain(owner, "running-ownership-persist-failed")
		go state.monitor()
		return state.waitForTerminal()
	}
	go state.monitor()
	return state.waitForTerminal()
}

type runState struct {
	mu          sync.Mutex
	controlMu   sync.Mutex
	descriptor  launchConfig
	spool       *spool
	process     backend.Process
	snapshot    Snapshot
	startedMono time.Time
	terminal    chan struct{}
	termOnce    sync.Once
}

func (s *runState) serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *runState) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	decoder := json.NewDecoder(io.LimitReader(conn, maxRPCBytes))
	var request rpcRequest
	if err := decoder.Decode(&request); err != nil {
		writeResponse(conn, rpcResponse{Version: ProtocolVersion, Error: "invalid request"})
		return
	}
	if request.Version != ProtocolVersion || !constantEqual(request.Token, s.descriptor.Token) {
		writeResponse(conn, rpcResponse{Version: ProtocolVersion, Error: "authentication failed"})
		return
	}
	response := s.dispatch(request)
	response.Version = ProtocolVersion
	writeResponse(conn, response)
}

func (s *runState) dispatch(request rpcRequest) rpcResponse {
	switch request.Op {
	case "observe":
		snapshot, err := s.observe()
		if err != nil {
			return rpcResponse{Error: "observation unavailable"}
		}
		return rpcResponse{Snapshot: &snapshot}
	case "signal":
		if err := s.signal(request.Signal); err != nil {
			return rpcResponse{Error: "signal-failed"}
		}
		return rpcResponse{}
	case "terminate":
		result, err := s.terminate(time.Duration(request.GraceMS)*time.Millisecond, request.Reason)
		if err != nil {
			return rpcResponse{Error: "termination-failed"}
		}
		return rpcResponse{Termination: &result}
	case "input":
		if err := s.writeInput(request.Data); err != nil {
			return rpcResponse{Error: "input-write-failed"}
		}
		return rpcResponse{}
	case "resize":
		if err := s.resize(request.Rows, request.Cols); err != nil {
			return rpcResponse{Error: "resize-failed"}
		}
		return rpcResponse{}
	case "close-input":
		if err := s.closeInput(); err != nil {
			return rpcResponse{Error: "input-close-failed"}
		}
		return rpcResponse{}
	case "output":
		chunk, err := s.spool.read(request.Stream, request.Offset, request.Limit)
		if err != nil {
			return rpcResponse{Error: "output-read-failed"}
		}
		return rpcResponse{Chunk: &chunk}
	case "lease-renew":
		snapshot, err := s.renewLease(request.LeaseGeneration, request.LeaseMs)
		if errors.Is(err, ErrLeaseExpired) {
			return rpcResponse{Error: "lease-expired"}
		}
		if errors.Is(err, ErrStaleGeneration) {
			return rpcResponse{Error: "stale-generation"}
		}
		if err != nil {
			return rpcResponse{Error: "lease-renewal-failed"}
		}
		return rpcResponse{Snapshot: &snapshot}
	case "control-pause", "control-resume", "control-memory-high", "control-cpu-quota":
		evidence := s.control(request)
		return rpcResponse{Control: &evidence}
	case "control-evidence":
		if !validControlRequestID(request.RequestID) {
			return rpcResponse{Error: "invalid-control-request-id"}
		}
		record, found, gap := s.lookupControl(request.RequestID)
		return rpcResponse{ControlRecord: &record, ControlFound: found, ControlGap: &gap}
	default:
		return rpcResponse{Error: "unsupported operation"}
	}
}

type guardianPhysicalControl interface {
	Pause() error
	Resume() error
	SetMemoryHigh(int64) error
	SetCPUQuotaPercent(int64) error
}

type guardianControlCapability interface {
	SupportsCgroupFreeze() bool
	SupportsMemoryHighControl() bool
	SupportsCPUQuotaControl() bool
}

type guardianPauseObserver interface {
	CurrentPauseState() (paused bool, known bool, err error)
}

type guardianMemoryHighObserver interface {
	CurrentMemoryHigh() (value int64, unlimited bool, err error)
}

type guardianCPUQuotaObserver interface {
	CurrentCPUQuotaPercent() (value int64, unlimited bool, err error)
}

func (s *runState) control(request rpcRequest) model.ControlEventPayload {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()

	requestID := request.RequestID
	action, control, target, valid := decodeControlRequest(request)
	evidence := model.ControlEventPayload{OperationID: requestID, Control: control, Action: action}
	if valid {
		evidence.Value = int64Pointer(target)
		if request.Op == "control-memory-high" || request.Op == "control-cpu-quota" {
			evidence.Unlimited = boolPointer(false)
		}
	}
	if !validControlRequestID(requestID) {
		evidence.Outcome = "failed"
		evidence.Reason = "request-id-invalid"
		return evidence
	}
	digest := controlRequestDigest(request.Op, target)

	s.mu.Lock()
	controls := s.ensureControlStateLocked()
	if pruneControlEvidence(controls, time.Now().UTC()) {
		controls.UpdatedAt = time.Now().UTC()
		if err := s.persistLocked(); err != nil {
			s.mu.Unlock()
			evidence.Outcome = "uncertain"
			evidence.Reason = "control-evidence-prune-persist-failed"
			return evidence
		}
	}
	if previous, ok := findControlOperation(controls, requestID); ok {
		if previous.Digest != digest {
			s.mu.Unlock()
			evidence.Outcome = "failed"
			evidence.Reason = "request-id-conflict"
			return evidence
		}
		evidence = previous.Evidence
		if previous.Status == "pending" {
			evidence.Outcome = "uncertain"
			evidence.Reason = "control-effect-pending"
			s.snapshot.State = model.Uncertain
			s.snapshot.Reason = evidence.Reason
			controls.Operations[controlOperationIndex(controls, requestID)].Evidence = evidence
			controls.Operations[controlOperationIndex(controls, requestID)].Status = "completed"
			controls.Operations[controlOperationIndex(controls, requestID)].UpdatedAt = time.Now().UTC()
			controls.UpdatedAt = time.Now().UTC()
			_ = s.persistLocked()
		}
		s.mu.Unlock()
		return evidence
	}
	if len(controls.Operations) >= maxControlEvidenceOperations {
		s.mu.Unlock()
		evidence.Outcome = "failed"
		evidence.Reason = "control-evidence-capacity"
		return evidence
	}
	process, state := s.process, s.snapshot.State
	var capabilities model.Capabilities
	if s.snapshot.EffectiveCapabilities != nil {
		capabilities = *s.snapshot.EffectiveCapabilities
	}
	s.mu.Unlock()

	if !valid {
		evidence.Outcome = "failed"
		evidence.Reason = "control-value-invalid"
		return s.persistControlOperation(requestID, digest, evidence)
	}
	if process == nil || state != model.Running {
		evidence.Outcome = "failed"
		evidence.Reason = "run-not-live"
		return s.persistControlOperation(requestID, digest, evidence)
	}
	controller, hasController := process.(guardianPhysicalControl)
	if !hasController || !controlAvailable(capabilities, request.Op) {
		evidence.Outcome = "failed"
		evidence.Reason = "unsupported"
		return s.persistControlOperation(requestID, digest, evidence)
	}
	if err := capturePreviousControl(process, request.Op, &evidence); err != nil {
		evidence.Outcome = "failed"
		evidence.Reason = "previous-control-observation-unavailable"
		return s.persistControlOperation(requestID, digest, evidence)
	}
	if err := s.persistPendingControlOperation(requestID, digest, evidence); err != nil {
		evidence.Outcome = "uncertain"
		evidence.Reason = "control-intent-persist-failed"
		return evidence
	}

	var applyErr error
	switch request.Op {
	case "control-pause":
		applyErr = controller.Pause()
	case "control-resume":
		applyErr = controller.Resume()
	case "control-memory-high":
		applyErr = controller.SetMemoryHigh(target)
	case "control-cpu-quota":
		applyErr = controller.SetCPUQuotaPercent(target)
	}
	if applyErr != nil {
		evidence.Outcome = "uncertain"
		evidence.Reason = "control-effect-unverified"
		return s.completeControlOperation(requestID, evidence, applyErr)
	}
	if err := verifyCurrentControl(process, request.Op, target); err != nil {
		evidence.Outcome = "uncertain"
		evidence.Reason = "control-effect-unverified"
		return s.completeControlOperation(requestID, evidence, err)
	}
	evidence.Outcome = "applied"
	return s.completeControlOperation(requestID, evidence, nil)
}

func decodeControlRequest(request rpcRequest) (action, control string, target int64, valid bool) {
	switch request.Op {
	case "control-pause":
		return "pause", "cgroup.freeze", 1, request.ControlValue == 0
	case "control-resume":
		return "resume", "cgroup.freeze", 0, request.ControlValue == 0
	case "control-memory-high":
		return "set", "memory.high", request.ControlValue, request.ControlValue > 0
	case "control-cpu-quota":
		return "set", "cpu.max", request.ControlValue, request.ControlValue > 0
	default:
		return "", "", 0, false
	}
}

func controlRequestDigest(operation string, target int64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", operation, target)))
	return hex.EncodeToString(digest[:])
}

func validControlRequestID(requestID string) bool {
	return len(requestID) > 0 && len(requestID) <= 128 && !strings.ContainsAny(requestID, "\x00\r\n")
}

func controlAvailable(capabilities model.Capabilities, operation string) bool {
	switch operation {
	case "control-pause", "control-resume":
		return capabilities.CgroupFreeze
	case "control-memory-high":
		return capabilities.MemoryHighControl
	case "control-cpu-quota":
		return capabilities.CPUQuotaControl
	default:
		return false
	}
}

func capturePreviousControl(process backend.Process, operation string, evidence *model.ControlEventPayload) error {
	switch operation {
	case "control-pause", "control-resume":
		observer, ok := process.(guardianPauseObserver)
		if !ok {
			return errors.New("pause-state observer is unavailable")
		}
		paused, known, err := observer.CurrentPauseState()
		if err != nil || !known {
			return errors.Join(err, errors.New("pause state is unavailable"))
		}
		value := int64(0)
		if paused {
			value = 1
		}
		evidence.PreviousValue = &value
	case "control-memory-high":
		observer, ok := process.(guardianMemoryHighObserver)
		if !ok {
			return errors.New("memory.high observer is unavailable")
		}
		value, unlimited, err := observer.CurrentMemoryHigh()
		if err != nil {
			return err
		}
		if unlimited {
			value = 0
		}
		evidence.PreviousValue = &value
		evidence.PreviousUnlimited = boolPointer(unlimited)
	case "control-cpu-quota":
		observer, ok := process.(guardianCPUQuotaObserver)
		if !ok {
			return errors.New("cpu quota observer is unavailable")
		}
		value, unlimited, err := observer.CurrentCPUQuotaPercent()
		if err != nil {
			return err
		}
		if unlimited {
			value = 0
		}
		evidence.PreviousValue = &value
		evidence.PreviousUnlimited = boolPointer(unlimited)
	default:
		return errors.New("unknown control operation")
	}
	return nil
}

func verifyCurrentControl(process backend.Process, operation string, target int64) error {
	switch operation {
	case "control-pause", "control-resume":
		observer, ok := process.(guardianPauseObserver)
		if !ok {
			return errors.New("pause-state observer is unavailable")
		}
		paused, known, err := observer.CurrentPauseState()
		want := operation == "control-pause"
		if err != nil || !known || paused != want {
			return errors.Join(err, errors.New("requested frozen state was not observed"))
		}
	case "control-memory-high":
		observer, ok := process.(guardianMemoryHighObserver)
		if !ok {
			return errors.New("memory.high observer is unavailable")
		}
		value, unlimited, err := observer.CurrentMemoryHigh()
		if err != nil || unlimited || value != target {
			return errors.Join(err, errors.New("requested memory.high value was not observed"))
		}
	case "control-cpu-quota":
		observer, ok := process.(guardianCPUQuotaObserver)
		if !ok {
			return errors.New("cpu quota observer is unavailable")
		}
		value, unlimited, err := observer.CurrentCPUQuotaPercent()
		if err != nil || unlimited || value != target {
			return errors.Join(err, errors.New("requested cpu quota was not observed"))
		}
	default:
		return errors.New("unknown control operation")
	}
	return nil
}

func (s *runState) persistControlOperation(requestID, digest string, evidence model.ControlEventPayload) model.ControlEventPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.ensureControlStateLocked()
	if len(state.Operations) >= maxControlEvidenceOperations {
		evidence.Outcome = "failed"
		evidence.Reason = "control-evidence-capacity"
		return evidence
	}
	now := time.Now().UTC()
	state.Operations = append(state.Operations, ControlOperation{
		RequestID: requestID, Digest: digest, Status: "completed", Evidence: evidence,
		CreatedAt: now, UpdatedAt: now,
	})
	refreshControlRetainedFrom(state)
	state.UpdatedAt = now
	if err := s.persistLocked(); err != nil {
		state.Operations = state.Operations[:len(state.Operations)-1]
		evidence.Outcome = "uncertain"
		evidence.Reason = "control-evidence-persist-failed"
		return evidence
	}
	return evidence
}

func (s *runState) persistPendingControlOperation(requestID, digest string, evidence model.ControlEventPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.ensureControlStateLocked()
	if len(state.Operations) >= maxControlEvidenceOperations {
		return errors.New("guardian: control evidence capacity is full")
	}
	now := time.Now().UTC()
	pending := evidence
	pending.Outcome = "uncertain"
	pending.Reason = "control-effect-pending"
	state.Operations = append(state.Operations, ControlOperation{
		RequestID: requestID, Digest: digest, Status: "pending", Evidence: pending,
		CreatedAt: now, UpdatedAt: now,
	})
	refreshControlRetainedFrom(state)
	state.UpdatedAt = now
	if err := s.persistLocked(); err != nil {
		return err
	}
	return nil
}

func (s *runState) completeControlOperation(requestID string, evidence model.ControlEventPayload, effectErr error) model.ControlEventPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.snapshot.Controls
	index := controlOperationIndex(state, requestID)
	if index < 0 {
		evidence.Outcome = "uncertain"
		evidence.Reason = "control-intent-missing"
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = evidence.Reason
		_ = s.persistLocked()
		return evidence
	}
	now := time.Now().UTC()
	state.Operations[index].Status = "completed"
	state.Operations[index].Evidence = evidence
	state.Operations[index].UpdatedAt = now
	state.UpdatedAt = now
	if evidence.Outcome == "applied" {
		switch evidence.Control {
		case "cgroup.freeze":
			paused := evidence.Action == "pause"
			state.Paused = &paused
		case "memory.high":
			value := dereferenceControlValue(evidence.Value)
			state.MemoryHighBytes = &value
		case "cpu.max":
			value := dereferenceControlValue(evidence.Value)
			state.CPUQuotaPercent = &value
		}
	}
	if evidence.Outcome == "uncertain" {
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = "physical-control-outcome-unproven"
	}
	if err := s.persistLocked(); err != nil {
		evidence.Outcome = "uncertain"
		evidence.Reason = "control-evidence-persist-failed"
		state.Operations[index].Evidence = evidence
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = evidence.Reason
		_ = s.persistLocked()
		return evidence
	}
	if effectErr != nil && evidence.Outcome == "applied" {
		// Defensive: a caller must not claim success if it reported an effect
		// error while assembling the evidence.
		evidence.Outcome = "uncertain"
		evidence.Reason = "control-effect-unverified"
		state.Operations[index].Evidence = evidence
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = evidence.Reason
		_ = s.persistLocked()
	}
	return evidence
}

func (s *runState) ensureControlStateLocked() *ControlState {
	if s.snapshot.Controls == nil {
		s.snapshot.Controls = &ControlState{}
	}
	return s.snapshot.Controls
}

func findControlOperation(state *ControlState, requestID string) (ControlOperation, bool) {
	index := controlOperationIndex(state, requestID)
	if index < 0 {
		return ControlOperation{}, false
	}
	return state.Operations[index], true
}

func controlOperationIndex(state *ControlState, requestID string) int {
	if state == nil {
		return -1
	}
	for index := len(state.Operations) - 1; index >= 0; index-- {
		if state.Operations[index].RequestID == requestID {
			return index
		}
	}
	return -1
}

func pruneControlEvidence(state *ControlState, now time.Time) bool {
	cutoff := now.Add(-24 * time.Hour)
	removed := 0
	for removed < len(state.Operations) && state.Operations[removed].CreatedAt.Before(cutoff) {
		operation := state.Operations[removed]
		if state.EvidenceGap.ExpiredCount == 0 {
			from := operation.CreatedAt
			state.EvidenceGap.ExpiredFrom = &from
		}
		through := operation.CreatedAt
		state.EvidenceGap.ExpiredThrough = &through
		state.EvidenceGap.ExpiredCount++
		removed++
	}
	if removed > 0 {
		state.Operations = append([]ControlOperation(nil), state.Operations[removed:]...)
	}
	return refreshControlRetainedFrom(state) || removed > 0
}

func refreshControlRetainedFrom(state *ControlState) bool {
	var next *time.Time
	if len(state.Operations) > 0 {
		retainedFrom := state.Operations[0].CreatedAt
		next = &retainedFrom
	}
	if (next == nil && state.EvidenceGap.RetainedFrom == nil) ||
		(next != nil && state.EvidenceGap.RetainedFrom != nil && next.Equal(*state.EvidenceGap.RetainedFrom)) {
		return false
	}
	state.EvidenceGap.RetainedFrom = next
	return true
}

func (s *runState) lookupControl(requestID string) (ControlOperation, bool, ControlEvidenceGap) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	var gap ControlEvidenceGap
	if state := s.snapshot.Controls; state != nil {
		priorOperations := state.Operations
		priorGap := state.EvidenceGap
		priorUpdatedAt := state.UpdatedAt
		if pruneControlEvidence(state, time.Now().UTC()) {
			state.UpdatedAt = time.Now().UTC()
			if err := s.persistLocked(); err != nil {
				state.Operations = priorOperations
				state.EvidenceGap = priorGap
				state.UpdatedAt = priorUpdatedAt
			}
		}
		gap = state.EvidenceGap
		if operation, ok := findControlOperation(state, requestID); ok {
			return operation, true, gap
		}
	}
	return ControlOperation{}, false, gap
}

func dereferenceControlValue(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func int64Pointer(value int64) *int64 { return &value }

func boolPointer(value bool) *bool { return &value }

func (s *runState) observe() (Snapshot, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.State != model.Terminal && s.process != nil {
		resources, observeErr := s.process.Observe()
		observedAt := time.Now().UTC()
		s.snapshot.LastResourceSampleAt = &observedAt
		if observeErr == nil {
			s.snapshot.Resources = mergeResources(s.snapshot.Resources, resources, s.descriptor.SampleIntervalMs, s.snapshot.EffectiveCapabilities)
		} else {
			s.snapshot.Resources = unavailableCurrent(s.snapshot.Resources)
		}
		if s.snapshot.State == model.Running && s.snapshot.Controls != nil {
			if err := verifyPersistedControlState(s.process, s.snapshot.Controls); err != nil {
				s.snapshot.State = model.Uncertain
				s.snapshot.Reason = "physical-control-state-unavailable-or-changed"
			}
		}
		s.snapshot.Resources.SampleIntervalMs = s.descriptor.SampleIntervalMs
		s.captureOutputEvidenceLocked()
		if err := s.persistLocked(); err != nil {
			return Snapshot{}, err
		}
	}
	return s.snapshot, nil
}

func verifyPersistedControlState(process backend.Process, state *ControlState) error {
	if state == nil {
		return nil
	}
	if state.Paused != nil {
		observer, ok := process.(guardianPauseObserver)
		if !ok {
			return errors.New("pause-state observer is unavailable")
		}
		paused, known, err := observer.CurrentPauseState()
		if err != nil || !known || paused != *state.Paused {
			return errors.Join(err, errors.New("persisted cgroup freeze state does not match the kernel"))
		}
	}
	if state.MemoryHighBytes != nil {
		observer, ok := process.(guardianMemoryHighObserver)
		if !ok {
			return errors.New("memory.high observer is unavailable")
		}
		value, unlimited, err := observer.CurrentMemoryHigh()
		if err != nil || unlimited || value != *state.MemoryHighBytes {
			return errors.Join(err, errors.New("persisted memory.high state does not match the kernel"))
		}
	}
	if state.CPUQuotaPercent != nil {
		observer, ok := process.(guardianCPUQuotaObserver)
		if !ok {
			return errors.New("cpu quota observer is unavailable")
		}
		value, unlimited, err := observer.CurrentCPUQuotaPercent()
		if err != nil || unlimited || value != *state.CPUQuotaPercent {
			return errors.Join(err, errors.New("persisted cpu quota does not match the kernel"))
		}
	}
	return nil
}

func (s *runState) signal(name string) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	process := s.process
	state := s.snapshot.State
	s.mu.Unlock()
	if process == nil || state == model.Terminal {
		return errors.New("guardian: Run is not live")
	}
	return process.Signal(name)
}

func (s *runState) terminate(grace time.Duration, reason string) (backend.TerminationResult, error) {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	if s.snapshot.State == model.Terminal {
		result := s.snapshot.Termination
		if !result.TreeEmpty {
			return backend.TerminationResult{}, errors.New("guardian: terminal receipt lacks tree-empty proof")
		}
		s.mu.Unlock()
		return result, nil
	}
	process := s.process
	if process == nil {
		s.mu.Unlock()
		return backend.TerminationResult{}, ErrUncertain
	}
	s.snapshot.State = model.Terminating
	s.snapshot.Termination.Requested = true
	s.snapshot.TerminationReason = boundedReason(reason)
	s.captureOutputEvidenceLocked()
	if err := s.persistLocked(); err != nil {
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = "termination-request-persist-failed"
		s.mu.Unlock()
		return backend.TerminationResult{}, ErrUncertain
	}
	s.mu.Unlock()
	if grace < 0 {
		grace = 0
	}
	result, err := process.Terminate(grace)
	s.mu.Lock()
	s.snapshot.Termination.Requested = result.Requested || s.snapshot.Termination.Requested
	s.snapshot.Termination.Forced = result.Forced || s.snapshot.Termination.Forced
	if err == nil {
		s.snapshot.Termination.TreeEmpty = result.TreeEmpty || s.snapshot.Termination.TreeEmpty
	}
	if result.Outcome != "" {
		s.snapshot.Termination.Outcome = result.Outcome
	}
	s.captureOutputEvidenceLocked()
	if err != nil || !s.snapshot.Termination.TreeEmpty {
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = "termination-outcome-unproven"
		_ = s.persistLocked()
		s.mu.Unlock()
		return result, ErrUncertain
	}
	if persistErr := s.persistLocked(); persistErr != nil {
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = "termination-result-persist-failed"
		_ = s.persistLocked()
		s.mu.Unlock()
		return result, ErrUncertain
	}
	s.mu.Unlock()
	return result, nil
}

func (s *runState) writeInput(data []byte) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if len(data) > maxRPCBytes/2 {
		return errors.New("guardian: input write exceeds bounded frame")
	}
	s.mu.Lock()
	process, state := s.process, s.snapshot.State
	s.mu.Unlock()
	if process == nil || state == model.Terminal {
		return errors.New("guardian: Run is not live")
	}
	return process.WriteInput(data)
}

func (s *runState) resize(rows, cols uint16) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	process, state := s.process, s.snapshot.State
	s.mu.Unlock()
	if process == nil || state == model.Terminal {
		return errors.New("guardian: Run is not live")
	}
	return process.Resize(rows, cols)
}

func (s *runState) closeInput() error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	process, state := s.process, s.snapshot.State
	s.mu.Unlock()
	if process == nil || state == model.Terminal {
		return errors.New("guardian: Run is not live")
	}
	return process.CloseInput()
}

func (s *runState) renewLease(expectedGeneration uint64, leaseMs int64) (Snapshot, error) {
	if leaseMs < 1000 || leaseMs > maxLeaseDurationMs {
		return Snapshot{}, errors.New("guardian: lease duration is out of range")
	}
	s.controlMu.Lock()
	s.mu.Lock()
	current := s.snapshot
	if current.LeaseExpiry == nil || current.LeaseGeneration == 0 {
		s.mu.Unlock()
		s.controlMu.Unlock()
		return Snapshot{}, errors.New("guardian: Run is not lease-bound")
	}
	if !time.Now().Before(*current.LeaseExpiry) {
		s.mu.Unlock()
		s.controlMu.Unlock()
		go s.terminate(time.Duration(s.descriptor.TerminationGraceMs)*time.Millisecond, "lease-expired")
		return Snapshot{}, ErrLeaseExpired
	}
	if current.LeaseGeneration == expectedGeneration+1 && current.LastLeaseExpectedGeneration == expectedGeneration && current.LastLeaseMs == leaseMs {
		s.mu.Unlock()
		s.controlMu.Unlock()
		return current, nil
	}
	if current.LeaseGeneration != expectedGeneration {
		s.mu.Unlock()
		s.controlMu.Unlock()
		return Snapshot{}, ErrStaleGeneration
	}
	if current.State != model.Starting && current.State != model.Running {
		s.mu.Unlock()
		s.controlMu.Unlock()
		return Snapshot{}, ErrUncertain
	}
	now := time.Now().UTC()
	expiry := now.Add(time.Duration(leaseMs) * time.Millisecond)
	next := current
	next.LastLeaseExpectedGeneration = expectedGeneration
	next.LastLeaseMs = leaseMs
	next.LeaseGeneration++
	next.LeaseExpiry = &expiry
	if err := writeSnapshot(s.descriptor.Dir, next); err != nil {
		s.mu.Unlock()
		s.controlMu.Unlock()
		return Snapshot{}, ErrUncertain
	}
	s.snapshot = next
	s.mu.Unlock()
	s.controlMu.Unlock()
	return next, nil
}

func (s *runState) monitor() {
	sample := time.NewTicker(time.Duration(s.descriptor.SampleIntervalMs) * time.Millisecond)
	defer sample.Stop()
	// Guardian is the only process that owns the monotonic execution start
	// instant. Keep deadline decisions in this loop instead of deriving them
	// from the wall-clock StartedAt value in Supervisor.
	deadline := time.NewTicker(50 * time.Millisecond)
	defer deadline.Stop()
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		var exit backend.Exit
		waitProven := false
		for {
			if !waitProven {
				observed, err := s.process.Wait()
				if err != nil {
					s.markUncertain("owned-tree-empty-unproven")
					time.Sleep(time.Second)
					continue
				}
				exit = observed
				waitProven = true
			}
			if s.finish(exit) {
				return
			}
			time.Sleep(time.Second)
		}
	}()
	for {
		select {
		case <-wait:
			return
		case <-s.terminal:
			return
		case <-sample.C:
			s.sample()
		case <-deadline.C:
			s.enforceDeadlines()
		}
	}
}

func (s *runState) enforceDeadlines() {
	s.mu.Lock()
	if s.snapshot.State != model.Running || s.snapshot.Termination.Requested {
		s.mu.Unlock()
		return
	}
	startedAt := s.startedMono
	leaseExpiry := s.snapshot.LeaseExpiry
	reason := ""
	if s.descriptor.Spec.Limits.WallTimeMs > 0 && !startedAt.IsZero() && time.Since(startedAt) >= time.Duration(s.descriptor.Spec.Limits.WallTimeMs)*time.Millisecond {
		reason = "timed-out"
	} else if leaseExpiry != nil && !time.Now().Before(*leaseExpiry) {
		reason = "lease-expired"
	}
	s.mu.Unlock()
	if reason != "" {
		_, _ = s.terminate(time.Duration(s.descriptor.TerminationGraceMs)*time.Millisecond, reason)
	}
}

func (s *runState) sample() {
	s.mu.Lock()
	if s.process == nil || s.snapshot.State == model.Terminal {
		s.mu.Unlock()
		return
	}
	process := s.process
	resources, observeErr := process.Observe()
	observedAt := time.Now().UTC()
	s.snapshot.LastResourceSampleAt = &observedAt
	if observeErr == nil {
		s.snapshot.Resources = mergeResources(s.snapshot.Resources, resources, s.descriptor.SampleIntervalMs, s.snapshot.EffectiveCapabilities)
	} else {
		s.snapshot.Resources = unavailableCurrent(s.snapshot.Resources)
	}
	s.snapshot.Resources.SampleIntervalMs = s.descriptor.SampleIntervalMs
	if limits, ok := process.(backend.LimitEvidence); ok {
		if outcome := normalizeLimitOutcome(limits.LimitOutcome()); outcome != "" && s.snapshot.LimitOutcome == "" {
			s.snapshot.LimitOutcome = outcome
		}
	}
	s.captureOutputEvidenceLocked()
	err := s.persistLocked()
	terminationReason := ""
	if err == nil && s.snapshot.State == model.Running && !s.snapshot.Termination.Requested && s.snapshot.LimitOutcome != "" {
		// Kernel resource-limit events are independent of wall-time and lease
		// deadlines. Deadline enforcement remains centralized in
		// enforceDeadlines, which consults Guardian's monotonic start instant.
		terminationReason = s.snapshot.LimitOutcome
	}
	s.mu.Unlock()
	if terminationReason != "" {
		_, _ = s.terminate(time.Duration(s.descriptor.TerminationGraceMs)*time.Millisecond, terminationReason)
	}
}

func (s *runState) finish(exit backend.Exit) bool {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	// Physical terminal proof is independent from output durability. A failed
	// spool sync marks history incomplete but must not turn a proven empty tree
	// into an uncertain process outcome.
	_ = s.spool.sync()
	s.mu.Lock()
	resources := s.snapshot.Resources
	limitOutcome := s.snapshot.LimitOutcome
	observed, observeErr := s.process.Observe()
	observedAt := time.Now().UTC()
	s.snapshot.LastResourceSampleAt = &observedAt
	if observeErr == nil {
		resources = mergeResources(s.snapshot.Resources, observed, s.descriptor.SampleIntervalMs, s.snapshot.EffectiveCapabilities)
	} else {
		resources = unavailableCurrent(resources)
	}
	resources.SampleIntervalMs = s.descriptor.SampleIntervalMs
	s.snapshot.Resources.SampleIntervalMs = s.descriptor.SampleIntervalMs
	output := s.captureOutputEvidenceLocked()
	s.mu.Unlock()
	finished := exit.FinishedAt
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	started := exit.StartedAt
	if started.IsZero() {
		s.mu.Lock()
		if s.snapshot.StartedAt != nil {
			started = *s.snapshot.StartedAt
		}
		s.mu.Unlock()
	}
	outcome := exit.Outcome
	if typedOutcome := normalizeLimitOutcome(outcome); typedOutcome != "" {
		limitOutcome = typedOutcome
		outcome = "resource-limit"
	}
	if limitOutcome != "" {
		outcome = "resource-limit"
	}
	if outcome == "" {
		if exit.Signal != "" {
			outcome = "signaled"
		} else {
			outcome = "exited"
		}
	}
	s.mu.Lock()
	if s.snapshot.LimitOutcome != "" {
		limitOutcome = s.snapshot.LimitOutcome
		outcome = "resource-limit"
	}
	switch s.snapshot.TerminationReason {
	case "timed-out":
		outcome = "timed-out"
	case "lease-expired", "cancelled":
		outcome = "cancelled"
	}
	s.snapshot.State = model.Terminal
	s.snapshot.LimitOutcome = limitOutcome
	s.snapshot.Resources = resources
	s.snapshot.Output = output
	s.snapshot.FinishedAt = &finished
	s.snapshot.Termination.TreeEmpty = true
	receipt := model.Receipt{
		Version: 1, RunID: s.snapshot.RunID, Outcome: outcome, ExitCode: exit.ExitCode,
		Signal: exit.Signal, StartedAt: timePtrOrNil(started), FinishedAt: finished,
		Resources: resources, Output: output,
		TerminationRequested: s.snapshot.Termination.Requested,
		Forced:               s.snapshot.Termination.Forced,
		Cleanup:              "complete",
	}
	if s.snapshot.EffectiveCapabilities != nil {
		effective := *s.snapshot.EffectiveCapabilities
		effective.Signals = append([]string(nil), effective.Signals...)
		receipt.EffectiveCapabilities = &effective
		receipt.Capabilities = *s.snapshot.EffectiveCapabilities
	}
	s.snapshot.Receipt = &receipt
	if err := s.persistLocked(); err != nil {
		s.snapshot.State = model.Uncertain
		s.snapshot.Receipt = nil
		s.snapshot.Reason = "terminal-receipt-persist-failed"
		_ = s.persistLocked()
		s.mu.Unlock()
		return false
	}
	s.termOnce.Do(func() { close(s.terminal) })
	s.mu.Unlock()
	return true
}

func (s *runState) markStartupFailure(reason string) {
	_ = s.spool.sync()
	finished := time.Now().UTC()
	s.mu.Lock()
	s.snapshot.State = model.Terminal
	s.snapshot.Reason = boundedReason(reason)
	s.snapshot.FinishedAt = &finished
	s.snapshot.Termination.TreeEmpty = true
	s.captureOutputEvidenceLocked()
	receipt := model.Receipt{Version: 1, RunID: s.snapshot.RunID, Outcome: "startup-failed", FinishedAt: finished, Resources: unavailableResourcesFor(s.descriptor.SampleIntervalMs), Output: s.snapshot.Output, Cleanup: "complete"}
	s.snapshot.Receipt = &receipt
	_ = s.persistLocked()
	s.termOnce.Do(func() { close(s.terminal) })
	s.mu.Unlock()
}

func (s *runState) markNoStartTerminal(outcome, reason string) {
	finished := time.Now().UTC()
	s.mu.Lock()
	s.snapshot.State = model.Terminal
	s.snapshot.Reason = boundedReason(reason)
	s.snapshot.TerminationReason = boundedReason(reason)
	s.snapshot.Termination.TreeEmpty = true
	s.snapshot.FinishedAt = &finished
	s.captureOutputEvidenceLocked()
	receipt := model.Receipt{
		Version: 1, RunID: s.snapshot.RunID, Outcome: outcome,
		FinishedAt: finished, Resources: unavailableResourcesFor(s.descriptor.SampleIntervalMs),
		Output: s.snapshot.Output, Cleanup: "complete",
	}
	s.snapshot.Receipt = &receipt
	_ = s.persistLocked()
	s.termOnce.Do(func() { close(s.terminal) })
	s.mu.Unlock()
}

func (s *runState) markUncertain(reason string) {
	s.mu.Lock()
	if s.snapshot.State != model.Terminal {
		s.snapshot.State = model.Uncertain
		s.snapshot.Reason = boundedReason(reason)
		s.captureOutputEvidenceLocked()
		_ = s.persistLocked()
	}
	s.mu.Unlock()
}

func (s *runState) markUncertainWithOwnership(owner model.Ownership, reason string) {
	s.mu.Lock()
	s.snapshot.State = model.Uncertain
	s.snapshot.Ownership = &owner
	s.snapshot.Reason = boundedReason(reason)
	s.captureOutputEvidenceLocked()
	_ = s.persistLocked()
	s.mu.Unlock()
}

func (s *runState) setMemoryUncertain(owner model.Ownership, reason string) {
	s.mu.Lock()
	s.snapshot.State = model.Uncertain
	s.snapshot.Ownership = &owner
	s.snapshot.Reason = boundedReason(reason)
	s.mu.Unlock()
}

func (s *runState) persist() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistLocked()
}

func (s *runState) captureOutputEvidenceLocked() model.Output {
	output, latestByStream := s.spool.evidence()
	s.snapshot.Output = output
	if len(latestByStream) == 0 {
		return output
	}

	merged := make(map[string]time.Time, 3)
	for _, stream := range []string{"stdout", "stderr", "pty"} {
		latest, exists := s.snapshot.OutputLastWriteAt[stream]
		if current, ok := latestByStream[stream]; ok && (!exists || current.After(latest)) {
			latest, exists = current, true
		}
		if exists && !latest.IsZero() {
			merged[stream] = latest
		}
	}
	if len(merged) == 0 {
		return output
	}
	s.snapshot.OutputLastWriteAt = merged
	var latest time.Time
	for _, at := range merged {
		if at.After(latest) {
			latest = at
		}
	}
	if s.snapshot.LastOutputAt == nil || latest.After(*s.snapshot.LastOutputAt) {
		s.snapshot.LastOutputAt = timePtrOrNil(latest)
	}
	return output
}

func (s *runState) persistLocked() error { return writeSnapshot(s.descriptor.Dir, s.snapshot) }

func (s *runState) waitForTerminal() error {
	<-s.terminal
	return nil
}

func effectiveCapabilities(selected backend.Backend, process backend.Process, interactive bool) model.Capabilities {
	capabilities := selected.Capabilities()
	if effective, ok := process.(interface{ EffectiveCapabilities() model.Capabilities }); ok {
		capabilities = effective.EffectiveCapabilities()
	} else {
		capabilities.Backend = process.Ownership().Backend
		capabilities.PTY = interactive && capabilities.PTY
	}
	// Physical controls are advertised for a Run only when its concrete
	// backend handle exposes the corresponding cgroup control surface.
	capabilities.CgroupFreeze = false
	capabilities.MemoryHighControl = false
	capabilities.CPUQuotaControl = false
	if controls, ok := process.(guardianControlCapability); ok {
		capabilities.CgroupFreeze = controls.SupportsCgroupFreeze()
		capabilities.MemoryHighControl = controls.SupportsMemoryHighControl()
		capabilities.CPUQuotaControl = controls.SupportsCPUQuotaControl()
	}
	capabilities.Signals = append([]string(nil), capabilities.Signals...)
	return capabilities
}

func writeResponse(conn net.Conn, response rpcResponse) {
	data, err := json.Marshal(response)
	if err != nil || len(data) > maxRPCBytes {
		data = []byte(`{"version":1,"error":"response unavailable"}`)
	}
	_, _ = conn.Write(append(data, '\n'))
}

func constantEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func boundedReason(reason string) string {
	if len(reason) == 0 {
		return ""
	}
	if len(reason) > 64 {
		return "requested"
	}
	for _, char := range reason {
		if !(char >= 'a' && char <= 'z') && !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') && char != '_' && char != '-' && char != '.' && char != ':' {
			return "requested"
		}
	}
	return reason
}

func normalizeLimitOutcome(outcome string) string {
	if outcome == "" || len(outcome) > 96 {
		return ""
	}
	for _, char := range outcome {
		if !(char >= 'a' && char <= 'z') && !(char >= 'A' && char <= 'Z') && !(char >= '0' && char <= '9') && char != '_' && char != '-' && char != '.' && char != ':' {
			return ""
		}
	}
	if outcome == "resource-limit" || len(outcome) > len("resource-limit:") && outcome[:len("resource-limit:")] == "resource-limit:" {
		return outcome
	}
	return ""
}

func timePtrOrNil(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func mergeResources(previous, current model.Resources, sampleIntervalMs int64, capabilities *model.Capabilities) model.Resources {
	taskSupported := current.TaskCount.Status != ""
	if capabilities != nil {
		taskSupported = capabilities.TaskTelemetry
	}
	if !taskSupported {
		current.TaskCount = model.Metric{Status: "unsupported"}
		current.PeakTaskCount = model.Metric{Status: "unsupported"}
	} else if current.TaskCount.Status == "" {
		current.TaskCount = model.Metric{Status: "unavailable"}
	}
	out := current
	out.PeakMemoryBytes = maxMetric(previous.PeakMemoryBytes, current.PeakMemoryBytes, current.MemoryBytes)
	out.PeakProcessCount = maxMetric(previous.PeakProcessCount, current.PeakProcessCount, current.ProcessCount)
	out.PeakTaskCount = maxMetric(previous.PeakTaskCount, current.PeakTaskCount, current.TaskCount)
	if current.CPUTimeNs.Status == "measured" || current.CPUTimeNs.Status == "unsupported" {
		out.CPUTimeNs = current.CPUTimeNs
	} else if previous.CPUTimeNs.Status == "measured" {
		out.CPUTimeNs = previous.CPUTimeNs
	}
	out.SampleIntervalMs = sampleIntervalMs
	return out
}

func maxMetric(previous, reported, current model.Metric) model.Metric {
	best := previous
	for _, candidate := range []model.Metric{reported, current} {
		if candidate.Status == "unsupported" && best.Status != "measured" {
			best = candidate
		}
		if candidate.Status == "measured" && (best.Status != "measured" || candidate.Value > best.Value) {
			best = candidate
		}
	}
	if best.Status == "" {
		best.Status = "unavailable"
	}
	return best
}

func unavailableCurrent(previous model.Resources) model.Resources {
	out := previous
	for _, metric := range []*model.Metric{&out.MemoryBytes, &out.CPUTimeNs, &out.ProcessCount, &out.TaskCount} {
		if metric.Status != "unsupported" {
			*metric = model.Metric{Status: "unavailable"}
		}
	}
	return out
}
