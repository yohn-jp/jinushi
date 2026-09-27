package supervisor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/yohn-jp/jinushi/internal/guardian"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

type guardianRunControls interface {
	Pause(string) (model.ControlEventPayload, error)
	Resume(string) (model.ControlEventPayload, error)
	SetMemoryHigh(string, int64) (model.ControlEventPayload, error)
	SetCPUQuotaPercent(string, int64) (model.ControlEventPayload, error)
	LookupControl(string) (guardian.ControlOperation, bool, guardian.ControlEvidenceGap, error)
}

type guardianControlLookup interface {
	LookupControl(runID, requestID string) (guardian.ControlOperation, bool, guardian.ControlEvidenceGap, error)
}

var (
	errControlJournalGap        = errors.New("control operation is absent from retained journal with a history gap; deduplication cannot be proven")
	errControlEvidenceReconcile = errors.New("Guardian control evidence could not be reconciled")
)

func (s *Service) pause(req protocol.Request) protocol.Response { return s.changeRunControl(req) }

func (s *Service) resume(req protocol.Request) protocol.Response { return s.changeRunControl(req) }

func (s *Service) setMemoryHigh(req protocol.Request) protocol.Response {
	return s.changeRunControl(req)
}

func (s *Service) setCPUQuota(req protocol.Request) protocol.Response {
	return s.changeRunControl(req)
}

func (s *Service) changeRunControl(req protocol.Request) protocol.Response {
	return s.mutateControl(req, func(a *active) (func() error, *protocol.Failure) {
		capabilities := a.run.EffectiveCapabilities
		if capabilities == nil {
			return nil, &protocol.Failure{Code: "unsupported-capability", Message: "Run control capability is unavailable"}
		}
		controls, ok := a.process.(guardianRunControls)
		if !ok {
			return nil, &protocol.Failure{Code: "unsupported-capability", Message: "Run control backend is unavailable"}
		}
		switch req.Op {
		case "pause":
			if !capabilities.CgroupFreeze {
				return nil, &protocol.Failure{Code: "unsupported-capability", Message: "cgroup freeze is unavailable for this Run"}
			}
			return func() error {
				return s.applyPhysicalControl(req, controls, func() (model.ControlEventPayload, error) {
					return controls.Pause(req.RequestID)
				})
			}, nil
		case "resume":
			if !capabilities.CgroupFreeze {
				return nil, &protocol.Failure{Code: "unsupported-capability", Message: "cgroup freeze is unavailable for this Run"}
			}
			return func() error {
				return s.applyPhysicalControl(req, controls, func() (model.ControlEventPayload, error) {
					return controls.Resume(req.RequestID)
				})
			}, nil
		case "memory-high":
			if req.MemoryHighBytes <= 0 {
				return nil, &protocol.Failure{Code: "invalid-request", Message: "memoryHighBytes must be positive"}
			}
			if !capabilities.MemoryHighControl {
				return nil, &protocol.Failure{Code: "unsupported-capability", Message: "memory.high control is unavailable for this Run"}
			}
			return func() error {
				return s.applyPhysicalControl(req, controls, func() (model.ControlEventPayload, error) {
					return controls.SetMemoryHigh(req.RequestID, req.MemoryHighBytes)
				})
			}, nil
		case "cpu-quota":
			if req.CPUQuotaPercent <= 0 || req.CPUQuotaPercent > (1<<63-1)/1000 {
				return nil, &protocol.Failure{Code: "invalid-request", Message: "cpuQuotaPercent is outside the finite Linux cgroup range"}
			}
			if !capabilities.CPUQuotaControl {
				return nil, &protocol.Failure{Code: "unsupported-capability", Message: "CPU quota control is unavailable for this Run"}
			}
			return func() error {
				return s.applyPhysicalControl(req, controls, func() (model.ControlEventPayload, error) {
					return controls.SetCPUQuotaPercent(req.RequestID, req.CPUQuotaPercent)
				})
			}, nil
		default:
			return nil, &protocol.Failure{Code: "invalid-request", Message: "unknown physical Run control"}
		}
	})
}

func (s *Service) applyPhysicalControl(req protocol.Request, controls guardianRunControls, apply func() (model.ControlEventPayload, error)) error {
	evidence, err := apply()
	if err != nil {
		recovered, found, gap, lookupErr := controls.LookupControl(req.RequestID)
		if lookupErr == nil && found {
			if validGuardianControlRecord(req, recovered) {
				return s.persistRecoveredControlEvent(req.RunID, recovered.Evidence)
			}
			if validGuardianControlUncertainRecord(req, recovered) {
				if persistErr := s.persistRecoveredControlEvent(req.RunID, recovered.Evidence); persistErr != nil {
					return persistErr
				}
				return errors.New("Guardian control outcome is explicitly uncertain")
			}
			return errors.Join(err, s.persistControlEvidenceGap(req, "mismatch", "Guardian control record does not match the requested mutation"))
		}
		outcome, message := "unavailable", "Guardian control result could not be read"
		if lookupErr == nil && gap.ExpiredCount > 0 {
			outcome, message = "expired", "Guardian control result expired before recovery"
		}
		return errors.Join(err, s.persistControlEvidenceGap(req, outcome, message))
	}
	if !validGuardianControlEvidence(req, evidence) {
		return errors.Join(errors.New("Guardian control evidence does not match the requested mutation"), s.persistControlEvidenceGap(req, "mismatch", "Guardian control evidence does not match the requested mutation"))
	}
	err = s.persistEvent(req.RunID, model.Event{
		Kind: model.EventControlChanged, ObservedAt: time.Now().UTC(),
		Payload: &model.EventPayload{Control: &evidence},
	})
	return err
}

func (s *Service) controlReplayRequest(req protocol.Request, request store.ControlRequest, process physical) protocol.Response {
	if request.Status != store.ControlRequestPending || !isRunControlOperation(req.Op) {
		return s.controlReplay(req.RunID, request)
	}
	digest, err := mutationDigest(controlIdentityFor(req))
	if err != nil || digest != request.MutationDigest {
		return failure("control-uncertain", "stored control identity does not match this request")
	}
	operation, found, gap, err := s.lookupGuardianControl(req.RunID, req.RequestID, process)
	if err != nil || !found {
		outcome, message := "unavailable", "Guardian has no retained result for the pending mutation"
		if gap.ExpiredCount > 0 {
			outcome, message = "expired", "Guardian control evidence expired"
		}
		if gapErr := s.persistControlEvidenceGap(req, outcome, message); gapErr != nil {
			return failure("control-uncertain", "Guardian control result is unavailable and its evidence gap could not be persisted")
		}
		return failure("control-uncertain", "Guardian control result is unavailable; the mutation will not be repeated")
	}
	if validGuardianControlUncertainRecord(req, operation) {
		if err := s.persistRecoveredControlEvent(req.RunID, operation.Evidence); err != nil {
			return failure("control-uncertain", "Guardian uncertain control evidence could not be persisted")
		}
		if err := s.store.CompleteControlRequest(req.RunID, req.RequestID, store.ControlRequestUncertain, "control-uncertain", "Guardian retained an unproven physical control outcome", time.Now().UTC()); err != nil {
			return failure("control-uncertain", "Guardian control outcome is uncertain and its disposition could not be persisted")
		}
		return failure("control-uncertain", "Guardian retained an unproven physical control outcome; the mutation will not be repeated")
	}
	if !validGuardianControlRecord(req, operation) {
		if gapErr := s.persistControlEvidenceGap(req, "mismatch", "Guardian control result does not match the pending mutation"); gapErr != nil {
			return failure("control-uncertain", "Guardian control result mismatch could not be journaled")
		}
		return failure("control-uncertain", "Guardian control result does not match the pending mutation")
	}
	if err := s.persistRecoveredControlEvent(req.RunID, operation.Evidence); err != nil {
		return failure("control-uncertain", "Guardian proved the control result but its journal evidence could not be persisted")
	}
	if err := s.store.CompleteControlRequest(req.RunID, req.RequestID, store.ControlRequestSucceeded, "", "", time.Now().UTC()); err != nil {
		return failure("control-uncertain", "Guardian proved the control result but its durable disposition could not be persisted")
	}
	return s.controlReplay(req.RunID, store.ControlRequest{Status: store.ControlRequestSucceeded})
}

func (s *Service) lookupGuardianControl(runID, requestID string, process physical) (guardian.ControlOperation, bool, guardian.ControlEvidenceGap, error) {
	if process != nil {
		if lookup, ok := process.(interface {
			LookupControl(string) (guardian.ControlOperation, bool, guardian.ControlEvidenceGap, error)
		}); ok {
			return lookup.LookupControl(requestID)
		}
	}
	if lookup, ok := s.backend.(guardianControlLookup); ok {
		return lookup.LookupControl(runID, requestID)
	}
	return guardian.ControlOperation{}, false, guardian.ControlEvidenceGap{}, errors.New("Guardian control evidence lookup is unavailable")
}

func (s *Service) persistRecoveredControlEvent(runID string, evidence model.ControlEventPayload) error {
	journalGap := false
	for after := uint64(0); ; {
		events, _, gap, err := s.store.Events(runID, after, 1000)
		if err != nil {
			return err
		}
		journalGap = journalGap || gap
		for _, event := range events {
			if event.Payload == nil || event.Payload.Control == nil || event.Payload.Control.OperationID != evidence.OperationID {
				continue
			}
			if (event.Kind != model.EventControlChanged && event.Kind != model.EventControlRecovered) || !reflect.DeepEqual(*event.Payload.Control, evidence) {
				return errors.New("control operation ID already has conflicting journal evidence")
			}
			return nil
		}
		if len(events) < 1000 {
			break
		}
		next := events[len(events)-1].Seq
		if next <= after {
			return errors.New("control journal cursor did not advance")
		}
		after = next
	}
	if journalGap {
		return errControlJournalGap
	}
	err := s.persistEvent(runID, model.Event{
		Kind: model.EventControlRecovered, ObservedAt: time.Now().UTC(),
		Payload: &model.EventPayload{Control: &evidence},
	})
	return err
}

func (s *Service) persistControlEvidenceGap(req protocol.Request, outcome, message string) error {
	message += ";requestId=" + req.RequestID
	gap := controlEvidenceGap(req.RunID, guardian.ControlEvidenceGap{}, outcome, message)
	return s.persistRecoveredControlEvent(req.RunID, *gap)
}

// importReconciledGuardianControls copies the bounded Guardian operation cache
// into the lifecycle journal before the Run is reconciled. It resolves only
// matching pending Supervisor claims; it never repeats a physical effect.
func (s *Service) importReconciledGuardianControls(runID string, operations []guardian.ControlOperation) error {
	if len(operations) > 256 {
		return errors.New("Guardian control evidence exceeds its import bound")
	}
	for _, operation := range operations {
		if operation.Status != "completed" || operation.Evidence.Outcome != "applied" {
			return errors.New("Guardian control result is not a completed applied operation")
		}
		request, ok := requestForGuardianControl(operation)
		if !ok {
			return errors.New("Guardian control evidence failed request identity validation")
		}
		digest, err := mutationDigest(controlIdentityFor(request))
		if err != nil {
			return err
		}
		stored, found, err := s.store.FindControlRequest(runID, request.RequestID, digest, time.Now().UTC())
		if err != nil {
			return err
		}
		if found && (stored.RunID != runID || stored.RequestID != request.RequestID || stored.MutationDigest != digest) {
			return errors.New("Guardian control evidence does not match the pending Supervisor claim")
		}
		if err := s.persistRecoveredControlEvent(runID, operation.Evidence); err != nil {
			return err
		}
		if !found || stored.Status != store.ControlRequestPending {
			continue
		}
		if err := s.store.CompleteControlRequest(runID, request.RequestID, store.ControlRequestSucceeded, "", "", time.Now().UTC()); err != nil {
			return fmt.Errorf("persist recovered control disposition: %w", err)
		}
	}
	return nil
}

// persistUncertainGuardianControls records an explicitly unproven Guardian
// outcome without treating it as an applied mutation.
func (s *Service) persistUncertainGuardianControls(runID string, operations []guardian.ControlOperation) error {
	if len(operations) > 256 {
		return errors.New("Guardian uncertain control evidence exceeds its import bound")
	}
	for _, operation := range operations {
		if !validRetainedGuardianControl(operation) || operation.Evidence.Outcome != "uncertain" {
			return errors.New("Guardian uncertain control identity validation failed")
		}
		request, ok := requestForGuardianControl(operation)
		if !ok {
			return errors.New("Guardian uncertain control request could not be reconstructed")
		}
		digest, err := mutationDigest(controlIdentityFor(request))
		if err != nil {
			return err
		}
		stored, found, err := s.store.FindControlRequest(runID, request.RequestID, digest, time.Now().UTC())
		if err != nil {
			return err
		}
		if found && (stored.RunID != runID || stored.RequestID != request.RequestID || stored.MutationDigest != digest) {
			return errors.New("Guardian uncertain outcome does not match the pending Supervisor claim")
		}
		if err := s.persistRecoveredControlEvent(runID, operation.Evidence); err != nil {
			return err
		}
		if found && stored.Status == store.ControlRequestPending {
			if err := s.store.CompleteControlRequest(runID, request.RequestID, store.ControlRequestUncertain, "control-uncertain", "Guardian retained an unproven physical control outcome", time.Now().UTC()); err != nil {
				return fmt.Errorf("persist uncertain control disposition: %w", err)
			}
		}
	}
	return nil
}

func validGuardianControlRecord(req protocol.Request, operation guardian.ControlOperation) bool {
	expectedDigest, ok := guardianControlDigestFor(req)
	return ok && operation.RequestID == req.RequestID && operation.Digest == expectedDigest && operation.Status == "completed" && validGuardianControlEvidence(req, operation.Evidence)
}

func validGuardianControlEvidence(req protocol.Request, evidence model.ControlEventPayload) bool {
	return validGuardianControlEvidenceOutcome(req, evidence, "applied")
}

func validGuardianControlUncertainRecord(req protocol.Request, operation guardian.ControlOperation) bool {
	expectedDigest, ok := guardianControlDigestFor(req)
	if !ok || operation.RequestID != req.RequestID || operation.Digest != expectedDigest || (operation.Status != "pending" && operation.Status != "completed") {
		return false
	}
	return validGuardianControlEvidenceOutcome(req, operation.Evidence, "uncertain")
}

func validGuardianControlEvidenceOutcome(req protocol.Request, evidence model.ControlEventPayload, outcome string) bool {
	operation, control, action, value, ok := expectedControl(req)
	if !ok || evidence.OperationID != req.RequestID || evidence.Control != control || evidence.Action != action || evidence.Value == nil || *evidence.Value != value || evidence.Outcome != outcome {
		return false
	}
	if operation == "control-memory-high" || operation == "control-cpu-quota" {
		return evidence.Unlimited != nil && !*evidence.Unlimited
	}
	return true
}

func expectedControl(req protocol.Request) (operation, control, action string, value int64, ok bool) {
	switch req.Op {
	case "pause":
		return "control-pause", "cgroup.freeze", "pause", 1, true
	case "resume":
		return "control-resume", "cgroup.freeze", "resume", 0, true
	case "memory-high":
		return "control-memory-high", "memory.high", "set", req.MemoryHighBytes, req.MemoryHighBytes > 0
	case "cpu-quota":
		return "control-cpu-quota", "cpu.max", "set", req.CPUQuotaPercent, req.CPUQuotaPercent > 0
	default:
		return "", "", "", 0, false
	}
}

func guardianControlDigestFor(req protocol.Request) (string, bool) {
	operation, _, _, value, ok := expectedControl(req)
	if !ok {
		return "", false
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", operation, value)))
	return hex.EncodeToString(digest[:]), true
}

func retainedGuardianControlEvidence(runID string, controls *guardian.ControlState) ([]guardian.ControlOperation, []guardian.ControlOperation, *model.ControlEventPayload) {
	if controls == nil {
		// Guardian only creates ControlState when the first mutable control is
		// claimed. An absent state therefore means no control was attempted.
		return nil, nil, nil
	}
	operations := controls.Operations
	invalid := uint64(0)
	if len(operations) > 256 {
		invalid += uint64(len(operations) - 256)
		operations = operations[:256]
	}
	evidence := make([]guardian.ControlOperation, 0, len(operations))
	uncertain := make([]guardian.ControlOperation, 0)
	for _, operation := range operations {
		if !validRetainedGuardianControl(operation) {
			invalid++
			continue
		}
		if operation.Status == "completed" && operation.Evidence.Outcome == "applied" {
			evidence = append(evidence, operation)
		} else {
			uncertain = append(uncertain, operation)
		}
	}
	gap := controls.EvidenceGap
	if gap.ExpiredCount > 0 {
		if ^uint64(0)-gap.ExpiredCount < invalid {
			gap.ExpiredCount = ^uint64(0)
		} else {
			gap.ExpiredCount += invalid
		}
		message := "Guardian control evidence expired"
		if invalid > 0 {
			message = "Guardian control evidence expired and contains unverified records"
		}
		return evidence, uncertain, controlEvidenceGap(runID, gap, "expired", message)
	}
	if invalid > 0 {
		gap.ExpiredCount = invalid
		return evidence, uncertain, controlEvidenceGap(runID, gap, "incomplete", "Guardian control snapshot contains unverified records")
	}
	return evidence, uncertain, nil
}

func validRetainedGuardianControl(operation guardian.ControlOperation) bool {
	requestID := operation.RequestID
	if len(requestID) == 0 || len(requestID) > 128 || strings.ContainsAny(requestID, "\x00\r\n") || operation.Evidence.OperationID != requestID {
		return false
	}
	if operation.Status != "completed" && operation.Status != "pending" {
		return false
	}
	if operation.Evidence.Outcome != "applied" && operation.Evidence.Outcome != "uncertain" {
		return false
	}
	if operation.Evidence.Outcome == "applied" && operation.Status != "completed" {
		return false
	}
	evidence := operation.Evidence
	request := protocol.Request{RequestID: requestID}
	switch {
	case evidence.Control == "cgroup.freeze" && evidence.Action == "pause" && evidence.Value != nil && *evidence.Value == 1:
		request.Op = "pause"
	case evidence.Control == "cgroup.freeze" && evidence.Action == "resume" && evidence.Value != nil && *evidence.Value == 0:
		request.Op = "resume"
	case evidence.Control == "memory.high" && evidence.Action == "set" && evidence.Value != nil && *evidence.Value > 0 && evidence.Unlimited != nil && !*evidence.Unlimited:
		request.Op, request.MemoryHighBytes = "memory-high", *evidence.Value
	case evidence.Control == "cpu.max" && evidence.Action == "set" && evidence.Value != nil && *evidence.Value > 0 && *evidence.Value <= (1<<63-1)/1000 && evidence.Unlimited != nil && !*evidence.Unlimited:
		request.Op, request.CPUQuotaPercent = "cpu-quota", *evidence.Value
	default:
		return false
	}
	digest, ok := guardianControlDigestFor(request)
	return ok && operation.Digest == digest
}

func requestForGuardianControl(operation guardian.ControlOperation) (protocol.Request, bool) {
	if !validRetainedGuardianControl(operation) {
		return protocol.Request{}, false
	}
	evidence := operation.Evidence
	request := protocol.Request{RequestID: operation.RequestID}
	switch {
	case evidence.Control == "cgroup.freeze" && evidence.Action == "pause":
		request.Op = "pause"
	case evidence.Control == "cgroup.freeze" && evidence.Action == "resume":
		request.Op = "resume"
	case evidence.Control == "memory.high" && evidence.Action == "set" && evidence.Value != nil:
		request.Op, request.MemoryHighBytes = "memory-high", *evidence.Value
	case evidence.Control == "cpu.max" && evidence.Action == "set" && evidence.Value != nil:
		request.Op, request.CPUQuotaPercent = "cpu-quota", *evidence.Value
	default:
		return protocol.Request{}, false
	}
	return request, true
}

func controlEvidenceGap(runID string, gap guardian.ControlEvidenceGap, outcome, message string) *model.ControlEventPayload {
	count := gap.ExpiredCount
	if count > uint64(1<<63-1) {
		count = uint64(1<<63 - 1)
	}
	value := int64(count)
	reason := message
	if gap.ExpiredFrom != nil {
		reason += ";expiredFrom=" + gap.ExpiredFrom.UTC().Format(time.RFC3339Nano)
	}
	if gap.ExpiredThrough != nil {
		reason += ";expiredThrough=" + gap.ExpiredThrough.UTC().Format(time.RFC3339Nano)
	}
	if gap.RetainedFrom != nil {
		reason += ";retainedFrom=" + gap.RetainedFrom.UTC().Format(time.RFC3339Nano)
	}
	identity := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%s", runID, outcome, count, reason)))
	return &model.ControlEventPayload{
		OperationID: "guardian-gap-" + hex.EncodeToString(identity[:12]),
		Control:     "guardian-control-evidence", Action: "gap",
		Value: &value, Reason: reason, Outcome: outcome,
	}
}

func isRunControlOperation(operation string) bool {
	switch operation {
	case "pause", "resume", "memory-high", "cpu-quota":
		return true
	default:
		return false
	}
}
