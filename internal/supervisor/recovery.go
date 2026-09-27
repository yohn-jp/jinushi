package supervisor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/guardian"
	"github.com/yohn-jp/jinushi/internal/model"
)

type guardianLossRecoverer interface {
	RecoverGuardianLoss(model.Ownership, time.Duration) (backend.TerminationResult, error)
}

type guardianLossRecoveryError struct {
	evidence        []model.ControlEventPayload
	uncertainReason string
	cause           error
}

func (e *guardianLossRecoveryError) Error() string {
	return "Guardian became unavailable and Linux ownership recovery did not produce a terminal receipt"
}

func (e *guardianLossRecoveryError) Unwrap() error { return e.cause }

func (g *guardedExecutor) recoverLostGuardian(run model.Run, cause error, controlsUnavailable bool) error {
	evidence := []model.ControlEventPayload{guardianLossEvidence(run.ID, "ownership-unproven")}
	if controlsUnavailable {
		gap := controlEvidenceGap(run.ID, guardian.ControlEvidenceGap{}, "unavailable", "Guardian control evidence could not be read")
		evidence = append(evidence, *gap)
	}
	uncertainReason := "guardian lost; process ownership is unproven"
	if run.Ownership == nil {
		return &guardianLossRecoveryError{evidence: evidence, uncertainReason: uncertainReason, cause: cause}
	}
	result, err := g.RecoverGuardianLoss(*run.Ownership, time.Duration(g.config.TerminationGraceMs)*time.Millisecond)
	if err != nil {
		evidence[0] = guardianLossEvidence(run.ID, "termination-unproven")
		uncertainReason = "guardian lost; owned process tree termination is unproven"
		return &guardianLossRecoveryError{evidence: evidence, uncertainReason: uncertainReason, cause: errors.Join(cause, err)}
	}
	if !result.TreeEmpty {
		evidence[0] = guardianLossEvidence(run.ID, "termination-unproven")
		uncertainReason = "guardian lost; owned process tree did not reach a proven empty state"
		return &guardianLossRecoveryError{evidence: evidence, uncertainReason: uncertainReason, cause: cause}
	}
	if result.Requested {
		evidence[0] = guardianLossEvidence(run.ID, "tree-terminated")
	} else {
		evidence[0] = guardianLossEvidence(run.ID, "tree-already-empty")
	}
	uncertainReason = "guardian lost; process tree is empty but terminal receipt is unavailable"
	return &guardianLossRecoveryError{evidence: evidence, uncertainReason: uncertainReason, cause: cause}
}

func (s *Service) recoverActiveGuardianLoss(a *active) bool {
	return s.recoverActiveGuardianLossWithExit(a, nil)
}

func (s *Service) recoverActiveGuardianLossAfterWait(a *active, exit exitResult) bool {
	return s.recoverActiveGuardianLossWithExit(a, &exit)
}

func (s *Service) recoverActiveGuardianLossWithExit(a *active, exit *exitResult) bool {
	a.controlMu.Lock()
	defer a.controlMu.Unlock()
	a.mu.Lock()
	process := a.process
	runID := a.run.ID
	owner := a.run.Ownership
	a.mu.Unlock()
	if _, ok := process.(guardianRunControls); !ok {
		return false
	}
	recoverer, ok := s.backend.(guardianLossRecoverer)
	if !ok {
		return false
	}
	evidence := []model.ControlEventPayload{guardianLossEvidence(runID, "ownership-unproven")}
	if snapshotter, ok := process.(interface {
		ControlSnapshot() (guardian.Snapshot, error)
	}); ok {
		snapshot, err := snapshotter.ControlSnapshot()
		if err != nil {
			if gap := controlEvidenceGap(runID, guardian.ControlEvidenceGap{}, "unavailable", "Guardian control evidence could not be read after control loss"); gap != nil {
				evidence = append(evidence, *gap)
			}
		} else if snapshot.RunID != runID {
			if gap := controlEvidenceGap(runID, guardian.ControlEvidenceGap{}, "identity-mismatch", "Guardian control snapshot Run identity does not match"); gap != nil {
				evidence = append(evidence, *gap)
			}
		} else {
			operations, uncertain, gap := retainedGuardianControlEvidence(runID, snapshot.Controls)
			if len(operations) > 0 {
				if err := s.importReconciledGuardianControls(runID, operations); err != nil {
					gap = controlEvidenceGap(runID, guardian.ControlEvidenceGap{}, "incomplete", "Guardian control evidence could not be imported")
				}
			}
			if len(uncertain) > 0 {
				if err := s.persistUncertainGuardianControls(runID, uncertain); err != nil {
					gap = controlEvidenceGap(runID, guardian.ControlEvidenceGap{}, "incomplete", "Guardian uncertain control evidence could not be imported")
				}
			}
			if gap != nil {
				evidence = append(evidence, *gap)
			}
		}
	} else {
		if gap := controlEvidenceGap(runID, guardian.ControlEvidenceGap{}, "unavailable", "Guardian control evidence lookup is unavailable after control loss"); gap != nil {
			evidence = append(evidence, *gap)
		}
	}
	uncertainReason := "guardian lost; process ownership is unproven"
	var recovery backend.TerminationResult
	treeEmpty := false
	if owner != nil {
		var err error
		recovery, err = recoverer.RecoverGuardianLoss(*owner, time.Duration(s.config.TerminationGraceMs)*time.Millisecond)
		if err == nil && recovery.TreeEmpty {
			treeEmpty = true
			if recovery.Requested {
				evidence[0] = guardianLossEvidence(runID, "tree-terminated")
			} else {
				evidence[0] = guardianLossEvidence(runID, "tree-already-empty")
			}
			uncertainReason = "guardian lost; process tree is empty but terminal receipt is unavailable"
		} else {
			evidence[0] = guardianLossEvidence(runID, "termination-unproven")
			uncertainReason = "guardian lost; owned process tree termination is unproven"
		}
	}
	for _, item := range evidence {
		if err := s.persistRecoveredControlEvent(runID, item); err != nil {
			uncertainReason = "guardian lost; control evidence could not be deduplicated or persisted"
			treeEmpty = false
			break
		}
	}
	if exit != nil && treeEmpty && exit.receipt != nil && exit.receipt.RunID == runID && exit.receipt.Cleanup == "complete" && !exit.receipt.FinishedAt.IsZero() {
		a.mu.Lock()
		reason := a.run.TerminationReason
		a.mu.Unlock()
		termination := terminationResult{complete: true, forced: recovery.Forced, outcome: recovery.Outcome}
		outcome := resolvedExitOutcome(exit, termination, reason)
		s.finishWithControlLock(a, outcome, *exit, recovery.Forced || exit.forced, "complete")
		return true
	}
	var receipt *model.Receipt
	if exit != nil && exit.receipt != nil {
		copy := *exit.receipt
		receipt = &copy
		if treeEmpty {
			uncertainReason = "guardian receipt reports cleanup complete but independent OS evidence is unavailable"
		} else {
			uncertainReason = "guardian receipt is retained but independent process-tree cleanup is unproven"
		}
	}
	s.markGuardianLossUncertain(a, uncertainReason, receipt)
	return true
}

func (s *Service) markGuardianLossUncertain(a *active, reason string, receipt *model.Receipt) {
	a.mu.Lock()
	if a.run.State == model.Terminal || a.run.State == model.Uncertain {
		a.mu.Unlock()
		return
	}
	a.run.Attachments = 0
	next := a.run
	next.State = model.Uncertain
	next.Generation++
	if receipt != nil {
		copy := *receipt
		copy.Output = next.Output
		// The Guardian receipt is evidence about the root process, but this
		// path could not independently prove cleanup of the owned tree. Never
		// publish its complete-cleanup claim as an ordinary terminal receipt.
		copy.Cleanup = "unproven"
		copy.EvidenceIncomplete = true
		if next.EffectiveCapabilities == nil && copy.EffectiveCapabilities != nil {
			effective := *copy.EffectiveCapabilities
			next.EffectiveCapabilities = &effective
		}
		s.populateReceipt(next, &copy)
		next.Receipt = &copy
		next.StartedAt = copy.StartedAt
		finished := copy.FinishedAt
		next.FinishedAt = &finished
		next.Resources = copy.Resources
	}
	now := time.Now().UTC()
	events := []model.Event{{Kind: model.EventRunUncertain, ObservedAt: now, Payload: &model.EventPayload{Run: &model.RunEventPayload{State: model.Uncertain, Generation: next.Generation, Reason: reason}}}}
	if err := s.persistRunEvents(next, events); err != nil {
		a.run.State = model.Uncertain
		a.run.Generation = next.Generation
	} else {
		a.run = next
	}
	close(a.done)
	a.mu.Unlock()
	s.mu.Lock()
	delete(s.active, a.run.ID)
	s.mu.Unlock()
}

func guardianRecoveryEvents(err error) ([]model.ControlEventPayload, string, bool) {
	var recovery *guardianLossRecoveryError
	if !errors.As(err, &recovery) {
		return nil, "", false
	}
	return recovery.evidence, recovery.uncertainReason, true
}

func guardianLossEvidence(runID, outcome string) model.ControlEventPayload {
	identity := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s", runID, outcome)))
	return model.ControlEventPayload{
		OperationID: "guardian-loss-" + hex.EncodeToString(identity[:12]),
		Control:     "guardian-loss", Action: "terminate", Outcome: outcome,
		Reason: "guardian-unavailable",
	}
}
