package supervisor

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

func acceptedSpecDigest(spec *model.RunSpec) (string, error) {
	var canonical any = spec
	if spec != nil {
		copy := *spec
		if copy.Lifetime.Mode == "" {
			copy.Lifetime.Mode = "detached"
		}
		canonical = copy
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func mutationDigest(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

type controlIdentity struct {
	Operation string `json:"operation"`
	Stream    string `json:"stream,omitempty"`
	Data      string `json:"data,omitempty"`
	Signal    string `json:"signal,omitempty"`
	Rows      int    `json:"rows,omitempty"`
	Cols      int    `json:"cols,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func controlIdentityFor(req protocol.Request) controlIdentity {
	// Add each new non-idempotent control operation and all of its mutation
	// values here before routing it through mutateControl.
	identity := controlIdentity{Operation: req.Op}
	switch req.Op {
	case "input":
		identity.Stream = req.Stream
		identity.Data = req.Data
	case "signal":
		identity.Signal = req.Signal
	case "resize":
		identity.Rows, identity.Cols = req.Rows, req.Cols
	case "cancel":
		identity.Reason = "cancelled"
	case "close-input":
		identity.Stream = "stdin"
	}
	return identity
}

// mutateControl serializes caller mutations for one active Run and durably
// claims the request before invoking its physical operation. `prepare` runs
// after replay detection and under the active Run lock; it must validate and
// return a closure that performs exactly one side effect.
func (s *Service) mutateControl(req protocol.Request, prepare func(*active) (func() error, *protocol.Failure)) protocol.Response {
	if len(req.RequestID) == 0 || len(req.RequestID) > store.RequestIDMaxBytes || req.ExpectedGeneration == 0 {
		return failure("invalid-request", "requestId and positive expectedGeneration are required")
	}
	digest, err := mutationDigest(controlIdentityFor(req))
	if err != nil {
		return failure("invalid-request", "control mutation could not be identified")
	}
	a, out := s.lookupActive(req.RunID)
	if a == nil {
		if replay, found, err := s.store.FindControlRequest(req.RunID, req.RequestID, digest, time.Now().UTC()); err != nil {
			return controlStoreFailure(err)
		} else if found {
			return s.controlReplay(req.RunID, replay)
		}
		return out
	}
	a.controlMu.Lock()
	defer a.controlMu.Unlock()
	if replay, found, err := s.store.FindControlRequest(req.RunID, req.RequestID, digest, time.Now().UTC()); err != nil {
		return controlStoreFailure(err)
	} else if found {
		return s.controlReplay(req.RunID, replay)
	}

	a.mu.Lock()
	apply, invalid := prepare(a)
	if invalid != nil {
		a.mu.Unlock()
		return protocol.Response{Version: model.ProtocolVersion, Error: invalid}
	}
	claimed, err := s.store.BeginControlRequest(req.RunID, req.RequestID, digest, req.ExpectedGeneration, time.Now().UTC())
	if err != nil {
		a.mu.Unlock()
		return controlStoreFailure(err)
	}
	if !claimed.Created {
		a.mu.Unlock()
		return s.controlReplay(req.RunID, claimed.Request)
	}
	a.run.Generation = claimed.Run.Generation
	a.mu.Unlock()

	if err := apply(); err != nil {
		if finishErr := s.store.CompleteControlRequest(req.RunID, req.RequestID, store.ControlRequestUncertain, "control-uncertain", "physical mutation outcome is uncertain", time.Now().UTC()); finishErr != nil {
			return failure("control-uncertain", "physical mutation outcome and durable disposition are uncertain")
		}
		return failure("control-uncertain", "physical mutation outcome is uncertain; retry will not repeat it")
	}
	if err := s.store.CompleteControlRequest(req.RunID, req.RequestID, store.ControlRequestSucceeded, "", "", time.Now().UTC()); err != nil {
		return failure("control-uncertain", "physical mutation completed but its durable result is uncertain")
	}
	out = response()
	if run, err := s.store.Get(req.RunID); err == nil {
		clean := publicRun(run)
		out.Run = &clean
	}
	return out
}

func (s *Service) controlReplay(runID string, request store.ControlRequest) protocol.Response {
	switch request.Status {
	case store.ControlRequestSucceeded:
		out := response()
		if run, err := s.store.Get(runID); err == nil {
			clean := publicRun(run)
			out.Run = &clean
		}
		return out
	case store.ControlRequestPending, store.ControlRequestUncertain:
		return failure("control-uncertain", "prior physical mutation outcome is uncertain; it will not be repeated")
	default:
		return failure("storage-failure", "stored control disposition is invalid")
	}
}

func controlStoreFailure(err error) protocol.Response {
	switch {
	case errors.Is(err, store.ErrInvalidRequestID):
		return failure("invalid-request", "requestId and positive expectedGeneration are required")
	case errors.Is(err, store.ErrControlRequestConflict):
		return failure("request-conflict", "requestId is already bound to a different mutation")
	case errors.Is(err, store.ErrControlRequestCapacity):
		return failure("request-window-full", "control request idempotency window is full")
	case errors.Is(err, store.ErrStaleControlGeneration):
		return failure("stale-generation", "expectedGeneration does not match the current Run generation")
	case errors.Is(err, store.ErrControlRunFinal):
		return failure("already-terminal", "Run is terminal or uncertain")
	case errors.Is(err, store.ErrRunNotFound):
		return failure("run-not-found", "Run not found")
	default:
		return failure("storage-failure", "control idempotency state could not be persisted")
	}
}

func submissionStoreFailure(err error) protocol.Response {
	switch {
	case errors.Is(err, store.ErrInvalidSubmissionID), errors.Is(err, store.ErrInvalidIdempotencyDigest):
		return failure("invalid-request", "submissionId is invalid")
	case errors.Is(err, store.ErrSubmissionConflict):
		return failure("submission-conflict", "submissionId is already bound to a different accepted specification")
	case errors.Is(err, store.ErrSubmissionCapacity):
		return failure("submission-window-full", "submission idempotency window is full")
	case errors.Is(err, store.ErrSubmissionRunCollected):
		return failure("submission-result-unavailable", "submission Run detail is unavailable; duplicate execution was refused")
	default:
		return failure("storage-failure", "submission idempotency state could not be persisted")
	}
}

func decodeInput(req protocol.Request) ([]byte, *protocol.Failure) {
	if len(req.Data) > 90000 {
		return nil, &protocol.Failure{Code: "invalid-request", Message: "input too large"}
	}
	data, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil || len(data) > 65536 {
		return nil, &protocol.Failure{Code: "invalid-request", Message: "invalid base64 input"}
	}
	return data, nil
}

func checkAttachmentLocked(a *active, id string) *protocol.Failure {
	if id == "" {
		return nil
	}
	expires, ok := a.attachments[id]
	if !ok || time.Now().After(expires) {
		return &protocol.Failure{Code: "attachment-expired", Message: "attachment expired"}
	}
	a.attachments[id] = time.Now().Add(10 * time.Second)
	return nil
}
