package supervisor

import (
	"errors"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

const maxWriterOwnerIDBytes = 128

const attachmentLeaseTTL = 10 * time.Second

// writerAcquire grants one writer lease while leaving attach itself read-only.
// AttachID identifies the observer/owner so detach can release its lease; a
// non-attached control client may also use a stable opaque owner ID.
func (s *Service) writerAcquire(req protocol.Request) protocol.Response {
	if !validWriterOwner(req.AttachID) {
		return failure("invalid-request", "attachId must contain between 1 and 128 bytes")
	}
	if a, out := s.lookupActive(req.RunID); a == nil {
		return out
	}
	lease, err := s.writerLeases.Acquire(req.RunID, req.AttachID)
	if err != nil {
		return writerLeaseFailure(err)
	}
	if out := s.ensureWriterRunActive(req.RunID); out.Error != nil {
		_ = s.writerLeases.Release(req.RunID, req.AttachID, lease.Token)
		return out
	}
	return writerLeaseResponse(lease)
}

func (s *Service) writerRenew(req protocol.Request) protocol.Response {
	if !validWriterOwner(req.AttachID) {
		return failure("invalid-request", "attachId must contain between 1 and 128 bytes")
	}
	if req.WriterToken == "" {
		return failure("writer-token-required", ErrWriterTokenRequired.Error())
	}
	if a, out := s.lookupActive(req.RunID); a == nil {
		return out
	}
	lease, err := s.writerLeases.Renew(req.RunID, req.AttachID, req.WriterToken)
	if err != nil {
		return writerLeaseFailure(err)
	}
	if out := s.ensureWriterRunActive(req.RunID); out.Error != nil {
		return out
	}
	return writerLeaseResponse(lease)
}

func (s *Service) writerRelease(req protocol.Request) protocol.Response {
	if !validWriterOwner(req.AttachID) {
		return failure("invalid-request", "attachId must contain between 1 and 128 bytes")
	}
	if req.WriterToken == "" {
		return failure("writer-token-required", ErrWriterTokenRequired.Error())
	}
	if a, out := s.lookupActive(req.RunID); a == nil {
		return out
	}
	if err := s.writerLeases.Release(req.RunID, req.AttachID, req.WriterToken); err != nil {
		return writerLeaseFailure(err)
	}
	return response()
}

func (s *Service) attachmentRenew(req protocol.Request) protocol.Response {
	if req.RunID == "" || req.AttachID == "" {
		return failure("invalid-request", "runId and attachId are required")
	}
	if err := s.renewAttachment(req.RunID, req.AttachID); err != nil {
		// A subscription can observe a final Run after its live attachment was
		// removed. Keep terminal output readable until Follow drains its final page.
		if run, getErr := s.store.Get(req.RunID); getErr == nil && (run.State == model.Terminal || run.State == model.Uncertain) {
			return response()
		}
		return failure("attachment-expired", "attachment is no longer active")
	}
	return response()
}

func (s *Service) ensureWriterRunActive(runID string) protocol.Response {
	a, out := s.lookupActive(runID)
	if a == nil {
		return out
	}
	a.mu.Lock()
	state := a.run.State
	a.mu.Unlock()
	if state == model.Terminal || state == model.Uncertain {
		if state == model.Terminal {
			return failure("already-terminal", "Run is terminal")
		}
		return failure("ownership-uncertain", "Run ownership unavailable")
	}
	return response()
}

func writerLeaseResponse(lease WriterLease) protocol.Response {
	expiresAt := lease.ExpiresAt
	out := response()
	out.WriterToken = lease.Token
	out.WriterLeaseExpiresAt = &expiresAt
	return out
}

func writerLeaseFailure(err error) protocol.Response {
	switch {
	case errors.Is(err, ErrWriterConflict):
		return failure("writer-conflict", "another observer holds the interactive writer lease")
	case errors.Is(err, ErrWriterStale):
		return failure("writer-stale", "interactive writer token is stale")
	case errors.Is(err, ErrWriterTokenRequired):
		return failure("writer-token-required", ErrWriterTokenRequired.Error())
	default:
		return failure("backend-failure", "interactive writer lease could not be created")
	}
}

func validWriterOwner(id string) bool { return len(id) > 0 && len(id) <= maxWriterOwnerIDBytes }

func (s *Service) sweepWriterLease(runID string) {
	s.writerLeases.SweepRun(runID)
}
