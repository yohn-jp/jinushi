package supervisor

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"io"
	"sync"
	"time"
)

var (
	// ErrWriterConflict means another attachment currently owns the Run's writer lease.
	ErrWriterConflict = errors.New("interactive writer lease is held by another attachment")
	// ErrWriterStale means the supplied token is no longer the active writer token.
	ErrWriterStale = errors.New("interactive writer token is stale")
	// ErrWriterTokenRequired means a physical input mutation omitted its writer token.
	ErrWriterTokenRequired = errors.New("interactive writer token is required")
	// ErrWriterIdentityRequired means a Run or attachment identity was omitted.
	ErrWriterIdentityRequired = errors.New("Run and attachment identities are required")
)

const (
	// DefaultWriterLeaseTTL is short enough to recover writer ownership after a lost
	// observer while allowing clients to renew during normal interactive use.
	DefaultWriterLeaseTTL = 10 * time.Second
	writerTokenBytes      = 32
)

// WriterLease is returned by Acquire and Renew. Token is a bearer secret and
// must not be logged or included in lifecycle events.
type WriterLease struct {
	Token     string
	ExpiresAt time.Time
}

type writerLeaseState struct {
	attachmentID string
	token        string
	expiresAt    time.Time
}

// WriterLeaseManager tracks at most one interactive input writer for each Run.
// Read-only observers do not use this manager and remain unrestricted.
// Expired entries are removed lazily on access or by SweepExpired.
type WriterLeaseManager struct {
	mu     sync.Mutex
	leases map[string]writerLeaseState
	ttl    time.Duration
	now    func() time.Time
	random io.Reader
}

// NewWriterLeaseManager creates a manager with a fixed lease duration.
func NewWriterLeaseManager(ttl time.Duration) (*WriterLeaseManager, error) {
	if ttl <= 0 {
		return nil, errors.New("writer lease duration must be positive")
	}
	return &WriterLeaseManager{
		leases: make(map[string]writerLeaseState),
		ttl:    ttl,
		now:    time.Now,
		random: rand.Reader,
	}, nil
}

// Acquire grants the sole writer lease for runID to attachmentID. Repeating
// Acquire for the current attachment returns its current lease without
// extending it, which makes an ambiguous acquire response safe to retry.
func (m *WriterLeaseManager) Acquire(runID, attachmentID string) (WriterLease, error) {
	if runID == "" || attachmentID == "" {
		return WriterLease{}, ErrWriterIdentityRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	if current, ok := m.leases[runID]; ok {
		if !now.Before(current.expiresAt) {
			delete(m.leases, runID)
		} else if current.attachmentID == attachmentID {
			return WriterLease{Token: current.token, ExpiresAt: current.expiresAt}, nil
		} else {
			return WriterLease{}, ErrWriterConflict
		}
	}

	tokenBytes := make([]byte, writerTokenBytes)
	if _, err := io.ReadFull(m.random, tokenBytes); err != nil {
		return WriterLease{}, err
	}
	lease := writerLeaseState{
		attachmentID: attachmentID,
		token:        base64.RawURLEncoding.EncodeToString(tokenBytes),
		expiresAt:    now.Add(m.ttl),
	}
	m.leases[runID] = lease
	return WriterLease{Token: lease.token, ExpiresAt: lease.expiresAt}, nil
}

// Renew extends an active lease owned by attachmentID. A stale token never
// renews a lease acquired later by the same or another attachment.
func (m *WriterLeaseManager) Renew(runID, attachmentID, token string) (WriterLease, error) {
	if runID == "" || attachmentID == "" {
		return WriterLease{}, ErrWriterIdentityRequired
	}
	if token == "" {
		return WriterLease{}, ErrWriterTokenRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	current, ok := m.currentLocked(runID, now)
	if !ok || current.attachmentID != attachmentID || !sameWriterToken(current.token, token) {
		return WriterLease{}, ErrWriterStale
	}
	current.expiresAt = now.Add(m.ttl)
	m.leases[runID] = current
	return WriterLease{Token: current.token, ExpiresAt: current.expiresAt}, nil
}

// Release removes an active lease held by attachmentID. A stale or already
// released lease is reported as ErrWriterStale.
func (m *WriterLeaseManager) Release(runID, attachmentID, token string) error {
	if runID == "" || attachmentID == "" {
		return ErrWriterIdentityRequired
	}
	if token == "" {
		return ErrWriterTokenRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.currentLocked(runID, m.now())
	if !ok || current.attachmentID != attachmentID || !sameWriterToken(current.token, token) {
		return ErrWriterStale
	}
	delete(m.leases, runID)
	return nil
}

// Validate is called before every physical input, resize, or close-input
// mutation. It does not renew the lease; renewal is an explicit operation.
func (m *WriterLeaseManager) Validate(runID, token string) error {
	if runID == "" {
		return ErrWriterIdentityRequired
	}
	if token == "" {
		return ErrWriterTokenRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.currentLocked(runID, m.now())
	if !ok || !sameWriterToken(current.token, token) {
		return ErrWriterStale
	}
	return nil
}

// Detach releases the writer lease, if any, held by attachmentID. It is safe
// to call this for read-only attachments and does not affect the Run lifetime.
func (m *WriterLeaseManager) Detach(runID, attachmentID string) {
	if runID == "" || attachmentID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.leases[runID]
	if ok && current.attachmentID == attachmentID {
		delete(m.leases, runID)
	}
}

// SweepExpired removes expired writer leases and returns the number removed.
// The caller can invoke it from the supervisor's existing bounded maintenance
// loop; this manager starts no goroutines and does not affect Run ownership.
func (m *WriterLeaseManager) SweepExpired() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	expired := 0
	for runID, lease := range m.leases {
		if !now.Before(lease.expiresAt) {
			delete(m.leases, runID)
			expired++
		}
	}
	return expired
}

func (m *WriterLeaseManager) currentLocked(runID string, now time.Time) (writerLeaseState, bool) {
	lease, ok := m.leases[runID]
	if !ok {
		return writerLeaseState{}, false
	}
	if !now.Before(lease.expiresAt) {
		delete(m.leases, runID)
		return writerLeaseState{}, false
	}
	return lease, true
}

func sameWriterToken(current, supplied string) bool {
	return subtle.ConstantTimeCompare([]byte(current), []byte(supplied)) == 1
}
