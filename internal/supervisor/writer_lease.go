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
	// ErrWriterMutationRequired means a validated writer operation omitted its mutation.
	ErrWriterMutationRequired = errors.New("writer mutation is required")
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

type writerLeaseEntry struct {
	mu     sync.Mutex
	lease  writerLeaseState
	refs   int  // guarded by WriterLeaseManager.mu
	retire bool // guarded by WriterLeaseManager.mu; used after terminal cleanup
}

// WriterLeaseManager tracks at most one interactive input writer for each Run.
// Read-only observers do not use this manager and remain unrestricted.
// Expired entries are removed lazily on access or by SweepExpired.
type WriterLeaseManager struct {
	mu       sync.Mutex
	leases   map[string]*writerLeaseEntry
	ttl      time.Duration
	now      func() time.Time
	random   io.Reader
	randomMu sync.Mutex
}

// NewWriterLeaseManager creates a manager with a fixed lease duration.
func NewWriterLeaseManager(ttl time.Duration) (*WriterLeaseManager, error) {
	if ttl <= 0 {
		return nil, errors.New("writer lease duration must be positive")
	}
	return &WriterLeaseManager{
		leases: make(map[string]*writerLeaseEntry),
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
	entry := m.entry(runID, true)
	defer m.releaseEntry(runID, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	now := m.now()
	if current := entry.lease; current.token != "" {
		if !now.Before(current.expiresAt) {
			entry.lease = writerLeaseState{}
		} else if current.attachmentID == attachmentID {
			return WriterLease{Token: current.token, ExpiresAt: current.expiresAt}, nil
		} else {
			return WriterLease{}, ErrWriterConflict
		}
	}

	tokenBytes := make([]byte, writerTokenBytes)
	m.randomMu.Lock()
	if _, err := io.ReadFull(m.random, tokenBytes); err != nil {
		m.randomMu.Unlock()
		return WriterLease{}, err
	}
	m.randomMu.Unlock()
	lease := writerLeaseState{
		attachmentID: attachmentID,
		token:        base64.RawURLEncoding.EncodeToString(tokenBytes),
		expiresAt:    now.Add(m.ttl),
	}
	entry.lease = lease
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
	entry := m.entry(runID, false)
	if entry == nil {
		return WriterLease{}, ErrWriterStale
	}
	defer m.releaseEntry(runID, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	now := m.now()
	current, ok := currentWriterLease(&entry.lease, now)
	if !ok || current.attachmentID != attachmentID || !sameWriterToken(current.token, token) {
		return WriterLease{}, ErrWriterStale
	}
	current.expiresAt = now.Add(m.ttl)
	entry.lease = current
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
	entry := m.entry(runID, false)
	if entry == nil {
		return ErrWriterStale
	}
	defer m.releaseEntry(runID, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	current, ok := currentWriterLease(&entry.lease, m.now())
	if !ok || current.attachmentID != attachmentID || !sameWriterToken(current.token, token) {
		return ErrWriterStale
	}
	entry.lease = writerLeaseState{}
	return nil
}

// Validate checks the current lease without renewing it. It is suitable for a
// non-mutating preflight; physical input, resize, and close-input operations
// should use WithValidatedWriter so ownership cannot change between validation
// and the physical side effect.
func (m *WriterLeaseManager) Validate(runID, token string) error {
	if runID == "" {
		return ErrWriterIdentityRequired
	}
	if token == "" {
		return ErrWriterTokenRequired
	}
	entry := m.entry(runID, false)
	if entry == nil {
		return ErrWriterStale
	}
	defer m.releaseEntry(runID, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	current, ok := currentWriterLease(&entry.lease, m.now())
	if !ok || !sameWriterToken(current.token, token) {
		return ErrWriterStale
	}
	return nil
}

// WithValidatedWriter verifies the current token and holds that Run's entry
// lock until mutation returns. This makes validation and the physical side
// effect atomic relative to same-Run acquire, renew, release, detach, expiry,
// and cleanup without blocking writer operations for other Runs. The callback
// must not call this manager for the same Run.
func (m *WriterLeaseManager) WithValidatedWriter(runID, token string, mutation func() error) error {
	if runID == "" {
		return ErrWriterIdentityRequired
	}
	if token == "" {
		return ErrWriterTokenRequired
	}
	if mutation == nil {
		return ErrWriterMutationRequired
	}
	entry := m.entry(runID, false)
	if entry == nil {
		return ErrWriterStale
	}
	defer m.releaseEntry(runID, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	current, ok := currentWriterLease(&entry.lease, m.now())
	if !ok || !sameWriterToken(current.token, token) {
		return ErrWriterStale
	}
	return mutation()
}

// Detach releases the writer lease, if any, held by attachmentID. It is safe
// to call this for read-only attachments and does not affect the Run lifetime.
func (m *WriterLeaseManager) Detach(runID, attachmentID string) {
	if runID == "" || attachmentID == "" {
		return
	}
	entry := m.entry(runID, false)
	if entry == nil {
		return
	}
	defer m.releaseEntry(runID, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.lease.token != "" && entry.lease.attachmentID == attachmentID {
		entry.lease = writerLeaseState{}
	}
}

// ClearRun removes any writer lease for a Run that has become terminal or
// uncertain. It is idempotent and does not affect the Run itself.
func (m *WriterLeaseManager) ClearRun(runID string) {
	if runID == "" {
		return
	}
	entry := m.entry(runID, false)
	if entry == nil {
		return
	}
	entry.mu.Lock()
	entry.lease = writerLeaseState{}
	m.mu.Lock()
	entry.retire = true
	m.mu.Unlock()
	entry.mu.Unlock()
	m.releaseEntry(runID, entry)
}

// SweepExpired removes expired writer leases and returns the number removed.
// The caller can invoke it from the supervisor's existing bounded maintenance
// loop; this manager starts no goroutines and does not affect Run ownership.
func (m *WriterLeaseManager) SweepExpired() int {
	m.mu.Lock()
	runIDs := make([]string, 0, len(m.leases))
	for runID := range m.leases {
		runIDs = append(runIDs, runID)
	}
	m.mu.Unlock()
	expired := 0
	for _, runID := range runIDs {
		if m.SweepRun(runID) {
			expired++
		}
	}
	return expired
}

// SweepRun removes the expired lease for runID, if present. Calling this from
// that Run's maintenance loop keeps cleanup proportional to active Runs.
func (m *WriterLeaseManager) SweepRun(runID string) bool {
	if runID == "" {
		return false
	}
	entry := m.entry(runID, false)
	if entry == nil {
		return false
	}
	defer m.releaseEntry(runID, entry)
	entry.mu.Lock()
	expired := entry.lease.token != "" && !m.now().Before(entry.lease.expiresAt)
	if expired {
		entry.lease = writerLeaseState{}
	}
	empty := entry.lease.token == ""
	if empty {
		m.mu.Lock()
		if entry.refs == 1 && m.leases[runID] == entry {
			delete(m.leases, runID)
		}
		m.mu.Unlock()
	}
	entry.mu.Unlock()
	return expired
}

func (m *WriterLeaseManager) entry(runID string, create bool) *writerLeaseEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.leases[runID]
	if entry == nil && create {
		entry = &writerLeaseEntry{}
		m.leases[runID] = entry
	}
	if entry != nil {
		entry.refs++
	}
	return entry
}

func (m *WriterLeaseManager) releaseEntry(runID string, entry *writerLeaseEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && entry.retire && m.leases[runID] == entry {
		delete(m.leases, runID)
	}
}

func currentWriterLease(lease *writerLeaseState, now time.Time) (writerLeaseState, bool) {
	if lease.token == "" {
		return writerLeaseState{}, false
	}
	if !now.Before(lease.expiresAt) {
		*lease = writerLeaseState{}
		return writerLeaseState{}, false
	}
	return *lease, true
}

func sameWriterToken(current, supplied string) bool {
	return subtle.ConstantTimeCompare([]byte(current), []byte(supplied)) == 1
}
