package supervisor

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestWriterLeaseManager(t *testing.T, ttl time.Duration) (*WriterLeaseManager, *time.Time) {
	t.Helper()
	manager, err := NewWriterLeaseManager(ttl)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	manager.now = func() time.Time { return now }
	return manager, &now
}

func TestWriterLeaseAllowsOnlyOneConcurrentAttachment(t *testing.T) {
	manager, _ := newTestWriterLeaseManager(t, time.Minute)
	const contenders = 32
	start := make(chan struct{})
	type result struct {
		attachment string
		lease      WriterLease
		err        error
	}
	results := make(chan result, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		attachment := string(rune('a' + i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := manager.Acquire("run_1", attachment)
			results <- result{attachment: attachment, lease: lease, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	winners := 0
	var winner result
	for got := range results {
		if got.err == nil {
			winners++
			winner = got
			continue
		}
		if !errors.Is(got.err, ErrWriterConflict) {
			t.Fatalf("Acquire error = %v, want conflict", got.err)
		}
	}
	if winners != 1 {
		t.Fatalf("successful acquisitions = %d, want 1", winners)
	}
	if winner.lease.Token == "" || len(winner.lease.Token) < 40 {
		t.Fatalf("writer token is empty or too short: %q", winner.lease.Token)
	}
	if err := manager.Validate("run_1", winner.lease.Token); err != nil {
		t.Fatalf("Validate winner token: %v", err)
	}
}

func TestWriterLeaseRetryRenewReleaseAndDetach(t *testing.T) {
	manager, now := newTestWriterLeaseManager(t, time.Minute)
	first, err := manager.Acquire("run_1", "att_1")
	if err != nil {
		t.Fatal(err)
	}
	retry, err := manager.Acquire("run_1", "att_1")
	if err != nil {
		t.Fatalf("same-attachment acquire retry: %v", err)
	}
	if retry.Token != first.Token || !retry.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("acquire retry changed lease: first=%+v retry=%+v", first, retry)
	}
	if _, err := manager.Acquire("run_1", "att_2"); !errors.Is(err, ErrWriterConflict) {
		t.Fatalf("second attachment acquire error = %v, want conflict", err)
	}
	if _, err := manager.Renew("run_1", "att_1", "wrong"); !errors.Is(err, ErrWriterStale) {
		t.Fatalf("wrong-token renew error = %v, want stale", err)
	}
	*now = now.Add(time.Second)
	renewed, err := manager.Renew("run_1", "att_1", first.Token)
	if err != nil {
		t.Fatalf("renew current lease: %v", err)
	}
	if !renewed.ExpiresAt.After(first.ExpiresAt) {
		t.Fatalf("renewed expiry %v did not advance past %v", renewed.ExpiresAt, first.ExpiresAt)
	}
	if err := manager.Validate("run_1", ""); !errors.Is(err, ErrWriterTokenRequired) {
		t.Fatalf("missing-token validation error = %v, want token-required", err)
	}
	if err := manager.Validate("run_1", first.Token); err != nil {
		t.Fatalf("Validate current lease: %v", err)
	}
	if err := manager.Release("run_1", "att_1", first.Token); err != nil {
		t.Fatalf("release current lease: %v", err)
	}
	if err := manager.Validate("run_1", first.Token); !errors.Is(err, ErrWriterStale) {
		t.Fatalf("released-token validation error = %v, want stale", err)
	}
	second, err := manager.Acquire("run_1", "att_2")
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	manager.Detach("run_1", "att_2")
	if err := manager.Validate("run_1", second.Token); !errors.Is(err, ErrWriterStale) {
		t.Fatalf("detached-token validation error = %v, want stale", err)
	}
	if _, err := manager.Acquire("run_1", "att_3"); err != nil {
		t.Fatalf("reconnect acquire after detach: %v", err)
	}
}

func TestWriterLeaseExpiryAllowsReconnectAndRejectsOldToken(t *testing.T) {
	manager, now := newTestWriterLeaseManager(t, 10*time.Second)
	old, err := manager.Acquire("run_1", "att_old")
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(10 * time.Second)
	if err := manager.Validate("run_1", old.Token); !errors.Is(err, ErrWriterStale) {
		t.Fatalf("expired-token validation error = %v, want stale", err)
	}
	if _, err := manager.Renew("run_1", "att_old", old.Token); !errors.Is(err, ErrWriterStale) {
		t.Fatalf("expired-token renew error = %v, want stale", err)
	}
	if removed := manager.SweepExpired(); removed != 0 {
		t.Fatalf("expired entries swept after lazy validation = %d, want 0", removed)
	}

	newLease, err := manager.Acquire("run_1", "att_reconnected")
	if err != nil {
		t.Fatalf("reconnected attachment acquire: %v", err)
	}
	if newLease.Token == old.Token {
		t.Fatal("reconnected attachment received the expired token")
	}
	if err := manager.Validate("run_1", old.Token); !errors.Is(err, ErrWriterStale) {
		t.Fatalf("old token after reconnect error = %v, want stale", err)
	}
	*now = newLease.ExpiresAt
	if removed := manager.SweepExpired(); removed != 1 {
		t.Fatalf("swept entries = %d, want 1", removed)
	}
	if _, err := manager.Acquire("run_1", "att_after_sweep"); err != nil {
		t.Fatalf("acquire after expiry sweep: %v", err)
	}
}

func TestWriterLeaseRequiresRunAndAttachmentIdentity(t *testing.T) {
	manager, _ := newTestWriterLeaseManager(t, time.Minute)
	if _, err := manager.Acquire("", "att"); !errors.Is(err, ErrWriterIdentityRequired) {
		t.Fatalf("missing Run identity error = %v", err)
	}
	if _, err := manager.Acquire("run", ""); !errors.Is(err, ErrWriterIdentityRequired) {
		t.Fatalf("missing attachment identity error = %v", err)
	}
	if _, err := NewWriterLeaseManager(0); err == nil {
		t.Fatal("zero-duration lease was accepted")
	}
}
