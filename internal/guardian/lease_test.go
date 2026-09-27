package guardian

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestRenewLeasePersistsAndRetriesIdempotently(t *testing.T) {
	dir := t.TempDir()
	spool, err := openSpool(filepath.Join(dir, "spool"), 3<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	expires := time.Now().Add(time.Second).UTC()
	state := &runState{
		descriptor: launchConfig{Dir: dir},
		spool:      spool,
		terminal:   make(chan struct{}),
		snapshot: Snapshot{
			Version: ProtocolVersion, RunID: "run_lease", State: model.Running,
			Resources: unavailableResources(), LeaseExpiry: &expires, LeaseGeneration: 7,
		},
	}
	if err := state.persist(); err != nil {
		t.Fatal(err)
	}

	first, err := state.renewLease(7, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if first.LeaseGeneration != 8 || first.LastLeaseExpectedGeneration != 7 || first.LastLeaseMs != 5000 {
		t.Fatalf("renewal evidence = generation %d, expected %d, duration %d", first.LeaseGeneration, first.LastLeaseExpectedGeneration, first.LastLeaseMs)
	}
	if first.LeaseExpiry == nil || !first.LeaseExpiry.After(expires) {
		t.Fatalf("renewal did not advance expiry: old=%s new=%v", expires, first.LeaseExpiry)
	}

	stored, err := readSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LeaseGeneration != first.LeaseGeneration || stored.LeaseExpiry == nil || !stored.LeaseExpiry.Equal(*first.LeaseExpiry) {
		t.Fatalf("renewal evidence was not durable: %+v", stored)
	}
	retry, err := state.renewLease(7, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if retry.LeaseGeneration != first.LeaseGeneration || retry.LeaseExpiry == nil || !retry.LeaseExpiry.Equal(*first.LeaseExpiry) {
		t.Fatalf("idempotent retry changed the committed lease: first=%+v retry=%+v", first, retry)
	}
	if _, err := state.renewLease(7, 6000); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("different retry for committed generation: err=%v, want ErrStaleGeneration", err)
	}
}

func TestExpiredLeaseRenewalIsRejected(t *testing.T) {
	dir := t.TempDir()
	spool, err := openSpool(filepath.Join(dir, "spool"), 3<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.close()
	expires := time.Now().Add(-time.Second).UTC()
	state := &runState{
		descriptor: launchConfig{Dir: dir, TerminationGraceMs: 100},
		spool:      spool,
		terminal:   make(chan struct{}),
		snapshot: Snapshot{
			Version: ProtocolVersion, RunID: "run_expired", State: model.Running,
			Resources: unavailableResources(), LeaseExpiry: &expires, LeaseGeneration: 1,
		},
	}
	if _, err := state.renewLease(1, 5000); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired renewal: err=%v, want ErrLeaseExpired", err)
	}
}
