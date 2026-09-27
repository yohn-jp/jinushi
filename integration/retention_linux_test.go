//go:build linux

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

func TestProductionRetentionEvictsTerminalEvidenceAndReportsStatus(t *testing.T) {
	h := newHarness(t)
	h.stopSupervisor(false)
	config := `{"retentionIntervalMs":1000,"retention":{"maxAgeMs":0,"maxTerminalRuns":0,"maxStateBytes":1048576,"preserveTombstones":true,"maxTombstones":10,"maxTombstoneAgeMs":86400000,"compactMinFreeBytes":65536,"compactMinFreeRatio":0}}`
	if err := os.WriteFile(filepath.Join(h.stateDir, "config.json"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	h.startSupervisor()

	live := h.run("--", "/bin/sh", "-c", "sleep 60")
	h.waitUntil("live Run to enter running state", 10*time.Second, func() bool { return h.inspect(live.ID).State == "running" })
	replayDir := t.TempDir()
	marker := filepath.Join(replayDir, "effects")
	spec := model.RunSpec{
		Argv: []string{"/bin/sh", "-c", "printf x >> \"$1\"; dd if=/dev/zero bs=65536 count=8 2>/dev/null", "jinushi-retention", marker},
		Cwd:  replayDir,
	}
	submission := protocol.Request{Version: model.ProtocolVersion, Op: "run", SubmissionID: "retention-replay", Spec: &spec}
	accepted, err := callProtocol(h.stateDir, submission)
	if err != nil || accepted.Error != nil || accepted.Run == nil {
		t.Fatalf("accept terminal replay fixture: response=%+v err=%v", accepted, err)
	}
	terminal := run{ID: accepted.Run.ID}
	h.runs = append(h.runs, terminal.ID)
	if code, completed := h.await(terminal.ID, 15*time.Second); code != 0 || completed.State != "terminal" {
		t.Fatalf("terminal Run completion: exit=%d run=%+v receipt=%+v", code, completed, completed.Receipt)
	}

	deadline := time.Now().Add(12 * time.Second)
	var inspected protocol.Response
	var status protocol.Response
	for time.Now().Before(deadline) {
		var err error
		inspected, err = callProtocol(h.stateDir, protocol.Request{Version: model.ProtocolVersion, Op: "inspect", RunID: terminal.ID})
		if err != nil {
			t.Fatal(err)
		}
		if inspected.Tombstone != nil {
			status, err = callProtocol(h.stateDir, protocol.Request{Version: model.ProtocolVersion, Op: "status"})
			if err != nil {
				t.Fatal(err)
			}
			if status.Status != nil && status.Status.Retention.CompactionStatus != "not-attempted" {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if inspected.Run == nil || inspected.Tombstone == nil || inspected.Run.Receipt == nil ||
		!inspected.Run.Receipt.EvidenceIncomplete || inspected.Run.Spec.Argv != nil ||
		inspected.Tombstone.RunID != terminal.ID || inspected.Tombstone.ReceiptSHA256 == "" || len(inspected.Tombstone.Reasons) == 0 {
		t.Fatalf("inspect did not return a bounded incomplete tombstone: run=%+v tombstone=%+v status=%+v supervisor=%s", inspected.Run, inspected.Tombstone, status.Status, h.sup.stderr.String())
	}
	if inspected.Run.State != model.Terminal {
		t.Fatalf("collected terminal identity state = %q", inspected.Run.State)
	}
	if result, err := os.ReadFile(marker); err != nil || string(result) != "x" {
		t.Fatalf("terminal physical side effect before replay = %q err=%v", result, err)
	}
	replayed, err := callProtocol(h.stateDir, submission)
	if err != nil || replayed.Error != nil || replayed.Run == nil || replayed.Run.ID != terminal.ID ||
		replayed.Run.Receipt == nil || !replayed.Run.Receipt.EvidenceIncomplete || replayed.Tombstone == nil {
		t.Fatalf("same submission after retention did not return its tombstone identity: response=%+v err=%v", replayed, err)
	}
	if result, err := os.ReadFile(marker); err != nil || string(result) != "x" {
		t.Fatalf("retry duplicated terminal physical side effect: %q err=%v", result, err)
	}

	for _, request := range []protocol.Request{
		{Version: model.ProtocolVersion, Op: "events", RunID: terminal.ID},
		{Version: model.ProtocolVersion, Op: "output", RunID: terminal.ID, Stream: "stdout"},
	} {
		got, err := callProtocol(h.stateDir, request)
		if err != nil {
			t.Fatal(err)
		}
		if got.Error == nil || got.Error.Code != "evidence-collected" || got.Tombstone == nil || got.Tombstone.RunID != terminal.ID {
			t.Fatalf("%s after collection = %+v", request.Op, got)
		}
	}

	if status.Error != nil {
		t.Fatalf("status returned error during retention/compaction: %+v", status.Error)
	}
	if status.Status == nil || status.Status.Version != 1 {
		t.Fatalf("status omitted runtime/store projection: %+v", status)
	}
	usage := status.Status.Store
	if usage.RunCount != 1 || usage.NonterminalRunCount != 1 || usage.TerminalRunCount != 0 || usage.TombstoneCount != 1 {
		t.Fatalf("status counts include wrong physical/retained state: %+v", usage)
	}
	retention := status.Status.Retention
	if retention.LastRunAt == nil || retention.EvictedRunsTotal != 1 || retention.TerminalRunsRemaining != 0 || retention.BudgetExceeded {
		t.Fatalf("status retention checkpoint = %+v", retention)
	}
	if retention.CompactionAttemptedAt == nil {
		t.Fatalf("retention did not record an attempt despite reclaimed pages: %+v", retention)
	}
	switch retention.CompactionStatus {
	case "completed":
		if retention.CompactionErrorCode != "" || retention.CompactionRecommended {
			t.Fatalf("successful compaction status = %+v usage=%+v", retention, usage)
		}
	case "unsupported":
		if retention.CompactionErrorCode != "compaction-unsupported" || !retention.CompactionRecommended {
			t.Fatalf("unsupported compaction status = %+v", retention)
		}
	default:
		t.Fatalf("unexpected production compaction status = %+v", retention)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := ipc.Call(ctx, h.stateDir, protocol.Request{Version: model.ProtocolVersion, Op: "cancel", RunID: live.ID, RequestID: "retention-test-stop", ExpectedGeneration: h.inspect(live.ID).Generation})
	if err != nil || response.Error != nil {
		t.Fatalf("cancel live Run after proving GC skipped it: response=%+v err=%v", response, err)
	}
	if _, completed := h.await(live.ID, 15*time.Second); completed.State != "terminal" {
		t.Fatalf("live Run cleanup state = %q", completed.State)
	}
}

func callProtocol(stateDir string, request protocol.Request) (protocol.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ipc.Call(ctx, stateDir, request)
}
