//go:build linux

package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

func TestSubmissionRetryAfterLostResponseIsIdempotentAcrossRestart(t *testing.T) {
	h := newHarness(t)
	cwd := t.TempDir()
	marker := filepath.Join(cwd, "physical-starts")
	release := filepath.Join(cwd, "release")
	submissionID := "lost-response-" + filepath.Base(cwd)
	script := `printf x >> "$1"; while [ ! -e "$2" ]; do sleep 0.02; done; exec "$3" wait 0`
	spec := &model.RunSpec{
		Argv: []string{"/bin/sh", "-c", script, "jinushi-lost-response", marker, release, helperBinary},
		Cwd:  cwd,
		Environment: model.Environment{
			Mode: "inherit-supervisor",
		},
		Lifetime: model.Lifetime{Mode: "detached"},
	}
	t.Cleanup(func() {
		_ = os.WriteFile(release, []byte("release"), 0600)
		trackHarnessRunsForCleanup(h)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	conn, err := ipc.Dial(ctx, h.stateDir)
	cancel()
	if err != nil {
		t.Fatalf("dial production supervisor IPC: %v", err)
	}
	defer func() { _ = conn.Close() }()
	request := protocol.Request{
		Version:      model.ProtocolVersion,
		Op:           "run",
		SubmissionID: submissionID,
		Spec:         spec,
	}
	if err := writeLostResponseRequest(conn, request); err != nil {
		_ = conn.Close()
		t.Fatalf("send initial Run request: %v", err)
	}
	h.waitUntil("first submission to start its physical workload", 10*time.Second, func() bool {
		data, err := os.ReadFile(marker)
		return err == nil && len(data) >= 1
	})
	firstMarker, err := os.ReadFile(marker)
	if err != nil || string(firstMarker) != "x" {
		_ = conn.Close()
		t.Fatalf("initial request did not start exactly one workload before the reply was discarded: marker=%q err=%v", firstMarker, err)
	}
	// Deliberately close without reading the accepted Run response. The workload
	// has already crossed the production IPC, Guardian, and Linux process paths.
	if err := conn.Close(); err != nil {
		t.Fatalf("close response-losing client connection: %v", err)
	}

	cliArgs := submissionCLIArgs(h.stateDir, submissionID, cwd, spec.Argv)
	code, _, retried := h.invoke(10*time.Second, cliArgs...)
	if code != 0 || retried.Error != nil || retried.Run == nil || retried.Run.ID == "" {
		t.Fatalf("retry the submission after its response was lost: exit=%d response=%+v", code, retried)
	}
	firstRunID := retried.Run.ID
	trackHarnessRuns(h)
	if string(readMarker(t, marker)) != "x" {
		t.Fatalf("same-ID retry launched another physical workload: marker=%q", readMarker(t, marker))
	}

	h.restartAfterCrash()
	code, _, replayed := h.invoke(10*time.Second, cliArgs...)
	if code != 0 || replayed.Error != nil || replayed.Run == nil || replayed.Run.ID != firstRunID {
		t.Fatalf("same submission did not replay the accepted Run after Supervisor restart: exit=%d response=%+v originalRunID=%s", code, replayed, firstRunID)
	}
	trackHarnessRuns(h)

	changedArgv := append(append([]string(nil), spec.Argv...), "different-spec")
	code, _, conflict := h.invoke(10*time.Second, submissionCLIArgs(h.stateDir, submissionID, cwd, changedArgv)...)
	if code == 0 || conflict.Error == nil || conflict.Error.Code != "submission-conflict" {
		t.Fatalf("changed specification did not conflict with the accepted submission ID: exit=%d response=%+v", code, conflict)
	}
	trackHarnessRuns(h)
	if got := listedRunIDs(t, h); len(got) != 1 || got[0] != firstRunID {
		t.Fatalf("retries produced Runs beyond the original accepted Run %s: %v", firstRunID, got)
	}
	time.Sleep(250 * time.Millisecond)
	if got := string(readMarker(t, marker)); got != "x" {
		t.Fatalf("retry or restart replay caused a duplicate physical start: marker=%q", got)
	}

	if err := os.WriteFile(release, []byte("release"), 0600); err != nil {
		t.Fatalf("release the workload: %v", err)
	}
	code, completed := h.await(firstRunID, 15*time.Second)
	if code != 0 || completed.State != "terminal" || completed.Receipt == nil || completed.Receipt.Outcome != "exited" {
		t.Fatalf("single accepted workload did not reach a terminal receipt: exit=%d run=%+v", code, completed)
	}
	if got := string(readMarker(t, marker)); got != "x" {
		t.Fatalf("terminal workload count = %q, want one physical execution", got)
	}
}

func submissionCLIArgs(stateDir, submissionID, cwd string, argv []string) []string {
	args := []string{
		"run", "--state-dir", stateDir,
		"--submission-id", submissionID,
		"--cwd", cwd,
		"--environment-mode", "inherit-supervisor",
		"--lifetime", "detached",
		"--",
	}
	return append(args, argv...)
}

func writeLostResponseRequest(conn net.Conn, request protocol.Request) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > protocol.MaxFrame {
		return ipc.ErrFrameTooLarge
	}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	for len(frame) > 0 {
		written, err := conn.Write(frame)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		frame = frame[written:]
	}
	return nil
}

func readMarker(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read physical execution marker: %v", err)
	}
	return data
}

func trackHarnessRuns(h *harness) {
	h.t.Helper()
	for _, id := range listedRunIDs(h.t, h) {
		found := false
		for _, tracked := range h.runs {
			if tracked == id {
				found = true
				break
			}
		}
		if !found {
			h.runs = append(h.runs, id)
		}
	}
}

func trackHarnessRunsForCleanup(h *harness) {
	if h.sup == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, jinushiBinary, "list", "--state-dir", h.stateDir, "--limit", "128")
	output, err := cmd.Output()
	if err != nil {
		return
	}
	var result response
	if json.Unmarshal(output, &result) != nil {
		return
	}
	for _, current := range result.Runs {
		found := false
		for _, tracked := range h.runs {
			if tracked == current.ID {
				found = true
				break
			}
		}
		if !found {
			h.runs = append(h.runs, current.ID)
		}
	}
}

func listedRunIDs(t *testing.T, h *harness) []string {
	t.Helper()
	code, _, result := h.invoke(5*time.Second, "list", "--state-dir", h.stateDir, "--limit", "128")
	if code != 0 || result.Error != nil {
		t.Fatalf("list Runs for submission retry audit: exit=%d response=%+v", code, result)
	}
	ids := make([]string, 0, len(result.Runs))
	for _, current := range result.Runs {
		ids = append(ids, current.ID)
	}
	return ids
}
