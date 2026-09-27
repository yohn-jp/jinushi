package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

func serveCLI(t *testing.T, stateDir string, handler ipc.Handler) {
	t.Helper()
	listener, err := ipc.Listen(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ipc.Serve(ctx, listener, handler) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve did not stop")
			listener.Close()
		}
	})
}

func TestRunForwardsArgumentBoundaries(t *testing.T) {
	stateDir := t.TempDir()
	workDir := t.TempDir()
	requestReceived := make(chan protocol.Request, 1)
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		requestReceived <- request
		return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: "run_test", State: model.Accepted}}
	})

	var stdout, stderr bytes.Buffer
	code := Main(context.Background(), []string{
		"run", "--state-dir", stateDir, "--submission-id", "caller-run-1", "--cwd", workDir, "--",
		"program name", "two words", "$(do-not-run)", "",
	}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("Main exit = %d; stderr=%s", code, stderr.String())
	}
	request := <-requestReceived
	if request.Op != "run" || request.Spec == nil || request.SubmissionID != "caller-run-1" {
		t.Fatalf("unexpected request: %#v", request)
	}
	want := []string{"program name", "two words", "$(do-not-run)", ""}
	if !equalStrings(request.Spec.Argv, want) {
		t.Fatalf("argv = %#v, want %#v", request.Spec.Argv, want)
	}
	if request.Spec.Cwd != workDir || request.Spec.Lifetime.Mode != "detached" {
		t.Fatalf("Run spec lost explicit cwd/lifetime: %#v", request.Spec)
	}
}

func TestListForwardsPageCursorAndLimitAndExposesNextCursor(t *testing.T) {
	stateDir := t.TempDir()
	requestReceived := make(chan protocol.Request, 1)
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		requestReceived <- request
		return protocol.Response{
			Version:    model.ProtocolVersion,
			Runs:       []model.Run{{ID: "run_page_item", State: model.Running}},
			NextCursor: "run_page_item",
		}
	})
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(), []string{"list", "--state-dir", stateDir, "--cursor", "run_before", "--limit", "17"}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("list exit = %d; stderr=%s", code, stderr.String())
	}
	request := <-requestReceived
	if request.Op != "list" || request.Cursor != "run_before" || request.Limit != 17 {
		t.Fatalf("list request = %#v; want cursor=run_before limit=17", request)
	}
	var response protocol.Response
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("list output is not JSON: %v; output=%s", err, stdout.String())
	}
	if response.NextCursor != "run_page_item" || len(response.Runs) != 1 || response.Runs[0].ID != "run_page_item" {
		t.Fatalf("list JSON response = %#v", response)
	}
}

func TestListDefaultsPageLimitAndRejectsNonPositiveLimit(t *testing.T) {
	stateDir := t.TempDir()
	requestReceived := make(chan protocol.Request, 1)
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		requestReceived <- request
		return protocol.Response{Version: model.ProtocolVersion}
	})
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(), []string{"list", "--state-dir", stateDir}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("default list exit = %d; stderr=%s", code, stderr.String())
	}
	request := <-requestReceived
	if request.Cursor != "" || request.Limit != 64 {
		t.Fatalf("default list request = %#v; want empty cursor and limit=64", request)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(stdout.Bytes(), &fields); err != nil {
		t.Fatalf("list output is not JSON: %v", err)
	}
	if _, ok := fields["nextCursor"]; !ok {
		t.Fatalf("default JSON omitted nextCursor: %s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = Main(context.Background(), []string{"list", "--state-dir", stateDir, "--limit", "0"}, &stdout, &stderr, nil)
	if code != 2 || !strings.Contains(stderr.String(), "--limit must be positive") {
		t.Fatalf("invalid list limit returned %d; stderr=%s", code, stderr.String())
	}
	select {
	case request := <-requestReceived:
		t.Fatalf("invalid list limit reached the supervisor: %#v", request)
	default:
	}
}

func TestAwaitProjectsOnlyExitedRunCode(t *testing.T) {
	cases := []struct {
		name    string
		outcome string
		exit    int
		want    int
	}{
		{name: "cancelled with zero code is failure", outcome: "cancelled", exit: 0, want: 1},
		{name: "timed out with zero code is failure", outcome: "timed-out", exit: 0, want: 1},
		{name: "exited code is propagated", outcome: "exited", exit: 7, want: 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := t.TempDir()
			exit := tc.exit
			serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
				if request.Op != "await" {
					return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: "unexpected", Message: request.Op}}
				}
				return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{
					ID: request.RunID, State: model.Terminal,
					Receipt: &model.Receipt{Version: 1, RunID: request.RunID, Outcome: tc.outcome, ExitCode: &exit, FinishedAt: time.Now()},
				}}
			})
			var stdout, stderr bytes.Buffer
			got := Main(context.Background(), []string{"await", "--state-dir", stateDir, "run_test"}, &stdout, &stderr, nil)
			if got != tc.want {
				t.Fatalf("exit = %d, want %d; stderr=%s", got, tc.want, stderr.String())
			}
		})
	}
}

func TestOutputDecodesArbitraryBytes(t *testing.T) {
	stateDir := t.TempDir()
	want := []byte{0, 0xff, '\n', 0xc0, 'x'}
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		if request.Op != "output" || request.Offset != 0 {
			return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: "unexpected", Message: request.Op}}
		}
		return protocol.Response{Version: model.ProtocolVersion, Data: base64.StdEncoding.EncodeToString(want)}
	})
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(), []string{"output", "--state-dir", stateDir, "run_test"}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("Main exit = %d; stderr=%s", code, stderr.String())
	}
	if !bytes.Equal(stdout.Bytes(), want) {
		t.Fatalf("output bytes = %v, want %v", stdout.Bytes(), want)
	}
}

func TestAttachLoopStreamsPTYBytesWithoutControlJSON(t *testing.T) {
	stateDir := t.TempDir()
	var outputCalls int
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		if request.Op != "inspect" && request.AttachID != "attach_test" {
			return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: "bad-attachment", Message: request.AttachID}}
		}
		switch request.Op {
		case "output":
			outputCalls++
			if outputCalls == 1 {
				return protocol.Response{Version: model.ProtocolVersion, Data: base64.StdEncoding.EncodeToString([]byte("pty-bytes"))}
			}
			return protocol.Response{Version: model.ProtocolVersion}
		case "inspect":
			return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: request.RunID, State: model.Terminal}}
		default:
			return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: "unexpected-op", Message: request.Op}}
		}
	})
	var stdout, stderr bytes.Buffer
	initial := &model.Run{ID: "run_test", Generation: 1, Spec: model.RunSpec{Interactive: true}}
	code := attachLoop(context.Background(), stateDir, "run_test", "attach_test", false, strings.NewReader(""), &stdout, &stderr, initial, false)
	if code != 0 {
		t.Fatalf("attachLoop exit = %d; stderr=%s", code, stderr.String())
	}
	if stdout.String() != "pty-bytes" {
		t.Fatalf("stdout = %q; want only PTY bytes", stdout.String())
	}
}

func TestAttachLoopReturnsFailureForUncertainRun(t *testing.T) {
	stateDir := t.TempDir()
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		switch request.Op {
		case "output":
			return protocol.Response{Version: model.ProtocolVersion}
		case "inspect":
			return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: request.RunID, State: model.Uncertain}}
		default:
			return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: "unexpected-op", Message: request.Op}}
		}
	})
	var stdout, stderr bytes.Buffer
	initial := &model.Run{ID: "run_uncertain", Generation: 1, Spec: model.RunSpec{Interactive: true}}
	code := attachLoop(context.Background(), stateDir, initial.ID, "attach_test", false, strings.NewReader(""), &stdout, &stderr, initial, false)
	if code != 1 || !strings.Contains(stderr.String(), "uncertain") {
		t.Fatalf("attachLoop exit=%d stderr=%q", code, stderr.String())
	}
}

func TestDetachUsesSupervisorAttachmentID(t *testing.T) {
	stateDir := t.TempDir()
	requestReceived := make(chan protocol.Request, 1)
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		requestReceived <- request
		return protocol.Response{Version: model.ProtocolVersion}
	})
	var stderr bytes.Buffer
	detachAttachment(context.Background(), stateDir, "run_test", "attach_test", &stderr)
	request := <-requestReceived
	if request.Op != "detach" || request.RunID != "run_test" || request.AttachID != "attach_test" {
		t.Fatalf("unexpected detach request: %#v", request)
	}
}

func TestEventsFollowEmitsNDJSONGapsAndDrainsLateTerminalEvent(t *testing.T) {
	stateDir := t.TempDir()
	var calls int
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		calls++
		if request.Op != "events" || request.Follow || request.Limit != 1 {
			return protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: "bad-follow-request", Message: "follow must be a one-event page"}}
		}
		running := &model.Run{ID: request.RunID, State: model.Running}
		terminal := &model.Run{ID: request.RunID, State: model.Terminal}
		switch calls {
		case 1:
			return protocol.Response{Version: model.ProtocolVersion, Run: running, Gap: true, RetainedFrom: 3, Events: []model.Event{{Version: 1, RunID: request.RunID, Seq: 3, Kind: "run.started"}}}
		case 2:
			return protocol.Response{Version: model.ProtocolVersion, Run: running, Events: []model.Event{{Version: 1, RunID: request.RunID, Seq: 5, Kind: "resource.sample"}}}
		case 3:
			return protocol.Response{Version: model.ProtocolVersion, Run: terminal}
		case 4:
			return protocol.Response{Version: model.ProtocolVersion, Run: terminal, Events: []model.Event{{Version: 1, RunID: request.RunID, Seq: 6, Kind: "run.terminal"}}}
		default:
			return protocol.Response{Version: model.ProtocolVersion, Run: terminal}
		}
	})
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(), []string{"events", "--state-dir", stateDir, "--follow", "run_test"}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("events --follow exit = %d; stderr=%s; output=%s", code, stderr.String(), stdout.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 6 {
		t.Fatalf("got %d NDJSON lines, want 6: %s", len(lines), stdout.String())
	}
	var records []map[string]any
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid NDJSON record %q: %v", line, err)
		}
		records = append(records, record)
	}
	if records[0]["type"] != "gap" || records[0]["reason"] != "journal" || records[0]["retainedFrom"] != float64(3) {
		t.Fatalf("missing retained-history watermark: %#v", records[0])
	}
	if records[1]["type"] != "event" || records[2]["type"] != "gap" || records[2]["reason"] != "sequence-hole" {
		t.Fatalf("unexpected records around sequence hole: %#v", records[:4])
	}
	terminalEvent := records[4]["event"].(map[string]any)
	if records[4]["type"] != "event" || terminalEvent["kind"] != "run.terminal" {
		t.Fatalf("late terminal event was not emitted before completion: %#v", records[4])
	}
	if records[5]["type"] != "terminal" || calls != 5 {
		t.Fatalf("terminal marker/call count = %v/%d, want terminal/5", records[5]["type"], calls)
	}
}

func TestEventsFollowEmitsUncertainFinalRecordAndFails(t *testing.T) {
	stateDir := t.TempDir()
	var calls int
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		calls++
		return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: request.RunID, State: model.Uncertain}}
	})
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(), []string{"events", "--state-dir", stateDir, "--follow", "run_uncertain"}, &stdout, &stderr, nil)
	if code != 1 {
		t.Fatalf("events --follow exit = %d; want 1 for uncertain Run; stderr=%s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d NDJSON records, want one uncertain final record: %s", len(lines), stdout.String())
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("invalid NDJSON record %q: %v", lines[0], err)
	}
	if record["type"] != "uncertain" || record["state"] != string(model.Uncertain) || record["runId"] != "run_uncertain" {
		t.Fatalf("uncertain final record = %#v", record)
	}
	if calls != 2 {
		t.Fatalf("event handler calls = %d, want initial uncertain snapshot plus one final query", calls)
	}
}

func TestCloseInputUsesRunIdentity(t *testing.T) {
	stateDir := t.TempDir()
	requestReceived := make(chan protocol.Request, 1)
	serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
		requestReceived <- request
		return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: request.RunID, State: model.Running}}
	})
	var stdout, stderr bytes.Buffer
	code := Main(context.Background(), []string{"close-input", "--state-dir", stateDir, "--request-id", "close-1", "--expected-generation", "3", "run_test"}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("close-input exit = %d; stderr=%s", code, stderr.String())
	}
	request := <-requestReceived
	if request.Op != "close-input" || request.RunID != "run_test" || request.RequestID != "close-1" || request.ExpectedGeneration != 3 {
		t.Fatalf("unexpected close-input request: %#v", request)
	}
}

func TestPhysicalControlCommandsForwardRetryIdentity(t *testing.T) {
	cases := []struct {
		name string
		args []string
		op   string
	}{
		{name: "input", args: []string{"input", "--request-id", "retry-1", "--expected-generation", "5", "run_test", "bytes"}, op: "input"},
		{name: "signal", args: []string{"signal", "--request-id", "retry-1", "--expected-generation", "5", "run_test", "USR1"}, op: "signal"},
		{name: "cancel", args: []string{"cancel", "--request-id", "retry-1", "--expected-generation", "5", "run_test"}, op: "cancel"},
		{name: "resize", args: []string{"resize", "--request-id", "retry-1", "--expected-generation", "5", "--rows", "24", "--cols", "80", "run_test"}, op: "resize"},
		{name: "close-input", args: []string{"close-input", "--request-id", "retry-1", "--expected-generation", "5", "run_test"}, op: "close-input"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := t.TempDir()
			requestReceived := make(chan protocol.Request, 1)
			serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
				requestReceived <- request
				return protocol.Response{Version: model.ProtocolVersion, Run: &model.Run{ID: request.RunID, State: model.Running, Generation: 6}}
			})
			args := append([]string{tc.args[0], "--state-dir", stateDir}, tc.args[1:]...)
			var stdout, stderr bytes.Buffer
			code := Main(context.Background(), args, &stdout, &stderr, nil)
			if code != 0 {
				t.Fatalf("%s exit=%d stderr=%s", tc.name, code, stderr.String())
			}
			request := <-requestReceived
			if request.Op != tc.op || request.RunID != "run_test" || request.RequestID != "retry-1" || request.ExpectedGeneration != 5 {
				t.Fatalf("request = %+v", request)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
