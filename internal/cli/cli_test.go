package cli

import (
	"bytes"
	"context"
	"encoding/base64"
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
		"run", "--state-dir", stateDir, "--cwd", workDir, "--",
		"program name", "two words", "$(do-not-run)", "",
	}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("Main exit = %d; stderr=%s", code, stderr.String())
	}
	request := <-requestReceived
	if request.Op != "run" || request.Spec == nil {
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
	initial := &model.Run{ID: "run_test", Spec: model.RunSpec{Interactive: true}}
	code := attachLoop(context.Background(), stateDir, "run_test", "attach_test", false, strings.NewReader(""), &stdout, &stderr, initial, false)
	if code != 0 {
		t.Fatalf("attachLoop exit = %d; stderr=%s", code, stderr.String())
	}
	if stdout.String() != "pty-bytes" {
		t.Fatalf("stdout = %q; want only PTY bytes", stdout.String())
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
