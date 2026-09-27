package supervisor

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	internalcli "github.com/yohn-jp/jinushi/internal/cli"
	"github.com/yohn-jp/jinushi/internal/ipc"
)

func TestCLIWriterControlsUseSupervisorLeaseEndToEnd(t *testing.T) {
	physical := &controlTestPhysical{}
	svc, _ := newControlTestService(t, physical)
	listener, err := ipc.Listen(svc.root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ipc.ServeWithNotifier(ctx, listener, svc.Handle, svc.notifier) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ServeWithNotifier: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("ServeWithNotifier did not stop")
		}
	})

	stateDir := filepath.Clean(svc.root)
	cases := [][]string{
		{"input", "--request-id", "cli-input", "--expected-generation", "1", "run_control_test", "payload"},
		{"resize", "--request-id", "cli-resize", "--expected-generation", "2", "--rows", "24", "--cols", "80", "run_control_test"},
		{"close-input", "--request-id", "cli-close", "--expected-generation", "3", "run_control_test"},
	}
	for _, args := range cases {
		args = append([]string{args[0], "--state-dir", stateDir}, args[1:]...)
		var stdout, stderr bytes.Buffer
		if code := internalcli.Main(context.Background(), args, &stdout, &stderr, nil); code != 0 {
			t.Fatalf("CLI %s exit=%d stderr=%q output=%q", args[0], code, stderr.String(), stdout.String())
		}
	}

	physical.mu.Lock()
	defer physical.mu.Unlock()
	if physical.inputCalls != 1 || string(physical.input) != "payload" || physical.resizeCalls != 1 || physical.closeCalls != 1 {
		t.Fatalf("physical effects input=%d/%q resize=%d close=%d", physical.inputCalls, physical.input, physical.resizeCalls, physical.closeCalls)
	}
}
