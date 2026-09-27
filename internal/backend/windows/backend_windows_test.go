//go:build windows

package windows

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestWindowsBackendChild(t *testing.T) {
	mode := os.Getenv("JINUSHI_WINDOWS_BACKEND_CHILD")
	if mode == "" {
		return
	}
	args := helperArgs()
	switch mode {
	case "echo":
		if len(args) > 0 {
			_, _ = fmt.Fprint(os.Stdout, args[0])
		}
	case "flood":
		payload := bytes.Repeat([]byte("x"), 64*1024)
		for range 128 {
			if _, err := os.Stdout.Write(payload); err != nil {
				return
			}
		}
	case "pty":
		input := make([]byte, 256)
		n, _ := os.Stdin.Read(input)
		_, _ = fmt.Fprintf(os.Stdout, "PTY:%s", input[:n])
	case "tree", "limited-tree":
		if len(args) == 0 {
			os.Exit(31)
		}
		terminateChild := mode == "limited-tree"
		if err := startGrandchild(args[0], terminateChild); err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "process-limit-enforced:%v", err)
			return
		}
		_, _ = fmt.Fprint(os.Stdout, "unexpected-descendant-started")
	case "grandchild":
		if len(args) == 0 {
			os.Exit(32)
		}
		_ = os.WriteFile(args[0], []byte("ready"), 0600)
		for {
			time.Sleep(time.Hour)
		}
	}
}

func TestCreateProcessPreservesArgvAndPipes(t *testing.T) {
	exe, _ := os.Executable()
	payload := `spaces and \slashes plus "quotes"`
	var stdout, stderr bytes.Buffer
	process, err := Start(model.RunSpec{
		Argv: []string{exe, "-test.run=^TestWindowsBackendChild$", "--", payload},
		Cwd:  t.TempDir(),
		Environment: model.Environment{
			Mode: "inherit-supervisor",
			Set:  map[string]string{"JINUSHI_WINDOWS_BACKEND_CHILD": "echo"},
		},
	}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := process.Observe()
	if err != nil {
		t.Fatal(err)
	}
	if resources.ProcessCount.Status != "measured" || resources.ProcessCount.Value < 1 {
		t.Fatalf("process count was not measured while running: %+v", resources.ProcessCount)
	}
	reconciled, err := Reconcile(process.Ownership())
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.State != model.Running || !reconciled.OwnershipProven {
		t.Fatalf("live Job Object was not reconciled: %+v", reconciled)
	}
	if err := process.WriteInput([]byte("ignored")); err != nil {
		t.Fatal(err)
	}
	if err := process.CloseInput(); err != nil {
		t.Fatal(err)
	}
	exit, err := process.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if exit.ExitCode == nil || *exit.ExitCode != 0 {
		t.Fatalf("unexpected process exit: %+v", exit)
	}
	if !strings.Contains(stdout.String(), payload) {
		t.Fatalf("child argv was not preserved; output %q does not contain %q", stdout.String(), payload)
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected child stderr: %q", stderr.String())
	}
	termination, err := process.Terminate(0)
	if err != nil {
		t.Fatal(err)
	}
	if termination.Requested || !termination.TreeEmpty {
		t.Fatalf("termination after normal completion changed lifecycle evidence: %+v", termination)
	}
}

func TestJobObjectTerminatesDescendants(t *testing.T) {
	exe, _ := os.Executable()
	marker := filepath.Join(t.TempDir(), "grandchild-ready")
	var stdout, stderr bytes.Buffer
	process, err := Start(model.RunSpec{
		Argv: []string{exe, "-test.run=^TestWindowsBackendChild$", "--", marker},
		Cwd:  t.TempDir(),
		Environment: model.Environment{
			Mode: "inherit-supervisor",
			Set:  map[string]string{"JINUSHI_WINDOWS_BACKEND_CHILD": "tree"},
		},
	}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitForFile(marker, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	resources, err := process.Observe()
	if err != nil {
		t.Fatal(err)
	}
	if resources.PeakProcessCount.Status != "measured" || resources.PeakProcessCount.Value < 2 {
		t.Fatalf("Job Object process notifications did not observe the descendant: %+v", resources.PeakProcessCount)
	}
	result, err := process.Terminate(100 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !result.TreeEmpty || !result.Forced {
		t.Fatalf("Job Object did not prove forced descendant cleanup: %+v", result)
	}
	exit, err := process.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if exit.ExitCode == nil {
		t.Fatalf("missing direct process exit code after Job Object termination: %+v", exit)
	}
}

func TestPipeOutputBackpressureAndFlood(t *testing.T) {
	exe, _ := os.Executable()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWriter := func() { releaseOnce.Do(func() { close(release) }) }
	writer := &gatedCounterWriter{started: make(chan struct{}), release: release}
	var stderr bytes.Buffer
	process, err := Start(model.RunSpec{
		Argv: []string{exe, "-test.run=^TestWindowsBackendChild$", "--"},
		Cwd:  t.TempDir(),
		Environment: model.Environment{
			Mode: "inherit-supervisor",
			Set:  map[string]string{"JINUSHI_WINDOWS_BACKEND_CHILD": "flood"},
		},
	}, writer, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseWriter()
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("output pump did not reach the writer")
	}
	wait := make(chan error, 1)
	go func() {
		_, err := process.Wait()
		wait <- err
	}()
	select {
	case err := <-wait:
		t.Fatalf("Run completed while its output writer was applying backpressure: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseWriter()
	select {
	case err := <-wait:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not complete after output backpressure was released")
	}
	if got := writer.bytes.Load(); got < 8<<20 {
		t.Fatalf("output flood was truncated: observed %d bytes, want at least %d", got, 8<<20)
	}
}

func TestJobObjectEnforcesProcessCount(t *testing.T) {
	exe, _ := os.Executable()
	marker := filepath.Join(t.TempDir(), "must-not-start")
	var stdout, stderr bytes.Buffer
	process, err := Start(model.RunSpec{
		Argv: []string{exe, "-test.run=^TestWindowsBackendChild$", "--", marker},
		Cwd:  t.TempDir(),
		Environment: model.Environment{
			Mode: "inherit-supervisor",
			Set:  map[string]string{"JINUSHI_WINDOWS_BACKEND_CHILD": "limited-tree"},
		},
		Limits: model.Limits{ProcessCount: 1},
	}, &stdout, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	exit, err := process.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("descendant escaped the one-process Job Object limit (stat err: %v)", err)
	}
	if !strings.Contains(stdout.String(), "process-limit-enforced") {
		t.Fatalf("child start did not report the process limit: %q", stdout.String())
	}
	if exit.Outcome != "exited" && exit.Outcome != "resource-limit:process-count" {
		t.Fatalf("unexpected outcome after the native process limit: %+v", exit)
	}
}

func TestConPTYInputOutputAndResize(t *testing.T) {
	if !Capabilities().PTY {
		t.Skip("ConPTY is unavailable on this Windows version")
	}
	exe, _ := os.Executable()
	var output bytes.Buffer
	process, err := Start(model.RunSpec{
		Argv: []string{exe, "-test.run=^TestWindowsBackendChild$", "--", "pty"},
		Cwd:  t.TempDir(), Interactive: true,
		Environment: model.Environment{
			Mode: "inherit-supervisor",
			Set:  map[string]string{"JINUSHI_WINDOWS_BACKEND_CHILD": "pty"},
		},
	}, &output, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Resize(40, 120); err != nil {
		t.Fatal(err)
	}
	if err := process.WriteInput([]byte("conpty-input\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Wait(); err != nil {
		t.Fatal(err)
	}
	result, err := process.Terminate(0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Requested {
		t.Fatalf("termination after proven normal completion was reported as requested: %+v", result)
	}
	if !strings.Contains(output.String(), "PTY:conpty-input") {
		t.Fatalf("ConPTY input/output was not transported: %q", output.String())
	}
}

func TestWindowsArgumentQuoting(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{"plain", "plain"},
		{"", `""`},
		{"two words", `"two words"`},
		{`quote"inside`, `"quote\"inside"`},
		{`space and trailing\`, `"space and trailing\\"`},
	}
	for _, test := range cases {
		if got := quoteWindowsArgument(test.input); got != test.want {
			t.Errorf("quoteWindowsArgument(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func helperArgs() []string {
	for index, arg := range os.Args {
		if arg == "--" && index+1 < len(os.Args) {
			return os.Args[index+1:]
		}
	}
	return nil
}

func startGrandchild(marker string, terminate bool) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, "-test.run=^TestWindowsBackendChild$", "--", marker)
	cmd.Env = make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(strings.ToUpper(entry), "JINUSHI_WINDOWS_BACKEND_CHILD=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "JINUSHI_WINDOWS_BACKEND_CHILD=grandchild")
	cmd.Stdout, cmd.Stderr, cmd.Stdin = io.Discard, io.Discard, strings.NewReader("")
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := waitForFile(marker, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return err
	}
	if terminate {
		if err := cmd.Process.Kill(); err != nil {
			return err
		}
		_, err := cmd.Process.Wait()
		return err
	}
	return cmd.Process.Release()
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("file %s was not created before timeout", path)
}

type gatedCounterWriter struct {
	once    sync.Once
	started chan struct{}
	release <-chan struct{}
	bytes   atomic.Int64
}

func (w *gatedCounterWriter) Write(data []byte) (int, error) {
	w.once.Do(func() {
		close(w.started)
		<-w.release
	})
	w.bytes.Add(int64(len(data)))
	return len(data), nil
}
