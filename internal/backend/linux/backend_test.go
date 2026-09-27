//go:build linux

package linux

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
	"golang.org/x/sys/unix"
)

const memoryLimitChildEnv = "JINUSHI_MEMORY_LIMIT_CHILD"
const escapedDescendantMarkerEnv = "JINUSHI_ESCAPED_DESCENDANT_MARKER"

func TestMain(m *testing.M) {
	wasSubreaper, err := childSubreaperEnabled()
	code := m.Run()
	if err == nil && !wasSubreaper {
		if err := setChildSubreaper(false); err != nil {
			code = 1
		}
	}
	os.Exit(code)
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func TestStartOwnsAndTerminatesRealDescendants(t *testing.T) {
	stdout := &lockedBuffer{}
	backend := New()
	process, err := backend.Start(testSpec(t, "/bin/sh", "-c", "sleep 60 & echo $!; wait"), stdout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	childPID := waitForChildPID(t, stdout)
	resources, err := process.Observe()
	if err != nil {
		t.Fatal(err)
	}
	if resources.ProcessCount.Status != "measured" || resources.ProcessCount.Value < 2 {
		t.Fatalf("expected root and descendant process evidence, got %+v", resources.ProcessCount)
	}
	reconciled, err := backend.Reconcile(process.Ownership())
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.State != model.Running || !reconciled.OwnershipProven {
		t.Fatalf("live execution was not revalidated: %+v", reconciled)
	}

	result, err := process.Terminate(150 * time.Millisecond)
	if err != nil {
		t.Fatalf("terminate owned tree: %v (result %+v)", err, result)
	}
	if !result.TreeEmpty {
		t.Fatalf("termination did not prove the tree empty: %+v", result)
	}
	exit, err := process.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if exit.Outcome != "cancelled" && exit.Outcome != "forced-termination" {
		t.Fatalf("unexpected termination outcome: %+v", exit)
	}
	if processIsActive(childPID) {
		t.Fatalf("descendant %d is still active after termination", childPID)
	}
}

func TestFastCommandKeepsPhysicalExitReceipt(t *testing.T) {
	for i := 0; i < 20; i++ {
		process, err := New().Start(testSpec(t, "/bin/sh", "-c", "exit 0"), io.Discard, io.Discard)
		if err != nil {
			t.Fatalf("iteration %d: start fast command: %v", i, err)
		}
		exit, err := process.Wait()
		if err != nil || exit.ExitCode == nil || *exit.ExitCode != 0 || exit.Outcome != "exited" {
			t.Fatalf("iteration %d: physical exit=%+v err=%v", i, exit, err)
		}
	}
}

func TestCompletedNoCgroupCPUTimeIsUnavailable(t *testing.T) {
	process, err := New().Start(testSpec(t, "/bin/sh", "-c", "exit 0"), io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	waitWithTimeout(t, process)
	if process.(*Process).cg != nil {
		t.Skip("no-cgroup CPU accounting fallback is not active")
	}
	resources, err := process.Observe()
	if err != nil {
		t.Fatal(err)
	}
	if resources.CPUTimeNs.Status != "unavailable" {
		t.Fatalf("an empty session cannot prove cumulative CPU time, got %+v", resources.CPUTimeNs)
	}
}

func TestSetsidDescendantRemainsOwnedAfterRootExit(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "escaped-pid")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	spec := testSpec(t, executable, "-test.run=^TestLinuxEscapedDescendantRootHelper$")
	spec.Environment.Set = map[string]string{escapedDescendantMarkerEnv: marker}
	process, err := New().Start(spec, io.Discard, io.Discard)
	if errors.Is(err, ErrUnsupported) {
		t.Skipf("kernel does not support required no-cgroup subreaper ownership: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	linuxProcess := process.(*Process)
	t.Cleanup(func() {
		result, err := process.Terminate(0)
		if err != nil {
			t.Errorf("cleanup escaped-descendant test Run: %v (result %+v)", err, result)
		}
		if !result.TreeEmpty {
			t.Errorf("escaped-descendant test Run cleanup was not proven: %+v", result)
		}
	})
	if linuxProcess.cg != nil {
		result, err := process.Terminate(0)
		if err != nil || !result.TreeEmpty {
			t.Fatalf("cleanup before cgroup-only skip failed: %+v, %v", result, err)
		}
		t.Skip("setsid fallback proof requires a host without writable cgroup v2")
	}

	childPID := waitForMarkedPID(t, marker)
	childInfo, err := readProcInfo(childPID)
	if err != nil {
		t.Fatal(err)
	}
	if !processIsActive(childPID) {
		t.Fatalf("setsid grandchild exited before ownership verification: %+v", childInfo)
	}
	owner := process.Ownership()
	if childInfo.Session == owner.ProcessGroup {
		t.Fatalf("grandchild did not escape the workload session: %+v", childInfo)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		root, err := readProcInfo(owner.PID)
		if errors.Is(err, os.ErrNotExist) || err == nil && (root.State == 'Z' || root.State == 'X' || root.State == 'x') {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if root, err := readProcInfo(owner.PID); err == nil && root.State != 'Z' && root.State != 'X' && root.State != 'x' {
		t.Fatalf("workload root did not exit before ownership check: %+v", root)
	}

	waitDone := make(chan backend.Exit, 1)
	go func() {
		exit, _ := process.Wait()
		waitDone <- exit
	}()
	select {
	case exit := <-waitDone:
		t.Fatalf("Wait reported terminal while setsid descendant %d is active: %+v", childPID, exit)
	case <-time.After(50 * time.Millisecond):
	}
	resources, err := process.Observe()
	if err != nil {
		t.Fatal(err)
	}
	if resources.ProcessCount.Status != "measured" || resources.ProcessCount.Value < 1 {
		t.Fatalf("observation missed setsid descendant after root exit: %+v", resources.ProcessCount)
	}

	reconciled, err := New().Reconcile(owner)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.State != model.Running || !reconciled.OwnershipProven {
		t.Fatalf("reconcile missed setsid descendant after root exit: %+v", reconciled)
	}
	parts := strings.Split(owner.Token, ";")
	if len(parts) != 6 {
		t.Fatalf("no-cgroup ownership token does not include subreaper identity: %q", owner.Token)
	}
	reaperStart, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	parts[5] = strconv.FormatUint(reaperStart+1, 10)
	owner.Token = strings.Join(parts, ";")
	uncertain, err := New().Reconcile(owner)
	if err != nil {
		t.Fatal(err)
	}
	if uncertain.State != model.Uncertain || uncertain.OwnershipProven {
		t.Fatalf("reconcile guessed after subreaper identity mismatch: %+v", uncertain)
	}
	owner = process.Ownership()

	termination, err := process.Terminate(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("terminate escaped descendant: %+v, %v", termination, err)
	}
	if !termination.TreeEmpty {
		t.Fatalf("termination did not prove escaped descendant cleanup: %+v", termination)
	}
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not complete after escaped descendant cleanup")
	}
	if processIsActive(childPID) {
		t.Fatalf("setsid descendant %d remains active after termination", childPID)
	}
}

func TestLinuxEscapedDescendantRootHelper(t *testing.T) {
	marker := os.Getenv(escapedDescendantMarkerEnv)
	if marker == "" {
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(executable, "-test.run=^TestLinuxEscapedDescendantChildHelper$")
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("setsid grandchild did not write its readiness marker")
}

func TestLinuxEscapedDescendantChildHelper(t *testing.T) {
	marker := os.Getenv(escapedDescendantMarkerEnv)
	if marker == "" {
		return
	}
	if _, err := unix.Setsid(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestInteractiveRunUsesRealPTY(t *testing.T) {
	stdout := &lockedBuffer{}
	process, err := New().Start(model.RunSpec{
		Argv:        []string{"/bin/sh", "-c", "read value; printf 'reply:%s\\n' \"$value\""},
		Cwd:         t.TempDir(),
		Interactive: true,
	}, stdout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Resize(40, 120); err != nil {
		t.Fatal(err)
	}
	if err := process.WriteInput([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if err := process.CloseInput(); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("PTY half-close should be explicitly unsupported, got %v", err)
	}
	waitWithTimeout(t, process)
	if output := stdout.String(); !strings.Contains(output, "reply:hello") {
		t.Fatalf("PTY output did not contain command response: %q", output)
	}
}

func TestReplaceEnvironmentPATHControlsExecutableLookup(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(bin, "printf")
	if err := os.WriteFile(program, []byte("#!/bin/sh\nprintf 'effective-path'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	stdout := &lockedBuffer{}
	process, err := New().Start(model.RunSpec{
		Argv: []string{"printf", "ignored"},
		Cwd:  root,
		Environment: model.Environment{
			Mode: "replace",
			Set:  map[string]string{"PATH": bin},
		},
	}, stdout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	exit := waitWithTimeout(t, process)
	if exit.ExitCode == nil || *exit.ExitCode != 0 || stdout.String() != "effective-path" {
		t.Fatalf("effective environment PATH was not used for direct execution: exit=%+v output=%q", exit, stdout.String())
	}
}

func TestTerminateAfterNormalWaitDoesNotMarkTerminationRequested(t *testing.T) {
	process, err := New().Start(testSpec(t, "/bin/sh", "-c", "exit 0"), io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	exit, err := process.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if exit.Outcome != "exited" {
		t.Fatalf("unexpected normal exit outcome: %+v", exit)
	}
	result, err := process.Terminate(0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Requested || !result.TreeEmpty || result.Outcome != "exited" {
		t.Fatalf("post-terminal cleanup changed the physical receipt: %+v", result)
	}
}

func TestNativeLimitsAreEnforcedOrRejectedExplicitly(t *testing.T) {
	caps := New().Capabilities()
	if !caps.MemoryEnforcement {
		_, err := New().Start(limitedSpec(t, model.Limits{MemoryBytes: 64 << 20}, "/bin/true"), io.Discard, io.Discard)
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("memory limit without delegated cgroup support must be rejected, got %v", err)
		}
	} else {
		process, err := New().Start(model.RunSpec{
			Argv:        []string{os.Args[0], "-test.run=TestLinuxMemoryLimitChildHelper"},
			Cwd:         t.TempDir(),
			Environment: model.Environment{Mode: "inherit-supervisor", Set: map[string]string{memoryLimitChildEnv: "1"}},
			Limits:      model.Limits{MemoryBytes: 64 << 20},
		}, io.Discard, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		exit := waitWithTimeout(t, process)
		if exit.Outcome != "resource-limit:memory" {
			t.Fatalf("memory cgroup evidence did not identify enforcement: %+v", exit)
		}
	}

	if caps.ProcessCountEnforcement {
		process, err := New().Start(limitedSpec(t, model.Limits{ProcessCount: 1}, "/bin/sh", "-c", "sleep 30 & wait"), io.Discard, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		exit := waitWithTimeout(t, process)
		if exit.Outcome != "resource-limit:process-count" {
			t.Fatalf("pids controller evidence did not identify enforcement: %+v", exit)
		}
	} else {
		_, err := New().Start(limitedSpec(t, model.Limits{ProcessCount: 2}, "/bin/true"), io.Discard, io.Discard)
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("process-count limit without delegated cgroup support must be rejected, got %v", err)
		}
	}

	if caps.CPUQuotaEnforcement {
		process, err := New().Start(limitedSpec(t, model.Limits{CPUQuotaPercent: 1}, "/bin/sh", "-c", "while :; do :; done"), io.Discard, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		linuxProcess := process.(*Process)
		time.Sleep(300 * time.Millisecond)
		if linuxProcess.cg.counterFile("cpu.stat")["nr_throttled"] <= linuxProcess.cg.baseline["cpu.nr_throttled"] {
			_, _ = process.Terminate(100 * time.Millisecond)
			t.Fatal("CPU quota test workload was not throttled by the kernel")
		}
		if outcome := process.(backend.LimitEvidence).LimitOutcome(); outcome != "" {
			_, _ = process.Terminate(100 * time.Millisecond)
			t.Fatalf("CPU rate throttling must not be a terminating limit event, got %q", outcome)
		}
		result, err := process.Terminate(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("terminate CPU-rate-limited workload: %v", err)
		}
		if result.Outcome == "resource-limit:cpu-quota" {
			t.Fatalf("CPU rate throttling was misclassified as a terminal limit: %+v", result)
		}
	} else {
		_, err := New().Start(limitedSpec(t, model.Limits{CPUQuotaPercent: 10}, "/bin/true"), io.Discard, io.Discard)
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("CPU quota without delegated cgroup support must be rejected, got %v", err)
		}
	}
}

func TestCPUThrottleCounterIsNotATerminatingLimit(t *testing.T) {
	root := t.TempDir()
	for name, contents := range map[string]string{
		"memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
		"pids.events":   "max 0\n",
		"cpu.stat":      "usage_usec 100000\nnr_periods 20\nnr_throttled 15\nthrottled_usec 50000\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cg := &cgroup{
		location: cgroupLocation{childPath: root},
		baseline: map[string]uint64{"memory.max": 0, "memory.oom": 0, "memory.oom_kill": 0, "pids.max": 0},
	}
	if outcome := cg.nativeLimitOutcome(); outcome != "" {
		t.Fatalf("cpu.stat throttling was returned as a terminating limit event: %q", outcome)
	}
}

func TestLinuxMemoryLimitChildHelper(t *testing.T) {
	if os.Getenv(memoryLimitChildEnv) != "1" {
		return
	}
	var held [][]byte
	for range 32 {
		chunk := make([]byte, 16<<20)
		for index := 0; index < len(chunk); index += 4096 {
			chunk[index] = 1
		}
		held = append(held, chunk)
	}
	_ = held
	select {}
}

func testSpec(t *testing.T, argv ...string) model.RunSpec {
	t.Helper()
	return model.RunSpec{Argv: argv, Cwd: t.TempDir(), Environment: model.Environment{Mode: "inherit-supervisor"}}
}

func limitedSpec(t *testing.T, limits model.Limits, argv ...string) model.RunSpec {
	t.Helper()
	spec := testSpec(t, argv...)
	spec.Limits = limits
	return spec
}

func waitForChildPID(t *testing.T, output *lockedBuffer) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		lines := strings.Fields(output.String())
		if len(lines) > 0 {
			pid, err := strconv.Atoi(lines[0])
			if err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for child PID in output %q", output.String())
	return 0
}

func waitForMarkedPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for escaped descendant marker %q", path)
	return 0
}

func processIsActive(pid int) bool {
	info, err := readProcInfo(pid)
	return err == nil && info.State != 'Z' && info.State != 'X' && info.State != 'x'
}

func waitWithTimeout(t *testing.T, process backend.Process) backend.Exit {
	t.Helper()
	type result struct {
		exit backend.Exit
		err  error
	}
	done := make(chan result, 1)
	go func() {
		exit, err := process.Wait()
		done <- result{exit: exit, err: err}
	}()
	select {
	case value := <-done:
		if value.err != nil {
			t.Fatalf("wait: %v (exit %+v)", value.err, value.exit)
		}
		return value.exit
	case <-time.After(8 * time.Second):
		termination, err := process.Terminate(0)
		t.Fatalf("execution did not terminate in time; cleanup=%+v error=%v", termination, err)
		return backend.Exit{}
	}
}
