//go:build linux

package linux

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestReadProcessEvidenceFromProcFixture(t *testing.T) {
	procRoot := t.TempDir()
	writeProcFixture(t, procRoot, 10, "reaper", 1, 10, 10, 1, 2, 100, 1)
	writeProcFixture(t, procRoot, 11, "worker) fixture", 10, 10, 10, 2, 3, 101, 2)
	writeProcFixture(t, procRoot, 12, "child", 11, 10, 10, 4, 5, 102, 3)
	writeProcFixture(t, procRoot, 13, "outside", 1, 13, 13, 0, 0, 103, 0)
	if err := os.WriteFile(filepath.Join(procRoot, "12", "comm"), []byte("child\x00worker\n"), 0600); err != nil {
		t.Fatal(err)
	}

	processes, complete, err := scanSubreaperProcessEvidence(procRoot, subreaperIdentity{PID: 10, StartTime: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(processes) != 2 {
		t.Fatalf("subreaper snapshot = (%d processes, complete=%t), want its two descendants", len(processes), complete)
	}
	root, child := processes[0], processes[1]
	if root.PID != 11 || root.ParentPID != 10 || root.ParentStartTimeTicks != 100 || !root.ParentObserved {
		t.Fatalf("root parent identity was not captured: %+v", root)
	}
	if root.Comm != "worker) fixture" || root.Membership != model.ProcessMembershipOwned {
		t.Fatalf("safe process identity mismatch: %+v", root)
	}
	if root.CPUTimeNs.Status != string(model.EvidenceMeasured) || root.CPUTimeNs.Value != 50_000_000 {
		t.Fatalf("per-process CPU evidence = %+v, want 50ms", root.CPUTimeNs)
	}
	if root.RSSBytes.Status != string(model.EvidenceMeasured) || root.RSSBytes.Value != 2*int64(os.Getpagesize()) {
		t.Fatalf("per-process RSS evidence = %+v", root.RSSBytes)
	}
	if child.PID != 12 || child.ParentPID != 11 || child.ParentStartTimeTicks != 101 || !child.ParentObserved {
		t.Fatalf("grandchild parent identity was not captured: %+v", child)
	}
	if strings.ContainsAny(child.Comm, "\x00\n\r\t") {
		t.Fatalf("comm retained a control character: %q", child.Comm)
	}
}

func TestCgroupPIDEvidenceParserIsBoundedAndRejectsInvalidIdentity(t *testing.T) {
	pids, complete, err := parseCgroupPIDList([]byte("41\n7 23\n"), 3)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(pids) != 3 || pids[0] != 7 || pids[1] != 23 || pids[2] != 41 {
		t.Fatalf("parsed cgroup process IDs = %v, complete=%t", pids, complete)
	}
	pids, complete, err = parseCgroupPIDList([]byte("9 3 7"), 2)
	if err != nil {
		t.Fatal(err)
	}
	if complete || len(pids) != 2 {
		t.Fatalf("over-limit cgroup membership was not explicitly bounded: %v complete=%t", pids, complete)
	}
	for _, input := range []string{"0", "-1", "not-a-pid", "4 4"} {
		if _, _, err := parseCgroupPIDList([]byte(input), 4); err == nil {
			t.Errorf("parseCgroupPIDList(%q) unexpectedly succeeded", input)
		}
	}
}

func TestProcessEvidenceTruncationMatchesStoreProcessLimit(t *testing.T) {
	bootID, err := readBootID()
	if err != nil {
		t.Fatal(err)
	}
	reaper := subreaperIdentity{PID: 10, StartTime: 100}

	for _, test := range []struct {
		name         string
		processCount int
		wantStatus   model.EvidenceStatus
		wantReason   string
		wantComplete bool
	}{
		{
			name:         "at store limit",
			processCount: maxEvidenceProcesses,
			wantStatus:   model.EvidenceMeasured,
			wantReason:   "membership-baseline-established",
			wantComplete: false,
		},
		{
			name:         "one over store limit",
			processCount: maxEvidenceProcesses + 1,
			wantStatus:   model.EvidenceUnavailable,
			wantReason:   "process-membership-truncated",
			wantComplete: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			procRoot := t.TempDir()
			writeProcFixture(t, procRoot, reaper.PID, "reaper", 1, 10, 10, 0, 0, reaper.StartTime, 1)
			for index := 0; index < test.processCount; index++ {
				pid := 1000 + index
				writeProcFixture(t, procRoot, pid, "worker", reaper.PID, 10, 10, 1, 0, uint64(200+index), 1)
			}

			process := &Process{ownership: model.Ownership{
				Backend:      "linux",
				PID:          1000,
				ProcessGroup: 10,
				Token:        ownershipTokenWithSubreaper(bootID, 10, reaper),
			}}
			sample, err := process.collectEvidenceAtProcRootLocked(time.Now().UTC(), procRoot)
			if err != nil {
				t.Fatalf("collect process evidence: %v", err)
			}
			if sample.ProcessEvidenceStatus != test.wantStatus || sample.ProcessEvidenceReason != test.wantReason || sample.ProcessEvidenceComplete != test.wantComplete {
				t.Fatalf("process evidence status = (%q, %q, complete=%t), want (%q, %q, complete=%t)",
					sample.ProcessEvidenceStatus, sample.ProcessEvidenceReason, sample.ProcessEvidenceComplete,
					test.wantStatus, test.wantReason, test.wantComplete)
			}
			if len(sample.Processes) > maxEvidenceProcesses {
				t.Fatalf("producer emitted %d processes above the store limit %d", len(sample.Processes), maxEvidenceProcesses)
			}
			if test.processCount == maxEvidenceProcesses && len(sample.Processes) != maxEvidenceProcesses {
				t.Fatalf("at-limit process evidence count = %d, want %d", len(sample.Processes), maxEvidenceProcesses)
			}
		})
	}
}

func TestProcessEvidenceCommIsBoundedAndSanitized(t *testing.T) {
	if got := sanitizeProcessComm([]byte("tool\x00name\r\n")); got != "tool�name�" {
		t.Fatalf("sanitized comm = %q", got)
	}
	if got := sanitizeProcessComm([]byte(strings.Repeat("x", 80))); len(got) > 64 {
		t.Fatalf("comm exceeded the storage bound: %d bytes", len(got))
	}
}

func TestLinuxProcessEvidenceCapturesOwnedTreeAndFinalExit(t *testing.T) {
	stdout := &lockedBuffer{}
	script := "sleep 60 & first=$!; printf '%s\\n' \"$first\"; read spawn; sleep 60 & second=$!; printf '%s\\n' \"$second\"; read finish; kill \"$first\" \"$second\"; wait \"$first\"; wait \"$second\""
	process, err := New().Start(testSpec(t, "/bin/sh", "-c", script), stdout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	linuxProcess := process.(*Process)
	cleaned := false
	defer func() {
		if !cleaned {
			_, _ = linuxProcess.Terminate(0)
			<-linuxProcess.waitDone
		}
	}()
	childPID := waitPIDAtOffset(t, stdout, 0)
	first, err := linuxProcess.Evidence()
	if err != nil {
		t.Fatalf("collect process evidence: %v", err)
	}
	if first.ProcessEvidenceStatus != model.EvidenceMeasured {
		t.Fatalf("process membership unavailable: %+v", first)
	}
	byPID := make(map[int]model.ProcessEvidence, len(first.Processes))
	for _, evidence := range first.Processes {
		byPID[evidence.PID] = evidence
	}
	root, rootOK := byPID[linuxProcess.ownership.PID]
	child, childOK := byPID[childPID]
	if !rootOK || !childOK {
		t.Fatalf("owned subtree omitted root/child: root=%t child=%t sample=%+v", rootOK, childOK, first.Processes)
	}
	if root.Membership != model.ProcessMembershipOwned || root.StartTimeTicks != linuxProcess.ownership.StartTime {
		t.Fatalf("root identity is not pinned to the Run: %+v owner=%+v", root, linuxProcess.ownership)
	}
	if !child.ParentObserved || child.ParentPID != root.PID {
		t.Fatalf("child parent relationship is not observed: %+v root=%+v", child, root)
	}
	if child.CPUTimeNs.Status != string(model.EvidenceMeasured) || child.RSSBytes.Status != string(model.EvidenceMeasured) {
		t.Fatalf("per-process counters are not measured: %+v", child)
	}
	assertMetricStatus(t, linuxProcess.cg != nil, first.IO.ReadBytes)
	assertMetricStatus(t, linuxProcess.cg != nil, first.PSI.CPU.SomeTotalUS)

	firstOutputLength := len(stdout.String())
	if err := linuxProcess.WriteInput([]byte("spawn\n")); err != nil {
		t.Fatal(err)
	}
	secondChildPID := waitPIDAtOffset(t, stdout, firstOutputLength)
	second, err := linuxProcess.Evidence()
	if err != nil {
		t.Fatalf("collect changed process evidence: %v", err)
	}
	startedSecondChild := false
	for _, change := range second.ProcessChanges {
		if change.Kind == model.ProcessStarted && change.Process.PID == secondChildPID {
			startedSecondChild = true
		}
	}
	if !startedSecondChild {
		t.Fatalf("process-start evidence omitted new child %d: %+v", secondChildPID, second.ProcessChanges)
	}

	if err := linuxProcess.WriteInput([]byte("finish\n")); err != nil {
		t.Fatal(err)
	}
	waitWithTimeout(t, process)
	cleaned = true
	final, err := linuxProcess.Evidence()
	if err != nil {
		t.Fatalf("read final process evidence: %v", err)
	}
	foundExit := false
	for _, change := range final.ProcessChanges {
		if change.Kind == model.ProcessExited && (change.Process.PID == childPID || change.Process.PID == root.PID || change.Process.PID == secondChildPID) {
			foundExit = true
		}
	}
	if !foundExit {
		t.Fatalf("final evidence omitted exited Run processes: %+v", final.ProcessChanges)
	}
}

func waitPIDAtOffset(t *testing.T, output *lockedBuffer, offset int) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		text := output.String()
		if offset <= len(text) {
			for _, line := range strings.Split(text[offset:], "\n") {
				pid, err := strconv.Atoi(strings.TrimSpace(line))
				if err == nil && pid > 0 {
					return pid
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for PID output after byte %d: %q", offset, output.String())
	return 0
}

func assertMetricStatus(t *testing.T, cgroupSelected bool, metric model.Metric) {
	t.Helper()
	if metric.Status != string(model.EvidenceMeasured) && metric.Status != string(model.EvidenceUnavailable) && metric.Status != string(model.EvidenceUnsupported) {
		t.Fatalf("metric has no explicit evidence status: %+v", metric)
	}
	if !cgroupSelected && metric.Status != string(model.EvidenceUnsupported) {
		t.Fatalf("process-only ownership reported cgroup evidence as %q", metric.Status)
	}
}

func writeProcFixture(t *testing.T, root string, pid int, comm string, ppid, pgrp, session int, user, system, start uint64, rssPages int64) {
	t.Helper()
	directory := filepath.Join(root, strconv.Itoa(pid))
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	fields := make([]string, 22)
	for index := range fields {
		fields[index] = "0"
	}
	fields[0] = "S"
	fields[1] = strconv.Itoa(ppid)
	fields[2] = strconv.Itoa(pgrp)
	fields[3] = strconv.Itoa(session)
	fields[11] = strconv.FormatUint(user, 10)
	fields[12] = strconv.FormatUint(system, 10)
	fields[17] = "1"
	fields[19] = strconv.FormatUint(start, 10)
	fields[21] = strconv.FormatInt(rssPages, 10)
	stat := fmt.Sprintf("%d (%s) %s\n", pid, comm, strings.Join(fields, " "))
	if err := os.WriteFile(filepath.Join(directory, "stat"), []byte(stat), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "comm"), []byte(comm+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
}
