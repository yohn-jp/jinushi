//go:build linux

package linux

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func TestCPUQuotaForPercentHasFiniteBoundedEncoding(t *testing.T) {
	for _, test := range []struct {
		percent int64
		want    int64
	}{
		{percent: 1, want: 1000},
		{percent: 100, want: 100000},
		{percent: 250, want: 250000},
	} {
		got, err := cpuQuotaForPercent(test.percent)
		if err != nil || got != test.want {
			t.Fatalf("cpuQuotaForPercent(%d) = %d, %v; want %d", test.percent, got, err, test.want)
		}
	}
	for _, invalid := range []int64{0, -1, int64(^uint64(0)>>1)/1000 + 1} {
		if _, err := cpuQuotaForPercent(invalid); err == nil {
			t.Errorf("cpuQuotaForPercent(%d) accepted invalid quota", invalid)
		}
	}
}

func TestParseCPUQuotaAndFiniteLimits(t *testing.T) {
	root := t.TempDir()
	dir, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	cg := &cgroup{file: dir}
	if err := os.WriteFile(root+"/cpu.max", []byte("250000 100000\n"), 0600); err != nil {
		t.Fatal(err)
	}
	percent, unlimited, err := readCPUQuota(cg)
	if err != nil || unlimited || percent != 250 {
		t.Fatalf("readCPUQuota = %d, %t, %v", percent, unlimited, err)
	}
	if err := os.WriteFile(root+"/cpu.max", []byte("max 100000\n"), 0600); err != nil {
		t.Fatal(err)
	}
	percent, unlimited, err = readCPUQuota(cg)
	if err != nil || !unlimited || percent != 0 {
		t.Fatalf("unlimited readCPUQuota = %d, %t, %v", percent, unlimited, err)
	}
	for _, test := range []struct {
		value   string
		want    int64
		wantErr bool
	}{{value: "2", want: 2}, {value: "-2", wantErr: true}, {value: "broken", wantErr: true}} {
		if err := os.WriteFile(root+"/memory.high", []byte(test.value+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		got, isUnlimited, err := readFiniteCgroupLimit(cg, "memory.high")
		if (err != nil) != test.wantErr || !test.wantErr && (isUnlimited || got != test.want) {
			t.Errorf("readFiniteCgroupLimit(%q) = %d, %t, %v", test.value, got, isUnlimited, err)
		}
	}
	if err := os.WriteFile(root+"/memory.high", []byte("max\n"), 0600); err != nil {
		t.Fatal(err)
	}
	value, unlimited, err := readFiniteCgroupLimit(cg, "memory.high")
	if err != nil || value != 0 || !unlimited {
		t.Fatalf("unlimited memory.high = %d, %t, %v", value, unlimited, err)
	}
}

func TestPhysicalControlsAreCapabilityGatedByDelegatedCgroup(t *testing.T) {
	process, err := New().Start(testSpec(t, "/run/current-system/sw/bin/sleep", "30"), io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = process.Terminate(0) }()
	linuxProcess := process.(*Process)
	caps := linuxProcess.ControlCapabilities()
	t.Logf("delegated physical controls for this Run: cgroup.freeze=%t memory.high=%t cpu.max=%t", caps.CgroupFreeze, caps.MemoryHigh, caps.CPUQuota)
	if !caps.CgroupFreeze {
		if err := linuxProcess.Pause(); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("pause without delegated cgroup freeze = %v, want unsupported", err)
		}
	} else {
		if err := linuxProcess.Pause(); err != nil {
			t.Fatalf("pause: %v", err)
		}
		if paused, known, err := linuxProcess.CurrentPauseState(); err != nil || !known || !paused {
			t.Fatalf("frozen cgroup observation = %t, %t, %v", paused, known, err)
		}
		if err := linuxProcess.Resume(); err != nil {
			t.Fatalf("resume: %v", err)
		}
		if paused, known, err := linuxProcess.CurrentPauseState(); err != nil || !known || paused {
			t.Fatalf("thawed cgroup observation = %t, %t, %v", paused, known, err)
		}
	}
	if caps.MemoryHigh {
		if err := linuxProcess.SetMemoryHigh(64 << 20); err != nil {
			t.Fatalf("set memory.high: %v", err)
		}
		if value, unlimited, err := linuxProcess.CurrentMemoryHigh(); err != nil || unlimited || value != 64<<20 {
			t.Fatalf("memory.high observation = %d, %t, %v", value, unlimited, err)
		}
	} else if err := linuxProcess.SetMemoryHigh(64 << 20); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("memory.high without delegated controller = %v, want unsupported", err)
	}
	if caps.CPUQuota {
		if err := linuxProcess.SetCPUQuotaPercent(200); err != nil {
			t.Fatalf("set cpu quota: %v", err)
		}
		if value, unlimited, err := linuxProcess.CurrentCPUQuotaPercent(); err != nil || unlimited || value != 200 {
			t.Fatalf("cpu quota observation = %d, %t, %v", value, unlimited, err)
		}
	} else if err := linuxProcess.SetCPUQuotaPercent(200); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("CPU quota without delegated controller = %v, want unsupported", err)
	}
}

func TestRecoverGuardianLossTerminatesOnlyProvenOwnedTree(t *testing.T) {
	stdout := &lockedBuffer{}
	implementation := &Backend{}
	process, err := implementation.Start(testSpec(t, "/bin/sh", "-c", "sleep 30 & echo $!; wait"), stdout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	childPID := waitForChildPID(t, stdout)
	result, err := implementation.RecoverGuardianLoss(process.Ownership(), 100*time.Millisecond)
	if err != nil {
		t.Fatalf("recover owned execution: %v (result %+v)", err, result)
	}
	if !result.Requested || !result.TreeEmpty {
		t.Fatalf("recovery lacks termination/tree-empty evidence: %+v", result)
	}
	if processIsActive(childPID) {
		t.Fatalf("owned child %d remained live after recovery", childPID)
	}
	if _, err := process.Wait(); err != nil {
		t.Fatalf("original backend did not observe recovery termination: %v", err)
	}
}

func TestRecoverGuardianLossDoesNotTargetUnprovenOwnership(t *testing.T) {
	process, err := New().Start(testSpec(t, "/run/current-system/sw/bin/sleep", "30"), io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	owner := process.Ownership()
	owner.Token = "invalid"
	result, err := (&Backend{}).RecoverGuardianLoss(owner, 0)
	if !errors.Is(err, ErrGuardianRecoveryUnproven) {
		t.Fatalf("unproven recovery error = %v (result %+v)", err, result)
	}
	if result.TreeEmpty || !processIsActive(process.Ownership().PID) {
		t.Fatalf("unproven recovery acted on the live execution: %+v", result)
	}
	termination, terminateErr := process.Terminate(0)
	if terminateErr != nil || !termination.TreeEmpty {
		t.Fatalf("cleanup test Run: %+v, %v", termination, terminateErr)
	}
}

func TestControlInputRejectsNonFiniteValues(t *testing.T) {
	process, err := New().Start(testSpec(t, "/run/current-system/sw/bin/sleep", "30"), io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	linuxProcess := process.(*Process)
	if err := linuxProcess.SetMemoryHigh(0); err == nil {
		t.Error("memory.high accepted an unbounded value")
	}
	if err := linuxProcess.SetCPUQuotaPercent(0); err == nil {
		t.Error("cpu.max accepted an unbounded value")
	}
	result, err := linuxProcess.Terminate(0)
	if err != nil || !result.TreeEmpty {
		t.Fatalf("cleanup test Run: %+v, %v", result, err)
	}
}
