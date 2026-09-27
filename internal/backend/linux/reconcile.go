//go:build linux

package linux

import (
	"os"
	"path/filepath"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

func (*Backend) Reconcile(owner model.Ownership) (backend.ReconcileResult, error) {
	uncertain := func(reason string) backend.ReconcileResult {
		return backend.ReconcileResult{State: model.Uncertain, Resources: unavailableResources(), Reason: reason}
	}
	if owner.Backend != "linux" {
		return uncertain("ownership backend is not Linux"), nil
	}
	token, err := parseLinuxOwnershipToken(owner.Token)
	if err != nil {
		return uncertain("Linux ownership token is invalid"), nil
	}
	bootID, sessionID, cgroupName := token.BootID, token.SessionID, token.CgroupName
	currentBootID, err := readBootID()
	if err != nil {
		return uncertain("current Linux boot identity is unavailable"), nil
	}
	if currentBootID != bootID {
		return uncertain("Linux boot identity changed after ownership was recorded"), nil
	}
	if sessionID != owner.ProcessGroup || owner.PID <= 0 || owner.StartTime == 0 {
		return uncertain("persisted Linux process identity is incomplete"), nil
	}

	if owner.CgroupPath != "" {
		if cgroupName == "-" || filepath.Base(filepath.Clean(owner.CgroupPath)) != cgroupName {
			return uncertain("persisted cgroup identity does not match the ownership token"), nil
		}
		expectedPath, err := expectedCgroupPath(owner.CgroupPath)
		if err != nil {
			return uncertain("persisted cgroup path cannot be mapped to the active cgroup v2 mount"), nil
		}
		if _, err := os.Stat(owner.CgroupPath); err != nil {
			return uncertain("owned cgroup is missing; physical execution cannot be proven"), nil
		}
		location, err := discoverCgroup()
		if err != nil {
			return uncertain("cgroup v2 mount is unavailable during reconciliation"), nil
		}
		location.childPath = filepath.Clean(owner.CgroupPath)
		location.childRel = expectedPath
		cg := &cgroup{location: location, name: cgroupName}
		resources := (&Process{ownership: owner, cg: cg}).observeCgroup()
		empty, err := cgroupEmpty(cg)
		if err != nil {
			return uncertain("owned cgroup membership cannot be observed"), nil
		}
		if empty {
			return backend.ReconcileResult{
				State:           model.Terminal,
				Resources:       resources,
				OwnershipProven: true,
				Reason:          "owned cgroup is empty; terminal outcome must come from persisted lifecycle evidence",
			}, nil
		}
		return backend.ReconcileResult{State: model.Running, Resources: resources, OwnershipProven: true}, nil
	}

	if cgroupName != "-" {
		return uncertain("ownership token names a cgroup but no cgroup path was persisted"), nil
	}
	if token.Version != 2 || token.Subreaper.PID <= 0 || token.Subreaper.StartTime == 0 {
		return uncertain("no-cgroup ownership lacks a validated Linux child-subreaper identity"), nil
	}
	processes, err := scanSubreaperTree(token.Subreaper)
	if err != nil {
		return uncertain("Linux child-subreaper descendants cannot be revalidated"), nil
	}
	resourceTotals := totals(processes)
	resources := model.Resources{
		MemoryBytes:      measuredMetric(resourceTotals.Memory),
		PeakMemoryBytes:  model.Metric{Status: "unavailable"},
		CPUTimeNs:        measuredMetric(resourceTotals.CPUTimeNS),
		ProcessCount:     measuredMetric(resourceTotals.Processes),
		PeakProcessCount: model.Metric{Status: "unavailable"},
	}
	if len(activeProcesses(processes)) == 0 {
		resources.CPUTimeNs = model.Metric{Status: "unavailable"}
		return backend.ReconcileResult{State: model.Terminal, Resources: resources, OwnershipProven: true, Reason: "validated Linux child-subreaper has no live Run descendants"}, nil
	}
	return backend.ReconcileResult{State: model.Running, Resources: resources, OwnershipProven: true}, nil
}
