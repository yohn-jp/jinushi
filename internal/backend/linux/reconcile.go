//go:build linux

package linux

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
	"golang.org/x/sys/unix"
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
		legacy := token.Version == 1 && token.CgroupName != "-"
		if (!legacy && (token.Version != 3 || token.CgroupID == 0)) || token.CgroupName == "-" || filepath.Base(filepath.Clean(owner.CgroupPath)) != token.CgroupName {
			return uncertain("persisted cgroup identity does not match the ownership token"), nil
		}
		expectedPath, err := expectedCgroupPath(owner.CgroupPath)
		if err != nil {
			return uncertain("persisted cgroup path cannot be mapped to the active cgroup v2 mount"), nil
		}
		cgroupDir, err := openDirectoryNoSymlinks(owner.CgroupPath)
		if err != nil {
			return uncertain("owned cgroup is missing; physical execution cannot be proven"), nil
		}
		if err := verifyCgroupDirectory(cgroupDir); err != nil {
			_ = cgroupDir.Close()
			return uncertain("owned cgroup path is not a cgroup v2 directory"), nil
		}
		var identity unix.Stat_t
		if err := unix.Fstat(int(cgroupDir.Fd()), &identity); err != nil || !legacy && identity.Ino != token.CgroupID {
			_ = cgroupDir.Close()
			return uncertain("owned cgroup path was replaced after ownership was recorded"), nil
		}
		defer cgroupDir.Close()
		location, err := discoverCgroup()
		if err != nil {
			return uncertain("cgroup v2 mount is unavailable during reconciliation"), nil
		}
		location.childPath = owner.CgroupPath
		location.childRel = expectedPath
		cg := &cgroup{location: location, name: token.CgroupName, identity: identity.Ino, file: cgroupDir}
		process := &Process{ownership: owner, cg: cg}
		members, err := scanCgroup(cg)
		if err != nil {
			return uncertain("owned cgroup membership cannot be observed"), nil
		}
		active := activeProcesses(members)
		for _, member := range active {
			inCgroup, memberErr := processInCgroup(member.PID, cg)
			if errors.Is(memberErr, os.ErrNotExist) || errors.Is(memberErr, syscall.ESRCH) {
				continue
			}
			if memberErr != nil || !inCgroup {
				return uncertain("owned cgroup process membership cannot be revalidated"), nil
			}
		}
		root, rootErr := readProcInfo(owner.PID)
		rootProven := rootErr == nil && root.StartTime == owner.StartTime && root.Session == owner.ProcessGroup
		if rootProven {
			rootProven, err = processInCgroup(owner.PID, cg)
			if err != nil {
				rootProven = false
			}
		}
		if legacy && len(active) == 0 && !rootProven {
			return uncertain("legacy cgroup ownership has no live PID or membership evidence to distinguish a replaced empty path"), nil
		}
		resources := process.observeCgroup()
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
		TaskCount:        measuredMetric(resourceTotals.Tasks),
		PeakTaskCount:    model.Metric{Status: "unavailable"},
	}
	if len(activeProcesses(processes)) == 0 {
		resources.CPUTimeNs = model.Metric{Status: "unavailable"}
		return backend.ReconcileResult{State: model.Terminal, Resources: resources, OwnershipProven: true, Reason: "validated Linux child-subreaper has no live Run descendants"}, nil
	}
	return backend.ReconcileResult{State: model.Running, Resources: resources, OwnershipProven: true}, nil
}
