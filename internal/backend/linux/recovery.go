//go:build linux

package linux

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
	"golang.org/x/sys/unix"
)

var (
	// ErrGuardianRecoveryUnproven means the persisted ownership evidence did
	// not authorize any physical control operation. Callers must preserve an
	// explicit uncertain Run state and must not fall back to a PID signal.
	ErrGuardianRecoveryUnproven = errors.New("Linux Guardian-loss ownership is unproven")
	ErrGuardianRecoveryFailed   = errors.New("Linux Guardian-loss termination is unproven")
)

// RecoverGuardianLoss independently reopens and verifies a Run's persisted
// cgroup ownership, then terminates that complete cgroup if it remains live.
// The process-subreaper fallback cannot be safely controlled after its
// Guardian exits, so this method deliberately refuses to target its PIDs.
func (*Backend) RecoverGuardianLoss(owner model.Ownership, grace time.Duration) (backend.TerminationResult, error) {
	if owner.CgroupPath == "" {
		return recoverSubreaperOwnedTree(owner, grace)
	}
	cg, err := openVerifiedOwnedCgroup(owner)
	if err != nil {
		return backend.TerminationResult{}, fmt.Errorf("%w: %v", ErrGuardianRecoveryUnproven, err)
	}
	defer cg.file.Close()

	if empty, err := verifiedCgroupEmpty(owner, cg); err != nil {
		return backend.TerminationResult{}, fmt.Errorf("%w: initial membership observation failed: %v", ErrGuardianRecoveryUnproven, err)
	} else if empty {
		return backend.TerminationResult{TreeEmpty: true}, nil
	}

	result := backend.TerminationResult{Requested: true, Outcome: "guardian-loss-recovery"}
	termErr := revalidateAndSignalCgroup(owner, cg, syscall.SIGTERM)
	if grace < 0 {
		grace = 0
	}
	if grace > 30*time.Second {
		grace = 30 * time.Second
	}
	if waitErr := waitOwnedCgroupEmpty(owner, cg, grace); waitErr != nil {
		// Before escalation, independently re-open the persisted path and
		// re-prove the same inode and all current process memberships.
		if err := revalidateOwnedCgroup(owner, cg); err != nil {
			return result, fmt.Errorf("%w: escalation ownership check failed: %v", ErrGuardianRecoveryUnproven, errors.Join(waitErr, err, termErr))
		}
		result.Forced = true
		killErr := killCgroup(cg)
		if empty, proofErr := waitAndVerifyCgroupEmpty(owner, cg, 5*time.Second); proofErr == nil && empty {
			result.TreeEmpty = true
			return result, nil
		} else {
			return result, fmt.Errorf("%w: owned cgroup did not reach proven empty state: %v", ErrGuardianRecoveryFailed, errors.Join(waitErr, termErr, killErr, proofErr))
		}
	}
	result.TreeEmpty = true
	return result, nil
}

// recoverSubreaperOwnedTree can recover a fallback Run only while its
// per-Run child subreaper remains alive and independently verifiable. Once
// the Guardian/subreaper is gone, scanOwnedTree fails and this path sends no
// signal to any PID.
func recoverSubreaperOwnedTree(owner model.Ownership, grace time.Duration) (backend.TerminationResult, error) {
	if owner.Backend != "linux" || owner.PID <= 0 || owner.StartTime == 0 || owner.ProcessGroup <= 0 {
		return backend.TerminationResult{}, fmt.Errorf("%w: fallback process ownership is incomplete", ErrGuardianRecoveryUnproven)
	}
	if _, err := scanOwnedTree(owner); err != nil {
		return backend.TerminationResult{}, fmt.Errorf("%w: child-subreaper ownership cannot be independently proven: %v", ErrGuardianRecoveryUnproven, err)
	}
	if empty, err := subreaperTreeEmpty(owner); err != nil {
		return backend.TerminationResult{}, fmt.Errorf("%w: initial child-subreaper observation failed: %v", ErrGuardianRecoveryUnproven, err)
	} else if empty {
		return backend.TerminationResult{TreeEmpty: true}, nil
	}
	result := backend.TerminationResult{Requested: true, Outcome: "guardian-loss-recovery"}
	termErr := signalOwnedTree(owner, syscall.SIGTERM)
	if grace < 0 {
		grace = 0
	}
	if grace > 30*time.Second {
		grace = 30 * time.Second
	}
	if waitErr := waitSubreaperTreeEmpty(owner, grace); waitErr == nil {
		result.TreeEmpty = true
		return result, nil
	}
	// scanOwnedTree is repeated by signalOwnedTree immediately before PIDFD
	// signaling; it refuses to signal after the subreaper exits or its identity
	// changes. Never fall back to the persisted root PID or process group.
	result.Forced = true
	killErr := signalOwnedTree(owner, syscall.SIGKILL)
	if waitErr := waitSubreaperTreeEmpty(owner, 5*time.Second); waitErr == nil {
		result.TreeEmpty = true
		return result, nil
	} else {
		return result, fmt.Errorf("%w: child-subreaper tree did not reach a proven empty state: %v", ErrGuardianRecoveryFailed, errors.Join(termErr, killErr, waitErr))
	}
}

func subreaperTreeEmpty(owner model.Ownership) (bool, error) {
	processes, err := scanOwnedTree(owner)
	if err != nil {
		return false, err
	}
	return len(activeProcesses(processes)) == 0, nil
}

func waitSubreaperTreeEmpty(owner model.Ownership, duration time.Duration) error {
	deadline := time.Now().Add(duration)
	for {
		empty, err := subreaperTreeEmpty(owner)
		if err != nil {
			return err
		}
		if empty {
			return nil
		}
		if duration <= 0 || !time.Now().Before(deadline) {
			return errors.New("proven Linux child-subreaper tree remains live")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func openVerifiedOwnedCgroup(owner model.Ownership) (*cgroup, error) {
	if owner.Backend != "linux" || owner.PID <= 0 || owner.StartTime == 0 || owner.ProcessGroup <= 0 || owner.CgroupPath == "" {
		return nil, errors.New("persisted Linux cgroup ownership is incomplete")
	}
	token, err := parseLinuxOwnershipToken(owner.Token)
	if err != nil {
		return nil, fmt.Errorf("ownership token is invalid: %w", err)
	}
	if token.Version != 3 || token.CgroupName == "-" || token.CgroupID == 0 || token.SessionID != owner.ProcessGroup {
		return nil, errors.New("ownership token lacks current cgroup v2 identity")
	}
	bootID, err := readBootID()
	if err != nil || bootID != token.BootID {
		if err == nil {
			err = errors.New("Linux boot identity changed")
		}
		return nil, err
	}
	if filepath.Base(filepath.Clean(owner.CgroupPath)) != token.CgroupName {
		return nil, errors.New("persisted cgroup path does not match its ownership token")
	}
	childRel, err := expectedCgroupPath(owner.CgroupPath)
	if err != nil {
		return nil, fmt.Errorf("persisted cgroup path is not under the active cgroup v2 mount: %w", err)
	}
	file, err := openDirectoryNoSymlinks(owner.CgroupPath)
	if err != nil {
		return nil, fmt.Errorf("open persisted cgroup without symlinks: %w", err)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = file.Close()
		}
	}()
	if err := verifyCgroupDirectory(file); err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return nil, err
	}
	if stat.Ino != token.CgroupID {
		return nil, errors.New("persisted cgroup directory was replaced")
	}
	location, err := discoverCgroup()
	if err != nil {
		return nil, fmt.Errorf("active cgroup v2 mount is unavailable: %w", err)
	}
	location.childPath = owner.CgroupPath
	location.childRel = childRel
	cg := &cgroup{location: location, name: token.CgroupName, identity: stat.Ino, file: file}
	if err := validateCgroupOwnership(owner, cg); err != nil {
		return nil, err
	}
	if err := revalidateOwnedCgroup(owner, cg); err != nil {
		return nil, err
	}
	closeOnError = false
	return cg, nil
}

func revalidateOwnedCgroup(owner model.Ownership, cg *cgroup) error {
	if cg == nil || cg.file == nil {
		return errors.New("pinned cgroup handle is unavailable")
	}
	if err := validateCgroupOwnership(owner, cg); err != nil {
		return err
	}
	current, err := openDirectoryNoSymlinks(owner.CgroupPath)
	if err != nil {
		return fmt.Errorf("reopen persisted cgroup path: %w", err)
	}
	defer current.Close()
	if err := verifyCgroupDirectory(current); err != nil {
		return err
	}
	var currentStat unix.Stat_t
	if err := unix.Fstat(int(current.Fd()), &currentStat); err != nil {
		return err
	}
	var pinnedStat unix.Stat_t
	if err := unix.Fstat(int(cg.file.Fd()), &pinnedStat); err != nil {
		return err
	}
	if currentStat.Ino != cg.identity || pinnedStat.Ino != cg.identity || filepath.Clean(owner.CgroupPath) != filepath.Clean(cg.location.childPath) {
		return errors.New("persisted cgroup path no longer names the pinned ownership inode")
	}
	members, err := scanCgroup(cg)
	if err != nil {
		return fmt.Errorf("scan owned cgroup members: %w", err)
	}
	for _, member := range activeProcesses(members) {
		inCgroup, err := processInCgroup(member.PID, cg)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			return fmt.Errorf("revalidate PID %d cgroup membership: %w", member.PID, err)
		}
		if !inCgroup {
			return fmt.Errorf("PID %d is no longer in the owned cgroup", member.PID)
		}
	}
	return nil
}

func verifiedCgroupEmpty(owner model.Ownership, cg *cgroup) (bool, error) {
	if err := revalidateOwnedCgroup(owner, cg); err != nil {
		return false, err
	}
	return cgroupEmpty(cg)
}

func revalidateAndSignalCgroup(owner model.Ownership, cg *cgroup, signal syscall.Signal) error {
	if err := revalidateOwnedCgroup(owner, cg); err != nil {
		return err
	}
	return signalCgroup(cg, signal)
}

func waitOwnedCgroupEmpty(owner model.Ownership, cg *cgroup, duration time.Duration) error {
	if duration <= 0 {
		empty, err := verifiedCgroupEmpty(owner, cg)
		if err != nil {
			return err
		}
		if empty {
			return nil
		}
		return errors.New("owned cgroup is still populated")
	}
	deadline := time.Now().Add(duration)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		empty, err := verifiedCgroupEmpty(owner, cg)
		if err != nil {
			return err
		}
		if empty {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errors.New("owned cgroup remained populated through the termination grace period")
		}
		<-ticker.C
	}
}

func waitAndVerifyCgroupEmpty(owner model.Ownership, cg *cgroup, timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		empty, err := verifiedCgroupEmpty(owner, cg)
		if err != nil {
			return false, err
		}
		if empty {
			return true, nil
		}
		if !time.Now().Before(deadline) {
			return false, errors.New("owned cgroup remained populated after forced termination")
		}
		<-ticker.C
	}
}
