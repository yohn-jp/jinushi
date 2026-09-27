//go:build linux

package linux

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"golang.org/x/sys/unix"
)

// PhysicalControlCapabilities describes controls supported by a particular
// delegated cgroup. A host probe is advisory; a Run's own cgroup is checked
// again before every operation.
type PhysicalControlCapabilities struct {
	CgroupFreeze bool
	MemoryHigh   bool
	CPUQuota     bool
}

// ControlCapabilities reports optional controls for the supplied live Run.
func (p *Process) ControlCapabilities() PhysicalControlCapabilities {
	if p == nil || p.cg == nil {
		return PhysicalControlCapabilities{}
	}
	return physicalControlCapabilities(p.cg)
}

// SupportsCgroupFreeze exposes the per-Run capability without leaking Linux
// backend types through Guardian's optional method interface.
func (p *Process) SupportsCgroupFreeze() bool { return p.ControlCapabilities().CgroupFreeze }

// SupportsMemoryHighControl exposes whether this Run's cgroup has a writable
// memory.high control file.
func (p *Process) SupportsMemoryHighControl() bool { return p.ControlCapabilities().MemoryHigh }

// SupportsCPUQuotaControl exposes whether this Run's cgroup has a writable
// cpu.max control file.
func (p *Process) SupportsCPUQuotaControl() bool { return p.ControlCapabilities().CPUQuota }

// ControlCapabilities probes a temporary cgroup using the same delegation
// path as ordinary Linux Runs. It never reports controls from kernel version
// assumptions alone.
func (*Backend) ControlCapabilities() PhysicalControlCapabilities {
	location, ok := probeCgroup()
	if !ok {
		return PhysicalControlCapabilities{}
	}
	name, err := randomCgroupName()
	if err != nil {
		return PhysicalControlCapabilities{}
	}
	cg, err := newCgroup(location, name, 0, 0, 0)
	if err != nil {
		return PhysicalControlCapabilities{}
	}
	defer cg.close()
	return physicalControlCapabilities(cg)
}

func physicalControlCapabilities(cg *cgroup) PhysicalControlCapabilities {
	if cg == nil || cg.file == nil {
		return PhysicalControlCapabilities{}
	}
	return PhysicalControlCapabilities{
		CgroupFreeze: fileWritableAt(cg.file, "cgroup.freeze") && hasCgroupEvent(cg, "frozen"),
		MemoryHigh:   fileWritableAt(cg.file, "memory.high") && readableCgroupLimit(cg, "memory.high") && readableCgroupLimit(cg, "memory.max"),
		CPUQuota:     fileWritableAt(cg.file, "cpu.max") && readableCPUQuota(cg),
	}
}

func hasCgroupEvent(cg *cgroup, key string) bool {
	_, err := cgroupEventValue(cg, key)
	return err == nil
}

func readableCgroupLimit(cg *cgroup, name string) bool {
	_, _, err := readFiniteCgroupLimit(cg, name)
	return err == nil
}

func readableCPUQuota(cg *cgroup) bool {
	_, _, err := readCPUQuota(cg)
	return err == nil
}

// Pause freezes all tasks in this Run's delegated cgroup and waits for the
// kernel's cgroup.events frozen state to confirm the transition.
func (p *Process) Pause() error {
	return p.setFrozen(true)
}

// Resume thaws all tasks in this Run's delegated cgroup and waits for the
// kernel's cgroup.events frozen state to clear.
func (p *Process) Resume() error {
	return p.setFrozen(false)
}

func (p *Process) setFrozen(frozen bool) error {
	if p == nil {
		return errors.New("Linux control: process is unavailable")
	}
	p.resultMu.Lock()
	defer p.resultMu.Unlock()
	cg, err := p.revalidateLiveControlCgroup()
	if err != nil {
		return err
	}
	if !fileWritableAt(cg.file, "cgroup.freeze") {
		return fmt.Errorf("%w: cgroup freeze/thaw requires a writable cgroup.freeze", ErrUnsupported)
	}
	want := 0
	if frozen {
		want = 1
	}
	if err := writeFileAt(cg.file, "cgroup.freeze", strconv.Itoa(want)); err != nil {
		return fmt.Errorf("write cgroup.freeze: %w", err)
	}
	if err := waitFrozenState(cg, want, 5*time.Second); err != nil {
		return err
	}
	if _, err := p.revalidateLiveControlCgroup(); err != nil {
		return fmt.Errorf("revalidate cgroup after freeze transition: %w", err)
	}
	return nil
}

// CurrentPauseState reports the kernel-observed state. The second result is
// false when the cgroup does not expose a valid frozen field.
func (p *Process) CurrentPauseState() (paused bool, known bool, err error) {
	cg, err := p.revalidateLiveControlCgroup()
	if err != nil {
		return false, false, err
	}
	frozen, err := cgroupEventValue(cg, "frozen")
	if err != nil {
		return false, false, err
	}
	return frozen == 1, true, nil
}

// SetMemoryHigh changes this Run's soft memory threshold. The value must be a
// positive finite byte count and cannot exceed the immutable memory.max limit.
func (p *Process) SetMemoryHigh(bytes int64) error {
	if bytes <= 0 {
		return errors.New("Linux control: memory.high must be a positive finite byte count")
	}
	p.resultMu.Lock()
	defer p.resultMu.Unlock()
	cg, err := p.revalidateLiveControlCgroup()
	if err != nil {
		return err
	}
	if !fileWritableAt(cg.file, "memory.high") {
		return fmt.Errorf("%w: memory.high requires the delegated memory controller", ErrUnsupported)
	}
	maximum, unlimited, err := readFiniteCgroupLimit(cg, "memory.max")
	if err != nil {
		return fmt.Errorf("read memory.max: %w", err)
	}
	if !unlimited && bytes > maximum {
		return errors.New("Linux control: memory.high exceeds this Run's memory.max")
	}
	if err := writeFileAt(cg.file, "memory.high", strconv.FormatInt(bytes, 10)); err != nil {
		return fmt.Errorf("write memory.high: %w", err)
	}
	current, unlimited, err := readFiniteCgroupLimit(cg, "memory.high")
	if err != nil || unlimited || current != bytes {
		if err == nil {
			err = errors.New("kernel did not retain requested memory.high value")
		}
		return fmt.Errorf("verify memory.high: %w", err)
	}
	if _, err := p.revalidateLiveControlCgroup(); err != nil {
		return fmt.Errorf("revalidate cgroup after memory.high update: %w", err)
	}
	return nil
}

// CurrentMemoryHigh reports the effective threshold. An unlimited threshold
// is returned as value 0 with unlimited=true.
func (p *Process) CurrentMemoryHigh() (value int64, unlimited bool, err error) {
	cg, err := p.revalidateLiveControlCgroup()
	if err != nil {
		return 0, false, err
	}
	return readFiniteCgroupLimit(cg, "memory.high")
}

// SetCPUQuotaPercent changes this Run's finite cgroup CPU rate cap. Zero is
// rejected so this operation cannot remove a physical CPU ceiling.
func (p *Process) SetCPUQuotaPercent(percent int64) error {
	if percent <= 0 || percent > math.MaxInt64/1000 {
		return errors.New("Linux control: cpu quota percent is outside the finite Linux cgroup range")
	}
	quota, err := cpuQuotaForPercent(percent)
	if err != nil {
		return err
	}
	p.resultMu.Lock()
	defer p.resultMu.Unlock()
	cg, err := p.revalidateLiveControlCgroup()
	if err != nil {
		return err
	}
	if !fileWritableAt(cg.file, "cpu.max") {
		return fmt.Errorf("%w: cpu.max requires the delegated CPU controller", ErrUnsupported)
	}
	const period = int64(100000)
	if err := writeFileAt(cg.file, "cpu.max", fmt.Sprintf("%d %d", quota, period)); err != nil {
		return fmt.Errorf("write cpu.max: %w", err)
	}
	current, unlimited, err := readCPUQuota(cg)
	if err != nil || unlimited || current != percent {
		if err == nil {
			err = fmt.Errorf("kernel retained CPU quota equivalent to %d percent", current)
		}
		return fmt.Errorf("verify cpu.max: %w", err)
	}
	if _, err := p.revalidateLiveControlCgroup(); err != nil {
		return fmt.Errorf("revalidate cgroup after cpu.max update: %w", err)
	}
	return nil
}

// CurrentCPUQuotaPercent reports the effective quota as an integer percent of
// one CPU. Unlimited is represented by value 0 and unlimited=true.
func (p *Process) CurrentCPUQuotaPercent() (value int64, unlimited bool, err error) {
	cg, err := p.revalidateLiveControlCgroup()
	if err != nil {
		return 0, false, err
	}
	return readCPUQuota(cg)
}

func cpuQuotaForPercent(percent int64) (int64, error) {
	if percent <= 0 || percent > math.MaxInt64/1000 {
		return 0, errors.New("Linux control: cpu quota percent is outside the finite Linux cgroup range")
	}
	const period = int64(100000)
	quota := percent * period / 100
	if quota < 1000 {
		quota = 1000
	}
	return percent * 1000, nil
}

func readCPUQuota(cg *cgroup) (int64, bool, error) {
	data, err := readFileAt(cg.file, "cpu.max")
	if err != nil {
		return 0, false, err
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return 0, false, errors.New("cpu.max has invalid format")
	}
	period, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || period <= 0 {
		return 0, false, errors.New("cpu.max period is invalid")
	}
	if fields[0] == "max" {
		return 0, true, nil
	}
	quota, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || quota <= 0 {
		return 0, false, errors.New("cpu.max quota is invalid")
	}
	whole, remainder := quota/period, quota%period
	if whole > math.MaxInt64/100 {
		return 0, false, errors.New("cpu.max quota exceeds integer percent range")
	}
	base := whole * 100
	hi, lo := bits.Mul64(uint64(remainder), 100)
	percentRemainder, _ := bits.Div64(hi, lo, uint64(period))
	if percentRemainder > uint64(math.MaxInt64-base) {
		return 0, false, errors.New("cpu.max quota exceeds integer percent range")
	}
	return base + int64(percentRemainder), false, nil
}

func readFiniteCgroupLimit(cg *cgroup, name string) (int64, bool, error) {
	data, err := readFileAt(cg.file, name)
	if err != nil {
		return 0, false, err
	}
	value := strings.TrimSpace(string(data))
	if value == "max" {
		return 0, true, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, false, fmt.Errorf("%s has invalid finite value", name)
	}
	return parsed, false, nil
}

func waitFrozenState(cg *cgroup, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, err := cgroupEventValue(cg, "frozen")
		if err != nil {
			return fmt.Errorf("observe cgroup frozen state: %w", err)
		}
		if value == want {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("cgroup freeze transition was not confirmed before deadline")
		}
		<-ticker.C
	}
}

func cgroupEventValue(cg *cgroup, key string) (int, error) {
	data, err := readFileAt(cg.file, "cgroup.events")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == key {
			value, err := strconv.Atoi(fields[1])
			if err != nil || value < 0 || value > 1 {
				return 0, fmt.Errorf("cgroup.events %s value is invalid", key)
			}
			return value, nil
		}
	}
	return 0, fmt.Errorf("cgroup.events lacks %s state", key)
}

func (p *Process) revalidateLiveControlCgroup() (*cgroup, error) {
	if p == nil || p.cg == nil {
		return nil, fmt.Errorf("%w: this Run has no delegated cgroup", ErrUnsupported)
	}
	result, err := (&Backend{}).Reconcile(p.ownership)
	if err != nil {
		return nil, fmt.Errorf("revalidate Linux control ownership: %w", err)
	}
	if !result.OwnershipProven || result.State != model.Running {
		return nil, errors.New("Linux control: Run is not a proven live cgroup execution")
	}
	if err := validateCgroupOwnership(p.ownership, p.cg); err != nil {
		return nil, fmt.Errorf("revalidate Linux cgroup identity: %w", err)
	}
	current, err := openDirectoryNoSymlinks(p.ownership.CgroupPath)
	if err != nil {
		return nil, fmt.Errorf("reopen Linux cgroup path: %w", err)
	}
	defer current.Close()
	if err := verifyCgroupDirectory(current); err != nil {
		return nil, err
	}
	var identity unix.Stat_t
	if err := unix.Fstat(int(current.Fd()), &identity); err != nil || identity.Ino != p.cg.identity {
		if err == nil {
			err = errors.New("Linux cgroup path was replaced")
		}
		return nil, err
	}
	if filepath.Clean(p.ownership.CgroupPath) != filepath.Clean(p.cg.location.childPath) {
		return nil, errors.New("Linux cgroup path no longer matches the Run's pinned control handle")
	}
	members, err := scanCgroup(p.cg)
	if err != nil {
		return nil, fmt.Errorf("revalidate Linux cgroup membership: %w", err)
	}
	active := activeProcesses(members)
	if len(active) == 0 {
		return nil, errors.New("Linux control: owned cgroup has no live process members")
	}
	for _, member := range active {
		inCgroup, err := processInCgroup(member.PID, p.cg)
		if err != nil || !inCgroup {
			if err == nil {
				err = errors.New("process left the owned cgroup")
			}
			return nil, fmt.Errorf("revalidate Linux cgroup process membership: %w", err)
		}
	}
	return p.cg, nil
}
