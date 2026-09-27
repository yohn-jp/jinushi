//go:build linux

package linux

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type procInfo struct {
	PID          int
	PPID         int
	ProcessGroup int
	Session      int
	State        byte
	Threads      int64
	UserTicks    uint64
	SysTicks     uint64
	StartTime    uint64
	RSSBytes     int64
}

type procTotals struct {
	Processes int64
	Tasks     int64
	Memory    int64
	CPUTimeNS int64
}

type subreaperIdentity struct {
	PID       int
	StartTime uint64
}

type linuxOwnershipToken struct {
	Version    int
	BootID     string
	SessionID  int
	CgroupName string
	CgroupID   uint64
	Subreaper  subreaperIdentity
}

func readProcInfo(pid int) (procInfo, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return procInfo{}, err
	}
	return parseProcStat(data)
}

func parseProcStat(data []byte) (procInfo, error) {
	line := strings.TrimSpace(string(data))
	open := strings.IndexByte(line, '(')
	close := strings.LastIndexByte(line, ')')
	if open <= 0 || close <= open || close+2 > len(line) {
		return procInfo{}, errors.New("malformed /proc stat record")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line[:open]))
	if err != nil {
		return procInfo{}, err
	}
	fields := strings.Fields(line[close+1:])
	if len(fields) < 22 {
		return procInfo{}, errors.New("short /proc stat record")
	}
	intField := func(index int) (int, error) { return strconv.Atoi(fields[index]) }
	uintField := func(index int) (uint64, error) { return strconv.ParseUint(fields[index], 10, 64) }
	ppid, err := intField(1)
	if err != nil {
		return procInfo{}, err
	}
	processGroup, err := intField(2)
	if err != nil {
		return procInfo{}, err
	}
	session, err := intField(3)
	if err != nil {
		return procInfo{}, err
	}
	userTicks, err := uintField(11)
	if err != nil {
		return procInfo{}, err
	}
	sysTicks, err := uintField(12)
	if err != nil {
		return procInfo{}, err
	}
	startTime, err := uintField(19)
	if err != nil {
		return procInfo{}, err
	}
	rssPages, err := strconv.ParseInt(fields[21], 10, 64)
	if err != nil {
		return procInfo{}, err
	}
	threads, err := strconv.ParseInt(fields[17], 10, 64)
	if err != nil || threads < 0 {
		return procInfo{}, errors.New("invalid Linux process task count")
	}
	return procInfo{
		PID:          pid,
		PPID:         ppid,
		ProcessGroup: processGroup,
		Session:      session,
		State:        fields[0][0],
		Threads:      threads,
		UserTicks:    userTicks,
		SysTicks:     sysTicks,
		StartTime:    startTime,
		RSSBytes:     rssPages * int64(os.Getpagesize()),
	}, nil
}

func scanProcesses() ([]procInfo, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	processes := make([]procInfo, 0)
	var incomplete error
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		info, err := readProcInfo(pid)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			incomplete = fmt.Errorf("cannot inspect process %d while enumerating Linux processes: %w", pid, err)
			continue
		}
		processes = append(processes, info)
	}
	if incomplete != nil {
		return nil, incomplete
	}
	return processes, nil
}

func enableSubreaper() (subreaperIdentity, error) {
	if err := setChildSubreaper(true); err != nil {
		return subreaperIdentity{}, fmt.Errorf("enable Linux child subreaper: %w", err)
	}
	enabled, err := childSubreaperEnabled()
	if err != nil {
		return subreaperIdentity{}, fmt.Errorf("verify Linux child subreaper: %w", err)
	}
	if !enabled {
		return subreaperIdentity{}, errors.New("Linux child subreaper flag did not become active")
	}
	info, err := readProcInfo(os.Getpid())
	if err != nil {
		return subreaperIdentity{}, fmt.Errorf("identify Linux child subreaper: %w", err)
	}
	return subreaperIdentity{PID: info.PID, StartTime: info.StartTime}, nil
}

func childSubreaperEnabled() (bool, error) {
	var enabled int32
	err := unix.Prctl(unix.PR_GET_CHILD_SUBREAPER, uintptr(unsafe.Pointer(&enabled)), 0, 0, 0)
	runtime.KeepAlive(&enabled)
	if err != nil {
		return false, err
	}
	return enabled == 1, nil
}

func setChildSubreaper(enabled bool) error {
	value := uintptr(0)
	if enabled {
		value = 1
	}
	return unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, value, 0, 0, 0)
}

// scanSubreaperTree returns all descendants of the validated per-Run helper.
// Children that detach from their session are reparented into this tree when
// their intermediate parent exits because the helper is a child subreaper.
func scanSubreaperTree(reaper subreaperIdentity) ([]procInfo, error) {
	if reaper.PID <= 0 || reaper.StartTime == 0 {
		return nil, errors.New("Linux subreaper identity is incomplete")
	}
	processes, err := scanProcesses()
	if err != nil {
		return nil, err
	}
	byParent := make(map[int][]procInfo)
	var reaperFound bool
	for _, process := range processes {
		if process.PID == reaper.PID {
			if process.StartTime != reaper.StartTime {
				return nil, errors.New("Linux subreaper PID was reused")
			}
			reaperFound = true
		}
		byParent[process.PPID] = append(byParent[process.PPID], process)
	}
	if !reaperFound {
		return nil, errors.New("Linux subreaper is no longer observable")
	}

	seen := map[int]struct{}{reaper.PID: {}}
	queue := []int{reaper.PID}
	descendants := make([]procInfo, 0)
	for len(queue) != 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range byParent[parent] {
			if _, exists := seen[child.PID]; exists {
				continue
			}
			seen[child.PID] = struct{}{}
			descendants = append(descendants, child)
			queue = append(queue, child.PID)
		}
	}
	return descendants, nil
}

func isSubreaperDescendant(pid int, startTime uint64, reaper subreaperIdentity) bool {
	if pid <= 0 || startTime == 0 {
		return false
	}
	current, err := readProcInfo(pid)
	if err != nil || current.StartTime != startTime {
		return false
	}
	seen := map[int]struct{}{pid: {}}
	for range 4096 {
		parentPID := current.PPID
		if parentPID == reaper.PID {
			parent, err := readProcInfo(reaper.PID)
			return err == nil && parent.StartTime == reaper.StartTime
		}
		if parentPID <= 1 {
			return false
		}
		if _, exists := seen[parentPID]; exists {
			return false
		}
		seen[parentPID] = struct{}{}
		current, err = readProcInfo(parentPID)
		if err != nil {
			return false
		}
	}
	return false
}

func reapAdoptedZombies(processes []procInfo, reaper subreaperIdentity, rootPID int, rootStartTime uint64) error {
	var failures []error
	for _, process := range processes {
		if process.PPID != reaper.PID || process.State != 'Z' {
			continue
		}
		if process.PID == rootPID && (rootStartTime == 0 || process.StartTime == rootStartTime) {
			continue
		}
		var status unix.WaitStatus
		if _, err := unix.Wait4(process.PID, &status, unix.WNOHANG, nil); err != nil && !errors.Is(err, syscall.ECHILD) && !errors.Is(err, syscall.ESRCH) {
			failures = append(failures, fmt.Errorf("reap adopted Linux child %d: %w", process.PID, err))
		}
	}
	return errors.Join(failures...)
}

func activeProcesses(processes []procInfo) []procInfo {
	active := make([]procInfo, 0, len(processes))
	for _, process := range processes {
		if process.State != 'Z' && process.State != 'X' && process.State != 'x' {
			active = append(active, process)
		}
	}
	return active
}

func totals(processes []procInfo) procTotals {
	var out procTotals
	for _, process := range activeProcesses(processes) {
		out.Processes++
		out.Tasks += process.Threads
		out.Memory += process.RSSBytes
		// Linux exports process CPU time in USER_HZ ticks. USER_HZ is 100 on
		// supported Linux ABIs, independent of the kernel's scheduling HZ.
		out.CPUTimeNS += int64(process.UserTicks+process.SysTicks) * 10_000_000
	}
	return out
}

func readBootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func ownershipToken(bootID string, sessionID int, cgroupName string, cgroupID uint64) string {
	if cgroupName == "" {
		cgroupName = "-"
	}
	return fmt.Sprintf("linux-v3;%s;%d;%s;%d", bootID, sessionID, cgroupName, cgroupID)
}

func ownershipTokenWithSubreaper(bootID string, sessionID int, reaper subreaperIdentity) string {
	return fmt.Sprintf("linux-v2;%s;%d;-;%d;%d", bootID, sessionID, reaper.PID, reaper.StartTime)
}

func parseLinuxOwnershipToken(token string) (linuxOwnershipToken, error) {
	parts := strings.Split(token, ";")
	if len(parts) < 4 || parts[1] == "" {
		return linuxOwnershipToken{}, errors.New("invalid Linux ownership token")
	}
	sessionID, err := strconv.Atoi(parts[2])
	if err != nil || sessionID <= 0 {
		return linuxOwnershipToken{}, errors.New("invalid Linux session identity")
	}
	parsed := linuxOwnershipToken{Version: 1, BootID: parts[1], SessionID: sessionID, CgroupName: parts[3]}
	if parts[0] == "linux-v1" {
		if len(parts) != 4 {
			return linuxOwnershipToken{}, errors.New("invalid legacy Linux ownership token")
		}
		return parsed, nil
	}
	if parts[0] == "linux-v3" {
		if len(parts) != 5 || parts[3] == "-" {
			return linuxOwnershipToken{}, errors.New("invalid Linux cgroup ownership token")
		}
		cgroupID, parseErr := strconv.ParseUint(parts[4], 10, 64)
		if parseErr != nil || cgroupID == 0 {
			return linuxOwnershipToken{}, errors.New("invalid Linux cgroup identity")
		}
		parsed.Version = 3
		parsed.CgroupID = cgroupID
		return parsed, nil
	}
	if parts[0] != "linux-v2" || len(parts) != 6 || parts[3] != "-" {
		return linuxOwnershipToken{}, errors.New("invalid Linux subreaper ownership token")
	}
	reaperPID, pidErr := strconv.Atoi(parts[4])
	reaperStart, startErr := strconv.ParseUint(parts[5], 10, 64)
	if pidErr != nil || reaperPID <= 0 || startErr != nil || reaperStart == 0 {
		return linuxOwnershipToken{}, errors.New("invalid Linux subreaper identity")
	}
	parsed.Version = 2
	parsed.Subreaper = subreaperIdentity{PID: reaperPID, StartTime: reaperStart}
	return parsed, nil
}

func procCgroupPath(pid int) (string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::") {
			return filepath.Clean(strings.TrimPrefix(line, "0::")), nil
		}
	}
	return "", errors.New("process has no unified cgroup membership")
}

func expectedCgroupPath(ownerPath string) (string, error) {
	location, err := discoverCgroup()
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(ownerPath) || filepath.Clean(ownerPath) != ownerPath {
		return "", errors.New("owned cgroup path is not a clean absolute path")
	}
	if !pathWithin(location.mountPoint, ownerPath) || ownerPath == location.mountPoint {
		return "", errors.New("owned cgroup path is outside the active cgroup mount")
	}
	rel := strings.TrimPrefix(ownerPath, location.mountPoint)
	rel = strings.TrimPrefix(rel, string(filepath.Separator))
	return filepath.Clean(filepath.Join(location.mountRoot, rel)), nil
}

func processInCgroup(pid int, cg *cgroup) (bool, error) {
	if pid <= 0 || cg == nil || cg.file == nil {
		return false, errors.New("Linux cgroup membership evidence is incomplete")
	}
	procPath, err := procCgroupPath(pid)
	if err != nil {
		return false, err
	}
	root := filepath.Clean(cg.location.mountRoot)
	if !pathWithin(root, procPath) {
		return false, nil
	}
	rel, err := filepath.Rel(root, procPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false, nil
	}
	visiblePath := filepath.Join(cg.location.mountPoint, rel)
	member, err := openDirectoryNoSymlinks(visiblePath)
	if err != nil {
		return false, err
	}
	defer member.Close()
	if err := verifyCgroupDirectory(member); err != nil {
		return false, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(member.Fd()), &stat); err != nil {
		return false, err
	}
	return stat.Ino == cg.identity, nil
}

func pidfdSignal(pid int, signal syscall.Signal, expected procInfo, validate func() bool) error {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	current, err := readProcInfo(pid)
	if err != nil {
		return err
	}
	if current.StartTime != expected.StartTime || current.Session != expected.Session || !validate() {
		return errors.New("process identity changed while validating ownership")
	}
	return unix.PidfdSendSignal(fd, unix.Signal(signal), nil, 0)
}
