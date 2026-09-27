package linux

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

type procInfo struct {
	PID          int
	PPID         int
	ProcessGroup int
	Session      int
	State        byte
	UserTicks    uint64
	SysTicks     uint64
	StartTime    uint64
	RSSBytes     int64
}

type procTotals struct {
	Processes int64
	Memory    int64
	CPUTimeNS int64
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
	return procInfo{
		PID:          pid,
		PPID:         ppid,
		ProcessGroup: processGroup,
		Session:      session,
		State:        fields[0][0],
		UserTicks:    userTicks,
		SysTicks:     sysTicks,
		StartTime:    startTime,
		RSSBytes:     rssPages * int64(os.Getpagesize()),
	}, nil
}

func scanSession(sessionID int) ([]procInfo, error) {
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
			incomplete = fmt.Errorf("cannot inspect process %d while enumerating session %d: %w", pid, sessionID, err)
			continue
		}
		if info.Session == sessionID {
			processes = append(processes, info)
		}
	}
	if incomplete != nil {
		return processes, incomplete
	}
	return processes, nil
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

func ownershipToken(bootID string, sessionID int, cgroupName string) string {
	if cgroupName == "" {
		cgroupName = "-"
	}
	return fmt.Sprintf("linux-v1;%s;%d;%s", bootID, sessionID, cgroupName)
}

func parseOwnershipToken(token string) (bootID string, sessionID int, err error) {
	bootID, sessionID, _, err = parseFullOwnershipToken(token)
	return bootID, sessionID, err
}

func parseFullOwnershipToken(token string) (bootID string, sessionID int, cgroupName string, err error) {
	parts := strings.Split(token, ";")
	if len(parts) != 4 || parts[0] != "linux-v1" || parts[1] == "" {
		return "", 0, "", errors.New("invalid Linux ownership token")
	}
	sessionID, err = strconv.Atoi(parts[2])
	if err != nil || sessionID <= 0 {
		return "", 0, "", errors.New("invalid Linux session identity")
	}
	return parts[1], sessionID, parts[3], nil
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
	ownerPath = filepath.Clean(ownerPath)
	if !pathWithin(location.mountPoint, ownerPath) || ownerPath == location.mountPoint {
		return "", errors.New("owned cgroup path is outside the active cgroup mount")
	}
	rel := strings.TrimPrefix(ownerPath, location.mountPoint)
	rel = strings.TrimPrefix(rel, string(filepath.Separator))
	return filepath.Clean(filepath.Join(location.mountRoot, rel)), nil
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
