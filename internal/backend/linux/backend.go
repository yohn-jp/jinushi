//go:build linux

// Package linux implements Jinushi's Linux process backend using dedicated
// sessions, cgroup v2 when delegated, and real PTYs for interactive Runs.
package linux

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
	"golang.org/x/sys/unix"
)

var ErrUnsupported = errors.New("Linux backend capability is unsupported")

type cgroupSpawnError struct{ err error }

func (e *cgroupSpawnError) Error() string { return e.err.Error() }
func (e *cgroupSpawnError) Unwrap() error { return e.err }

type Backend struct{}

func New() backend.Backend { return &Backend{} }

func (*Backend) ValidateLimits(limits model.Limits) error {
	if limits.CPUQuotaPercent > (int64(^uint64(0)>>1) / 1000) {
		return errors.New("CPU quota exceeds Linux cgroup v2 range")
	}
	if limits.ProcessCount > 0 {
		return fmt.Errorf("%w: Linux cgroup v2 cannot enforce a process-leader limit; use a task-count limit", ErrUnsupported)
	}
	if limits.TaskCount > 0 {
		location, ok := probeCgroup()
		if !ok || !location.pidsLimit {
			return fmt.Errorf("%w: Linux task-count enforcement requires a delegated cgroup v2 pids controller", ErrUnsupported)
		}
	}
	return nil
}

func (*Backend) Capabilities() model.Capabilities {
	location, ok := probeCgroup()
	capabilities := model.Capabilities{
		Backend:                 "linux",
		PTY:                     true,
		MemoryTelemetry:         true,
		CPUTelemetry:            true,
		ProcessTelemetry:        true,
		TaskTelemetry:           true,
		ProcessCountEnforcement: false,
		RestartReconciliation:   "process-subreaper-tree",
		Signals:                 []string{"SIGHUP", "SIGINT", "SIGKILL", "SIGQUIT", "SIGTERM", "SIGUSR1", "SIGUSR2"},
	}
	if ok {
		capabilities.RestartReconciliation = "cgroup-v2"
		capabilities.MemoryEnforcement = location.memoryLimit
		capabilities.CPUQuotaEnforcement = location.cpuLimit
		capabilities.TaskCountEnforcement = location.pidsLimit
	}
	return capabilities
}

func (*Backend) Start(spec model.RunSpec, stdout, stderr io.Writer) (backend.Process, error) {
	if len(spec.Argv) == 0 || spec.Argv[0] == "" {
		return nil, errors.New("argv must contain an executable")
	}
	if !filepath.IsAbs(spec.Cwd) {
		return nil, errors.New("cwd must be absolute")
	}
	if spec.Limits.MemoryBytes < 0 || spec.Limits.CPUQuotaPercent < 0 || spec.Limits.ProcessCount < 0 || spec.Limits.TaskCount < 0 || spec.Limits.WallTimeMs < 0 || spec.Limits.OutputBytes < 0 {
		return nil, errors.New("resource limits cannot be negative")
	}
	if spec.Limits.ProcessCount > 0 {
		return nil, fmt.Errorf("%w: Linux cgroup v2 cannot enforce a process-leader limit; use a task-count limit", ErrUnsupported)
	}
	env, err := runEnvironment(spec.Environment)
	if err != nil {
		return nil, err
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}

	limitRequested := spec.Limits.MemoryBytes > 0 || spec.Limits.CPUQuotaPercent > 0 || spec.Limits.TaskCount > 0
	var cg *cgroup
	var cgroupSetupErr error
	location, locationErr := discoverCgroup()
	if locationErr == nil && cgroupWritable(location.basePath) {
		name, nameErr := randomCgroupName()
		if nameErr != nil {
			return nil, nameErr
		}
		cg, cgroupSetupErr = newCgroup(location, name, spec.Limits.MemoryBytes, spec.Limits.CPUQuotaPercent, spec.Limits.TaskCount)
	}
	if limitRequested && cg == nil {
		if cgroupSetupErr == nil {
			cgroupSetupErr = locationErr
		}
		if cgroupSetupErr == nil {
			cgroupSetupErr = ErrUnsupported
		}
		return nil, fmt.Errorf("%w: requested Linux resource limit cannot be enforced: %v", ErrUnsupported, cgroupSetupErr)
	}
	if cg != nil {
		defer func() {
			if cg != nil {
				cg.close()
			}
		}()
	}

	process, err := startCommand(spec, env, stdout, stderr, cg)
	var spawnErr *cgroupSpawnError
	if err != nil && cg != nil && !limitRequested && errors.As(err, &spawnErr) && isCgroupSpawnUnavailable(err) {
		cg.close()
		cg = nil
		process, err = startCommand(spec, env, stdout, stderr, nil)
	}
	if err != nil {
		return nil, err
	}
	// The local handle now owns cleanup of this cgroup.
	cg = nil
	return process, nil
}

func startCommand(spec model.RunSpec, env []string, stdout, stderr io.Writer, cg *cgroup) (*Process, error) {
	cwd, runCwd, err := openRunDirectory(spec.Cwd)
	if err != nil {
		return nil, err
	}
	defer cwd.Close()
	path, err := resolveExecutable(spec.Argv[0], runCwd, env)
	if err != nil {
		return nil, err
	}
	bootID, err := readBootID()
	if err != nil {
		return nil, fmt.Errorf("read Linux boot identity: %w", err)
	}
	var reaper subreaperIdentity
	if cg == nil {
		reaper, err = enableSubreaper()
		if err != nil {
			return nil, fmt.Errorf("%w: no-cgroup process-tree ownership requires a child subreaper: %v", ErrUnsupported, err)
		}
	}
	cmd := exec.Command(path, spec.Argv[1:]...)
	cmd.Args = append([]string(nil), spec.Argv...)
	cmd.Dir = runCwd
	cmd.Env = env
	p := &Process{
		cmd:         cmd,
		startedAt:   time.Now().UTC(),
		interactive: spec.Interactive,
		waitDone:    make(chan struct{}),
		outputDone:  make(chan struct{}),
		cg:          cg,
	}

	var master, slave *os.File
	if spec.Interactive {
		master, slave, err = pty.Open()
		if err != nil {
			return nil, fmt.Errorf("open PTY: %w", err)
		}
		if err := pty.Setsize(master, &pty.Winsize{Rows: 24, Cols: 80}); err != nil {
			_ = master.Close()
			_ = slave.Close()
			return nil, fmt.Errorf("set initial PTY size: %w", err)
		}
		cmd.Stdin = slave
		cmd.Stdout = slave
		cmd.Stderr = slave
		p.ptyMaster = master
		p.input = master
		p.output = master
	} else {
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		p.input = stdin
		cmd.Stdout = stdout
		cmd.Stderr = stderr
	}

	attributes := &syscall.SysProcAttr{Setsid: true}
	if spec.Interactive {
		attributes.Setctty = true
		attributes.Ctty = 0
	}
	if cg != nil {
		attributes.UseCgroupFD = true
		attributes.CgroupFD = int(cg.file.Fd())
	}
	cmd.SysProcAttr = attributes
	if err := cmd.Start(); err != nil {
		if master != nil {
			_ = master.Close()
		}
		if slave != nil {
			_ = slave.Close()
		}
		if cg != nil {
			return nil, &cgroupSpawnError{err: err}
		}
		return nil, err
	}
	if slave != nil {
		_ = slave.Close()
	}

	info, infoErr := readProcInfo(cmd.Process.Pid)
	owner := model.Ownership{Backend: "linux", PID: cmd.Process.Pid, ProcessGroup: cmd.Process.Pid}
	if cg != nil {
		owner.CgroupPath = cg.location.childPath
		owner.Token = ownershipToken(bootID, cmd.Process.Pid, cg.name, cg.identity)
	} else {
		owner.Token = ownershipTokenWithSubreaper(bootID, cmd.Process.Pid, reaper)
	}
	if infoErr != nil || info.StartTime == 0 || info.Session != cmd.Process.Pid {
		cause := infoErr
		if cause == nil {
			cause = errors.New("could not validate started process identity")
		}
		if cg != nil {
			owner.CgroupPath = cg.location.childPath
		}
		cleanupErr := cleanupUnidentifiedStart(cmd, cg, owner)
		if cleanupErr != nil {
			return nil, &backend.UncertainError{Ownership: owner, Err: errors.Join(cause, cleanupErr)}
		}
		if master != nil {
			_ = master.Close()
		}
		return nil, fmt.Errorf("validate started process identity: %w", cause)
	}
	p.ownership = model.Ownership{
		Backend:      "linux",
		PID:          cmd.Process.Pid,
		StartTime:    info.StartTime,
		ProcessGroup: info.ProcessGroup,
		Token:        ownershipTokenWithSubreaper(bootID, info.Session, reaper),
	}
	if cg != nil {
		p.ownership.CgroupPath = cg.location.childPath
		p.ownership.Token = ownershipToken(bootID, info.Session, cg.name, cg.identity)
	}
	if spec.Interactive {
		go p.copyPTYOutput(stdout)
	} else {
		close(p.outputDone)
	}
	go p.waitForExit()
	return p, nil
}

func openRunDirectory(path string) (*os.File, string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("open Run working directory: %w", err)
	}
	info, err := dir.Stat()
	if err != nil {
		_ = dir.Close()
		return nil, "", fmt.Errorf("inspect Run working directory: %w", err)
	}
	if !info.IsDir() {
		_ = dir.Close()
		return nil, "", errors.New("Run working directory is not a directory")
	}
	// Go's fork/exec child changes directory before exec. Resolving this procfs
	// magic link in that step pins the child to the directory opened above,
	// even if the caller's original path is renamed or replaced meanwhile.
	return dir, fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), dir.Fd()), nil
}

func cleanupUnidentifiedStart(cmd *exec.Cmd, cg *cgroup, owner model.Ownership) error {
	var controlErr error
	if cg != nil {
		controlErr = killCgroup(cg)
	} else {
		controlErr = signalOwnedTree(owner, syscall.SIGKILL)
	}
	if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			return errors.Join(controlErr, err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var empty bool
		var err error
		if cg != nil {
			empty, err = cgroupEmpty(cg)
		} else {
			var list []procInfo
			list, err = scanOwnedTree(owner)
			if err == nil {
				parsed, parseErr := subreaperToken(owner)
				if parseErr != nil {
					err = parseErr
				} else if reapErr := reapAdoptedZombies(list, parsed.Subreaper, owner.PID, owner.StartTime); reapErr != nil {
					err = reapErr
				}
			}
			empty = err == nil && len(activeProcesses(list)) == 0
		}
		if err == nil && empty {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.Join(controlErr, errors.New("started process tree did not reach a proven empty state"))
}

func isCgroupSpawnUnavailable(err error) bool {
	return errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.EPERM)
}

func resolveExecutable(argv0, cwd string, environment []string) (string, error) {
	if strings.ContainsRune(argv0, filepath.Separator) {
		if filepath.IsAbs(argv0) {
			return argv0, nil
		}
		return filepath.Clean(filepath.Join(cwd, argv0)), nil
	}
	pathValue, found := "", false
	for _, item := range environment {
		key, value, ok := strings.Cut(item, "=")
		if ok && key == "PATH" {
			pathValue, found = value, true
		}
	}
	if !found {
		return "", fmt.Errorf("executable %q cannot be resolved: effective environment has no PATH", argv0)
	}
	for _, directory := range strings.Split(pathValue, string(os.PathListSeparator)) {
		if directory == "" {
			directory = cwd
		} else if !filepath.IsAbs(directory) {
			directory = filepath.Join(cwd, directory)
		}
		candidate := filepath.Join(directory, argv0)
		if err := unix.Access(candidate, unix.X_OK); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("executable %q was not found in the effective PATH", argv0)
}

func runEnvironment(environment model.Environment) ([]string, error) {
	values := make(map[string]string)
	switch environment.Mode {
	case "", "inherit-supervisor":
		for _, item := range os.Environ() {
			key, value, found := strings.Cut(item, "=")
			if found {
				values[key] = value
			}
		}
	case "replace":
	default:
		return nil, fmt.Errorf("unsupported environment mode %q", environment.Mode)
	}
	for _, key := range environment.Unset {
		if key == "" || strings.ContainsAny(key, "=\x00") {
			return nil, fmt.Errorf("invalid environment variable name %q", key)
		}
		delete(values, key)
	}
	keys := make([]string, 0, len(environment.Set))
	for key := range environment.Set {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(environment.Set[key], '\x00') {
			return nil, fmt.Errorf("invalid environment entry for %q", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values[key] = environment.Set[key]
	}
	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, key+"="+value)
	}
	sort.Strings(out)
	return out, nil
}

func randomCgroupName() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return "jinushi-" + hex.EncodeToString(id[:]), nil
}

func probeCgroup() (cgroupLocation, bool) {
	location, err := discoverCgroup()
	if err != nil {
		return cgroupLocation{}, false
	}
	parent, err := openDirectoryNoSymlinks(location.basePath)
	if err != nil {
		return cgroupLocation{}, false
	}
	defer parent.Close()
	if verifyCgroupDirectory(parent) != nil {
		return cgroupLocation{}, false
	}
	name, err := randomCgroupName()
	if err != nil {
		return cgroupLocation{}, false
	}
	probePath := filepath.Join(location.basePath, name)
	if err := unix.Mkdirat(int(parent.Fd()), name, 0700); err != nil {
		return cgroupLocation{}, false
	}
	probeFD, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return cgroupLocation{}, false
	}
	probe := os.NewFile(uintptr(probeFD), probePath)
	var probeIdentity unix.Stat_t
	if err := unix.Fstat(probeFD, &probeIdentity); err != nil {
		_ = probe.Close()
		return cgroupLocation{}, false
	}
	defer func() {
		_ = probe.Close()
		_ = removeCgroupAt(parent, name, probeIdentity.Ino)
	}()
	if verifyCgroupDirectory(probe) != nil {
		return cgroupLocation{}, false
	}
	location.childPath = probePath
	location.childRel = childRelativePath(location, name)
	if _, err := readFileAt(probe, "cgroup.procs"); err != nil {
		return cgroupLocation{}, false
	}
	location.memoryLimit = fileWritableAt(probe, "memory.max") && fileWritableAt(probe, "memory.oom.group")
	location.cpuLimit = fileWritableAt(probe, "cpu.max")
	location.pidsLimit = fileWritableAt(probe, "pids.max")
	return location, true
}

func fileWritableAt(dir *os.File, name string) bool {
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return false
	}
	return unix.Close(fd) == nil
}

func killCgroup(cg *cgroup) error {
	if cg == nil {
		return nil
	}
	if err := writeFileAt(cg.file, "cgroup.kill", "1"); err == nil {
		return nil
	}
	return signalCgroup(cg, syscall.SIGKILL)
}

func signalCgroup(cg *cgroup, signal syscall.Signal) error {
	data, err := readFileAt(cg.file, "cgroup.procs")
	if err != nil {
		return err
	}
	var failures []error
	for _, line := range strings.Fields(string(data)) {
		pid := atoiOrZero(line)
		if pid <= 0 {
			continue
		}
		before, err := readProcInfo(pid)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			failures = append(failures, err)
			continue
		}
		inCgroup, err := processInCgroup(pid, cg)
		if err != nil || !inCgroup {
			if err == nil {
				err = errors.New("process is outside the owned cgroup")
			}
			failures = append(failures, err)
			continue
		}
		err = pidfdSignal(pid, signal, before, func() bool {
			current, pathErr := processInCgroup(pid, cg)
			return pathErr == nil && current
		})
		if err != nil && !errors.Is(err, syscall.ESRCH) {
			failures = append(failures, fmt.Errorf("signal cgroup process %d: %w", pid, err))
		}
	}
	return errors.Join(failures...)
}

func subreaperToken(owner model.Ownership) (linuxOwnershipToken, error) {
	if owner.Backend != "linux" || owner.PID <= 0 {
		return linuxOwnershipToken{}, errors.New("incomplete Linux process ownership evidence")
	}
	parsed, err := parseLinuxOwnershipToken(owner.Token)
	if err != nil {
		return linuxOwnershipToken{}, err
	}
	if parsed.Version != 2 || parsed.CgroupName != "-" || parsed.Subreaper.PID <= 0 || parsed.Subreaper.StartTime == 0 {
		return linuxOwnershipToken{}, errors.New("Linux process-tree ownership lacks subreaper evidence")
	}
	if parsed.SessionID != owner.ProcessGroup {
		return linuxOwnershipToken{}, errors.New("Linux process session does not match persisted ownership")
	}
	currentBootID, err := readBootID()
	if err != nil || currentBootID != parsed.BootID {
		if err == nil {
			err = errors.New("Linux boot identity changed")
		}
		return linuxOwnershipToken{}, err
	}
	return parsed, nil
}

func scanOwnedTree(owner model.Ownership) ([]procInfo, error) {
	parsed, err := subreaperToken(owner)
	if err != nil {
		return nil, err
	}
	return scanSubreaperTree(parsed.Subreaper)
}

func signalOwnedTree(owner model.Ownership, signal syscall.Signal) error {
	parsed, err := subreaperToken(owner)
	if err != nil {
		return err
	}
	processes, err := scanSubreaperTree(parsed.Subreaper)
	if err != nil {
		return err
	}
	var failures []error
	for _, process := range activeProcesses(processes) {
		before := process
		err := pidfdSignal(process.PID, signal, before, func() bool {
			currentBootID, bootErr := readBootID()
			return bootErr == nil && currentBootID == parsed.BootID && isSubreaperDescendant(process.PID, process.StartTime, parsed.Subreaper)
		})
		if err != nil && !errors.Is(err, syscall.ESRCH) {
			failures = append(failures, fmt.Errorf("signal owned Linux descendant %d: %w", process.PID, err))
		}
	}
	return errors.Join(failures...)
}

func atoiOrZero(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}
