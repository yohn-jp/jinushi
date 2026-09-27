//go:build linux

package linux

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

type Process struct {
	cmd          *exec.Cmd
	ownership    model.Ownership
	startedAt    time.Time
	cg           *cgroup
	ptyMaster    *os.File
	input        io.WriteCloser
	output       io.Reader
	inputMu      sync.Mutex
	waitDone     chan struct{}
	outputDone   chan struct{}
	resultMu     sync.RWMutex
	exit         backend.Exit
	waitErr      error
	outputErr    error
	peakMemory   int64
	peakPIDs     int64
	treeEmpty    bool
	limitOutcome string
	requested    bool
	forced       bool
	reason       string
}

func (p *Process) Ownership() model.Ownership { return p.ownership }

func (p *Process) Wait() (backend.Exit, error) {
	<-p.waitDone
	p.resultMu.RLock()
	defer p.resultMu.RUnlock()
	return p.exit, p.waitErr
}

func (p *Process) Signal(name string) error {
	signal, err := parseSignal(name)
	if err != nil {
		return err
	}
	if p.cg != nil {
		if err := validateCgroupOwnership(p.ownership, p.cg); err != nil {
			return err
		}
		return signalCgroup(p.cg, signal)
	}
	return signalOwnedTree(p.ownership, signal)
}

// LimitOutcome returns typed kernel evidence observed since this Run's cgroup
// was created. It is empty when the OS exposes no trigger or no event occurred.
func (p *Process) LimitOutcome() string {
	if p.cg == nil {
		p.resultMu.RLock()
		defer p.resultMu.RUnlock()
		return p.limitOutcome
	}
	outcome := p.cg.nativeLimitOutcome()
	if outcome == "" {
		p.resultMu.RLock()
		defer p.resultMu.RUnlock()
		return p.limitOutcome
	}
	p.resultMu.Lock()
	p.limitOutcome = outcome
	p.resultMu.Unlock()
	return outcome
}

func (p *Process) Terminate(grace time.Duration) (backend.TerminationResult, error) {
	p.resultMu.Lock()
	if p.treeEmpty {
		p.resultMu.Unlock()
		return p.terminationResult(false), p.waitError()
	}
	p.requested = true
	if p.reason == "" {
		p.reason = "cancelled"
	}
	p.resultMu.Unlock()

	if _, waitFailed := p.WaitIfDone(); waitFailed {
		return backend.TerminationResult{Requested: true, Outcome: "uncertain"}, p.waitError()
	} else if done, _ := p.isDone(); done {
		return p.terminationResult(false), nil
	}
	termErr := p.Signal("SIGTERM")
	if grace < 0 {
		grace = 0
	}
	if grace == 0 {
		p.markForced()
		killErr := p.Signal("SIGKILL")
		return p.waitAfterKill(errors.Join(termErr, killErr))
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-p.waitDone:
		return p.terminationResult(false), p.waitError()
	case <-timer.C:
		p.markForced()
		killErr := p.Signal("SIGKILL")
		return p.waitAfterKill(errors.Join(termErr, killErr))
	}
}

// WaitIfDone avoids blocking while checking whether a prior wait has proved
// termination. The result bool is the process wait error, if already final.
func (p *Process) WaitIfDone() (backend.Exit, bool) {
	select {
	case <-p.waitDone:
		exit, err := p.Wait()
		return exit, err != nil
	default:
		return backend.Exit{}, false
	}
}

func (p *Process) isDone() (bool, error) {
	select {
	case <-p.waitDone:
		return true, p.waitError()
	default:
		return false, nil
	}
}

func (p *Process) waitError() error {
	p.resultMu.RLock()
	defer p.resultMu.RUnlock()
	return p.waitErr
}

func (p *Process) markForced() {
	p.resultMu.Lock()
	p.forced = true
	p.reason = "forced-termination"
	p.resultMu.Unlock()
}

func (p *Process) waitAfterKill(signalErr error) (backend.TerminationResult, error) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-p.waitDone:
		return p.terminationResult(true), errors.Join(signalErr, p.waitError())
	case <-timer.C:
		return backend.TerminationResult{Requested: true, Forced: true, TreeEmpty: false, Outcome: "uncertain"}, errors.Join(signalErr, errors.New("process tree did not reach a proven empty state after SIGKILL"))
	}
}

func (p *Process) terminationResult(forced bool) backend.TerminationResult {
	p.resultMu.RLock()
	defer p.resultMu.RUnlock()
	outcome := p.exit.Outcome
	if outcome == "" {
		outcome = p.reason
	}
	return backend.TerminationResult{
		Requested: p.requested,
		Forced:    forced || p.forced,
		TreeEmpty: p.treeEmpty,
		Outcome:   outcome,
	}
}

func (p *Process) WriteInput(data []byte) error {
	p.inputMu.Lock()
	defer p.inputMu.Unlock()
	if p.input == nil {
		return errors.New("process input is closed")
	}
	_, err := p.input.Write(data)
	return err
}

func (p *Process) Resize(rows, cols uint16) error {
	p.inputMu.Lock()
	defer p.inputMu.Unlock()
	if p.ptyMaster == nil {
		return fmt.Errorf("resize: %w", ErrUnsupported)
	}
	if rows == 0 || cols == 0 {
		return errors.New("PTY rows and columns must be positive")
	}
	return pty.Setsize(p.ptyMaster, &pty.Winsize{Rows: rows, Cols: cols})
}

func (p *Process) CloseInput() error {
	p.inputMu.Lock()
	defer p.inputMu.Unlock()
	if p.input == nil {
		return nil
	}
	if p.ptyMaster != nil {
		return fmt.Errorf("closing one direction of a Linux PTY is unavailable: %w", ErrUnsupported)
	}
	err := p.input.Close()
	p.input = nil
	return err
}

func (p *Process) Observe() (model.Resources, error) {
	if p.cg != nil {
		if err := validateCgroupOwnership(p.ownership, p.cg); err != nil {
			return unavailableResources(), err
		}
		return p.observeCgroup(), nil
	}
	processes, err := scanOwnedTree(p.ownership)
	if err != nil {
		return unavailableResources(), err
	}
	resources := p.observeProcesses(processes)
	if len(activeProcesses(processes)) == 0 {
		// Process-table scans only see live processes. Once all members have exited,
		// their cumulative CPU time is no longer observable without cgroup
		// accounting; reporting the empty sum as measured zero would erase the
		// last valid sample in the final receipt.
		resources.CPUTimeNs = model.Metric{Status: "unavailable"}
	}
	return resources, nil
}

func (p *Process) observeCgroup() model.Resources {
	resources := unavailableResources()
	base := p.cg.location.childPath
	processes, processErr := scanCgroup(p.cg)
	if processErr != nil {
		return resources
	}
	total := totals(processes)
	resources.MemoryBytes = readMetric(filepath.Join(base, "memory.current"))
	resources.PeakMemoryBytes = readMetric(filepath.Join(base, "memory.peak"))
	if resources.MemoryBytes.Status == "unsupported" {
		resources.MemoryBytes = measuredMetric(total.Memory)
	}
	if resources.PeakMemoryBytes.Status != "measured" && resources.MemoryBytes.Status == "measured" {
		resources.PeakMemoryBytes = p.sampledMemoryPeak(resources.MemoryBytes.Value)
	} else if resources.MemoryBytes.Status == "measured" {
		p.updatePeaks(resources.MemoryBytes.Value, 0)
	}
	resources.CPUTimeNs = readCPUTime(filepath.Join(base, "cpu.stat"))
	if resources.CPUTimeNs.Status == "unsupported" {
		resources.CPUTimeNs = measuredMetric(total.CPUTimeNS)
	}
	resources.ProcessCount = readProcessCount(filepath.Join(base, "cgroup.procs"))
	if resources.ProcessCount.Status == "measured" {
		resources.PeakProcessCount = p.sampledProcessPeak(resources.ProcessCount.Value)
	}
	return resources
}

func scanCgroup(cg *cgroup) ([]procInfo, error) {
	data, err := os.ReadFile(filepath.Join(cg.location.childPath, "cgroup.procs"))
	if err != nil {
		return nil, err
	}
	processes := make([]procInfo, 0)
	for _, rawPID := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(rawPID)
		if err != nil || pid <= 0 {
			return nil, errors.New("cgroup.procs contains an invalid PID")
		}
		info, err := readProcInfo(pid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect cgroup process %d: %w", pid, err)
		}
		processes = append(processes, info)
	}
	return processes, nil
}

func (p *Process) observeProcesses(processes []procInfo) model.Resources {
	resource := unavailableResources()
	current := totals(processes)
	resource.MemoryBytes = measuredMetric(current.Memory)
	resource.PeakMemoryBytes = p.sampledMemoryPeak(current.Memory)
	resource.CPUTimeNs = measuredMetric(current.CPUTimeNS)
	resource.ProcessCount = measuredMetric(current.Processes)
	resource.PeakProcessCount = p.sampledProcessPeak(current.Processes)
	return resource
}

func (p *Process) updatePeaks(memory, processes int64) {
	p.resultMu.Lock()
	if memory > p.peakMemory {
		p.peakMemory = memory
	}
	if processes > p.peakPIDs {
		p.peakPIDs = processes
	}
	p.resultMu.Unlock()
}

func (p *Process) sampledMemoryPeak(current int64) model.Metric {
	p.resultMu.Lock()
	if current > p.peakMemory {
		p.peakMemory = current
	}
	peak := p.peakMemory
	p.resultMu.Unlock()
	return measuredMetric(peak)
}

func (p *Process) sampledProcessPeak(current int64) model.Metric {
	p.resultMu.Lock()
	if current > p.peakPIDs {
		p.peakPIDs = current
	}
	peak := p.peakPIDs
	p.resultMu.Unlock()
	return measuredMetric(peak)
}

func (p *Process) waitForExit() {
	waitErr := p.cmd.Wait()
	state := p.cmd.ProcessState
	exit := backend.Exit{StartedAt: p.startedAt, FinishedAt: time.Now().UTC()}
	if state != nil {
		exit.ExitCode = exitCode(state)
		exit.Signal = exitSignal(state)
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			waitErr = nil
		}
	} else {
		waitErr = errors.Join(waitErr, errors.New("process exit status is unavailable"))
	}

	treeErr := p.waitForTreeEmpty()
	if treeErr != nil {
		waitErr = errors.Join(waitErr, treeErr)
		exit.Outcome = "uncertain"
	} else {
		p.resultMu.Lock()
		p.treeEmpty = true
		p.resultMu.Unlock()
		if p.cg != nil {
			exit.Outcome = p.cg.nativeLimitOutcome()
			p.resultMu.Lock()
			p.limitOutcome = exit.Outcome
			p.resultMu.Unlock()
		}
	}
	if exit.Outcome == "" {
		p.resultMu.RLock()
		reason := p.reason
		p.resultMu.RUnlock()
		switch {
		case reason != "":
			exit.Outcome = reason
		case exit.Signal != "":
			exit.Outcome = "signaled"
		default:
			exit.Outcome = "exited"
		}
	}
	if p.ptyMaster != nil {
		p.inputMu.Lock()
		_ = p.ptyMaster.Close()
		p.input = nil
		p.inputMu.Unlock()
		<-p.outputDone
	}
	p.resultMu.RLock()
	copyErr := p.outputErr
	p.resultMu.RUnlock()
	if copyErr != nil {
		waitErr = errors.Join(waitErr, fmt.Errorf("PTY output writer: %w", copyErr))
	}
	if p.cg != nil {
		p.cg.close()
	}

	p.resultMu.Lock()
	p.exit = exit
	p.waitErr = waitErr
	p.resultMu.Unlock()
	close(p.waitDone)
}

func (p *Process) waitForTreeEmpty() error {
	for {
		var empty bool
		var err error
		if p.cg != nil {
			empty, err = cgroupEmpty(p.cg)
		} else {
			var processes []procInfo
			processes, err = scanOwnedTree(p.ownership)
			if err == nil {
				parsed, parseErr := subreaperToken(p.ownership)
				if parseErr != nil {
					err = parseErr
				} else if reapErr := reapAdoptedZombies(processes, parsed.Subreaper, p.ownership.PID, p.ownership.StartTime); reapErr != nil {
					err = reapErr
				}
			}
			empty = len(activeProcesses(processes)) == 0
		}
		if err != nil {
			return err
		}
		if empty {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (p *Process) copyPTYOutput(writer io.Writer) {
	_, err := io.Copy(writer, p.output)
	// Linux reports EIO when the last slave descriptor closes; this is the
	// normal terminal EOF condition after the owned process tree exits.
	if err != nil && !errors.Is(err, syscall.EIO) && !errors.Is(err, io.EOF) {
		p.resultMu.Lock()
		p.outputErr = err
		p.resultMu.Unlock()
	}
	close(p.outputDone)
}

func validateCgroupOwnership(owner model.Ownership, cg *cgroup) error {
	if owner.Backend != "linux" || owner.PID <= 0 || owner.StartTime == 0 || owner.CgroupPath == "" {
		return errors.New("incomplete Linux cgroup ownership evidence")
	}
	bootID, sessionID, name, err := parseFullOwnershipToken(owner.Token)
	if err != nil {
		return err
	}
	currentBoot, err := readBootID()
	if err != nil || currentBoot != bootID {
		if err == nil {
			err = errors.New("Linux boot identity changed")
		}
		return err
	}
	if name != cg.name || filepath.Clean(owner.CgroupPath) != filepath.Clean(cg.location.childPath) || sessionID != owner.ProcessGroup {
		return errors.New("Linux cgroup ownership token does not match the execution")
	}
	return nil
}

func cgroupEmpty(cg *cgroup) (bool, error) {
	data, err := os.ReadFile(filepath.Join(cg.location.childPath, "cgroup.events"))
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[0] == "populated" {
				value, parseErr := strconv.Atoi(fields[1])
				return value == 0, parseErr
			}
		}
	}
	pids, readErr := os.ReadFile(filepath.Join(cg.location.childPath, "cgroup.procs"))
	if readErr != nil {
		return false, errors.Join(err, readErr)
	}
	return len(strings.Fields(string(pids))) == 0, nil
}

func readMetric(path string) model.Metric {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.Metric{Status: "unsupported"}
		}
		return model.Metric{Status: "unavailable"}
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || value < 0 {
		return model.Metric{Status: "unavailable"}
	}
	return measuredMetric(value)
}

func readCPUTime(path string) model.Metric {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.Metric{Status: "unsupported"}
		}
		return model.Metric{Status: "unavailable"}
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			value, parseErr := strconv.ParseInt(fields[1], 10, 64)
			if parseErr != nil || value < 0 {
				return model.Metric{Status: "unavailable"}
			}
			return measuredMetric(value * 1000)
		}
	}
	return model.Metric{Status: "unsupported"}
}

func readProcessCount(path string) model.Metric {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.Metric{Status: "unsupported"}
		}
		return model.Metric{Status: "unavailable"}
	}
	return measuredMetric(int64(len(strings.Fields(string(data)))))
}

func measuredMetric(value int64) model.Metric { return model.Metric{Status: "measured", Value: value} }

func unavailableResources() model.Resources {
	return model.Resources{
		MemoryBytes:      model.Metric{Status: "unavailable"},
		PeakMemoryBytes:  model.Metric{Status: "unavailable"},
		CPUTimeNs:        model.Metric{Status: "unavailable"},
		ProcessCount:     model.Metric{Status: "unavailable"},
		PeakProcessCount: model.Metric{Status: "unavailable"},
	}
}

func parseSignal(name string) (syscall.Signal, error) {
	signal, ok := map[string]syscall.Signal{
		"SIGHUP":  syscall.SIGHUP,
		"SIGINT":  syscall.SIGINT,
		"SIGKILL": syscall.SIGKILL,
		"SIGQUIT": syscall.SIGQUIT,
		"SIGTERM": syscall.SIGTERM,
		"SIGUSR1": syscall.SIGUSR1,
		"SIGUSR2": syscall.SIGUSR2,
	}[strings.ToUpper(name)]
	if !ok {
		return 0, fmt.Errorf("unsupported Linux signal %q", name)
	}
	return signal, nil
}

func exitCode(state *os.ProcessState) *int {
	if state == nil {
		return nil
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !status.Exited() {
		return nil
	}
	value := status.ExitStatus()
	return &value
}

func exitSignal(state *os.ProcessState) string {
	if state == nil {
		return ""
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() {
		return ""
	}
	switch status.Signal() {
	case syscall.SIGHUP:
		return "SIGHUP"
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGQUIT:
		return "SIGQUIT"
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGUSR1:
		return "SIGUSR1"
	case syscall.SIGUSR2:
		return "SIGUSR2"
	default:
		return status.Signal().String()
	}
}
