//go:build windows

// Package windows implements Jinushi's Windows process ownership backend.
package windows

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf16"
	"unsafe"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
	"golang.org/x/sys/windows"
)

const (
	jobObjectMemoryUsageInformation = 28
	jobCPUEnable                    = 0x1
	jobCPUHardCap                   = 0x4
	completionActiveProcessLimit    = 3
	completionActiveProcessZero     = 4
	completionNewProcess            = 6
	completionExitProcess           = 7
	completionAbnormalExit          = 8
	completionProcessMemoryLimit    = 9
	completionJobMemoryLimit        = 10
	maxPipeRead                     = 64 * 1024
)

var (
	procOpenJobObjectW = windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")
	procSetLastError   = windows.NewLazySystemDLL("kernel32.dll").NewProc("SetLastError")
	errUnsupported     = errors.New("Windows backend does not support the requested capability")
	errProcessExited   = errors.New("Run process tree is already terminal")
)

type jobAccountingInformation struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaults           uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

type jobMemoryUsageInformation struct {
	JobMemory     uint64
	PeakJobMemory uint64
}

type jobCPURateControlInformation struct {
	ControlFlags uint32
	CPURate      uint32
}

type jobAssociateCompletionPort struct {
	CompletionKey  uintptr
	CompletionPort windows.Handle
}

// Process owns one Windows Job Object and its initial process. Job assignment
// happens while that process is suspended, before it can create descendants.
type Process struct {
	mu           sync.Mutex
	completionMu sync.Mutex

	job         windows.Handle
	process     windows.Handle
	input       windows.Handle
	pty         windows.Handle
	stdout      windows.Handle
	stderr      windows.Handle
	completion  windows.Handle
	pid         uint32
	startTime   uint64
	startedAt   time.Time
	interactive bool
	closed      bool

	stdoutDone        sync.WaitGroup
	outputErr         error
	waitOnce          sync.Once
	waitDone          chan struct{}
	waitResult        backend.Exit
	waitErr           error
	finalResources    model.Resources
	hasFinalResources bool

	peakProcessCount atomic.Int64
	limitMessage     atomic.Uint32
	activePIDs       map[uint32]struct{}
}

var _ backend.Process = (*Process)(nil)
var _ backend.LimitEvidence = (*Process)(nil)

type implementation struct{}

var _ backend.Backend = implementation{}

func New() backend.Backend { return implementation{} }

func (implementation) Capabilities() model.Capabilities { return Capabilities() }

func (implementation) Start(spec model.RunSpec, stdout, stderr io.Writer) (backend.Process, error) {
	return Start(spec, stdout, stderr)
}

func (implementation) Reconcile(ownership model.Ownership) (backend.ReconcileResult, error) {
	return Reconcile(ownership)
}

// Capabilities describes the Windows APIs present on this host. Hard memory
// and process-count limits use Job Object enforcement. CPU rate capability is
// probed by applying a hard cap to a temporary Job Object.
func Capabilities() model.Capabilities {
	c := model.Capabilities{
		Backend:                 "windows-job",
		MemoryEnforcement:       true,
		ProcessCountEnforcement: true,
		MemoryTelemetry:         supportsJobMemoryTelemetry(),
		CPUTelemetry:            true,
		ProcessTelemetry:        true,
		RestartReconciliation:   "guardian-required; missing-job-is-uncertain",
		Signals:                 []string{"ctrl-break", "terminate"},
	}
	if err := windows.NewLazySystemDLL("kernel32.dll").NewProc("CreatePseudoConsole").Find(); err == nil {
		c.PTY = true
	}
	c.CPUQuotaEnforcement = supportsCPURateControl()
	return c
}

// Start creates the requested command directly. No shell is introduced.
// Interactive output is merged by ConPTY and delivered to stdout.
func Start(spec model.RunSpec, stdout, stderr io.Writer) (*Process, error) {
	if len(spec.Argv) == 0 || spec.Argv[0] == "" {
		return nil, errors.New("argv must contain an executable")
	}
	if spec.Cwd == "" || !filepath.IsAbs(spec.Cwd) {
		return nil, errors.New("cwd must be an absolute path")
	}
	cwd, err := filepath.Abs(spec.Cwd)
	if err != nil {
		return nil, fmt.Errorf("resolve cwd: %w", err)
	}
	info, err := os.Stat(cwd)
	if err != nil {
		return nil, fmt.Errorf("inspect cwd: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("cwd is not a directory")
	}

	if spec.Interactive && !Capabilities().PTY {
		return nil, fmt.Errorf("ConPTY: %w", errUnsupported)
	}
	if spec.Limits.MemoryBytes < 0 || spec.Limits.ProcessCount < 0 || spec.Limits.CPUQuotaPercent < 0 {
		return nil, errors.New("resource limits must not be negative")
	}
	if spec.Limits.MemoryBytes > 0 && uint64(spec.Limits.MemoryBytes) > uint64(^uintptr(0)) {
		return nil, fmt.Errorf("memory limit exceeds Windows Job Object range: %w", errUnsupported)
	}
	if spec.Limits.ProcessCount > int64(^uint32(0)) {
		return nil, fmt.Errorf("process-count limit exceeds Windows Job Object range: %w", errUnsupported)
	}
	if spec.Limits.CPUQuotaPercent > 0 {
		if spec.Limits.CPUQuotaPercent > 100 || !Capabilities().CPUQuotaEnforcement {
			return nil, fmt.Errorf("CPU quota must be enforceable as a Windows Job Object hard cap: %w", errUnsupported)
		}
	}

	envVars, err := environmentValues(spec.Environment)
	if err != nil {
		return nil, err
	}
	executable, err := resolveExecutable(spec.Argv[0], cwd, envVars)
	if err != nil {
		return nil, fmt.Errorf("resolve executable: %w", err)
	}
	commandLine, err := makeCommandLine(spec.Argv)
	if err != nil {
		return nil, err
	}
	appName, err := windows.UTF16PtrFromString(executable)
	if err != nil {
		return nil, fmt.Errorf("encode executable path: %w", err)
	}
	cmdLine, err := windows.UTF16FromString(commandLine)
	if err != nil {
		return nil, fmt.Errorf("encode command line: %w", err)
	}
	cwd16, err := windows.UTF16PtrFromString(cwd)
	if err != nil {
		return nil, fmt.Errorf("encode cwd: %w", err)
	}
	envBlock := environmentBlock(envVars)

	prepared, err := createSuspended(spec, appName, &cmdLine[0], cwd16, envBlock)
	if err != nil {
		return nil, err
	}
	pi := prepared.info
	defer windows.CloseHandle(pi.Thread)

	startedAt := time.Now().UTC()
	prepared.ownership = model.Ownership{Backend: "windows-job", PID: int(pi.ProcessId)}
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(pi.Process, &creation, &exit, &kernel, &user); err != nil {
		return nil, prepared.cleanup(fmt.Errorf("read initial process identity: %w", err))
	}
	startTime := uint64(creation.Nanoseconds())
	prepared.ownership = model.Ownership{Backend: "windows-job", PID: int(pi.ProcessId), StartTime: startTime}
	jobName := jobName(pi.ProcessId, startTime)
	job, err := createUniqueJob(jobName)
	if err != nil {
		return nil, prepared.cleanup(fmt.Errorf("create Job Object: %w", err))
	}
	jobOwned := false
	defer func() {
		if !jobOwned {
			_ = windows.CloseHandle(job)
		}
	}()

	if err := configureJob(job, spec.Limits); err != nil {
		return nil, prepared.cleanup(fmt.Errorf("configure Job Object: %w", err))
	}
	completion, err := createCompletionPort(job)
	if err != nil {
		return nil, prepared.cleanup(fmt.Errorf("monitor Job Object: %w", err))
	}
	completionOwned := false
	defer func() {
		if !completionOwned {
			_ = windows.CloseHandle(completion)
		}
	}()

	if err := windows.AssignProcessToJobObject(job, pi.Process); err != nil {
		return nil, prepared.cleanup(fmt.Errorf("assign process to Job Object: %w", err))
	}
	ownership := model.Ownership{Backend: "windows-job", PID: int(pi.ProcessId), StartTime: startTime}
	resumed, resumeErr := windows.ResumeThread(pi.Thread)
	if resumeErr != nil {
		if cleanupErr := cleanupAssigned(job, pi); cleanupErr != nil {
			jobOwned = true
			completionOwned = true
			prepared.retain = true
			return nil, &backend.UncertainError{Ownership: ownership, Err: errors.Join(fmt.Errorf("resume initial process: %w", resumeErr), cleanupErr)}
		}
		prepared.closeAfterClean()
		return nil, fmt.Errorf("resume initial process: %w", resumeErr)
	}
	if resumed != 1 {
		unexpected := fmt.Errorf("initial thread had unexpected suspend count %d", resumed)
		if cleanupErr := cleanupAssigned(job, pi); cleanupErr != nil {
			jobOwned = true
			completionOwned = true
			prepared.retain = true
			return nil, &backend.UncertainError{Ownership: ownership, Err: errors.Join(unexpected, cleanupErr)}
		}
		prepared.closeAfterClean()
		return nil, unexpected
	}

	p := &Process{
		job: job, process: pi.Process, input: prepared.input, pty: prepared.pty,
		stdout: prepared.stdout, stderr: prepared.stderr, completion: completion,
		pid: pi.ProcessId, startTime: startTime, startedAt: startedAt,
		interactive: spec.Interactive, waitDone: make(chan struct{}),
		activePIDs: map[uint32]struct{}{pi.ProcessId: {}},
	}
	p.peakProcessCount.Store(1)
	if !spec.Interactive {
		p.startPump(stdout, p.stdout)
		p.startPump(stderr, p.stderr)
	} else {
		p.startPump(stdout, p.stdout)
	}
	jobOwned = true
	completionOwned = true
	prepared.retain = true
	return p, nil
}

// Ownership contains public OS identity evidence only. The Job Object name is
// derived from PID and creation time, so no guardian/control secret is exposed.
func (p *Process) Ownership() model.Ownership {
	return model.Ownership{Backend: "windows-job", PID: int(p.pid), StartTime: p.startTime}
}

// LimitOutcome returns only native Job Object evidence. An empty value means
// that the completion port has not observed a hard-limit notification.
func (p *Process) LimitOutcome() string {
	p.drainCompletionPort()
	return p.terminationOutcome("")
}

func (p *Process) WriteInput(data []byte) error {
	p.mu.Lock()
	if p.closed || p.input == 0 {
		p.mu.Unlock()
		return errProcessExited
	}
	var input windows.Handle
	if err := windows.DuplicateHandle(windows.CurrentProcess(), p.input, windows.CurrentProcess(), &input, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		p.mu.Unlock()
		return fmt.Errorf("duplicate Run stdin handle: %w", err)
	}
	p.mu.Unlock()
	defer windows.CloseHandle(input)
	for len(data) > 0 {
		chunk := data
		if len(chunk) > 1<<30 {
			chunk = chunk[:1<<30]
		}
		var written uint32
		if err := windows.WriteFile(input, chunk, &written, nil); err != nil {
			return fmt.Errorf("write Run stdin: %w", err)
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func (p *Process) CloseInput() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.input == 0 {
		return nil
	}
	input := p.input
	p.input = 0
	if err := windows.CloseHandle(input); err != nil {
		return fmt.Errorf("close Run stdin: %w", err)
	}
	return nil
}

func (p *Process) Resize(rows, cols uint16) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.interactive || p.pty == 0 {
		return fmt.Errorf("Resize requires an interactive ConPTY Run: %w", errUnsupported)
	}
	if rows == 0 || cols == 0 || rows > 32767 || cols > 32767 {
		return errors.New("ConPTY rows and columns must be between 1 and 32767")
	}
	if p.closed {
		return errProcessExited
	}
	if err := windows.ResizePseudoConsole(p.pty, windows.Coord{X: int16(cols), Y: int16(rows)}); err != nil {
		return fmt.Errorf("resize ConPTY: %w", err)
	}
	return nil
}

func (p *Process) Signal(name string) error {
	switch strings.ToLower(name) {
	case "ctrl-break", "break", "sigint", "interrupt":
		p.mu.Lock()
		closed := p.closed || p.job == 0
		p.mu.Unlock()
		if closed {
			return errProcessExited
		}
		if err := p.validateRootProcess(); err != nil {
			return err
		}
		if err := windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, p.pid); err != nil {
			return fmt.Errorf("send CTRL_BREAK_EVENT to owned Run: %w", err)
		}
		return nil
	case "terminate", "kill", "sigterm", "sigkill":
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.closed || p.job == 0 {
			return errProcessExited
		}
		if err := windows.TerminateJobObject(p.job, 1); err != nil {
			return fmt.Errorf("terminate owned Job Object: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported Windows signal %q", name)
	}
}

func (p *Process) Terminate(grace time.Duration) (backend.TerminationResult, error) {
	p.drainCompletionPort()
	p.mu.Lock()
	if p.closed || p.job == 0 {
		outcome := p.waitResult.Outcome
		p.mu.Unlock()
		if outcome == "" {
			outcome = "exited"
		}
		return backend.TerminationResult{Outcome: outcome, TreeEmpty: true}, nil
	}
	job := p.job
	active, err := activeProcessCount(job)
	if err != nil {
		p.mu.Unlock()
		return backend.TerminationResult{}, fmt.Errorf("observe owned Job Object before termination: %w", err)
	}
	if active == 0 {
		p.mu.Unlock()
		p.drainCompletionPort()
		return backend.TerminationResult{TreeEmpty: true, Outcome: p.terminationOutcome("exited")}, nil
	}
	p.mu.Unlock()
	result := backend.TerminationResult{Requested: true, Outcome: "forced-termination"}
	if grace < 0 {
		grace = 0
	}
	if err := p.validateRootProcess(); err == nil {
		_ = windows.GenerateConsoleCtrlEvent(windows.CTRL_BREAK_EVENT, p.pid)
	}
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		active, err := p.activeProcessCount()
		if err != nil {
			return result, fmt.Errorf("observe owned Job Object during graceful termination: %w", err)
		}
		if active == 0 {
			result.TreeEmpty = true
			p.drainCompletionPort()
			result.Outcome = p.terminationOutcome("cancelled")
			return result, nil
		}
		time.Sleep(min(25*time.Millisecond, time.Until(deadline)))
	}

	p.mu.Lock()
	job, closed := p.job, p.closed
	if closed || job == 0 {
		p.mu.Unlock()
		result.TreeEmpty = true
		result.Outcome = p.terminationOutcome("cancelled")
		return result, nil
	}
	if err := windows.TerminateJobObject(job, 1); err != nil {
		p.mu.Unlock()
		return result, fmt.Errorf("terminate owned Job Object: %w", err)
	}
	p.mu.Unlock()
	result.Forced = true
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		active, err := p.activeProcessCount()
		if err != nil {
			return result, fmt.Errorf("verify Job Object termination: %w", err)
		}
		if active == 0 {
			result.TreeEmpty = true
			p.drainCompletionPort()
			result.Outcome = p.terminationOutcome("forced-termination")
			return result, nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return result, errors.New("Job Object termination was requested but the owned process tree is still observed")
}

func (p *Process) Observe() (model.Resources, error) {
	p.drainCompletionPort()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hasFinalResources {
		return p.finalResources, nil
	}
	if p.closed || p.job == 0 {
		return unavailableResources(), errors.New("owned Job Object is closed before final observation")
	}
	return observeJob(p.job, &p.peakProcessCount)
}

func (p *Process) Wait() (backend.Exit, error) {
	p.waitOnce.Do(func() {
		p.waitErr = p.wait()
		close(p.waitDone)
	})
	<-p.waitDone
	return p.waitResult, p.waitErr
}

func (p *Process) wait() error {
	if _, err := windows.WaitForSingleObject(p.process, windows.INFINITE); err != nil {
		return fmt.Errorf("wait for initial Run process: %w", err)
	}
	for {
		active, err := activeProcessCount(p.job)
		if err != nil {
			return fmt.Errorf("observe process-tree completion: %w", err)
		}
		if active == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	p.mu.Lock()
	pty := p.pty
	p.pty = 0
	p.mu.Unlock()
	if pty != 0 {
		windows.ClosePseudoConsole(pty)
	}
	p.stdoutDone.Wait()
	p.drainCompletionPort()

	var code uint32
	if err := windows.GetExitCodeProcess(p.process, &code); err != nil {
		return fmt.Errorf("read Run exit code: %w", err)
	}
	result := backend.Exit{StartedAt: p.startedAt, FinishedAt: time.Now().UTC()}
	exitCode := int(int32(code))
	result.ExitCode = &exitCode
	result.Outcome = p.terminationOutcome("exited")
	resources, resourceErr := observeJob(p.job, &p.peakProcessCount)
	p.mu.Lock()
	p.waitResult = result
	p.finalResources = resources
	p.hasFinalResources = true
	p.closed = true
	input, process, job, completion := p.input, p.process, p.job, p.completion
	p.input, p.process, p.job, p.completion = 0, 0, 0, 0
	p.mu.Unlock()
	if input != 0 {
		_ = windows.CloseHandle(input)
	}
	_ = windows.CloseHandle(process)
	_ = windows.CloseHandle(job)
	if completion != 0 {
		_ = windows.CloseHandle(completion)
	}
	_ = resourceErr // unavailable counters remain explicit in finalResources.
	return nil
}

func (p *Process) recordCompletionMessage(message uint32, data *windows.Overlapped) {
	switch message {
	case completionActiveProcessLimit, completionProcessMemoryLimit, completionJobMemoryLimit:
		p.limitMessage.Store(message)
	case completionNewProcess:
		if data != nil {
			pid := uint32(uintptr(unsafe.Pointer(data)))
			p.mu.Lock()
			if _, exists := p.activePIDs[pid]; !exists {
				p.activePIDs[pid] = struct{}{}
				p.updatePeakProcessCount(int64(len(p.activePIDs)))
			}
			p.mu.Unlock()
		}
	case completionExitProcess, completionAbnormalExit:
		if data != nil {
			pid := uint32(uintptr(unsafe.Pointer(data)))
			p.mu.Lock()
			delete(p.activePIDs, pid)
			p.mu.Unlock()
		}
	case completionActiveProcessZero:
		p.mu.Lock()
		clear(p.activePIDs)
		p.mu.Unlock()
	}
}

func (p *Process) updatePeakProcessCount(count int64) {
	for {
		previous := p.peakProcessCount.Load()
		if count <= previous || p.peakProcessCount.CompareAndSwap(previous, count) {
			return
		}
	}
}

func (p *Process) drainCompletionPort() {
	p.completionMu.Lock()
	defer p.completionMu.Unlock()
	p.mu.Lock()
	port := p.completion
	p.mu.Unlock()
	if port == 0 {
		return
	}
	for {
		var message uint32
		var key uintptr
		var overlapped *windows.Overlapped
		err := windows.GetQueuedCompletionStatus(port, &message, &key, &overlapped, 0)
		if err == windows.WAIT_TIMEOUT || err == syscall.Errno(0) && overlapped == nil && message == 0 {
			return
		}
		if err != nil {
			return
		}
		p.recordCompletionMessage(message, overlapped)
	}
}

func (p *Process) terminationOutcome(fallback string) string {
	switch p.limitMessage.Load() {
	case completionProcessMemoryLimit, completionJobMemoryLimit:
		return "resource-limit:memory"
	case completionActiveProcessLimit:
		return "resource-limit:process-count"
	default:
		return fallback
	}
}

func (p *Process) startPump(dst io.Writer, source windows.Handle) {
	if source == 0 {
		return
	}
	p.stdoutDone.Add(1)
	go func() {
		defer p.stdoutDone.Done()
		defer windows.CloseHandle(source)
		buf := make([]byte, maxPipeRead)
		for {
			var n uint32
			err := windows.ReadFile(source, buf, &n, nil)
			if n > 0 && dst != nil {
				if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
					p.mu.Lock()
					if p.outputErr == nil {
						p.outputErr = writeErr
					}
					p.mu.Unlock()
					return
				}
			}
			if err != nil {
				if err == windows.ERROR_BROKEN_PIPE || err == windows.ERROR_OPERATION_ABORTED {
					return
				}
				p.mu.Lock()
				if p.outputErr == nil {
					p.outputErr = fmt.Errorf("read Run output: %w", err)
				}
				p.mu.Unlock()
				return
			}
			if n == 0 {
				return
			}
		}
	}()
}

func Reconcile(ownership model.Ownership) (backend.ReconcileResult, error) {
	uncertain := backend.ReconcileResult{State: model.Uncertain, OwnershipProven: false}
	if ownership.Backend != "windows-job" || ownership.PID <= 0 || ownership.StartTime == 0 {
		uncertain.Reason = "persisted Windows Job Object identity is incomplete"
		return uncertain, nil
	}
	name, err := windows.UTF16PtrFromString(jobName(uint32(ownership.PID), ownership.StartTime))
	if err != nil {
		uncertain.Reason = "persisted Windows Job Object identity cannot be encoded"
		return uncertain, nil
	}
	job, err := openJobObjectByName(jobQuery, name)
	if err != nil {
		uncertain.Reason = "the persisted Job Object cannot be reopened; process ownership and outcome are uncertain"
		return uncertain, nil
	}
	defer windows.CloseHandle(job)
	active, err := activeProcessCount(job)
	if err != nil {
		uncertain.Reason = "the reopened Job Object could not be observed; process ownership and outcome are uncertain"
		return uncertain, nil
	}
	resources, _ := observeJob(job, nil)
	if active == 0 {
		uncertain.OwnershipProven = true
		uncertain.Resources = resources
		uncertain.Reason = "the owned Job Object is empty but its terminal receipt was not persisted"
		return uncertain, nil
	}
	return backend.ReconcileResult{
		State: model.Running, Resources: resources, OwnershipProven: true,
		Reason: "the named Job Object remains open and contains live owned processes",
	}, nil
}

const jobQuery = 0x0004 // JOB_OBJECT_QUERY

func openJobObjectByName(access uint32, name *uint16) (windows.Handle, error) {
	if err := procOpenJobObjectW.Find(); err != nil {
		return 0, err
	}
	r, _, callErr := procOpenJobObjectW.Call(uintptr(access), 0, uintptr(unsafe.Pointer(name)))
	if r == 0 {
		if callErr != syscall.Errno(0) {
			return 0, callErr
		}
		return 0, windows.GetLastError()
	}
	return windows.Handle(r), nil
}

func observeJob(job windows.Handle, peak *atomic.Int64) (model.Resources, error) {
	resources := unavailableResources()
	var accounting jobAccountingInformation
	if err := windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&accounting)), uint32(unsafe.Sizeof(accounting)), nil); err != nil {
		return resources, fmt.Errorf("query Job Object accounting: %w", err)
	}
	count := int64(accounting.ActiveProcesses)
	if peak != nil {
		for {
			previous := peak.Load()
			if count <= previous || peak.CompareAndSwap(previous, count) {
				break
			}
		}
		if count < peak.Load() {
			count = peak.Load()
		}
	}
	resources.ProcessCount = measured(count)
	if peak == nil {
		resources.PeakProcessCount = unavailable()
	} else {
		resources.PeakProcessCount = measured(peak.Load())
	}
	user := accounting.TotalUserTime
	kernel := accounting.TotalKernelTime
	if user < 0 || kernel < 0 {
		resources.CPUTimeNs = unavailable()
	} else {
		resources.CPUTimeNs = measured((user + kernel) * 100)
	}
	var memory jobMemoryUsageInformation
	if err := windows.QueryInformationJobObject(job, jobObjectMemoryUsageInformation, uintptr(unsafe.Pointer(&memory)), uint32(unsafe.Sizeof(memory)), nil); err == nil {
		resources.MemoryBytes = measured(int64(memory.JobMemory))
		resources.PeakMemoryBytes = measured(int64(memory.PeakJobMemory))
	} else if err == windows.ERROR_INVALID_PARAMETER || err == windows.ERROR_NOT_SUPPORTED {
		resources.MemoryBytes = unsupported()
		resources.PeakMemoryBytes = unsupported()
	} else {
		resources.MemoryBytes = unavailable()
		resources.PeakMemoryBytes = unavailable()
	}
	return resources, nil
}

func activeProcessCount(job windows.Handle) (uint32, error) {
	var accounting jobAccountingInformation
	if err := windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation, uintptr(unsafe.Pointer(&accounting)), uint32(unsafe.Sizeof(accounting)), nil); err != nil {
		return 0, err
	}
	return accounting.ActiveProcesses, nil
}

func configureJob(job windows.Handle, limits model.Limits) error {
	var extended windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	flags := uint32(windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
	if limits.MemoryBytes > 0 {
		flags |= windows.JOB_OBJECT_LIMIT_JOB_MEMORY
		extended.JobMemoryLimit = uintptr(limits.MemoryBytes)
	}
	if limits.ProcessCount > 0 {
		flags |= windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS
		extended.BasicLimitInformation.ActiveProcessLimit = uint32(limits.ProcessCount)
	}
	extended.BasicLimitInformation.LimitFlags = flags
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&extended)), uint32(unsafe.Sizeof(extended))); err != nil {
		return err
	}
	if limits.CPUQuotaPercent > 0 {
		cpu := jobCPURateControlInformation{ControlFlags: jobCPUEnable | jobCPUHardCap, CPURate: uint32(limits.CPUQuotaPercent * 100)}
		if _, err := windows.SetInformationJobObject(job, windows.JobObjectCpuRateControlInformation, uintptr(unsafe.Pointer(&cpu)), uint32(unsafe.Sizeof(cpu))); err != nil {
			return err
		}
	}
	return nil
}

func createCompletionPort(job windows.Handle) (windows.Handle, error) {
	port, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, 1)
	if err != nil {
		return 0, err
	}
	association := jobAssociateCompletionPort{CompletionKey: uintptr(job), CompletionPort: port}
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectAssociateCompletionPortInformation, uintptr(unsafe.Pointer(&association)), uint32(unsafe.Sizeof(association))); err != nil {
		_ = windows.CloseHandle(port)
		return 0, err
	}
	return port, nil
}

func supportsCPURateControl() bool {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return false
	}
	defer windows.CloseHandle(job)
	cpu := jobCPURateControlInformation{ControlFlags: jobCPUEnable | jobCPUHardCap, CPURate: 10000}
	_, err = windows.SetInformationJobObject(job, windows.JobObjectCpuRateControlInformation, uintptr(unsafe.Pointer(&cpu)), uint32(unsafe.Sizeof(cpu)))
	return err == nil
}

func supportsJobMemoryTelemetry() bool {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return false
	}
	defer windows.CloseHandle(job)
	var memory jobMemoryUsageInformation
	err = windows.QueryInformationJobObject(job, jobObjectMemoryUsageInformation, uintptr(unsafe.Pointer(&memory)), uint32(unsafe.Sizeof(memory)), nil)
	return err == nil
}

func createUniqueJob(name string) (windows.Handle, error) {
	name16, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	procSetLastError.Call(0)
	job, err := windows.CreateJobObject(nil, name16)
	if err != nil {
		return 0, err
	}
	lastErr := windows.GetLastError()
	if lastErr == windows.ERROR_ALREADY_EXISTS {
		_ = windows.CloseHandle(job)
		return 0, errors.New("derived Job Object name already exists")
	}
	return job, nil
}

func (p *Process) activeProcessCount() (uint32, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.job == 0 {
		return 0, nil
	}
	return activeProcessCount(p.job)
}

func (p *Process) validateRootProcess() error {
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, p.pid)
	if err != nil {
		return fmt.Errorf("revalidate owned root process: %w", err)
	}
	defer windows.CloseHandle(process)
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(process, &creation, &exit, &kernel, &user); err != nil {
		return fmt.Errorf("revalidate owned root process identity: %w", err)
	}
	if uint64(creation.Nanoseconds()) != p.startTime {
		return errors.New("owned root PID no longer identifies the original Run process")
	}
	state, err := windows.WaitForSingleObject(process, 0)
	if err != nil {
		return fmt.Errorf("observe owned root process: %w", err)
	}
	if state == windows.WAIT_OBJECT_0 {
		return errors.New("owned root process has exited; a root-only control signal is no longer safe")
	}
	return nil
}

type preparedProcess struct {
	info        windows.ProcessInformation
	ownership   model.Ownership
	pty         windows.Handle
	input       windows.Handle
	stdout      windows.Handle
	stderr      windows.Handle
	retain      bool
	cleanupOnce sync.Once
}

func (prepared *preparedProcess) cleanup(cause error) error {
	prepared.cleanupOnce.Do(func() {
		if prepared.retain {
			return
		}
		if prepared.pty != 0 {
			windows.ClosePseudoConsole(prepared.pty)
		}
		closeHandles(prepared.input, prepared.stdout, prepared.stderr)
		if prepared.info.Process == 0 {
			return
		}
		state, waitErr := windows.WaitForSingleObject(prepared.info.Process, 0)
		if waitErr != nil {
			cause = &backend.UncertainError{Ownership: prepared.ownership, Err: errors.Join(cause, fmt.Errorf("observe suspended child before cleanup: %w", waitErr))}
			return
		}
		if state != windows.WAIT_OBJECT_0 {
			if err := windows.TerminateProcess(prepared.info.Process, 0xE0010001); err != nil {
				cause = &backend.UncertainError{Ownership: prepared.ownership, Err: errors.Join(cause, fmt.Errorf("terminate suspended child: %w", err))}
				return
			}
			state, waitErr = windows.WaitForSingleObject(prepared.info.Process, 5000)
			if waitErr != nil || state != windows.WAIT_OBJECT_0 {
				if waitErr == nil {
					waitErr = errors.New("suspended process did not become signaled after termination")
				}
				cause = &backend.UncertainError{Ownership: prepared.ownership, Err: errors.Join(cause, fmt.Errorf("verify suspended child cleanup: %w", waitErr))}
				return
			}
		}
		prepared.closeAfterClean()
	})
	return cause
}

func (prepared *preparedProcess) closeAfterClean() {
	if prepared.pty != 0 {
		windows.ClosePseudoConsole(prepared.pty)
		prepared.pty = 0
	}
	closeHandles(prepared.input, prepared.stdout, prepared.stderr, prepared.info.Process)
	prepared.input, prepared.stdout, prepared.stderr, prepared.info.Process = 0, 0, 0, 0
}

func createSuspended(spec model.RunSpec, appName, cmdLine, cwd *uint16, env []uint16) (*preparedProcess, error) {
	prepared := &preparedProcess{}
	var si windows.StartupInfoEx
	attrs, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		return nil, err
	}
	defer attrs.Delete()
	si.ProcThreadAttributeList = attrs.List()
	si.Cb = uint32(unsafe.Sizeof(si))
	var inherit bool
	var childHandles []windows.Handle
	if spec.Interactive {
		var inputRead, inputWrite, outputRead, outputWrite windows.Handle
		if err := makePipe(&inputRead, &inputWrite); err != nil {
			return nil, fmt.Errorf("create ConPTY input pipe: %w", err)
		}
		if err := makePipe(&outputRead, &outputWrite); err != nil {
			closeHandles(inputRead, inputWrite)
			return nil, fmt.Errorf("create ConPTY output pipe: %w", err)
		}
		if err := windows.SetHandleInformation(inputWrite, windows.HANDLE_FLAG_INHERIT, 0); err != nil {
			closeHandles(inputRead, inputWrite, outputRead, outputWrite)
			return nil, err
		}
		if err := windows.SetHandleInformation(outputRead, windows.HANDLE_FLAG_INHERIT, 0); err != nil {
			closeHandles(inputRead, inputWrite, outputRead, outputWrite)
			return nil, err
		}
		if err := windows.CreatePseudoConsole(windows.Coord{X: 80, Y: 24}, inputRead, outputWrite, 0, &prepared.pty); err != nil {
			closeHandles(inputRead, inputWrite, outputRead, outputWrite)
			return nil, fmt.Errorf("create ConPTY: %w", err)
		}
		_ = windows.CloseHandle(inputRead)
		_ = windows.CloseHandle(outputWrite)
		prepared.input, prepared.stdout = inputWrite, outputRead
		if err := attrs.Update(windows.PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, unsafe.Pointer(&prepared.pty), unsafe.Sizeof(prepared.pty)); err != nil {
			windows.ClosePseudoConsole(prepared.pty)
			closeHandles(prepared.input, prepared.stdout)
			return nil, err
		}
	} else {
		var stdinRead, stdinWrite, stdoutRead, stdoutWrite, stderrRead, stderrWrite windows.Handle
		if err := makePipe(&stdinRead, &stdinWrite); err != nil {
			return nil, fmt.Errorf("create stdin pipe: %w", err)
		}
		if err := makePipe(&stdoutRead, &stdoutWrite); err != nil {
			closeHandles(stdinRead, stdinWrite)
			return nil, fmt.Errorf("create stdout pipe: %w", err)
		}
		if err := makePipe(&stderrRead, &stderrWrite); err != nil {
			closeHandles(stdinRead, stdinWrite, stdoutRead, stdoutWrite)
			return nil, fmt.Errorf("create stderr pipe: %w", err)
		}
		if err := windows.SetHandleInformation(stdinWrite, windows.HANDLE_FLAG_INHERIT, 0); err != nil {
			closeHandles(stdinRead, stdinWrite, stdoutRead, stdoutWrite, stderrRead, stderrWrite)
			return nil, err
		}
		if err := windows.SetHandleInformation(stdoutRead, windows.HANDLE_FLAG_INHERIT, 0); err != nil {
			closeHandles(stdinRead, stdinWrite, stdoutRead, stdoutWrite, stderrRead, stderrWrite)
			return nil, err
		}
		if err := windows.SetHandleInformation(stderrRead, windows.HANDLE_FLAG_INHERIT, 0); err != nil {
			closeHandles(stdinRead, stdinWrite, stdoutRead, stdoutWrite, stderrRead, stderrWrite)
			return nil, err
		}
		childHandles = []windows.Handle{stdinRead, stdoutWrite, stderrWrite}
		if err := attrs.Update(windows.PROC_THREAD_ATTRIBUTE_HANDLE_LIST, unsafe.Pointer(&childHandles[0]), uintptr(len(childHandles))*unsafe.Sizeof(childHandles[0])); err != nil {
			closeHandles(stdinRead, stdinWrite, stdoutRead, stdoutWrite, stderrRead, stderrWrite)
			return nil, err
		}
		si.StartupInfo.Flags |= windows.STARTF_USESTDHANDLES
		si.StartupInfo.StdInput, si.StartupInfo.StdOutput, si.StartupInfo.StdErr = stdinRead, stdoutWrite, stderrWrite
		prepared.input, prepared.stdout, prepared.stderr = stdinWrite, stdoutRead, stderrRead
		inherit = true
	}

	flags := uint32(windows.CREATE_SUSPENDED | windows.CREATE_UNICODE_ENVIRONMENT | windows.EXTENDED_STARTUPINFO_PRESENT | windows.CREATE_NEW_PROCESS_GROUP)
	if err := windows.CreateProcess(appName, cmdLine, nil, nil, inherit, flags, &env[0], cwd, &si.StartupInfo, &prepared.info); err != nil {
		if prepared.pty != 0 {
			windows.ClosePseudoConsole(prepared.pty)
		}
		closeHandles(prepared.input, prepared.stdout, prepared.stderr)
		closeHandles(childHandles...)
		return nil, err
	}
	closeHandles(childHandles...)
	return prepared, nil
}

func makePipe(read, write *windows.Handle) error {
	security := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), InheritHandle: 1}
	return windows.CreatePipe(read, write, &security, maxPipeRead)
}

func cleanupAssigned(job windows.Handle, pi windows.ProcessInformation) error {
	if err := windows.TerminateJobObject(job, 0xE0010002); err != nil {
		return fmt.Errorf("terminate assigned Job Object: %w", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		active, err := activeProcessCount(job)
		if err != nil {
			return fmt.Errorf("observe assigned Job Object cleanup: %w", err)
		}
		if active == 0 {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("assigned Job Object remains non-empty after termination")
}

func jobName(pid uint32, startTime uint64) string {
	return fmt.Sprintf("Local\\Jinushi.Job.%d.%x", pid, startTime)
}

func resolveExecutable(name, cwd string, environment map[string]string) (string, error) {
	if strings.IndexByte(name, 0) >= 0 || name == "" {
		return "", errors.New("executable path is empty or contains NUL")
	}
	if hasPathSeparator(name) {
		candidate := name
		if !filepath.IsAbs(candidate) {
			candidate = filepath.Join(cwd, candidate)
		}
		return checkExecutable(candidate)
	}
	pathValue, ok := environmentValue(environment, "PATH")
	if !ok || pathValue == "" {
		return "", errors.New("executable must be absolute when the effective environment has no PATH")
	}
	extensions := []string{""}
	if filepath.Ext(name) == "" {
		extensions = []string{".exe", ".com"}
	} else if strings.EqualFold(filepath.Ext(name), ".bat") || strings.EqualFold(filepath.Ext(name), ".cmd") {
		return "", errors.New("batch files must be invoked through an explicitly supplied command interpreter")
	}
	for _, directory := range strings.Split(pathValue, ";") {
		if directory == "" {
			directory = cwd
		} else if !filepath.IsAbs(directory) {
			directory = filepath.Join(cwd, directory)
		}
		for _, extension := range extensions {
			candidate := filepath.Join(directory, name+extension)
			if path, err := checkExecutable(candidate); err == nil {
				return path, nil
			}
		}
	}
	return "", fmt.Errorf("executable %q was not found on the effective PATH", name)
}

func hasPathSeparator(path string) bool {
	return strings.ContainsAny(path, `\\/:`)
}

func checkExecutable(path string) (string, error) {
	if strings.EqualFold(filepath.Ext(path), ".bat") || strings.EqualFold(filepath.Ext(path), ".cmd") {
		return "", errors.New("batch files must be invoked through an explicitly supplied command interpreter")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", err
	}
	if info.IsDir() {
		return "", errors.New("executable path is a directory")
	}
	return abs, nil
}

func makeCommandLine(argv []string) (string, error) {
	var b strings.Builder
	for i, arg := range argv {
		if strings.IndexByte(arg, 0) >= 0 {
			return "", fmt.Errorf("argv[%d] contains NUL", i)
		}
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(quoteWindowsArgument(arg))
	}
	return b.String(), nil
}

// quoteWindowsArgument encodes one argv item using the Windows CRT parsing
// rules; it does not interpret or compose a shell command.
func quoteWindowsArgument(arg string) string {
	if arg != "" && !strings.ContainsAny(arg, " \t\n\v\"") {
		return arg
	}
	var b strings.Builder
	b.WriteByte('"')
	backslashes := 0
	for _, r := range arg {
		if r == '\\' {
			backslashes++
			continue
		}
		if r == '"' {
			b.WriteString(strings.Repeat("\\", backslashes*2+1))
			b.WriteRune(r)
			backslashes = 0
			continue
		}
		if backslashes > 0 {
			b.WriteString(strings.Repeat("\\", backslashes))
			backslashes = 0
		}
		b.WriteRune(r)
	}
	if backslashes > 0 {
		b.WriteString(strings.Repeat("\\", backslashes*2))
	}
	b.WriteByte('"')
	return b.String()
}

func environmentValues(environment model.Environment) (map[string]string, error) {
	vars := map[string]envEntry{}
	if environment.Mode == "" || environment.Mode == "inherit-supervisor" {
		for _, item := range os.Environ() {
			key, value, ok := splitEnvironmentEntry(item)
			if ok {
				vars[strings.ToUpper(key)] = envEntry{key: key, value: value}
			}
		}
	} else if environment.Mode != "replace" {
		return nil, fmt.Errorf("unsupported environment mode %q", environment.Mode)
	}
	for _, key := range environment.Unset {
		if strings.IndexByte(key, 0) >= 0 || strings.Contains(key, "=") {
			return nil, errors.New("environment unset contains an invalid name")
		}
		delete(vars, strings.ToUpper(key))
	}
	for key, value := range environment.Set {
		if key == "" || strings.IndexByte(key, 0) >= 0 || strings.Contains(key, "=") || strings.IndexByte(value, 0) >= 0 {
			return nil, errors.New("environment set contains an invalid name or value")
		}
		vars[strings.ToUpper(key)] = envEntry{key: key, value: value}
	}
	values := make(map[string]string, len(vars))
	for key, item := range vars {
		values[key] = item.value
	}
	return values, nil
}

func environmentBlock(values map[string]string) []uint16 {
	entries := make([]envEntry, 0, len(values))
	for key, value := range values {
		entries = append(entries, envEntry{key: key, value: value})
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := strings.ToUpper(entries[i].key), strings.ToUpper(entries[j].key)
		if a == b {
			return entries[i].key < entries[j].key
		}
		return a < b
	})
	var text strings.Builder
	for _, entry := range entries {
		text.WriteString(entry.key)
		text.WriteByte('=')
		text.WriteString(entry.value)
		text.WriteByte(0)
	}
	text.WriteByte(0)
	if len(entries) == 0 {
		text.WriteByte(0)
	}
	encoded := utf16.Encode([]rune(text.String()))
	return encoded
}

type envEntry struct{ key, value string }

func splitEnvironmentEntry(item string) (string, string, bool) {
	index := strings.IndexByte(item, '=')
	if index == 0 {
		relative := strings.IndexByte(item[1:], '=')
		if relative < 0 {
			return "", "", false
		}
		index = relative + 1
	}
	if index < 0 || index >= len(item) {
		return "", "", false
	}
	return item[:index], item[index+1:], true
}

func closeHandles(handles ...windows.Handle) {
	for _, handle := range handles {
		if handle != 0 {
			_ = windows.CloseHandle(handle)
		}
	}
}

func unavailableResources() model.Resources {
	return model.Resources{
		MemoryBytes: unsupported(), PeakMemoryBytes: unsupported(), CPUTimeNs: unavailable(),
		ProcessCount: unavailable(), PeakProcessCount: unavailable(),
	}
}

func measured(value int64) model.Metric { return model.Metric{Status: "measured", Value: value} }
func unavailable() model.Metric         { return model.Metric{Status: "unavailable"} }
func unsupported() model.Metric         { return model.Metric{Status: "unsupported"} }

func environmentValue(values map[string]string, key string) (string, bool) {
	value, ok := values[strings.ToUpper(key)]
	return value, ok
}
