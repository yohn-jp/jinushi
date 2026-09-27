//go:build linux

package linux

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/yohn-jp/jinushi/internal/model"
	"golang.org/x/sys/unix"
)

const hostCgroupName = "jinushi-workload-v1"

var ErrHostEnvelopeAdmission = errors.New("Linux host safety envelope rejected Run admission")

// Host-envelope API types are shared with the runtime status protocol.
type HostEnvelopeConfig = model.HostEnvelopeConfig
type HostEnvelopeCapabilities = model.HostEnvelopeCapabilities
type HostPressure = model.HostPressure
type HostEnvelopeStatus = model.HostEnvelopeStatus

// HostAdmissionError is returned before process establishment when an
// observed aggregate physical ceiling has no remaining capacity, or when the
// configured ceiling cannot be observed safely.
type HostAdmissionError struct {
	Resource string
	Current  int64
	Limit    int64
	Reason   string
}

func (e *HostAdmissionError) Error() string {
	if e == nil {
		return ErrHostEnvelopeAdmission.Error()
	}
	if e.Reason != "" {
		return fmt.Sprintf("%s: %s admission unavailable: %s", ErrHostEnvelopeAdmission, e.Resource, e.Reason)
	}
	return fmt.Sprintf("%s: %s ceiling reached (%d of %d)", ErrHostEnvelopeAdmission, e.Resource, e.Current, e.Limit)
}

func (*HostAdmissionError) Unwrap() error { return ErrHostEnvelopeAdmission }

// HostAdmissionFailureCode lets the Guardian retain a machine-readable
// admission rejection when a competing physical start wins after preflight.
func (*HostAdmissionError) HostAdmissionFailureCode() string { return "host-envelope-admission" }

type hostCgroup struct {
	location cgroupLocation
	name     string
	file     *os.File
	parent   *os.File
	identity uint64
}

func (b *Backend) ConfigureHostEnvelope(config HostEnvelopeConfig) error {
	b.hostMu.Lock()
	defer b.hostMu.Unlock()
	if config.MemoryBytes < 0 || config.TaskCount < 0 || config.MaxActiveRuns < 0 {
		return errors.New("host envelope ceilings cannot be negative")
	}

	if config == (HostEnvelopeConfig{}) {
		if b.host != nil {
			return errors.New("cannot disable a shared host envelope on a live backend; restart with an empty configuration")
		}
		b.hostConfig = HostEnvelopeConfig{}
		b.hostConfigured = false
		b.hostFailure = ""
		return nil
	}
	var location cgroupLocation
	if b.host != nil {
		release, err := b.host.lockAdmission()
		if err != nil {
			return fmt.Errorf("serialize host envelope configuration: %w", err)
		}
		defer release()
		if err := b.host.validatePath(); err != nil {
			return fmt.Errorf("validate existing workload root: %w", err)
		}
		if config == b.hostConfig {
			return nil
		}
		return errors.New("host envelope ceilings are immutable after configuration; restart the backend to change them")
	} else {
		var err error
		location, err = discoverCgroup()
		if err != nil {
			b.hostConfigured = true
			b.hostConfig = config
			return b.failHostConfigurationLocked(config, fmt.Errorf("discover delegated cgroup v2 scope: %w", err))
		}
		release, lockErr := lockHostAdmission()
		if lockErr != nil {
			b.hostConfigured = true
			b.hostConfig = config
			return b.failHostConfigurationLocked(config, fmt.Errorf("serialize host envelope configuration: %w", lockErr))
		}
		defer release()
	}
	host, err := openHostCgroup(location, config)
	if err != nil {
		if b.host != nil {
			return fmt.Errorf("reconfigure Linux host envelope; previous configuration remains active: %w", err)
		}
		b.hostConfigured = true
		b.hostConfig = config
		return b.failHostConfigurationLocked(config, err)
	}
	if b.host != nil {
		b.host.close()
	}
	b.host = host
	b.hostConfig = config
	b.hostConfigured = true
	b.hostFailure = ""
	return nil
}

func (b *Backend) failHostConfigurationLocked(config HostEnvelopeConfig, err error) error {
	b.host = nil
	b.hostConfig = config
	b.hostConfigured = true
	b.hostFailure = err.Error()
	return fmt.Errorf("%w: %s", ErrUnsupported, err)
}

// HostEnvelopeStatus reports bounded aggregate usage and pressure. When the
// envelope was requested but cgroup delegation is unavailable, all affected
// observations are explicitly unsupported and the reason is included.
func (b *Backend) HostEnvelopeStatus() HostEnvelopeStatus {
	b.hostMu.Lock()
	defer b.hostMu.Unlock()
	if !b.hostConfigured {
		return unavailableHostStatus("unconfigured", "host envelope is not configured", HostEnvelopeConfig{})
	}
	if b.host == nil {
		return unavailableHostStatus("unsupported", b.hostFailure, b.hostConfig)
	}
	return b.host.status(b.hostConfig)
}

// ValidateHostAdmission performs a non-reserving preflight for supervisor
// feedback. Start repeats the same physical checks while holding the shared
// admission lock, which is the authority across independent Guardian backends.
func (b *Backend) ValidateHostAdmission() error {
	b.hostMu.Lock()
	defer b.hostMu.Unlock()
	if !b.hostConfigured {
		return nil
	}
	if b.host == nil {
		return fmt.Errorf("%w: configured Linux host envelope is unavailable: %s", ErrUnsupported, b.hostFailure)
	}
	release, err := b.host.lockAdmission()
	if err != nil {
		return &HostAdmissionError{Resource: "workload-root", Limit: 1, Reason: err.Error()}
	}
	defer release()
	return b.host.checkAdmission(b.hostConfig)
}

func unavailableHostStatus(status, reason string, config HostEnvelopeConfig) HostEnvelopeStatus {
	unsupported := model.Metric{Status: "unsupported"}
	pressure := unsupportedHostPressure()
	return HostEnvelopeStatus{
		Status:         status,
		Config:         config,
		ActiveRuns:     unsupported,
		MemoryBytes:    unsupported,
		TaskCount:      unsupported,
		MemoryPressure: pressure,
		CPUPressure:    pressure,
		IOPressure:     pressure,
		Reason:         reason,
	}
}

func (b *Backend) runCgroupLocationLocked() (cgroupLocation, error) {
	if b.host == nil {
		return discoverCgroup()
	}
	if err := b.host.validatePath(); err != nil {
		return cgroupLocation{}, fmt.Errorf("validate workload cgroup root: %w", err)
	}
	return b.host.runLocation(), nil
}

func (b *Backend) probeRunCgroupLocked() (cgroupLocation, bool) {
	if b.hostConfigured && b.host == nil {
		return cgroupLocation{}, false
	}
	if b.host == nil {
		return probeCgroup()
	}
	if b.host.validatePath() != nil {
		return cgroupLocation{}, false
	}
	return probeCgroupAt(b.host.runLocation())
}

func (b *Backend) newRunCgroupLocked(location cgroupLocation, name string, memoryBytes, cpuPercent, taskCount int64) (*cgroup, error) {
	if b.host != nil {
		return newCgroupUnder(location, name, memoryBytes, cpuPercent, taskCount, b.host.file)
	}
	return newCgroup(location, name, memoryBytes, cpuPercent, taskCount)
}

func openHostCgroup(location cgroupLocation, config HostEnvelopeConfig) (*hostCgroup, error) {
	parent, err := openDirectoryNoSymlinks(location.basePath)
	if err != nil {
		return nil, fmt.Errorf("open delegated cgroup parent safely: %w", err)
	}
	if err := verifyCgroupDirectory(parent); err != nil {
		_ = parent.Close()
		return nil, err
	}
	if unix.Faccessat(int(parent.Fd()), ".", unix.W_OK|unix.X_OK, 0) != nil {
		_ = parent.Close()
		return nil, errors.New("delegated cgroup parent is not writable")
	}

	created := false
	if err := unix.Mkdirat(int(parent.Fd()), hostCgroupName, 0700); err == nil {
		created = true
	} else if !errors.Is(err, unix.EEXIST) {
		_ = parent.Close()
		return nil, fmt.Errorf("create Jinushi workload cgroup: %w", err)
	}
	rootPath := filepath.Join(location.basePath, hostCgroupName)
	rootFD, err := unix.Openat(int(parent.Fd()), hostCgroupName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		_ = parent.Close()
		return nil, fmt.Errorf("open Jinushi workload cgroup without following links: %w", err)
	}
	root := os.NewFile(uintptr(rootFD), rootPath)
	if err := verifyCgroupDirectory(root); err != nil {
		var identity unix.Stat_t
		identityErr := unix.Fstat(rootFD, &identity)
		_ = root.Close()
		if created {
			if identityErr == nil {
				_ = removeCgroupAt(parent, hostCgroupName, identity.Ino)
			}
		}
		_ = parent.Close()
		return nil, err
	}
	var identity unix.Stat_t
	if err := unix.Fstat(rootFD, &identity); err != nil {
		_ = root.Close()
		_ = parent.Close()
		return nil, err
	}
	host := &hostCgroup{
		location: location,
		name:     hostCgroupName,
		file:     root,
		parent:   parent,
		identity: identity.Ino,
	}
	fail := func(err error) (*hostCgroup, error) {
		if created {
			_ = removeCgroupAt(parent, hostCgroupName, identity.Ino)
		}
		host.close()
		return nil, err
	}

	if err := host.ensureNoInternalProcesses(); err != nil {
		return fail(fmt.Errorf("workload root must remain outside the control plane: %w", err))
	}
	active, err := host.activeRuns()
	if err != nil {
		return fail(fmt.Errorf("observe existing workload children: %w", err))
	}
	if active > 0 {
		if config.MaxActiveRuns > 0 && active > uint64(config.MaxActiveRuns) {
			return fail(errors.New("existing active Runs exceed the configured active Run ceiling"))
		}
		if !host.limitsMatch(config) {
			return fail(errors.New("existing workload Runs prevent changing host envelope ceilings"))
		}
	}
	if err := host.enableRunControllers(config); err != nil {
		return fail(err)
	}
	if config.MemoryBytes > 0 && !fileWritableAt(host.file, "memory.max") {
		return fail(fmt.Errorf("%w: aggregate memory enforcement requires delegated cgroup v2 memory controller", ErrUnsupported))
	}
	if config.TaskCount > 0 && !fileWritableAt(host.file, "pids.max") {
		return fail(fmt.Errorf("%w: aggregate task enforcement requires delegated cgroup v2 pids controller", ErrUnsupported))
	}
	if config.MemoryBytes > 0 {
		if err := host.setLimit("memory.max", strconv.FormatInt(config.MemoryBytes, 10)); err != nil {
			return fail(fmt.Errorf("set aggregate workload memory ceiling: %w", err))
		}
	} else if fileWritableAt(host.file, "memory.max") {
		if err := host.setLimit("memory.max", "max"); err != nil {
			return fail(fmt.Errorf("clear aggregate workload memory ceiling: %w", err))
		}
	}
	if config.TaskCount > 0 {
		if err := host.setLimit("pids.max", strconv.FormatInt(config.TaskCount, 10)); err != nil {
			return fail(fmt.Errorf("set aggregate workload task ceiling: %w", err))
		}
	} else if fileWritableAt(host.file, "pids.max") {
		if err := host.setLimit("pids.max", "max"); err != nil {
			return fail(fmt.Errorf("clear aggregate workload task ceiling: %w", err))
		}
	}
	return host, nil
}

func (h *hostCgroup) close() {
	if h == nil {
		return
	}
	if h.file != nil {
		_ = h.file.Close()
		h.file = nil
	}
	if h.parent != nil {
		_ = h.parent.Close()
		h.parent = nil
	}
}

// lockAdmission serializes physical admission across independently created
// Guardian backends. flock is released by the kernel if a Guardian exits while
// starting a Run.
func (h *hostCgroup) lockAdmission() (func(), error) {
	if h == nil {
		return nil, errors.New("workload root is unavailable")
	}
	return lockHostAdmission()
}

func lockHostAdmission() (func(), error) {
	// One lock file per Unix user keeps Guardian processes synchronized while
	// avoiding an unbounded trail of lock files for transient cgroup scopes.
	lockName := fmt.Sprintf("jinushi-host-admission-%d.lock", unix.Geteuid())
	directory, err := openDirectoryNoSymlinks(filepath.Clean(os.TempDir()))
	if err != nil {
		return nil, fmt.Errorf("open host admission lock directory: %w", err)
	}
	defer directory.Close()
	var directoryStat unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &directoryStat); err != nil {
		return nil, err
	}
	if directoryStat.Mode&unix.S_IFMT != unix.S_IFDIR || directoryStat.Mode&0022 != 0 && directoryStat.Mode&01000 == 0 {
		return nil, errors.New("host admission lock directory is writable by other users without sticky protection")
	}
	fd, err := unix.Openat(int(directory.Fd()), lockName, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, fmt.Errorf("open host admission lock safely: %w", err)
	}
	file := os.NewFile(uintptr(fd), lockName)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = file.Close()
		return nil, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != uint32(unix.Geteuid()) || stat.Mode&0077 != 0 || stat.Nlink != 1 {
		_ = file.Close()
		return nil, errors.New("host admission lock file ownership or permissions are unsafe")
	}
	for {
		err = unix.Flock(fd, unix.LOCK_EX)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		break
	}
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("acquire host admission lock: %w", err)
	}
	return func() {
		_ = unix.Flock(fd, unix.LOCK_UN)
		_ = file.Close()
	}, nil
}

func (h *hostCgroup) runLocation() cgroupLocation {
	location := h.location
	location.basePath = h.location.childPath
	location.baseRel = h.location.childRel
	return location
}

func (h *hostCgroup) validatePath() error {
	if h == nil || h.file == nil || h.identity == 0 {
		return errors.New("workload root ownership is incomplete")
	}
	if err := verifyCgroupDirectory(h.file); err != nil {
		return err
	}
	var fdIdentity unix.Stat_t
	if err := unix.Fstat(int(h.file.Fd()), &fdIdentity); err != nil {
		return err
	}
	if fdIdentity.Ino != h.identity {
		return errors.New("workload root handle identity changed")
	}
	path, err := openDirectoryNoSymlinks(h.location.childPath)
	if err != nil {
		return fmt.Errorf("reopen workload root path: %w", err)
	}
	defer path.Close()
	var pathIdentity unix.Stat_t
	if err := unix.Fstat(int(path.Fd()), &pathIdentity); err != nil {
		return err
	}
	if pathIdentity.Ino != h.identity {
		return errors.New("workload root path was replaced")
	}
	return nil
}

func (h *hostCgroup) ensureNoInternalProcesses() error {
	data, err := readFileAt(h.file, "cgroup.procs")
	if err != nil {
		return err
	}
	if len(strings.Fields(string(data))) != 0 {
		return errors.New("workload root contains a process")
	}
	return nil
}

func (h *hostCgroup) enableRunControllers(config HostEnvelopeConfig) error {
	required := config.MemoryBytes > 0 || config.TaskCount > 0
	data, err := readFileAt(h.file, "cgroup.controllers")
	if err != nil {
		if !required {
			return nil
		}
		return fmt.Errorf("read delegated workload controllers: %w", err)
	}
	available := make(map[string]bool)
	for _, controller := range strings.Fields(string(data)) {
		available[controller] = true
	}
	currentData, err := readFileAt(h.file, "cgroup.subtree_control")
	if err != nil {
		if !required {
			return nil
		}
		return fmt.Errorf("read workload subtree controllers: %w", err)
	}
	current := make(map[string]bool)
	for _, controller := range strings.Fields(string(currentData)) {
		current[controller] = true
	}
	for _, controller := range []string{"cpu", "memory", "pids"} {
		if !available[controller] || current[controller] {
			continue
		}
		if err := writeFileAt(h.file, "cgroup.subtree_control", "+"+controller); err != nil {
			controllerRequired := controller == "memory" && config.MemoryBytes > 0 || controller == "pids" && config.TaskCount > 0
			if controllerRequired {
				return fmt.Errorf("enable delegated workload child %s controller: %w", controller, err)
			}
		}
	}
	return nil
}

func (h *hostCgroup) limitsMatch(config HostEnvelopeConfig) bool {
	return h.limitMatches("memory.max", config.MemoryBytes) && h.limitMatches("pids.max", config.TaskCount)
}

func (h *hostCgroup) limitMatches(file string, requested int64) bool {
	data, err := readFileAt(h.file, file)
	if err != nil {
		return requested == 0
	}
	value := strings.TrimSpace(string(data))
	if requested == 0 {
		return value == "max"
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return err == nil && parsed == requested
}

func (h *hostCgroup) setLimit(file, value string) error {
	if err := h.validatePath(); err != nil {
		return err
	}
	return writeFileAt(h.file, file, value)
}

func (h *hostCgroup) checkAdmission(config HostEnvelopeConfig) error {
	if err := h.validatePath(); err != nil {
		return &HostAdmissionError{Resource: "workload-root", Limit: 1, Reason: err.Error()}
	}
	if config.MaxActiveRuns > 0 {
		current, err := h.activeRuns()
		if err != nil {
			return &HostAdmissionError{Resource: "active-runs", Limit: config.MaxActiveRuns, Reason: err.Error()}
		}
		if current >= uint64(config.MaxActiveRuns) {
			return &HostAdmissionError{Resource: "active-runs", Current: safeInt64(current), Limit: config.MaxActiveRuns}
		}
	}
	if config.MemoryBytes > 0 {
		current, err := h.counter("memory.current")
		if err != nil {
			return &HostAdmissionError{Resource: "memory-bytes", Limit: config.MemoryBytes, Reason: err.Error()}
		}
		if current >= uint64(config.MemoryBytes) {
			return &HostAdmissionError{Resource: "memory-bytes", Current: safeInt64(current), Limit: config.MemoryBytes}
		}
	}
	if config.TaskCount > 0 {
		current, err := h.counter("pids.current")
		if err != nil {
			return &HostAdmissionError{Resource: "task-count", Limit: config.TaskCount, Reason: err.Error()}
		}
		if current >= uint64(config.TaskCount) {
			return &HostAdmissionError{Resource: "task-count", Current: safeInt64(current), Limit: config.TaskCount}
		}
	}
	return nil
}

func (h *hostCgroup) status(config HostEnvelopeConfig) HostEnvelopeStatus {
	status := HostEnvelopeStatus{Status: "enforced", Config: config}
	rootValid := h.validatePath() == nil
	if !rootValid {
		status.Status = "degraded"
		status.Reason = "workload root path identity cannot be revalidated"
	}
	active, activeErr := h.activeRuns()
	if activeErr != nil {
		status.ActiveRuns = model.Metric{Status: "unavailable"}
		status.Status = "degraded"
		status.Reason = activeErr.Error()
	} else {
		status.ActiveRuns = measuredMetric(safeInt64(active))
	}
	status.MemoryBytes = h.metric("memory.current")
	status.TaskCount = h.metric("pids.current")
	if status.MemoryBytes.Status == "unavailable" || status.TaskCount.Status == "unavailable" {
		status.Status = "degraded"
	}
	status.MemoryPressure = h.pressure("memory.pressure")
	status.CPUPressure = h.pressure("cpu.pressure")
	status.IOPressure = h.pressure("io.pressure")
	pressureSupported := status.MemoryPressure.Status == "measured" || status.CPUPressure.Status == "measured" || status.IOPressure.Status == "measured"
	status.Capabilities = HostEnvelopeCapabilities{
		WorkloadRoot:         rootValid,
		MemoryEnforcement:    rootValid && config.MemoryBytes > 0 && h.limitMatches("memory.max", config.MemoryBytes),
		TaskCountEnforcement: rootValid && config.TaskCount > 0 && h.limitMatches("pids.max", config.TaskCount),
		ActiveRunEnforcement: rootValid && config.MaxActiveRuns > 0,
		MemoryTelemetry:      status.MemoryBytes.Status == "measured",
		TaskTelemetry:        status.TaskCount.Status == "measured",
		PressureTelemetry:    pressureSupported,
	}
	if config.MemoryBytes > 0 && !status.Capabilities.MemoryEnforcement || config.TaskCount > 0 && !status.Capabilities.TaskCountEnforcement {
		status.Status = "degraded"
		if status.Reason == "" {
			status.Reason = "configured host cgroup ceiling no longer matches its enforced value"
		}
	}
	return status
}

func (h *hostCgroup) activeRuns() (uint64, error) {
	if h == nil || h.file == nil {
		return 0, errors.New("workload root handle is unavailable")
	}
	fd, err := unix.Openat(int(h.file.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return 0, err
	}
	dir := os.NewFile(uintptr(fd), "workload-root")
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return 0, err
	}
	var active uint64
	for _, name := range names {
		childFD, openErr := unix.Openat(int(h.file.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(openErr, unix.ENOTDIR) || errors.Is(openErr, unix.ENOENT) {
			continue
		}
		if openErr != nil {
			return 0, fmt.Errorf("open workload child %q: %w", name, openErr)
		}
		child := os.NewFile(uintptr(childFD), name)
		if verifyErr := verifyCgroupDirectory(child); verifyErr != nil {
			_ = child.Close()
			return 0, fmt.Errorf("verify workload child %q: %w", name, verifyErr)
		}
		data, readErr := readFileAt(child, "cgroup.events")
		_ = child.Close()
		if readErr != nil {
			return 0, fmt.Errorf("read workload child %q population: %w", name, readErr)
		}
		populated, found, parseErr := parseCgroupPopulated(data)
		if parseErr != nil || !found {
			if parseErr == nil {
				parseErr = errors.New("cgroup.events has no populated field")
			}
			return 0, fmt.Errorf("parse workload child %q population: %w", name, parseErr)
		}
		if populated {
			active++
		}
	}
	return active, nil
}

func parseCgroupPopulated(data []byte) (bool, bool, error) {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "populated" {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 1)
		if err != nil || value > 1 {
			return false, true, errors.New("invalid cgroup populated value")
		}
		return value == 1, true, nil
	}
	return false, false, nil
}

func (h *hostCgroup) counter(file string) (uint64, error) {
	data, err := readFileAt(h.file, file)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", file, err)
	}
	return value, nil
}

func (h *hostCgroup) metric(file string) model.Metric {
	value, err := h.counter(file)
	if err == nil {
		if value > math.MaxInt64 {
			return model.Metric{Status: "unavailable"}
		}
		return measuredMetric(int64(value))
	}
	return metricForObservationError(err)
}

func metricForObservationError(err error) model.Metric {
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENODEV) || errors.Is(err, unix.EOPNOTSUPP) {
		return model.Metric{Status: "unsupported"}
	}
	return model.Metric{Status: "unavailable"}
}

func safeInt64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

func (h *hostCgroup) pressure(file string) HostPressure {
	data, err := readFileAt(h.file, file)
	if err != nil {
		metric := metricForObservationError(err)
		return HostPressure{
			Status:                 metric.Status,
			SomeAvg10MilliPercent:  metric,
			SomeAvg60MilliPercent:  metric,
			SomeAvg300MilliPercent: metric,
			SomeTotalUsec:          metric,
			FullAvg10MilliPercent:  metric,
			FullAvg60MilliPercent:  metric,
			FullAvg300MilliPercent: metric,
			FullTotalUsec:          metric,
		}
	}
	return parseHostPressure(data)
}

func unsupportedHostPressure() HostPressure {
	metric := model.Metric{Status: "unsupported"}
	return HostPressure{
		Status:                 "unsupported",
		SomeAvg10MilliPercent:  metric,
		SomeAvg60MilliPercent:  metric,
		SomeAvg300MilliPercent: metric,
		SomeTotalUsec:          metric,
		FullAvg10MilliPercent:  metric,
		FullAvg60MilliPercent:  metric,
		FullAvg300MilliPercent: metric,
		FullTotalUsec:          metric,
	}
}

func parseHostPressure(data []byte) HostPressure {
	pressure := HostPressure{
		Status:                 "unavailable",
		SomeAvg10MilliPercent:  model.Metric{Status: "unavailable"},
		SomeAvg60MilliPercent:  model.Metric{Status: "unavailable"},
		SomeAvg300MilliPercent: model.Metric{Status: "unavailable"},
		SomeTotalUsec:          model.Metric{Status: "unavailable"},
		FullAvg10MilliPercent:  model.Metric{Status: "unsupported"},
		FullAvg60MilliPercent:  model.Metric{Status: "unsupported"},
		FullAvg300MilliPercent: model.Metric{Status: "unsupported"},
		FullTotalUsec:          model.Metric{Status: "unsupported"},
	}
	rows := make(map[string]map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || (fields[0] != "some" && fields[0] != "full") {
			continue
		}
		values := make(map[string]string)
		for _, field := range fields[1:] {
			key, value, ok := strings.Cut(field, "=")
			if ok {
				values[key] = value
			}
		}
		rows[fields[0]] = values
	}
	if row, ok := rows["some"]; ok {
		pressure.SomeAvg10MilliPercent = parsePressureAverage(row["avg10"])
		pressure.SomeAvg60MilliPercent = parsePressureAverage(row["avg60"])
		pressure.SomeAvg300MilliPercent = parsePressureAverage(row["avg300"])
		pressure.SomeTotalUsec = parsePressureTotal(row["total"])
	}
	if row, ok := rows["full"]; ok {
		pressure.FullAvg10MilliPercent = parsePressureAverage(row["avg10"])
		pressure.FullAvg60MilliPercent = parsePressureAverage(row["avg60"])
		pressure.FullAvg300MilliPercent = parsePressureAverage(row["avg300"])
		pressure.FullTotalUsec = parsePressureTotal(row["total"])
	}
	for _, metric := range []model.Metric{pressure.SomeAvg10MilliPercent, pressure.SomeAvg60MilliPercent, pressure.SomeAvg300MilliPercent, pressure.SomeTotalUsec, pressure.FullAvg10MilliPercent, pressure.FullAvg60MilliPercent, pressure.FullAvg300MilliPercent, pressure.FullTotalUsec} {
		if metric.Status == "measured" {
			pressure.Status = "measured"
			return pressure
		}
	}
	if pressure.SomeAvg10MilliPercent.Status == "unsupported" {
		pressure.Status = "unsupported"
	}
	return pressure
}

func parsePressureAverage(value string) model.Metric {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 || parsed > 100 {
		return model.Metric{Status: "unavailable"}
	}
	scaled := math.Round(parsed * 1000)
	return measuredMetric(int64(scaled))
}

func parsePressureTotal(value string) model.Metric {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed > math.MaxInt64 {
		return model.Metric{Status: "unavailable"}
	}
	return measuredMetric(int64(parsed))
}
