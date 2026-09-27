//go:build linux

package linux

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type cgroupLocation struct {
	mountPoint  string
	mountRoot   string
	basePath    string
	baseRel     string
	childPath   string
	childRel    string
	memoryLimit bool
	cpuLimit    bool
	pidsLimit   bool
}

type cgroup struct {
	location cgroupLocation
	name     string
	file     *os.File
	parent   *os.File
	identity uint64
	baseline map[string]uint64
}

const cgroup2SuperMagic = 0x63677270

// openDirectoryNoSymlinks opens an absolute directory one component at a time.
// A path-based open can silently traverse a replaced symlink in a writable
// delegated hierarchy, so every component is opened relative to its already
// validated parent directory.
func openDirectoryNoSymlinks(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, '\x00') {
		return nil, errors.New("directory path must be a clean absolute path")
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if part == "." || part == ".." {
			_ = unix.Close(fd)
			return nil, errors.New("directory path contains an ambiguous component")
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), path), nil
}

func verifyCgroupDirectory(file *os.File) error {
	if file == nil {
		return errors.New("cgroup directory handle is unavailable")
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &fs); err != nil {
		return err
	}
	if uint64(fs.Type) != cgroup2SuperMagic {
		return errors.New("directory is not on a cgroup v2 filesystem")
	}
	return nil
}

func readFileAt(dir *os.File, name string) ([]byte, error) {
	if dir == nil || !isControlName(name) {
		return nil, errors.New("invalid cgroup control file")
	}
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	return io.ReadAll(file)
}

func writeFileAt(dir *os.File, name, value string) error {
	if dir == nil || !isControlName(name) {
		return errors.New("invalid cgroup control file")
	}
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	_, writeErr := io.WriteString(file, value)
	return errors.Join(writeErr, file.Close())
}

func isControlName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00")
}

func removeCgroupAt(parent *os.File, name string, identity uint64) error {
	if parent == nil || !isControlName(name) || identity == 0 {
		return errors.New("cgroup removal identity is incomplete")
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	var current unix.Stat_t
	statErr := unix.Fstat(fd, &current)
	_ = unix.Close(fd)
	if statErr != nil {
		return statErr
	}
	if current.Ino != identity {
		return errors.New("cgroup path was replaced before removal")
	}
	return unix.Unlinkat(int(parent.Fd()), name, unix.AT_REMOVEDIR)
}

func discoverCgroup() (cgroupLocation, error) {
	var selfPath string
	cgroupFile, err := os.Open("/proc/self/cgroup")
	if err != nil {
		return cgroupLocation{}, err
	}
	scanner := bufio.NewScanner(cgroupFile)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "0::") {
			selfPath = strings.TrimPrefix(line, "0::")
			break
		}
	}
	scanErr := scanner.Err()
	closeErr := cgroupFile.Close()
	if scanErr != nil {
		return cgroupLocation{}, scanErr
	}
	if closeErr != nil {
		return cgroupLocation{}, closeErr
	}
	if selfPath == "" || !filepath.IsAbs(selfPath) {
		return cgroupLocation{}, errors.New("unified cgroup path is unavailable")
	}

	mountFile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return cgroupLocation{}, err
	}
	defer mountFile.Close()
	scanner = bufio.NewScanner(mountFile)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for i, field := range fields {
			if field == "-" {
				separator = i
				break
			}
		}
		if separator < 6 || separator+2 >= len(fields) || fields[separator+1] != "cgroup2" {
			continue
		}
		root := unescapeMountField(fields[3])
		mountPoint := unescapeMountField(fields[4])
		if !pathWithin(root, selfPath) {
			continue
		}
		relative := strings.TrimPrefix(strings.TrimPrefix(selfPath, root), string(filepath.Separator))
		basePath := filepath.Join(mountPoint, filepath.FromSlash(relative))
		baseRel := filepath.Join(root, relative)
		if relative == "" {
			baseRel = root
		}
		return cgroupLocation{
			mountPoint: filepath.Clean(mountPoint),
			mountRoot:  filepath.Clean(root),
			basePath:   filepath.Clean(basePath),
			baseRel:    filepath.Clean(baseRel),
		}, nil
	}
	if err := scanner.Err(); err != nil {
		return cgroupLocation{}, err
	}
	return cgroupLocation{}, errors.New("cgroup v2 mount does not contain the current process cgroup")
}

func unescapeMountField(value string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(value)
}

func pathWithin(parent, child string) bool {
	parent = filepath.Clean(parent)
	child = filepath.Clean(child)
	if parent == string(filepath.Separator) {
		return filepath.IsAbs(child)
	}
	return child == parent || strings.HasPrefix(child, parent+string(filepath.Separator))
}

func childRelativePath(location cgroupLocation, name string) string {
	return filepath.Join(location.baseRel, name)
}

func cgroupControllerEnabled(basePath, name string) bool {
	base, err := openDirectoryNoSymlinks(basePath)
	if err != nil {
		return false
	}
	defer base.Close()
	if verifyCgroupDirectory(base) != nil {
		return false
	}
	data, err := readFileAt(base, "cgroup.subtree_control")
	if err != nil {
		return false
	}
	for _, controller := range strings.Fields(string(data)) {
		if controller == name {
			return true
		}
	}
	return false
}

func cgroupWritable(basePath string) bool {
	base, err := openDirectoryNoSymlinks(basePath)
	if err != nil {
		return false
	}
	defer base.Close()
	return verifyCgroupDirectory(base) == nil && unix.Faccessat(int(base.Fd()), ".", unix.W_OK|unix.X_OK, 0) == nil
}

func newCgroup(location cgroupLocation, name string, memoryBytes, cpuPercent, taskCount int64) (*cgroup, error) {
	parent, err := openDirectoryNoSymlinks(location.basePath)
	if err != nil {
		return nil, fmt.Errorf("open delegated cgroup parent safely: %w", err)
	}
	if err := verifyCgroupDirectory(parent); err != nil {
		_ = parent.Close()
		return nil, err
	}
	childPath := filepath.Join(location.basePath, name)
	childRel := childRelativePath(location, name)
	if !isControlName(name) || strings.Contains(name, string(filepath.Separator)) {
		_ = parent.Close()
		return nil, errors.New("invalid cgroup name")
	}
	if err := unix.Mkdirat(int(parent.Fd()), name, 0700); err != nil {
		_ = parent.Close()
		return nil, err
	}
	removeOnError := true
	var child *os.File
	var childIdentity uint64
	defer func() {
		if removeOnError {
			if child != nil {
				_ = child.Close()
			}
			if childIdentity != 0 {
				_ = removeCgroupAt(parent, name, childIdentity)
			}
			_ = parent.Close()
		}
	}()
	childFD, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	child = os.NewFile(uintptr(childFD), childPath)
	if err := verifyCgroupDirectory(child); err != nil {
		_ = child.Close()
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(childFD, &stat); err != nil {
		_ = child.Close()
		return nil, err
	}
	childIdentity = stat.Ino

	location.childPath = childPath
	location.childRel = filepath.Clean(childRel)
	cg := &cgroup{location: location, name: name, file: child, parent: parent, identity: stat.Ino, baseline: make(map[string]uint64)}
	writeLimit := func(controller, file, value string) error {
		data, readErr := readFileAt(parent, "cgroup.subtree_control")
		if readErr != nil || !strings.Contains(" "+strings.TrimSpace(string(data))+" ", " "+controller+" ") {
			return fmt.Errorf("cgroup v2 %s controller is not enabled for this delegated scope", controller)
		}
		if err := writeFileAt(cg.file, file, value); err != nil {
			return fmt.Errorf("set cgroup %s: %w", file, err)
		}
		return nil
	}
	if memoryBytes > 0 {
		if err := writeLimit("memory", "memory.max", strconv.FormatInt(memoryBytes, 10)); err != nil {
			return nil, err
		}
		if err := writeLimit("memory", "memory.oom.group", "1"); err != nil {
			return nil, err
		}
	}
	if cpuPercent > 0 {
		if cpuPercent > (int64(^uint64(0)>>1) / 1000) {
			return nil, errors.New("CPU quota is too large")
		}
		const period = int64(100000)
		quota := cpuPercent * period / 100
		if quota < 1000 {
			quota = 1000
		}
		if err := writeLimit("cpu", "cpu.max", fmt.Sprintf("%d %d", quota, period)); err != nil {
			return nil, err
		}
	}
	if taskCount > 0 {
		if err := writeLimit("pids", "pids.max", strconv.FormatInt(taskCount, 10)); err != nil {
			return nil, err
		}
	}
	cg.baseline = readLimitCounters(cg)
	removeOnError = false
	return cg, nil
}

func (c *cgroup) close() {
	if c == nil {
		return
	}
	if c.file != nil {
		_ = c.file.Close()
		c.file = nil
	}
	if c.parent != nil {
		_ = removeCgroupAt(c.parent, c.name, c.identity)
		_ = c.parent.Close()
		c.parent = nil
	}
}

func (c *cgroup) counterFile(file string) map[string]uint64 {
	if c.file != nil {
		data, err := readFileAt(c.file, file)
		if err != nil {
			return map[string]uint64{}
		}
		return parseCounterData(data)
	}
	return parseCounters(filepath.Join(c.location.childPath, file))
}

func (c *cgroup) nativeLimitOutcome() string {
	if c == nil {
		return ""
	}
	// cpu.max is a rate cap: nr_throttled records normal enforcement and does
	// not mean the Run exceeded a terminal budget or should be stopped.
	memory := c.counterFile("memory.events")
	if memory["oom_kill"] > c.baseline["memory.oom_kill"] || memory["oom"] > c.baseline["memory.oom"] || memory["max"] > c.baseline["memory.max"] {
		return "resource-limit:memory"
	}
	pids := c.counterFile("pids.events")
	if pids["max"] > c.baseline["pids.max"] {
		return "resource-limit:task-count"
	}
	return ""
}

func readLimitCounters(c *cgroup) map[string]uint64 {
	out := make(map[string]uint64)
	for key, value := range c.counterFile("memory.events") {
		out["memory."+key] = value
	}
	for key, value := range c.counterFile("pids.events") {
		out["pids."+key] = value
	}
	return out
}

func parseCounters(path string) map[string]uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return map[string]uint64{}
	}
	return parseCounterData(data)
}

func parseCounterData(data []byte) map[string]uint64 {
	out := make(map[string]uint64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err == nil {
			out[fields[0]] = value
		}
	}
	return out
}
