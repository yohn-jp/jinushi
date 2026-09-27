//go:build linux

package linux

import (
	"bufio"
	"errors"
	"fmt"
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
	location   cgroupLocation
	name       string
	file       *os.File
	baseline   map[string]uint64
	cpuLimited bool
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
	data, err := os.ReadFile(filepath.Join(basePath, "cgroup.subtree_control"))
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
	err := unix.Access(basePath, unix.W_OK|unix.X_OK)
	return err == nil
}

func newCgroup(location cgroupLocation, name string, memoryBytes, cpuPercent, processCount int64) (*cgroup, error) {
	childPath := filepath.Join(location.basePath, name)
	childRel := childRelativePath(location, name)
	if err := os.Mkdir(childPath, 0700); err != nil {
		return nil, err
	}
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = os.Remove(childPath)
		}
	}()

	location.childPath = childPath
	location.childRel = filepath.Clean(childRel)
	cg := &cgroup{location: location, name: name, baseline: make(map[string]uint64)}
	writeLimit := func(controller, file, value string) error {
		if !cgroupControllerEnabled(location.basePath, controller) {
			return fmt.Errorf("cgroup v2 %s controller is not enabled for this delegated scope", controller)
		}
		if err := writeControl(filepath.Join(childPath, file), value); err != nil {
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
		cg.cpuLimited = true
	}
	if processCount > 0 {
		if err := writeLimit("pids", "pids.max", strconv.FormatInt(processCount, 10)); err != nil {
			return nil, err
		}
	}
	dir, err := os.Open(childPath)
	if err != nil {
		return nil, err
	}
	cg.file = dir
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
	_ = os.Remove(c.location.childPath)
}

func (c *cgroup) counterFile(file string) map[string]uint64 {
	return parseCounters(filepath.Join(c.location.childPath, file))
}

func (c *cgroup) nativeLimitOutcome() string {
	if c == nil {
		return ""
	}
	memory := c.counterFile("memory.events")
	if memory["oom_kill"] > c.baseline["memory.oom_kill"] || memory["oom"] > c.baseline["memory.oom"] || memory["max"] > c.baseline["memory.max"] {
		return "resource-limit:memory"
	}
	pids := c.counterFile("pids.events")
	if pids["max"] > c.baseline["pids.max"] {
		return "resource-limit:process-count"
	}
	if c.cpuLimited {
		cpu := c.counterFile("cpu.stat")
		if cpu["nr_throttled"] > c.baseline["cpu.nr_throttled"] {
			return "resource-limit:cpu-quota"
		}
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
	for key, value := range c.counterFile("cpu.stat") {
		out["cpu."+key] = value
	}
	return out
}

func parseCounters(path string) map[string]uint64 {
	out := make(map[string]uint64)
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
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
