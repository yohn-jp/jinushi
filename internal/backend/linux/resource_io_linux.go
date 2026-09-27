//go:build linux

package linux

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/yohn-jp/jinushi/internal/model"
	"golang.org/x/sys/unix"
)

const (
	maxCgroupEvidenceFileBytes = 1 << 20
	maxCgroupEvidenceDevices   = 64
	maxPSIFileBytes            = 4096
)

func unavailableMetric() model.Metric {
	return model.Metric{Status: string(model.EvidenceUnavailable)}
}

func unsupportedMetric() model.Metric {
	return model.Metric{Status: string(model.EvidenceUnsupported)}
}

func measuredEvidenceMetric(value int64) model.Metric {
	return model.Metric{Status: string(model.EvidenceMeasured), Value: value}
}

func emptyIOMetrics(status model.Metric) model.IOMetrics {
	evidenceStatus := model.EvidenceUnavailable
	if status.Status == string(model.EvidenceUnsupported) {
		evidenceStatus = model.EvidenceUnsupported
	}
	return model.IOMetrics{
		ReadBytes:              status,
		WriteBytes:             status,
		ReadOperations:         status,
		WriteOperations:        status,
		Devices:                []model.DeviceIOMetrics{},
		DeviceEvidenceStatus:   evidenceStatus,
		DeviceEvidenceComplete: false,
	}
}

// observeCgroupIO reports cgroup-v2 I/O counters for this Run. A missing
// io.stat file is an unsupported kernel/controller capability; permission,
// ownership, and parse failures remain unavailable observations.
func observeCgroupIO(cg *cgroup) model.IOMetrics {
	if cg == nil {
		return emptyIOMetrics(unsupportedMetric())
	}
	if cg.file == nil {
		return emptyIOMetrics(unavailableMetric())
	}
	data, err := readEvidenceFileAt(cg.file, "io.stat", maxCgroupEvidenceFileBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return emptyIOMetrics(unsupportedMetric())
		}
		return emptyIOMetrics(unavailableMetric())
	}
	metrics, err := parseCgroupIOStat(data)
	if err != nil {
		return emptyIOMetrics(unavailableMetric())
	}
	return metrics
}

// readEvidenceFileAt reads at most limit bytes plus one byte used to detect
// truncation. It is deliberately separate from readFileAt because high-rate
// evidence files may grow with the number of devices or processes.
func readEvidenceFileAt(dir *os.File, name string, limit int64) ([]byte, error) {
	if dir == nil || !isControlName(name) || limit <= 0 {
		return nil, errors.New("invalid cgroup evidence file")
	}
	fd, err := openControlFileAt(dir, name)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("cgroup evidence file exceeds its bounded read limit")
	}
	return data, nil
}

func openControlFileAt(dir *os.File, name string) (int, error) {
	return unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
}

// parseCgroupIOStat aggregates only the counters with stable semantics while
// retaining a bounded number of per-device observations. Unknown future
// kernel fields are ignored. A successful empty file is a measured zero.
func parseCgroupIOStat(data []byte) (model.IOMetrics, error) {
	if len(data) > maxCgroupEvidenceFileBytes {
		return model.IOMetrics{}, errors.New("cgroup io.stat exceeds its bounded parse limit")
	}
	type counters struct {
		readBytes  uint64
		writeBytes uint64
		readOps    uint64
		writeOps   uint64
	}
	var total counters
	var totalComplete counters
	devices := make([]model.DeviceIOMetrics, 0, maxCgroupEvidenceDevices)
	seen := make(map[string]struct{})
	complete := true
	lineCount := 0
	recordCount := uint64(0)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 1024), maxCgroupEvidenceFileBytes)
	for scanner.Scan() {
		lineCount++
		if lineCount > 65536 {
			return model.IOMetrics{}, errors.New("cgroup io.stat has too many records")
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		major, minor, ok := strings.Cut(fields[0], ":")
		if !ok || !decimalUint(major) || !decimalUint(minor) {
			return model.IOMetrics{}, errors.New("cgroup io.stat has an invalid device identity")
		}
		device := fields[0]
		if _, duplicate := seen[device]; duplicate {
			return model.IOMetrics{}, fmt.Errorf("cgroup io.stat repeats device %q", device)
		}
		seen[device] = struct{}{}
		recordCount++
		values := make(map[string]uint64, 6)
		for _, field := range fields[1:] {
			key, raw, found := strings.Cut(field, "=")
			if !found || key == "" || raw == "" {
				return model.IOMetrics{}, errors.New("cgroup io.stat has a malformed counter")
			}
			switch key {
			case "rbytes", "wbytes", "rios", "wios":
				if _, duplicate := values[key]; duplicate {
					return model.IOMetrics{}, fmt.Errorf("cgroup io.stat repeats counter %q", key)
				}
				value, err := strconv.ParseUint(raw, 10, 64)
				if err != nil || value > math.MaxInt64 {
					return model.IOMetrics{}, fmt.Errorf("cgroup io.stat counter %q is outside the supported range", key)
				}
				values[key] = value
			default:
				// Kernel versions may add discard or flush counters. They do not
				// change the meaning of the four metrics exposed here.
			}
		}
		for _, counter := range []struct {
			name  string
			value *uint64
			seen  *uint64
		}{
			{name: "rbytes", value: &total.readBytes, seen: &totalComplete.readBytes},
			{name: "wbytes", value: &total.writeBytes, seen: &totalComplete.writeBytes},
			{name: "rios", value: &total.readOps, seen: &totalComplete.readOps},
			{name: "wios", value: &total.writeOps, seen: &totalComplete.writeOps},
		} {
			if value, ok := values[counter.name]; ok {
				updated, err := checkedEvidenceAdd(*counter.value, value)
				if err != nil {
					return model.IOMetrics{}, err
				}
				*counter.value = updated
				(*counter.seen)++
			}
		}
		if len(devices) < maxCgroupEvidenceDevices {
			devices = append(devices, model.DeviceIOMetrics{
				Device:          device,
				ReadBytes:       evidenceCounter(values, "rbytes"),
				WriteBytes:      evidenceCounter(values, "wbytes"),
				ReadOperations:  evidenceCounter(values, "rios"),
				WriteOperations: evidenceCounter(values, "wios"),
			})
		} else {
			complete = false
		}
	}
	if err := scanner.Err(); err != nil {
		return model.IOMetrics{}, err
	}
	statusFor := func(availableRows, totalRows, value uint64) model.Metric {
		if availableRows != totalRows {
			return unavailableMetric()
		}
		return measuredEvidenceMetric(int64(value))
	}
	return model.IOMetrics{
		ReadBytes:              statusFor(totalComplete.readBytes, recordCount, total.readBytes),
		WriteBytes:             statusFor(totalComplete.writeBytes, recordCount, total.writeBytes),
		ReadOperations:         statusFor(totalComplete.readOps, recordCount, total.readOps),
		WriteOperations:        statusFor(totalComplete.writeOps, recordCount, total.writeOps),
		Devices:                devices,
		DeviceEvidenceStatus:   evidenceCompletenessStatus(complete),
		DeviceEvidenceComplete: complete,
	}, nil
}

func evidenceCounter(counters map[string]uint64, name string) model.Metric {
	value, ok := counters[name]
	if !ok {
		return unsupportedMetric()
	}
	return measuredEvidenceMetric(int64(value))
}

func evidenceCompletenessStatus(complete bool) model.EvidenceStatus {
	if complete {
		return model.EvidenceMeasured
	}
	return model.EvidenceUnavailable
}

func decimalUint(value string) bool {
	if value == "" {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func checkedEvidenceAdd(left, right uint64) (uint64, error) {
	if right > math.MaxInt64-left {
		return 0, errors.New("cgroup evidence counter total overflows signed protocol range")
	}
	return left + right, nil
}

func observeCgroupPSI(cg *cgroup) model.PSIMetrics {
	if cg == nil {
		return unsupportedPSI()
	}
	return model.PSIMetrics{
		CPU:    readCgroupPressure(cg, "cpu.pressure"),
		Memory: readCgroupPressure(cg, "memory.pressure"),
		IO:     readCgroupPressure(cg, "io.pressure"),
	}
}

func unsupportedPSI() model.PSIMetrics {
	return model.PSIMetrics{
		CPU:    emptyPressure(string(model.EvidenceUnsupported)),
		Memory: emptyPressure(string(model.EvidenceUnsupported)),
		IO:     emptyPressure(string(model.EvidenceUnsupported)),
	}
}

func unavailablePSI() model.PSIMetrics {
	return model.PSIMetrics{
		CPU:    emptyPressure(string(model.EvidenceUnavailable)),
		Memory: emptyPressure(string(model.EvidenceUnavailable)),
		IO:     emptyPressure(string(model.EvidenceUnavailable)),
	}
}

func emptyPressure(status string) model.PressureMetrics {
	metric := model.Metric{Status: status}
	return model.PressureMetrics{
		SomeAvg10BasisPoints:  metric,
		SomeAvg60BasisPoints:  metric,
		SomeAvg300BasisPoints: metric,
		SomeTotalUS:           metric,
		FullAvg10BasisPoints:  metric,
		FullAvg60BasisPoints:  metric,
		FullAvg300BasisPoints: metric,
		FullTotalUS:           metric,
	}
}

func readCgroupPressure(cg *cgroup, name string) model.PressureMetrics {
	if cg == nil || cg.file == nil {
		return emptyPressure(string(model.EvidenceUnavailable))
	}
	data, err := readEvidenceFileAt(cg.file, name, maxPSIFileBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return emptyPressure(string(model.EvidenceUnsupported))
		}
		return emptyPressure(string(model.EvidenceUnavailable))
	}
	pressure, err := parsePSI(data)
	if err != nil {
		return emptyPressure(string(model.EvidenceUnavailable))
	}
	return pressure
}

func parsePSI(data []byte) (model.PressureMetrics, error) {
	if len(data) > maxPSIFileBytes {
		return model.PressureMetrics{}, errors.New("PSI file exceeds its bounded parse limit")
	}
	lines := make(map[string]model.PressureMetrics, 2)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 256), maxPSIFileBytes)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		kind := fields[0]
		if kind != "some" && kind != "full" {
			return model.PressureMetrics{}, fmt.Errorf("PSI has unknown pressure line %q", kind)
		}
		if _, duplicate := lines[kind]; duplicate {
			return model.PressureMetrics{}, fmt.Errorf("PSI repeats %q pressure line", kind)
		}
		values := make(map[string]model.Metric, 4)
		for _, field := range fields[1:] {
			key, raw, found := strings.Cut(field, "=")
			if !found || key == "" || raw == "" {
				return model.PressureMetrics{}, errors.New("PSI has a malformed value")
			}
			switch key {
			case "avg10", "avg60", "avg300":
				if _, duplicate := values[key]; duplicate {
					return model.PressureMetrics{}, fmt.Errorf("PSI repeats %q", key)
				}
				value, err := strconv.ParseFloat(raw, 64)
				if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value*100 > math.MaxInt64 {
					return model.PressureMetrics{}, fmt.Errorf("PSI %q is outside the supported range", key)
				}
				values[key] = measuredEvidenceMetric(int64(math.Round(value * 100)))
			case "total":
				if _, duplicate := values[key]; duplicate {
					return model.PressureMetrics{}, errors.New("PSI repeats total")
				}
				value, err := strconv.ParseUint(raw, 10, 64)
				if err != nil || value > math.MaxInt64 {
					return model.PressureMetrics{}, errors.New("PSI total is outside the supported range")
				}
				values[key] = measuredEvidenceMetric(int64(value))
			default:
				// Ignore new kernel fields that do not alter the current schema.
			}
		}
		for _, required := range []string{"avg10", "avg60", "avg300", "total"} {
			if _, ok := values[required]; !ok {
				return model.PressureMetrics{}, fmt.Errorf("PSI %q line omits %q", kind, required)
			}
		}
		line := emptyPressure(string(model.EvidenceUnsupported))
		if kind == "some" {
			line.SomeAvg10BasisPoints = values["avg10"]
			line.SomeAvg60BasisPoints = values["avg60"]
			line.SomeAvg300BasisPoints = values["avg300"]
			line.SomeTotalUS = values["total"]
		} else {
			line.FullAvg10BasisPoints = values["avg10"]
			line.FullAvg60BasisPoints = values["avg60"]
			line.FullAvg300BasisPoints = values["avg300"]
			line.FullTotalUS = values["total"]
		}
		lines[kind] = line
	}
	if err := scanner.Err(); err != nil {
		return model.PressureMetrics{}, err
	}
	if _, ok := lines["some"]; !ok {
		return model.PressureMetrics{}, errors.New("PSI lacks the required some line")
	}
	out := lines["some"]
	if full, ok := lines["full"]; ok {
		out.FullAvg10BasisPoints = full.FullAvg10BasisPoints
		out.FullAvg60BasisPoints = full.FullAvg60BasisPoints
		out.FullAvg300BasisPoints = full.FullAvg300BasisPoints
		out.FullTotalUS = full.FullTotalUS
	}
	return out, nil
}
