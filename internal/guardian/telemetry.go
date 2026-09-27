package guardian

import (
	"encoding/json"
	"time"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

const (
	maxGuardianTelemetryBytes     = 512 << 10
	maxGuardianTelemetryProcesses = 1024
	maxGuardianTelemetryChanges   = 256
	maxGuardianTelemetryDevices   = 64
)

type processEvidenceSource interface {
	Evidence() (model.TelemetrySample, error)
}

func initialActivity(interactive bool) model.IOActivity {
	inputStatus := string(model.EvidenceMeasured)
	resizeStatus := string(model.EvidenceUnsupported)
	if interactive {
		resizeStatus = string(model.EvidenceMeasured)
	}
	return model.IOActivity{
		InputBytes:  model.Metric{Status: inputStatus},
		InputWrites: model.Metric{Status: inputStatus},
		ResizeCount: model.Metric{Status: resizeStatus},
	}
}

func normalizedActivity(activity model.IOActivity, interactive bool) model.IOActivity {
	defaults := initialActivity(interactive)
	if activity.InputBytes.Status == "" {
		activity.InputBytes = defaults.InputBytes
	}
	if activity.InputWrites.Status == "" {
		activity.InputWrites = defaults.InputWrites
	}
	if activity.ResizeCount.Status == "" {
		activity.ResizeCount = defaults.ResizeCount
	}
	return cloneActivity(activity)
}

func observeTelemetry(process backend.Process, runID string, capabilities *model.Capabilities, activity model.IOActivity, intervalMs int64, observedAt time.Time) (model.TelemetrySample, error) {
	var sample model.TelemetrySample
	var err error
	if source, ok := process.(processEvidenceSource); ok {
		sample, err = source.Evidence()
	} else {
		var resources model.Resources
		resources, err = process.Observe()
		sample = unsupportedTelemetry(resources, capabilities, activity)
	}
	sample = boundTelemetrySample(sample, runID, capabilities, activity, intervalMs, observedAt)
	return sample, err
}

func unsupportedTelemetry(resources model.Resources, capabilities *model.Capabilities, activity model.IOActivity) model.TelemetrySample {
	unsupported := func() model.Metric { return model.Metric{Status: string(model.EvidenceUnsupported)} }
	status := model.EvidenceUnsupported
	reason := "process-evidence-unsupported"
	if capabilities != nil && capabilities.ProcessTelemetry {
		status = model.EvidenceUnavailable
		reason = "process-evidence-unavailable"
	}
	return model.TelemetrySample{
		Resources: resources,
		IO: model.IOMetrics{
			ReadBytes: unsupported(), WriteBytes: unsupported(),
			ReadOperations: unsupported(), WriteOperations: unsupported(),
			DeviceEvidenceStatus: model.EvidenceUnsupported,
		},
		PSI:       unsupportedPSI(),
		Activity:  activity,
		Processes: []model.ProcessEvidence{}, ProcessChanges: []model.ProcessEvidenceChange{},
		ProcessEvidenceStatus: status, ProcessEvidenceReason: reason,
		ProcessEvidenceComplete: false,
	}
}

func unsupportedPSI() model.PSIMetrics {
	unsupported := func() model.Metric { return model.Metric{Status: string(model.EvidenceUnsupported)} }
	pressure := func() model.PressureMetrics {
		return model.PressureMetrics{
			SomeAvg10BasisPoints: unsupported(), SomeAvg60BasisPoints: unsupported(),
			SomeAvg300BasisPoints: unsupported(), SomeTotalUS: unsupported(),
			FullAvg10BasisPoints: unsupported(), FullAvg60BasisPoints: unsupported(),
			FullAvg300BasisPoints: unsupported(), FullTotalUS: unsupported(),
		}
	}
	return model.PSIMetrics{CPU: pressure(), Memory: pressure(), IO: pressure()}
}

func boundTelemetrySample(sample model.TelemetrySample, runID string, capabilities *model.Capabilities, activity model.IOActivity, intervalMs int64, observedAt time.Time) model.TelemetrySample {
	sample.Version = model.TelemetrySchemaVersion
	sample.RunID = runID
	sample.Sequence = 0
	if sample.ObservedAt.IsZero() {
		sample.ObservedAt = observedAt.UTC()
	} else {
		sample.ObservedAt = sample.ObservedAt.UTC()
	}
	if sample.ObservedAt.IsZero() {
		sample.ObservedAt = time.Now().UTC()
	}
	sample.Resources.SampleIntervalMs = intervalMs
	normalizeTelemetryResources(&sample.Resources)
	sample.Activity = cloneActivity(activity)
	if sample.Processes == nil {
		sample.Processes = []model.ProcessEvidence{}
	}
	if sample.ProcessChanges == nil {
		sample.ProcessChanges = []model.ProcessEvidenceChange{}
	}
	if sample.ProcessEvidenceStatus == "" {
		if capabilities != nil && capabilities.ProcessTelemetry {
			sample.ProcessEvidenceStatus = model.EvidenceUnavailable
			sample.ProcessEvidenceReason = "process-evidence-unavailable"
		} else {
			sample.ProcessEvidenceStatus = model.EvidenceUnsupported
			sample.ProcessEvidenceReason = "process-evidence-unsupported"
		}
		sample.ProcessEvidenceComplete = false
	}
	if sample.IO.DeviceEvidenceStatus == "" {
		sample.IO.DeviceEvidenceStatus = model.EvidenceUnavailable
		sample.IO.DeviceEvidenceComplete = false
	}
	normalizeTelemetryMetrics(&sample)
	if len(sample.Processes) > maxGuardianTelemetryProcesses {
		sample.Processes = sample.Processes[:maxGuardianTelemetryProcesses]
		sample.ProcessEvidenceComplete = false
		if sample.ProcessEvidenceReason == "" {
			sample.ProcessEvidenceReason = "process-snapshot-truncated"
		}
	}
	if len(sample.ProcessChanges) > maxGuardianTelemetryChanges {
		sample.ProcessChanges = sample.ProcessChanges[:maxGuardianTelemetryChanges]
		sample.ProcessEvidenceComplete = false
		if sample.ProcessEvidenceReason == "" {
			sample.ProcessEvidenceReason = "process-changes-truncated"
		}
	}
	if len(sample.IO.Devices) > maxGuardianTelemetryDevices {
		sample.IO.Devices = sample.IO.Devices[:maxGuardianTelemetryDevices]
		sample.IO.DeviceEvidenceStatus = model.EvidenceUnavailable
		sample.IO.DeviceEvidenceComplete = false
	}

	if data, err := json.Marshal(sample); err == nil && len(data) > maxGuardianTelemetryBytes {
		if len(sample.Processes) > 0 {
			sample.Processes = nil
			sample.ProcessEvidenceComplete = false
			sample.ProcessEvidenceReason = "telemetry-sample-size-limit"
		}
		if data, err = json.Marshal(sample); err == nil && len(data) > maxGuardianTelemetryBytes {
			sample.ProcessChanges = nil
			sample.ProcessEvidenceComplete = false
			sample.ProcessEvidenceReason = "telemetry-sample-size-limit"
		}
		if data, err = json.Marshal(sample); err == nil && len(data) > maxGuardianTelemetryBytes {
			sample.IO.Devices = nil
			sample.IO.DeviceEvidenceStatus = model.EvidenceUnavailable
			sample.IO.DeviceEvidenceComplete = false
		}
	}
	return sample
}

func normalizeTelemetryResources(resources *model.Resources) {
	if resources == nil {
		return
	}
	for _, metric := range []*model.Metric{
		&resources.MemoryBytes, &resources.PeakMemoryBytes,
		&resources.CPUTimeNs, &resources.ProcessCount, &resources.PeakProcessCount,
		&resources.TaskCount, &resources.PeakTaskCount,
	} {
		if metric.Status == "" {
			metric.Status = string(model.EvidenceUnavailable)
		}
	}
}

func normalizeTelemetryMetrics(sample *model.TelemetrySample) {
	normalizeMetric := func(metric *model.Metric) {
		switch model.EvidenceStatus(metric.Status) {
		case model.EvidenceMeasured, model.EvidenceUnavailable, model.EvidenceUnsupported:
		default:
			metric.Status = string(model.EvidenceUnavailable)
			metric.Value = 0
		}
		if metric.Value < 0 {
			metric.Status = string(model.EvidenceUnavailable)
			metric.Value = 0
		}
	}
	for _, metric := range []*model.Metric{
		&sample.Resources.MemoryBytes, &sample.Resources.PeakMemoryBytes,
		&sample.Resources.CPUTimeNs, &sample.Resources.ProcessCount, &sample.Resources.PeakProcessCount,
		&sample.Resources.TaskCount, &sample.Resources.PeakTaskCount,
		&sample.IO.ReadBytes, &sample.IO.WriteBytes, &sample.IO.ReadOperations, &sample.IO.WriteOperations,
		&sample.Activity.InputBytes, &sample.Activity.InputWrites, &sample.Activity.ResizeCount,
	} {
		normalizeMetric(metric)
	}
	for i := range sample.IO.Devices {
		device := &sample.IO.Devices[i]
		for _, metric := range []*model.Metric{&device.ReadBytes, &device.WriteBytes, &device.ReadOperations, &device.WriteOperations} {
			normalizeMetric(metric)
		}
	}
	for i := range sample.Processes {
		process := &sample.Processes[i]
		normalizeMetric(&process.CPUTimeNs)
		normalizeMetric(&process.RSSBytes)
	}
	for _, metric := range []*model.Metric{
		&sample.PSI.CPU.SomeAvg10BasisPoints, &sample.PSI.CPU.SomeAvg60BasisPoints,
		&sample.PSI.CPU.SomeAvg300BasisPoints, &sample.PSI.CPU.SomeTotalUS,
		&sample.PSI.CPU.FullAvg10BasisPoints, &sample.PSI.CPU.FullAvg60BasisPoints,
		&sample.PSI.CPU.FullAvg300BasisPoints, &sample.PSI.CPU.FullTotalUS,
		&sample.PSI.Memory.SomeAvg10BasisPoints, &sample.PSI.Memory.SomeAvg60BasisPoints,
		&sample.PSI.Memory.SomeAvg300BasisPoints, &sample.PSI.Memory.SomeTotalUS,
		&sample.PSI.Memory.FullAvg10BasisPoints, &sample.PSI.Memory.FullAvg60BasisPoints,
		&sample.PSI.Memory.FullAvg300BasisPoints, &sample.PSI.Memory.FullTotalUS,
		&sample.PSI.IO.SomeAvg10BasisPoints, &sample.PSI.IO.SomeAvg60BasisPoints,
		&sample.PSI.IO.SomeAvg300BasisPoints, &sample.PSI.IO.SomeTotalUS,
		&sample.PSI.IO.FullAvg10BasisPoints, &sample.PSI.IO.FullAvg60BasisPoints,
		&sample.PSI.IO.FullAvg300BasisPoints, &sample.PSI.IO.FullTotalUS,
	} {
		normalizeMetric(metric)
	}
	if sample.ProcessEvidenceStatus != model.EvidenceMeasured && sample.ProcessEvidenceStatus != model.EvidenceUnavailable && sample.ProcessEvidenceStatus != model.EvidenceUnsupported {
		sample.ProcessEvidenceStatus = model.EvidenceUnavailable
		sample.ProcessEvidenceComplete = false
		if sample.ProcessEvidenceReason == "" {
			sample.ProcessEvidenceReason = "process-evidence-unavailable"
		}
	}
	if sample.IO.DeviceEvidenceStatus != model.EvidenceMeasured && sample.IO.DeviceEvidenceStatus != model.EvidenceUnavailable && sample.IO.DeviceEvidenceStatus != model.EvidenceUnsupported {
		sample.IO.DeviceEvidenceStatus = model.EvidenceUnavailable
		sample.IO.DeviceEvidenceComplete = false
	}
}

func cloneActivity(activity model.IOActivity) model.IOActivity {
	if activity.LastInputAt != nil {
		at := *activity.LastInputAt
		activity.LastInputAt = &at
	}
	if activity.LastResizeAt != nil {
		at := *activity.LastResizeAt
		activity.LastResizeAt = &at
	}
	return activity
}

func cloneTelemetrySample(sample model.TelemetrySample) model.TelemetrySample {
	sample.Processes = append([]model.ProcessEvidence(nil), sample.Processes...)
	sample.ProcessChanges = append([]model.ProcessEvidenceChange(nil), sample.ProcessChanges...)
	sample.IO.Devices = append([]model.DeviceIOMetrics(nil), sample.IO.Devices...)
	sample.Activity = cloneActivity(sample.Activity)
	return sample
}

func incrementActivityMetric(metric *model.Metric, amount int64) {
	if metric.Status != string(model.EvidenceMeasured) || amount < 0 || metric.Value < 0 || metric.Value > int64(^uint64(0)>>1)-amount {
		metric.Status = string(model.EvidenceUnavailable)
		metric.Value = 0
		return
	}
	metric.Value += amount
}

func invalidateActivity(activity *model.IOActivity, input, resize bool) {
	if input {
		activity.InputBytes = model.Metric{Status: string(model.EvidenceUnavailable)}
		activity.InputWrites = model.Metric{Status: string(model.EvidenceUnavailable)}
	}
	if resize {
		activity.ResizeCount = model.Metric{Status: string(model.EvidenceUnavailable)}
	}
}
