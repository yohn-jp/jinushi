//go:build linux

package linux

import (
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/yohn-jp/jinushi/internal/model"
)

const (
	maxEvidenceProcesses    = 4096
	maxEvidenceProcEntries  = 65536
	maxEvidenceCommBytes    = 256
	maxEvidenceProcStatSize = 8192
)

var errEvidenceTruncated = errors.New("Linux process evidence exceeded its bounded snapshot limit")

// Evidence returns one bounded Linux physical telemetry sample. The caller
// supplies RunID and sequence when storing the sample. Process names come only
// from /proc/<pid>/comm; argv and environment are never read or retained.
func (p *Process) Evidence() (model.TelemetrySample, error) {
	p.evidenceMu.Lock()
	defer p.evidenceMu.Unlock()
	if p.finalEvidence != nil {
		return cloneTelemetrySample(*p.finalEvidence), nil
	}
	return p.collectEvidenceLocked(time.Now().UTC())
}

func (p *Process) collectEvidenceLocked(observedAt time.Time) (model.TelemetrySample, error) {
	sample := model.TelemetrySample{
		Version:    model.TelemetrySchemaVersion,
		ObservedAt: observedAt.UTC(),
		Resources:  unavailableResources(),
		IO:         emptyIOMetrics(unsupportedMetric()),
		PSI:        unsupportedPSI(),
		Activity: model.IOActivity{
			InputBytes:  unsupportedMetric(),
			InputWrites: unsupportedMetric(),
			ResizeCount: unsupportedMetric(),
		},
		Processes:      []model.ProcessEvidence{},
		ProcessChanges: []model.ProcessEvidenceChange{},
	}
	var failures []error

	var current []model.ProcessEvidence
	complete := false
	var reason string
	if p.cg != nil {
		resources, _ := p.Observe()
		sample.Resources = resources
		if err := validateCgroupOwnership(p.ownership, p.cg); err != nil {
			reason = "cgroup-ownership-unavailable"
			failures = append(failures, err)
			sample.IO = emptyIOMetrics(unavailableMetric())
			sample.PSI = unavailablePSI()
		} else {
			sample.IO = observeCgroupIO(p.cg)
			sample.PSI = observeCgroupPSI(p.cg)
			var err error
			current, complete, err = scanCgroupProcessEvidence(p.cg, "/proc")
			if err != nil {
				reason = "cgroup-process-membership-unavailable"
				if errors.Is(err, errEvidenceTruncated) {
					reason = "process-membership-truncated"
				}
				failures = append(failures, err)
			}
		}
	} else {
		parsed, err := subreaperToken(p.ownership)
		if err != nil {
			reason = "subreaper-ownership-unavailable"
			failures = append(failures, err)
		} else {
			var infos []procInfo
			infos, complete, err = scanSubreaperProcInfosBounded("/proc", parsed.Subreaper)
			if err != nil {
				reason = "subreaper-process-membership-unavailable"
				if errors.Is(err, errEvidenceTruncated) {
					reason = "process-membership-truncated"
				}
				failures = append(failures, err)
			} else if !complete {
				reason = "process-membership-truncated"
				sample.Resources = unavailableResources()
			} else {
				var factsComplete bool
				current, factsComplete = processEvidenceFromInfos("/proc", infos)
				if !factsComplete {
					complete = false
					reason = "process-evidence-incomplete"
				}
				sample.Resources = p.observeProcesses(infos)
				if len(activeProcesses(infos)) == 0 {
					// /proc no longer holds exited processes' cumulative CPU time.
					sample.Resources.CPUTimeNs = unavailableMetric()
				}
			}
		}
	}

	if reason == "" && !complete {
		reason = "process-membership-truncated"
	}
	if reason == "" {
		sample.ProcessEvidenceStatus = model.EvidenceMeasured
		sample.ProcessEvidenceReason = ""
		sample.Processes = current
		changes, changesComplete := p.processEvidenceChangesLocked(current, sample.ObservedAt)
		sample.ProcessChanges = changes
		if !p.evidenceReady {
			reason = "membership-baseline-established"
			complete = false
		} else if !changesComplete {
			reason = "process-change-gap"
			complete = false
		}
		sample.ProcessEvidenceComplete = complete
		sample.ProcessEvidenceReason = reason
		if complete {
			p.evidencePrior = evidenceMap(current)
			p.evidenceReady = true
		} else if reason == "membership-baseline-established" {
			// The current process snapshot is complete, so it can be the
			// comparison baseline for the next observation. It cannot establish
			// changes that happened before this first sample.
			p.evidencePrior = evidenceMap(current)
			p.evidenceReady = true
		} else {
			p.evidencePrior = nil
			p.evidenceReady = false
		}
	} else {
		sample.ProcessEvidenceStatus = model.EvidenceUnavailable
		sample.ProcessEvidenceReason = reason
		sample.ProcessEvidenceComplete = false
		sample.Processes = current
		p.evidencePrior = nil
		p.evidenceReady = false
	}
	return sample, errors.Join(failures...)
}

func (p *Process) processEvidenceChangesLocked(current []model.ProcessEvidence, observedAt time.Time) ([]model.ProcessEvidenceChange, bool) {
	if !p.evidenceReady {
		return []model.ProcessEvidenceChange{}, true
	}
	currentByID := evidenceMap(current)
	changes := make([]model.ProcessEvidenceChange, 0)
	complete := true
	identities := make([]model.ProcessIdentity, 0, len(p.evidencePrior))
	for identity := range p.evidencePrior {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].PID == identities[j].PID {
			return identities[i].StartTimeTicks < identities[j].StartTimeTicks
		}
		return identities[i].PID < identities[j].PID
	})
	for _, identity := range identities {
		if _, stillOwned := currentByID[identity]; stillOwned {
			continue
		}
		membership, exited, err := p.previousMembership(identity)
		if err != nil {
			complete = false
			continue
		}
		if exited {
			changes = append(changes, model.ProcessEvidenceChange{
				ObservedAt: observedAt, Kind: model.ProcessExited, Process: identity,
				PreviousMembership: model.ProcessMembershipOwned,
			})
			continue
		}
		if membership != model.ProcessMembershipOwned {
			changes = append(changes, model.ProcessEvidenceChange{
				ObservedAt: observedAt, Kind: model.ProcessMembershipChanged, Process: identity,
				PreviousMembership: model.ProcessMembershipOwned, Membership: membership,
			})
		} else {
			complete = false
		}
	}
	if !complete {
		return changes, false
	}
	oldIDs := p.evidencePrior
	newIdentities := make([]model.ProcessIdentity, 0)
	for identity := range currentByID {
		if _, previouslyOwned := oldIDs[identity]; !previouslyOwned {
			newIdentities = append(newIdentities, identity)
		}
	}
	sort.Slice(newIdentities, func(i, j int) bool {
		if newIdentities[i].PID == newIdentities[j].PID {
			return newIdentities[i].StartTimeTicks < newIdentities[j].StartTimeTicks
		}
		return newIdentities[i].PID < newIdentities[j].PID
	})
	for _, identity := range newIdentities {
		changes = append(changes, model.ProcessEvidenceChange{
			ObservedAt: observedAt, Kind: model.ProcessStarted, Process: identity,
			Membership: model.ProcessMembershipOwned,
		})
	}
	return changes, true
}

func (p *Process) previousMembership(identity model.ProcessIdentity) (model.ProcessMembership, bool, error) {
	info, err := readProcInfoAt("/proc", identity.PID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return model.ProcessMembershipOutside, true, nil
	}
	if err != nil {
		return model.ProcessMembershipUnknown, false, err
	}
	if info.StartTime != identity.StartTimeTicks || info.State == 'Z' || info.State == 'X' || info.State == 'x' {
		return model.ProcessMembershipOutside, true, nil
	}
	if p.cg != nil {
		inside, err := processInCgroup(identity.PID, p.cg)
		if err != nil {
			return model.ProcessMembershipUnknown, false, err
		}
		if inside {
			return model.ProcessMembershipOwned, false, nil
		}
		return model.ProcessMembershipOutside, false, nil
	}
	parsed, err := subreaperToken(p.ownership)
	if err != nil {
		return model.ProcessMembershipUnknown, false, err
	}
	owned, err := isSubreaperDescendantAt("/proc", identity.PID, identity.StartTimeTicks, parsed.Subreaper)
	if err != nil {
		return model.ProcessMembershipUnknown, false, err
	}
	if owned {
		return model.ProcessMembershipOwned, false, nil
	}
	return model.ProcessMembershipOutside, false, nil
}

func evidenceMap(processes []model.ProcessEvidence) map[model.ProcessIdentity]model.ProcessEvidence {
	result := make(map[model.ProcessIdentity]model.ProcessEvidence, len(processes))
	for _, process := range processes {
		identity := model.ProcessIdentity{PID: process.PID, StartTimeTicks: process.StartTimeTicks}
		result[identity] = process
	}
	return result
}

func cloneTelemetrySample(sample model.TelemetrySample) model.TelemetrySample {
	sample.Processes = append([]model.ProcessEvidence(nil), sample.Processes...)
	sample.ProcessChanges = append([]model.ProcessEvidenceChange(nil), sample.ProcessChanges...)
	sample.IO.Devices = append([]model.DeviceIOMetrics(nil), sample.IO.Devices...)
	if sample.Activity.LastInputAt != nil {
		lastInput := *sample.Activity.LastInputAt
		sample.Activity.LastInputAt = &lastInput
	}
	if sample.Activity.LastResizeAt != nil {
		lastResize := *sample.Activity.LastResizeAt
		sample.Activity.LastResizeAt = &lastResize
	}
	return sample
}

func scanCgroupProcessEvidence(cg *cgroup, procRoot string) ([]model.ProcessEvidence, bool, error) {
	if cg == nil || cg.file == nil {
		return nil, false, errors.New("Linux cgroup process-membership source is unavailable")
	}
	data, err := readEvidenceFileAt(cg.file, "cgroup.procs", maxCgroupEvidenceFileBytes)
	if err != nil {
		return nil, false, err
	}
	pids, complete, err := parseCgroupPIDList(data, maxEvidenceProcesses)
	if err != nil {
		return nil, false, err
	}
	processes := make([]model.ProcessEvidence, 0, len(pids))
	for _, pid := range pids {
		info, err := readProcInfoAt(procRoot, pid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			complete = false
			continue
		}
		inside, err := processInCgroup(pid, cg)
		if err != nil {
			complete = false
			continue
		}
		if !inside {
			complete = false
			continue
		}
		confirmed, err := readProcInfoAt(procRoot, pid)
		if err != nil || confirmed.StartTime != info.StartTime {
			complete = false
			continue
		}
		evidence, factsComplete := processEvidenceAt(procRoot, confirmed)
		processes = append(processes, evidence)
		if !factsComplete {
			complete = false
		}
	}
	if len(processes) > maxEvidenceProcesses {
		processes = processes[:maxEvidenceProcesses]
		complete = false
	}
	sortProcessEvidence(processes)
	return processes, complete, nil
}

func parseCgroupPIDList(data []byte, limit int) ([]int, bool, error) {
	if limit <= 0 || len(data) > maxCgroupEvidenceFileBytes {
		return nil, false, errEvidenceTruncated
	}
	fields := strings.Fields(string(data))
	pids := make([]int, 0, min(len(fields), limit))
	seen := make(map[int]struct{}, min(len(fields), limit))
	complete := true
	for _, raw := range fields {
		pid, err := strconv.Atoi(raw)
		if err != nil || pid <= 0 {
			return nil, false, errors.New("cgroup.procs contains an invalid PID")
		}
		if _, duplicate := seen[pid]; duplicate {
			return nil, false, errors.New("cgroup.procs contains a duplicate PID")
		}
		seen[pid] = struct{}{}
		if len(pids) == limit {
			complete = false
			continue
		}
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	return pids, complete, nil
}

func scanSubreaperProcessEvidence(procRoot string, reaper subreaperIdentity) ([]model.ProcessEvidence, bool, error) {
	infos, complete, err := scanSubreaperProcInfosBounded(procRoot, reaper)
	if err != nil {
		return nil, false, err
	}
	processes, factsComplete := processEvidenceFromInfos(procRoot, infos)
	return processes, complete && factsComplete, nil
}

func scanSubreaperProcInfosBounded(procRoot string, reaper subreaperIdentity) ([]procInfo, bool, error) {
	if reaper.PID <= 0 || reaper.StartTime == 0 {
		return nil, false, errors.New("Linux subreaper identity is incomplete")
	}
	all, complete, err := scanProcInfosBounded(procRoot, maxEvidenceProcEntries)
	if err != nil {
		return nil, false, err
	}
	byParent := make(map[int][]procInfo, len(all))
	var reaperFound bool
	for _, info := range all {
		if info.PID == reaper.PID {
			if info.StartTime != reaper.StartTime {
				return nil, false, errors.New("Linux subreaper PID was reused")
			}
			reaperFound = true
		}
		byParent[info.PPID] = append(byParent[info.PPID], info)
	}
	if !reaperFound {
		return nil, false, errors.New("Linux subreaper is not observable in the bounded process table")
	}
	seen := map[int]struct{}{reaper.PID: {}}
	queue := []int{reaper.PID}
	infos := make([]procInfo, 0)
	for len(queue) != 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range byParent[parent] {
			if _, exists := seen[child.PID]; exists {
				continue
			}
			seen[child.PID] = struct{}{}
			infos = append(infos, child)
			queue = append(queue, child.PID)
		}
	}
	if len(infos) > maxEvidenceProcesses {
		infos = infos[:maxEvidenceProcesses]
		complete = false
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].PID < infos[j].PID })
	return infos, complete, nil
}

func processEvidenceFromInfos(procRoot string, infos []procInfo) ([]model.ProcessEvidence, bool) {
	processes := make([]model.ProcessEvidence, 0, len(infos))
	complete := true
	for _, info := range infos {
		confirmed, err := readProcInfoAt(procRoot, info.PID)
		if err != nil || confirmed.StartTime != info.StartTime {
			complete = false
			continue
		}
		evidence, factsComplete := processEvidenceAt(procRoot, confirmed)
		processes = append(processes, evidence)
		if !factsComplete {
			complete = false
		}
	}
	sortProcessEvidence(processes)
	return processes, complete
}

func scanProcInfosBounded(procRoot string, limit int) ([]procInfo, bool, error) {
	directory, err := os.Open(procRoot)
	if err != nil {
		return nil, false, err
	}
	entries, readErr := directory.ReadDir(limit + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, false, readErr
	}
	if closeErr != nil {
		return nil, false, closeErr
	}
	if len(entries) > limit {
		return nil, false, errEvidenceTruncated
	}
	infos := make([]procInfo, 0, len(entries))
	complete := true
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		info, err := readProcInfoAt(procRoot, pid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			complete = false
			continue
		}
		infos = append(infos, info)
	}
	return infos, complete, nil
}

func readProcInfoAt(procRoot string, pid int) (procInfo, error) {
	if pid <= 0 {
		return procInfo{}, errors.New("process PID must be positive")
	}
	path := filepath.Join(procRoot, strconv.Itoa(pid), "stat")
	file, err := os.Open(path)
	if err != nil {
		return procInfo{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxEvidenceProcStatSize+1))
	closeErr := file.Close()
	if readErr != nil {
		return procInfo{}, readErr
	}
	if closeErr != nil {
		return procInfo{}, closeErr
	}
	if len(data) > maxEvidenceProcStatSize {
		return procInfo{}, errors.New("Linux proc stat record exceeds its read limit")
	}
	info, err := parseProcStat(data)
	if err != nil {
		return procInfo{}, err
	}
	if info.PID != pid {
		return procInfo{}, errors.New("Linux proc stat PID does not match its directory")
	}
	return info, nil
}

func processEvidenceAt(procRoot string, info procInfo) (model.ProcessEvidence, bool) {
	evidence := model.ProcessEvidence{
		PID:            info.PID,
		StartTimeTicks: info.StartTime,
		ParentPID:      info.PPID,
		Membership:     model.ProcessMembershipOwned,
		State:          string(info.State),
		CPUTimeNs:      processCPUTimeMetric(info.UserTicks, info.SysTicks),
		RSSBytes:       processRSSMetric(info.RSSBytes),
	}
	complete := true
	if comm, err := readProcessCommAt(procRoot, info.PID); err == nil {
		evidence.Comm = comm
	} else {
		complete = false
	}
	if info.PPID > 0 {
		parent, err := readProcInfoAt(procRoot, info.PPID)
		if err == nil {
			childAgain, childErr := readProcInfoAt(procRoot, info.PID)
			parentAgain, parentErr := readProcInfoAt(procRoot, info.PPID)
			if childErr == nil && parentErr == nil && childAgain.StartTime == info.StartTime && childAgain.PPID == info.PPID && parentAgain.StartTime == parent.StartTime {
				evidence.ParentStartTimeTicks = parent.StartTime
				evidence.ParentObserved = true
			}
		}
	}
	return evidence, complete
}

func readProcessCommAt(procRoot string, pid int) (string, error) {
	path := filepath.Join(procRoot, strconv.Itoa(pid), "comm")
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxEvidenceCommBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return "", readErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if len(data) > maxEvidenceCommBytes {
		return "", errEvidenceTruncated
	}
	return sanitizeProcessComm(data), nil
}

func sanitizeProcessComm(raw []byte) string {
	value := strings.TrimSuffix(string(raw), "\n")
	value = strings.ToValidUTF8(value, "�")
	var safe strings.Builder
	for _, r := range value {
		if unicode.IsControl(r) {
			r = '�'
		}
		if safe.Len()+utf8.RuneLen(r) > 64 {
			break
		}
		safe.WriteRune(r)
	}
	return safe.String()
}

func processCPUTimeMetric(userTicks, systemTicks uint64) model.Metric {
	if systemTicks > math.MaxUint64-userTicks {
		return unavailableMetric()
	}
	ticks := userTicks + systemTicks
	const nanosecondsPerTick = uint64(10_000_000)
	if ticks > math.MaxInt64/nanosecondsPerTick {
		return unavailableMetric()
	}
	return measuredEvidenceMetric(int64(ticks * nanosecondsPerTick))
}

func processRSSMetric(value int64) model.Metric {
	if value < 0 {
		return unavailableMetric()
	}
	return measuredEvidenceMetric(value)
}

func sortProcessEvidence(processes []model.ProcessEvidence) {
	sort.Slice(processes, func(i, j int) bool {
		if processes[i].PID == processes[j].PID {
			return processes[i].StartTimeTicks < processes[j].StartTimeTicks
		}
		return processes[i].PID < processes[j].PID
	})
}

func isSubreaperDescendantAt(procRoot string, pid int, startTime uint64, reaper subreaperIdentity) (bool, error) {
	if pid <= 0 || startTime == 0 || reaper.PID <= 0 || reaper.StartTime == 0 {
		return false, errors.New("Linux process ancestry identity is incomplete")
	}
	current, err := readProcInfoAt(procRoot, pid)
	if err != nil {
		return false, err
	}
	if current.StartTime != startTime {
		return false, nil
	}
	seen := map[int]struct{}{pid: {}}
	for range 4096 {
		parentPID := current.PPID
		if parentPID == reaper.PID {
			parent, err := readProcInfoAt(procRoot, reaper.PID)
			if err != nil {
				return false, err
			}
			return parent.StartTime == reaper.StartTime, nil
		}
		if parentPID <= 1 {
			return false, nil
		}
		if _, exists := seen[parentPID]; exists {
			return false, errors.New("Linux process ancestry contains a cycle")
		}
		seen[parentPID] = struct{}{}
		current, err = readProcInfoAt(procRoot, parentPID)
		if err != nil {
			return false, err
		}
	}
	return false, errors.New("Linux process ancestry exceeds its bounded walk")
}
