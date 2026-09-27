//go:build linux

package linux

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestParseCgroupPopulated(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		populated bool
		found     bool
		wantErr   bool
	}{
		{name: "populated", data: "populated 1\nfrozen 0\n", populated: true, found: true},
		{name: "empty", data: "populated 0\nfrozen 0\n", found: true},
		{name: "missing", data: "frozen 0\n"},
		{name: "malformed", data: "populated many\n", found: true, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, found, err := parseCgroupPopulated([]byte(test.data))
			if got != test.populated || found != test.found || (err != nil) != test.wantErr {
				t.Fatalf("parseCgroupPopulated(%q) = (%v, %v, %v)", test.data, got, found, err)
			}
		})
	}
}

func TestParseHostPressurePreservesMeasuredZeroAndGaps(t *testing.T) {
	pressure := parseHostPressure([]byte("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"))
	if pressure.Status != "measured" {
		t.Fatalf("pressure status = %q, want measured", pressure.Status)
	}
	if pressure.SomeAvg10MilliPercent.Status != "measured" || pressure.SomeAvg10MilliPercent.Value != 0 || pressure.SomeTotalUsec.Status != "measured" || pressure.SomeTotalUsec.Value != 0 {
		t.Fatalf("zero PSI observation was not retained as measured: %+v", pressure)
	}
	if pressure.FullTotalUsec.Status != "measured" || pressure.FullTotalUsec.Value != 0 {
		t.Fatalf("zero full PSI observation was not retained as measured: %+v", pressure)
	}

	partial := parseHostPressure([]byte("some avg10=1.25 avg60=2.50 avg300=3.75 total=42\n"))
	if partial.SomeAvg10MilliPercent.Status != "measured" || partial.SomeAvg10MilliPercent.Value != 1250 {
		t.Fatalf("PSI average unit conversion failed: %+v", partial.SomeAvg10MilliPercent)
	}
	if partial.FullAvg10MilliPercent.Status != "unsupported" {
		t.Fatalf("missing full row status = %q, want unsupported", partial.FullAvg10MilliPercent.Status)
	}
	malformed := parseHostPressure([]byte("some avg10=NaN avg60=0 avg300=0 total=nope\n"))
	if malformed.SomeAvg10MilliPercent.Status != "unavailable" || malformed.SomeTotalUsec.Status != "unavailable" {
		t.Fatalf("malformed PSI values were not marked unavailable: %+v", malformed)
	}
}

func TestHostEnvelopeDelegationAndCrossBackendAdmission(t *testing.T) {
	config := HostEnvelopeConfig{MaxActiveRuns: 1}
	first := &Backend{}
	if err := first.ConfigureHostEnvelope(config); err != nil {
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("configure unavailable host envelope: %v", err)
		}
		if location, discoverErr := discoverCgroup(); discoverErr == nil && cgroupWritable(location.basePath) {
			t.Fatalf("writable delegated cgroup rejected active-Run envelope: %v", err)
		}
		status := first.HostEnvelopeStatus()
		if status.Status != "unsupported" || status.ActiveRuns.Status != "unsupported" {
			t.Fatalf("unsupported delegation was not explicit in status: %+v", status)
		}
		_, startErr := first.Start(hostSleepSpec(t), nil, nil)
		if !errors.Is(startErr, ErrHostEnvelopeAdmission) {
			t.Fatalf("configured unsupported envelope must fail Run admission, got %v", startErr)
		}
		return
	}
	second := &Backend{}
	if err := second.ConfigureHostEnvelope(config); err != nil {
		t.Fatalf("second backend could not reopen the shared workload root: %v", err)
	}
	status := first.HostEnvelopeStatus()
	if !status.Capabilities.WorkloadRoot || !status.Capabilities.ActiveRunEnforcement || status.ActiveRuns.Status != "measured" {
		t.Fatalf("delegated host envelope status is incomplete: %+v", status)
	}

	process, err := first.Start(hostSleepSpec(t), nil, nil)
	if err != nil {
		t.Fatalf("start first physical Run under workload root: %v", err)
	}
	owner := process.Ownership()
	if owner.CgroupPath == "" || !pathWithin(first.host.location.childPath, owner.CgroupPath) || owner.CgroupPath == first.host.location.childPath {
		_, _ = process.Terminate(0)
		_, _ = process.Wait()
		t.Fatalf("Run cgroup is not a child of the workload root: root=%q run=%q", first.host.location.childPath, owner.CgroupPath)
	}
	status = first.HostEnvelopeStatus()
	if status.ActiveRuns.Status != "measured" || status.ActiveRuns.Value != 1 {
		_, _ = process.Terminate(0)
		_, _ = process.Wait()
		t.Fatalf("active physical Run was not observed in the workload root: %+v", status.ActiveRuns)
	}
	secondProcess, secondErr := second.Start(hostSleepSpec(t), nil, nil)
	if secondProcess != nil {
		_, _ = secondProcess.Terminate(0)
		_, _ = secondProcess.Wait()
	}
	if !errors.Is(secondErr, ErrHostEnvelopeAdmission) {
		_, _ = process.Terminate(0)
		_, _ = process.Wait()
		t.Fatalf("independent backends bypassed active Run ceiling, got %v", secondErr)
	}
	if result, err := process.Terminate(0); err != nil || !result.TreeEmpty {
		t.Fatalf("terminate first physical Run: result=%+v err=%v", result, err)
	}
	if _, err := process.Wait(); err != nil {
		t.Fatalf("wait for first physical Run: %v", err)
	}

	process, err = second.Start(hostSleepSpec(t), nil, nil)
	if err != nil {
		t.Fatalf("admission was not released after physical terminal state: %v", err)
	}
	if result, err := process.Terminate(0); err != nil || !result.TreeEmpty {
		t.Fatalf("terminate admitted physical Run: result=%+v err=%v", result, err)
	}
	if _, err := process.Wait(); err != nil {
		t.Fatalf("wait for admitted physical Run: %v", err)
	}
}

func hostSleepSpec(t *testing.T) model.RunSpec {
	t.Helper()
	return model.RunSpec{
		Argv: []string{"/bin/sleep", "30"},
		Cwd:  os.TempDir(),
	}
}

func TestHostAdmissionErrorIsTyped(t *testing.T) {
	err := &HostAdmissionError{Resource: "task-count", Current: 4, Limit: 4}
	if !errors.Is(err, ErrHostEnvelopeAdmission) {
		t.Fatalf("HostAdmissionError does not wrap ErrHostEnvelopeAdmission: %v", err)
	}
	if err.Error() == "" {
		t.Fatal("HostAdmissionError has empty message")
	}
}

func TestHostAdmissionLockSerializesBackendInstances(t *testing.T) {
	firstRelease, err := lockHostAdmission()
	if err != nil {
		t.Fatalf("acquire first host admission lock: %v", err)
	}
	second := make(chan error, 1)
	go func() {
		release, err := lockHostAdmission()
		if err == nil {
			release()
		}
		second <- err
	}()
	select {
	case err := <-second:
		firstRelease()
		t.Fatalf("second backend bypassed host admission lock: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	firstRelease()
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("second backend could not acquire released host admission lock: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second backend did not acquire host admission lock after release")
	}
}
