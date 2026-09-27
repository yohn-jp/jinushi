//go:build linux

package linux

import (
	"strconv"
	"strings"
	"testing"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestParseCgroupIOStatAggregatesAndBoundsDeviceEvidence(t *testing.T) {
	data := []byte("8:0 rbytes=10 wbytes=20 rios=3 wios=4 dbytes=9 dios=2\n8:16 rbytes=5 wbytes=7 rios=11 wios=13\n")
	got, err := parseCgroupIOStat(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReadBytes.Status != string(model.EvidenceMeasured) || got.ReadBytes.Value != 15 {
		t.Fatalf("read bytes = %+v, want measured 15", got.ReadBytes)
	}
	if got.WriteBytes.Value != 27 || got.ReadOperations.Value != 14 || got.WriteOperations.Value != 17 {
		t.Fatalf("unexpected aggregate counters: %+v", got)
	}
	if len(got.Devices) != 2 || got.Devices[0].Device != "8:0" || got.Devices[1].Device != "8:16" || !got.DeviceEvidenceComplete || got.DeviceEvidenceStatus != model.EvidenceMeasured {
		t.Fatalf("unexpected device evidence: %+v", got)
	}

	var tooMany strings.Builder
	for index := 0; index < maxCgroupEvidenceDevices+1; index++ {
		_, _ = tooMany.WriteString("8:")
		_, _ = tooMany.WriteString(strconv.Itoa(index))
		_, _ = tooMany.WriteString(" rbytes=1 wbytes=2 rios=3 wios=4\n")
	}
	bounded, err := parseCgroupIOStat([]byte(tooMany.String()))
	if err != nil {
		t.Fatal(err)
	}
	if len(bounded.Devices) != maxCgroupEvidenceDevices || bounded.DeviceEvidenceComplete || bounded.DeviceEvidenceStatus != model.EvidenceUnavailable {
		t.Fatalf("device evidence was not explicitly truncated: %+v", bounded)
	}
	if bounded.ReadBytes.Value != maxCgroupEvidenceDevices+1 {
		t.Fatalf("aggregate omitted truncated device: %+v", bounded.ReadBytes)
	}
}

func TestParseCgroupIOStatDistinguishesMeasuredZeroAndRejectsInvalidEvidence(t *testing.T) {
	zero, err := parseCgroupIOStat(nil)
	if err != nil {
		t.Fatal(err)
	}
	if zero.ReadBytes.Status != string(model.EvidenceMeasured) || zero.ReadBytes.Value != 0 || zero.DeviceEvidenceStatus != model.EvidenceMeasured {
		t.Fatalf("empty readable io.stat must be measured zero: %+v", zero)
	}
	for _, data := range []string{
		"bogus rbytes=1 wbytes=1 rios=1 wios=1\n",
		"8:0 rbytes=1 wbytes=1 rios=1 wios=1\n8:0 rbytes=1 wbytes=1 rios=1 wios=1\n",
		"8:0 rbytes=9223372036854775807 wbytes=1 rios=0 wios=0\n8:1 rbytes=1 wbytes=0 rios=0 wios=0\n",
	} {
		if _, err := parseCgroupIOStat([]byte(data)); err == nil {
			t.Errorf("parseCgroupIOStat(%q) unexpectedly succeeded", data)
		}
	}
	partial, err := parseCgroupIOStat([]byte("8:0 rbytes=1 wbytes=2 rios=3\n"))
	if err != nil {
		t.Fatal(err)
	}
	if partial.ReadBytes.Status != string(model.EvidenceMeasured) || partial.WriteOperations.Status != string(model.EvidenceUnavailable) || partial.Devices[0].WriteOperations.Status != string(model.EvidenceUnsupported) {
		t.Fatalf("missing kernel counter was not explicit: %+v", partial)
	}
}

func TestParsePSIUsesBasisPointsAndPreservesUnsupportedFullLine(t *testing.T) {
	got, err := parsePSI([]byte("some avg10=1.25 avg60=0.20 avg300=0.01 total=1234\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got.SomeAvg10BasisPoints.Value != 125 || got.SomeAvg60BasisPoints.Value != 20 || got.SomeAvg300BasisPoints.Value != 1 || got.SomeTotalUS.Value != 1234 {
		t.Fatalf("PSI basis point conversion mismatch: %+v", got)
	}
	if got.FullAvg10BasisPoints.Status != string(model.EvidenceUnsupported) || got.FullTotalUS.Status != string(model.EvidenceUnsupported) {
		t.Fatalf("missing optional PSI full line was not unsupported: %+v", got)
	}
}

func TestParsePSIRetainsSomeAndFullAndRejectsMalformedEvidence(t *testing.T) {
	got, err := parsePSI([]byte("some avg10=0.00 avg60=0.50 avg300=1.00 total=0\nfull avg10=0.10 avg60=0.20 avg300=0.30 total=4\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got.SomeAvg10BasisPoints.Value != 0 || got.SomeTotalUS.Value != 0 || got.FullAvg10BasisPoints.Value != 10 || got.FullTotalUS.Value != 4 {
		t.Fatalf("PSI measured values mismatch: %+v", got)
	}
	for _, data := range []string{
		"full avg10=0.1 avg60=0.2 avg300=0.3 total=1\n",
		"some avg10=NaN avg60=0 avg300=0 total=1\n",
		"some avg10=0 avg60=0 avg300=0 total=1\nsome avg10=0 avg60=0 avg300=0 total=1\n",
		"some avg10=0 avg60=0 total=1\n",
	} {
		if _, err := parsePSI([]byte(data)); err == nil {
			t.Errorf("parsePSI(%q) unexpectedly succeeded", data)
		}
	}
}
