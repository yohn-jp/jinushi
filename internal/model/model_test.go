package model

import (
	"encoding/json"
	"testing"
)

func TestProcessAndTaskEvidenceSerializeSeparately(t *testing.T) {
	run := Run{Resources: Resources{
		ProcessCount: Metric{Status: "measured", Value: 3},
		TaskCount:    Metric{Status: "measured", Value: 8},
	}}
	receipt := Receipt{
		Resources: Resources{
			PeakProcessCount: Metric{Status: "measured", Value: 4},
			PeakTaskCount:    Metric{Status: "measured", Value: 9},
		},
		EffectiveCapabilities: &Capabilities{
			Backend:                 "linux-cgroup-v2",
			ProcessCountEnforcement: false,
			TaskCountEnforcement:    true,
		},
	}
	runJSON, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var gotRun map[string]any
	var gotReceipt map[string]any
	if err := json.Unmarshal(runJSON, &gotRun); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(receiptJSON, &gotReceipt); err != nil {
		t.Fatal(err)
	}
	resources := gotRun["resources"].(map[string]any)
	if resources["processCount"].(map[string]any)["value"] != float64(3) ||
		resources["taskCount"].(map[string]any)["value"] != float64(8) {
		t.Fatalf("live resource counts were conflated: %s", runJSON)
	}
	receiptResources := gotReceipt["resources"].(map[string]any)
	if receiptResources["peakProcessCount"].(map[string]any)["value"] != float64(4) ||
		receiptResources["peakTaskCount"].(map[string]any)["value"] != float64(9) {
		t.Fatalf("receipt resource counts were conflated: %s", receiptJSON)
	}
	effective := gotReceipt["effectiveCapabilities"].(map[string]any)
	if effective["backend"] != "linux-cgroup-v2" || effective["processCountEnforcement"] != false ||
		effective["taskCountEnforcement"] != true {
		t.Fatalf("effective count enforcement was conflated: %s", receiptJSON)
	}
}
