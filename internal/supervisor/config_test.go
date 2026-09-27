package supervisor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigDefaultsAndBounds(t *testing.T) {
	root := t.TempDir()
	defaults, err := loadConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.SampleIntervalMs <= 0 || defaults.DefaultOutputBytes <= 0 || defaults.RetentionIntervalMs <= 0 || defaults.Retention.MaxTerminalRuns <= 0 {
		t.Fatalf("invalid defaults: %+v", defaults)
	}
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, []byte(`{"sampleIntervalMs":100,"maxOutputBytes":2097152,"hostMemoryBytes":1073741824,"hostTaskCount":128,"maxActiveRuns":8}`), 0600); err != nil {
		t.Fatal(err)
	}
	configured, err := loadConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	if configured.SampleIntervalMs != 100 || configured.MaxOutputBytes != 2097152 || configured.TerminationGraceMs != defaults.TerminationGraceMs || configured.HostMemoryBytes != 1073741824 || configured.HostTaskCount != 128 || configured.MaxActiveRuns != 8 || configured.Retention != defaults.Retention || configured.RetentionIntervalMs != defaults.RetentionIntervalMs {
		t.Fatalf("partial config was not applied with defaults: %+v", configured)
	}
	for _, document := range []string{
		`{"sampleIntervalMs":0}`,
		`{"maxOutputBytes":1024}`,
		`{"hostMemoryBytes":-1}`,
		`{"hostTaskCount":-1}`,
		`{"maxActiveRuns":-1}`,
		`{"retentionIntervalMs":0}`,
		`{"retention":{"maxAgeMs":-1}}`,
		`{"retention":{"preserveTombstones":true,"maxTombstones":0}}`,
		`{"retention":{"compactMinFreeRatio":1.5}}`,
		`{"unrecognized":1}`,
		`{"sampleIntervalMs":100} {"sampleIntervalMs":200}`,
	} {
		if err := os.WriteFile(path, []byte(document), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(root); err == nil {
			t.Errorf("accepted invalid config %q", document)
		}
	}
	if err := os.WriteFile(path, []byte(strings.Repeat(" ", (64<<10)+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(root); err == nil {
		t.Fatal("accepted oversized config")
	}
}

func TestLoadConfigRetentionPolicyUsesMillisecondsAndPartialDefaults(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, []byte(`{"retentionIntervalMs":2000,"retention":{"maxAgeMs":0,"maxTerminalRuns":17}}`), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := loadConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	defaults := defaultConfig()
	if config.RetentionIntervalMs != 2000 || config.Retention.MaxAgeMs != 0 || config.Retention.MaxTerminalRuns != 17 {
		t.Fatalf("configured retention fields = %+v", config.Retention)
	}
	if config.Retention.MaxStateBytes != defaults.Retention.MaxStateBytes || config.Retention.MaxTombstones != defaults.Retention.MaxTombstones || config.Retention.MaxTombstoneAgeMs != defaults.Retention.MaxTombstoneAgeMs {
		t.Fatalf("omitted retention fields did not retain defaults: %+v", config.Retention)
	}
	if policy := config.Retention.policy(); policy.MaxAge != 0 || policy.MaxTerminalRuns != 17 {
		t.Fatalf("retention policy conversion = %+v", policy)
	}
}
