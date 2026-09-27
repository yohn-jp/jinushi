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
	if defaults.SampleIntervalMs <= 0 || defaults.DefaultOutputBytes <= 0 {
		t.Fatalf("invalid defaults: %+v", defaults)
	}
	path := filepath.Join(root, "config.json")
	if err := os.WriteFile(path, []byte(`{"sampleIntervalMs":100,"maxOutputBytes":2097152}`), 0600); err != nil {
		t.Fatal(err)
	}
	configured, err := loadConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	if configured.SampleIntervalMs != 100 || configured.MaxOutputBytes != 2097152 || configured.TerminationGraceMs != defaults.TerminationGraceMs {
		t.Fatalf("partial config was not applied with defaults: %+v", configured)
	}
	for _, document := range []string{
		`{"sampleIntervalMs":0}`,
		`{"maxOutputBytes":1024}`,
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
