package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Config is supervisor-owned safety policy loaded from stateDir/config.json.
// A missing file uses deterministic defaults. Zero is invalid for every field
// so a partial JSON file retains the default for omitted fields.
type Config struct {
	SampleIntervalMs    int64 `json:"sampleIntervalMs"`
	TerminationGraceMs  int64 `json:"terminationGraceMs"`
	DefaultOutputBytes  int64 `json:"defaultOutputBytes"`
	MaxOutputBytes      int64 `json:"maxOutputBytes"`
	MaxWallTimeMs       int64 `json:"maxWallTimeMs"`
	MaxMemoryBytes      int64 `json:"maxMemoryBytes"`
	MaxProcessCount     int64 `json:"maxProcessCount"`
	EventRetentionCount int   `json:"eventRetentionCount"`
	EventRetentionBytes int64 `json:"eventRetentionBytes"`
}

func defaultConfig() Config {
	return Config{
		SampleIntervalMs: 250, TerminationGraceMs: 2000,
		DefaultOutputBytes: 1 << 20, MaxOutputBytes: 64 << 20,
		MaxWallTimeMs:  int64((7 * 24 * time.Hour) / time.Millisecond),
		MaxMemoryBytes: 1 << 40, MaxProcessCount: 4096,
		EventRetentionCount: 4096, EventRetentionBytes: 16 << 20,
	}
}

func loadConfig(root string) (Config, error) {
	config := defaultConfig()
	f, err := os.Open(filepath.Join(root, "config.json"))
	if errors.Is(err, os.ErrNotExist) {
		return config, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("open supervisor config: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Config{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return Config{}, errors.New("supervisor config must be a regular file under 64 KiB")
	}
	decoder := json.NewDecoder(io.LimitReader(f, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode supervisor config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Config{}, errors.New("supervisor config has trailing data")
	}
	if config.SampleIntervalMs < 50 || config.SampleIntervalMs > 60000 {
		return Config{}, errors.New("sampleIntervalMs must be between 50 and 60000")
	}
	if config.TerminationGraceMs < 100 || config.TerminationGraceMs > 30000 {
		return Config{}, errors.New("terminationGraceMs must be between 100 and 30000")
	}
	if config.DefaultOutputBytes < 1 || config.MaxOutputBytes < config.DefaultOutputBytes || config.MaxOutputBytes > 1<<30 {
		return Config{}, errors.New("invalid output retention limits")
	}
	if config.MaxWallTimeMs < 1000 || config.MaxWallTimeMs > int64((30*24*time.Hour)/time.Millisecond) {
		return Config{}, errors.New("invalid maxWallTimeMs")
	}
	if config.MaxMemoryBytes < 1 || config.MaxProcessCount < 1 {
		return Config{}, errors.New("invalid resource safety ceilings")
	}
	if config.EventRetentionCount < 16 || config.EventRetentionCount > 1000000 || config.EventRetentionBytes < 256<<10 || config.EventRetentionBytes > 1<<30 {
		return Config{}, errors.New("invalid event retention bounds")
	}
	return config, nil
}
