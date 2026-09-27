package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yohn-jp/jinushi/internal/store"
)

// Config is supervisor-owned safety policy loaded from stateDir/config.json.
// A missing file uses deterministic defaults. Zero disables optional host
// envelope ceilings; other zero fields retain their defaults in partial JSON.
type Config struct {
	SampleIntervalMs    int64           `json:"sampleIntervalMs"`
	TerminationGraceMs  int64           `json:"terminationGraceMs"`
	DefaultOutputBytes  int64           `json:"defaultOutputBytes"`
	MaxOutputBytes      int64           `json:"maxOutputBytes"`
	MaxWallTimeMs       int64           `json:"maxWallTimeMs"`
	MaxMemoryBytes      int64           `json:"maxMemoryBytes"`
	MaxProcessCount     int64           `json:"maxProcessCount"`
	MaxTaskCount        int64           `json:"maxTaskCount"`
	HostMemoryBytes     int64           `json:"hostMemoryBytes"`
	HostTaskCount       int64           `json:"hostTaskCount"`
	MaxActiveRuns       int64           `json:"maxActiveRuns"`
	EventRetentionCount int             `json:"eventRetentionCount"`
	EventRetentionBytes int64           `json:"eventRetentionBytes"`
	RetentionIntervalMs int64           `json:"retentionIntervalMs"`
	Retention           RetentionConfig `json:"retention"`
}

// RetentionConfig is the supervisor's stable millisecond-based JSON
// projection of the store retention policy.
type RetentionConfig struct {
	MaxAgeMs            int64   `json:"maxAgeMs"`
	MaxTerminalRuns     int     `json:"maxTerminalRuns"`
	MaxStateBytes       int64   `json:"maxStateBytes"`
	PreserveTombstones  bool    `json:"preserveTombstones"`
	MaxTombstones       int     `json:"maxTombstones"`
	MaxTombstoneAgeMs   int64   `json:"maxTombstoneAgeMs"`
	CompactMinFreeBytes int64   `json:"compactMinFreeBytes"`
	CompactMinFreeRatio float64 `json:"compactMinFreeRatio"`
}

func defaultConfig() Config {
	retention := retentionConfigFromPolicy(store.DefaultRetentionPolicy())
	return Config{
		SampleIntervalMs: 250, TerminationGraceMs: 2000,
		DefaultOutputBytes: 1 << 20, MaxOutputBytes: 64 << 20,
		MaxWallTimeMs:  int64((7 * 24 * time.Hour) / time.Millisecond),
		MaxMemoryBytes: 1 << 40, MaxProcessCount: 4096, MaxTaskCount: 4096,
		EventRetentionCount: 4096, EventRetentionBytes: 16 << 20,
		RetentionIntervalMs: 60_000, Retention: retention,
	}
}

func retentionConfigFromPolicy(policy store.RetentionPolicy) RetentionConfig {
	return RetentionConfig{
		MaxAgeMs:            policy.MaxAge.Milliseconds(),
		MaxTerminalRuns:     policy.MaxTerminalRuns,
		MaxStateBytes:       policy.MaxStateBytes,
		PreserveTombstones:  policy.PreserveTombstones,
		MaxTombstones:       policy.MaxTombstones,
		MaxTombstoneAgeMs:   policy.MaxTombstoneAge.Milliseconds(),
		CompactMinFreeBytes: policy.CompactMinFreeBytes,
		CompactMinFreeRatio: policy.CompactMinFreeRatio,
	}
}

func (config RetentionConfig) policy() store.RetentionPolicy {
	return store.RetentionPolicy{
		MaxAge:              time.Duration(config.MaxAgeMs) * time.Millisecond,
		MaxTerminalRuns:     config.MaxTerminalRuns,
		MaxStateBytes:       config.MaxStateBytes,
		PreserveTombstones:  config.PreserveTombstones,
		MaxTombstones:       config.MaxTombstones,
		MaxTombstoneAge:     time.Duration(config.MaxTombstoneAgeMs) * time.Millisecond,
		CompactMinFreeBytes: config.CompactMinFreeBytes,
		CompactMinFreeRatio: config.CompactMinFreeRatio,
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
	if config.MaxMemoryBytes < 1 || config.MaxProcessCount < 1 || config.MaxTaskCount < 1 {
		return Config{}, errors.New("invalid resource safety ceilings")
	}
	if config.HostMemoryBytes < 0 || config.HostTaskCount < 0 || config.MaxActiveRuns < 0 {
		return Config{}, errors.New("host safety ceilings must be non-negative")
	}
	if config.EventRetentionCount < 16 || config.EventRetentionCount > 1000000 || config.EventRetentionBytes < 256<<10 || config.EventRetentionBytes > 1<<30 {
		return Config{}, errors.New("invalid event retention bounds")
	}
	if config.RetentionIntervalMs < 1000 || config.RetentionIntervalMs > int64((24*time.Hour)/time.Millisecond) {
		return Config{}, errors.New("retentionIntervalMs must be between 1000 and 86400000")
	}
	if err := validateRetentionConfig(config.Retention); err != nil {
		return Config{}, err
	}
	return config, nil
}
