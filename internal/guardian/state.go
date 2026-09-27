package guardian

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

const (
	descriptorName = "descriptor.json"
	statusName     = "status.json"
)

type launchConfig struct {
	Version            int          `json:"version"`
	RunID              string       `json:"runId"`
	Dir                string       `json:"dir"`
	Spec               modelRunSpec `json:"spec"`
	MaxOutputBytes     int64        `json:"maxOutputBytes"`
	InitialLeaseExpiry *time.Time   `json:"initialLeaseExpiry,omitempty"`
	LeaseGeneration    uint64       `json:"leaseGeneration,omitempty"`
	TerminationGraceMs int64        `json:"terminationGraceMs"`
	SampleIntervalMs   int64        `json:"sampleIntervalMs"`
	Token              string       `json:"token"`
}

// modelRunSpec keeps the JSON protocol internal without changing or
// reinterpreting caller fields.
type modelRunSpec = model.RunSpec

func writeDescriptor(d privateDescriptor) error {
	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("guardian: encode descriptor: %w", err)
	}
	path := filepath.Join(d.Dir, descriptorName)
	f, err := openStateFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrAlreadyStarted
		}
		return fmt.Errorf("guardian: create descriptor: %w", err)
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("guardian: persist descriptor: %w", err)
	}
	return syncDir(d.Dir)
}

func readDescriptor(dir, runID string) (privateDescriptor, error) {
	path := filepath.Join(dir, descriptorName)
	f, err := openStateFile(path, os.O_RDONLY, 0)
	if err != nil {
		return privateDescriptor{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return privateDescriptor{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > 16<<10 {
		return privateDescriptor{}, errors.New("guardian: invalid descriptor file")
	}
	var d privateDescriptor
	dec := json.NewDecoder(io.LimitReader(f, 16<<10))
	if err := dec.Decode(&d); err != nil {
		return privateDescriptor{}, fmt.Errorf("guardian: decode descriptor: %w", err)
	}
	if d.Version != ProtocolVersion || d.RunID != runID || d.Dir != dir || d.Token == "" {
		return privateDescriptor{}, errors.New("guardian: descriptor does not match run")
	}
	return d, nil
}

func writeSnapshot(dir string, snapshot Snapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("guardian: encode status: %w", err)
	}
	return atomicWriteState(filepath.Join(dir, statusName), data, 0600)
}

func readSnapshot(dir string) (Snapshot, error) {
	path := filepath.Join(dir, statusName)
	f, err := openStateFile(path, os.O_RDONLY, 0)
	if err != nil {
		return Snapshot{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Snapshot{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxRPCBytes {
		return Snapshot{}, errors.New("guardian: invalid status file")
	}
	var s Snapshot
	dec := json.NewDecoder(io.LimitReader(f, maxRPCBytes))
	if err := dec.Decode(&s); err != nil {
		return Snapshot{}, fmt.Errorf("guardian: decode status: %w", err)
	}
	if s.Version != ProtocolVersion {
		return Snapshot{}, errors.New("guardian: unsupported status version")
	}
	return s, nil
}
