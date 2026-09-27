package guardian

import (
	"errors"
	"io"
	"testing"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

type hostEnvelopeTestBackend struct {
	*capabilityBackend
	configured model.HostEnvelopeConfig
	started    bool
}

func (b *hostEnvelopeTestBackend) ConfigureHostEnvelope(config model.HostEnvelopeConfig) error {
	b.configured = config
	return nil
}

func (b *hostEnvelopeTestBackend) Start(model.RunSpec, io.Writer, io.Writer) (backend.Process, error) {
	if b.configured == (model.HostEnvelopeConfig{}) {
		return nil, errors.New("backend started before host envelope configuration")
	}
	b.started = true
	return nil, nil
}

func TestStartBackendConfiguresHostEnvelopeBeforePhysicalStart(t *testing.T) {
	selected := &hostEnvelopeTestBackend{capabilityBackend: &capabilityBackend{}}
	config := HostEnvelopeConfig{MemoryBytes: 1 << 30, TaskCount: 512, MaxActiveRuns: 8}
	process, err := startBackendWithHostEnvelope(selected, config, model.RunSpec{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := model.HostEnvelopeConfig{MemoryBytes: config.MemoryBytes, TaskCount: config.TaskCount, MaxActiveRuns: config.MaxActiveRuns}
	if selected.configured != want {
		t.Fatalf("backend host config = %+v, want %+v", selected.configured, want)
	}
	if !selected.started || process != nil {
		t.Fatalf("backend start = (called %t, process %v), want called after config with no test process", selected.started, process)
	}
}

func TestConfigureBackendHostEnvelopeRejectsUnsupportedBackend(t *testing.T) {
	selected := &capabilityBackend{}
	config := HostEnvelopeConfig{MaxActiveRuns: 1}
	process, err := startBackendWithHostEnvelope(selected, config, model.RunSpec{}, nil, nil)
	var configurationFailure *hostEnvelopeConfigurationError
	if !errors.As(err, &configurationFailure) || process != nil {
		t.Fatal("configured host envelope was silently ignored")
	}
}

type hostAdmissionFailureError struct{}

func (hostAdmissionFailureError) Error() string { return "host ceiling reached" }
func (hostAdmissionFailureError) HostAdmissionFailureCode() string {
	return "host-envelope-admission"
}

func TestHostEnvelopeStartupAdmissionFailureStaysMachineReadable(t *testing.T) {
	if got := hostEnvelopeStartupFailureReason(hostAdmissionFailureError{}); got != "host-envelope-admission" {
		t.Fatalf("host admission startup reason = %q", got)
	}
	if got := hostEnvelopeStartupFailureReason(&hostEnvelopeConfigurationError{err: errors.New("unsupported")}); got != "host-envelope-unsupported" {
		t.Fatalf("host configuration startup reason = %q", got)
	}
	if got := hostEnvelopeStartupFailureReason(errors.New("ordinary backend start failure")); got != "" {
		t.Fatalf("ordinary backend startup failure was reclassified as %q", got)
	}
}

var _ backend.Backend = (*hostEnvelopeTestBackend)(nil)
