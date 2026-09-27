package guardian

import (
	"errors"
	"fmt"
	"io"

	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/model"
)

type hostEnvelopeConfigurationError struct{ err error }

func (e *hostEnvelopeConfigurationError) Error() string {
	return fmt.Sprintf("guardian: configure host envelope: %v", e.err)
}

func (e *hostEnvelopeConfigurationError) Unwrap() error { return e.err }

func hostEnvelopeStartupFailureReason(err error) string {
	var configurationFailure *hostEnvelopeConfigurationError
	if errors.As(err, &configurationFailure) {
		return "host-envelope-unsupported"
	}
	var admissionFailure interface{ HostAdmissionFailureCode() string }
	if errors.As(err, &admissionFailure) {
		return admissionFailure.HostAdmissionFailureCode()
	}
	return ""
}

func configureBackendHostEnvelope(selected backend.Backend, config HostEnvelopeConfig) error {
	if config == (HostEnvelopeConfig{}) {
		return nil
	}
	configurator, ok := selected.(interface {
		ConfigureHostEnvelope(model.HostEnvelopeConfig) error
	})
	if !ok {
		return errors.New("guardian: configured host envelope is unsupported by selected backend")
	}
	return configurator.ConfigureHostEnvelope(model.HostEnvelopeConfig{
		MemoryBytes: config.MemoryBytes, TaskCount: config.TaskCount,
		MaxActiveRuns: config.MaxActiveRuns,
	})
}

func startBackendWithHostEnvelope(selected backend.Backend, config HostEnvelopeConfig, spec model.RunSpec, stdout, stderr io.Writer) (backend.Process, error) {
	if err := configureBackendHostEnvelope(selected, config); err != nil {
		return nil, &hostEnvelopeConfigurationError{err: err}
	}
	return selected.Start(spec, stdout, stderr)
}
