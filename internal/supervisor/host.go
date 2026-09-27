package supervisor

import (
	"fmt"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

func configuredHostEnvelope(config Config) model.HostEnvelopeConfig {
	return model.HostEnvelopeConfig{
		MemoryBytes:   config.HostMemoryBytes,
		TaskCount:     config.HostTaskCount,
		MaxActiveRuns: config.MaxActiveRuns,
	}
}

func (g *guardedExecutor) HostEnvelopeStatus() model.HostEnvelopeStatus {
	if reporter, ok := g.native.(interface {
		HostEnvelopeStatus() model.HostEnvelopeStatus
	}); ok {
		return reporter.HostEnvelopeStatus()
	}
	status := "unsupported"
	reason := "selected backend does not expose the Linux host envelope"
	config := configuredHostEnvelope(g.config)
	if config == (model.HostEnvelopeConfig{}) {
		status = "unconfigured"
		reason = "host envelope is not configured"
	}
	return unavailableHostEnvelopeStatus(status, reason, config)
}

func (g *guardedExecutor) ValidateHostAdmission() *protocol.Failure {
	config := configuredHostEnvelope(g.config)
	if config == (model.HostEnvelopeConfig{}) {
		return nil
	}
	validator, ok := g.native.(interface{ ValidateHostAdmission() error })
	if !ok {
		return &protocol.Failure{Code: "unsupported-capability", Message: "configured host safety envelope is unsupported by selected backend"}
	}
	if err := validator.ValidateHostAdmission(); err != nil {
		code := "host-envelope-admission"
		status := g.HostEnvelopeStatus()
		if status.Status == "unsupported" || !status.Capabilities.WorkloadRoot {
			code = "unsupported-capability"
		}
		return &protocol.Failure{Code: code, Message: err.Error()}
	}
	return nil
}

func (s *Service) validateHostAdmission() *protocol.Failure {
	if limit := s.config.MaxActiveRuns; limit > 0 {
		s.mu.RLock()
		active := len(s.active)
		s.mu.RUnlock()
		if int64(active) >= limit {
			return &protocol.Failure{
				Code:    "host-envelope-admission",
				Message: fmt.Sprintf("active physical Run capacity reached (%d accepted or nonterminal Runs of %d)", active, limit),
			}
		}
	}
	if validator, ok := s.backend.(interface{ ValidateHostAdmission() *protocol.Failure }); ok {
		return validator.ValidateHostAdmission()
	}
	return nil
}

func (s *Service) hostEnvelopeResponse(out protocol.Response) protocol.Response {
	if reporter, ok := s.backend.(interface {
		HostEnvelopeStatus() model.HostEnvelopeStatus
	}); ok {
		status := reporter.HostEnvelopeStatus()
		out.HostEnvelope = &status
	}
	return out
}

func unavailableHostEnvelopeStatus(status, reason string, config model.HostEnvelopeConfig) model.HostEnvelopeStatus {
	unsupported := model.Metric{Status: "unsupported"}
	pressure := model.HostPressure{
		Status:                 "unsupported",
		SomeAvg10MilliPercent:  unsupported,
		SomeAvg60MilliPercent:  unsupported,
		SomeAvg300MilliPercent: unsupported,
		SomeTotalUsec:          unsupported,
		FullAvg10MilliPercent:  unsupported,
		FullAvg60MilliPercent:  unsupported,
		FullAvg300MilliPercent: unsupported,
		FullTotalUsec:          unsupported,
	}
	return model.HostEnvelopeStatus{
		Status: status, Config: config,
		ActiveRuns: unsupported, MemoryBytes: unsupported, TaskCount: unsupported,
		MemoryPressure: pressure, CPUPressure: pressure, IOPressure: pressure,
		Reason: reason,
	}
}
