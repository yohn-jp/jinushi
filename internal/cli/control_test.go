package cli

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

func TestPhysicalControlCommandsForwardIdentityAndValue(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		operation  string
		memoryHigh int64
		cpuQuota   int64
	}{
		{name: "pause", args: []string{"pause"}, operation: "pause"},
		{name: "resume", args: []string{"resume"}, operation: "resume"},
		{name: "memory-high", args: []string{"memory-high", "--bytes", "67108864"}, operation: "memory-high", memoryHigh: 67108864},
		{name: "cpu-quota", args: []string{"cpu-quota", "--percent", "250"}, operation: "cpu-quota", cpuQuota: 250},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := t.TempDir()
			requestReceived := make(chan protocol.Request, 1)
			serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
				requestReceived <- request
				return protocol.Response{Version: model.ProtocolVersion}
			})

			args := append([]string(nil), tc.args...)
			args = append(args, "--state-dir", stateDir, "--request-id", "control-retry-1", "--expected-generation", "7", "run_test")
			var stdout, stderr bytes.Buffer
			if code := Main(context.Background(), args, &stdout, &stderr, nil); code != 0 {
				t.Fatalf("Main exit=%d stderr=%s", code, stderr.String())
			}
			request := <-requestReceived
			if request.Op != tc.operation || request.RunID != "run_test" || request.RequestID != "control-retry-1" || request.ExpectedGeneration != 7 {
				t.Fatalf("request identity = %+v", request)
			}
			if request.MemoryHighBytes != tc.memoryHigh || request.CPUQuotaPercent != tc.cpuQuota {
				t.Fatalf("request values = memory-high:%d cpu-quota:%d", request.MemoryHighBytes, request.CPUQuotaPercent)
			}
		})
	}
}

func TestPhysicalControlCommandsRejectInvalidInputsBeforeIPC(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "pause missing request id", args: []string{"pause", "--expected-generation", "1", "run_test"}},
		{name: "resume missing generation", args: []string{"resume", "--request-id", "retry", "run_test"}},
		{name: "memory-high zero", args: []string{"memory-high", "--bytes", "0", "--request-id", "retry", "--expected-generation", "1", "run_test"}},
		{name: "memory-high negative", args: []string{"memory-high", "--bytes", "-1", "--request-id", "retry", "--expected-generation", "1", "run_test"}},
		{name: "memory-high out of range", args: []string{"memory-high", "--bytes", "9223372036854775808", "--request-id", "retry", "--expected-generation", "1", "run_test"}},
		{name: "cpu-quota zero", args: []string{"cpu-quota", "--percent", "0", "--request-id", "retry", "--expected-generation", "1", "run_test"}},
		{name: "cpu-quota negative", args: []string{"cpu-quota", "--percent", "-1", "--request-id", "retry", "--expected-generation", "1", "run_test"}},
		{name: "cpu-quota above finite kernel range", args: []string{"cpu-quota", "--percent", "9223372036854776", "--request-id", "retry", "--expected-generation", "1", "run_test"}},
		{name: "request id too long", args: []string{"pause", "--request-id", strings.Repeat("x", maxControlRequestIDBytes+1), "--expected-generation", "1", "run_test"}},
		{name: "request id contains line break", args: []string{"resume", "--request-id", "retry\nforged", "--expected-generation", "1", "run_test"}},
		{name: "generation zero", args: []string{"pause", "--request-id", "retry", "--expected-generation", "0", "run_test"}},
		{name: "missing run id", args: []string{"resume", "--request-id", "retry", "--expected-generation", "1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := t.TempDir()
			requestReceived := make(chan protocol.Request, 1)
			serveCLI(t, stateDir, func(_ context.Context, request protocol.Request) protocol.Response {
				requestReceived <- request
				return protocol.Response{Version: model.ProtocolVersion}
			})
			args := append([]string{tc.args[0], "--state-dir", stateDir}, tc.args[1:]...)
			var stdout, stderr bytes.Buffer
			if code := Main(context.Background(), args, &stdout, &stderr, nil); code != 2 {
				t.Fatalf("Main exit=%d, want usage error 2; stderr=%s", code, stderr.String())
			}
			select {
			case request := <-requestReceived:
				t.Fatalf("invalid control reached supervisor: %+v", request)
			default:
			}
		})
	}
}

func TestCPUQuotaMaximumMatchesLinuxFiniteRange(t *testing.T) {
	if maxCPUQuotaPercent != math.MaxInt64/1000 {
		t.Fatalf("max CPU quota percent = %d, want %d", maxCPUQuotaPercent, math.MaxInt64/1000)
	}
}
