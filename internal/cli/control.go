package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/yohn-jp/jinushi/internal/protocol"
)

const (
	maxControlRequestIDBytes = 128
	maxControlValue          = int64(1<<63 - 1)
	maxCPUQuotaPercent       = maxControlValue / 1000
)

func physicalControlCommand(ctx context.Context, command string, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags(command, args, stderr)
	requestID := fs.String("request-id", "", "stable identity for retrying this physical mutation")
	expectedGeneration := fs.Uint64("expected-generation", 0, "current Run generation from inspect")
	var memoryHighBytes, cpuQuotaPercent *int64
	switch command {
	case "memory-high":
		memoryHighBytes = fs.Int64("bytes", 0, "positive finite memory.high threshold in bytes")
	case "cpu-quota":
		cpuQuotaPercent = fs.Int64("percent", 0, "positive CPU quota as a percentage of one CPU")
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 || !validPhysicalControlRequestID(*requestID) || *expectedGeneration == 0 {
		return usageError(stderr, controlUsage(command))
	}

	request := protocol.Request{
		Op:                 controlOperation(command),
		RunID:              fs.Arg(0),
		RequestID:          *requestID,
		ExpectedGeneration: *expectedGeneration,
	}
	switch command {
	case "memory-high":
		if *memoryHighBytes <= 0 || *memoryHighBytes > maxControlValue {
			return usageError(stderr, "memory-high requires --bytes between 1 and 9223372036854775807")
		}
		request.MemoryHighBytes = *memoryHighBytes
	case "cpu-quota":
		if *cpuQuotaPercent <= 0 || *cpuQuotaPercent > maxCPUQuotaPercent {
			return usageError(stderr, fmt.Sprintf("cpu-quota requires --percent between 1 and %d", maxCPUQuotaPercent))
		}
		request.CPUQuotaPercent = *cpuQuotaPercent
	case "pause", "resume":
	default:
		return usageError(stderr, "unknown physical control command")
	}

	_, code := requestAndRender(ctx, *stateDir, request, *human, stdout, stderr, false)
	return code
}

func controlOperation(command string) string {
	return command
}

func controlUsage(command string) string {
	value := ""
	switch command {
	case "memory-high":
		value = " --bytes N"
	case "cpu-quota":
		value = " --percent N"
	}
	return fmt.Sprintf("%s requires one Run ID%s, --request-id (1 to %d bytes), and positive --expected-generation", command, value, maxControlRequestIDBytes)
}

func validPhysicalControlRequestID(requestID string) bool {
	return len(requestID) > 0 && len(requestID) <= maxControlRequestIDBytes && !strings.ContainsAny(requestID, "\x00\r\n")
}
