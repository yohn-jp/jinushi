// Package cli is the thin command-line client for the Jinushi supervisor.
package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yohn-jp/jinushi/internal/ipc"
	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

const outputChunkSize = 64 * 1024

type SupervisorRunner func(context.Context, string) error

// Main runs one Jinushi command. SupervisorRunner is injected from cmd/jinushi
// so internal/cli remains a protocol client and does not own runtime behavior.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer, runSupervisor SupervisorRunner) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printUsage(stdout)
		return 0
	}
	command := args[0]
	if command == "supervisor" {
		fs := flag.NewFlagSet("supervisor", flag.ContinueOnError)
		fs.SetOutput(stderr)
		stateDir := fs.String("state-dir", defaultStateDir(), "supervisor state directory")
		if err := fs.Parse(args[1:]); err != nil {
			return 2
		}
		if len(fs.Args()) != 0 {
			fmt.Fprintln(stderr, "supervisor takes no positional arguments")
			return 2
		}
		if runSupervisor == nil {
			fmt.Fprintln(stderr, "supervisor is unavailable in this build")
			return 1
		}
		if err := runSupervisor(ctx, *stateDir); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(stderr, "supervisor: %v\n", err)
			return 1
		}
		return 0
	}

	switch command {
	case "run":
		return runCommand(ctx, args[1:], stdout, stderr)
	case "list":
		return listCommand(ctx, args[1:], stdout, stderr)
	case "inspect":
		return idCommand(ctx, "inspect", args[1:], stdout, stderr)
	case "await":
		return idCommand(ctx, "await", args[1:], stdout, stderr)
	case "events":
		return eventsCommand(ctx, args[1:], stdout, stderr)
	case "close-input":
		return closeInputCommand(ctx, args[1:], stdout, stderr)
	case "output":
		return outputCommand(ctx, args[1:], stdout, stderr)
	case "attach":
		return attachCommand(ctx, args[1:], stdout, stderr)
	case "signal":
		return signalCommand(ctx, args[1:], stdout, stderr)
	case "cancel":
		return controlIDCommand(ctx, "cancel", args[1:], stdout, stderr)
	case "capabilities":
		return simpleCommand(ctx, "capabilities", args[1:], stdout, stderr)
	case "status":
		return simpleCommand(ctx, "status", args[1:], stdout, stderr)
	case "lease":
		return leaseCommand(ctx, args[1:], stdout, stderr)
	case "input":
		return inputCommand(ctx, args[1:], stdout, stderr)
	case "resize":
		return resizeCommand(ctx, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", command)
		printUsage(stderr)
		return 2
	}
}

func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	stateDir := fs.String("state-dir", defaultStateDir(), "supervisor state directory")
	human := fs.Bool("human", false, "render a concise human-readable response")
	submissionID := fs.String("submission-id", "", "stable caller-generated identity for safe Run retries")
	cwd := fs.String("cwd", "", "working directory (defaults to the current directory)")
	environmentMode := fs.String("environment-mode", "inherit-supervisor", "inherit-supervisor or replace")
	interactive := fs.Bool("interactive", false, "request a PTY/ConPTY")
	lifetime := fs.String("lifetime", "detached", "detached or lease-bound")
	leaseMS := fs.Int64("lease-ms", 0, "lease duration for lease-bound Runs")
	memory := fs.Int64("memory-bytes", 0, "memory limit")
	cpu := fs.Int64("cpu-quota-percent", 0, "CPU quota percentage")
	processes := fs.Int64("process-count", 0, "process-count limit")
	tasks := fs.Int64("task-count", 0, "Linux task/PID limit (pids.max)")
	wall := fs.Int64("wall-time-ms", 0, "wall-time limit")
	output := fs.Int64("output-bytes", 0, "retained output limit")
	parent := fs.String("parent-run-id", "", "opaque parent Run ID")
	wait := fs.Bool("wait", false, "wait for terminal physical outcome")
	var envSet, envUnset, correlation stringList
	fs.Var(&envSet, "env", "set environment entry KEY=VALUE (repeatable)")
	fs.Var(&envUnset, "unset-env", "unset environment variable (repeatable)")
	fs.Var(&correlation, "correlation", "opaque correlation entry KEY=VALUE (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	argv := fs.Args()
	if len(argv) == 0 {
		fmt.Fprintln(stderr, "run requires an executable; use: jinushi run [options] -- executable [args...]")
		return 2
	}
	if *submissionID == "" || len(*submissionID) > 128 {
		fmt.Fprintln(stderr, "run requires --submission-id between 1 and 128 bytes")
		return 2
	}
	if *lifetime != "detached" && *lifetime != "lease-bound" {
		fmt.Fprintln(stderr, "--lifetime must be detached or lease-bound")
		return 2
	}
	if *lifetime == "lease-bound" && *leaseMS <= 0 {
		fmt.Fprintln(stderr, "--lease-ms must be positive for a lease-bound Run")
		return 2
	}
	if *lifetime == "detached" && *leaseMS != 0 {
		fmt.Fprintln(stderr, "--lease-ms is valid only for a lease-bound Run")
		return 2
	}
	if *environmentMode != "inherit-supervisor" && *environmentMode != "replace" {
		fmt.Fprintln(stderr, "--environment-mode must be inherit-supervisor or replace")
		return 2
	}
	for _, value := range []*int64{memory, cpu, processes, tasks, wall, output} {
		if *value < 0 {
			fmt.Fprintln(stderr, "resource limits must be non-negative")
			return 2
		}
	}
	if *cwd == "" {
		current, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(stderr, "read current directory: %v\n", err)
			return 1
		}
		*cwd = current
	}
	set, err := parseMap(envSet, "environment")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	labels, err := parseMap(correlation, "correlation")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	spec := model.RunSpec{
		Argv: argv,
		Cwd:  *cwd,
		Environment: model.Environment{
			Mode:  *environmentMode,
			Set:   set,
			Unset: append([]string(nil), envUnset...),
		},
		Interactive: *interactive,
		Lifetime:    model.Lifetime{Mode: *lifetime, LeaseMs: *leaseMS},
		Limits: model.Limits{
			MemoryBytes: *memory, CPUQuotaPercent: *cpu, ProcessCount: *processes, TaskCount: *tasks,
			WallTimeMs: *wall, OutputBytes: *output,
		},
		ParentRunID: *parent,
		Correlation: labels,
	}
	response, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: "run", SubmissionID: *submissionID, Spec: &spec}, *human, stdout, stderr, false)
	if code != 0 || !*wait {
		return code
	}
	if response.Run == nil {
		fmt.Fprintln(stderr, "run response did not include a Run")
		return 1
	}
	return awaitAccepted(ctx, *stateDir, response.Run.ID, *human, stdout, stderr)
}

func listCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags("list", args, stderr)
	cursor := fs.String("cursor", "", "return Runs after this Run ID")
	limit := fs.Int64("limit", 64, "maximum Runs to return in this page")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 0 {
		return usageError(stderr, "list takes no positional arguments")
	}
	if *limit <= 0 {
		return usageError(stderr, "--limit must be positive")
	}
	_, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: "list", Cursor: *cursor, Limit: *limit}, *human, stdout, stderr, false)
	return code
}

func idCommand(ctx context.Context, op string, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags(op, args, stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 {
		return usageError(stderr, fmt.Sprintf("%s requires one Run ID", op))
	}
	request := protocol.Request{Op: op, RunID: fs.Arg(0)}
	if op == "await" {
		return awaitAccepted(ctx, *stateDir, request.RunID, *human, stdout, stderr)
	}
	_, code := requestAndRender(ctx, *stateDir, request, *human, stdout, stderr, false)
	return code
}

func controlIDCommand(ctx context.Context, op string, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags(op, args, stderr)
	requestID := fs.String("request-id", "", "stable identity for retrying this physical mutation")
	expectedGeneration := fs.Uint64("expected-generation", 0, "current Run generation from inspect")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 || *requestID == "" || len(*requestID) > 128 || *expectedGeneration == 0 {
		return usageError(stderr, fmt.Sprintf("%s requires a Run ID, --request-id, and positive --expected-generation", op))
	}
	request := protocol.Request{Op: op, RunID: fs.Arg(0), RequestID: *requestID, ExpectedGeneration: *expectedGeneration}
	_, code := requestAndRender(ctx, *stateDir, request, *human, stdout, stderr, false)
	return code
}

func awaitAccepted(ctx context.Context, stateDir, runID string, human bool, stdout, stderr io.Writer) int {
	response, code := requestAndRender(ctx, stateDir, protocol.Request{Op: "await", RunID: runID}, human, stdout, stderr, false)
	if code != 0 {
		return code
	}
	if response.Run == nil || response.Run.Receipt == nil {
		fmt.Fprintln(stderr, "await response did not include a terminal receipt")
		return 1
	}
	if response.Run.Receipt.Outcome == "exited" {
		if response.Run.Receipt.ExitCode != nil {
			return *response.Run.Receipt.ExitCode
		}
		return 0
	}
	return 1
}

func eventsCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags("events", args, stderr)
	after := fs.Uint64("after", 0, "return events with sequence greater than this value")
	follow := fs.Bool("follow", false, "subscribe until the Run reaches terminal")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 {
		return usageError(stderr, "events requires one Run ID")
	}
	if *follow {
		if *human {
			return usageError(stderr, "--human cannot be combined with --follow; follow always emits NDJSON")
		}
		return followEventsCommand(ctx, *stateDir, fs.Arg(0), *after, stdout, stderr)
	}
	_, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: "events", RunID: fs.Arg(0), After: *after}, *human, stdout, stderr, false)
	return code
}

type eventRecord struct {
	Type  string      `json:"type"`
	Event model.Event `json:"event"`
}

type gapRecord struct {
	Type         string `json:"type"`
	RunID        string `json:"runId"`
	After        uint64 `json:"after"`
	RetainedFrom uint64 `json:"retainedFrom"`
	Reason       string `json:"reason,omitempty"`
}

type terminalRecord struct {
	Type  string      `json:"type"`
	RunID string      `json:"runId"`
	State model.State `json:"state"`
}

type uncertainRecord struct {
	Type  string      `json:"type"`
	RunID string      `json:"runId"`
	State model.State `json:"state"`
}

type errorRecord struct {
	Type  string           `json:"type"`
	Error protocol.Failure `json:"error"`
}

var errStopFollow = errors.New("stop event follow after remote error")

func followEventsCommand(ctx context.Context, stateDir, runID string, after uint64, stdout, stderr io.Writer) int {
	var finalState model.State
	var retentionGapSeen bool
	var retentionGapWatermark uint64
	var remoteFailure bool
	var writeErr error
	followErr := ipc.FollowEvents(ctx, stateDir, protocol.Request{Op: "events", RunID: runID, After: after, Follow: true}, func(response protocol.Response) error {
		if response.Error != nil {
			remoteFailure = true
			if err := writeNDJSON(stdout, errorRecord{Type: "error", Error: *response.Error}); err != nil {
				writeErr = err
				return err
			}
			return errStopFollow
		}
		if response.Gap && (!retentionGapSeen || response.RetainedFrom != retentionGapWatermark) {
			if err := writeNDJSON(stdout, gapRecord{Type: "gap", RunID: runID, After: after, RetainedFrom: response.RetainedFrom, Reason: "journal"}); err != nil {
				writeErr = err
				return err
			}
			retentionGapSeen = true
			retentionGapWatermark = response.RetainedFrom
		}
		for _, event := range response.Events {
			if event.RunID != runID {
				remoteFailure = true
				failure := protocol.Failure{Code: "invalid-event-stream", Message: "event Run ID did not match the subscription"}
				if err := writeNDJSON(stdout, errorRecord{Type: "error", Error: failure}); err != nil {
					writeErr = err
					return err
				}
				return errStopFollow
			}
			if event.Seq <= after {
				remoteFailure = true
				failure := protocol.Failure{Code: "invalid-event-stream", Message: "event sequence did not advance"}
				if err := writeNDJSON(stdout, errorRecord{Type: "error", Error: failure}); err != nil {
					writeErr = err
					return err
				}
				return errStopFollow
			}
			expected := after
			if response.Gap && response.RetainedFrom > expected+1 {
				expected = response.RetainedFrom - 1
			}
			if expected != ^uint64(0) && event.Seq > expected+1 {
				if err := writeNDJSON(stdout, gapRecord{Type: "gap", RunID: runID, After: after, RetainedFrom: event.Seq, Reason: "sequence-hole"}); err != nil {
					writeErr = err
					return err
				}
			}
			if err := writeNDJSON(stdout, eventRecord{Type: "event", Event: event}); err != nil {
				writeErr = err
				return err
			}
			after = event.Seq
		}
		if response.Run != nil && (response.Run.State == model.Terminal || response.Run.State == model.Uncertain) {
			if response.Run.ID != runID {
				remoteFailure = true
				failure := protocol.Failure{Code: "invalid-event-stream", Message: "Run snapshot ID did not match the subscription"}
				if err := writeNDJSON(stdout, errorRecord{Type: "error", Error: failure}); err != nil {
					writeErr = err
					return err
				}
				return errStopFollow
			}
			finalState = response.Run.State
		}
		return nil
	})
	if writeErr != nil {
		fmt.Fprintf(stderr, "write event stream: %v\n", writeErr)
		return 1
	}
	if remoteFailure {
		return 1
	}
	if ctx.Err() != nil {
		return 0
	}
	if followErr != nil {
		return writeFollowError(stdout, stderr, protocol.Failure{Code: "event-follow-failed", Message: followErr.Error()})
	}
	if finalState == "" {
		return writeFollowError(stdout, stderr, protocol.Failure{Code: "subscription-closed", Message: "event subscription ended before terminal state was observed"})
	}
	if finalState == model.Uncertain {
		if err := writeNDJSON(stdout, uncertainRecord{Type: "uncertain", RunID: runID, State: finalState}); err != nil {
			fmt.Fprintf(stderr, "write event stream: %v\n", err)
			return 1
		}
		return 1
	}
	if err := writeNDJSON(stdout, terminalRecord{Type: "terminal", RunID: runID, State: finalState}); err != nil {
		fmt.Fprintf(stderr, "write event stream: %v\n", err)
		return 1
	}
	return 0
}

func writeFollowError(stdout, stderr io.Writer, failure protocol.Failure) int {
	if err := writeNDJSON(stdout, errorRecord{Type: "error", Error: failure}); err != nil {
		fmt.Fprintf(stderr, "write event stream: %v\n", err)
	}
	return 1
}

func writeNDJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func closeInputCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags("close-input", args, stderr)
	requestID := fs.String("request-id", "", "stable identity for retrying this physical mutation")
	expectedGeneration := fs.Uint64("expected-generation", 0, "current Run generation from inspect")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 || *requestID == "" || len(*requestID) > 128 || *expectedGeneration == 0 {
		return usageError(stderr, "close-input requires a Run ID, --request-id, and positive --expected-generation")
	}
	_, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: "close-input", RunID: fs.Arg(0), RequestID: *requestID, ExpectedGeneration: *expectedGeneration}, *human, stdout, stderr, false)
	return code
}

func outputCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags("output", args, stderr)
	stream := fs.String("stream", "stdout", "stdout, stderr, or pty")
	offset := fs.Int64("offset", 0, "byte offset into retained output")
	limit := fs.Int64("limit", outputChunkSize, "maximum bytes to return")
	jsonOutput := fs.Bool("json", false, "write the base64 JSON response instead of raw bytes")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 {
		return usageError(stderr, "output requires one Run ID")
	}
	if *offset < 0 || *limit <= 0 || *limit > protocol.MaxFrame {
		return usageError(stderr, "output offset must be non-negative and limit must be between 1 and the frame limit")
	}
	response, err := ipc.Call(ctx, *stateDir, protocol.Request{Op: "output", RunID: fs.Arg(0), Stream: *stream, Offset: *offset, Limit: *limit})
	if err != nil {
		failure := protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: requestErrorCode(err), Message: err.Error()}}
		if *human && !*jsonOutput {
			fmt.Fprintf(stderr, "%s: %s\n", failure.Error.Code, failure.Error.Message)
		} else {
			writeJSON(stdout, failure)
		}
		return 1
	}
	if response.Error != nil {
		if *human && !*jsonOutput {
			fmt.Fprintf(stderr, "%s: %s\n", response.Error.Code, response.Error.Message)
		} else {
			writeJSON(stdout, response)
		}
		return 1
	}
	if *jsonOutput {
		writeJSON(stdout, response)
		return 0
	}
	data, err := base64.StdEncoding.DecodeString(response.Data)
	if err != nil {
		fmt.Fprintf(stderr, "decode output: %v\n", err)
		return 1
	}
	if _, err := stdout.Write(data); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}
	if response.Gap {
		fmt.Fprintf(stderr, "output history gap; retained output starts at byte %d\n", response.RetainedFrom)
	}
	return 0
}

func attachCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags("attach", args, stderr)
	rows := fs.Int("rows", 0, "initial terminal rows")
	cols := fs.Int("cols", 0, "initial terminal columns")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 {
		return usageError(stderr, "attach requires one Run ID")
	}
	if (*rows == 0) != (*cols == 0) || *rows < 0 || *cols < 0 {
		return usageError(stderr, "both --rows and --cols must be positive when specified")
	}
	runID := fs.Arg(0)
	restore, termRows, termCols, terminal, err := configureTerminal(os.Stdin)
	if err != nil {
		fmt.Fprintf(stderr, "configure local terminal: %v\n", err)
		return 1
	}
	defer restore()
	if *rows > 0 {
		termRows, termCols = *rows, *cols
	}
	response, err := ipc.Call(ctx, *stateDir, protocol.Request{Op: "attach", RunID: runID})
	if err != nil {
		fmt.Fprintf(stderr, "attach: %v\n", err)
		return 1
	}
	if response.Error != nil {
		fmt.Fprintf(stderr, "attach: %s: %s\n", response.Error.Code, response.Error.Message)
		return 1
	}
	if response.AttachID == "" {
		fmt.Fprintln(stderr, "attach response did not include an attachment ID")
		return 1
	}
	if response.Run == nil || response.Run.Generation == 0 {
		fmt.Fprintln(stderr, "attach response did not include the current Run generation")
		return 1
	}
	defer detachAttachment(ctx, *stateDir, runID, response.AttachID, stderr)
	if termRows > 0 && termCols > 0 {
		if response.Run == nil || response.Run.Generation == 0 {
			fmt.Fprintln(stderr, "attach response did not include the current Run generation")
			return 1
		}
		requestID, err := newControlRequestID()
		if err != nil {
			fmt.Fprintf(stderr, "resize identity: %v\n", err)
			return 1
		}
		resize, err := ipc.Call(ctx, *stateDir, protocol.Request{Op: "resize", RunID: runID, AttachID: response.AttachID, Rows: termRows, Cols: termCols, RequestID: requestID, ExpectedGeneration: response.Run.Generation})
		if err != nil {
			fmt.Fprintf(stderr, "resize: %v\n", err)
			return 1
		}
		if resize.Error != nil {
			fmt.Fprintf(stderr, "resize: %s: %s\n", resize.Error.Code, resize.Error.Message)
			return 1
		}
		if resize.Run != nil {
			response.Run = resize.Run
		}
	}
	return attachLoop(ctx, *stateDir, runID, response.AttachID, *human, stdinReader{}, stdout, stderr, response.Run, terminal && termRows > 0 && termCols > 0)
}

// stdinReader is replaceable in tests while Main uses process stdin.
type stdinReader struct{}

func (stdinReader) Read(p []byte) (int, error) { return os.Stdin.Read(p) }

func attachLoop(ctx context.Context, stateDir, runID, attachID string, human bool, stdin io.Reader, stdout, stderr io.Writer, initial *model.Run, terminal bool) int {
	attachCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var controlGeneration atomic.Uint64
	var controlSendMu sync.Mutex
	if initial != nil {
		controlGeneration.Store(initial.Generation)
	}
	if terminal {
		go watchTerminalResize(attachCtx, stateDir, runID, attachID, &controlGeneration, &controlSendMu, stderr)
	}
	inputDone := make(chan struct{})
	detachRequested := make(chan struct{})
	go func() {
		defer close(inputDone)
		buffer := make([]byte, 32*1024)
		for {
			n, err := stdin.Read(buffer)
			if n > 0 {
				input := buffer[:n]
				detachAt := bytes.IndexByte(input, 0x1d) // Ctrl+] detaches without ending the Run.
				if detachAt >= 0 {
					input = input[:detachAt]
					close(detachRequested)
				}
				if len(input) > 0 {
					requestID, err := newControlRequestID()
					if err != nil {
						fmt.Fprintf(stderr, "attach input identity: %v\n", err)
						return
					}
					controlSendMu.Lock()
					request := protocol.Request{Op: "input", RunID: runID, AttachID: attachID, Stream: "pty", Data: base64.StdEncoding.EncodeToString(input), RequestID: requestID, ExpectedGeneration: controlGeneration.Load()}
					response, callErr := callAttachControl(attachCtx, stateDir, request, &controlGeneration)
					if response.Run != nil {
						updateControlGeneration(&controlGeneration, response.Run.Generation)
					}
					controlSendMu.Unlock()
					if callErr != nil {
						if attachCtx.Err() == nil {
							fmt.Fprintf(stderr, "attach input: %v\n", callErr)
						}
						return
					}
					if response.Error != nil {
						fmt.Fprintf(stderr, "attach input: %s: %s\n", response.Error.Code, response.Error.Message)
						return
					}
					if attachCtx.Err() != nil {
						return
					}
				}
				if detachAt >= 0 {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	var offset int64
	if initial != nil {
		if stream, ok := initial.Output.PTY, initial.Spec.Interactive; ok {
			offset = stream.RetainedFrom
		}
	}
	for {
		if attachCtx.Err() != nil {
			return 0
		}
		response, err := ipc.Call(attachCtx, stateDir, protocol.Request{Op: "output", RunID: runID, AttachID: attachID, Stream: "pty", Offset: offset, Limit: outputChunkSize})
		if err != nil {
			if attachCtx.Err() != nil {
				return 0
			}
			fmt.Fprintf(stderr, "attach output: %v\n", err)
			return 1
		}
		if response.Error != nil {
			fmt.Fprintf(stderr, "attach output: %s: %s\n", response.Error.Code, response.Error.Message)
			return 1
		}
		if response.Gap {
			fmt.Fprintf(stderr, "output history gap; retained output starts at byte %d\n", response.RetainedFrom)
			offset = int64(response.RetainedFrom)
		}
		if response.Run != nil {
			updateControlGeneration(&controlGeneration, response.Run.Generation)
		}
		data, err := base64.StdEncoding.DecodeString(response.Data)
		if err != nil {
			fmt.Fprintf(stderr, "decode PTY output: %v\n", err)
			return 1
		}
		if len(data) > 0 {
			if _, err := stdout.Write(data); err != nil {
				fmt.Fprintf(stderr, "write PTY output: %v\n", err)
				return 1
			}
			offset += int64(len(data))
			continue
		}
		inspection, err := ipc.Call(attachCtx, stateDir, protocol.Request{Op: "inspect", RunID: runID})
		if err != nil {
			if attachCtx.Err() != nil {
				return 0
			}
			fmt.Fprintf(stderr, "attach inspect: %v\n", err)
			return 1
		}
		if inspection.Error != nil {
			fmt.Fprintf(stderr, "attach inspect: %s: %s\n", inspection.Error.Code, inspection.Error.Message)
			return 1
		}
		if inspection.Run != nil && (inspection.Run.State == model.Terminal || inspection.Run.State == model.Uncertain) {
			if human {
				renderResponse(stderr, inspection, true)
			}
			if inspection.Run.State == model.Uncertain {
				fmt.Fprintln(stderr, "attach: Run physical state is uncertain")
				return 1
			}
			return 0
		}
		if inspection.Run != nil {
			updateControlGeneration(&controlGeneration, inspection.Run.Generation)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-attachCtx.Done():
			timer.Stop()
			return 0
		case <-detachRequested:
			timer.Stop()
			return 0
		case <-inputDone:
			// EOF only detaches stdin; output remains observable until terminal.
			inputDone = nil
		case <-timer.C:
		}
	}
}

func newControlRequestID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func detachAttachment(ctx context.Context, stateDir, runID, attachID string, stderr io.Writer) {
	detachCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, err := ipc.Call(detachCtx, stateDir, protocol.Request{Op: "detach", RunID: runID, AttachID: attachID})
	if err != nil {
		if ctx.Err() == nil {
			fmt.Fprintf(stderr, "detach: %v\n", err)
		}
		return
	}
	if response.Error != nil {
		fmt.Fprintf(stderr, "detach: %s: %s\n", response.Error.Code, response.Error.Message)
	}
}

func updateControlGeneration(generation *atomic.Uint64, value uint64) {
	for current := generation.Load(); value > current; current = generation.Load() {
		if generation.CompareAndSwap(current, value) {
			return
		}
	}
}

// A concurrent control can advance the Run while an attachment is reading
// input. A stale-generation rejection has not claimed the request identity,
// so retrying that same identity after inspection cannot duplicate input.
func callAttachControl(ctx context.Context, stateDir string, request protocol.Request, generation *atomic.Uint64) (protocol.Response, error) {
	response, err := ipc.Call(ctx, stateDir, request)
	if err != nil || response.Error == nil || response.Error.Code != "stale-generation" {
		return response, err
	}
	current, err := ipc.Call(ctx, stateDir, protocol.Request{Op: "inspect", RunID: request.RunID})
	if err != nil || current.Error != nil || current.Run == nil {
		return response, err
	}
	updateControlGeneration(generation, current.Run.Generation)
	request.ExpectedGeneration = current.Run.Generation
	return ipc.Call(ctx, stateDir, request)
}

func watchTerminalResize(ctx context.Context, stateDir, runID, attachID string, generation *atomic.Uint64, sendMu *sync.Mutex, stderr io.Writer) {
	rows, cols, ok := terminalDimensions(os.Stdin)
	if !ok {
		return
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		newRows, newCols, valid := terminalDimensions(os.Stdin)
		if !valid || (newRows == rows && newCols == cols) {
			continue
		}
		requestID, err := newControlRequestID()
		if err != nil {
			fmt.Fprintf(stderr, "resize identity: %v\n", err)
			return
		}
		sendMu.Lock()
		response, err := callAttachControl(ctx, stateDir, protocol.Request{Op: "resize", RunID: runID, AttachID: attachID, Rows: newRows, Cols: newCols, RequestID: requestID, ExpectedGeneration: generation.Load()}, generation)
		if response.Run != nil {
			updateControlGeneration(generation, response.Run.Generation)
		}
		sendMu.Unlock()
		if err != nil {
			if ctx.Err() == nil {
				fmt.Fprintf(stderr, "resize: %v\n", err)
			}
			return
		}
		if response.Error != nil {
			fmt.Fprintf(stderr, "resize: %s: %s\n", response.Error.Code, response.Error.Message)
			return
		}
		rows, cols = newRows, newCols
	}
}

func signalCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags("signal", args, stderr)
	requestID := fs.String("request-id", "", "stable identity for retrying this physical mutation")
	expectedGeneration := fs.Uint64("expected-generation", 0, "current Run generation from inspect")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 2 || *requestID == "" || len(*requestID) > 128 || *expectedGeneration == 0 {
		return usageError(stderr, "signal requires a Run ID, signal name, --request-id, and positive --expected-generation")
	}
	_, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: "signal", RunID: fs.Arg(0), Signal: fs.Arg(1), RequestID: *requestID, ExpectedGeneration: *expectedGeneration}, *human, stdout, stderr, false)
	return code
}

func simpleCommand(ctx context.Context, op string, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags(op, args, stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 0 {
		return usageError(stderr, op+" takes no positional arguments")
	}
	_, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: op}, *human, stdout, stderr, false)
	return code
}

func leaseCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "renew" {
		return usageError(stderr, "use: jinushi lease renew --generation N --lease-ms N <run-id>")
	}
	fs, stateDir, human := commonFlags("lease renew", args[1:], stderr)
	generation := fs.Uint64("generation", 0, "lease generation returned by inspect")
	leaseMS := fs.Int64("lease-ms", 0, "new lease duration")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 || *generation == 0 || *leaseMS <= 0 {
		return usageError(stderr, "lease renew requires a Run ID, positive --generation, and positive --lease-ms")
	}
	_, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: "lease-renew", RunID: fs.Arg(0), LeaseGeneration: *generation, LeaseMs: *leaseMS}, *human, stdout, stderr, false)
	return code
}

func inputCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags("input", args, stderr)
	stream := fs.String("stream", "stdin", "stdin or pty")
	requestID := fs.String("request-id", "", "stable identity for retrying this physical mutation")
	expectedGeneration := fs.Uint64("expected-generation", 0, "current Run generation from inspect")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 2 || *requestID == "" || len(*requestID) > 128 || *expectedGeneration == 0 {
		return usageError(stderr, "input requires a Run ID, text, --request-id, and positive --expected-generation")
	}
	_, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: "input", RunID: fs.Arg(0), Stream: *stream, Data: base64.StdEncoding.EncodeToString([]byte(fs.Arg(1))), RequestID: *requestID, ExpectedGeneration: *expectedGeneration}, *human, stdout, stderr, false)
	return code
}

func resizeCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, stateDir, human := commonFlags("resize", args, stderr)
	rows := fs.Int("rows", 0, "terminal rows")
	cols := fs.Int("cols", 0, "terminal columns")
	requestID := fs.String("request-id", "", "stable identity for retrying this physical mutation")
	expectedGeneration := fs.Uint64("expected-generation", 0, "current Run generation from inspect")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(fs.Args()) != 1 || *rows <= 0 || *cols <= 0 || *requestID == "" || len(*requestID) > 128 || *expectedGeneration == 0 {
		return usageError(stderr, "resize requires a Run ID, positive --rows and --cols, --request-id, and positive --expected-generation")
	}
	_, code := requestAndRender(ctx, *stateDir, protocol.Request{Op: "resize", RunID: fs.Arg(0), Rows: *rows, Cols: *cols, RequestID: *requestID, ExpectedGeneration: *expectedGeneration}, *human, stdout, stderr, false)
	return code
}

func commonFlags(name string, args []string, stderr io.Writer) (*flag.FlagSet, *string, *bool) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	stateDir := fs.String("state-dir", defaultStateDir(), "supervisor state directory")
	human := fs.Bool("human", false, "render a concise human-readable response")
	return fs, stateDir, human
}

func requestAndRender(ctx context.Context, stateDir string, request protocol.Request, human bool, stdout, stderr io.Writer, forceJSON bool) (protocol.Response, int) {
	response, err := ipc.Call(ctx, stateDir, request)
	if err != nil {
		response = protocol.Response{Version: model.ProtocolVersion, Error: &protocol.Failure{Code: requestErrorCode(err), Message: err.Error()}}
		if !human || forceJSON {
			writeJSON(stdout, response)
		} else {
			fmt.Fprintf(stderr, "%s: %s\n", response.Error.Code, response.Error.Message)
		}
		return response, 1
	}
	if response.Error != nil {
		if !human || forceJSON {
			writeJSON(stdout, response)
		} else {
			fmt.Fprintf(stderr, "%s: %s\n", response.Error.Code, response.Error.Message)
		}
		return response, 1
	}
	if forceJSON {
		writeJSON(stdout, response)
		return response, 0
	}
	return response, renderResponse(stdout, response, human)
}

func requestErrorCode(err error) string {
	switch {
	case errors.Is(err, ipc.ErrFrameTooLarge):
		return "invalid-request"
	case errors.Is(err, context.Canceled):
		return "request-cancelled"
	default:
		return "supervisor-unavailable"
	}
}

func renderResponse(w io.Writer, response protocol.Response, human bool) int {
	if !human {
		writeJSON(w, response)
		return 0
	}
	if response.Run != nil {
		run := response.Run
		if run.Receipt != nil {
			if run.Receipt.ExitCode != nil {
				fmt.Fprintf(w, "%s %s outcome=%s exit=%d\n", run.ID, run.State, run.Receipt.Outcome, *run.Receipt.ExitCode)
			} else {
				fmt.Fprintf(w, "%s %s outcome=%s\n", run.ID, run.State, run.Receipt.Outcome)
			}
		} else {
			fmt.Fprintf(w, "%s %s\n", run.ID, run.State)
		}
		return 0
	}
	if response.Runs != nil {
		for _, run := range response.Runs {
			fmt.Fprintf(w, "%s\t%s\tgen=%d\n", run.ID, run.State, run.Generation)
		}
		return 0
	}
	if response.Capabilities != nil {
		writeJSON(w, response.Capabilities)
		return 0
	}
	if response.Events != nil {
		writeJSON(w, response)
		return 0
	}
	if response.Gap {
		fmt.Fprintf(w, "history gap; retained from %d\n", response.RetainedFrom)
	}
	if response.Data != "" {
		fmt.Fprintln(w, response.Data)
		return 0
	}
	fmt.Fprintln(w, "ok")
	return 0
}

func writeJSON(w io.Writer, value any) {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		fmt.Fprintf(os.Stderr, "encode CLI response: %v\n", err)
	}
}

func parseMap(values []string, label string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(values))
	for _, value := range values {
		key, val, ok := strings.Cut(value, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("%s entry %q must use KEY=VALUE form", label, value)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%s key %q was specified more than once", label, key)
		}
		result[key] = val
	}
	return result, nil
}

type stringList []string

func (list *stringList) String() string { return strings.Join(*list, ",") }
func (list *stringList) Set(value string) error {
	*list = append(*list, value)
	return nil
}

func defaultStateDir() string {
	if configured := os.Getenv("JINUSHI_STATE_DIR"); configured != "" {
		return configured
	}
	if os.PathSeparator == '\\' {
		if local := os.Getenv("LOCALAPPDATA"); local != "" {
			return filepath.Join(local, "Jinushi")
		}
	}
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
		return filepath.Join(xdg, "jinushi")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".", ".jinushi")
	}
	return filepath.Join(home, ".local", "state", "jinushi")
}

func usageError(w io.Writer, message string) int {
	fmt.Fprintln(w, message)
	return 2
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, `Jinushi local execution runtime

Usage:
  jinushi supervisor [--state-dir DIR]
  jinushi run [options] -- executable [args...]
  jinushi list [--cursor RUN_ID] [--limit N]
  jinushi inspect <run-id>
  jinushi await <run-id>
  jinushi events [--after SEQ] [--follow] <run-id>
  jinushi output [--stream stdout|stderr|pty] [--offset N] [--limit N] <run-id>
  jinushi attach [--rows N --cols N] <run-id>
  jinushi signal <run-id> <signal>
  jinushi cancel <run-id>
  jinushi capabilities
  jinushi status
  jinushi lease renew --generation N --lease-ms N <run-id>
  jinushi input <run-id> <text>
  jinushi close-input <run-id>
  jinushi resize --rows N --cols N <run-id>

Responses are JSON by default. Use --human for concise status output. The
output command writes decoded bytes by default; pass --json for a base64 JSON response.
In attach mode, Ctrl+] detaches the client and leaves the Run alive.`)
}
