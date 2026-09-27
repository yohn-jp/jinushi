package supervisor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

type countedTerminalExecutor struct{ starts atomic.Int32 }

func (*countedTerminalExecutor) Capabilities() model.Capabilities {
	return model.Capabilities{Backend: "test", MemoryTelemetry: true, CPUTelemetry: true, ProcessTelemetry: true}
}

func (e *countedTerminalExecutor) Start(run model.Run, _ model.RunSpec, _, _ io.Writer) (physical, error) {
	e.starts.Add(1)
	finished := time.Now().UTC()
	return nil, &cleanStartTerminal{receipt: model.Receipt{
		Version: model.ProtocolVersion, RunID: run.ID, Outcome: "exited", FinishedAt: finished, Cleanup: "complete",
		Resources: initialResources(e.Capabilities(), 250), Capabilities: e.Capabilities(),
	}}
}

func (*countedTerminalExecutor) Reconcile(model.Run, io.Writer, io.Writer) (reconcileResult, error) {
	return reconcileResult{}, nil
}

func TestRunSubmissionRetryDoesNotStartAgainAndHidesEnvironment(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	executor := &countedTerminalExecutor{}
	svc := newService(root, db, executor, defaultConfig())
	spec := &model.RunSpec{
		Argv: []string{"/bin/true"}, Cwd: t.TempDir(),
		Environment: model.Environment{Mode: "replace", Set: map[string]string{"TOKEN": "private-run-secret-value"}},
	}
	req := protocol.Request{Version: model.ProtocolVersion, Op: "run", SubmissionID: "retry-run-1", Spec: spec}
	responses := make([]protocol.Response, 2)
	var submitters sync.WaitGroup
	submitters.Add(len(responses))
	for i := range responses {
		go func(i int) {
			defer submitters.Done()
			responses[i] = svc.Handle(context.Background(), req)
		}(i)
	}
	submitters.Wait()
	first, second := responses[0], responses[1]
	if first.Error != nil || first.Run == nil {
		t.Fatalf("first submission = %+v", first.Error)
	}
	if second.Error != nil || second.Run == nil || second.Run.ID != first.Run.ID {
		t.Fatalf("retry response = %+v; first Run=%+v", second.Error, first.Run)
	}
	conflict := req
	conflict.Spec = &model.RunSpec{Argv: []string{"/bin/false"}, Cwd: spec.Cwd}
	conflictResponse := svc.Handle(context.Background(), conflict)
	if conflictResponse.Error == nil || conflictResponse.Error.Code != "submission-conflict" {
		t.Fatalf("changed accepted specification error = %+v", conflictResponse.Error)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	if got := executor.starts.Load(); got != 1 {
		t.Fatalf("physical starts=%d, want exactly one", got)
	}

	dbBytes, err := os.ReadFile(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(dbBytes, []byte("private-run-secret-value")) {
		t.Fatal("raw Run environment value was persisted")
	}
	responseBytes, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(responseBytes, []byte("private-run-secret-value")) {
		t.Fatal("raw Run environment value was returned")
	}

	reopened, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	restartedExecutor := &countedTerminalExecutor{}
	restarted := newService(root, reopened, restartedExecutor, defaultConfig())
	retry := restarted.Handle(context.Background(), req)
	if retry.Error != nil || retry.Run == nil || retry.Run.ID != first.Run.ID {
		t.Fatalf("post-restart retry = %+v; original Run=%s", retry.Error, first.Run.ID)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	if got := restartedExecutor.starts.Load(); got != 0 {
		t.Fatalf("post-restart retry started %d physical Runs", got)
	}
}

type controlTestExecutor struct{}

func (controlTestExecutor) Capabilities() model.Capabilities {
	return model.Capabilities{Backend: "test", Signals: []string{"USR1"}}
}
func (controlTestExecutor) Start(model.Run, model.RunSpec, io.Writer, io.Writer) (physical, error) {
	return nil, errors.New("unexpected Start")
}
func (controlTestExecutor) Reconcile(model.Run, io.Writer, io.Writer) (reconcileResult, error) {
	return reconcileResult{}, errors.New("unexpected Reconcile")
}

type controlTestPhysical struct {
	mu          sync.Mutex
	inputCalls  int
	input       []byte
	inputErr    error
	inputStart  chan struct{}
	inputResume chan struct{}
	signalCalls int
	resizeCalls int
	closeCalls  int
}

func (*controlTestPhysical) Ownership() model.Ownership        { return model.Ownership{Backend: "test"} }
func (*controlTestPhysical) Wait() (exitResult, error)         { return exitResult{}, nil }
func (*controlTestPhysical) Observe() (model.Resources, error) { return model.Resources{}, nil }
func (p *controlTestPhysical) Signal(string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.signalCalls++
	return nil
}
func (*controlTestPhysical) Terminate(time.Duration) (terminationResult, error) {
	return terminationResult{complete: true}, nil
}
func (p *controlTestPhysical) WriteInput(data []byte) error {
	if p.inputStart != nil {
		close(p.inputStart)
		<-p.inputResume
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inputCalls++
	p.input = append(p.input, data...)
	return p.inputErr
}
func (p *controlTestPhysical) Resize(uint16, uint16) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resizeCalls++
	return nil
}
func (p *controlTestPhysical) CloseInput() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeCalls++
	return nil
}

func newControlTestService(t *testing.T, p physical) (*Service, *store.Store) {
	t.Helper()
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	run := model.Run{ID: "run_control_test", State: model.Running, Generation: 1, CreatedAt: time.Now().UTC(), Spec: model.RunSpec{Argv: []string{"/bin/cat"}, Cwd: t.TempDir()}}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	svc := newService(root, db, controlTestExecutor{}, defaultConfig())
	svc.active[run.ID] = &active{run: run, spec: run.Spec, process: p, done: make(chan struct{})}
	t.Cleanup(func() { _ = svc.Close() })
	return svc, db
}

func acquireControlTestWriter(t *testing.T, svc *Service, runID, attachID string) string {
	t.Helper()
	response := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: attachID})
	if response.Error != nil || response.WriterToken == "" || response.WriterLeaseExpiresAt == nil {
		t.Fatalf("writer acquire response = %+v", response.Error)
	}
	return response.WriterToken
}

func TestInputRetryAndConcurrentReplayWriteBytesOnce(t *testing.T) {
	p := &controlTestPhysical{}
	svc, db := newControlTestService(t, p)
	writerToken := acquireControlTestWriter(t, svc, "run_control_test", "writer-input")
	request := protocol.Request{
		Version: model.ProtocolVersion, Op: "input", RunID: "run_control_test", Stream: "stdin",
		Data: base64.StdEncoding.EncodeToString([]byte("payload-once")), RequestID: "input-identity", ExpectedGeneration: 1,
		WriterToken: writerToken,
	}
	responses := make([]protocol.Response, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	for i := range responses {
		go func(i int) {
			defer workers.Done()
			responses[i] = svc.Handle(context.Background(), request)
		}(i)
	}
	workers.Wait()
	for i, response := range responses {
		if response.Error != nil || response.Run == nil || response.Run.Generation != 2 {
			t.Fatalf("request %d response = %+v", i, response.Error)
		}
	}
	if p.inputCalls != 1 || string(p.input) != "payload-once" {
		t.Fatalf("input side effect calls=%d data=%q", p.inputCalls, p.input)
	}
	conflict := request
	conflict.Data = base64.StdEncoding.EncodeToString([]byte("different-payload"))
	if got := svc.Handle(context.Background(), conflict); got.Error == nil || got.Error.Code != "request-conflict" {
		t.Fatalf("reused request identity response = %+v", got.Error)
	}
	stale := request
	stale.RequestID = "new-input-identity"
	if got := svc.Handle(context.Background(), stale); got.Error == nil || got.Error.Code != "stale-generation" {
		t.Fatalf("stale input response = %+v", got.Error)
	}
	run, err := db.Get(request.RunID)
	if err != nil || run.Generation != 2 {
		t.Fatalf("persisted generation=%d err=%v", run.Generation, err)
	}
	finished := time.Now().UTC()
	run.State = model.Terminal
	run.Generation++
	run.FinishedAt = &finished
	run.Receipt = &model.Receipt{Version: model.ProtocolVersion, RunID: run.ID, Outcome: "exited", FinishedAt: finished, Resources: run.Resources, Output: run.Output, Cleanup: "complete"}
	if _, err := db.Update(run, &model.Event{Kind: model.EventRunTerminal, ObservedAt: finished}); err != nil {
		t.Fatal(err)
	}
	svc.mu.Lock()
	delete(svc.active, request.RunID)
	svc.mu.Unlock()
	svc.writerLeases.ClearRun(request.RunID)
	terminalReplay := svc.Handle(context.Background(), request)
	if terminalReplay.Error != nil || terminalReplay.Run == nil || terminalReplay.Run.State != model.Terminal {
		t.Fatalf("post-terminal replay = %+v", terminalReplay.Error)
	}
	if p.inputCalls != 1 {
		t.Fatalf("post-terminal replay repeated input %d times", p.inputCalls)
	}
}

func TestUncertainInputResultCannotBeRepeated(t *testing.T) {
	p := &controlTestPhysical{inputErr: errors.New("ambiguous physical write")}
	svc, _ := newControlTestService(t, p)
	writerToken := acquireControlTestWriter(t, svc, "run_control_test", "writer-uncertain")
	request := protocol.Request{
		Version: model.ProtocolVersion, Op: "input", RunID: "run_control_test", Stream: "pty",
		Data: base64.StdEncoding.EncodeToString([]byte("once")), RequestID: "ambiguous-input", ExpectedGeneration: 1,
		WriterToken: writerToken,
	}
	first := svc.Handle(context.Background(), request)
	second := svc.Handle(context.Background(), request)
	if first.Error == nil || first.Error.Code != "control-uncertain" || second.Error == nil || second.Error.Code != "control-uncertain" {
		t.Fatalf("first=%+v replay=%+v; both should be uncertain", first.Error, second.Error)
	}
	if p.inputCalls != 1 {
		t.Fatalf("ambiguous input was attempted %d times", p.inputCalls)
	}
}

func TestPendingInputReplayAfterServiceRestartCannotBeRepeated(t *testing.T) {
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	run := model.Run{ID: "run_restart_control", State: model.Running, Generation: 1, CreatedAt: now, Spec: model.RunSpec{Argv: []string{"/bin/cat"}, Cwd: t.TempDir()}}
	if _, _, err := db.Create(run, nil); err != nil {
		t.Fatal(err)
	}
	p := &controlTestPhysical{inputErr: errors.New("ambiguous physical write")}
	firstService := newService(root, db, controlTestExecutor{}, defaultConfig())
	firstService.active[run.ID] = &active{run: run, spec: run.Spec, process: p, done: make(chan struct{})}
	writerToken := acquireControlTestWriter(t, firstService, run.ID, "writer-restart")
	req := protocol.Request{
		Version: model.ProtocolVersion, Op: "input", RunID: run.ID, Stream: "stdin",
		Data: base64.StdEncoding.EncodeToString([]byte("one-shot")), RequestID: "restart-input", ExpectedGeneration: run.Generation,
		WriterToken: writerToken,
	}
	first := firstService.Handle(context.Background(), req)
	if first.Error == nil || first.Error.Code != "control-uncertain" || p.inputCalls != 1 {
		t.Fatalf("first input response=%+v calls=%d", first.Error, p.inputCalls)
	}
	if err := firstService.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	secondService := newService(root, reopened, controlTestExecutor{}, defaultConfig())
	replay := secondService.Handle(context.Background(), req)
	if replay.Error == nil || replay.Error.Code != "control-uncertain" {
		t.Fatalf("post-restart replay response=%+v", replay.Error)
	}
	if p.inputCalls != 1 {
		t.Fatalf("post-restart replay repeated physical input %d times", p.inputCalls)
	}
	if err := secondService.Close(); err != nil {
		t.Fatal(err)
	}
}
