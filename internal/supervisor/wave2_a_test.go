//go:build linux

package supervisor

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

type fastTerminalExecutor struct {
	receipt model.Receipt
	start   chan struct{}
	release chan struct{}
}

func (e *fastTerminalExecutor) Capabilities() model.Capabilities {
	return model.Capabilities{Backend: "test", MemoryTelemetry: true, CPUTelemetry: true, ProcessTelemetry: true}
}

func (e *fastTerminalExecutor) Start(run model.Run, _ model.RunSpec, _, _ io.Writer) (physical, error) {
	if e.start != nil {
		close(e.start)
		<-e.release
	}
	receipt := e.receipt
	receipt.RunID = run.ID
	return nil, &cleanStartTerminal{receipt: receipt}
}

func (*fastTerminalExecutor) Reconcile(model.Run, io.Writer, io.Writer) (reconcileResult, error) {
	return reconcileResult{}, nil
}

func newFastTerminalService(t *testing.T, executor *fastTerminalExecutor) *Service {
	t.Helper()
	root := t.TempDir()
	db, err := store.Open(filepath.Join(root, "state.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return newService(root, db, executor, defaultConfig())
}

func TestFastTerminalImportPreservesPhysicalReceipt(t *testing.T) {
	finished := time.Now().UTC().Truncate(time.Millisecond)
	started := finished.Add(-time.Second)
	code := 7
	executor := &fastTerminalExecutor{receipt: model.Receipt{
		Version: model.ProtocolVersion, Outcome: "exited", ExitCode: &code,
		StartedAt: &started, FinishedAt: finished, Cleanup: "complete",
		Resources: model.Resources{MemoryBytes: model.Metric{Status: "measured", Value: 1234}},
	}}
	s := newFastTerminalService(t, executor)
	defer s.Close()
	spec := &model.RunSpec{Argv: []string{"/bin/true"}, Cwd: t.TempDir()}
	accepted := s.create(protocol.Request{SubmissionID: "fast-terminal", Spec: spec})
	if accepted.Error != nil {
		t.Fatal(accepted.Error)
	}
	awaitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	settled := s.await(awaitCtx, accepted.Run.ID)
	if settled.Error != nil {
		t.Fatal(settled.Error)
	}
	stored, err := s.store.Get(accepted.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Receipt == nil || stored.Receipt.ExitCode == nil || *stored.Receipt.ExitCode != code || stored.Receipt.Resources.MemoryBytes.Value != 1234 || !stored.Receipt.FinishedAt.Equal(finished) || stored.StartedAt == nil || !stored.StartedAt.Equal(started) {
		t.Fatalf("physical receipt was narrowed: %+v", stored.Receipt)
	}
}

func TestCloseWaitsForAcceptedStartBeforeClosingStore(t *testing.T) {
	executor := &fastTerminalExecutor{receipt: model.Receipt{Version: model.ProtocolVersion, Outcome: "exited", FinishedAt: time.Now().UTC(), Cleanup: "complete"}, start: make(chan struct{}), release: make(chan struct{})}
	s := newFastTerminalService(t, executor)
	spec := &model.RunSpec{Argv: []string{"/bin/true"}, Cwd: t.TempDir()}
	accepted := s.create(protocol.Request{SubmissionID: "close-race", Spec: spec})
	if accepted.Error != nil {
		t.Fatal(accepted.Error)
	}
	<-executor.start
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned before accepted start finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(executor.release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestLinuxStatePathRejectsSymlink(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if err := secureStateDir(link); err == nil {
		t.Fatal("symlinked state directory accepted")
	}
	if err := os.Symlink(filepath.Join(base, "foreign"), filepath.Join(real, "state.db")); err != nil {
		t.Fatal(err)
	}
	if err := secureStateDir(real); err == nil {
		t.Fatal("symlinked state database accepted")
	}
}
