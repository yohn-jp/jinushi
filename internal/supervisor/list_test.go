package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

func TestListPagesStayWithinIPCFrame(t *testing.T) {
	db, err := store.Open(t.TempDir()+"/state.db", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for i := 0; i < 90; i++ {
		run := model.Run{
			ID: fmt.Sprintf("run-%03d", i), State: model.Accepted, Generation: 1,
			Spec: model.RunSpec{Argv: []string{"true"}, Cwd: "/", Correlation: map[string]string{"opaque": strings.Repeat("x", 20000)}},
		}
		if _, _, err := db.Create(run, nil); err != nil {
			t.Fatal(err)
		}
	}
	svc := newService(t.TempDir(), db, nil, defaultConfig())
	cursor := ""
	seen := 0
	for {
		resp := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "list", Cursor: cursor, Limit: 128})
		if resp.Error != nil {
			t.Fatalf("list after %q: %+v", cursor, resp.Error)
		}
		encoded, err := json.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) >= protocol.MaxFrame || len(resp.Runs) == 0 {
			t.Fatalf("page size=%d runs=%d", len(encoded), len(resp.Runs))
		}
		for _, run := range resp.Runs {
			if run.ID != fmt.Sprintf("run-%03d", seen) {
				t.Fatalf("run %d = %q", seen, run.ID)
			}
			seen++
		}
		if resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}
	if seen != 90 {
		t.Fatalf("listed %d runs, want 90", seen)
	}
}

type frameTestExecutor struct{}

func (frameTestExecutor) Capabilities() model.Capabilities { return model.Capabilities{} }
func (frameTestExecutor) Start(model.Run, model.RunSpec, io.Writer, io.Writer) (physical, error) {
	panic("oversized Run must not start")
}
func (frameTestExecutor) Reconcile(model.Run, io.Writer, io.Writer) (reconcileResult, error) {
	panic("oversized Run must not reconcile")
}

func TestOversizedAcceptedResponseDoesNotCreateRun(t *testing.T) {
	db, err := store.Open(t.TempDir()+"/state.db", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	argv := []string{"/bin/true"}
	for i := 0; i < 31; i++ {
		argv = append(argv, strings.Repeat("a", 32768))
	}
	argv = append(argv, strings.Repeat("b", 32000))
	spec := model.RunSpec{Argv: argv, Cwd: t.TempDir()}
	request, err := json.Marshal(protocol.Request{Version: model.ProtocolVersion, Op: "run", Spec: &spec})
	if err != nil || len(request) >= protocol.MaxFrame {
		t.Fatalf("test request size=%d err=%v", len(request), err)
	}
	svc := newService(t.TempDir(), db, frameTestExecutor{}, defaultConfig())
	resp := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "run", Spec: &spec})
	if resp.Error == nil || resp.Error.Code != "response-too-large" {
		t.Fatalf("oversized acceptance response=%+v", resp.Error)
	}
	runs, err := db.List()
	if err != nil || len(runs) != 0 {
		t.Fatalf("oversized request persisted %d Runs: %v", len(runs), err)
	}
}
