package supervisor

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

func TestServiceWriterLeaseGatesInputResizeAndCloseInput(t *testing.T) {
	p := &controlTestPhysical{}
	svc, _ := newControlTestService(t, p)
	const runID = "run_control_test"

	first := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: "observer-1"})
	if first.Error != nil || first.WriterToken == "" || first.WriterLeaseExpiresAt == nil {
		t.Fatalf("first writer acquire = %+v", first.Error)
	}
	if retry := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: "observer-1"}); retry.Error != nil || retry.WriterToken != first.WriterToken {
		t.Fatalf("same-owner acquire retry = %+v token=%q", retry.Error, retry.WriterToken)
	}
	if conflict := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: "observer-2"}); conflict.Error == nil || conflict.Error.Code != "writer-conflict" {
		t.Fatalf("second writer acquire = %+v, want writer-conflict", conflict.Error)
	}

	input := protocol.Request{
		Version: model.ProtocolVersion, Op: "input", RunID: runID, Stream: "stdin",
		Data: base64.StdEncoding.EncodeToString([]byte("single-writer")), RequestID: "writer-input", ExpectedGeneration: 1,
	}
	if missing := svc.Handle(context.Background(), input); missing.Error == nil || missing.Error.Code != "writer-token-required" {
		t.Fatalf("input without a writer token = %+v", missing.Error)
	}
	input.WriterToken = first.WriterToken
	written := svc.Handle(context.Background(), input)
	if written.Error != nil || written.Run == nil || written.Run.Generation != 2 {
		t.Fatalf("authorized input = %+v", written.Error)
	}
	if p.inputCalls != 1 || string(p.input) != "single-writer" {
		t.Fatalf("input calls=%d bytes=%q", p.inputCalls, p.input)
	}

	renewed := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-renew", RunID: runID, AttachID: "observer-1", WriterToken: first.WriterToken})
	if renewed.Error != nil || renewed.WriterLeaseExpiresAt == nil || !renewed.WriterLeaseExpiresAt.After(*first.WriterLeaseExpiresAt) {
		t.Fatalf("writer renewal = %+v expiry=%v", renewed.Error, renewed.WriterLeaseExpiresAt)
	}
	if released := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-release", RunID: runID, AttachID: "observer-1", WriterToken: first.WriterToken}); released.Error != nil {
		t.Fatalf("writer release = %+v", released.Error)
	}
	if replay := svc.Handle(context.Background(), input); replay.Error != nil || replay.Run == nil || replay.Run.Generation != 2 {
		t.Fatalf("input replay after lease release = %+v", replay.Error)
	}
	if stale := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "input", RunID: runID, Stream: "stdin", Data: input.Data, RequestID: "stale-writer-input", ExpectedGeneration: 2, WriterToken: first.WriterToken}); stale.Error == nil || stale.Error.Code != "writer-stale" {
		t.Fatalf("stale writer input = %+v", stale.Error)
	}
	if p.inputCalls != 1 {
		t.Fatalf("stale/replayed inputs reached backend %d times", p.inputCalls)
	}

	second := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: "observer-2"})
	if second.Error != nil || second.WriterToken == "" {
		t.Fatalf("writer reacquire after release = %+v", second.Error)
	}
	resize := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "resize", RunID: runID, Rows: 24, Cols: 80, RequestID: "writer-resize", ExpectedGeneration: 2, WriterToken: second.WriterToken})
	if resize.Error != nil || resize.Run == nil || resize.Run.Generation != 3 || p.resizeCalls != 1 {
		t.Fatalf("authorized resize = %+v calls=%d", resize.Error, p.resizeCalls)
	}
	closeInput := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "close-input", RunID: runID, RequestID: "writer-close-input", ExpectedGeneration: 3, WriterToken: second.WriterToken})
	if closeInput.Error != nil || closeInput.Run == nil || closeInput.Run.Generation != 4 || p.closeCalls != 1 {
		t.Fatalf("authorized close-input = %+v calls=%d", closeInput.Error, p.closeCalls)
	}
}

func TestServiceWriterGateSerializesLeaseHandoffWithPhysicalInput(t *testing.T) {
	p := &controlTestPhysical{inputStart: make(chan struct{}), inputResume: make(chan struct{})}
	svc, _ := newControlTestService(t, p)
	const runID = "run_control_test"
	lease := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: "observer-old"})
	if lease.Error != nil || lease.WriterToken == "" {
		t.Fatalf("writer acquire = %+v", lease.Error)
	}

	inputDone := make(chan protocol.Response, 1)
	go func() {
		inputDone <- svc.Handle(context.Background(), protocol.Request{
			Version: model.ProtocolVersion, Op: "input", RunID: runID, Stream: "stdin",
			Data: base64.StdEncoding.EncodeToString([]byte("physical")), RequestID: "gated-input", ExpectedGeneration: 1,
			WriterToken: lease.WriterToken,
		})
	}()
	<-p.inputStart

	releaseStarted := make(chan struct{})
	releaseDone := make(chan protocol.Response, 1)
	go func() {
		close(releaseStarted)
		releaseDone <- svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-release", RunID: runID, AttachID: "observer-old", WriterToken: lease.WriterToken})
	}()
	<-releaseStarted
	select {
	case response := <-releaseDone:
		t.Fatalf("lease release completed during physical input: %+v", response.Error)
	case <-time.After(20 * time.Millisecond):
	}

	close(p.inputResume)
	if response := <-inputDone; response.Error != nil {
		t.Fatalf("physical input response = %+v", response.Error)
	}
	if response := <-releaseDone; response.Error != nil {
		t.Fatalf("lease release after physical input = %+v", response.Error)
	}
	next := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: "observer-next"})
	if next.Error != nil || next.WriterToken == "" || next.WriterToken == lease.WriterToken {
		t.Fatalf("writer handoff = %+v", next.Error)
	}
}

func TestWriterLeaseServiceDetachAndTerminalCleanup(t *testing.T) {
	p := &controlTestPhysical{}
	svc, _ := newControlTestService(t, p)
	const runID = "run_control_test"
	lease := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: "owner"})
	if lease.Error != nil {
		t.Fatal(lease.Error)
	}
	svc.writerLeases.Detach(runID, "owner")
	if stale := svc.writerLeases.Validate(runID, lease.WriterToken); !errors.Is(stale, ErrWriterStale) {
		t.Fatalf("detached writer validation = %v, want stale", stale)
	}
	if next := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "writer-acquire", RunID: runID, AttachID: "next"}); next.Error != nil {
		t.Fatalf("reacquire after detach = %+v", next.Error)
	}
	svc.writerLeases.ClearRun(runID)
	if got := svc.writerLeases.SweepRun(runID); got {
		t.Fatal("ClearRun left an entry for SweepRun")
	}
}

func TestAttachmentRenewExtendsIdleFollowLease(t *testing.T) {
	p := &controlTestPhysical{}
	svc, _ := newControlTestService(t, p)
	const runID = "run_control_test"
	a := svc.active[runID]
	if a == nil {
		t.Fatal("test Run is not active")
	}
	a.mu.Lock()
	a.attachments = map[string]time.Time{"idle-observer": time.Now().Add(time.Second)}
	a.mu.Unlock()

	renewed := svc.Handle(context.Background(), protocol.Request{Version: model.ProtocolVersion, Op: "attach-renew", RunID: runID, AttachID: "idle-observer"})
	if renewed.Error != nil {
		t.Fatalf("attachment renewal = %+v", renewed.Error)
	}
	a.mu.Lock()
	expiresAt := a.attachments["idle-observer"]
	a.mu.Unlock()
	if !expiresAt.After(time.Now().Add(attachmentLeaseTTL - 2*time.Second)) {
		t.Fatalf("attachment expiry=%s; renewal did not extend the idle lease", expiresAt)
	}
}
