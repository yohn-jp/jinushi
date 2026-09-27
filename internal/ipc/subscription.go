package ipc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
)

const (
	followPollInterval       = 100 * time.Millisecond
	followWriteTimeout       = 30 * time.Second
	maxFollowOutputBytes     = 64 << 10
	maxFollowPageItems       = 1
	maxFollowTelemetryPoints = 16
)

// Notifier subscribes to changes relevant to one follow request. Subscribe
// must atomically register the returned subscription before it returns.
// Changes after registration must leave a coalesced wakeup available until
// Wait consumes it, including changes that happen while a snapshot is read.
type Notifier interface {
	Subscribe(context.Context, protocol.Request) (Subscription, error)
}

// Subscription waits for a relevant runtime change. Implementations should
// coalesce repeated notifications; the IPC layer needs only one pending wake.
type Subscription interface {
	Wait(context.Context) error
	Close()
}

// Follow opens a framed subscription connection for events, output, watch, or
// telemetry. Each bounded response is passed to onResponse in order. Closing
// the client cancels only this subscription request, never its Run.
func Follow(ctx context.Context, stateDir string, request protocol.Request, onResponse func(protocol.Response) error) error {
	if !request.Follow || !followOperation(request.Op) {
		return errors.New("follow requires Op=events, output, watch, or telemetry and Follow=true")
	}
	if onResponse == nil {
		return errors.New("nil follow response handler")
	}
	if request.Op == "telemetry" {
		if _, err := telemetryFollowRunID(request); err != nil {
			return err
		}
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	conn, err := Dial(dialCtx, stateDir)
	cancelDial()
	if err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer func() {
		stop()
		_ = conn.Close()
	}()

	if request.Version == 0 {
		request.Version = model.ProtocolVersion
	}
	if err := writeFrame(conn, request); err != nil {
		return fmt.Errorf("send follow request: %w", err)
	}
	for {
		var response protocol.Response
		if err := readFrame(conn, &response); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read follow frame: %w", err)
		}
		if response.Version != model.ProtocolVersion {
			return fmt.Errorf("supervisor returned unsupported protocol version %d", response.Version)
		}
		if err := onResponse(response); err != nil {
			return err
		}
	}
}

// FollowEvents preserves the original event-only client API.
func FollowEvents(ctx context.Context, stateDir string, request protocol.Request, onResponse func(protocol.Response) error) error {
	if request.Op != "events" || !request.Follow {
		return errors.New("event follow requires Op=events and Follow=true")
	}
	return Follow(ctx, stateDir, request, onResponse)
}

func followOperation(op string) bool {
	switch op {
	case "events", "output", "watch", "telemetry":
		return true
	default:
		return false
	}
}

func serveFollow(ctx context.Context, conn net.Conn, request protocol.Request, handler Handler, notifier Notifier) {
	if request.Op == "telemetry" {
		if _, err := telemetryFollowRunID(request); err != nil {
			writeFailure(conn, "invalid-request", "telemetry follow requires a consistent Run ID and query")
			return
		}
	}
	subscription, err := subscribe(ctx, request, notifier)
	if err != nil {
		writeFollowFrame(conn, errorResponse("subscription-unavailable", "subscription could not be established"))
		return
	}
	defer subscription.Close()

	request.Follow = false
	request.Limit = boundedFollowLimit(request)
	after := request.After
	offset := request.Offset
	cursor := request.Cursor
	var seenTelemetryGaps map[string]struct{}
	if request.Op == "telemetry" {
		query := *request.TelemetryQuery
		query.RunID, _ = telemetryFollowRunID(request)
		if query.Limit <= 0 || query.Limit > maxFollowTelemetryPoints {
			query.Limit = maxFollowTelemetryPoints
		}
		request.TelemetryQuery = &query
		cursor = query.Cursor
	}
	finalSeen := false
	for {
		if ctx.Err() != nil {
			return
		}
		wasFinal := finalSeen
		request.After = after
		request.Offset = offset
		request.Cursor = cursor
		if request.Op == "telemetry" {
			query := *request.TelemetryQuery
			query.Cursor = cursor
			request.TelemetryQuery = &query
		}
		response := callHandler(ctx, request, handler)
		if response.Error != nil {
			writeFollowFrame(conn, response)
			return
		}
		if request.Op == "telemetry" {
			if err := filterRepeatedTelemetryGaps(&response, &seenTelemetryGaps); err != nil {
				writeFailure(conn, "invalid-telemetry-stream", "telemetry response could not be validated")
				return
			}
		}

		progress, err := advanceFollow(request, response, &after, &offset, &cursor)
		if err != nil {
			writeFailure(conn, err.code, err.message)
			return
		}
		if response.Run != nil && isFinalRunState(response.Run.State) {
			finalSeen = true
		}
		terminalDrainComplete := request.Op == "telemetry" && wasFinal && !progress
		if progress || finalSeen && !terminalDrainComplete {
			if !writeFollowFrame(conn, response) {
				return
			}
		}

		// Give terminal output/events one extra durable read after the terminal
		// snapshot. Continue paging if that read still contains retained data.
		if wasFinal && !progress {
			return
		}
		if progress || finalSeen {
			continue
		}
		if err := subscription.Wait(ctx); err != nil {
			if ctx.Err() == nil {
				writeFailure(conn, "subscription-failed", "subscription wait failed")
			}
			return
		}
	}
}

func boundedFollowLimit(request protocol.Request) int64 {
	switch request.Op {
	case "events", "watch":
		return maxFollowPageItems
	case "output":
		if request.Limit <= 0 || request.Limit > maxFollowOutputBytes {
			return maxFollowOutputBytes
		}
		return request.Limit
	case "telemetry":
		return maxFollowTelemetryPoints
	default:
		return maxFollowPageItems
	}
}

type followFailure struct {
	code    string
	message string
}

func advanceFollow(request protocol.Request, response protocol.Response, after *uint64, offset *int64, cursor *string) (bool, *followFailure) {
	switch request.Op {
	case "events":
		for _, event := range response.Events {
			if event.RunID != request.RunID {
				return false, &followFailure{"invalid-event-stream", "event Run ID did not match the subscription"}
			}
			if event.Seq <= *after {
				return false, &followFailure{"invalid-event-stream", "event sequence did not advance"}
			}
			*after = event.Seq
		}
		if response.Gap && len(response.Events) == 0 {
			if response.RetainedFrom == 0 {
				return false, &followFailure{"invalid-event-stream", "event gap had no retained-from watermark"}
			}
			if *after < response.RetainedFrom-1 {
				*after = response.RetainedFrom - 1
			}
		}
		return len(response.Events) > 0 || response.Gap, nil
	case "output":
		data, err := base64.StdEncoding.DecodeString(response.Data)
		if err != nil {
			return false, &followFailure{"invalid-output-stream", "output response was not valid base64"}
		}
		if int64(len(data)) > request.Limit {
			return false, &followFailure{"invalid-output-stream", "output page exceeded its requested byte limit"}
		}
		if response.Gap {
			if response.RetainedFrom > uint64(1<<63-1) {
				return false, &followFailure{"invalid-output-stream", "output retained-from watermark exceeded the offset range"}
			}
			retainedFrom := int64(response.RetainedFrom)
			if retainedFrom > *offset {
				*offset = retainedFrom
			}
		}
		if *offset < 0 || int64(len(data)) > int64(1<<63-1)-*offset {
			return false, &followFailure{"invalid-output-stream", "output offset overflowed"}
		}
		*offset += int64(len(data))
		return len(data) > 0 || response.Gap, nil
	case "watch":
		if response.Gap && len(response.Events) == 0 && len(response.Runs) == 0 &&
			(response.WatchWatermark == "" || response.NextCursor != response.WatchWatermark) {
			return false, &followFailure{"invalid-watch-cursor", "watch gap without events must advance to its watermark"}
		}
		previous := *cursor
		if response.NextCursor != previous {
			*cursor = response.NextCursor
		}
		payload := len(response.Events) > 0 || len(response.Runs) > 0
		advanced := *cursor != previous
		if (payload || response.Gap) && !advanced {
			return false, &followFailure{"invalid-watch-cursor", "watch page did not advance its cursor"}
		}
		return payload || response.Gap || advanced, nil
	case "telemetry":
		return advanceTelemetryFollow(request, response, cursor)
	default:
		return false, &followFailure{"invalid-request", "unsupported follow operation"}
	}
}

func telemetryFollowRunID(request protocol.Request) (string, error) {
	if request.TelemetryQuery == nil {
		return "", errors.New("telemetry follow requires a TelemetryQuery")
	}
	queryRunID := request.TelemetryQuery.RunID
	if request.RunID != "" && queryRunID != "" && request.RunID != queryRunID {
		return "", errors.New("telemetry follow Run ID is inconsistent")
	}
	if request.RunID != "" {
		return request.RunID, nil
	}
	if queryRunID == "" {
		return "", errors.New("telemetry follow requires a Run ID")
	}
	return queryRunID, nil
}

func advanceTelemetryFollow(request protocol.Request, response protocol.Response, cursor *string) (bool, *followFailure) {
	runID, err := telemetryFollowRunID(request)
	if err != nil || response.Telemetry == nil || response.Telemetry.RunID != runID || response.Run == nil || response.Run.ID != runID {
		return false, &followFailure{"invalid-telemetry-stream", "telemetry response did not match the subscription Run"}
	}
	page := response.Telemetry
	hasPoints := len(page.Samples) > 0 || len(page.Aggregates) > 0
	if page.NextCursor != "" {
		if page.NextCursor == *cursor || page.Watermark == "" || page.Watermark != page.NextCursor || !hasPoints {
			return false, &followFailure{"invalid-telemetry-cursor", "telemetry page cursor did not match its delivered watermark"}
		}
		*cursor = page.NextCursor
		return true, nil
	}
	if hasPoints {
		if page.Watermark == "" || page.Watermark == *cursor {
			return false, &followFailure{"invalid-telemetry-cursor", "telemetry points did not advance their watermark"}
		}
		*cursor = page.Watermark
		return true, nil
	}
	if page.Watermark != "" && page.Watermark != *cursor {
		*cursor = page.Watermark
		return true, nil
	}
	return len(page.Gaps) > 0, nil
}

// QueryTelemetry repeats retained gaps on every cursor page. During one live
// subscription, emit only gaps that appeared since the previous snapshot;
// keep only the current retained set so this state remains bounded by store
// retention even if the subscription runs indefinitely.
func filterRepeatedTelemetryGaps(response *protocol.Response, previous *map[string]struct{}) error {
	if response.Telemetry == nil {
		return nil
	}
	page := *response.Telemetry
	current := make(map[string]struct{}, len(page.Gaps))
	newGaps := make([]model.TelemetryGap, 0, len(page.Gaps))
	for _, gap := range page.Gaps {
		encoded, err := json.Marshal(gap)
		if err != nil {
			return err
		}
		key := string(encoded)
		if _, exists := current[key]; exists {
			continue
		}
		current[key] = struct{}{}
		if *previous == nil {
			newGaps = append(newGaps, gap)
			continue
		}
		if _, exists := (*previous)[key]; !exists {
			newGaps = append(newGaps, gap)
		}
	}
	page.Gaps = newGaps
	response.Telemetry = &page
	*previous = current
	return nil
}

func isFinalRunState(state model.State) bool {
	return state == model.Terminal || state == model.Uncertain
}

func subscribe(ctx context.Context, request protocol.Request, notifier Notifier) (Subscription, error) {
	if notifier == nil {
		return pollingSubscription{}, nil
	}
	subscription, err := notifier.Subscribe(ctx, request)
	if err != nil {
		return nil, err
	}
	if subscription == nil {
		return nil, errors.New("notifier returned a nil subscription")
	}
	return subscription, nil
}

type pollingSubscription struct{}

func (pollingSubscription) Wait(ctx context.Context) error {
	timer := time.NewTimer(followPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (pollingSubscription) Close() {}

func writeFollowFrame(conn net.Conn, response protocol.Response) bool {
	payload, err := json.Marshal(response)
	if err == nil && (len(payload) == 0 || len(payload) > protocol.MaxFrame) {
		err = ErrFrameTooLarge
	}
	if err != nil {
		_ = conn.SetWriteDeadline(time.Now().Add(followWriteTimeout))
		writeFailure(conn, "invalid-response", "supervisor response could not be encoded within the protocol limit")
		_ = conn.SetWriteDeadline(time.Time{})
		return false
	}
	_ = conn.SetWriteDeadline(time.Now().Add(followWriteTimeout))
	err = writePayloadFrame(conn, payload)
	_ = conn.SetWriteDeadline(time.Time{})
	if err != nil {
		return false
	}
	return true
}
