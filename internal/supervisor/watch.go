package supervisor

import (
	"encoding/json"
	"errors"

	"github.com/yohn-jp/jinushi/internal/model"
	"github.com/yohn-jp/jinushi/internal/protocol"
	"github.com/yohn-jp/jinushi/internal/store"
)

const (
	defaultWatchPageLimit = 64
	maxWatchPageLimit     = 128
)

// watch returns one bounded all-Run event page. The durable store owns the
// opaque global cursor and gap decision; the IPC notifier only controls when
// callers retry this snapshot read.
func (s *Service) watch(req protocol.Request) protocol.Response {
	if req.RunID != "" {
		return failure("invalid-request", "all-Run watch does not accept a Run ID")
	}
	limit := defaultWatchPageLimit
	if req.Limit > 0 {
		if req.Limit < maxWatchPageLimit {
			limit = int(req.Limit)
		} else {
			limit = maxWatchPageLimit
		}
	}
	page, err := s.store.WatchPage(req.Cursor, limit)
	if errors.Is(err, store.ErrInvalidWatchCursor) {
		return failure("invalid-cursor", "all-Run watch cursor is invalid")
	}
	if err != nil {
		return failure("storage-failure", "all-Run watch history could not be read")
	}

	out := response()
	out.Gap = page.Gap
	out.NextCursor = req.Cursor
	out.WatchWatermark = page.Watermark
	out.WatchRetainedFrom = page.RetainedFrom
	for _, entry := range page.Events {
		candidate := out
		candidate.Events = append(append([]model.Event(nil), out.Events...), entry.Event)
		candidate.NextCursor = entry.Cursor
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return failure("storage-failure", "all-Run watch page could not be encoded")
		}
		if len(encoded) >= protocol.MaxFrame-4096 {
			if len(out.Events) == 0 {
				return failure("response-too-large", "all-Run watch event exceeds the IPC response limit")
			}
			break
		}
		out = candidate
	}
	if out.Gap && len(out.Events) == 0 {
		if page.Watermark == "" {
			return failure("storage-failure", "all-Run watch gap has no current watermark")
		}
		// When retention removed every event after the requested cursor, skip
		// directly to the observed watermark so reconnect does not repeat the
		// same gap forever.
		out.NextCursor = page.Watermark
	}
	return out
}
