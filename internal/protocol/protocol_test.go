package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/yohn-jp/jinushi/internal/model"
)

func TestHostEnvelopeAndWriterLeaseWireFields(t *testing.T) {
	expiresAt := time.Date(2026, time.September, 27, 12, 34, 56, 0, time.UTC)
	request, err := json.Marshal(Request{WriterToken: "writer-token"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(request), `"writerToken":"writer-token"`) {
		t.Fatalf("writer token missing from request: %s", request)
	}

	response, err := json.Marshal(Response{
		HostEnvelope: &model.HostEnvelopeStatus{
			Status:      "unsupported",
			Config:      model.HostEnvelopeConfig{MemoryBytes: 1024, TaskCount: 8, MaxActiveRuns: 2},
			MemoryBytes: model.Metric{Status: "unsupported"},
		},
		WriterToken:          "writer-token",
		WriterLeaseExpiresAt: &expiresAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		`"hostEnvelope"`, `"status":"unsupported"`, `"memoryBytes":1024`,
		`"writerToken":"writer-token"`, `"writerLeaseExpiresAt":"2026-09-27T12:34:56Z"`,
	} {
		if !strings.Contains(string(response), field) {
			t.Fatalf("response missing %s: %s", field, response)
		}
	}
}
