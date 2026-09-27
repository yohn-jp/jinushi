package protocol

import "github.com/yohn-jp/jinushi/internal/model"

const MaxFrame = 1 << 20

type Request struct {
	Version            int                   `json:"version"`
	Op                 string                `json:"op"`
	RunID              string                `json:"runId,omitempty"`
	SubmissionID       string                `json:"submissionId,omitempty"`
	RequestID          string                `json:"requestId,omitempty"`
	ExpectedGeneration uint64                `json:"expectedGeneration,omitempty"`
	AttachID           string                `json:"attachId,omitempty"`
	Follow             bool                  `json:"follow,omitempty"`
	Cursor             string                `json:"cursor,omitempty"`
	Spec               *model.RunSpec        `json:"spec,omitempty"`
	After              uint64                `json:"after,omitempty"`
	Offset             int64                 `json:"offset,omitempty"`
	Limit              int64                 `json:"limit,omitempty"`
	Stream             string                `json:"stream,omitempty"`
	Data               string                `json:"data,omitempty"`
	Signal             string                `json:"signal,omitempty"`
	Rows               int                   `json:"rows,omitempty"`
	Cols               int                   `json:"cols,omitempty"`
	LeaseGeneration    uint64                `json:"leaseGeneration,omitempty"`
	LeaseMs            int64                 `json:"leaseMs,omitempty"`
	TelemetryQuery     *model.TelemetryQuery `json:"telemetryQuery,omitempty"`
	MemoryHighBytes    int64                 `json:"memoryHighBytes,omitempty"`
	CPUQuotaPercent    int64                 `json:"cpuQuotaPercent,omitempty"`
}

type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Response struct {
	Version      int                      `json:"version"`
	Run          *model.Run               `json:"run,omitempty"`
	Runs         []model.Run              `json:"runs,omitempty"`
	NextCursor   string                   `json:"nextCursor"`
	Events       []model.Event            `json:"events,omitempty"`
	RetainedFrom uint64                   `json:"retainedFrom,omitempty"`
	Gap          bool                     `json:"gap,omitempty"`
	Data         string                   `json:"data,omitempty"`
	Capabilities *model.Capabilities      `json:"capabilities,omitempty"`
	AttachID     string                   `json:"attachId,omitempty"`
	Telemetry    *model.TelemetryResponse `json:"telemetry,omitempty"`
	Error        *Failure                 `json:"error,omitempty"`
}
