package store

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/yohn-jp/jinushi/internal/model"
)

var (
	ErrInvalidSubmissionID      = errors.New("invalid submission identity")
	ErrSubmissionConflict       = errors.New("submission identity conflicts with accepted specification")
	ErrSubmissionCapacity       = errors.New("submission idempotency capacity reached")
	ErrSubmissionRunCollected   = errors.New("submission Run detail was collected")
	ErrInvalidRequestID         = errors.New("invalid control request identity")
	ErrControlRequestConflict   = errors.New("control request identity conflicts with mutation")
	ErrControlRequestCapacity   = errors.New("control request idempotency capacity reached")
	ErrStaleControlGeneration   = errors.New("stale control generation")
	ErrControlRunFinal          = errors.New("Run is not mutable")
	ErrControlRequestFinalized  = errors.New("control request already has a final disposition")
	ErrInvalidIdempotencyDigest = errors.New("invalid idempotency digest")
)

const (
	// SubmissionIDMaxBytes bounds caller-selected keys retained in the durable
	// submission index.
	SubmissionIDMaxBytes = 128
	// SubmissionRetentionWindow bounds the period in which an accepted key is
	// guaranteed to resolve to the same Run.
	SubmissionRetentionWindow = 30 * 24 * time.Hour
	MaxRetainedSubmissions    = 100_000

	RequestIDMaxBytes         = 128
	ControlRequestWindow      = 24 * time.Hour
	MaxControlRequestsPerRun  = 8192
	MaxControlRequestsRuntime = 100_000
	maxControlFailureCode     = 64
	maxControlFailureMessage  = 256
)

const (
	submissionsBucketName      = "submissions"
	submissionRunsBucketName   = "submission-runs"
	submissionExpiryBucketName = "submission-expiry"
	controlRequestsBucketName  = "control-requests"
	controlRequestExpiryBucket = "control-request-expiry"
)

// SubmissionResult describes an atomic accepted Run identity binding.
// Created is false for a retry that resolved to a previously accepted Run.
type SubmissionResult struct {
	Run     model.Run
	Event   *model.Event
	Created bool
}

type submissionBinding struct {
	RunID        string     `json:"runId"`
	SpecDigest   string     `json:"specDigest"`
	CreatedAt    time.Time  `json:"createdAt"`
	ExpiresAt    time.Time  `json:"expiresAt"`
	CollectedRun *model.Run `json:"collectedRun,omitempty"`
}

// AcceptSubmission atomically creates a Run, its initial event, and its
// private idempotency binding. A concurrent retry with the same ID and digest
// resolves to the first Run without creating another lifecycle record.
func (s *Store) AcceptSubmission(run model.Run, event *model.Event, submissionID, specDigest string, now time.Time) (SubmissionResult, error) {
	if err := validateSubmissionIdentity(submissionID, specDigest); err != nil {
		return SubmissionResult{}, err
	}
	if err := validateRun(run); err != nil {
		return SubmissionResult{}, err
	}
	if err := validateNewRunOutput(run.Output); err != nil {
		return SubmissionResult{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if run.CreatedAt.IsZero() {
		run.CreatedAt = now
	}
	run.Output.HistoryComplete = true
	var result SubmissionResult
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := ensureIdempotencyBuckets(tx); err != nil {
			return err
		}
		submissions := tx.Bucket([]byte(submissionsBucketName))
		runIndex := tx.Bucket([]byte(submissionRunsBucketName))
		expiry := tx.Bucket([]byte(submissionExpiryBucketName))
		if err := pruneExpiredSubmissionsTx(submissions, runIndex, expiry, now); err != nil {
			return err
		}
		if raw := submissions.Get([]byte(submissionID)); raw != nil {
			binding, err := decodeSubmissionBinding(raw)
			if err != nil {
				return err
			}
			if binding.SpecDigest != specDigest {
				return ErrSubmissionConflict
			}
			if !binding.ExpiresAt.After(now) {
				if err := deleteSubmissionBindingTx(submissions, runIndex, expiry, submissionID, binding); err != nil {
					return err
				}
			} else {
				if binding.CollectedRun != nil {
					if binding.CollectedRun.ID != binding.RunID || binding.CollectedRun.State != model.Terminal {
						return errors.New("invalid collected submission Run")
					}
					result = SubmissionResult{Run: *binding.CollectedRun}
					return nil
				}
				runBytes := tx.Bucket([]byte(runsBucketName)).Get([]byte(binding.RunID))
				if runBytes == nil {
					return ErrSubmissionRunCollected
				}
				var existing model.Run
				if err := json.Unmarshal(runBytes, &existing); err != nil {
					return fmt.Errorf("decode bound Run: %w", err)
				}
				result = SubmissionResult{Run: existing}
				return nil
			}
		}
		if submissions.Stats().KeyN >= MaxRetainedSubmissions {
			return ErrSubmissionCapacity
		}
		if tx.Bucket([]byte(runsBucketName)).Get([]byte(run.ID)) != nil {
			return ErrRunExists
		}
		if runIndex.Get([]byte(run.ID)) != nil {
			return ErrRunExists
		}
		var appended *model.Event
		if event != nil {
			copy, err := s.appendEventTx(tx, run.ID, *event, run.State != model.Terminal)
			if err != nil {
				return err
			}
			appended = &copy
		}
		if run.State == model.Terminal {
			meta, err := readEventMetaTx(tx, run.ID)
			if err != nil {
				return err
			}
			stampReceiptEventRange(run.Receipt, meta)
		}
		encodedRun, err := json.Marshal(run)
		if err != nil {
			return fmt.Errorf("encode Run: %w", err)
		}
		binding := submissionBinding{RunID: run.ID, SpecDigest: specDigest, CreatedAt: now, ExpiresAt: now.Add(SubmissionRetentionWindow)}
		encodedBinding, err := json.Marshal(binding)
		if err != nil {
			return fmt.Errorf("encode submission binding: %w", err)
		}
		if err := tx.Bucket([]byte(runsBucketName)).Put([]byte(run.ID), encodedRun); err != nil {
			return fmt.Errorf("persist Run: %w", err)
		}
		if err := submissions.Put([]byte(submissionID), encodedBinding); err != nil {
			return fmt.Errorf("persist submission binding: %w", err)
		}
		if err := runIndex.Put([]byte(run.ID), []byte(submissionID)); err != nil {
			return fmt.Errorf("index submission Run: %w", err)
		}
		if err := expiry.Put(expiryIndexKey(binding.ExpiresAt, []byte(submissionID)), nil); err != nil {
			return fmt.Errorf("index submission expiry: %w", err)
		}
		result = SubmissionResult{Run: run, Event: appended, Created: true}
		return nil
	})
	if err != nil {
		return SubmissionResult{}, err
	}
	return result, nil
}

// ResolveSubmission returns an existing Run for a matching live binding. It
// removes only expired key metadata; Run and terminal evidence are retained by
// the GC guard until the binding expires.
func (s *Store) ResolveSubmission(submissionID, specDigest string, now time.Time) (model.Run, bool, error) {
	if err := validateSubmissionIdentity(submissionID, specDigest); err != nil {
		return model.Run{}, false, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	var run model.Run
	found := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		submissions := tx.Bucket([]byte(submissionsBucketName))
		if submissions == nil {
			return nil
		}
		raw := submissions.Get([]byte(submissionID))
		if raw == nil {
			return nil
		}
		binding, err := decodeSubmissionBinding(raw)
		if err != nil {
			return err
		}
		if !binding.ExpiresAt.After(now) {
			return deleteSubmissionBindingTx(submissions, tx.Bucket([]byte(submissionRunsBucketName)), tx.Bucket([]byte(submissionExpiryBucketName)), submissionID, binding)
		}
		if binding.SpecDigest != specDigest {
			return ErrSubmissionConflict
		}
		if binding.CollectedRun != nil {
			if binding.CollectedRun.ID != binding.RunID || binding.CollectedRun.State != model.Terminal {
				return errors.New("invalid collected submission Run")
			}
			run = *binding.CollectedRun
			found = true
			return nil
		}
		runBytes := tx.Bucket([]byte(runsBucketName)).Get([]byte(binding.RunID))
		if runBytes == nil {
			return ErrSubmissionRunCollected
		}
		if err := json.Unmarshal(runBytes, &run); err != nil {
			return fmt.Errorf("decode bound Run: %w", err)
		}
		found = true
		return nil
	})
	return run, found, err
}

// submissionProtectsRunTx reports whether a live submission binding must be
// preserved when terminal Run detail is collected. A live binding can be
// converted to a compact tombstone in that same GC transaction.
func submissionProtectsRunTx(tx *bolt.Tx, runID string, now time.Time) (bool, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	runIndex := tx.Bucket([]byte(submissionRunsBucketName))
	submissions := tx.Bucket([]byte(submissionsBucketName))
	if runIndex == nil || submissions == nil {
		return false, nil
	}
	submissionID := runIndex.Get([]byte(runID))
	if submissionID == nil {
		return false, nil
	}
	raw := submissions.Get(submissionID)
	if raw == nil {
		return false, nil
	}
	binding, err := decodeSubmissionBinding(raw)
	if err != nil {
		return false, err
	}
	if binding.RunID != runID {
		return false, errors.New("submission Run index mismatch")
	}
	return binding.ExpiresAt.After(now), nil
}

// submissionRunTombstone returns a compact terminal Run projection for a
// submission retry after detail GC. It preserves only truthful terminal
// receipt identity/outcome and explicitly marks resource, event, and output
// history as incomplete.
func submissionRunTombstone(run model.Run) model.Run {
	stub := model.Run{
		ID: run.ID, State: model.Terminal, Generation: run.Generation, CreatedAt: run.CreatedAt,
		StartedAt: run.StartedAt, FinishedAt: run.FinishedAt,
		EffectiveCapabilities: run.EffectiveCapabilities, ResourceGap: true,
		TerminationReason: run.TerminationReason,
	}
	stub.Output = compactOutputHistory(run.Output)
	if run.Receipt == nil {
		return stub
	}
	receipt := *run.Receipt
	receipt.Resources = compactResources(receipt.Resources)
	receipt.Output = compactOutputHistory(receipt.Output)
	receipt.EventHistoryComplete = false
	if receipt.EventLastSeq < math.MaxUint64 {
		receipt.EventRetainedFrom = receipt.EventLastSeq + 1
	} else {
		receipt.EventRetainedFrom = math.MaxUint64
	}
	receipt.EvidenceIncomplete = true
	stub.Resources = receipt.Resources
	stub.Output = receipt.Output
	stub.Receipt = &receipt
	return stub
}

func compactResources(resources model.Resources) model.Resources {
	resources.MemoryBytes = unavailableMetric(resources.MemoryBytes)
	resources.PeakMemoryBytes = unavailableMetric(resources.PeakMemoryBytes)
	resources.CPUTimeNs = unavailableMetric(resources.CPUTimeNs)
	resources.ProcessCount = unavailableMetric(resources.ProcessCount)
	resources.PeakProcessCount = unavailableMetric(resources.PeakProcessCount)
	resources.TaskCount = unavailableMetric(resources.TaskCount)
	resources.PeakTaskCount = unavailableMetric(resources.PeakTaskCount)
	return resources
}

func unavailableMetric(metric model.Metric) model.Metric {
	if metric.Status == "unsupported" {
		return model.Metric{Status: "unsupported"}
	}
	return model.Metric{Status: "unavailable"}
}

func compactOutputHistory(output model.Output) model.Output {
	output.HistoryComplete = false
	for _, stream := range []*model.OutputStream{&output.Stdout, &output.Stderr, &output.PTY} {
		if stream.ObservedBytes > 0 {
			stream.Truncated = true
		}
		stream.RetainedBytes = 0
		stream.RetainedFrom = stream.ObservedBytes
	}
	return output
}

// markSubmissionCollectedTx records a bounded same-identity replay stub. GC
// calls it before deleting the full Run in its current write transaction.
func markSubmissionCollectedTx(tx *bolt.Tx, runID string, stub model.Run, now time.Time) (bool, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	runIndex := tx.Bucket([]byte(submissionRunsBucketName))
	submissions := tx.Bucket([]byte(submissionsBucketName))
	if runIndex == nil || submissions == nil {
		return false, nil
	}
	submissionID := runIndex.Get([]byte(runID))
	if submissionID == nil {
		return false, nil
	}
	raw := submissions.Get(submissionID)
	if raw == nil {
		return false, nil
	}
	binding, err := decodeSubmissionBinding(raw)
	if err != nil {
		return false, err
	}
	if binding.RunID != runID {
		return false, errors.New("submission Run index mismatch")
	}
	if !binding.ExpiresAt.After(now) {
		return false, nil
	}
	if binding.CollectedRun != nil {
		return true, nil
	}
	if stub.ID != runID || stub.State != model.Terminal || stub.Receipt == nil || stub.Receipt.RunID != runID || !stub.Receipt.EvidenceIncomplete || stub.Receipt.EventHistoryComplete || stub.Receipt.Output.HistoryComplete {
		return false, errors.New("invalid submission Run tombstone")
	}
	binding.CollectedRun = &stub
	encoded, err := json.Marshal(binding)
	if err != nil {
		return false, fmt.Errorf("encode collected submission binding: %w", err)
	}
	if err := submissions.Put(submissionID, encoded); err != nil {
		return false, fmt.Errorf("persist collected submission binding: %w", err)
	}
	return true, nil
}

func validateSubmissionIdentity(submissionID, digest string) error {
	if len(submissionID) == 0 || len(submissionID) > SubmissionIDMaxBytes {
		return ErrInvalidSubmissionID
	}
	return validateDigest(digest)
}

func validateDigest(digest string) error {
	if len(digest) != 64 {
		return ErrInvalidIdempotencyDigest
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return ErrInvalidIdempotencyDigest
	}
	return nil
}

func ensureIdempotencyBuckets(tx *bolt.Tx) error {
	for _, name := range [][]byte{
		[]byte(submissionsBucketName),
		[]byte(submissionRunsBucketName),
		[]byte(submissionExpiryBucketName),
		[]byte(controlRequestsBucketName),
		[]byte(controlRequestExpiryBucket),
	} {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return fmt.Errorf("create idempotency bucket %q: %w", name, err)
		}
	}
	return nil
}

func decodeSubmissionBinding(raw []byte) (submissionBinding, error) {
	var binding submissionBinding
	if err := json.Unmarshal(raw, &binding); err != nil {
		return submissionBinding{}, fmt.Errorf("decode submission binding: %w", err)
	}
	if binding.RunID == "" || binding.SpecDigest == "" || binding.CreatedAt.IsZero() || binding.ExpiresAt.IsZero() {
		return submissionBinding{}, errors.New("invalid submission binding")
	}
	return binding, nil
}

func expiryIndexKey(expiry time.Time, key []byte) []byte {
	result := make([]byte, 8+len(key))
	binary.BigEndian.PutUint64(result[:8], uint64(expiry.UnixNano()))
	copy(result[8:], key)
	return result
}

func pruneExpiredSubmissionsTx(submissions, runIndex, expiry *bolt.Bucket, now time.Time) error {
	if submissions == nil || runIndex == nil || expiry == nil {
		return nil
	}
	cutoff := make([]byte, 8)
	binary.BigEndian.PutUint64(cutoff, uint64(now.UnixNano()))
	for {
		cursor := expiry.Cursor()
		key, _ := cursor.First()
		if key == nil || len(key) < 8 || bytes.Compare(key[:8], cutoff) > 0 {
			return nil
		}
		submissionID := append([]byte(nil), key[8:]...)
		raw := submissions.Get(submissionID)
		if raw != nil {
			binding, err := decodeSubmissionBinding(raw)
			if err != nil {
				return err
			}
			if err := deleteSubmissionBindingTx(submissions, runIndex, expiry, string(submissionID), binding); err != nil {
				return err
			}
		} else if err := expiry.Delete(key); err != nil {
			return err
		}
	}
}

func deleteSubmissionBindingTx(submissions, runIndex, expiry *bolt.Bucket, submissionID string, binding submissionBinding) error {
	if submissions != nil {
		if err := submissions.Delete([]byte(submissionID)); err != nil {
			return err
		}
	}
	if runIndex != nil && bytes.Equal(runIndex.Get([]byte(binding.RunID)), []byte(submissionID)) {
		if err := runIndex.Delete([]byte(binding.RunID)); err != nil {
			return err
		}
	}
	if expiry != nil {
		if err := expiry.Delete(expiryIndexKey(binding.ExpiresAt, []byte(submissionID))); err != nil {
			return err
		}
	}
	return nil
}

type ControlRequestStatus string

const (
	ControlRequestPending   ControlRequestStatus = "pending"
	ControlRequestSucceeded ControlRequestStatus = "succeeded"
	ControlRequestUncertain ControlRequestStatus = "uncertain"
)

// ControlRequest is bounded durable evidence for one caller-selected physical
// mutation identity. It never contains input bytes or environment values.
type ControlRequest struct {
	RunID              string               `json:"runId"`
	RequestID          string               `json:"requestId"`
	MutationDigest     string               `json:"mutationDigest"`
	ExpectedGeneration uint64               `json:"expectedGeneration"`
	AcceptedGeneration uint64               `json:"acceptedGeneration"`
	CreatedAt          time.Time            `json:"createdAt"`
	ExpiresAt          time.Time            `json:"expiresAt"`
	Status             ControlRequestStatus `json:"status"`
	FailureCode        string               `json:"failureCode,omitempty"`
	FailureMessage     string               `json:"failureMessage,omitempty"`
}

type ControlBeginResult struct {
	Run     model.Run
	Request ControlRequest
	Created bool
}

// BeginControlRequest durably claims one non-idempotent physical mutation and
// advances Run.Generation in the same transaction. A retry reads the prior
// disposition before checking currentness, allowing a response-lost request to
// receive its original result without repeating its side effect.
func (s *Store) BeginControlRequest(runID, requestID, mutationDigest string, expectedGeneration uint64, now time.Time) (ControlBeginResult, error) {
	if err := validateControlIdentity(runID, requestID, mutationDigest, expectedGeneration); err != nil {
		return ControlBeginResult{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	var result ControlBeginResult
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := ensureIdempotencyBuckets(tx); err != nil {
			return err
		}
		requests := tx.Bucket([]byte(controlRequestsBucketName))
		expiry := tx.Bucket([]byte(controlRequestExpiryBucket))
		if err := pruneExpiredControlRequestsTx(requests, expiry, now); err != nil {
			return err
		}
		key := controlRequestKey(runID, requestID)
		if raw := requests.Get(key); raw != nil {
			var request ControlRequest
			if err := json.Unmarshal(raw, &request); err != nil {
				return fmt.Errorf("decode control request: %w", err)
			}
			if request.RunID != runID || request.RequestID != requestID {
				return errors.New("control request key mismatch")
			}
			if request.MutationDigest != mutationDigest {
				return ErrControlRequestConflict
			}
			var run model.Run
			if runBytes := tx.Bucket([]byte(runsBucketName)).Get([]byte(runID)); runBytes != nil {
				if err := json.Unmarshal(runBytes, &run); err != nil {
					return fmt.Errorf("decode Run: %w", err)
				}
			}
			result = ControlBeginResult{Run: run, Request: request}
			return nil
		}
		if err := enforceControlCapacityTx(requests, expiry, runID); err != nil {
			return err
		}
		runBytes := tx.Bucket([]byte(runsBucketName)).Get([]byte(runID))
		if runBytes == nil {
			return ErrRunNotFound
		}
		var run model.Run
		if err := json.Unmarshal(runBytes, &run); err != nil {
			return fmt.Errorf("decode Run: %w", err)
		}
		if run.State == model.Terminal || run.State == model.Uncertain {
			return ErrControlRunFinal
		}
		if expectedGeneration == 0 || run.Generation != expectedGeneration {
			return ErrStaleControlGeneration
		}
		if run.Generation == math.MaxUint64 {
			return errors.New("Run generation exhausted")
		}
		run.Generation++
		request := ControlRequest{
			RunID: runID, RequestID: requestID, MutationDigest: mutationDigest,
			ExpectedGeneration: expectedGeneration, AcceptedGeneration: run.Generation,
			CreatedAt: now, ExpiresAt: now.Add(ControlRequestWindow), Status: ControlRequestPending,
		}
		encodedRequest, err := json.Marshal(request)
		if err != nil {
			return fmt.Errorf("encode control request: %w", err)
		}
		encodedRun, err := json.Marshal(run)
		if err != nil {
			return fmt.Errorf("encode Run: %w", err)
		}
		if err := tx.Bucket([]byte(runsBucketName)).Put([]byte(runID), encodedRun); err != nil {
			return fmt.Errorf("persist control generation: %w", err)
		}
		if err := requests.Put(key, encodedRequest); err != nil {
			return fmt.Errorf("persist control request: %w", err)
		}
		if err := expiry.Put(expiryIndexKey(request.ExpiresAt, key), nil); err != nil {
			return fmt.Errorf("index control request expiry: %w", err)
		}
		result = ControlBeginResult{Run: run, Request: request, Created: true}
		return nil
	})
	if err != nil {
		return ControlBeginResult{}, err
	}
	return result, nil
}

// CompleteControlRequest records a physical mutation result before the
// supervisor acknowledges success. Pending requests remain permanently
// uncertain within their retry window unless this commit succeeds.
func (s *Store) CompleteControlRequest(runID, requestID string, status ControlRequestStatus, failureCode, failureMessage string, now time.Time) error {
	if len(runID) == 0 || len(requestID) == 0 || len(requestID) > RequestIDMaxBytes {
		return ErrInvalidRequestID
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if status != ControlRequestSucceeded && status != ControlRequestUncertain {
		return errors.New("invalid control request disposition")
	}
	if len(failureCode) > maxControlFailureCode || len(failureMessage) > maxControlFailureMessage {
		return errors.New("control failure evidence exceeds its bound")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		requests := tx.Bucket([]byte(controlRequestsBucketName))
		if requests == nil {
			return ErrInvalidRequestID
		}
		key := controlRequestKey(runID, requestID)
		raw := requests.Get(key)
		if raw == nil {
			return ErrInvalidRequestID
		}
		var request ControlRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return fmt.Errorf("decode control request: %w", err)
		}
		if request.RunID != runID || request.RequestID != requestID {
			return errors.New("control request key mismatch")
		}
		if request.Status != ControlRequestPending {
			if request.Status == status && request.FailureCode == failureCode && request.FailureMessage == failureMessage {
				return nil
			}
			return ErrControlRequestFinalized
		}
		request.Status = status
		request.FailureCode = failureCode
		request.FailureMessage = failureMessage
		encoded, err := json.Marshal(request)
		if err != nil {
			return fmt.Errorf("encode control request: %w", err)
		}
		if err := requests.Put(key, encoded); err != nil {
			return fmt.Errorf("persist control disposition: %w", err)
		}
		return nil
	})
}

func validateControlIdentity(runID, requestID, digest string, expectedGeneration uint64) error {
	if runID == "" || len(requestID) == 0 || len(requestID) > RequestIDMaxBytes || expectedGeneration == 0 {
		return ErrInvalidRequestID
	}
	return validateDigest(digest)
}

func controlRunPrefix(runID string) []byte {
	prefix := make([]byte, 4+len(runID))
	binary.BigEndian.PutUint32(prefix[:4], uint32(len(runID)))
	copy(prefix[4:], runID)
	return prefix
}

func controlRequestKey(runID, requestID string) []byte {
	key := controlRunPrefix(runID)
	return append(key, requestID...)
}

// FindControlRequest checks a previously accepted mutation identity without
// requiring the Run to remain live. It lets response-lost retries recover a
// durable result after lifecycle state has advanced or become terminal.
func (s *Store) FindControlRequest(runID, requestID, mutationDigest string, now time.Time) (ControlRequest, bool, error) {
	if err := validateControlIdentity(runID, requestID, mutationDigest, 1); err != nil {
		return ControlRequest{}, false, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	var request ControlRequest
	found := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		requests := tx.Bucket([]byte(controlRequestsBucketName))
		if requests == nil {
			return nil
		}
		key := controlRequestKey(runID, requestID)
		raw := requests.Get(key)
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &request); err != nil {
			return fmt.Errorf("decode control request: %w", err)
		}
		if request.RunID != runID || request.RequestID != requestID {
			return errors.New("control request key mismatch")
		}
		if !request.ExpiresAt.After(now) {
			if err := requests.Delete(key); err != nil {
				return err
			}
			if expiry := tx.Bucket([]byte(controlRequestExpiryBucket)); expiry != nil {
				if err := expiry.Delete(expiryIndexKey(request.ExpiresAt, key)); err != nil {
					return err
				}
			}
			return nil
		}
		if request.MutationDigest != mutationDigest {
			return ErrControlRequestConflict
		}
		found = true
		return nil
	})
	return request, found, err
}

func pruneExpiredControlRequestsTx(requests, expiry *bolt.Bucket, now time.Time) error {
	if requests == nil || expiry == nil {
		return nil
	}
	cutoff := make([]byte, 8)
	binary.BigEndian.PutUint64(cutoff, uint64(now.UnixNano()))
	for {
		cursor := expiry.Cursor()
		key, _ := cursor.First()
		if key == nil || len(key) < 8 || bytes.Compare(key[:8], cutoff) > 0 {
			return nil
		}
		requestKey := append([]byte(nil), key[8:]...)
		if err := expiry.Delete(key); err != nil {
			return err
		}
		if err := requests.Delete(requestKey); err != nil {
			return err
		}
	}
}

func enforceControlCapacityTx(requests, expiry *bolt.Bucket, runID string) error {
	if requests.Stats().KeyN >= MaxControlRequestsRuntime {
		return ErrControlRequestCapacity
	}
	prefix := controlRunPrefix(runID)
	count := 0
	cursor := requests.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		count++
		if count >= MaxControlRequestsPerRun {
			return ErrControlRequestCapacity
		}
	}
	_ = expiry // expiry is retained as an argument for symmetry with global pruning.
	return nil
}
