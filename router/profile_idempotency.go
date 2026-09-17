package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

// profileIdempotencyLocks closes the small in-process race between the
// durable lookup and the first provider Submit. The database row remains the
// source of truth across restarts and processes; this lock only prevents two
// goroutines in this process from both reaching the provider before either
// projection is visible.
type profileIdempotencyLocks struct {
	mu    sync.Mutex
	items map[string]*profileIdempotencyLock
}

type profileIdempotencyLock struct {
	mu   sync.Mutex
	refs int
}

var profileSubmitLocks = profileIdempotencyLocks{items: make(map[string]*profileIdempotencyLock)}

func (locks *profileIdempotencyLocks) acquire(key string) func() {
	key = strings.TrimSpace(key)
	if key == "" {
		return func() {}
	}
	locks.mu.Lock()
	item := locks.items[key]
	if item == nil {
		item = &profileIdempotencyLock{}
		locks.items[key] = item
	}
	item.refs++
	locks.mu.Unlock()
	item.mu.Lock()
	return func() {
		item.mu.Unlock()
		locks.mu.Lock()
		item.refs--
		if item.refs == 0 {
			delete(locks.items, key)
		}
		locks.mu.Unlock()
	}
}

func profileCallerIdempotencyKey(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if key == "" || len(key) > 255 || strings.ContainsAny(key, "\r\n") {
		return ""
	}
	return key
}

func profileRequestFingerprint(operation, modelName string, body any) (string, error) {
	encoded, err := json.Marshal(struct {
		Operation string `json:"operation"`
		Model     string `json:"model"`
		Body      any    `json:"body"`
	}{Operation: strings.TrimSpace(operation), Model: strings.TrimSpace(modelName), Body: body})
	if err != nil {
		return "", fmt.Errorf("encode idempotency fingerprint: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func profileReservationTaskRunID(operation, idempotencyKey, fingerprint string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(operation) + "\x00" + strings.TrimSpace(idempotencyKey) + "\x00" + strings.TrimSpace(fingerprint)))
	return "prr_" + hex.EncodeToString(sum[:])[:60]
}

func reserveProfileTaskRun(ctx context.Context, candidate *config.UpstreamChannel, binding *db.ChannelProtocolBinding, revision *db.ProtocolProfileRevision, compiled protocol.CompiledProfile, op protocol.Operation, taskKind, idempotencyKey, fingerprint, originRequestID string) (*db.TaskRun, bool, error) {
	if candidate == nil || binding == nil || revision == nil || strings.TrimSpace(idempotencyKey) == "" || strings.TrimSpace(fingerprint) == "" {
		return nil, false, errors.New("profile task reservation is incomplete")
	}
	now := time.Now()
	run := &db.TaskRun{
		ID:                 profileReservationTaskRunID(binding.Operation, idempotencyKey, fingerprint),
		OriginRequestID:    strings.TrimSpace(originRequestID),
		TaskKind:           taskKind,
		Operation:          binding.Operation,
		ChannelID:          candidate.ID,
		Engine:             "profile",
		ProfileID:          binding.ProfileID,
		ProfileRevision:    revision.Revision,
		ProfileDigest:      compiled.Digest(),
		IdempotencyKey:     idempotencyKey,
		RequestFingerprint: fingerprint,
		PollingMode:        op.PollingMode,
		SubmissionState:    "submitting",
		TaskStatus:         "queued",
		TaskOutcome:        "pending",
		AcceptedAt:         &now,
	}
	if op.Poll != nil && op.PollingMode != protocol.PollingOff {
		deadline := now.Add(time.Duration(op.Poll.MaxDurationMS) * time.Millisecond)
		run.DeadlineAt = &deadline
		if op.PollingMode == protocol.PollingBackground {
			next := now.Add(time.Duration(op.Poll.IntervalMS) * time.Millisecond)
			run.NextPollAt = &next
		}
	}
	if err := db.CreateTaskRunContext(ctx, run); err == nil {
		return run, true, nil
	}
	existing, loadErr := db.GetTaskRunContext(ctx, run.ID)
	if loadErr != nil {
		return nil, false, loadErr
	}
	if existing.IdempotencyKey != idempotencyKey || existing.RequestFingerprint != fingerprint || existing.Operation != binding.Operation {
		return nil, false, db.ErrTaskIdempotencyConflict
	}
	return existing, false, nil
}

func profileTaskRunCanBeReused(run *db.TaskRun) bool {
	return run != nil && strings.EqualFold(strings.TrimSpace(run.SubmissionState), "accepted") && strings.TrimSpace(run.ProviderTaskID) != ""
}

func profileVideoResponseFromTaskRun(run *db.TaskRun) (*model.VideoTaskResponse, error) {
	if !profileTaskRunCanBeReused(run) {
		return nil, errors.New("idempotent profile submission is not durably accepted")
	}
	var response model.VideoTaskResponse
	if strings.TrimSpace(run.ResultBody) != "" && json.Unmarshal([]byte(run.ResultBody), &response) == nil {
		if response.ID == "" {
			response.ID = run.ProviderTaskID
		}
		if response.TaskID == "" {
			response.TaskID = run.ProviderTaskID
		}
		if response.Status == "" {
			response.Status = normalizeProfileStatus(run.TaskStatus)
		}
		return &response, nil
	}
	return &model.VideoTaskResponse{ID: run.ProviderTaskID, TaskID: run.ProviderTaskID, Status: normalizeProfileStatus(run.TaskStatus)}, nil
}

func profileImageResponseFromTaskRun(run *db.TaskRun) (map[string]any, error) {
	if !profileTaskRunCanBeReused(run) {
		return nil, errors.New("idempotent profile submission is not durably accepted")
	}
	var response map[string]any
	if strings.TrimSpace(run.ResultBody) != "" && json.Unmarshal([]byte(run.ResultBody), &response) == nil {
		if _, ok := response["id"]; !ok {
			response["id"] = run.ProviderTaskID
		}
		if _, ok := response["task_id"]; !ok {
			response["task_id"] = run.ProviderTaskID
		}
		if _, ok := response["status"]; !ok {
			response["status"] = normalizeProfileStatus(run.TaskStatus)
		}
		return response, nil
	}
	return map[string]any{"id": run.ProviderTaskID, "task_id": run.ProviderTaskID, "status": normalizeProfileStatus(run.TaskStatus)}, nil
}

func profileSubmissionErrorState(err error) string {
	var executorErr *protocol.ExecutorError
	if errors.As(err, &executorErr) && executorErr.MayHaveSubmitted {
		return "submission_unknown"
	}
	return "rejected"
}

func profileRequestID(c *gin.Context) string {
	if c == nil {
		return ""
	}
	return audit.RequestID(c.Request.Context())
}
