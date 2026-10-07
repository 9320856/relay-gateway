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
	"gorm.io/gorm"
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
	semaphore chan struct{}
	refs      int
}

var profileSubmitLocks = profileIdempotencyLocks{items: make(map[string]*profileIdempotencyLock)}

func (locks *profileIdempotencyLocks) acquire(ctx context.Context, key string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return func() {}, nil
	}
	locks.mu.Lock()
	if locks.items == nil {
		locks.items = make(map[string]*profileIdempotencyLock)
	}
	item := locks.items[key]
	if item == nil {
		item = &profileIdempotencyLock{semaphore: make(chan struct{}, 1)}
		locks.items[key] = item
	}
	item.refs++
	locks.mu.Unlock()
	dropReference := func() {
		locks.mu.Lock()
		item.refs--
		if item.refs == 0 {
			delete(locks.items, key)
		}
		locks.mu.Unlock()
	}
	select {
	case item.semaphore <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-item.semaphore
			dropReference()
			return nil, err
		}
	case <-ctx.Done():
		dropReference()
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() { once.Do(func() { <-item.semaphore; dropReference() }) }, nil
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
		MediaRetention:     op.EffectiveMediaRetention(),
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

func profileVideoResponseFromTaskRun(c *gin.Context, run *db.TaskRun) (*model.VideoTaskResponse, error) {
	if !profileTaskRunCanBeReused(run) {
		return nil, errors.New("idempotent profile submission is not durably accepted")
	}
	lookupID, err := profileReplayTaskID(c.Request.Context(), run)
	if err != nil {
		return nil, err
	}
	if handled, latest := profileRequiredMediaStatus(c, run, asyncTaskKindVideo); handled {
		run = latest
	}
	return profileDurableStatus(c, run, asyncTaskKindVideo, lookupID).(*model.VideoTaskResponse), nil
}

func profileImageResponseFromTaskRun(c *gin.Context, run *db.TaskRun) (map[string]any, error) {
	if !profileTaskRunCanBeReused(run) {
		return nil, errors.New("idempotent profile submission is not durably accepted")
	}
	lookupID, err := profileReplayTaskID(c.Request.Context(), run)
	if err != nil {
		return nil, err
	}
	if handled, latest := profileRequiredMediaStatus(c, run, asyncTaskKindImage); handled {
		run = latest
	}
	return profileDurableStatus(c, run, asyncTaskKindImage, lookupID).(map[string]any), nil
}

// Poll results may contain the provider's colliding ID. The create alias is
// the public identity returned by the original submit and must survive replay.
func profileReplayTaskID(ctx context.Context, run *db.TaskRun) (string, error) {
	var alias db.TaskAlias
	err := db.DBForContext(ctx).WithContext(ctx).Where("task_run_id = ? AND source = ?", run.ID, "create").Order("id ASC").First(&alias).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return run.ProviderTaskID, nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(alias.LookupID, imageTaskIDPrefix), nil
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
