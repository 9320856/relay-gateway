package db

import (
	"context"
	"errors"
	"fmt"
	"relay-gateway/media"
	"strings"
	"time"

	"gorm.io/gorm"
)

// UpdateTaskRunResultContext stores the provider result independently from
// task state. A successful upstream poll must remain observable even when
// subsequent media materialization is delayed or fails.
func UpdateTaskRunResultContext(ctx context.Context, id, body string, truncated bool) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("task run id is required")
	}
	result := db.Model(&TaskRun{}).Where("id = ?", id).Updates(map[string]any{
		"result_body": body, "result_truncated": truncated, "updated_at": time.Now(),
	})
	return result.Error
}

func UpdateTaskRunResult(id, body string, truncated bool) error {
	return UpdateTaskRunResultContext(context.Background(), id, body, truncated)
}

// UpdateTaskRunProviderTaskIDContext records the provider-facing task ID for an
// existing TaskRun when later polling reveals it, without re-pointing its public
// alias or touching other channels.
func UpdateTaskRunProviderTaskIDContext(ctx context.Context, id, providerTaskID string) error {
	dbConn, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id, providerTaskID = strings.TrimSpace(id), strings.TrimSpace(providerTaskID)
	if id == "" {
		return errors.New("task run id is required")
	}
	result := dbConn.Model(&TaskRun{}).Where("id = ?", id).Updates(map[string]any{
		"provider_task_id": providerTaskID,
		"updated_at":       time.Now(),
	})
	return result.Error
}

func UpdateTaskRunProviderTaskID(id, providerTaskID string) error {
	return UpdateTaskRunProviderTaskIDContext(context.Background(), id, providerTaskID)
}

// EnsureTaskResultMediaContext creates the logical asset and durable
// materialization job for each provider result URL. The task/ordinal unique
// key makes this safe across repeated polls and process restarts.
func EnsureTaskResultMediaContext(ctx context.Context, taskRunID, kind string, urls []string) ([]MediaAsset, error) {
	dbConn, err := taskDB(ctx)
	if err != nil {
		return nil, err
	}
	taskRunID, kind = strings.TrimSpace(taskRunID), strings.TrimSpace(kind)
	if taskRunID == "" {
		return nil, errors.New("task run id is required")
	}
	var assets []MediaAsset
	err = dbConn.Transaction(func(tx *gorm.DB) error {
		for ordinal, raw := range urls {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			var asset MediaAsset
			findErr := tx.Where("task_run_id = ? AND ordinal = ?", taskRunID, ordinal).First(&asset).Error
			if errors.Is(findErr, gorm.ErrRecordNotFound) {
				publicID, capability, capabilityHash, tokenErr := NewMediaLinkIdentity()
				if tokenErr != nil {
					return tokenErr
				}
				capabilityCiphertext, encryptErr := EncryptMediaCapability(capability)
				if encryptErr != nil {
					return encryptErr
				}
				source := media.NormalizeMediaSource(raw)
				asset = MediaAsset{PublicID: publicID, CapabilityHash: capabilityHash, CapabilityCiphertext: capabilityCiphertext, TaskRunID: taskRunID, Kind: kind, Ordinal: ordinal, Status: MediaAssetPending, SourceKind: source.SourceKind, SourceLocator: source.Locator, ContentType: source.ContentType}
				if err := tx.Create(&asset).Error; err != nil {
					return err
				}
			} else if findErr != nil {
				return findErr
			}
			job := MediaMaterializationJob{AssetID: asset.ID, Status: MediaJobPending}
			if err := tx.Where("asset_id = ?", asset.ID).FirstOrCreate(&job).Error; err != nil {
				return err
			}
			assets = append(assets, asset)
		}
		return nil
	})
	return assets, err
}

// EnsureTaskContentMediaContext creates a durable media asset whose source is
// the Profile operation's provider content endpoint. Some asynchronous video
// APIs expose the completed task URL only through a separate authenticated
// `/content` request; in that shape there is no result URL for the normal URL
// materializer to enqueue. The TaskRun retains the channel, operation, and
// immutable Profile revision needed by the media worker to fetch that content
// later without submitting or polling the provider task again.
func EnsureTaskContentMediaContext(ctx context.Context, taskRunID, kind string) ([]MediaAsset, error) {
	dbConn, err := taskDB(ctx)
	if err != nil {
		return nil, err
	}
	taskRunID, kind = strings.TrimSpace(taskRunID), strings.TrimSpace(kind)
	if taskRunID == "" {
		return nil, errors.New("task run id is required")
	}
	if kind == "" {
		return nil, errors.New("task kind is required")
	}
	var assets []MediaAsset
	err = dbConn.Transaction(func(tx *gorm.DB) error {
		var run TaskRun
		if err := tx.Where("id = ?", taskRunID).First(&run).Error; err != nil {
			return err
		}
		providerTaskID := strings.TrimSpace(run.ProviderTaskID)
		if providerTaskID == "" {
			return errors.New("provider task id is required for content media")
		}
		var asset MediaAsset
		findErr := tx.Where("task_run_id = ? AND kind = ? AND ordinal = ?", taskRunID, kind, 0).First(&asset).Error
		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			publicID, capability, capabilityHash, tokenErr := NewMediaLinkIdentity()
			if tokenErr != nil {
				return tokenErr
			}
			capabilityCiphertext, encryptErr := EncryptMediaCapability(capability)
			if encryptErr != nil {
				return encryptErr
			}
			asset = MediaAsset{
				PublicID: publicID, CapabilityHash: capabilityHash, CapabilityCiphertext: capabilityCiphertext,
				TaskRunID: taskRunID, Kind: kind, Ordinal: 0, Status: MediaAssetPending,
				SourceKind: "provider_content", SourceLocator: providerTaskID,
			}
			if err := tx.Create(&asset).Error; err != nil {
				return err
			}
		} else if findErr != nil {
			return findErr
		}
		job := MediaMaterializationJob{AssetID: asset.ID, Status: MediaJobPending}
		if err := tx.Where("asset_id = ?", asset.ID).FirstOrCreate(&job).Error; err != nil {
			return err
		}
		assets = append(assets, asset)
		return nil
	})
	return assets, err
}

var (
	ErrTaskStateRegression     = errors.New("task state cannot move backwards")
	ErrTaskAlreadyTerminal     = errors.New("task is already terminal")
	ErrTaskLeaseUnavailable    = errors.New("task lease unavailable")
	ErrTaskLeaseOwner          = errors.New("task lease owner mismatch")
	ErrTaskIdempotencyConflict = errors.New("task idempotency key conflicts with another request")
	// ErrTaskAliasConflict prevents a lookup ID from resolving to more than
	// one durable task. Without this guard, an image/video ID collision could
	// make status routing depend on database row order.
	ErrTaskAliasConflict = errors.New("task alias is pinned to a different task run")
)

// TaskRun is the durable lifecycle aggregate for a generated task. Legacy
// task mappings remain available for compatibility; new code can progressively
// populate this record without changing existing public routes.
type TaskRun struct {
	ID              string `gorm:"primaryKey;size:64" json:"id"`
	OriginRequestID string `gorm:"size:64;index" json:"origin_request_id,omitempty"`
	TaskKind        string `gorm:"size:32;index" json:"task_kind"`
	Operation       string `gorm:"size:64;index" json:"operation"`
	ChannelID       string `gorm:"size:64;index" json:"channel_id"`
	Engine          string `gorm:"size:16;not null;default:'legacy'" json:"engine"`
	ProfileID       string `gorm:"size:64;index" json:"profile_id,omitempty"`
	ProfileRevision int    `gorm:"default:0" json:"profile_revision,omitempty"`
	ProfileDigest   string `gorm:"size:128" json:"profile_digest,omitempty"`
	// IdempotencyKey and RequestFingerprint are populated for Profile-backed
	// creates when the caller supplies an Idempotency-Key. They let a retry
	// reuse the durable TaskRun after a process restart without relying on a
	// provider-specific idempotency implementation.
	IdempotencyKey        string     `gorm:"size:255;index:idx_task_run_idempotency,priority:1" json:"-"`
	RequestFingerprint    string     `gorm:"size:64;index:idx_task_run_idempotency,priority:2" json:"-"`
	PollingMode           string     `gorm:"size:16;not null;default:'off'" json:"polling_mode"`
	ProviderTaskID        string     `gorm:"size:128;index" json:"provider_task_id,omitempty"`
	SubmissionState       string     `gorm:"size:32;not null;default:'accepted'" json:"submission_state"`
	TaskStatus            string     `gorm:"size:32;not null;default:'queued';index" json:"task_status"`
	TaskOutcome           string     `gorm:"size:32;not null;default:'pending';index" json:"task_outcome"`
	PollCount             int        `gorm:"not null;default:0" json:"poll_count"`
	PollSuccessCount      int        `gorm:"not null;default:0" json:"poll_success_count"`
	PollFailureCount      int        `gorm:"not null;default:0" json:"poll_failure_count"`
	ConsecutivePollErrors int        `gorm:"not null;default:0" json:"consecutive_poll_errors"`
	LastPollAt            *time.Time `json:"last_poll_at,omitempty"`
	NextPollAt            *time.Time `gorm:"index" json:"next_poll_at,omitempty"`
	DeadlineAt            *time.Time `json:"deadline_at,omitempty"`
	LeaseOwner            string     `gorm:"size:128;index" json:"lease_owner,omitempty"`
	LeaseExpiresAt        *time.Time `json:"lease_expires_at,omitempty"`
	StateVersion          uint64     `gorm:"not null;default:0" json:"state_version"`
	EventSequence         uint64     `gorm:"not null;default:0" json:"event_sequence"`
	LastHTTPStatus        int        `gorm:"not null;default:0" json:"last_http_status,omitempty"`
	ResultBody            string     `gorm:"type:text" json:"result_body,omitempty"`
	ResultTruncated       bool       `gorm:"not null;default:false" json:"result_truncated,omitempty"`
	TaskError             string     `gorm:"type:text" json:"task_error,omitempty"`
	AcceptedAt            *time.Time `json:"accepted_at,omitempty"`
	CompletedAt           *time.Time `json:"completed_at,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

func (TaskRun) TableName() string { return "async_task_runs" }

type TaskAlias struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	TaskRunID string    `gorm:"size:64;not null;uniqueIndex:idx_task_alias_lookup,priority:1;index" json:"task_run_id"`
	LookupID  string    `gorm:"size:160;not null;uniqueIndex:idx_task_alias_lookup,priority:2" json:"lookup_id"`
	Source    string    `gorm:"size:16;not null;default:'create'" json:"source"`
	CreatedAt time.Time `json:"created_at"`
}

func (TaskAlias) TableName() string { return "async_task_aliases" }

type TaskAttempt struct {
	ID           uint       `gorm:"primaryKey" json:"id"`
	TaskRunID    string     `gorm:"size:64;not null;index:idx_task_attempt_started,priority:1" json:"task_run_id"`
	AttemptType  string     `gorm:"size:16;not null" json:"attempt_type"`
	StartedAt    time.Time  `gorm:"not null;index:idx_task_attempt_started,priority:2" json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	HTTPStatus   int        `gorm:"not null;default:0" json:"http_status,omitempty"`
	Outcome      string     `gorm:"size:32;not null" json:"outcome"`
	Error        string     `gorm:"type:text" json:"error,omitempty"`
	ResponseMeta string     `gorm:"type:text" json:"response_meta,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

// TaskPollRecord is the provider-independent durable form of one Poll
// observation. Protocol packages define their own attempt type; this small
// DB record keeps the persistence layer independent from the executor.
type TaskPollRecord struct {
	StartedAt  time.Time
	FinishedAt time.Time
	HTTPStatus int
	Outcome    string
	Error      string
}

func (TaskAttempt) TableName() string { return "async_task_attempts" }

type TaskEvent struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	TaskRunID string    `gorm:"size:64;not null;index:idx_task_event_sequence,priority:1" json:"task_run_id"`
	Sequence  uint64    `gorm:"not null;uniqueIndex:idx_task_event_sequence,priority:2" json:"sequence"`
	Type      string    `gorm:"size:64;not null;index" json:"type"`
	Data      string    `gorm:"type:text" json:"data,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func (TaskEvent) TableName() string { return "async_task_events" }

func taskDB(ctx context.Context) (*gorm.DB, error) {
	db := DBForContext(ctx)
	if db == nil {
		return nil, errors.New("database is not initialized")
	}
	return db, nil
}

func CreateTaskRun(run *TaskRun) error { return CreateTaskRunContext(context.Background(), run) }

func CreateTaskRunContext(ctx context.Context, run *TaskRun) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	if run == nil || strings.TrimSpace(run.ID) == "" {
		return errors.New("task run id is required")
	}
	run.ID = strings.TrimSpace(run.ID)
	if run.Engine == "" {
		run.Engine = "legacy"
	}
	if run.TaskStatus == "" {
		run.TaskStatus = "queued"
	}
	if run.TaskOutcome == "" {
		run.TaskOutcome = "pending"
	}
	if run.AcceptedAt == nil {
		now := time.Now()
		run.AcceptedAt = &now
	}
	if run.CompletedAt == nil && isTaskTerminal(run.TaskStatus) {
		now := time.Now()
		run.CompletedAt = &now
	}
	return db.Create(run).Error
}

// FindProfileTaskRunByIdempotencyContext returns the durable Profile task for
// a caller-provided idempotency key and request fingerprint. A key reused for
// a different request, or a database that already contains duplicate rows for
// the same key, is a hard conflict so callers cannot guess which provider task
// should be returned.
func FindProfileTaskRunByIdempotencyContext(ctx context.Context, key, operation, fingerprint string) (*TaskRun, error) {
	run, err := FindProfileTaskRunByIdempotencyKeyContext(ctx, key, operation)
	if err != nil {
		return nil, err
	}
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" || strings.TrimSpace(run.RequestFingerprint) != fingerprint {
		return nil, ErrTaskIdempotencyConflict
	}
	return run, nil
}

// FindProfileTaskRunByIdempotencyKeyContext looks up a Profile task before
// channel candidate resolution. This keeps a retry bound to the original
// accepted TaskRun even if its channel is temporarily disabled or its
// priority changes after the first request.
func FindProfileTaskRunByIdempotencyKeyContext(ctx context.Context, key, operation string) (*TaskRun, error) {
	db, err := taskDB(ctx)
	if err != nil {
		return nil, err
	}
	key, operation = strings.TrimSpace(key), strings.TrimSpace(operation)
	if key == "" || operation == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var runs []TaskRun
	if err := db.Where("engine = ? AND idempotency_key = ? AND operation = ?", "profile", key, operation).
		Order("created_at ASC, id ASC").Find(&runs).Error; err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(runs) > 1 {
		return nil, ErrTaskIdempotencyConflict
	}
	return &runs[0], nil
}

// UpdateProfileTaskRunProjectionContext fills a reserved Profile TaskRun
// after the provider accepts a task. The explicit column map avoids Save's
// association semantics and keeps this update compatible with old rows.
func UpdateProfileTaskRunProjectionContext(ctx context.Context, run *TaskRun) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	if run == nil || strings.TrimSpace(run.ID) == "" {
		return errors.New("task run id is required")
	}
	updates := map[string]any{
		"origin_request_id":       run.OriginRequestID,
		"task_kind":               run.TaskKind,
		"operation":               run.Operation,
		"channel_id":              run.ChannelID,
		"engine":                  run.Engine,
		"profile_id":              run.ProfileID,
		"profile_revision":        run.ProfileRevision,
		"profile_digest":          run.ProfileDigest,
		"idempotency_key":         run.IdempotencyKey,
		"request_fingerprint":     run.RequestFingerprint,
		"polling_mode":            run.PollingMode,
		"provider_task_id":        run.ProviderTaskID,
		"submission_state":        run.SubmissionState,
		"task_status":             run.TaskStatus,
		"task_outcome":            run.TaskOutcome,
		"poll_count":              run.PollCount,
		"poll_success_count":      run.PollSuccessCount,
		"poll_failure_count":      run.PollFailureCount,
		"last_poll_at":            run.LastPollAt,
		"last_http_status":        run.LastHTTPStatus,
		"consecutive_poll_errors": run.ConsecutivePollErrors,
		"next_poll_at":            run.NextPollAt,
		"deadline_at":             run.DeadlineAt,
		"result_body":             run.ResultBody,
		"result_truncated":        run.ResultTruncated,
		"task_error":              run.TaskError,
		"accepted_at":             run.AcceptedAt,
		"completed_at":            run.CompletedAt,
		"updated_at":              time.Now(),
	}
	result := db.Model(&TaskRun{}).Where("id = ?", strings.TrimSpace(run.ID)).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// UpdateTaskRunSubmissionStateContext records whether a reserved Profile
// submission was accepted, rejected before dispatch, or became ambiguous
// after the provider request may have reached the network.
func UpdateTaskRunSubmissionStateContext(ctx context.Context, id, state, taskError string) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id, state = strings.TrimSpace(id), strings.TrimSpace(state)
	if id == "" || state == "" {
		return errors.New("task submission state identity is required")
	}
	result := db.Model(&TaskRun{}).Where("id = ?", id).Updates(map[string]any{
		"submission_state": state,
		"task_error":       strings.TrimSpace(taskError),
		"updated_at":       time.Now(),
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func GetTaskRun(id string) (*TaskRun, error) { return GetTaskRunContext(context.Background(), id) }

func GetTaskRunContext(ctx context.Context, id string) (*TaskRun, error) {
	db, err := taskDB(ctx)
	if err != nil {
		return nil, err
	}
	var run TaskRun
	if err := db.First(&run, "id = ?", strings.TrimSpace(id)).Error; err != nil {
		return nil, err
	}
	return &run, nil
}

// GetTaskRunByAlias resolves a public/provider lookup ID to its durable task
// aggregate. It is used by Profile status handlers so client polling shares
// the same TaskRun instead of creating a second lifecycle record.
func GetTaskRunByAliasContext(ctx context.Context, lookupID string) (*TaskRun, error) {
	db, err := taskDB(ctx)
	if err != nil {
		return nil, err
	}
	lookupID = strings.TrimSpace(lookupID)
	if lookupID == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var alias TaskAlias
	if err := db.Where("lookup_id = ?", lookupID).First(&alias).Error; err != nil {
		return nil, err
	}
	return GetTaskRunContext(ctx, alias.TaskRunID)
}

func GetTaskRunByAlias(lookupID string) (*TaskRun, error) {
	return GetTaskRunByAliasContext(context.Background(), lookupID)
}

func UpdateTaskRunStatus(id, status, outcome string) error {
	return UpdateTaskRunStatusContext(context.Background(), id, status, outcome)
}

func UpdateTaskRunStatusContext(ctx context.Context, id, status, outcome string) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id, status, outcome = strings.TrimSpace(id), strings.TrimSpace(status), strings.TrimSpace(outcome)
	if id == "" || status == "" {
		return errors.New("task status identity is required")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var run TaskRun
		if err := tx.First(&run, "id = ?", id).Error; err != nil {
			return err
		}
		if taskStatusRank(status) < taskStatusRank(run.TaskStatus) {
			return fmt.Errorf("%w: %s -> %s", ErrTaskStateRegression, run.TaskStatus, status)
		}
		if isTaskTerminal(run.TaskStatus) && status != run.TaskStatus {
			return fmt.Errorf("%w: %s -> %s", ErrTaskAlreadyTerminal, run.TaskStatus, status)
		}
		now := time.Now()
		updates := map[string]any{"task_status": status, "state_version": run.StateVersion + 1, "updated_at": now}
		if outcome != "" {
			updates["task_outcome"] = outcome
		}
		if isTaskTerminal(status) {
			updates["completed_at"] = now
		}
		return tx.Model(&run).Updates(updates).Error
	})
}

// UpdateTaskRunStatusForLease applies a status only while owner still holds
// a live lease, avoiding stale updates after worker takeover.
func UpdateTaskRunStatusForLease(id, owner, status, outcome string) error {
	return UpdateTaskRunStatusForLeaseContext(context.Background(), id, owner, status, outcome)
}

func UpdateTaskRunStatusForLeaseContext(ctx context.Context, id, owner, status, outcome string) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id, owner, status, outcome = strings.TrimSpace(id), strings.TrimSpace(owner), strings.TrimSpace(status), strings.TrimSpace(outcome)
	if id == "" || owner == "" || status == "" {
		return errors.New("task lease identity is required")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var run TaskRun
		if err := tx.Where("id = ? AND lease_owner = ? AND lease_expires_at > ?", id, owner, time.Now()).First(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrTaskLeaseOwner
			}
			return err
		}
		if taskStatusRank(status) < taskStatusRank(run.TaskStatus) {
			return fmt.Errorf("%w: %s -> %s", ErrTaskStateRegression, run.TaskStatus, status)
		}
		if isTaskTerminal(run.TaskStatus) && status != run.TaskStatus {
			return fmt.Errorf("%w: %s -> %s", ErrTaskAlreadyTerminal, run.TaskStatus, status)
		}
		now := time.Now()
		updates := map[string]any{"task_status": status, "state_version": run.StateVersion + 1, "updated_at": now}
		if outcome != "" {
			updates["task_outcome"] = outcome
		}
		if isTaskTerminal(status) {
			updates["completed_at"] = now
		}
		return tx.Model(&run).Updates(updates).Error
	})
}

// RecordTaskPoll atomically accounts for one status observation. It is safe to
// call after a provider response has been received; the counters are kept on
// the TaskRun so client, gateway-wait, and background pollers share one view.
func RecordTaskPoll(id string, success bool, httpStatus int) error {
	return RecordTaskPollContext(context.Background(), id, success, httpStatus)
}

func RecordTaskPollForLease(id, owner string, success bool, httpStatus int) error {
	return RecordTaskPollForLeaseContext(context.Background(), id, owner, success, httpStatus)
}

// RecordTaskPollForLease refuses observations from an expired or replaced
// lease, preventing slow workers from mutating a task after takeover.
func RecordTaskPollForLeaseContext(ctx context.Context, id, owner string, success bool, httpStatus int) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id, owner = strings.TrimSpace(id), strings.TrimSpace(owner)
	if id == "" || owner == "" {
		return errors.New("task lease identity is required")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var run TaskRun
		if err := tx.Where("id = ? AND lease_owner = ? AND lease_expires_at > ?", id, owner, time.Now()).First(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrTaskLeaseOwner
			}
			return err
		}
		now := time.Now()
		updates := map[string]any{"poll_count": run.PollCount + 1, "last_poll_at": now, "state_version": run.StateVersion + 1, "updated_at": now}
		if httpStatus > 0 {
			updates["last_http_status"] = httpStatus
		}
		if success {
			updates["poll_success_count"] = run.PollSuccessCount + 1
			updates["consecutive_poll_errors"] = 0
		} else {
			updates["poll_failure_count"] = run.PollFailureCount + 1
			updates["consecutive_poll_errors"] = run.ConsecutivePollErrors + 1
		}
		return tx.Model(&run).Updates(updates).Error
	})
}

func RecordTaskPollContext(ctx context.Context, id string, success bool, httpStatus int) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("task run id is required")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var run TaskRun
		if err := tx.First(&run, "id = ?", id).Error; err != nil {
			return err
		}
		updates := map[string]any{
			"poll_count":    run.PollCount + 1,
			"last_poll_at":  time.Now(),
			"state_version": run.StateVersion + 1,
			"updated_at":    time.Now(),
		}
		if httpStatus > 0 {
			updates["last_http_status"] = httpStatus
		}
		if success {
			updates["poll_success_count"] = run.PollSuccessCount + 1
			updates["consecutive_poll_errors"] = 0
		} else {
			updates["poll_failure_count"] = run.PollFailureCount + 1
			updates["consecutive_poll_errors"] = run.ConsecutivePollErrors + 1
		}
		return tx.Model(&run).Updates(updates).Error
	})
}

// PersistTaskPollBatchContext records gateway_wait observations after Submit
// has been accepted. It updates the same TaskRun counters and appends every
// Poll Attempt atomically, so a polling error cannot leave only an in-memory
// count or a detached request log.
func PersistTaskPollBatchContext(ctx context.Context, id string, records []TaskPollRecord) error {
	dbConn, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("task run id is required")
	}
	if len(records) == 0 {
		return nil
	}
	return dbConn.Transaction(func(tx *gorm.DB) error {
		var run TaskRun
		if err := tx.First(&run, "id = ?", id).Error; err != nil {
			return err
		}
		successCount, failureCount, consecutiveErrors := run.PollSuccessCount, run.PollFailureCount, run.ConsecutivePollErrors
		var lastPollAt *time.Time
		lastHTTPStatus := run.LastHTTPStatus
		for _, record := range records {
			started := record.StartedAt
			if started.IsZero() {
				started = time.Now()
			}
			finished := record.FinishedAt
			if finished.IsZero() {
				finished = started
			}
			outcome := strings.TrimSpace(record.Outcome)
			if outcome == "" {
				outcome = "failed"
			}
			if outcome == "success" {
				successCount++
				consecutiveErrors = 0
			} else {
				failureCount++
				consecutiveErrors++
			}
			if record.HTTPStatus > 0 {
				lastHTTPStatus = record.HTTPStatus
			}
			finishedCopy := finished
			lastPollAt = &finishedCopy
			if err := tx.Create(&TaskAttempt{TaskRunID: id, AttemptType: "poll", StartedAt: started, FinishedAt: &finishedCopy, HTTPStatus: record.HTTPStatus, Outcome: outcome, Error: strings.TrimSpace(record.Error)}).Error; err != nil {
				return err
			}
		}
		return tx.Model(&run).Updates(map[string]any{
			"poll_count": run.PollCount + len(records), "poll_success_count": successCount,
			"poll_failure_count": failureCount, "consecutive_poll_errors": consecutiveErrors,
			"last_poll_at": lastPollAt, "last_http_status": lastHTTPStatus,
			"state_version": run.StateVersion + 1, "updated_at": time.Now(),
		}).Error
	})
}

// ClaimDueTaskRun atomically claims one non-terminal background task whose
// lease is absent/expired and whose next poll is due. The returned lease must
// be released or rescheduled after the network call completes.
func ClaimDueTaskRun(owner string, lease time.Duration) (*TaskRun, error) {
	return ClaimDueTaskRunContext(context.Background(), owner, lease)
}

func ClaimDueTaskRunContext(ctx context.Context, owner string, lease time.Duration) (*TaskRun, error) {
	db, err := taskDB(ctx)
	if err != nil {
		return nil, err
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, errors.New("task lease owner is required")
	}
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	now := time.Now()
	nonPollableStatuses := []string{"materializing", "storage_retry_pending", "completed", "succeeded", "failed", "cancelled", "canceled", "expired", "timeout", "timed_out"}
	var claimed TaskRun
	err = db.Transaction(func(tx *gorm.DB) error {
		result := tx.Where("polling_mode = ? AND task_status NOT IN ? AND (next_poll_at IS NULL OR next_poll_at <= ?) AND (lease_expires_at IS NULL OR lease_expires_at <= ?)", "background", nonPollableStatuses, now, now).
			Order("COALESCE(next_poll_at, created_at) ASC, created_at ASC").First(&claimed)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return ErrTaskLeaseUnavailable
		}
		if result.Error != nil {
			return result.Error
		}
		expires := now.Add(lease)
		result = tx.Model(&claimed).Where("id = ? AND polling_mode = ? AND task_status NOT IN ? AND (next_poll_at IS NULL OR next_poll_at <= ?) AND (lease_expires_at IS NULL OR lease_expires_at <= ?)", claimed.ID, "background", nonPollableStatuses, now, now).
			Updates(map[string]any{"lease_owner": owner, "lease_expires_at": expires, "state_version": claimed.StateVersion + 1, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrTaskLeaseUnavailable
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	expires := now.Add(lease)
	claimed.LeaseOwner = owner
	claimed.LeaseExpiresAt = &expires
	claimed.UpdatedAt = now
	return &claimed, nil
}

// ClaimTaskRunPollContext leases one non-terminal client task for a single
// provider Poll. It shares the same durable lease columns as background work,
// so concurrent clients (and separate processes) cannot issue duplicate
// status requests for the same TaskRun.
func ClaimTaskRunPollContext(ctx context.Context, id, owner string, lease time.Duration) (*TaskRun, error) {
	db, err := taskDB(ctx)
	if err != nil {
		return nil, err
	}
	id, owner = strings.TrimSpace(id), strings.TrimSpace(owner)
	if id == "" || owner == "" {
		return nil, errors.New("task poll lease identity is required")
	}
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	now := time.Now()
	var claimed TaskRun
	err = db.Transaction(func(tx *gorm.DB) error {
		result := tx.Where("id = ? AND (polling_mode <> ? OR polling_mode = '') AND task_status NOT IN ? AND (lease_owner IS NULL OR lease_owner = '' OR lease_expires_at IS NULL OR lease_expires_at <= ?)", id, "off", []string{"completed", "succeeded", "failed", "cancelled", "canceled", "expired", "timeout", "timed_out"}, now).First(&claimed)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return ErrTaskLeaseUnavailable
		}
		if result.Error != nil {
			return result.Error
		}
		expires := now.Add(lease)
		result = tx.Model(&claimed).Where("id = ? AND (polling_mode <> ? OR polling_mode = '') AND task_status NOT IN ? AND (lease_owner IS NULL OR lease_owner = '' OR lease_expires_at IS NULL OR lease_expires_at <= ?)", id, "off", []string{"completed", "succeeded", "failed", "cancelled", "canceled", "expired", "timeout", "timed_out"}, now).
			Updates(map[string]any{"lease_owner": owner, "lease_expires_at": expires, "state_version": claimed.StateVersion + 1, "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrTaskLeaseUnavailable
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	expires := now.Add(lease)
	claimed.LeaseOwner, claimed.LeaseExpiresAt, claimed.UpdatedAt = owner, &expires, now
	return &claimed, nil
}

func ClaimTaskRunPoll(id, owner string, lease time.Duration) (*TaskRun, error) {
	return ClaimTaskRunPollContext(context.Background(), id, owner, lease)
}

func RescheduleTaskRunPoll(id, owner string, next time.Time) error {
	return RescheduleTaskRunPollContext(context.Background(), id, owner, next)
}

func RescheduleTaskRunPollContext(ctx context.Context, id, owner string, next time.Time) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	id, owner = strings.TrimSpace(id), strings.TrimSpace(owner)
	if id == "" || owner == "" {
		return errors.New("task lease identity is required")
	}
	result := db.Model(&TaskRun{}).Where("id = ? AND lease_owner = ?", id, owner).Updates(map[string]any{"next_poll_at": next, "lease_owner": "", "lease_expires_at": nil, "state_version": gorm.Expr("state_version + 1"), "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrTaskLeaseOwner
	}
	return nil
}

func ReleaseTaskRunLease(id, owner string) error {
	return ReleaseTaskRunLeaseContext(context.Background(), id, owner)
}

func ReleaseTaskRunLeaseContext(ctx context.Context, id, owner string) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	result := db.Model(&TaskRun{}).Where("id = ? AND lease_owner = ?", strings.TrimSpace(id), strings.TrimSpace(owner)).Updates(map[string]any{"lease_owner": "", "lease_expires_at": nil, "state_version": gorm.Expr("state_version + 1"), "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrTaskLeaseOwner
	}
	return nil
}

func taskStatusRank(status string) int {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "queued", "pending", "accepted":
		return 1
	case "processing", "running", "in_progress":
		return 2
	case "materializing", "storage_retry_pending":
		return 3
	case "completed", "succeeded", "failed", "cancelled", "canceled", "expired", "timeout", "timed_out":
		return 4
	default:
		return 0
	}
}

func isTaskTerminal(status string) bool { return taskStatusRank(status) == 4 }

func RecordTaskAlias(alias *TaskAlias) error {
	return RecordTaskAliasContext(context.Background(), alias)
}

func RecordTaskAliasContext(ctx context.Context, alias *TaskAlias) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	if alias == nil || strings.TrimSpace(alias.TaskRunID) == "" || strings.TrimSpace(alias.LookupID) == "" {
		return errors.New("task alias identity is required")
	}
	alias.TaskRunID, alias.LookupID = strings.TrimSpace(alias.TaskRunID), strings.TrimSpace(alias.LookupID)
	if alias.Source == "" {
		alias.Source = "create"
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var existing TaskAlias
		// A lookup ID is a global routing identity. Keep the existing row when
		// the same run retries, but refuse to attach it to another run.
		if err := tx.Where("lookup_id = ?", alias.LookupID).Order("id asc").First(&existing).Error; err == nil {
			if existing.TaskRunID != alias.TaskRunID {
				return fmt.Errorf("%w: %s", ErrTaskAliasConflict, alias.LookupID)
			}
			alias.ID, alias.CreatedAt = existing.ID, existing.CreatedAt
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return tx.Create(alias).Error
	})
}

func AppendTaskAttempt(attempt *TaskAttempt) error {
	return AppendTaskAttemptContext(context.Background(), attempt)
}

func AppendTaskAttemptContext(ctx context.Context, attempt *TaskAttempt) error {
	db, err := taskDB(ctx)
	if err != nil {
		return err
	}
	if attempt == nil || strings.TrimSpace(attempt.TaskRunID) == "" || strings.TrimSpace(attempt.AttemptType) == "" {
		return errors.New("task attempt identity is required")
	}
	attempt.TaskRunID, attempt.AttemptType = strings.TrimSpace(attempt.TaskRunID), strings.TrimSpace(attempt.AttemptType)
	if attempt.StartedAt.IsZero() {
		attempt.StartedAt = time.Now()
	}
	return db.Create(attempt).Error
}

func AppendTaskEvent(ctx context.Context, taskRunID, eventType, data string) (*TaskEvent, error) {
	db, err := taskDB(ctx)
	if err != nil {
		return nil, err
	}
	taskRunID, eventType = strings.TrimSpace(taskRunID), strings.TrimSpace(eventType)
	if taskRunID == "" || eventType == "" {
		return nil, errors.New("task event identity is required")
	}
	var event TaskEvent
	err = db.Transaction(func(tx *gorm.DB) error {
		var run TaskRun
		if err := tx.First(&run, "id = ?", taskRunID).Error; err != nil {
			return err
		}
		sequence := run.EventSequence + 1
		if err := tx.Model(&run).Updates(map[string]any{"event_sequence": sequence, "state_version": run.StateVersion + 1, "updated_at": time.Now()}).Error; err != nil {
			return err
		}
		event = TaskEvent{TaskRunID: taskRunID, Sequence: sequence, Type: eventType, Data: data}
		return tx.Create(&event).Error
	})
	if err != nil {
		return nil, err
	}
	return &event, nil
}
