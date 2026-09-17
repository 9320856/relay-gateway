package audit

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"

	"relay-gateway/db"
	"relay-gateway/model"
)

const MaxFieldBytes = 2 << 20

type contextKey string

const auditContextKey contextKey = "audit_entry"

var sensitiveName = regexp.MustCompile(`(?i)(authorization|api[-_]?key|token|secret|password|cookie|credential|signature)`)
var sensitiveInline = regexp.MustCompile(`(?i)\b(authorization|api[-_ ]?key|token|secret|password|cookie|credential|signature)\b\s*[:=]\s*["']?[^\s"',}&]+`)
var mediaCapabilityURL = regexp.MustCompile(`(?i)(?:https?://[^\s/"']+)?/v1/media/[A-Za-z0-9._~-]+/[A-Za-z0-9._~-]+`)

type AuditEntry struct {
	mu                  sync.Mutex
	row                 db.RequestLogModel
	conn                *gorm.DB
	sequence            int
	startErr            error
	writeErr            error
	done                bool
	asyncPoll           *asyncTaskPollMarker
	asyncPollCoalescing bool
	asyncPollCoalesced  bool

	ID        string            `json:"id"`
	StartTime time.Time         `json:"start_time"`
	Headers   map[string]string `json:"headers"`
}

type EventData struct {
	ChannelID  string
	TargetURL  string
	StatusCode int
	Message    string
	Data       any
}

type ListQuery struct {
	Page      int
	PageSize  int
	Kind      string
	ChannelID string
	Model     string
	Outcome   string
	Search    string
	From      *time.Time
	To        *time.Time
}

type Detail struct {
	Log         db.RequestLogModel     `json:"log"`
	Events      []db.RequestEventModel `json:"events"`
	TaskRun     *db.TaskRun            `json:"task_run,omitempty"`
	Attempts    []db.TaskAttempt       `json:"attempts,omitempty"`
	TaskEvents  []db.TaskEvent         `json:"task_events,omitempty"`
	MediaAssets []MediaAssetDetail     `json:"media_assets,omitempty"`
}

// MediaAssetDetail is the management-only projection of an asset. The
// capability-bearing URL is populated by the authenticated router response;
// it is never written back to the database or task/audit records.
type MediaAssetDetail struct {
	db.MediaAsset
	PublicURL      string `json:"public_url,omitempty"`
	PublicURLError string `json:"public_url_error,omitempty"`
}

// asyncTaskPollMarker is intentionally kept on the short-lived child audit
// entry until its HTTP result is known. The audit middleware invokes
// CoalesceMarkedAsyncTaskPoll after RecordResult; until then a failed request
// still has its own independent audit row.
type asyncTaskPollMarker struct {
	mapping         db.TaskMapping
	taskID          string
	taskKind        string
	status          string
	resultBody      string
	resultTruncated bool
	taskError       string
}

func newID() string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("req_%d", time.Now().UnixNano())
	}
	return "req_" + hex.EncodeToString(b)
}

func Start(kind, clientIP, method, path string, headers http.Header) (*AuditEntry, error) {
	return startWithDB(db.DB, kind, clientIP, method, path, headers)
}

// StartWithDB creates an audit entry on conn. Passing a transaction lets a
// management mutation commit its business row and its audit row atomically.
func StartWithDB(conn *gorm.DB, kind, clientIP, method, path string, headers http.Header) (*AuditEntry, error) {
	return startWithDB(conn, kind, clientIP, method, path, headers)
}

func startWithDB(conn *gorm.DB, kind, clientIP, method, path string, headers http.Header) (*AuditEntry, error) {
	now := time.Now().UTC()
	entry := &AuditEntry{ID: newID(), StartTime: now, Headers: sanitizeHeaders(headers)}
	encodedHeaders, _ := json.Marshal(entry.Headers)
	entry.row = db.RequestLogModel{ID: entry.ID, Kind: kind, StartedAt: now, Method: method, Path: path, ClientIP: clientIP, Outcome: "running", RequestHeaders: string(encodedHeaders)}
	entry.conn = conn
	if conn == nil {
		entry.startErr = errors.New("database is not initialized")
		return entry, entry.startErr
	}
	if err := conn.Create(&entry.row).Error; err != nil {
		entry.startErr = err
		return entry, err
	}
	if err := entry.addEventLocked("request_received", EventData{Message: method + " " + path}); err != nil {
		return entry, err
	}
	return entry, nil
}

// NewEntry is kept for integrations that used the original audit API.
// Deprecated: use Start or StartWithDB so transaction ownership is explicit.
func NewEntry(clientIP, method, path string, headers map[string]string) *AuditEntry {
	h := make(http.Header)
	for k, v := range headers {
		h.Set(k, v)
	}
	entry, _ := Start("api_call", clientIP, method, path, h)
	return entry
}

func WithAudit(ctx context.Context, entry *AuditEntry) context.Context {
	return context.WithValue(ctx, auditContextKey, entry)
}

func FromContext(ctx context.Context) *AuditEntry {
	if ctx == nil {
		return nil
	}
	entry, _ := ctx.Value(auditContextKey).(*AuditEntry)
	return entry
}

func RequestID(ctx context.Context) string {
	if entry := FromContext(ctx); entry != nil {
		return entry.ID
	}
	return ""
}

func AddEvent(ctx context.Context, phase string, data EventData) {
	if entry := FromContext(ctx); entry != nil {
		_ = entry.AddEvent(phase, data)
	}
}

func (e *AuditEntry) AddEvent(phase string, data EventData) error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.addEventLocked(phase, data)
}

// AddEventWithDB appends an event on conn instead of the entry's default
// connection. It is used by short-lived management transactions that must
// commit a business mutation and the corresponding audit event together. The
// network portion of a probe remains outside the transaction; only the final
// health-row update and this event share the SQLite transaction.
func (e *AuditEntry) AddEventWithDB(conn *gorm.DB, phase string, data EventData) error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if conn == nil {
		return errors.New("database is not initialized")
	}
	return e.addEventLockedOn(conn, phase, data)
}

func (e *AuditEntry) addEventLocked(phase string, data EventData) error {
	return e.addEventLockedOn(e.connection(), phase, data)
}

func (e *AuditEntry) addEventLockedOn(conn *gorm.DB, phase string, data EventData) error {
	if e.startErr != nil || conn == nil {
		return e.startErr
	}
	e.sequence++
	now := time.Now().UTC()
	encoded := ""
	if data.Data != nil {
		if b, err := json.Marshal(sanitizeValue("data", data.Data)); err == nil {
			encoded, _ = capture(b)
		}
	}
	event := db.RequestEventModel{RequestID: e.ID, Sequence: e.sequence, OccurredAt: now, ElapsedMs: now.Sub(e.StartTime).Milliseconds(), Phase: phase, ChannelID: data.ChannelID, TargetURL: sanitizeURL(data.TargetURL), StatusCode: data.StatusCode, Message: sanitizeText(data.Message), Data: encoded}
	err := conn.Create(&event).Error
	e.setWriteErr(err)
	return err
}

func (e *AuditEntry) connection() *gorm.DB {
	if e != nil && e.conn != nil {
		return e.conn
	}
	return db.DB
}

func (e *AuditEntry) setWriteErr(err error) {
	if err != nil && e.writeErr == nil {
		e.writeErr = err
	}
}

// WriteError reports a persistence failure after the initial request row was
// created. Transactional admin middleware uses it to roll back the business
// mutation instead of committing a change with an incomplete audit trail.
func (e *AuditEntry) WriteError() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.writeErr
}

func (e *AuditEntry) SetReqBody(body []byte, modelName string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.row.RequestedModel = strings.TrimSpace(modelName)
	e.row.IsStream = detectStream(body)
	e.row.RequestBody, e.row.RequestTruncated = sanitizePayload(body)
	if conn := e.connection(); e.startErr == nil && conn != nil {
		err := conn.Model(&db.RequestLogModel{}).Where("id = ?", e.ID).Updates(map[string]any{"requested_model": e.row.RequestedModel, "is_stream": e.row.IsStream, "request_body": e.row.RequestBody, "request_truncated": e.row.RequestTruncated}).Error
		e.setWriteErr(err)
		_ = e.addEventLocked("request_parsed", EventData{Message: "request body parsed", Data: map[string]any{"model": e.row.RequestedModel, "stream": e.row.IsStream}})
	}
}

func (e *AuditEntry) RecordDispatch(channelID, channelType, baseURL, targetModel string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.row.ChannelID, e.row.ChannelType, e.row.TargetModel = channelID, channelType, targetModel
	if conn := e.connection(); conn != nil {
		var cm db.ChannelModel
		if conn.Select("name").First(&cm, "id = ?", channelID).Error == nil {
			e.row.ChannelName = cm.Name
		}
		err := conn.Model(&db.RequestLogModel{}).Where("id = ?", e.ID).Updates(map[string]any{"channel_id": channelID, "channel_name": e.row.ChannelName, "channel_type": channelType, "target_model": targetModel}).Error
		e.setWriteErr(err)
		_ = e.addEventLocked("attempt_started", EventData{ChannelID: channelID, TargetURL: baseURL, Message: "dispatching to upstream", Data: map[string]any{"channel_type": channelType, "target_model": targetModel}})
	}
}

// RecordCandidates records the channels considered before the first upstream
// attempt, making failover decisions visible without one event per candidate.
func (e *AuditEntry) RecordCandidates(channelIDs []string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(channelIDs) == 0 || e.startErr != nil || db.DB == nil {
		return
	}
	_ = e.addEventLocked("candidate_channels", EventData{Data: map[string]any{"channel_ids": channelIDs}})
}

func (e *AuditEntry) RecordFailover(message string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.row.RetryCount++
	if conn := e.connection(); conn != nil {
		err := conn.Model(&db.RequestLogModel{}).Where("id = ?", e.ID).Update("retry_count", e.row.RetryCount).Error
		e.setWriteErr(err)
		_ = e.addEventLocked("failover", EventData{Message: message})
	}
}

func (e *AuditEntry) RecordResult(statusCode int, response []byte, resultErr error) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done {
		return
	}
	e.done = true
	now := time.Now().UTC()
	e.row.FinishedAt = &now
	e.row.DurationMs = now.Sub(e.StartTime).Milliseconds()
	e.row.StatusCode = statusCode
	e.row.ResponseBody, e.row.ResponseTruncated = sanitizePayload(response)
	if e.row.IsStream {
		streamText := aggregateStreamText(response)
		if streamText != "" {
			e.row.StreamText, e.row.StreamTruncated = capture([]byte(sanitizeText(streamText)))
		}
	}
	e.row.Outcome = outcome(statusCode, resultErr)
	if resultErr != nil && !isClientCancellation(resultErr) {
		e.row.ErrorMessage = sanitizeText(resultErr.Error())
	}
	extractUsage(response, &e.row)
	if conn := e.connection(); e.startErr != nil || conn == nil {
		return
	}
	conn := e.connection()
	err := conn.Model(&db.RequestLogModel{}).Where("id = ?", e.ID).Updates(map[string]any{
		"finished_at": e.row.FinishedAt, "duration_ms": e.row.DurationMs, "status_code": e.row.StatusCode,
		"response_body": e.row.ResponseBody, "response_truncated": e.row.ResponseTruncated,
		"stream_text": e.row.StreamText, "stream_truncated": e.row.StreamTruncated,
		"outcome": e.row.Outcome, "error_message": e.row.ErrorMessage,
		"input_tokens": e.row.InputTokens, "output_tokens": e.row.OutputTokens,
	}).Error
	e.setWriteErr(err)
	phase := "response_completed"
	if e.row.Outcome == "cancelled" {
		phase = "request_cancelled"
	} else if e.row.Outcome != "success" {
		phase = "request_failed"
	}
	_ = e.addEventLocked(phase, EventData{StatusCode: statusCode, Message: e.row.ErrorMessage, Data: map[string]any{"duration_ms": e.row.DurationMs}})
}

// RecordAsyncTaskCreated initializes the asynchronous-task summary on the
// current creation request. Call it once after all task aliases have been
// persisted, using the same request transaction when one is active. The
// parent request keeps its own HTTP outcome; later task failures are recorded
// exclusively in the async_* fields.
func RecordAsyncTaskCreated(ctx context.Context, taskID, taskKind, initialStatus string) error {
	entry := FromContext(ctx)
	if entry == nil {
		return nil
	}
	taskID = strings.TrimSpace(taskID)
	taskKind = normalizeAsyncTaskKind(taskKind)
	initialStatus = NormalizeAsyncTaskStatus(initialStatus)
	if taskID == "" || taskKind == "" {
		return errors.New("async task id and kind are required")
	}
	conn := db.DBForContext(ctx)
	if conn == nil {
		return errors.New("database is not initialized")
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.startErr != nil {
		return entry.startErr
	}
	updates := map[string]any{
		"async_task_kind":   taskKind,
		"async_task_id":     taskID,
		"async_task_status": initialStatus,
		"async_poll_count":  0,
	}
	var completedAt *time.Time
	if isTerminalAsyncTaskStatus(initialStatus) {
		now := time.Now().UTC()
		completedAt = &now
		updates["async_completed_at"] = completedAt
	}
	result := conn.Model(&db.RequestLogModel{}).Where("id = ?", entry.ID).Updates(updates)
	entry.setWriteErr(result.Error)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		err := gorm.ErrRecordNotFound
		entry.setWriteErr(err)
		return err
	}
	entry.row.AsyncTaskKind = taskKind
	entry.row.AsyncTaskID = taskID
	entry.row.AsyncTaskStatus = initialStatus
	entry.row.AsyncPollCount = 0
	entry.row.AsyncCompletedAt = completedAt
	return entry.addEventLockedOn(conn, "async_task_created", EventData{
		Message: "asynchronous task created",
		Data: map[string]any{
			"task_id":   taskID,
			"task_kind": taskKind,
			"status":    initialStatus,
		},
	})
}

// RecordAsyncTaskProgress records a private in-request poll against the
// current request's async-task summary. Unlike MarkAsyncTaskPoll it does not
// require a persisted task mapping and does not delete a child log: it is for
// adapters that submit and poll a provider task while serving one synchronous
// gateway request.
//
// Successful polls increment AsyncPollCount and refresh AsyncLastPolledAt.
// Every successful poll retains its latest sanitized result. A terminal status
// (or resultErr) additionally retains completion/error metadata while
// intentionally leaving the parent's HTTP Outcome untouched.
func RecordAsyncTaskProgress(ctx context.Context, taskID, taskKind, status string, response any, resultErr error) error {
	entry := FromContext(ctx)
	if entry == nil {
		return nil
	}
	taskID = truncateAsyncTaskField(strings.TrimSpace(taskID), 128)
	taskKind = normalizeAsyncTaskKind(taskKind)
	status = NormalizeAsyncTaskStatus(status)
	if resultErr != nil && !isTerminalAsyncTaskStatus(status) {
		status = "failed"
	}
	if taskID == "" || taskKind == "" || status == "" {
		return errors.New("async task id, kind, and status are required")
	}
	conn := db.DBForContext(ctx)
	if conn == nil {
		return errors.New("database is not initialized")
	}
	resultBody, resultTruncated := sanitizeAsyncTaskResult(response)
	taskError := ""
	if resultErr != nil {
		taskError = truncateAsyncTaskField(sanitizeText(resultErr.Error()), MaxFieldBytes)
	} else if isFailedAsyncTaskStatus(status) {
		taskError = summarizeAsyncTaskError(resultBody)
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.startErr != nil {
		return entry.startErr
	}
	now := time.Now().UTC()
	statusCanAdvance := asyncTaskStatusCanAdvance(entry.row.AsyncTaskStatus, status)
	statusChanged := statusCanAdvance && (entry.row.AsyncTaskKind != taskKind ||
		entry.row.AsyncTaskID != taskID ||
		entry.row.AsyncTaskStatus != status)
	resultCanRefresh := statusCanAdvance && (statusChanged ||
		asyncTaskResultCanRefresh(status, entry.row.AsyncResultBody, resultBody))
	nextPollCount := entry.row.AsyncPollCount
	updates := map[string]any{
		"async_task_kind": taskKind,
		"async_task_id":   taskID,
	}
	if statusCanAdvance {
		updates["async_task_status"] = status
	}
	if resultErr == nil {
		nextPollCount++
		updates["async_poll_count"] = nextPollCount
		updates["async_last_polled_at"] = &now
		if resultCanRefresh {
			updates["async_result_body"] = resultBody
			updates["async_result_truncated"] = resultTruncated
		}
	}
	if statusCanAdvance && isTerminalAsyncTaskStatus(status) {
		if entry.row.AsyncCompletedAt == nil {
			updates["async_completed_at"] = &now
		}
		updates["async_result_body"] = resultBody
		updates["async_result_truncated"] = resultTruncated
		updates["async_task_error"] = taskError
	}
	result := conn.Model(&db.RequestLogModel{}).Where("id = ?", entry.ID).Updates(updates)
	entry.setWriteErr(result.Error)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		err := gorm.ErrRecordNotFound
		entry.setWriteErr(err)
		return err
	}
	entry.row.AsyncTaskKind = taskKind
	entry.row.AsyncTaskID = taskID
	if statusCanAdvance {
		entry.row.AsyncTaskStatus = status
	}
	if resultErr == nil {
		entry.row.AsyncPollCount = nextPollCount
		entry.row.AsyncLastPolledAt = &now
		if resultCanRefresh {
			entry.row.AsyncResultBody = resultBody
			entry.row.AsyncResultTruncated = resultTruncated
		}
	}
	if statusCanAdvance && isTerminalAsyncTaskStatus(status) {
		if entry.row.AsyncCompletedAt == nil {
			entry.row.AsyncCompletedAt = &now
		}
		entry.row.AsyncResultBody = resultBody
		entry.row.AsyncResultTruncated = resultTruncated
		entry.row.AsyncTaskError = taskError
	}
	if !statusChanged {
		return nil
	}
	return entry.addEventLockedOn(conn, "async_task_status_changed", EventData{
		Message: "asynchronous task status changed to " + status,
		Data: map[string]any{
			"task_id":    taskID,
			"task_kind":  taskKind,
			"status":     status,
			"poll_count": nextPollCount,
			"terminal":   isTerminalAsyncTaskStatus(status),
		},
	})
}

// MarkAsyncTaskPoll marks the current request as a successfully handled
// asynchronous-task status poll. It deliberately does not mutate persistent
// state yet: if the handler, response writer, or audit completion later fails,
// the child request must remain independently auditable. It returns false when
// there is no known task mapping, no origin audit request, a task-kind mismatch,
// or an upstream error.
func MarkAsyncTaskPoll(ctx context.Context, taskID, taskKind, status string, response any, resultErr error) bool {
	entry := FromContext(ctx)
	if entry == nil || resultErr != nil {
		return false
	}
	taskKind = normalizeAsyncTaskKind(taskKind)
	if taskKind == "" {
		return false
	}
	mapping := db.GetTaskMappingForKind(taskID, taskKind)
	if mapping == nil || strings.TrimSpace(mapping.OriginRequestID) == "" {
		return false
	}
	status = NormalizeAsyncTaskStatus(status)
	if taskKind == "" || status == "" {
		return false
	}
	resultBody, resultTruncated := sanitizeAsyncTaskResult(response)
	marker := &asyncTaskPollMarker{
		mapping:         *mapping,
		taskID:          canonicalAsyncTaskID(*mapping),
		taskKind:        taskKind,
		status:          status,
		resultBody:      resultBody,
		resultTruncated: resultTruncated,
	}
	if isFailedAsyncTaskStatus(status) {
		marker.taskError = summarizeAsyncTaskError(resultBody)
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.startErr != nil || entry.done {
		return false
	}
	entry.asyncPoll = marker
	return true
}

// CoalesceMarkedAsyncTaskPoll merges a completed, marked polling request into
// its creation request. The parent summary update, optional lifecycle event,
// and child-row/event deletion all happen in one transaction. Any error rolls
// the transaction back, leaving the child log untouched for investigation.
//
// The caller should invoke this only after entry.RecordResult. Calling it for
// unmarked or unsuccessful requests is a no-op.
func CoalesceMarkedAsyncTaskPoll(entry *AuditEntry) error {
	if entry == nil {
		return nil
	}
	entry.mu.Lock()
	if entry.writeErr != nil {
		err := entry.writeErr
		entry.mu.Unlock()
		return err
	}
	if entry.asyncPoll == nil || entry.asyncPollCoalesced || entry.asyncPollCoalescing || entry.row.Outcome != "success" || !entry.done {
		entry.mu.Unlock()
		return nil
	}
	marker := *entry.asyncPoll
	childID := entry.ID
	entry.asyncPollCoalescing = true
	entry.mu.Unlock()

	if marker.mapping.OriginRequestID == "" || marker.mapping.OriginRequestID == childID {
		entry.mu.Lock()
		entry.asyncPollCoalescing = false
		entry.mu.Unlock()
		return nil
	}
	if db.DB == nil {
		entry.mu.Lock()
		entry.asyncPollCoalescing = false
		entry.mu.Unlock()
		return errors.New("database is not initialized")
	}

	err := db.DB.Transaction(func(tx *gorm.DB) error {
		// Re-read the mapping inside this transaction. A deleted, expired, or
		// repointed mapping must not cause an unrelated log to be merged.
		var mapping db.TaskMapping
		if err := tx.First(&mapping, "task_id = ?", marker.mapping.TaskID).Error; err != nil {
			return err
		}
		mappingTaskKind := normalizeAsyncTaskKind(mapping.TaskKind)
		if mappingTaskKind == "" {
			mappingTaskKind = "video"
		}
		if mapping.OriginRequestID != marker.mapping.OriginRequestID ||
			mappingTaskKind != marker.taskKind ||
			mapping.CreatedAt.IsZero() || time.Since(mapping.CreatedAt) > 7*24*time.Hour {
			return errors.New("async task mapping is no longer eligible for audit coalescing")
		}

		var parent db.RequestLogModel
		if err := tx.First(&parent, "id = ?", mapping.OriginRequestID).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		pollUpdates := map[string]any{
			"async_task_kind":      marker.taskKind,
			"async_task_id":        marker.taskID,
			"async_poll_count":     gorm.Expr("async_poll_count + ?", 1),
			"async_last_polled_at": &now,
		}
		if result := tx.Model(&db.RequestLogModel{}).Where("id = ?", parent.ID).Updates(pollUpdates); result.Error != nil {
			return result.Error
		} else if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		// pollUpdates obtains the SQLite writer lock. Re-read after it so status
		// and progress decisions use the latest committed result rather than the
		// snapshot taken while validating the parent row above.
		if err := tx.First(&parent, "id = ?", parent.ID).Error; err != nil {
			return err
		}

		statusUpdates := map[string]any{
			"async_task_status":      marker.status,
			"async_result_body":      marker.resultBody,
			"async_result_truncated": marker.resultTruncated,
		}
		if isTerminalAsyncTaskStatus(marker.status) {
			statusUpdates["async_completed_at"] = gorm.Expr("COALESCE(async_completed_at, ?)", now)
			statusUpdates["async_task_error"] = marker.taskError
		}

		currentStatus := NormalizeAsyncTaskStatus(parent.AsyncTaskStatus)
		statusCanAdvance := asyncTaskStatusCanAdvance(currentStatus, marker.status)
		statusChanged := statusCanAdvance && currentStatus != marker.status
		if statusChanged {
			// Compare-and-set the raw stored status as well as checking the status
			// rank in Go. This prevents a concurrent terminal update from being
			// replaced on databases that do not serialize writers like SQLite.
			transition := tx.Model(&db.RequestLogModel{}).
				Where("id = ? AND COALESCE(async_task_status, '') = ?", parent.ID, parent.AsyncTaskStatus).
				Updates(statusUpdates)
			if transition.Error != nil {
				return transition.Error
			}
			statusChanged = transition.RowsAffected > 0
		} else if statusCanAdvance && currentStatus == marker.status &&
			asyncTaskResultCanRefresh(marker.status, parent.AsyncResultBody, marker.resultBody) {
			// Repeated observations may update progress or enrich a terminal result.
			// Use the previous body as a CAS token so an unseen concurrent refresh
			// cannot be overwritten with an older same-status response.
			refresh := tx.Model(&db.RequestLogModel{}).
				Where("id = ? AND COALESCE(async_task_status, '') = ?", parent.ID, parent.AsyncTaskStatus).
				Where("COALESCE(async_result_body, '') = ?", parent.AsyncResultBody).
				Updates(statusUpdates)
			if refresh.Error != nil {
				return refresh.Error
			}
		}

		if err := tx.First(&parent, "id = ?", parent.ID).Error; err != nil {
			return err
		}
		nextPollCount := parent.AsyncPollCount

		if statusChanged {
			var latestSequence int
			if err := tx.Model(&db.RequestEventModel{}).Where("request_id = ?", parent.ID).Select("COALESCE(MAX(sequence), 0)").Scan(&latestSequence).Error; err != nil {
				return err
			}
			eventData, _ := json.Marshal(sanitizeValue("data", map[string]any{
				"task_id":    marker.taskID,
				"task_kind":  marker.taskKind,
				"status":     marker.status,
				"poll_count": nextPollCount,
				"terminal":   isTerminalAsyncTaskStatus(marker.status),
			}))
			event := db.RequestEventModel{
				RequestID:  parent.ID,
				Sequence:   latestSequence + 1,
				OccurredAt: now,
				ElapsedMs:  now.Sub(parent.StartedAt).Milliseconds(),
				Phase:      "async_task_status_changed",
				Message:    sanitizeText("asynchronous task status changed to " + marker.status),
				Data:       string(eventData),
			}
			if err := tx.Create(&event).Error; err != nil {
				return err
			}
		}

		if err := tx.Where("request_id = ?", childID).Delete(&db.RequestEventModel{}).Error; err != nil {
			return err
		}
		if result := tx.Delete(&db.RequestLogModel{}, "id = ?", childID); result.Error != nil {
			return result.Error
		} else if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
	entry.mu.Lock()
	entry.asyncPollCoalescing = false
	if err == nil {
		entry.asyncPollCoalesced = true
	}
	entry.mu.Unlock()
	return err
}

// NormalizeAsyncTaskStatus makes the async summary stable across providers
// that use different terminal-status spellings. Unknown nonempty states are
// retained verbatim (lowercased) rather than being guessed as terminal.
func NormalizeAsyncTaskStatus(status string) string {
	return truncateAsyncTaskField(model.NormalizeTaskStatus(status), 64)
}

func normalizeAsyncTaskKind(kind string) string {
	return truncateAsyncTaskField(strings.ToLower(strings.TrimSpace(kind)), 32)
}

func canonicalAsyncTaskID(mapping db.TaskMapping) string {
	if alias := strings.TrimSpace(mapping.TaskAlias); alias != "" {
		return truncateAsyncTaskField(alias, 128)
	}
	return truncateAsyncTaskField(mapping.TaskID, 128)
}

func truncateAsyncTaskField(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func isTerminalAsyncTaskStatus(status string) bool {
	return status == "completed" || isFailedAsyncTaskStatus(status)
}

// Known task states advance monotonically from queued to processing to a
// terminal result. Unknown provider-specific states remain permissive so a new
// spelling cannot freeze a task indefinitely. Repeated observations of the
// same terminal state may still enrich the retained result body.
func asyncTaskStatusCanAdvance(current, next string) bool {
	current = NormalizeAsyncTaskStatus(current)
	next = NormalizeAsyncTaskStatus(next)
	if isTerminalAsyncTaskStatus(current) {
		return current == next
	}
	currentRank, currentKnown := asyncTaskStatusRank(current)
	nextRank, nextKnown := asyncTaskStatusRank(next)
	if currentKnown && nextKnown {
		return nextRank >= currentRank
	}
	return true
}

func asyncTaskStatusRank(status string) (int, bool) {
	switch NormalizeAsyncTaskStatus(status) {
	case "":
		return 0, true
	case "queued":
		return 1, true
	case "processing", "materializing":
		return 2, true
	case "completed", "failed":
		return 3, true
	default:
		return 0, false
	}
}

// Non-terminal result bodies are kept only when their numeric progress does
// not go backwards. Poll counters and timestamps still advance for stale
// observations. Terminal responses remain refreshable so providers can add a
// result URL after first reporting completion.
func asyncTaskResultCanRefresh(status, currentBody, nextBody string) bool {
	if isTerminalAsyncTaskStatus(NormalizeAsyncTaskStatus(status)) {
		return true
	}
	currentProgress, currentHasProgress := asyncTaskResultProgress(currentBody)
	if !currentHasProgress {
		return true
	}
	nextProgress, nextHasProgress := asyncTaskResultProgress(nextBody)
	return nextHasProgress && nextProgress >= currentProgress
}

func asyncTaskResultProgress(body string) (float64, bool) {
	if strings.TrimSpace(body) == "" {
		return 0, false
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return 0, false
	}
	return findAsyncTaskProgress(value)
}

func findAsyncTaskProgress(value any) (float64, bool) {
	switch item := value.(type) {
	case map[string]any:
		if raw, ok := item["progress"]; ok {
			switch progress := raw.(type) {
			case json.Number:
				parsed, err := progress.Float64()
				return parsed, err == nil
			case float64:
				return progress, true
			case string:
				parsed, err := strconv.ParseFloat(strings.TrimSpace(progress), 64)
				return parsed, err == nil
			}
		}
		for _, key := range []string{"response", "job", "data"} {
			if nested, ok := item[key]; ok {
				if progress, found := findAsyncTaskProgress(nested); found {
					return progress, true
				}
			}
		}
	case []any:
		for _, nested := range item {
			if progress, found := findAsyncTaskProgress(nested); found {
				return progress, true
			}
		}
	}
	return 0, false
}

func isFailedAsyncTaskStatus(status string) bool {
	return status == "failed"
}

func sanitizeAsyncTaskResult(response any) (string, bool) {
	switch value := response.(type) {
	case nil:
		return "", false
	case []byte:
		return sanitizePayload(value)
	case json.RawMessage:
		return sanitizePayload(value)
	case string:
		return sanitizePayload([]byte(value))
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return "[UNSERIALIZABLE async task result]", false
		}
		return sanitizePayload(encoded)
	}
}

func summarizeAsyncTaskError(resultBody string) string {
	if strings.TrimSpace(resultBody) == "" {
		return "asynchronous task failed"
	}
	var value any
	if json.Unmarshal([]byte(resultBody), &value) == nil {
		if message := findAsyncTaskError(value); message != "" {
			return truncateAsyncTaskField(sanitizeText(message), MaxFieldBytes)
		}
	}
	return "asynchronous task failed"
}

func findAsyncTaskError(value any) string {
	switch item := value.(type) {
	case map[string]any:
		for _, key := range []string{"error", "error_message", "message", "detail", "reason"} {
			if raw, ok := item[key]; ok {
				switch typed := raw.(type) {
				case string:
					if strings.TrimSpace(typed) != "" {
						return typed
					}
				default:
					if nested := findAsyncTaskError(typed); nested != "" {
						return nested
					}
				}
			}
		}
		for _, raw := range item {
			if nested := findAsyncTaskError(raw); nested != "" {
				return nested
			}
		}
	case []any:
		for _, raw := range item {
			if nested := findAsyncTaskError(raw); nested != "" {
				return nested
			}
		}
	}
	return ""
}

func outcome(status int, err error) string {
	if isClientCancellation(err) {
		return "cancelled"
	}
	if status >= 200 && status < 400 && err == nil {
		return "success"
	}
	if status == 0 {
		return "interrupted"
	}
	return "error"
}

func isClientCancellation(err error) bool {
	return errors.Is(err, context.Canceled)
}

func sanitizeHeaders(headers http.Header) map[string]string {
	out := make(map[string]string, len(headers))
	for key, values := range headers {
		if sensitiveName.MatchString(key) {
			out[key] = "[REDACTED]"
		} else {
			// A provider-specific header may carry a bearer/API token even when
			// its name is not one of the standard credential headers.
			out[key] = sanitizeText(strings.Join(values, ", "))
		}
	}
	return out
}

func sanitizePayload(body []byte) (string, bool) {
	if len(body) == 0 {
		return "", false
	}
	if !utf8.Valid(body) {
		sum := sha256.Sum256(body)
		return fmt.Sprintf("[BINARY length=%d sha256=%x]", len(body), sum[:]), false
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if decoder.Decode(&value) == nil {
		clean := sanitizeValue("", value)
		if encoded, err := json.MarshalIndent(clean, "", "  "); err == nil {
			return capture(encoded)
		}
	}
	return capture([]byte(sanitizeText(string(body))))
}

func sanitizeValue(key string, value any) any {
	if sensitiveName.MatchString(key) {
		return "[REDACTED]"
	}
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, v := range typed {
			out[k] = sanitizeValue(k, v)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, v := range typed {
			out[i] = sanitizeValue(key, v)
		}
		return out
	case string:
		if looksLikeBase64Field(key, typed) {
			sum := sha256.Sum256([]byte(typed))
			return fmt.Sprintf("[BASE64 length=%d sha256=%x]", len(typed), sum[:])
		}
		if parsed, err := url.Parse(typed); err == nil && parsed.IsAbs() && (parsed.RawQuery != "" || isPublicMediaURLPath(parsed.Path)) {
			return sanitizeURL(typed)
		}
		// Parsed JSON payloads used to bypass sanitizeText, allowing inline
		// credentials embedded in ordinary error/message strings (for example
		// {"error":"token=..."}) to be persisted verbatim.  Apply the same
		// recursive inline-secret redaction to every string value.
		return sanitizeText(typed)
	default:
		return value
	}
}

func looksLikeBase64Field(key, value string) bool {
	lowerKey := strings.ToLower(strings.TrimSpace(key))
	// b64_json 是标准 OpenAI 图像生成接口的核心返回数据，审计日志需保留完整 Base64 供控制台画廊直接渲染呈现图片
	if lowerKey == "b64_json" {
		return false
	}
	sample := value
	if comma := strings.Index(sample, ","); strings.HasPrefix(sample, "data:") && comma >= 0 {
		if !strings.Contains(strings.ToLower(sample[:comma]), ";base64") {
			return false
		}
		sample = sample[comma+1:]
	}
	// Explicit base64 fields may be short; arbitrary text is only considered
	// base64 when it is large enough to avoid turning ordinary prose into a
	// digest.  Data URLs are unambiguous and are handled at any size.
	explicit := strings.Contains(lowerKey, "base64") || lowerKey == "b64"
	if len(sample) < 1024 && !explicit && !strings.HasPrefix(strings.ToLower(value), "data:") {
		return false
	}
	trimmed := sample[:len(sample)-(len(sample)%4)]
	if trimmed == "" {
		return false
	}
	_, err := base64.StdEncoding.DecodeString(trimmed)
	return err == nil
}

func sanitizeURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return sanitizeText(raw)
	}
	if isPublicMediaURLPath(u.Path) {
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		u.Path = "/v1/media/" + parts[2] + "/[REDACTED]"
		u.RawPath = ""
	}
	query := u.Query()
	for key := range query {
		if sensitiveName.MatchString(key) {
			query.Set(key, "[REDACTED]")
		}
	}
	u.RawQuery = query.Encode()
	u.User = nil
	return u.String()
}

func sanitizeText(value string) string {
	value = mediaCapabilityURL.ReplaceAllString(value, "[MEDIA_URL_REDACTED]")
	value = sensitiveInline.ReplaceAllString(value, "$1=[REDACTED]")
	value = regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+\-/]+=*`).ReplaceAllString(value, "Bearer [REDACTED]")
	return regexp.MustCompile(`(?i)\bsk-[A-Za-z0-9._~+\-/]+=*`).ReplaceAllString(value, "[REDACTED]")
}

func isPublicMediaURLPath(path string) bool {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return len(parts) == 4 && strings.EqualFold(parts[0], "v1") && strings.EqualFold(parts[1], "media") && parts[2] != "" && parts[3] != ""
}

func capture(body []byte) (string, bool) {
	if len(body) <= MaxFieldBytes {
		return string(body), false
	}
	return string(body[:MaxFieldBytes]) + "\n[TRUNCATED at 2 MiB]", true
}

func detectStream(body []byte) bool {
	var meta struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(body, &meta)
	return meta.Stream
}

// aggregateStreamText extracts the user-visible text from common SSE response
// formats without creating one audit event per token.  The raw response is
// still retained separately (subject to the normal capture limit), while this
// field makes a streamed request useful to inspect in the log drawer.
func aggregateStreamText(body []byte) string {
	var out strings.Builder
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(payload), &event) != nil {
			continue
		}
		// OpenAI Chat Completions chunks.
		if choices, ok := event["choices"].([]any); ok {
			for _, rawChoice := range choices {
				choice, _ := rawChoice.(map[string]any)
				delta, _ := choice["delta"].(map[string]any)
				appendStreamString(&out, delta, "content")
				appendStreamString(&out, delta, "reasoning_content")
			}
		}
		// Anthropic content_block_delta events and OpenAI Responses text deltas.
		if delta, ok := event["delta"].(map[string]any); ok {
			appendStreamString(&out, delta, "text")
			appendStreamString(&out, delta, "thinking")
			appendStreamString(&out, delta, "partial_json")
		} else if delta, ok := event["delta"].(string); ok {
			out.WriteString(delta)
		}
	}
	return out.String()
}

func appendStreamString(out *strings.Builder, value map[string]any, key string) {
	if text, ok := value[key].(string); ok {
		out.WriteString(text)
	}
}

func extractUsage(body []byte, row *db.RequestLogModel) {
	var payload struct {
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			InputTokens      int `json:"input_tokens"`
			OutputTokens     int `json:"output_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &payload) == nil {
		row.InputTokens = payload.Usage.PromptTokens
		if row.InputTokens == 0 {
			row.InputTokens = payload.Usage.InputTokens
		}
		row.OutputTokens = payload.Usage.CompletionTokens
		if row.OutputTokens == 0 {
			row.OutputTokens = payload.Usage.OutputTokens
		}
	}
}

func GetDetail(id string) (*Detail, error) {
	if db.DB == nil {
		return nil, errors.New("database is not initialized")
	}
	detail := Detail{
		Events: make([]db.RequestEventModel, 0),
	}
	if err := db.DB.First(&detail.Log, "id = ?", id).Error; err != nil {
		return nil, err
	}
	if err := db.DB.Where("request_id = ?", id).Order("sequence asc").Find(&detail.Events).Error; err != nil {
		return nil, err
	}

	// TaskRun is the durable lifecycle aggregate for an asynchronous request.
	// Prefer the immutable origin request link; the alias lookup is a
	// compatibility fallback for older projections that did not retain it.
	var run db.TaskRun
	runErr := gorm.ErrRecordNotFound
	if strings.TrimSpace(id) != "" {
		runErr = db.DB.Where("origin_request_id = ?", id).Order("created_at asc, id asc").First(&run).Error
	}
	if errors.Is(runErr, gorm.ErrRecordNotFound) && strings.TrimSpace(detail.Log.AsyncTaskID) != "" {
		lookupIDs := []string{strings.TrimSpace(detail.Log.AsyncTaskID)}
		// Image mappings use an internal namespace to avoid colliding with
		// video task IDs, while the audit row intentionally stores the public ID.
		if strings.EqualFold(strings.TrimSpace(detail.Log.AsyncTaskKind), "image") {
			lookupIDs = append(lookupIDs, db.ImageTaskMappingLookupPrefix+strings.TrimSpace(detail.Log.AsyncTaskID))
		}
		for _, lookupID := range lookupIDs {
			var alias db.TaskAlias
			aliasErr := db.DB.Where("lookup_id = ?", lookupID).Order("id asc").First(&alias).Error
			if errors.Is(aliasErr, gorm.ErrRecordNotFound) {
				continue
			}
			if aliasErr != nil {
				return nil, aliasErr
			}
			runErr = db.DB.First(&run, "id = ?", alias.TaskRunID).Error
			break
		}
	}
	if runErr != nil && !errors.Is(runErr, gorm.ErrRecordNotFound) {
		return nil, runErr
	}
	if runErr == nil {
		detail.TaskRun = &run
		detail.Attempts = make([]db.TaskAttempt, 0)
		if err := db.DB.Where("task_run_id = ?", run.ID).Order("started_at asc, id asc").Find(&detail.Attempts).Error; err != nil {
			return nil, err
		}
		detail.TaskEvents = make([]db.TaskEvent, 0)
		if err := db.DB.Where("task_run_id = ?", run.ID).Order("sequence asc, id asc").Find(&detail.TaskEvents).Error; err != nil {
			return nil, err
		}
	}

	// Synchronous media has no TaskRun and is linked directly to the audit
	// request. Async media normally has both links, while historical rows may
	// have only one, so query both identities in one de-duplicated result set.
	assets := make([]db.MediaAsset, 0)
	assetQuery := db.DB.Where("origin_request_id = ?", id)
	if detail.TaskRun != nil {
		assetQuery = db.DB.Where("origin_request_id = ? OR task_run_id = ?", id, detail.TaskRun.ID)
	}
	if err := assetQuery.Order("ordinal asc, id asc").Find(&assets).Error; err != nil {
		return nil, err
	}
	if len(assets) > 0 {
		detail.MediaAssets = make([]MediaAssetDetail, 0, len(assets))
		for _, asset := range assets {
			detail.MediaAssets = append(detail.MediaAssets, MediaAssetDetail{MediaAsset: asset})
		}
	}
	return &detail, nil
}

func List(query ListQuery) ([]db.RequestLogModel, int64, error) {
	if db.DB == nil {
		return nil, 0, errors.New("database is not initialized")
	}
	if query.Page < 1 {
		query.Page = 1
	}
	// OFFSET grows linearly with the page number and can monopolize SQLite for
	// minutes on a large audit table. Keep the UI/API within a bounded window;
	// callers needing archival export should use a time range and page through
	// it rather than an unbounded deep offset.
	if query.Page > 10000 {
		query.Page = 10000
	}
	if query.PageSize < 1 {
		query.PageSize = 50
	}
	if query.PageSize > 200 {
		query.PageSize = 200
	}
	query.Model = strings.TrimSpace(query.Model)
	query.Search = strings.TrimSpace(query.Search)
	if len(query.Model) > 128 {
		query.Model = query.Model[:128]
	}
	if len(query.Search) > 256 {
		query.Search = query.Search[:256]
	}
	q := db.DB.Model(&db.RequestLogModel{})
	if query.Kind != "" {
		q = q.Where("kind = ?", query.Kind)
	}
	if query.ChannelID != "" {
		q = q.Where("channel_id = ?", query.ChannelID)
	}
	if query.Model != "" {
		q = q.Where("requested_model LIKE ? OR target_model LIKE ?", "%"+query.Model+"%", "%"+query.Model+"%")
	}
	if query.Outcome != "" {
		q = q.Where("outcome = ?", query.Outcome)
	}
	if query.Search != "" {
		like := "%" + query.Search + "%"
		q = q.Where("id LIKE ? OR path LIKE ? OR error_message LIKE ? OR request_body LIKE ? OR target_model LIKE ? OR requested_model LIKE ? OR channel_name LIKE ?", like, like, like, like, like, like, like)
	}
	if query.From != nil {
		q = q.Where("started_at >= ?", *query.From)
	}
	if query.To != nil {
		q = q.Where("started_at <= ?", *query.To)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var rows []db.RequestLogModel
	err := q.Select("id", "kind", "started_at", "finished_at", "duration_ms", "method", "path", "client_ip", "requested_model", "target_model", "channel_id", "channel_name", "channel_type", "status_code", "outcome", "is_stream", "input_tokens", "output_tokens", "retry_count", "async_task_kind", "async_task_id", "async_task_status", "async_poll_count", "async_last_polled_at", "async_completed_at", "request_truncated", "response_truncated", "request_body", "error_message").Order("started_at desc").Offset((query.Page - 1) * query.PageSize).Limit(query.PageSize).Find(&rows).Error
	return rows, total, err
}

func DeleteAll() error {
	return DeleteAllContext(context.Background(), "")
}

// DeleteAllContext clears historical logs while optionally preserving the
// audit row for the deletion request itself. This lets DELETE /api/logs remain
// an auditable, atomic admin mutation when called inside the request tx.
func DeleteAllContext(ctx context.Context, preserveID string) error {
	conn := db.DBForContext(ctx)
	if conn == nil {
		return errors.New("database is not initialized")
	}
	deleteFn := func(tx *gorm.DB) error {
		events := tx.Session(&gorm.Session{AllowGlobalUpdate: true})
		logs := tx.Session(&gorm.Session{AllowGlobalUpdate: true})
		if strings.TrimSpace(preserveID) != "" {
			events = events.Where("request_id <> ?", preserveID)
			logs = logs.Where("id <> ?", preserveID)
		}
		if err := events.Delete(&db.RequestEventModel{}).Error; err != nil {
			return err
		}
		return logs.Delete(&db.RequestLogModel{}).Error
	}
	if conn == db.DB {
		return conn.Transaction(deleteFn)
	}
	return deleteFn(conn)
}

func Cleanup(retentionDays int) error {
	if db.DB == nil {
		return errors.New("database is not initialized")
	}
	if retentionDays < 1 {
		retentionDays = 1
	}
	if retentionDays > 365 {
		retentionDays = 365
	}
	cutoff := time.Now().UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
	for {
		batchSize := 0
		if err := db.DB.Transaction(func(tx *gorm.DB) error {
			var ids []string
			if err := tx.Model(&db.RequestLogModel{}).Where("started_at < ?", cutoff).Order("started_at asc").Limit(500).Pluck("id", &ids).Error; err != nil {
				return err
			}
			batchSize = len(ids)
			if len(ids) == 0 {
				return nil
			}
			if err := tx.Where("request_id IN ?", ids).Delete(&db.RequestEventModel{}).Error; err != nil {
				return err
			}
			return tx.Where("id IN ?", ids).Delete(&db.RequestLogModel{}).Error
		}); err != nil {
			return err
		}
		if batchSize < 500 {
			return nil
		}
	}
}

// StartCleanupWorker starts periodic audit cleanup and returns a channel that
// closes after the worker has observed cancellation. Callers should wait for it
// before closing the database during shutdown.
func StartCleanupWorker(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		run := func() {
			days, err := strconv.Atoi(db.GetSetting("audit_retention_days", "30"))
			if err != nil {
				days = 30
			}
			_ = Cleanup(days)
		}
		run()
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
	return done
}

func MaskSecret(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return "Bearer " + maskToken(strings.TrimSpace(value[7:]))
	}
	return maskToken(value)
}

func maskToken(token string) string {
	if len(token) <= 8 {
		return "****"
	}
	if len(token) <= 16 {
		return token[:3] + "****" + token[len(token)-3:]
	}
	return token[:6] + "***" + token[len(token)-4:]
}
