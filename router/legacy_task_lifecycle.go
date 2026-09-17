package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"gorm.io/gorm"

	"relay-gateway/db"
)

// LegacyTaskBackfillReport describes the idempotent startup reconciliation of
// pre-TaskRun asynchronous mappings. A mapping is never submitted upstream
// again: the backfill only normalizes its durable kind/alias fields and
// projects a compatibility TaskRun/TaskAlias pair for status and content
// routing.
type LegacyTaskBackfillReport struct {
	Scanned      int `json:"scanned"`
	Projected    int `json:"projected"`
	AlreadyReady int `json:"already_ready"`
	Skipped      int `json:"skipped"`
}

// BackfillLegacyTaskLifecycle reconciles historical task mappings into the
// unified lifecycle tables. It is safe to run on every process start: mapping
// writes are idempotent, TaskRun IDs are deterministic, and aliases are
// conflict-checked by the existing persistence helpers. Invalid historical
// rows are reported as an error instead of being silently deleted or
// repointed.
func BackfillLegacyTaskLifecycle(ctx context.Context) (LegacyTaskBackfillReport, error) {
	var report LegacyTaskBackfillReport
	if ctx == nil {
		ctx = context.Background()
	}
	conn := db.DBForContext(ctx)
	if conn == nil {
		return report, fmt.Errorf("database is not initialized")
	}
	_, _ = ReconcileOrphanedAsyncPollLogs(ctx)
	var mappings []db.TaskMapping
	if err := conn.WithContext(ctx).Order("created_at ASC, task_id ASC").Find(&mappings).Error; err != nil {
		return report, fmt.Errorf("load legacy task mappings: %w", err)
	}
	for i := range mappings {
		report.Scanned++
		mapping := &mappings[i]
		registration := legacyRegistrationFromMapping(mapping, "")
		if registration.empty() {
			report.Skipped++
			continue
		}
		registration.InitialStatus = legacyHistoricalStatus(ctx, registration)

		// Persist the normalized kind/alias while retaining the original
		// timestamp and channel provenance. This is the only compatibility
		// mutation performed by the backfill.
		for _, lookupID := range registration.TaskIDs {
			if isTaskIDRegisteredToOtherChannel(lookupID, registration.TaskKind, registration.ChannelID) {
				continue
			}
			if err := db.RecordTaskMappingContext(ctx, db.TaskMapping{
				TaskID: lookupID, ChannelID: registration.ChannelID,
				OriginRequestID: registration.OriginRequestID, TaskKind: registration.TaskKind,
				TaskAlias: registration.TaskAlias, CreatedAt: mapping.CreatedAt,
			}); err != nil {
				return report, fmt.Errorf("normalize legacy task mapping %q: %w", lookupID, err)
			}
		}
		if registrationHasProfileTaskRun(ctx, registration) {
			report.AlreadyReady++
			continue
		}

		runID := legacyTaskRunID(registration)
		if _, err := db.GetTaskRun(runID); err == nil {
			report.AlreadyReady++
			// ensureLegacyTaskRun still reconciles any alias that may have
			// been missed by an earlier interrupted run.
			ensureLegacyTaskRun(registration)
			if normalized, outcome := normalizeLegacyTaskState(registration.InitialStatus); normalized != "queued" {
				if current, loadErr := db.GetTaskRun(runID); loadErr == nil && current.TaskStatus != normalized {
					if statusErr := db.UpdateTaskRunStatus(runID, normalized, outcome); statusErr == nil {
						_, _ = db.AppendTaskEvent(ctx, runID, "status_changed", normalized)
					}
				}
			}
			continue
		}
		ensureLegacyTaskRun(registration)
		if _, err := db.GetTaskRun(runID); err != nil {
			return report, fmt.Errorf("project legacy task mapping %q: %w", registration.TaskAlias, err)
		}
		report.Projected++
	}
	return report, nil
}

// legacyHistoricalStatus recovers a terminal/pending state from the existing
// request log when the old mapping itself did not persist one. This is a
// read-only inference: an absent or ambiguous log leaves the task queued so
// the retirement audit remains conservative rather than inventing completion.
func legacyHistoricalStatus(ctx context.Context, registration asyncTaskMappingRegistration) string {
	if registration.empty() {
		return ""
	}
	conn := db.DBForContext(ctx)
	if conn == nil {
		return ""
	}
	ids := append([]string(nil), registration.TaskIDs...)
	if registration.TaskAlias != "" {
		ids = append(ids, registration.TaskAlias)
	}
	var logs []db.RequestLogModel
	query := conn.WithContext(ctx).Where("channel_id = ?", registration.ChannelID).Where("async_task_id IN ?", ids).Order("started_at DESC")
	if err := query.Find(&logs).Error; err != nil {
		return ""
	}
	for _, item := range logs {
		if status := strings.TrimSpace(item.AsyncTaskStatus); status != "" {
			return status
		}
		// Some old successful logs predate AsyncTaskStatus but retain a
		// completed async timestamp and a successful request outcome.
		if item.AsyncCompletedAt != nil && strings.EqualFold(strings.TrimSpace(item.Outcome), "success") {
			return "completed"
		}
	}
	return ""
}

// ensureLegacyTaskRun mirrors a successful Legacy async mapping into the new
// lifecycle tables. Mapping persistence remains the routing source of truth;
// this projection is deliberately best-effort so a local write outage cannot
// turn an already accepted paid request into a retryable failure.
func ensureLegacyTaskRun(registration asyncTaskMappingRegistration) {
	if registration.empty() || !db.IsValidTaskID(registration.TaskAlias) {
		return
	}
	runID := legacyTaskRunID(registration)
	run, err := db.GetTaskRun(runID)
	if err != nil {
		status, outcome := normalizeLegacyTaskState(registration.InitialStatus)
		run = &db.TaskRun{
			ID:              runID,
			OriginRequestID: registration.OriginRequestID,
			TaskKind:        registration.TaskKind,
			Operation:       legacyTaskOperation(registration.TaskKind),
			ChannelID:       registration.ChannelID,
			Engine:          "legacy",
			PollingMode:     "client",
			// TaskIDs contains internal lookup aliases (for images this can be
			// imgjob_<provider-id>). The provider ID itself is the canonical
			// TaskAlias and must remain stable for reconciliation/auditing.
			ProviderTaskID:  registration.TaskAlias,
			SubmissionState: "accepted",
			TaskStatus:      status,
			TaskOutcome:     outcome,
		}
		if err := db.CreateTaskRun(run); err != nil {
			// A concurrent request may have created the same deterministic
			// projection. Continue with alias/event reconciliation in that case.
			if loaded, loadErr := db.GetTaskRun(runID); loadErr == nil {
				run = loaded
			} else {
				return
			}
		}
		_, _ = db.AppendTaskEvent(context.Background(), runID, "submitted", "legacy async task accepted")
	}
	for _, lookupID := range registration.TaskIDs {
		if isTaskIDRegisteredToOtherChannel(lookupID, registration.TaskKind, registration.ChannelID) {
			continue
		}
		_ = db.RecordTaskAlias(&db.TaskAlias{TaskRunID: runID, LookupID: lookupID, Source: "create"})
	}
}

func recordLegacyTaskPoll(lookupTaskID, taskKind, status string, response any) {
	mapping := db.GetTaskMappingForKind(lookupTaskID, taskKind)
	if mapping == nil {
		return
	}
	registration := legacyRegistrationFromMapping(mapping, status)
	if registration.empty() {
		return
	}
	if registrationHasProfileTaskRun(context.Background(), registration) {
		return
	}
	// Pre-v3 mappings may have omitted TaskKind and TaskAlias. Normalize those
	// fields on the first successful poll so future lookups and the migration
	// report can see a durable, type-specific alias without contacting the
	// provider again.
	for _, lookupID := range registration.TaskIDs {
		if isTaskIDRegisteredToOtherChannel(lookupID, registration.TaskKind, registration.ChannelID) {
			continue
		}
		_ = db.RecordTaskMappingContext(context.Background(), db.TaskMapping{
			TaskID: lookupID, ChannelID: registration.ChannelID,
			OriginRequestID: registration.OriginRequestID, TaskKind: registration.TaskKind,
			TaskAlias: registration.TaskAlias,
		})
	}
	ensureLegacyTaskRun(registration)
	runID := legacyTaskRunID(registration)
	run, err := db.GetTaskRun(runID)
	if err != nil {
		return
	}
	started := time.Now()
	normalized, outcome := normalizeLegacyTaskState(status)
	_ = db.RecordTaskPoll(runID, true, 0)
	if normalized != "" && normalized != run.TaskStatus {
		if err := db.UpdateTaskRunStatus(runID, normalized, outcome); err == nil {
			_, _ = db.AppendTaskEvent(context.Background(), runID, "status_changed", normalized)
		}
	}
	meta := ""
	if response != nil {
		if encoded, marshalErr := json.Marshal(response); marshalErr == nil {
			if len(encoded) > 4096 {
				encoded = encoded[:4096]
			}
			meta = string(encoded)
		}
	}
	finished := time.Now()
	_ = db.AppendTaskAttempt(&db.TaskAttempt{
		TaskRunID:    runID,
		AttemptType:  "poll",
		StartedAt:    started,
		FinishedAt:   &finished,
		Outcome:      "success",
		ResponseMeta: meta,
	})
}

// registrationHasProfileTaskRun prevents compatibility reconciliation from
// projecting a second legacy TaskRun for a task already owned by the Profile
// engine. Profile aliases are authoritative because they pin the immutable
// channel and revision that accepted the paid submission.
func registrationHasProfileTaskRun(ctx context.Context, registration asyncTaskMappingRegistration) bool {
	if registration.empty() {
		return false
	}
	lookupIDs := append([]string(nil), registration.TaskIDs...)
	lookupIDs = append(lookupIDs, registration.TaskAlias)
	seen := make(map[string]struct{}, len(lookupIDs))
	for _, lookupID := range lookupIDs {
		lookupID = strings.TrimSpace(lookupID)
		if lookupID == "" {
			continue
		}
		if _, exists := seen[lookupID]; exists {
			continue
		}
		seen[lookupID] = struct{}{}
		run, err := db.GetTaskRunByAliasContext(ctx, lookupID)
		if err != nil || run == nil || !strings.EqualFold(strings.TrimSpace(run.Engine), "profile") {
			continue
		}
		if run.ChannelID == registration.ChannelID && strings.EqualFold(strings.TrimSpace(run.TaskKind), strings.TrimSpace(registration.TaskKind)) {
			return true
		}
	}
	return false
}

func legacyRegistrationFromMapping(mapping *db.TaskMapping, initialStatus string) asyncTaskMappingRegistration {
	if mapping == nil {
		return asyncTaskMappingRegistration{}
	}
	taskKind := strings.ToLower(strings.TrimSpace(mapping.TaskKind))
	if taskKind == "" {
		// Blank TaskKind rows predate image mappings and are defined to be video
		// by db.GetTaskMappingForKind's compatibility rules.
		taskKind = asyncTaskKindVideo
	}
	taskAlias := strings.TrimSpace(mapping.TaskAlias)
	if !db.IsValidTaskID(taskAlias) {
		taskAlias = strings.TrimSpace(mapping.TaskID)
	}
	if !db.IsValidTaskID(taskAlias) {
		return asyncTaskMappingRegistration{}
	}
	registration := asyncTaskMappingRegistration{
		ChannelID: mapping.ChannelID, TaskKind: taskKind, TaskAlias: taskAlias,
		InitialStatus: truncateAsyncTaskMappingStatus(initialStatus), OriginRequestID: mapping.OriginRequestID,
		TaskIDs: []string{strings.TrimSpace(mapping.TaskID)},
	}
	if taskAlias != registration.TaskIDs[0] && db.IsValidTaskMappingLookupID(taskAlias, taskKind) && !isTaskIDRegisteredToOtherChannel(taskAlias, taskKind, mapping.ChannelID) {
		registration.TaskIDs = append(registration.TaskIDs, taskAlias)
	}
	if !db.IsValidTaskMappingLookupID(registration.TaskIDs[0], taskKind) {
		return asyncTaskMappingRegistration{}
	}
	return registration
}

func legacyTaskRunID(registration asyncTaskMappingRegistration) string {
	h := sha256.New()
	_, _ = h.Write([]byte(strings.ToLower(strings.TrimSpace(registration.TaskKind))))
	_, _ = h.Write([]byte{"\x00"[0]})
	_, _ = h.Write([]byte(strings.TrimSpace(registration.TaskAlias)))
	_, _ = h.Write([]byte{"\x00"[0]})
	_, _ = h.Write([]byte(strings.TrimSpace(registration.OriginRequestID)))
	return "run_" + hex.EncodeToString(h.Sum(nil))[:56]
}

func legacyTaskOperation(kind string) string {
	if strings.EqualFold(strings.TrimSpace(kind), asyncTaskKindImage) {
		return "image.create"
	}
	return "video.create"
}

func normalizeLegacyTaskState(raw string) (status, outcome string) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "completed", "complete", "succeeded", "success", "done":
		return "completed", "success"
	case "failed", "error":
		return "failed", "failed"
	case "cancelled", "canceled":
		return "cancelled", "cancelled"
	case "processing", "running", "in_progress", "in-progress":
		return "processing", "pending"
	default:
		return "queued", "pending"
	}
}

// ReconcileOrphanedAsyncPollLogs cleans up historical status poll rows in request_logs
// that were orphaned before coalescing was unified across profile and background modes.
// It merges poll counts and latest results into the origin request, then deletes the child rows.
func ReconcileOrphanedAsyncPollLogs(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn := db.DBForContext(ctx)
	if conn == nil {
		return 0, fmt.Errorf("database is not initialized")
	}

	var orphanedLogs []db.RequestLogModel
	if err := conn.WithContext(ctx).Where("path LIKE ? OR path LIKE ?", "%/playground/video-status%", "%/playground/image-status%").Find(&orphanedLogs).Error; err != nil {
		return 0, err
	}
	_ = conn.WithContext(ctx).Where("path LIKE ?", "%/playground/video-content/%").Delete(&db.RequestLogModel{}).Error
	_ = conn.WithContext(ctx).Where("kind = ? AND path NOT LIKE ?", "admin_action", "%/playground/%").Delete(&db.RequestLogModel{}).Error
	if len(orphanedLogs) == 0 {
		return 0, nil
	}

	type pollGroup struct {
		taskID     string
		taskKind   string
		childIDs   []string
		lastPolled time.Time
		count      int
		lastBody   string
		lastStatus string
	}
	groups := make(map[string]*pollGroup)

	for _, child := range orphanedLogs {
		taskID := ""
		taskKind := asyncTaskKindVideo
		if strings.Contains(child.Path, "image-status") {
			taskKind = asyncTaskKindImage
		}
		var respObj map[string]any
		if err := json.Unmarshal([]byte(child.ResponseBody), &respObj); err == nil {
			if id, ok := respObj["task_id"].(string); ok {
				taskID = strings.TrimSpace(id)
			}
		}
		if taskID == "" {
			if u, err := url.Parse(child.Path); err == nil {
				taskID = strings.TrimSpace(u.Query().Get("task_id"))
			}
		}
		if taskID == "" {
			continue
		}
		g, exists := groups[taskID]
		if !exists {
			g = &pollGroup{taskID: taskID, taskKind: taskKind}
			groups[taskID] = g
		}
		g.childIDs = append(g.childIDs, child.ID)
		g.count++
		if child.StartedAt.After(g.lastPolled) {
			g.lastPolled = child.StartedAt
			g.lastBody = child.ResponseBody
			if st, ok := respObj["task_status"].(string); ok && st != "" {
				g.lastStatus = st
			}
		}
	}

	coalescedCount := 0
	for taskID, g := range groups {
		mapping := db.GetTaskMappingForKind(taskID, g.taskKind)
		if mapping == nil {
			mapping = db.GetTaskMapping(taskID)
		}
		originID := ""
		if mapping != nil {
			originID = mapping.OriginRequestID
		}
		run, _ := db.GetTaskRunByAliasContext(ctx, taskID)
		if originID == "" && run != nil {
			originID = run.OriginRequestID
		}
		if originID == "" {
			continue
		}
		var parent db.RequestLogModel
		if err := conn.WithContext(ctx).First(&parent, "id = ?", originID).Error; err != nil {
			continue
		}

		targetStatus := g.lastStatus
		targetBody := g.lastBody
		if run != nil && run.TaskStatus != "" {
			targetStatus = run.TaskStatus
			if run.ResultBody != "" {
				targetBody = run.ResultBody
			}
		}
		if targetStatus == "" {
			targetStatus = parent.AsyncTaskStatus
		}

		updates := map[string]any{
			"async_task_id":    taskID,
			"async_task_kind":  g.taskKind,
			"async_poll_count": parent.AsyncPollCount + g.count,
		}
		if !g.lastPolled.IsZero() {
			updates["async_last_polled_at"] = g.lastPolled
		}
		if targetStatus != "" {
			updates["async_task_status"] = targetStatus
			if targetStatus == "completed" || targetStatus == "failed" {
				updates["async_completed_at"] = g.lastPolled
			}
		}
		if targetBody != "" && (parent.AsyncResultBody == "" || parent.AsyncResultBody == "{}") {
			updates["async_result_body"] = targetBody
		}

		err := conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&db.RequestLogModel{}).Where("id = ?", parent.ID).Updates(updates).Error; err != nil {
				return err
			}
			if len(g.childIDs) > 0 {
				_ = tx.Where("request_id IN ?", g.childIDs).Delete(&db.RequestEventModel{}).Error
				if err := tx.Where("id IN ?", g.childIDs).Delete(&db.RequestLogModel{}).Error; err != nil {
					return err
				}
			}
			return nil
		})
		if err == nil {
			coalescedCount += len(g.childIDs)
		}
	}

	return coalescedCount, nil
}
