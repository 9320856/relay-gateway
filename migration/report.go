// Package migration contains read-only checks used to decide whether a
// channel can leave the Legacy Adapter. It deliberately has no routing or
// mutation side effects: the report is evidence for an operator's rollout
// decision, not an automatic cutover.
package migration

import (
	"errors"
	"strings"

	"gorm.io/gorm"
	"relay-gateway/db"
)

var terminalStatuses = []string{"completed", "succeeded", "failed", "cancelled", "canceled", "expired", "timeout", "timed_out"}

func count(query *gorm.DB, destination *int64) error {
	return query.Count(destination).Error
}

func countAliasConflicts(database *gorm.DB, destination *int64) error {
	if database == nil || destination == nil {
		return errors.New("database and destination are required")
	}
	rows, err := database.Model(&db.TaskAlias{}).
		Select("lookup_id").Group("lookup_id").
		Having("COUNT(DISTINCT task_run_id) > 1").Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	*destination = count
	return nil
}

// countIdempotencyConflicts finds durable Profile creates that share one
// caller key and operation but were projected as multiple TaskRuns. This is a
// local consistency check; provider-side traffic and retry metrics still need
// production evidence.
func countIdempotencyConflicts(database *gorm.DB, channelType string, destination *int64) error {
	if database == nil || destination == nil {
		return errors.New("database and destination are required")
	}
	query := database.Model(&db.TaskRun{}).
		Where("async_task_runs.engine = ? AND TRIM(COALESCE(async_task_runs.idempotency_key, '')) <> ''", "profile")
	if strings.TrimSpace(channelType) != "" {
		query = query.Joins("JOIN channel_models ON channel_models.id = async_task_runs.channel_id").Where("channel_models.type = ?", channelType)
	}
	rows, err := query.Select("async_task_runs.idempotency_key, async_task_runs.operation").
		Group("async_task_runs.idempotency_key, async_task_runs.operation").Having("COUNT(*) > 1").Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	*destination = count
	return nil
}

// countProfileTaskCreateLogMismatches verifies that a Profile TaskRun's
// origin log is the creation log for the same provider task. A missing origin
// is reported by ProfileTasksMissingPrimaryLog; this gate catches an origin
// that exists but points at a different task or request kind.
func countProfileTaskCreateLogMismatches(database *gorm.DB, channelType string, destination *int64) error {
	if database == nil || destination == nil {
		return errors.New("database and destination are required")
	}
	query := profileTaskRunsForChannel(database, channelType).
		Where("TRIM(COALESCE(async_task_runs.origin_request_id, '')) <> ''").
		Where("NOT EXISTS (SELECT 1 FROM request_log_models l WHERE l.id = async_task_runs.origin_request_id AND TRIM(COALESCE(l.channel_id, '')) = TRIM(COALESCE(async_task_runs.channel_id, '')) AND TRIM(COALESCE(l.async_task_id, '')) = TRIM(COALESCE(async_task_runs.provider_task_id, '')) AND LOWER(TRIM(COALESCE(l.async_task_kind, ''))) = LOWER(TRIM(COALESCE(async_task_runs.task_kind, ''))))")
	return count(query, destination)
}

func countProfileCreateRequests(database *gorm.DB, channelType string, destination *int64) error {
	if database == nil || destination == nil {
		return errors.New("database and destination are required")
	}
	return count(database.Model(&db.RequestLogModel{}).
		Where("channel_type = ? AND TRIM(COALESCE(async_task_id, '')) <> ''", channelType), destination)
}

// A create log is reconciled by its origin ID or provider task ID. Either
// relationship is accepted here so reports remain useful for older rows that
// predate the durable OriginRequestID projection.
func countProfileCreateRequestsWithoutTaskRun(database *gorm.DB, channelType string, destination *int64) error {
	if database == nil || destination == nil {
		return errors.New("database and destination are required")
	}
	query := database.Model(&db.RequestLogModel{}).
		Where("request_log_models.channel_type = ? AND TRIM(COALESCE(request_log_models.channel_id, '')) <> '' AND TRIM(COALESCE(request_log_models.async_task_id, '')) <> ?", channelType, "").
		Where("NOT EXISTS (SELECT 1 FROM async_task_runs r JOIN channel_models c ON c.id = r.channel_id WHERE c.type = ? AND r.engine = ? AND ((r.origin_request_id = request_log_models.id AND r.channel_id = request_log_models.channel_id) OR (TRIM(COALESCE(r.provider_task_id, '')) <> '' AND r.provider_task_id = request_log_models.async_task_id AND r.channel_id = request_log_models.channel_id AND (TRIM(COALESCE(request_log_models.async_task_kind, '')) = '' OR LOWER(TRIM(COALESCE(r.task_kind, ''))) = LOWER(TRIM(COALESCE(request_log_models.async_task_kind, '')))))))", channelType, "profile")
	return count(query, destination)
}

// More than one creation log for one channel/provider task violates the
// one-task/one-Primary-log contract, even though RequestLogModel has no
// explicit is_primary column.
func countDuplicateProfileCreateLogs(database *gorm.DB, channelType string, destination *int64) error {
	if database == nil || destination == nil {
		return errors.New("database and destination are required")
	}
	rows, err := database.Model(&db.RequestLogModel{}).
		Where("channel_type = ? AND TRIM(COALESCE(async_task_id, '')) <> ?", channelType, "").
		Select("channel_id, channel_type, async_task_kind, async_task_id").
		Group("channel_id, channel_type, async_task_kind, async_task_id").Having("COUNT(*) > 1").Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	*destination = n
	return nil
}
