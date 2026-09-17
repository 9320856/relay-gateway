package migration

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/protocol"
)

// ChannelMigrationSummary contains the durable checks needed before one
// channel type can leave the compatibility Adapter path. It intentionally
// does not probe providers or infer production traffic from configuration.
type ChannelMigrationSummary struct {
	Type                                string `json:"type"`
	EnabledChannels                     int64  `json:"enabled_channels"`
	EnabledChannelsWithBinding          int64  `json:"enabled_channels_with_binding"`
	EnabledChannelsWithoutBinding       int64  `json:"enabled_channels_without_binding"`
	EnabledBindings                     int64  `json:"enabled_bindings"`
	BindingsMissingRevision             int64  `json:"bindings_missing_published_revision"`
	LegacyTaskRuns                      int64  `json:"legacy_task_runs"`
	NonTerminalLegacyTaskRuns           int64  `json:"non_terminal_legacy_task_runs"`
	ProfileTaskRuns                     int64  `json:"profile_task_runs"`
	NonTerminalProfileTaskRuns          int64  `json:"non_terminal_profile_task_runs"`
	ProfileTasksMissingRevision         int64  `json:"profile_tasks_missing_revision"`
	ProfileTasksMissingDigest           int64  `json:"profile_tasks_missing_digest"`
	ProfileTasksMissingProviderID       int64  `json:"profile_tasks_missing_provider_task_id"`
	ProfileTasksMissingPrimaryLog       int64  `json:"profile_tasks_missing_primary_log"`
	ProfileTasksCreateLogMismatches     int64  `json:"profile_tasks_create_log_mismatches"`
	ProfileCreateRequests               int64  `json:"profile_create_requests"`
	ProfileCreateRequestsWithoutTaskRun int64  `json:"profile_create_requests_without_task_run"`
	ProfilePrimaryLogConflicts          int64  `json:"profile_primary_log_conflicts"`
	ProviderTaskIDConflicts             int64  `json:"provider_task_id_conflicts"`
	OperationsMissingBinding            int64  `json:"operations_missing_profile_binding"`
	IdempotencyConflicts                int64  `json:"idempotency_conflicts"`
	LegacyMappingsWithoutTaskRun        int64  `json:"legacy_mappings_without_task_run"`
	MediaPending                        int64  `json:"media_pending"`
	MediaFailed                         int64  `json:"media_failed"`
}

// ChannelsReport is a read-only report. A report is evidence for an
// operator's rollout decision; it never changes
// bindings, routes, tasks, or database rows.
type ChannelsReport struct {
	GeneratedAt                         time.Time                 `json:"generated_at"`
	LegacyDisabled                      bool                      `json:"legacy_disabled"`
	EnabledChannels                     int64                     `json:"enabled_channels"`
	EnabledChannelsWithBinding          int64                     `json:"enabled_channels_with_binding"`
	EnabledChannelsWithoutBinding       int64                     `json:"enabled_channels_without_binding"`
	LegacyTaskRuns                      int64                     `json:"legacy_task_runs"`
	NonTerminalLegacyTaskRuns           int64                     `json:"non_terminal_legacy_task_runs"`
	ProfileTaskRuns                     int64                     `json:"profile_task_runs"`
	NonTerminalProfileTaskRuns          int64                     `json:"non_terminal_profile_task_runs"`
	BindingsMissingRevision             int64                     `json:"bindings_missing_published_revision"`
	ProfileTasksMissingRevision         int64                     `json:"profile_tasks_missing_revision"`
	ProfileTasksMissingDigest           int64                     `json:"profile_tasks_missing_digest"`
	ProfileTasksMissingProviderID       int64                     `json:"profile_tasks_missing_provider_task_id"`
	ProfileTasksMissingPrimaryLog       int64                     `json:"profile_tasks_missing_primary_log"`
	ProfileTasksCreateLogMismatches     int64                     `json:"profile_tasks_create_log_mismatches"`
	ProfileCreateRequests               int64                     `json:"profile_create_requests"`
	ProfileCreateRequestsWithoutTaskRun int64                     `json:"profile_create_requests_without_task_run"`
	ProfilePrimaryLogConflicts          int64                     `json:"profile_primary_log_conflicts"`
	ProviderTaskIDConflicts             int64                     `json:"provider_task_id_conflicts"`
	OperationsMissingBinding            int64                     `json:"operations_missing_profile_binding"`
	IdempotencyConflicts                int64                     `json:"idempotency_conflicts"`
	LegacyMappingsWithoutTaskRun        int64                     `json:"legacy_mappings_without_task_run"`
	AliasConflicts                      int64                     `json:"alias_conflicts"`
	MediaPending                        int64                     `json:"media_pending"`
	MediaFailed                         int64                     `json:"media_failed"`
	Channels                            []ChannelMigrationSummary `json:"channels"`
	Ready                               bool                      `json:"ready"`
	Blockers                            []string                  `json:"blockers,omitempty"`
}

// AuditChannels computes the local retirement gate for every configured
// channel type. It deliberately treats an empty deployment as ready while
// retaining a default legacy-disabled requirement once traffic exists.
func AuditChannels(ctx context.Context) (*ChannelsReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	database := db.DBForContext(ctx)
	if database == nil {
		return nil, errors.New("database is not initialized")
	}
	database = database.WithContext(ctx)
	report := &ChannelsReport{
		GeneratedAt:    time.Now().UTC(),
		LegacyDisabled: os.Getenv("RELAY_DISABLE_LEGACY") == "1",
		Channels:       make([]ChannelMigrationSummary, 0),
		Blockers:       make([]string, 0),
	}
	var types []string
	if err := database.Model(&db.ChannelModel{}).Distinct("type").Order("type ASC").Pluck("type", &types).Error; err != nil {
		return nil, err
	}
	for _, channelType := range types {
		channelType = strings.TrimSpace(channelType)
		if channelType == "" {
			continue
		}
		item, err := auditChannelType(database, channelType)
		if err != nil {
			return nil, err
		}
		report.Channels = append(report.Channels, item)
		report.EnabledChannels += item.EnabledChannels
		report.EnabledChannelsWithBinding += item.EnabledChannelsWithBinding
		report.EnabledChannelsWithoutBinding += item.EnabledChannelsWithoutBinding
		report.LegacyTaskRuns += item.LegacyTaskRuns
		report.NonTerminalLegacyTaskRuns += item.NonTerminalLegacyTaskRuns
		report.ProfileTaskRuns += item.ProfileTaskRuns
		report.NonTerminalProfileTaskRuns += item.NonTerminalProfileTaskRuns
		report.BindingsMissingRevision += item.BindingsMissingRevision
		report.ProfileTasksMissingRevision += item.ProfileTasksMissingRevision
		report.ProfileTasksMissingDigest += item.ProfileTasksMissingDigest
		report.ProfileTasksMissingProviderID += item.ProfileTasksMissingProviderID
		report.ProfileTasksMissingPrimaryLog += item.ProfileTasksMissingPrimaryLog
		report.ProfileTasksCreateLogMismatches += item.ProfileTasksCreateLogMismatches
		report.ProfileCreateRequests += item.ProfileCreateRequests
		report.ProfileCreateRequestsWithoutTaskRun += item.ProfileCreateRequestsWithoutTaskRun
		report.ProfilePrimaryLogConflicts += item.ProfilePrimaryLogConflicts
		report.ProviderTaskIDConflicts += item.ProviderTaskIDConflicts
		report.OperationsMissingBinding += item.OperationsMissingBinding
		report.IdempotencyConflicts += item.IdempotencyConflicts
		report.LegacyMappingsWithoutTaskRun += item.LegacyMappingsWithoutTaskRun
		report.MediaPending += item.MediaPending
		report.MediaFailed += item.MediaFailed
	}
	if err := countAliasConflicts(database, &report.AliasConflicts); err != nil {
		return nil, err
	}
	addChannelReportBlockers(report)
	return report, nil
}

func auditChannelType(database *gorm.DB, channelType string) (ChannelMigrationSummary, error) {
	item := ChannelMigrationSummary{Type: channelType}
	base := database.Model(&db.ChannelModel{}).Where("channel_models.type = ?", channelType)
	if err := count(base.Where("channel_models.enabled = ?", true), &item.EnabledChannels); err != nil {
		return item, err
	}
	withBinding := base.Joins("JOIN channel_protocol_bindings b ON b.channel_id = channel_models.id AND b.enabled = ?", true).
		Where("channel_models.enabled = ?", true).Distinct("channel_models.id")
	if err := count(withBinding, &item.EnabledChannelsWithBinding); err != nil {
		return item, err
	}
	item.EnabledChannelsWithoutBinding = item.EnabledChannels - item.EnabledChannelsWithBinding
	if err := count(database.Model(&db.ChannelProtocolBinding{}).
		Joins("JOIN channel_models ON channel_models.id = channel_protocol_bindings.channel_id").
		Where("channel_models.type = ? AND channel_models.enabled = ? AND channel_protocol_bindings.enabled = ?", channelType, true, true), &item.EnabledBindings); err != nil {
		return item, err
	}
	if err := count(database.Model(&db.ChannelProtocolBinding{}).
		Joins("JOIN channel_models ON channel_models.id = channel_protocol_bindings.channel_id").
		Joins("LEFT JOIN protocol_profile_revisions r ON r.profile_id = channel_protocol_bindings.profile_id AND r.revision = channel_protocol_bindings.profile_revision").
		Where("channel_models.type = ? AND channel_models.enabled = ? AND channel_protocol_bindings.enabled = ?", channelType, true, true).
		Where("r.id IS NULL OR r.state <> ?", db.ProfileRevisionPublished), &item.BindingsMissingRevision); err != nil {
		return item, err
	}
	legacy := database.Model(&db.TaskRun{}).
		Joins("JOIN channel_models ON channel_models.id = async_task_runs.channel_id").
		Where("channel_models.type = ? AND async_task_runs.engine = ?", channelType, "legacy")
	if err := count(legacy, &item.LegacyTaskRuns); err != nil {
		return item, err
	}
	if err := count(legacy.Where("async_task_runs.task_status NOT IN ?", terminalStatuses), &item.NonTerminalLegacyTaskRuns); err != nil {
		return item, err
	}
	profile := profileTaskRunsForChannel(database, channelType)
	if err := count(profile, &item.ProfileTaskRuns); err != nil {
		return item, err
	}
	if err := count(profileTaskRunsForChannel(database, channelType).Where("async_task_runs.task_status NOT IN ?", terminalStatuses), &item.NonTerminalProfileTaskRuns); err != nil {
		return item, err
	}
	if err := count(profileTaskRunsForChannel(database, channelType).Where("TRIM(COALESCE(async_task_runs.profile_id, '')) = '' OR async_task_runs.profile_revision <= 0 OR NOT EXISTS (SELECT 1 FROM protocol_profile_revisions r WHERE r.profile_id = async_task_runs.profile_id AND r.revision = async_task_runs.profile_revision AND r.state = ?)", db.ProfileRevisionPublished), &item.ProfileTasksMissingRevision); err != nil {
		return item, err
	}
	if err := count(profileTaskRunsForChannel(database, channelType).Where("TRIM(COALESCE(async_task_runs.profile_digest, '')) = ''"), &item.ProfileTasksMissingDigest); err != nil {
		return item, err
	}
	if err := count(profileTaskRunsForChannel(database, channelType).Where("TRIM(COALESCE(async_task_runs.provider_task_id, '')) = ''"), &item.ProfileTasksMissingProviderID); err != nil {
		return item, err
	}
	if err := count(profileTaskRunsForChannel(database, channelType).
		Where("TRIM(COALESCE(async_task_runs.origin_request_id, '')) = '' OR NOT EXISTS (SELECT 1 FROM request_log_models l WHERE l.id = async_task_runs.origin_request_id)"), &item.ProfileTasksMissingPrimaryLog); err != nil {
		return item, err
	}
	if err := countProfileTaskCreateLogMismatches(database, channelType, &item.ProfileTasksCreateLogMismatches); err != nil {
		return item, err
	}
	if err := countProfileCreateRequests(database, channelType, &item.ProfileCreateRequests); err != nil {
		return item, err
	}
	if err := countProfileCreateRequestsWithoutTaskRun(database, channelType, &item.ProfileCreateRequestsWithoutTaskRun); err != nil {
		return item, err
	}
	if err := countDuplicateProfileCreateLogs(database, channelType, &item.ProfilePrimaryLogConflicts); err != nil {
		return item, err
	}
	if err := countProviderTaskIDConflicts(database, channelType, &item.ProviderTaskIDConflicts); err != nil {
		return item, err
	}
	if err := countMissingProfileOperations(database, channelType, &item.OperationsMissingBinding); err != nil {
		return item, err
	}
	legacyMappings := database.Model(&db.TaskMapping{}).
		Joins("JOIN channel_models ON channel_models.id = video_task_mappings.channel_id").
		Where("channel_models.type = ?", channelType).
		Where("NOT EXISTS (SELECT 1 FROM async_task_aliases a WHERE a.lookup_id = video_task_mappings.task_id OR a.lookup_id = video_task_mappings.task_alias)")
	if err := count(legacyMappings, &item.LegacyMappingsWithoutTaskRun); err != nil {
		return item, err
	}
	mediaBase := database.Model(&db.MediaAsset{}).
		Joins("JOIN async_task_runs ON async_task_runs.id = media_assets.task_run_id").
		Joins("JOIN channel_models ON channel_models.id = async_task_runs.channel_id").
		Where("channel_models.type = ?", channelType)
	if err := count(mediaBase.Where("media_assets.status IN ?", []string{db.MediaAssetPending, db.MediaAssetMaterializing}), &item.MediaPending); err != nil {
		return item, err
	}
	if err := count(mediaBase.Where("media_assets.status IN ?", []string{db.MediaAssetFailed, db.MediaAssetDeleteFailed}), &item.MediaFailed); err != nil {
		return item, err
	}
	if err := countIdempotencyConflicts(database, channelType, &item.IdempotencyConflicts); err != nil {
		return item, err
	}
	return item, nil
}

// countMissingProfileOperations checks operation coverage per enabled channel,
// rather than treating one enabled binding as proof that the whole channel is
// migrated. A wildcard or model-specific binding both count as coverage for
// the operation; model-level completeness remains an operator responsibility
// because the database cannot infer the provider's full model inventory.
func countMissingProfileOperations(database *gorm.DB, channelType string, target *int64) error {
	if database == nil || target == nil {
		return errors.New("database and target are required")
	}
	preset, err := protocol.BuiltinPreset(channelType)
	if err != nil {
		// Unknown channel types are rejected by the management API. Keep the
		// report forward-compatible with older databases instead of failing the
		// entire read-only audit when one stale type is present.
		return nil
	}
	for _, operation := range preset.Operations {
		var missing int64
		query := database.Model(&db.ChannelModel{}).
			Where("channel_models.type = ? AND channel_models.enabled = ?", channelType, true).
			Where("NOT EXISTS (SELECT 1 FROM channel_protocol_bindings b WHERE b.channel_id = channel_models.id AND b.operation = ? AND b.enabled = ?)", operation.Operation, true)
		if err := count(query, &missing); err != nil {
			return err
		}
		*target += missing
	}
	return nil
}

// profileTaskRunsForChannel returns a fresh query for every gate count. GORM
// chains are mutable; reusing one after Where can leak a previous condition
// into later counters and hide independent migration defects.
func profileTaskRunsForChannel(database *gorm.DB, channelType string) *gorm.DB {
	return database.Model(&db.TaskRun{}).
		Joins("JOIN channel_models ON channel_models.id = async_task_runs.channel_id").
		Where("channel_models.type = ? AND async_task_runs.engine = ?", channelType, "profile")
}

// countProviderTaskIDConflicts identifies duplicate provider IDs within the
// same channel and task kind. IDs from different channels are intentionally
// allowed because providers commonly scope task IDs to their credential set.
func countProviderTaskIDConflicts(database *gorm.DB, channelType string, destination *int64) error {
	if database == nil || destination == nil {
		return errors.New("database and destination are required")
	}
	query := database.Model(&db.TaskRun{}).
		Joins("JOIN channel_models ON channel_models.id = async_task_runs.channel_id").
		Where("channel_models.type = ? AND async_task_runs.engine = ? AND TRIM(COALESCE(async_task_runs.provider_task_id, '')) <> ?", channelType, "profile", "")
	rows, err := query.Select("async_task_runs.channel_id, async_task_runs.task_kind, async_task_runs.provider_task_id").
		Group("async_task_runs.channel_id, async_task_runs.task_kind, async_task_runs.provider_task_id").
		Having("COUNT(*) > 1").Rows()
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

func addChannelReportBlockers(report *ChannelsReport) {
	if report == nil {
		return
	}
	if report.EnabledChannels > 0 && !report.LegacyDisabled {
		report.Blockers = append(report.Blockers, "generic Legacy adapters are still enabled")
	}
	if report.EnabledChannelsWithoutBinding > 0 {
		report.Blockers = append(report.Blockers, "enabled channels without an enabled Profile binding")
	}
	if report.NonTerminalLegacyTaskRuns > 0 {
		report.Blockers = append(report.Blockers, "non-terminal tasks still owned by Legacy")
	}
	if report.BindingsMissingRevision > 0 {
		report.Blockers = append(report.Blockers, "enabled Profile bindings do not point to a published revision")
	}
	if report.LegacyMappingsWithoutTaskRun > 0 {
		report.Blockers = append(report.Blockers, "historical mappings have no durable TaskRun projection")
	}
	if report.ProfileTasksMissingRevision > 0 {
		report.Blockers = append(report.Blockers, "Profile tasks are missing an immutable revision")
	}
	if report.ProfileTasksMissingDigest > 0 {
		report.Blockers = append(report.Blockers, "Profile tasks are missing an immutable digest snapshot")
	}
	if report.ProfileTasksMissingProviderID > 0 {
		report.Blockers = append(report.Blockers, "Profile tasks are missing a provider task ID")
	}
	if report.ProfileTasksMissingPrimaryLog > 0 {
		report.Blockers = append(report.Blockers, "Profile tasks are missing their Primary request log")
	}
	if report.ProfileTasksCreateLogMismatches > 0 {
		report.Blockers = append(report.Blockers, "Profile TaskRuns do not match their creation request log")
	}
	if report.ProfileCreateRequestsWithoutTaskRun > 0 {
		report.Blockers = append(report.Blockers, "Profile creation requests have no durable TaskRun")
	}
	if report.ProfilePrimaryLogConflicts > 0 {
		report.Blockers = append(report.Blockers, "Profile tasks have more than one Primary request log")
	}
	if report.ProviderTaskIDConflicts > 0 {
		report.Blockers = append(report.Blockers, "Profile provider task IDs conflict within a channel and task kind")
	}
	if report.LegacyDisabled && report.OperationsMissingBinding > 0 {
		report.Blockers = append(report.Blockers, "enabled channels are missing Profile bindings for one or more operations")
	}
	if report.IdempotencyConflicts > 0 {
		report.Blockers = append(report.Blockers, "Profile idempotency keys resolve to multiple TaskRuns")
	}
	if report.AliasConflicts > 0 {
		report.Blockers = append(report.Blockers, "task aliases resolve to more than one TaskRun")
	}
	if report.MediaFailed > 0 {
		report.Blockers = append(report.Blockers, "media assets have failed materialization or deletion")
	}
	if report.MediaPending > 0 {
		report.Blockers = append(report.Blockers, "media assets are still pending materialization")
	}
	report.Ready = len(report.Blockers) == 0
}
