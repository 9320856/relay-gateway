package db

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestRecordVideoTaskConcurrency(t *testing.T) {
	testDBPath := filepath.Join(t.TempDir(), "test_concurrency.db")
	t.Cleanup(func() { _ = Close() })

	if err := InitDB(testDBPath); err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}

	const goroutines = 50
	const tasksPerGoroutine = 20
	var wg sync.WaitGroup

	// 1. 并发写入大量任务映射
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < tasksPerGoroutine; i++ {
				tid := fmt.Sprintf("task_conc_%d_%d", gid, i)
				RecordVideoTask(tid, "chan_test")
			}
		}(g)
	}

	wg.Wait()

	// 验证内存缓存能够快速命中反查
	for g := 0; g < goroutines; g++ {
		for i := 0; i < tasksPerGoroutine; i++ {
			tid := fmt.Sprintf("task_conc_%d_%d", g, i)
			ch := GetVideoTaskChannel(tid)
			if ch != "chan_test" {
				t.Errorf("expected channel 'chan_test' for task %s, got %q", tid, ch)
			}
		}
	}

}

func TestRecordTaskMappingContextPublishesCompleteCacheOnlyAfterCommit(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "task-transaction.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	tx := DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	taskID := "tx-rollback-task"
	if err := RecordTaskMappingContext(WithTx(context.Background(), tx), TaskMapping{
		TaskID:          taskID,
		ChannelID:       "channel-a",
		OriginRequestID: "req-rollback",
		TaskKind:        "video",
		TaskAlias:       "public-rollback-task",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if got := GetVideoTaskChannel(taskID); got != "" {
		t.Fatalf("rolled-back task mapping was visible as %q", got)
	}

	tx = DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	committedID := "tx-commit-task"
	mapping := TaskMapping{
		TaskID:          committedID,
		ChannelID:       "channel-b",
		OriginRequestID: "req-origin",
		TaskKind:        "image",
		TaskAlias:       "public-image-task",
	}
	if err := RecordTaskMappingContext(WithTx(context.Background(), tx), mapping); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	// A transaction-aware caller publishes its cache only after commit.
	PublishTaskMappingCache(mapping)
	got := GetTaskMapping(committedID)
	if got == nil {
		t.Fatal("committed task mapping was not found")
	}
	if got.ChannelID != "channel-b" || got.OriginRequestID != "req-origin" || got.TaskKind != "image" || got.TaskAlias != "public-image-task" {
		t.Fatalf("committed task mapping = %+v, want complete mapping", got)
	}
}

func TestEnsureTaskMappingsIsIdempotentAndNeverRepointsChannel(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "safe-task-mapping.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	original := TaskMapping{
		// This is deliberately shaped like an image lookup key. A video provider
		// can legally return the same literal ID, and the single-column primary
		// key must reject the resulting same-channel cross-kind collision.
		TaskID:          ImageTaskMappingLookupPrefix + "safe-provider-task",
		ChannelID:       "original-channel",
		OriginRequestID: "original-request",
		TaskKind:        "video",
		TaskAlias:       "safe-public-task",
	}
	if err := EnsureTaskMappings(original); err != nil {
		t.Fatal(err)
	}
	// Replaying exactly the same recovery item is a no-op, preserving the
	// original provenance instead of replacing it with a later retry request.
	if err := EnsureTaskMappings(TaskMapping{
		TaskID:          original.TaskID,
		ChannelID:       original.ChannelID,
		OriginRequestID: "later-request",
		TaskKind:        "video",
		TaskAlias:       "later-alias",
	}); err != nil {
		t.Fatalf("same-channel replay failed: %v", err)
	}
	got := GetTaskMapping(original.TaskID)
	if got == nil || got.OriginRequestID != original.OriginRequestID || got.TaskAlias != original.TaskAlias {
		t.Fatalf("same-channel replay changed original mapping: %+v", got)
	}

	err := EnsureTaskMappings(TaskMapping{
		TaskID:    original.TaskID,
		ChannelID: "different-channel",
		TaskKind:  "video",
	})
	if !errors.Is(err, ErrTaskMappingChannelConflict) {
		t.Fatalf("cross-channel replay error = %v, want ErrTaskMappingChannelConflict", err)
	}
	got = GetTaskMapping(original.TaskID)
	if got == nil || got.ChannelID != original.ChannelID {
		t.Fatalf("cross-channel replay repointed mapping: %+v", got)
	}

	err = EnsureTaskMappings(TaskMapping{
		TaskID:    original.TaskID,
		ChannelID: original.ChannelID,
		TaskKind:  "image",
	})
	if !errors.Is(err, ErrTaskMappingChannelConflict) {
		t.Fatalf("same-channel cross-kind replay error = %v, want ErrTaskMappingChannelConflict", err)
	}
	got = GetTaskMapping(original.TaskID)
	if got == nil || got.ChannelID != original.ChannelID || got.TaskKind != original.TaskKind || got.OriginRequestID != original.OriginRequestID || got.TaskAlias != original.TaskAlias {
		t.Fatalf("same-channel cross-kind replay changed original mapping: %+v", got)
	}

	if err := EnsureTaskMappings(TaskMapping{TaskID: strings.Repeat("x", 129), ChannelID: "original-channel", TaskKind: "video"}); err == nil {
		t.Fatal("oversized task ID was accepted")
	}
}

func TestGetCanonicalTaskIDForKindUsesProviderAlias(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "canonical-task.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := EnsureTaskMappings(
		TaskMapping{TaskID: "public-video", ChannelID: "video-channel", TaskKind: "video", TaskAlias: "public-video"},
		TaskMapping{TaskID: "provider-video", ChannelID: "video-channel", TaskKind: "video", TaskAlias: "public-video"},
	); err != nil {
		t.Fatal(err)
	}
	if got := GetCanonicalTaskIDForKind("public-video", "video"); got != "provider-video" {
		t.Fatalf("canonical provider ID = %q, want provider-video", got)
	}
	if got := GetCanonicalTaskIDForKind("provider-video", "video"); got != "provider-video" {
		t.Fatalf("provider ID was not stable: %q", got)
	}
}

func TestGetCanonicalTaskIDForKindUsesRetainedProviderAliasWhenNotMappedInSameChannel(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "canonical-task-conflict.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := EnsureTaskMappings(
		TaskMapping{TaskID: "public-video", ChannelID: "channel-b", TaskKind: "video", TaskAlias: "shared-provider-video"},
		TaskMapping{TaskID: "shared-provider-video", ChannelID: "channel-a", TaskKind: "video", TaskAlias: "existing-task"},
	); err != nil {
		t.Fatal(err)
	}
	if got := GetCanonicalTaskIDForKind("public-video", "video"); got != "shared-provider-video" {
		t.Fatalf("canonical provider ID = %q, want shared-provider-video", got)
	}
	if got := GetCanonicalTaskIDForKind("shared-provider-video", "video"); got != "existing-task" {
		t.Fatalf("channel-a provider ID = %q, want existing-task", got)
	}
}

func TestEnsureTaskMappingsAllowsFullLengthImageLookupKeys(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "full-length-image-keys.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	for _, rawLength := range []int{121, 122, 128} {
		rawID := strings.Repeat(string(rune('a'+rawLength%20)), rawLength)
		lookupID := ImageTaskMappingLookupPrefix + rawID
		if got, want := len(lookupID), len(ImageTaskMappingLookupPrefix)+rawLength; got != want {
			t.Fatalf("lookup key length = %d, want %d", got, want)
		}
		if err := EnsureTaskMappings(TaskMapping{
			TaskID:    lookupID,
			ChannelID: "image-channel",
			TaskKind:  "image",
			TaskAlias: rawID,
		}); err != nil {
			t.Fatalf("persist %d-byte image ID: %v", rawLength, err)
		}
		mapping := GetTaskMappingForKind(rawID, "image")
		if mapping == nil || mapping.TaskID != lookupID || mapping.ChannelID != "image-channel" || mapping.TaskAlias != rawID {
			t.Fatalf("%d-byte image lookup = %+v, want key %q", rawLength, mapping, lookupID)
		}
		if got := GetVideoTaskChannel(rawID); got != "" {
			t.Fatalf("%d-byte image ID was exposed as video channel %q", rawLength, got)
		}
	}

	// An omitted alias derives the original public image ID rather than the
	// 135-byte internal imgjob_ lookup key.
	rawID := strings.Repeat("z", taskMappingExternalIDMaxBytes)
	lookupID := ImageTaskMappingLookupPrefix + rawID
	if err := EnsureTaskMappings(TaskMapping{TaskID: lookupID, ChannelID: "image-channel", TaskKind: "image"}); err != nil {
		t.Fatal(err)
	}
	if mapping := GetTaskMappingForKind(rawID, "image"); mapping == nil || mapping.TaskAlias != rawID {
		t.Fatalf("default image alias = %+v, want raw ID length %d", mapping, len(rawID))
	}

	// The legacy direct-record API follows the same internal-key boundary and
	// derives a public-size alias when callers omit one.
	directRawID := strings.Repeat("d", taskMappingExternalIDMaxBytes)
	if err := RecordTaskMapping(TaskMapping{
		TaskID:    ImageTaskMappingLookupPrefix + directRawID,
		ChannelID: "image-channel",
		TaskKind:  "image",
	}); err != nil {
		t.Fatal(err)
	}
	if mapping := GetTaskMappingForKind(directRawID, "image"); mapping == nil || mapping.TaskAlias != directRawID {
		t.Fatalf("direct-record image alias = %+v, want raw ID length %d", mapping, len(directRawID))
	}

	tooLongRawID := strings.Repeat("q", taskMappingExternalIDMaxBytes+1)
	if err := EnsureTaskMappings(TaskMapping{
		TaskID:    ImageTaskMappingLookupPrefix + tooLongRawID,
		ChannelID: "image-channel",
		TaskKind:  "image",
		TaskAlias: "valid-alias",
	}); err == nil {
		t.Fatal("image lookup key for a 129-byte public ID was accepted")
	}
}

func TestGetTaskMappingSupportsLegacyImageLookupPrefix(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "image-task-alias.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	mapping := TaskMapping{
		TaskID:          "imgjob_image-job-123",
		ChannelID:       "image-channel",
		OriginRequestID: "req-image-origin",
		TaskKind:        "image",
		TaskAlias:       "image-job-123",
	}
	if err := RecordTaskMapping(mapping); err != nil {
		t.Fatal(err)
	}
	got := GetTaskMapping("image-job-123")
	if got == nil || got.TaskID != mapping.TaskID || got.TaskAlias != mapping.TaskAlias || got.ChannelID != mapping.ChannelID {
		t.Fatalf("legacy image lookup returned %+v, want %+v", got, mapping)
	}
}

func TestGetTaskMappingForKindIsolatesImageNamespace(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "typed-task-mapping.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	const sharedID = "shared-task-id"
	imageMapping := TaskMapping{
		TaskID:    "imgjob_" + sharedID,
		ChannelID: "image-channel",
		TaskKind:  "image",
		TaskAlias: sharedID,
	}
	if err := RecordTaskMapping(imageMapping); err != nil {
		t.Fatal(err)
	}

	// The generic helper keeps its historical image compatibility behavior, but
	// no type-specific video route may borrow that prefixed image mapping.
	if got := GetTaskMapping(sharedID); got == nil || got.ChannelID != imageMapping.ChannelID {
		t.Fatalf("generic legacy image lookup = %+v, want channel %q", got, imageMapping.ChannelID)
	}
	if got := GetTaskMappingForKind(sharedID, "video"); got != nil {
		t.Fatalf("video lookup borrowed image mapping: %+v", got)
	}
	if got := GetVideoTaskChannel(sharedID); got != "" {
		t.Fatalf("video channel = %q, want no image fallback", got)
	}
	if got := GetImageTaskChannel(sharedID); got != imageMapping.ChannelID {
		t.Fatalf("image channel = %q, want %q", got, imageMapping.ChannelID)
	}

	// Rows from before TaskKind existed were video mappings. Retain their
	// playback/routing compatibility without treating them as image jobs.
	legacyVideo := TaskMapping{TaskID: "legacy-video-task", ChannelID: "legacy-video-channel"}
	if err := RecordTaskMapping(legacyVideo); err != nil {
		t.Fatal(err)
	}
	if got := GetVideoTaskChannel(legacyVideo.TaskID); got != legacyVideo.ChannelID {
		t.Fatalf("legacy video channel = %q, want %q", got, legacyVideo.ChannelID)
	}
	if got := GetImageTaskChannel(legacyVideo.TaskID); got != "" {
		t.Fatalf("legacy video was treated as an image mapping: %q", got)
	}
}

func TestTaskMappingSurvivesCacheExpiryAndRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "durable-video-task.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	mapping := TaskMapping{
		TaskID:    "old-video-task",
		ChannelID: "original-video-channel",
		TaskKind:  "video",
		TaskAlias: "old-video-task",
		CreatedAt: time.Now().UTC().Add(-videoTaskCacheTTL - time.Hour),
	}
	if err := RecordTaskMapping(mapping); err != nil {
		t.Fatal(err)
	}

	// The mapping itself is older than the cache TTL, but a freshly written
	// cache entry must still be usable. Cache lifetime is independent from a
	// video's real creation timestamp.
	if cached := getCachedTaskMapping(mapping.TaskID); cached == nil || cached.ChannelID != mapping.ChannelID {
		t.Fatalf("fresh cache for an old mapping = %+v, want channel %q", cached, mapping.ChannelID)
	}
	if got := GetVideoTaskChannel(mapping.TaskID); got != mapping.ChannelID {
		t.Fatalf("old mapping resolved to %q, want %q", got, mapping.ChannelID)
	}

	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if got := GetVideoTaskChannel(mapping.TaskID); got != mapping.ChannelID {
		t.Fatalf("mapping after restart resolved to %q, want %q", got, mapping.ChannelID)
	}
}

func TestInitDBRejectsLegacySchemaWithoutDataLoss(t *testing.T) {
	root := t.TempDir()
	legacyDB := filepath.Join(root, "legacy.db")
	// Build a minimal pre-v2 database. A newer binary must reject it without
	// deleting data, leaving an explicit migration/restore decision to the user.
	legacy, err := gorm.Open(sqlite.Open(legacyDB), &gorm.Config{})
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	if err := legacy.Exec(`CREATE TABLE channel_models (id TEXT PRIMARY KEY, name TEXT, type TEXT, base_url TEXT, enabled BOOLEAN, priority INTEGER, weight INTEGER)`).Error; err != nil {
		t.Fatalf("create legacy channel table: %v", err)
	}
	if err := legacy.Exec(`INSERT INTO channel_models (id,name,type,base_url,enabled,priority,weight) VALUES ('legacy','old','openai','https://legacy.invalid/v1',1,1,1)`).Error; err != nil {
		t.Fatalf("insert legacy channel: %v", err)
	}
	legacySQLDB, err := legacy.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacySQLDB.Close() })

	err = InitDB(legacyDB)
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("InitDB legacy schema error = %v, want ErrIncompatibleSchema", err)
	}
	if legacy.Migrator().HasTable(&SchemaMeta{}) {
		t.Fatal("legacy database unexpectedly gained a schema marker")
	}
	var legacyRowCount int64
	if err := legacy.Raw("SELECT count(*) FROM channel_models WHERE id = ?", "legacy").Scan(&legacyRowCount).Error; err != nil {
		t.Fatal(err)
	}
	if legacyRowCount != 1 {
		t.Fatalf("legacy channel_models row count = %d, want 1", legacyRowCount)
	}
}

func TestInitDBMigratesV2SchemaToV3Idempotently(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "v2-upgrade.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if err := DB.Model(&SchemaMeta{}).Where("key = ?", "schema_version").Update("value", previousSchemaVersion).Error; err != nil {
		t.Fatal(err)
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatalf("v2 -> v3 migration failed: %v", err)
	}
	t.Cleanup(func() { _ = Close() })
	var marker SchemaMeta
	if err := DB.First(&marker, "key = ?", "schema_version").Error; err != nil {
		t.Fatal(err)
	}
	if marker.Value != SchemaVersion {
		t.Fatalf("schema marker = %q, want %q", marker.Value, SchemaVersion)
	}
	if !DB.Migrator().HasTable(&ProtocolProfile{}) || !DB.Migrator().HasTable(&ChannelProtocolBinding{}) {
		t.Fatal("profile tables were not created during migration")
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatalf("restarting migrated database failed: %v", err)
	}
}

func TestInitDBMigratesRealV2FixturePreservingData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "real-v2-fixture.db")
	legacy, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		t.Fatalf("open v2 fixture: %v", err)
	}
	legacyModels := []any{
		&SchemaMeta{}, &ChannelModel{}, &ChannelKeyModel{}, &ModelMappingModel{},
		&SettingModel{}, &AdminUserModel{}, &AdminSessionModel{}, &GatewayTokenModel{},
		&VideoTaskMapping{}, &RequestLogModel{}, &RequestEventModel{},
	}
	if err := legacy.AutoMigrate(legacyModels...); err != nil {
		t.Fatalf("create v2 fixture schema: %v", err)
	}
	createdAt := time.Now().UTC().Add(-time.Hour)
	fixtureChannel := ChannelModel{
		ID: "v2-channel", Name: "V2 Channel", Type: "openai",
		BaseURL: "https://v2.example.invalid/v1", Enabled: true,
		Priority: 3, Weight: 2, ModelsRaw: "v2-model", CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	fixtureMapping := ModelMappingModel{ChannelID: fixtureChannel.ID, SourceModel: "legacy-model", TargetModel: "v2-model", CreatedAt: createdAt, UpdatedAt: createdAt}
	fixtureTask := TaskMapping{TaskID: "v2-video-task", ChannelID: fixtureChannel.ID, TaskKind: "video", TaskAlias: "v2-video-task", CreatedAt: createdAt}
	fixtureLog := RequestLogModel{ID: "v2-request", Kind: "api_call", StartedAt: createdAt, Method: "POST", Path: "/v1/videos", Outcome: "success", StatusCode: 202, RequestBody: `{"model":"v2-model"}`}
	fixtureEvent := RequestEventModel{RequestID: fixtureLog.ID, Sequence: 1, OccurredAt: createdAt, Phase: "response_completed", StatusCode: 202}
	for _, value := range []any{
		&SchemaMeta{Key: "schema_version", Value: previousSchemaVersion, UpdatedAt: createdAt},
		&fixtureChannel,
		&ChannelKeyModel{ChannelID: fixtureChannel.ID, Position: 0, Secret: "v2-secret", CreatedAt: createdAt, UpdatedAt: createdAt},
		&fixtureMapping,
		&SettingModel{Key: "audit_retention_days", Value: "17", UpdatedAt: createdAt},
		&AdminUserModel{Username: "v2-admin", PasswordHash: "v2-hash", SessionVersion: 4, CreatedAt: createdAt, UpdatedAt: createdAt},
		&fixtureTask,
		&fixtureLog,
		&fixtureEvent,
	} {
		if err := legacy.Create(value).Error; err != nil {
			t.Fatalf("insert v2 fixture row %T: %v", value, err)
		}
	}
	legacySQLDB, err := legacy.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := legacySQLDB.Close(); err != nil {
		t.Fatalf("close v2 fixture: %v", err)
	}

	if err := InitDB(dbPath); err != nil {
		t.Fatalf("migrate real v2 fixture: %v", err)
	}
	t.Cleanup(func() { _ = Close() })
	var marker SchemaMeta
	if err := DB.First(&marker, "key = ?", "schema_version").Error; err != nil {
		t.Fatal(err)
	}
	if marker.Value != SchemaVersion {
		t.Fatalf("schema marker after fixture migration = %q, want %q", marker.Value, SchemaVersion)
	}
	var channel ChannelModel
	if err := DB.First(&channel, "id = ?", fixtureChannel.ID).Error; err != nil {
		t.Fatal(err)
	}
	if channel.Name != fixtureChannel.Name || channel.Priority != fixtureChannel.Priority || channel.Weight != fixtureChannel.Weight {
		t.Fatalf("v2 channel changed during migration: %+v", channel)
	}
	var mapping ModelMappingModel
	if err := DB.Where("channel_id = ? AND source_model = ?", fixtureChannel.ID, fixtureMapping.SourceModel).First(&mapping).Error; err != nil {
		t.Fatal(err)
	}
	if mapping.TargetModel != fixtureMapping.TargetModel {
		t.Fatalf("v2 model mapping changed during migration: %+v", mapping)
	}
	var task TaskMapping
	if err := DB.First(&task, "task_id = ?", fixtureTask.TaskID).Error; err != nil {
		t.Fatal(err)
	}
	if task.ChannelID != fixtureTask.ChannelID || task.TaskKind != fixtureTask.TaskKind {
		t.Fatalf("v2 task mapping changed during migration: %+v", task)
	}
	var logRow RequestLogModel
	if err := DB.First(&logRow, "id = ?", fixtureLog.ID).Error; err != nil {
		t.Fatal(err)
	}
	if logRow.Path != fixtureLog.Path || logRow.RequestBody != fixtureLog.RequestBody || logRow.StatusCode != fixtureLog.StatusCode {
		t.Fatalf("v2 request log changed during migration: %+v", logRow)
	}
	var event RequestEventModel
	if err := DB.Where("request_id = ? AND sequence = ?", fixtureEvent.RequestID, fixtureEvent.Sequence).First(&event).Error; err != nil {
		t.Fatal(err)
	}
	if event.Phase != fixtureEvent.Phase {
		t.Fatalf("v2 request event changed during migration: %+v", event)
	}
	var setting SettingModel
	if err := DB.First(&setting, "key = ?", "audit_retention_days").Error; err != nil {
		t.Fatal(err)
	}
	if setting.Value != "17" {
		t.Fatalf("v2 setting changed during migration: %+v", setting)
	}
	if !DB.Migrator().HasTable(&ProtocolProfile{}) || !DB.Migrator().HasTable(&TaskRun{}) || !DB.Migrator().HasTable(&MediaAsset{}) {
		t.Fatal("v3 profile, task, or media tables were not created")
	}

	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatalf("restart after real v2 migration failed: %v", err)
	}
	defer Close()
	if err := DB.First(&channel, "id = ?", fixtureChannel.ID).Error; err != nil {
		t.Fatalf("v2 channel missing after restart: %v", err)
	}
	if err := DB.First(&marker, "key = ?", "schema_version").Error; err != nil || marker.Value != SchemaVersion {
		t.Fatalf("schema marker after restart = %+v, err=%v", marker, err)
	}
}

func TestInitDBMarksRunningRequestsInterrupted(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "running.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	started := time.Now().Add(-time.Minute)
	if err := DB.Create(&RequestLogModel{ID: "running-request", Kind: "api_call", StartedAt: started, Method: "POST", Path: "/v1/chat/completions", Outcome: "running"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	var row RequestLogModel
	if err := DB.First(&row, "id = ?", "running-request").Error; err != nil {
		t.Fatal(err)
	}
	if row.Outcome != "interrupted" || row.FinishedAt == nil || row.ErrorMessage == "" {
		t.Fatalf("running request was not recovered: %+v", row)
	}
}

func TestInitDBAddsAsyncAuditColumnsWithoutChangingHistoricalLogs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "async-audit-upgrade.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	legacy := RequestLogModel{
		ID:          "historical-request",
		Kind:        "api_call",
		StartedAt:   time.Now().UTC().Add(-time.Minute),
		Method:      "POST",
		Path:        "/v1/images/generations",
		Outcome:     "success",
		StatusCode:  200,
		RequestBody: `{"model":"legacy-image"}`,
	}
	if err := DB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	// Simulate a version-2 database created before async-summary fields were
	// introduced. SQLite rebuilds the table for DropColumn, preserving data.
	for _, field := range []string{"AsyncTaskKind", "AsyncTaskID", "AsyncTaskStatus", "AsyncPollCount", "AsyncLastPolledAt", "AsyncCompletedAt", "AsyncResultBody", "AsyncResultTruncated", "AsyncTaskError"} {
		if err := DB.Migrator().DropColumn(&RequestLogModel{}, field); err != nil {
			t.Fatalf("drop %s: %v", field, err)
		}
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	for _, field := range []string{"AsyncTaskKind", "AsyncTaskID", "AsyncTaskStatus", "AsyncPollCount", "AsyncLastPolledAt", "AsyncCompletedAt", "AsyncResultBody", "AsyncResultTruncated", "AsyncTaskError"} {
		if !DB.Migrator().HasColumn(&RequestLogModel{}, field) {
			t.Fatalf("InitDB did not add %s", field)
		}
	}
	var restored RequestLogModel
	if err := DB.First(&restored, "id = ?", legacy.ID).Error; err != nil {
		t.Fatal(err)
	}
	if restored.Path != legacy.Path || restored.RequestBody != legacy.RequestBody || restored.Outcome != legacy.Outcome || restored.AsyncPollCount != 0 || restored.AsyncTaskID != "" {
		t.Fatalf("historical audit changed during async-column migration: %+v", restored)
	}
}

func TestInitDBPreservesV2DataOnRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "preserve.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if err := SaveChannelModel(&ChannelModel{ID: "keep", Name: "keep", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true, Priority: 2, Weight: 3, APIKey: "secret"}); err != nil {
		t.Fatal(err)
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	channel, err := GetChannelModel("keep")
	if err != nil {
		t.Fatal(err)
	}
	if channel.Priority != 2 || channel.Weight != 3 || channel.APIKey != "secret" {
		t.Fatalf("v2 channel changed after restart: %+v", channel)
	}
}

func TestChannelKeysEncryptAtRestWhenConfigured(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "test-encryption-key-with-sufficient-entropy")
	dbPath := filepath.Join(t.TempDir(), "encrypted-keys.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := SaveChannelModel(&ChannelModel{ID: "encrypted", Type: "openai", BaseURL: "https://example.invalid/v1", APIKey: "provider-secret"}); err != nil {
		t.Fatal(err)
	}
	var row ChannelKeyModel
	if err := DB.Where("channel_id = ?", "encrypted").First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.Secret == "provider-secret" || !strings.HasPrefix(row.Secret, encryptedSecretPrefix) {
		t.Fatalf("channel key was not encrypted at rest: %q", row.Secret)
	}
	loaded, err := GetChannelModel("encrypted")
	if err != nil || loaded.APIKey != "provider-secret" {
		t.Fatalf("encrypted channel key did not decrypt: %+v, err=%v", loaded, err)
	}
}

func TestEncryptedChannelKeyFailureIsNotSilentlyActivated(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "correct-encryption-key")
	dbPath := filepath.Join(t.TempDir(), "encrypted-key-failure.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	channel := &ChannelModel{ID: "encrypted-failure", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true, APIKey: "provider-secret"}
	if err := SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "wrong-encryption-key")
	if _, err := GetChannelModel(channel.ID); err == nil {
		t.Fatal("expected loading a channel with an undecryptable key to fail")
	}
	RefreshActiveChannelsCache()
	for _, active := range GetActiveUpstreamChannels() {
		if active.ID == channel.ID {
			t.Fatal("channel with an undecryptable key was activated with empty credentials")
		}
	}
}

func TestInitDBRejectsEncryptedChannelKeysWithoutConfiguredKey(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "missing-encryption-key.db")
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "configured-encryption-key")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if err := SaveChannelModel(&ChannelModel{ID: "encrypted-restart", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true, APIKey: "provider-secret"}); err != nil {
		t.Fatal(err)
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "")
	err := InitDB(dbPath)
	if !errors.Is(err, ErrDBEncryptionKeyRequired) && (err == nil || !strings.Contains(err.Error(), "RELAY_DB_ENCRYPTION_KEY")) {
		t.Fatalf("InitDB error = %v, want missing RELAY_DB_ENCRYPTION_KEY failure", err)
	}
}

func TestSaveChannelInvalidatesSyncedModels(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "invalidate-models.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	channel := &ChannelModel{ID: "updated", Name: "updated", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true, Priority: 1, Weight: 1, APIKey: "secret"}
	if err := SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	UpdateChannelHealth(channel.ID, "healthy", 10, "", []string{"old-model"})
	channel.Name = "renamed"
	if err := SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	stored, err := GetChannelModel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ModelsSyncedRaw != "" {
		t.Fatalf("synced model cache = %q, want cleared after save", stored.ModelsSyncedRaw)
	}
}

func TestModelMappingsHaveCanonicalRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mapping.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := SaveChannelModel(&ChannelModel{ID: "mapped", Type: "openai", BaseURL: "https://example.invalid/v1", ModelMapRaw: `{"friendly":"provider-model"}`}); err != nil {
		t.Fatal(err)
	}
	var rows []ModelMappingModel
	if err := DB.Where("channel_id = ?", "mapped").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].SourceModel != "friendly" || rows[0].TargetModel != "provider-model" {
		t.Fatalf("unexpected canonical model mappings: %+v", rows)
	}
}

func TestSaveChannelRejectsInvalidHeaders(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "headers.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	for _, raw := range []string{`{"X-Test":`, `[]`, "{\"X-Test\":\"line\\nvalue\"}"} {
		channel := &ChannelModel{ID: "invalid-headers", Type: "openai", BaseURL: "https://api.example.test/v1", HeadersRaw: raw}
		if err := SaveChannelModel(channel); err == nil {
			t.Fatalf("invalid headers %q were accepted", raw)
		}
	}
}

func TestSaveChannelRejectsEmptyModelMapping(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "invalid-mapping.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	channel := &ChannelModel{ID: "invalid-mapping", Type: "openai", BaseURL: "https://api.example.test/v1", ModelMapRaw: `{"public-name":""}`}
	if err := SaveChannelModel(channel); err == nil {
		t.Fatal("model mapping with an empty upstream target was accepted")
	}
}

func TestSaveChannelPreservesExplicitFalseFlags(t *testing.T) {
	if err := InitDB(t.TempDir() + "/channel-flags.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	channel := &ChannelModel{
		ID:          "manual-models",
		Name:        "Manual models",
		Type:        "openai",
		BaseURL:     "https://api.example.com/v1",
		Enabled:     false,
		FetchModels: false,
		Priority:    1,
		Weight:      1,
		ModelsRaw:   "test-model",
	}
	if err := SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}

	var stored ChannelModel
	if err := DB.First(&stored, "id = ?", channel.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Enabled || stored.FetchModels {
		t.Fatalf("explicit false channel flags were replaced by defaults: enabled=%v fetch_models=%v", stored.Enabled, stored.FetchModels)
	}
}

func init() {
	if os.Getenv("RELAY_DB_ENCRYPTION_KEY") == "" {
		_ = os.Setenv("RELAY_DB_ENCRYPTION_KEY", "default-test-encryption-key-for-unit-tests-entropy")
	}
}

func TestChannelCredentialsRequireExternalEncryptionKey(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "")
	dbPath := filepath.Join(t.TempDir(), "unencrypted-reject.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	// Saving channel with APIKey must fail with ErrDBEncryptionKeyRequired
	chWithKey := &ChannelModel{
		ID:      "with-key",
		Name:    "With Key",
		Type:    "openai",
		BaseURL: "https://example.invalid/v1",
		APIKey:  "sensitive-credential",
		Enabled: true,
	}
	err := SaveChannelModel(chWithKey)
	if err == nil || !errors.Is(err, ErrDBEncryptionKeyRequired) {
		t.Fatalf("expected ErrDBEncryptionKeyRequired, got err=%v", err)
	}

	// Saving channel with APIKeys must fail with ErrDBEncryptionKeyRequired
	chWithKeys := &ChannelModel{
		ID:      "with-keys",
		Name:    "With Keys",
		Type:    "openai",
		BaseURL: "https://example.invalid/v1",
		APIKeys: []string{"cred-1", "cred-2"},
		Enabled: true,
	}
	err = SaveChannelModel(chWithKeys)
	if err == nil || !errors.Is(err, ErrDBEncryptionKeyRequired) {
		t.Fatalf("expected ErrDBEncryptionKeyRequired, got err=%v", err)
	}

	// Saving channel without credentials must succeed even when RELAY_DB_ENCRYPTION_KEY is unset
	chNoKey := &ChannelModel{
		ID:      "no-key",
		Name:    "No Key",
		Type:    "openai",
		BaseURL: "https://example.invalid/v1",
		Enabled: true,
	}
	if err := SaveChannelModel(chNoKey); err != nil {
		t.Fatalf("saving channel without credentials should succeed, got err=%v", err)
	}
}

func TestMigrateLegacyDatabaseEncryptionSuccess(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "")
	dbPath := filepath.Join(t.TempDir(), "legacy-migration.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}

	// 1. Manually insert legacy db_encryption_key into settings
	legacyPass := "legacy-database-key-secret-entropy"
	legacyKeySum := sha256.Sum256([]byte(legacyPass))
	legacyKey := legacyKeySum[:]
	if err := DB.Create(&SettingModel{Key: "db_encryption_key", Value: legacyPass}).Error; err != nil {
		t.Fatal(err)
	}

	// 2. Insert channel with key encrypted using legacy key
	legacyCipher, err := encryptWithKey("legacy-api-secret-12345", legacyKey)
	if err != nil {
		t.Fatal(err)
	}
	ch := &ChannelModel{
		ID:      "legacy-ch",
		Name:    "Legacy Channel",
		Type:    "openai",
		BaseURL: "https://example.invalid/v1",
		Enabled: true,
	}
	if err := DB.Create(ch).Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Create(&ChannelKeyModel{ChannelID: ch.ID, Position: 0, Secret: legacyCipher}).Error; err != nil {
		t.Fatal(err)
	}

	// 3. Insert MediaAsset with capability encrypted using legacy key
	legacyCapCipher, err := encryptWithKey("legacy-media-capability-token", legacyKey)
	if err != nil {
		t.Fatal(err)
	}
	asset := &MediaAsset{
		PublicID:             "legacy-asset-pubid",
		CapabilityHash:       hashMediaCapability("legacy-media-capability-token"),
		CapabilityCiphertext: legacyCapCipher,
		Kind:                 "image",
		Status:               MediaAssetAvailable,
		SourceKind:           "upstream_url",
	}
	if err := DB.Create(asset).Error; err != nil {
		t.Fatal(err)
	}
	_ = Close()

	// 4. Now set external RELAY_DB_ENCRYPTION_KEY and restart InitDB to trigger migration
	externalKey := "new-external-configured-encryption-key"
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", externalKey)
	if err := InitDB(dbPath); err != nil {
		t.Fatalf("InitDB with external key failed: %v", err)
	}
	t.Cleanup(func() { _ = Close() })

	// Verify legacy db_encryption_key was removed from settings
	var count int64
	if err := DB.Model(&SettingModel{}).Where("key = ?", "db_encryption_key").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("legacy db_encryption_key was not removed from settings, count=%d", count)
	}

	// Verify channel key can be loaded and decrypted with external key
	loadedCh, err := GetChannelModel(ch.ID)
	if err != nil {
		t.Fatalf("failed to load channel after migration: %v", err)
	}
	if loadedCh.APIKey != "legacy-api-secret-12345" {
		t.Fatalf("channel key decrypted to %q, want %q", loadedCh.APIKey, "legacy-api-secret-12345")
	}

	// Verify MediaAsset capability can be recovered with external key
	loadedAsset, err := GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatalf("failed to load media asset: %v", err)
	}
	recoveredCap, err := RecoverMediaAssetCapability(loadedAsset)
	if err != nil {
		t.Fatalf("failed to recover capability after migration: %v", err)
	}
	if recoveredCap != "legacy-media-capability-token" {
		t.Fatalf("media capability decrypted to %q, want %q", recoveredCap, "legacy-media-capability-token")
	}
}

func TestMigrateLegacyDatabaseEncryptionRollbackOnFailure(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "")
	dbPath := filepath.Join(t.TempDir(), "legacy-migration-rollback.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	legacyPass := "legacy-database-key-secret-entropy"
	legacyKeySum := sha256.Sum256([]byte(legacyPass))
	legacyKey := legacyKeySum[:]
	if err := DB.Create(&SettingModel{Key: "db_encryption_key", Value: legacyPass}).Error; err != nil {
		t.Fatal(err)
	}

	// Insert one valid row encrypted with legacy key
	validCipher, err := encryptWithKey("valid-secret", legacyKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := DB.Create(&ChannelModel{ID: "valid-ch", Name: "Valid", Type: "openai", BaseURL: "https://example.invalid/v1"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Create(&ChannelKeyModel{ChannelID: "valid-ch", Position: 0, Secret: validCipher}).Error; err != nil {
		t.Fatal(err)
	}

	// Insert one corrupted row with invalid ciphertext
	if err := DB.Create(&ChannelModel{ID: "corrupt-ch", Name: "Corrupt", Type: "openai", BaseURL: "https://example.invalid/v1"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Create(&ChannelKeyModel{ChannelID: "corrupt-ch", Position: 0, Secret: "enc:v1:this-is-not-valid-base64-or-gcm"}).Error; err != nil {
		t.Fatal(err)
	}

	// Trigger migration with external key
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "new-external-key")
	err = MigrateLegacyDatabaseEncryption(DB)
	if err == nil {
		t.Fatal("expected MigrateLegacyDatabaseEncryption to fail due to corrupted row")
	}

	// Verify rollback: legacy db_encryption_key MUST STILL EXIST in settings
	var s SettingModel
	if err := DB.First(&s, "key = ?", "db_encryption_key").Error; err != nil {
		t.Fatalf("legacy db_encryption_key should still exist after rollback: %v", err)
	}
	if s.Value != legacyPass {
		t.Fatalf("legacy db_encryption_key changed: %q, want %q", s.Value, legacyPass)
	}

	// Verify rollback: valid channel key MUST STILL HAVE the original legacy ciphertext
	var validRow ChannelKeyModel
	if err := DB.First(&validRow, "channel_id = ?", "valid-ch").Error; err != nil {
		t.Fatal(err)
	}
	if validRow.Secret != validCipher {
		t.Fatalf("valid channel key was modified despite transaction rollback: %q != %q", validRow.Secret, validCipher)
	}
}

func TestMigrateLegacyDatabaseEncryptionMediaCapabilityFailureRollback(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "")
	dbPath := filepath.Join(t.TempDir(), "legacy-migration-media-rollback.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	legacyPass := "legacy-database-key-secret-entropy"
	legacyKeySum := sha256.Sum256([]byte(legacyPass))
	legacyKey := legacyKeySum[:]
	if err := DB.Create(&SettingModel{Key: "db_encryption_key", Value: legacyPass}).Error; err != nil {
		t.Fatal(err)
	}

	// 1. Insert one valid channel key
	validCipher, err := encryptWithKey("valid-secret", legacyKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := DB.Create(&ChannelModel{ID: "valid-ch", Name: "Valid", Type: "openai", BaseURL: "https://example.invalid/v1"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Create(&ChannelKeyModel{ChannelID: "valid-ch", Position: 0, Secret: validCipher}).Error; err != nil {
		t.Fatal(err)
	}

	// 2. Insert one valid media asset encrypted with legacy key
	validCapCipher, err := encryptWithKey("valid-cap-secret", legacyKey)
	if err != nil {
		t.Fatal(err)
	}
	validAsset := &MediaAsset{
		PublicID:             "valid-asset-1",
		TaskRunID:            "run-1",
		Ordinal:              0,
		CapabilityHash:       "hash-1",
		CapabilityCiphertext: validCapCipher,
		Kind:                 "image",
		Status:               "ready",
		SourceKind:           "url",
	}
	if err := DB.Create(validAsset).Error; err != nil {
		t.Fatal(err)
	}

	// 3. Insert one corrupted media asset with un-decryptable capability ciphertext
	corruptAsset := &MediaAsset{
		PublicID:             "corrupt-asset-2",
		TaskRunID:            "run-2",
		Ordinal:              0,
		CapabilityHash:       "hash-2",
		CapabilityCiphertext: "enc:v1:corrupted-capability-ciphertext-fails-decrypt",
		Kind:                 "image",
		Status:               "ready",
		SourceKind:           "url",
	}
	if err := DB.Create(corruptAsset).Error; err != nil {
		t.Fatal(err)
	}

	// 4. Trigger migration with new external key
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "new-external-key")
	err = MigrateLegacyDatabaseEncryption(DB)
	if err == nil {
		t.Fatal("expected MigrateLegacyDatabaseEncryption to fail due to corrupted media asset capability")
	}

	// 5. Verify rollback: legacy db_encryption_key MUST STILL EXIST in settings
	var s SettingModel
	if err := DB.First(&s, "key = ?", "db_encryption_key").Error; err != nil {
		t.Fatalf("legacy db_encryption_key should still exist after rollback: %v", err)
	}
	if s.Value != legacyPass {
		t.Fatalf("legacy db_encryption_key changed: %q, want %q", s.Value, legacyPass)
	}

	// 6. Verify rollback: valid channel key MUST STILL HAVE original legacy ciphertext
	var validRow ChannelKeyModel
	if err := DB.First(&validRow, "channel_id = ?", "valid-ch").Error; err != nil {
		t.Fatal(err)
	}
	if validRow.Secret != validCipher {
		t.Fatalf("valid channel key was modified despite transaction rollback: %q != %q", validRow.Secret, validCipher)
	}

	// 7. Verify rollback: valid media asset capability MUST STILL HAVE original legacy ciphertext
	var assetRow MediaAsset
	if err := DB.First(&assetRow, "id = ?", validAsset.ID).Error; err != nil {
		t.Fatal(err)
	}
	if assetRow.CapabilityCiphertext != validCapCipher {
		t.Fatalf("valid media asset capability was modified despite transaction rollback: %q != %q", assetRow.CapabilityCiphertext, validCapCipher)
	}
}
