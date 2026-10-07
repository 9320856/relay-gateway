package migration

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/protocol"
)

func compiledArchiveReportProfile(t *testing.T, name string) protocol.CompiledProfile {
	t.Helper()
	preset, err := protocol.BuiltinPreset(protocol.PresetOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	preset.Name = name
	compiled, err := protocol.Compile(preset)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func createArchivedReportTask(t *testing.T, ctx context.Context, id, profileID string, revision int, digest string) {
	t.Helper()
	conn := db.SQLDBForContext(ctx)
	providerID, logID := "provider-"+id, "log-"+id
	if err := conn.Create(&db.RequestLogModel{ID: logID, Kind: "api_call", StartedAt: time.Now(), Method: "POST", Path: "/v1/videos", ChannelID: "archive-channel", ChannelType: "openai", AsyncTaskKind: "video", AsyncTaskID: providerID, Outcome: "success"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTaskRunContext(ctx, &db.TaskRun{ID: id, OriginRequestID: logID, ChannelID: "archive-channel", Engine: "profile", ProfileID: profileID, ProfileRevision: revision, ProfileDigest: digest, TaskKind: "video", Operation: "video.create", ProviderTaskID: providerID, TaskStatus: "completed", TaskOutcome: "success"}); err != nil {
		t.Fatal(err)
	}
}

func initArchivedReportDB(t *testing.T) {
	t.Helper()
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	if err := db.InitDB(t.TempDir() + "/archive-report.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "archive-channel", Type: "openai", BaseURL: "https://archive.example.invalid/v1", Enabled: false}); err != nil {
		t.Fatal(err)
	}
}

func TestAuditChannelsRecognizesDetachedValidArchivedHistory(t *testing.T) {
	initArchivedReportDB(t)
	compiled := compiledArchiveReportProfile(t, "Valid archive")
	if err := db.DB.Create(&db.ProtocolProfileSnapshot{ContentDigest: compiled.Digest(), SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON())}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		createArchivedReportTask(t, context.Background(), fmt.Sprintf("archived-%d", i), "", 0, compiled.Digest())
	}
	var archiveReads atomic.Int32
	const callback = "test:count_report_archive_validation"
	if err := db.DB.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "protocol_profile_snapshots" {
			archiveReads.Add(1)
		}
	}); err != nil {
		t.Fatal(err)
	}
	report, err := AuditChannels(context.Background())
	_ = db.DB.Callback().Query().Remove(callback)
	if err != nil || !report.Ready || report.ProfileTaskRuns != 3 || report.ProfileTasksMissingRevision != 0 || report.ProfileTasksMissingDigest != 0 {
		t.Fatalf("archived history blocked migration: report=%+v err=%v", report, err)
	}
	if archiveReads.Load() != 1 {
		t.Fatalf("identical captured definitions were repeatedly loaded: reads=%d", archiveReads.Load())
	}
	var task db.TaskRun
	if err := db.DB.First(&task, "id = ?", "archived-0").Error; err != nil || task.ProfileID != "" || task.ProfileRevision != 0 || task.ProfileDigest != compiled.Digest() {
		t.Fatalf("read-only audit changed detached history: task=%+v err=%v", task, err)
	}
}

func TestAuditChannelsStillBlocksMissingAndCorruptCapturedDefinitions(t *testing.T) {
	initArchivedReportDB(t)
	valid := compiledArchiveReportProfile(t, "Valid retained definition")
	corrupt := compiledArchiveReportProfile(t, "Corrupted archive")
	missing := compiledArchiveReportProfile(t, "Missing archive")
	for _, row := range []any{
		&db.ProtocolProfileSnapshot{ContentDigest: valid.Digest(), SchemaVersion: valid.Profile().SchemaVersion, ContentJSON: string(valid.CanonicalJSON())},
		&db.ProtocolProfileSnapshot{ContentDigest: corrupt.Digest(), SchemaVersion: corrupt.Profile().SchemaVersion, ContentJSON: `{}`},
		&db.ProtocolProfileRevision{ProfileID: "retired", Revision: 1, SchemaVersion: valid.Profile().SchemaVersion, ContentJSON: string(valid.CanonicalJSON()), ContentDigest: valid.Digest(), State: db.ProfileRevisionRetired},
		&db.ProtocolProfileRevision{ProfileID: "mismatch", Revision: 1, SchemaVersion: valid.Profile().SchemaVersion, ContentJSON: string(valid.CanonicalJSON()), ContentDigest: valid.Digest(), State: db.ProfileRevisionPublished},
		&db.ProtocolProfileRevision{ProfileID: "draft", Revision: 1, SchemaVersion: valid.Profile().SchemaVersion, ContentJSON: string(valid.CanonicalJSON()), ContentDigest: valid.Digest(), State: db.ProfileRevisionDraft},
	} {
		if err := db.DB.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	createArchivedReportTask(t, ctx, "valid-archive", "", 0, valid.Digest())
	createArchivedReportTask(t, ctx, "valid-retired", "retired", 1, valid.Digest())
	createArchivedReportTask(t, ctx, "missing", "", 0, missing.Digest())
	createArchivedReportTask(t, ctx, "corrupt", "", 0, corrupt.Digest())
	createArchivedReportTask(t, ctx, "mismatch", "mismatch", 1, corrupt.Digest())
	createArchivedReportTask(t, ctx, "draft", "draft", 1, valid.Digest())
	for i := 0; i < 3; i++ {
		createArchivedReportTask(t, ctx, fmt.Sprintf("missing-digest-%d", i), "", 0, "")
	}
	report, err := AuditChannels(ctx)
	if err != nil || report.Ready || report.ProfileTaskRuns != 9 || report.ProfileTasksMissingRevision != 7 || report.ProfileTasksMissingDigest != 3 || len(report.Channels) != 1 || report.Channels[0].ProfileTasksMissingRevision != 7 {
		t.Fatalf("missing/corrupt definitions were hidden by archive support: report=%+v err=%v", report, err)
	}
}

func TestAuditChannelsArchivedLookupPropagatesSQLErrors(t *testing.T) {
	initArchivedReportDB(t)
	compiled := compiledArchiveReportProfile(t, "Operational lookup failure")
	createArchivedReportTask(t, context.Background(), "lookup-failure", "", 0, compiled.Digest())
	injected := errors.New("injected snapshot database failure")
	const callback = "test:fail_report_archive_lookup"
	if err := db.DB.Callback().Query().Before("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "protocol_profile_snapshots" {
			tx.AddError(injected)
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, err := AuditChannels(context.Background())
	_ = db.DB.Callback().Query().Remove(callback)
	if !errors.Is(err, injected) {
		t.Fatalf("database failure was counted as a missing definition: %v", err)
	}
}

func TestAuditChannelsUsesOwnerTransactionForArchivedDefinitions(t *testing.T) {
	initArchivedReportDB(t)
	compiled := compiledArchiveReportProfile(t, "Transaction-local archive")
	owner := db.DB.Begin()
	if owner.Error != nil {
		t.Fatal(owner.Error)
	}
	defer owner.Rollback()
	ctx := db.WithTx(context.Background(), owner)
	if err := owner.Create(&db.ProtocolProfileSnapshot{ContentDigest: compiled.Digest(), SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON())}).Error; err != nil {
		t.Fatal(err)
	}
	createArchivedReportTask(t, ctx, "transaction-local", "", 0, compiled.Digest())
	report, err := AuditChannels(ctx)
	if err != nil || !report.Ready || report.ProfileTaskRuns != 1 || report.ProfileTasksMissingRevision != 0 {
		t.Fatalf("report bypassed its owner transaction: report=%+v err=%v", report, err)
	}
	if err := owner.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	report, err = AuditChannels(context.Background())
	if err != nil || report.ProfileTaskRuns != 0 {
		t.Fatalf("audit published rolled-back archive/task state: report=%+v err=%v", report, err)
	}
}
