package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"relay-gateway/protocol"
)

func TestInitDBMigratesVersionFourWithoutArchivePreservingHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actual-v4.db")
	legacy, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	// This is the complete version-four table set. Build it independently of
	// schemaModels so the new archive cannot accidentally enter the fixture.
	v4Models := []any{&SchemaMeta{}, &ChannelModel{}, &ChannelKeyModel{}, &ModelMappingModel{}, &SettingModel{}, &AdminUserModel{}, &AdminSessionModel{}, &GatewayTokenModel{}, &VideoTaskMapping{}, &RequestLogModel{}, &RequestEventModel{}, &ProtocolProfile{}, &ProtocolProfileRevision{}, &ChannelProtocolBinding{}, &TaskRun{}, &TaskAlias{}, &TaskAttempt{}, &TaskEvent{}, &MediaAsset{}, &MediaObject{}, &MediaMaterializationJob{}, &MediaDeletionJob{}}
	if err := legacy.AutoMigrate(v4Models...); err != nil {
		t.Fatal(err)
	}
	if legacy.Migrator().HasTable(&ProtocolProfileSnapshot{}) {
		t.Fatal("version-four fixture already contains the new archive")
	}
	preset, err := protocol.BuiltinPreset(protocol.PresetOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := protocol.Compile(preset)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Second)
	profile := ProtocolProfile{ID: "v4-profile", Name: "Preserved profile", Source: ProfileSourceCustom, LatestRevision: 1, CreatedAt: createdAt, UpdatedAt: createdAt}
	revision := ProtocolProfileRevision{ProfileID: profile.ID, Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: ProfileRevisionPublished, CreatedAt: createdAt, UpdatedAt: createdAt}
	run := TaskRun{ID: "v4-completed-task", ChannelID: "v4-channel", Engine: "profile", ProfileID: profile.ID, ProfileRevision: 1, ProfileDigest: revision.ContentDigest, TaskKind: "video", Operation: "video.create", ProviderTaskID: "v4-provider-id", TaskStatus: "completed", ResultBody: `{"url":"https://result.example.invalid/video.mp4"}`, CreatedAt: createdAt, UpdatedAt: createdAt}
	for _, row := range []any{
		&SchemaMeta{Key: "schema_version", Value: "4"},
		&SettingModel{Key: "audit_retention_days", Value: "19"},
		&ChannelModel{ID: "v4-channel", Name: "V4", Type: "openai", BaseURL: "https://v4.example.invalid/v1", Enabled: true, SelectedModelsRaw: `["v4-model"]`},
		&profile, &revision, &run,
		&TaskAlias{TaskRunID: run.ID, LookupID: "v4-public-task"},
	} {
		if err := legacy.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	pool, err := legacy.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if report, err := InspectDatabase(context.Background(), path); err != nil || report.SchemaVersion != "4" {
		t.Fatalf("pre-upgrade inspection rejected version four: report=%+v err=%v", report, err)
	}
	backup := filepath.Join(t.TempDir(), "v4-backup.db")
	if err := BackupDatabase(context.Background(), path, backup); err != nil {
		t.Fatal(err)
	}
	if report, err := InspectDatabase(context.Background(), backup); err != nil || report.SchemaVersion != "4" {
		t.Fatalf("version-four backup inspection failed: report=%+v err=%v", report, err)
	}
	if err := InitDB(path); err != nil {
		t.Fatalf("version-four upgrade: %v", err)
	}
	t.Cleanup(func() { _ = Close() })
	if !DB.Migrator().HasTable(&ProtocolProfileSnapshot{}) {
		t.Fatal("upgrade did not add the archive table")
	}
	archive := ProtocolProfileSnapshot{ContentDigest: revision.ContentDigest, SchemaVersion: revision.SchemaVersion, ContentJSON: revision.ContentJSON, CreatedAt: createdAt}
	if err := DB.Create(&archive).Error; err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		var marker SchemaMeta
		if err := DB.First(&marker, "key = ?", "schema_version").Error; err != nil || marker.Value != SchemaVersion {
			t.Fatalf("schema marker after upgrade/restart: marker=%+v err=%v", marker, err)
		}
		var storedRun TaskRun
		if err := DB.First(&storedRun, "id = ?", run.ID).Error; err != nil || storedRun.ProfileDigest != run.ProfileDigest || storedRun.ProfileID != run.ProfileID || storedRun.ResultBody != run.ResultBody || !storedRun.CreatedAt.Equal(createdAt) {
			t.Fatalf("historical task changed during upgrade/restart: run=%+v err=%v", storedRun, err)
		}
		var storedArchive ProtocolProfileSnapshot
		if err := DB.First(&storedArchive, "content_digest = ?", archive.ContentDigest).Error; err != nil || storedArchive.ContentJSON != archive.ContentJSON || !storedArchive.CreatedAt.Equal(createdAt) {
			t.Fatalf("archive changed after restart: archive=%+v err=%v", storedArchive, err)
		}
		loaded, err := GetTaskProfileRevisionContext(context.Background(), &storedRun)
		if err != nil || loaded.ContentDigest != revision.ContentDigest || loaded.ContentJSON != revision.ContentJSON {
			t.Fatalf("captured version-four definition was lost: revision=%+v err=%v", loaded, err)
		}
		channel, err := GetChannelModel("v4-channel")
		if err != nil || channel.SelectedModelsRaw != `["v4-model"]` || GetSetting("audit_retention_days", "") != "19" {
			t.Fatalf("version-four settings changed: channel=%+v err=%v", channel, err)
		}
		if restart == 0 {
			if err := Close(); err != nil {
				t.Fatal(err)
			}
			if err := InitDB(path); err != nil {
				t.Fatal(err)
			}
		}
	}
	currentBackup := filepath.Join(t.TempDir(), "current-backup.db")
	if err := BackupDatabase(context.Background(), path, currentBackup); err != nil {
		t.Fatal(err)
	}
	if report, err := InspectDatabase(context.Background(), currentBackup); err != nil || report.SchemaVersion != SchemaVersion {
		t.Fatalf("upgraded backup inspection failed: report=%+v err=%v", report, err)
	}
}

func TestProfileSnapshotSchemaSupportsExplicitOlderMarkers(t *testing.T) {
	for _, version := range []string{"2", "3", "4", "5", SchemaVersion} {
		t.Run(version, func(t *testing.T) {
			path := makeBackupTestDB(t, version)
			if report, err := InspectDatabase(context.Background(), path); err != nil || report.SchemaVersion != version {
				t.Fatalf("inspect supported marker %s: report=%+v err=%v", version, report, err)
			}
			if err := InitDB(path); err != nil {
				t.Fatalf("upgrade supported marker %s: %v", version, err)
			}
			t.Cleanup(func() { _ = Close() })
			if !DB.Migrator().HasTable(&ProtocolProfileSnapshot{}) {
				t.Fatal("supported upgrade omitted the archive table")
			}
			var marker SchemaMeta
			if err := DB.First(&marker, "key = ?", "schema_version").Error; err != nil || marker.Value != SchemaVersion {
				t.Fatalf("upgraded marker=%+v err=%v", marker, err)
			}
		})
	}
}

func TestProfileSnapshotSchemaRejectsUnsupportedMarkersWithoutChangingDatabase(t *testing.T) {
	for _, version := range []string{"1", "7", "99"} {
		t.Run(version, func(t *testing.T) {
			path := makeBackupTestDB(t, version)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := InspectDatabase(context.Background(), path); !errors.Is(err, ErrIncompatibleSchema) {
				t.Fatalf("inspection accepted unsupported marker: %v", err)
			}
			if err := InitDB(path); !errors.Is(err, ErrIncompatibleSchema) {
				t.Fatalf("startup accepted unsupported marker: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("unsupported database changed: %v", err)
			}
		})
	}
}
