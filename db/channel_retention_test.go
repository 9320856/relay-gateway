package db

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"relay-gateway/media"
)

func TestChannelMediaRetentionDefaultsValidationAndCache(t *testing.T) {
	newSnapshotTestDB(t)
	channel := &ChannelModel{ID: "retention-cache", Type: "openai", BaseURL: "https://retention.example.invalid/v1", Enabled: true}
	if err := SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if channel.MediaRetention != media.RetentionDisabled {
		t.Fatalf("new channel policy = %q", channel.MediaRetention)
	}
	for _, policy := range []string{media.RetentionDisabled, media.RetentionBestEffort, media.RetentionRequired} {
		channel.MediaRetention = policy
		if err := SaveChannelModel(channel); err != nil {
			t.Fatal(err)
		}
		stored, err := GetChannelModel(channel.ID)
		if err != nil || stored.MediaRetention != policy || stored.ToUpstreamChannel().MediaRetention != policy {
			t.Fatalf("channel retention was lost: stored=%+v err=%v", stored, err)
		}
		cached := GetActiveUpstreamChannels()
		if len(cached) != 1 || cached[0].MediaRetention != policy {
			t.Fatalf("active cache has stale retention: %+v", cached)
		}
		snapshot, err := loadActiveChannelsSnapshot(DB)
		if err != nil || len(snapshot) != 1 || snapshot[0].MediaRetention != policy {
			t.Fatalf("channel snapshot has stale retention: %+v, %v", snapshot, err)
		}
	}
	for _, policy := range []string{"sometimes", "REQUIRED", " required", " "} {
		channel.MediaRetention = policy
		if err := SaveChannelModel(channel); err == nil {
			t.Fatalf("channel save accepted invalid retention %q", policy)
		}
		stored, err := GetChannelModel(channel.ID)
		if err != nil || stored.MediaRetention != media.RetentionRequired {
			t.Fatalf("invalid save changed previous policy: %+v, %v", stored, err)
		}
	}
}

func TestChannelMediaRetentionMigrationPreservesOldTasksAndRestartSettings(t *testing.T) {
	for _, version := range []string{"2", "3", "4", "5"} {
		t.Run("schema-"+version, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "before-retention.db")
			legacy, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			if err := legacy.AutoMigrate(schemaModels()...); err != nil {
				t.Fatal(err)
			}
			for _, row := range []any{
				&SchemaMeta{Key: "schema_version", Value: version},
				&ChannelModel{ID: "historical", Name: "Historical", Type: "openai", BaseURL: "https://historical.example.invalid/v1", Enabled: true, SelectedModelsRaw: `["media-model"]`},
				&TaskRun{ID: "old-profile", Engine: "profile", ChannelID: "historical", Operation: "video.create", TaskStatus: "processing", ProfileID: "captured", ProfileRevision: 1, ProfileDigest: "saved-digest", ResultBody: `{"status":"processing"}`},
				&TaskRun{ID: "old-legacy", Engine: "legacy", ChannelID: "historical", Operation: "video.create", TaskStatus: "queued"},
			} {
				if err := legacy.Create(row).Error; err != nil {
					t.Fatal(err)
				}
			}
			// The historical schema genuinely lacks both retention columns.
			if err := legacy.Migrator().DropColumn(&ChannelModel{}, "MediaRetention"); err != nil {
				t.Fatal(err)
			}
			if err := legacy.Migrator().DropColumn(&TaskRun{}, "MediaRetention"); err != nil {
				t.Fatal(err)
			}
			pool, err := legacy.DB()
			if err != nil {
				t.Fatal(err)
			}
			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
			if report, err := InspectDatabase(context.Background(), path); err != nil || report.SchemaVersion != version {
				t.Fatalf("pre-migration inspection rejected schema %s: %+v, %v", version, report, err)
			}
			backup := filepath.Join(t.TempDir(), "old-backup.db")
			if err := BackupDatabase(context.Background(), path, backup); err != nil {
				t.Fatal(err)
			}
			if report, err := InspectDatabase(context.Background(), backup); err != nil || report.SchemaVersion != version {
				t.Fatalf("historical backup rejected schema %s: %+v, %v", version, report, err)
			}
			if err := InitDB(path); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = Close() })
			channel, err := GetChannelModel("historical")
			if err != nil || channel.MediaRetention != media.RetentionDisabled {
				t.Fatalf("existing channel did not default to disabled: %+v, %v", channel, err)
			}
			channel.MediaRetention = media.RetentionRequired
			if err := SaveChannelModel(channel); err != nil {
				t.Fatal(err)
			}
			for restart := 0; restart < 2; restart++ {
				if err := Close(); err != nil {
					t.Fatal(err)
				}
				if err := InitDB(path); err != nil {
					t.Fatal(err)
				}
				channel, err := GetChannelModel("historical")
				if err != nil || channel.MediaRetention != media.RetentionRequired || channel.SelectedModelsRaw != `["media-model"]` {
					t.Fatalf("restart reset channel policy/settings: %+v, %v", channel, err)
				}
				profile, err := GetTaskRun("old-profile")
				if err != nil || profile.MediaRetention != "" || profile.ProfileDigest != "saved-digest" || profile.ProfileRevision != 1 || profile.TaskStatus != "processing" || profile.ResultBody != `{"status":"processing"}` {
					t.Fatalf("migration changed an old Profile task: %+v, %v", profile, err)
				}
				oldLegacy, err := GetTaskRun("old-legacy")
				if err != nil || oldLegacy.MediaRetention != "" || oldLegacy.TaskStatus != "queued" {
					t.Fatalf("migration changed an old legacy task: %+v, %v", oldLegacy, err)
				}
				var marker SchemaMeta
				if err := DB.First(&marker, "key = ?", "schema_version").Error; err != nil || marker.Value != SchemaVersion {
					t.Fatalf("migration marker = %+v, %v", marker, err)
				}
			}
			currentBackup := filepath.Join(t.TempDir(), "current-backup.db")
			if err := BackupDatabase(context.Background(), path, currentBackup); err != nil {
				t.Fatal(err)
			}
			if report, err := InspectDatabase(context.Background(), currentBackup); err != nil || report.SchemaVersion != SchemaVersion {
				t.Fatalf("current backup rejected schema: %+v, %v", report, err)
			}
		})
	}
}
