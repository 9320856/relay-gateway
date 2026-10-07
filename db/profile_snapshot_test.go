package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"gorm.io/gorm"
	"relay-gateway/protocol"
)

func newSnapshotTestDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.db")
	if err := InitDB(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	return path
}

func snapshotTestProfile(t *testing.T, name string) protocol.CompiledProfile {
	t.Helper()
	source, err := protocol.BuiltinPreset(protocol.PresetOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	source.Name = name
	compiled, err := protocol.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func createSnapshotTestRevision(t *testing.T, id string, number int, compiled protocol.CompiledProfile) {
	t.Helper()
	if number == 1 {
		if err := CreateProtocolProfile(&ProtocolProfile{ID: id, Source: ProfileSourceCustom}); err != nil {
			t.Fatal(err)
		}
	}
	if err := SaveProtocolProfileRevision(&ProtocolProfileRevision{ProfileID: id, Revision: number,
		SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()),
		ContentDigest: compiled.Digest(), State: ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
}

func createSnapshotTestRun(t *testing.T, id, profileID string, revision int, digest string) *TaskRun {
	t.Helper()
	run := &TaskRun{ID: id, ChannelID: "original-channel", Engine: "profile", TaskKind: "video", Operation: "video.create",
		ProfileID: profileID, ProfileRevision: revision, ProfileDigest: digest, ProviderTaskID: "provider-" + id,
		TaskStatus: "completed", TaskOutcome: "success", ResultBody: `{"status":"completed"}`}
	if err := CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	return run
}

func deleteSnapshotTestCatalog(ctx context.Context, kind, profileID string) error {
	if kind == "revision" {
		return DeleteProtocolProfileRevisionContext(ctx, profileID, 1)
	}
	return DeleteProtocolProfileContext(ctx, profileID)
}

func TestProfileSnapshotDeduplicatesAndBackfillsAcrossDeletionAndRestart(t *testing.T) {
	path := newSnapshotTestDB(t)
	compiled := snapshotTestProfile(t, "Shared immutable definition")
	createSnapshotTestRevision(t, "first", 1, compiled)
	createSnapshotTestRevision(t, "first", 2, compiled)
	createSnapshotTestRevision(t, "second", 1, compiled)
	runs := []*TaskRun{
		createSnapshotTestRun(t, "first-one", "first", 1, ""),
		createSnapshotTestRun(t, "first-two", "first", 2, compiled.Digest()),
		createSnapshotTestRun(t, "second-one", "second", 1, compiled.Digest()),
	}
	if err := DeleteProtocolProfileRevision("first", 1); err != nil {
		t.Fatal(err)
	}
	var originalArchive ProtocolProfileSnapshot
	if err := DB.First(&originalArchive).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		if err := DeleteProtocolProfile(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(path); err != nil {
		t.Fatal(err)
	}
	var archives []ProtocolProfileSnapshot
	if err := DB.Find(&archives).Error; err != nil || len(archives) != 1 || !archives[0].CreatedAt.Equal(originalArchive.CreatedAt) {
		t.Fatalf("archive was duplicated or overwritten: archives=%+v err=%v", archives, err)
	}
	for _, before := range runs {
		run, err := GetTaskRun(before.ID)
		if err != nil || run.ProfileID != "" || run.ProfileRevision != 0 || run.ProfileDigest != compiled.Digest() ||
			run.ChannelID != before.ChannelID || run.ProviderTaskID != before.ProviderTaskID || run.ResultBody != before.ResultBody {
			t.Fatalf("deletion lost captured task data: run=%+v err=%v", run, err)
		}
		loaded, err := LoadTaskProfileContext(context.Background(), run)
		if err != nil || loaded.Digest() != compiled.Digest() {
			t.Fatalf("restarted task lost its definition: digest=%s err=%v", loaded.Digest(), err)
		}
	}
	// A reader that captured the task before deletion can resolve its archive
	// without having to reload the detached task. Old tasks without a digest
	// must be reloaded to obtain the transactionally backfilled identity.
	if loaded, err := LoadTaskProfileContext(context.Background(), runs[2]); err != nil || loaded.Digest() != compiled.Digest() {
		t.Fatalf("reader held across deletion lost the archived definition: %v", err)
	}
	createSnapshotTestRevision(t, "second", 1, snapshotTestProfile(t, "Replacement definition"))
	fresh, err := GetTaskRun(runs[2].ID)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loaded, err := LoadTaskProfileContext(context.Background(), fresh)
			if err != nil || loaded.Digest() != compiled.Digest() {
				t.Errorf("concurrent historical read used replacement definition: %v", err)
			}
		}()
	}
	wg.Wait()
	if _, err := LoadTaskProfileContext(context.Background(), runs[2]); !errors.Is(err, ErrProfileSnapshotInvalid) {
		t.Fatalf("stale catalog identity accepted a different digest: %v", err)
	}
}

func assertSnapshotDeleteRolledBack(t *testing.T, run *TaskRun) {
	t.Helper()
	stored, err := GetTaskRun(run.ID)
	if err != nil || stored.ProfileID != run.ProfileID || stored.ProfileRevision != run.ProfileRevision || stored.ProfileDigest != run.ProfileDigest {
		t.Fatalf("failed deletion detached or backfilled a task: task=%+v err=%v", stored, err)
	}
	if _, err := GetProtocolProfile(run.ProfileID); err != nil {
		t.Fatalf("failed deletion removed profile: %v", err)
	}
	if _, err := GetProtocolProfileRevision(run.ProfileID, run.ProfileRevision); err != nil {
		t.Fatalf("failed deletion removed revision: %v", err)
	}
	var count int64
	if err := DB.Model(&ProtocolProfileSnapshot{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("failed deletion left archive rows: count=%d err=%v", count, err)
	}
}

func TestProfileSnapshotDeletionFailureRollsBackInsideOwnerTransaction(t *testing.T) {
	for _, kind := range []string{"profile", "revision"} {
		for _, failure := range []string{"detach", "cancel"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				newSnapshotTestDB(t)
				compiled := snapshotTestProfile(t, "Atomic deletion")
				createSnapshotTestRevision(t, "atomic", 1, compiled)
				run := createSnapshotTestRun(t, "atomic-run", "atomic", 1, "")
				if failure == "detach" {
					if err := DB.Exec(`CREATE TRIGGER reject_detach BEFORE UPDATE ON async_task_runs WHEN NEW.profile_id = '' BEGIN SELECT RAISE(ABORT, 'reject detach'); END`).Error; err != nil {
						t.Fatal(err)
					}
				}
				if err := DB.Transaction(func(owner *gorm.DB) error {
					ctx, cancel := context.WithCancel(WithTx(context.Background(), owner))
					defer cancel()
					const callback = "test:cancel_archive_insert"
					if failure == "cancel" {
						if err := DB.Callback().Create().After("gorm:create").Register(callback, func(tx *gorm.DB) {
							if tx.Statement.Table == "protocol_profile_snapshots" {
								cancel()
							}
						}); err != nil {
							return err
						}
						defer DB.Callback().Create().Remove(callback)
					}
					err := deleteSnapshotTestCatalog(ctx, kind, "atomic")
					if err == nil || failure == "cancel" && !errors.Is(err, context.Canceled) {
						return fmt.Errorf("expected deletion failure %s, got %v", failure, err)
					}
					return SetSettingContext(WithTx(context.Background(), owner), "recovered-owner", "true")
				}); err != nil {
					t.Fatal(err)
				}
				assertSnapshotDeleteRolledBack(t, run)
				if GetSetting("recovered-owner", "") != "true" {
					t.Fatal("failed deletion prevented owner transaction recovery")
				}
			})
		}
	}
}

func TestProfileSnapshotDeletionRejectsInvalidCapturedIdentity(t *testing.T) {
	for _, invalid := range []string{"task-digest", "content", "schema", "draft", "archive-conflict"} {
		t.Run(invalid, func(t *testing.T) {
			newSnapshotTestDB(t)
			compiled := snapshotTestProfile(t, "Validated deletion")
			createSnapshotTestRevision(t, "validated", 1, compiled)
			run := createSnapshotTestRun(t, "validated-run", "validated", 1, compiled.Digest())
			var err error
			switch invalid {
			case "task-digest":
				run.ProfileDigest = snapshotTestProfile(t, "Different definition").Digest()
				err = DB.Model(run).Update("profile_digest", run.ProfileDigest).Error
			case "content":
				err = DB.Model(&ProtocolProfileRevision{}).Where("profile_id = ?", "validated").Update("content_json", `{}`).Error
			case "schema":
				err = DB.Model(&ProtocolProfileRevision{}).Where("profile_id = ?", "validated").Update("schema_version", 999).Error
			case "draft":
				err = DB.Model(&ProtocolProfileRevision{}).Where("profile_id = ?", "validated").Update("state", ProfileRevisionDraft).Error
			case "archive-conflict":
				err = DB.Create(&ProtocolProfileSnapshot{ContentDigest: compiled.Digest(), SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: `{}`}).Error
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := DeleteProtocolProfile("validated"); !errors.Is(err, ErrProfileSnapshotInvalid) {
				t.Fatalf("deletion accepted %s captured identity: %v", invalid, err)
			}
			if invalid == "archive-conflict" {
				var stored ProtocolProfileSnapshot
				if err := DB.First(&stored).Error; err != nil || stored.ContentJSON != `{}` {
					t.Fatalf("immutable conflicting archive was overwritten: %+v %v", stored, err)
				}
				// Remove only the pre-existing corrupt test row before checking
				// that the failed deletion did not leave any new archive rows.
				if err := DB.Delete(&stored).Error; err != nil {
					t.Fatal(err)
				}
			}
			assertSnapshotDeleteRolledBack(t, run)
		})
	}
}
