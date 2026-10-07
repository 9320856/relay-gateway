package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

type revisionSaveObservationKey struct{}

func TestSaveProtocolProfileRevisionCannotOverwriteConcurrentPublication(t *testing.T) {
	if err := InitDB(t.TempDir() + "/save-publish.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "concurrent"}); err != nil {
		t.Fatal(err)
	}
	draft := &ProtocolProfileRevision{ProfileID: "concurrent", Revision: 1, ContentJSON: "original"}
	if err := SaveProtocolProfileRevision(draft); err != nil {
		t.Fatal(err)
	}

	observedDraft := make(chan struct{})
	resumeSave := make(chan struct{})
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resumeSave) }) }
	defer resume()
	const callback = "test:pause_revision_save"
	if err := DB.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "protocol_profile_revisions" && tx.Statement.Context.Value(revisionSaveObservationKey{}) != nil {
			close(observedDraft)
			<-resumeSave
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer DB.Callback().Query().Remove(callback)
	saved := make(chan error, 1)
	go func() {
		candidate := *draft
		candidate.ContentJSON = "edited before publication"
		ctx := context.WithValue(context.Background(), revisionSaveObservationKey{}, true)
		saved <- SaveProtocolProfileRevisionContext(ctx, &candidate)
	}()
	select {
	case <-observedDraft:
	case <-time.After(5 * time.Second):
		t.Fatal("save did not read the draft")
	}

	published := make(chan error, 1)
	go func() { published <- PublishProtocolProfileRevision("concurrent", 1) }()
	// An atomic save retains the writer while it checks the draft, so publish
	// waits. The former implementation allowed publish to finish in this gap.
	var publishErr error
	publicationFinished := false
	select {
	case publishErr = <-published:
		publicationFinished = true
	case <-time.After(100 * time.Millisecond):
	}
	resume()
	select {
	case err := <-saved:
		if err != nil {
			t.Fatalf("save draft: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("save did not finish")
	}
	if !publicationFinished {
		select {
		case publishErr = <-published:
		case <-time.After(5 * time.Second):
			t.Fatal("publish did not finish")
		}
	}
	if publishErr != nil {
		t.Fatalf("publish: %v", publishErr)
	}
	stored, err := GetProtocolProfileRevision("concurrent", 1)
	if err != nil || stored.State != ProfileRevisionPublished || stored.ContentJSON != "edited before publication" {
		t.Fatalf("concurrent save undid publication: revision=%+v err=%v", stored, err)
	}
}

func TestSaveProtocolProfileRevisionRollsBackFailedCreateInOwnerTransaction(t *testing.T) {
	if err := InitDB(t.TempDir() + "/save-create-rollback.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "atomic"}); err != nil {
		t.Fatal(err)
	}
	if err := DB.Exec(`CREATE TRIGGER reject_latest BEFORE UPDATE ON protocol_profiles WHEN NEW.latest_revision = 2 BEGIN SELECT RAISE(ABORT, 'rejected latest revision'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Transaction(func(owner *gorm.DB) error {
		candidate := &ProtocolProfileRevision{ProfileID: "atomic", Revision: 2, ContentJSON: "rejected"}
		if err := SaveProtocolProfileRevisionContext(WithTx(context.Background(), owner), candidate); err == nil {
			return errors.New("save should fail when the latest revision update is rejected")
		}
		return SetSettingContext(WithTx(context.Background(), owner), "owner-recovered", "true")
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := GetProtocolProfileRevision("atomic", 2); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("failed save left a revision after its owner committed: %v", err)
	}
	profile, err := GetProtocolProfile("atomic")
	if err != nil || profile.LatestRevision != 0 || GetSetting("owner-recovered", "") != "true" {
		t.Fatalf("failed save damaged owner transaction: profile=%+v err=%v", profile, err)
	}
}

func TestSaveProtocolProfileRevisionCanceledSaveRollsBackItsSavepoint(t *testing.T) {
	if err := InitDB(t.TempDir() + "/save-cancel.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "cancel"}); err != nil {
		t.Fatal(err)
	}
	if err := DB.Transaction(func(owner *gorm.DB) error {
		ctx, cancel := context.WithCancel(WithTx(context.Background(), owner))
		defer cancel()
		const callback = "test:cancel_revision_save_insert"
		if err := DB.Callback().Create().After("gorm:create").Register(callback, func(tx *gorm.DB) {
			if tx.Statement.Table == "protocol_profile_revisions" {
				cancel()
			}
		}); err != nil {
			return err
		}
		candidate := &ProtocolProfileRevision{ProfileID: "cancel", Revision: 2}
		err := SaveProtocolProfileRevisionContext(ctx, candidate)
		_ = DB.Callback().Create().Remove(callback)
		if !errors.Is(err, context.Canceled) || candidate.ID != 0 {
			return fmt.Errorf("canceled save error=%v revision=%+v", err, candidate)
		}
		profile, err := GetProtocolProfileContext(WithTx(context.Background(), owner), "cancel")
		if err != nil || profile.LatestRevision != 0 {
			return fmt.Errorf("canceled save retained metadata: profile=%+v err=%v", profile, err)
		}
		return SaveProtocolProfileRevisionContext(WithTx(context.Background(), owner), &ProtocolProfileRevision{ProfileID: "cancel", Revision: 1, ContentJSON: "recovered"})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := GetProtocolProfileRevision("cancel", 2); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("canceled save left a revision: %v", err)
	}
	if revision, err := GetProtocolProfileRevision("cancel", 1); err != nil || revision.ContentJSON != "recovered" {
		t.Fatalf("owner could not recover after cancellation: revision=%+v err=%v", revision, err)
	}
}

func TestSaveProtocolProfileRevisionCreatesByProfileAndRevision(t *testing.T) {
	if err := InitDB(t.TempDir() + "/save-create-identity.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "copied"}); err != nil {
		t.Fatal(err)
	}
	original := &ProtocolProfileRevision{ProfileID: "copied", Revision: 1, ContentJSON: "published", State: ProfileRevisionPublished}
	if err := SaveProtocolProfileRevision(original); err != nil {
		t.Fatal(err)
	}
	next := *original
	next.Revision, next.ContentJSON, next.State = 2, "next draft", ProfileRevisionDraft
	if err := SaveProtocolProfileRevisionContext(nil, &next); err != nil {
		t.Fatalf("create with a copied revision ID: %v", err)
	}
	if next.ID == 0 || next.ID == original.ID {
		t.Fatalf("new revision did not receive its own identity: old=%d new=%d", original.ID, next.ID)
	}
	stored, err := GetProtocolProfileRevision("copied", 1)
	if err != nil || stored.State != ProfileRevisionPublished || stored.ContentJSON != original.ContentJSON {
		t.Fatalf("create changed the original immutable revision: revision=%+v err=%v", stored, err)
	}
	// Updating the earlier draft fields must preserve explicit zero values and
	// must never move the profile's latest revision pointer backwards.
	next.SchemaVersion, next.ContentJSON, next.ContentDigest = 0, "", ""
	if err := SaveProtocolProfileRevision(&next); err != nil {
		t.Fatal(err)
	}
	stored, err = GetProtocolProfileRevision("copied", 2)
	if err != nil || stored.SchemaVersion != 0 || stored.ContentJSON != "" || stored.ContentDigest != "" || stored.ID != next.ID {
		t.Fatalf("draft update lost zero values: revision=%+v err=%v", stored, err)
	}
	if err := SaveProtocolProfileRevision(&ProtocolProfileRevision{ProfileID: "copied", Revision: 3, ContentJSON: "later"}); err != nil {
		t.Fatal(err)
	}
	if err := SaveProtocolProfileRevision(&next); err != nil {
		t.Fatal(err)
	}
	profile, err := GetProtocolProfile("copied")
	if err != nil || profile.LatestRevision != 3 {
		t.Fatalf("older draft save regressed latest revision: profile=%+v err=%v", profile, err)
	}
}
