package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"gorm.io/gorm"
)

func TestCreateProtocolProfileRevisionPreservesExistingDraft(t *testing.T) {
	if err := InitDB(t.TempDir() + "/create-revision.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "draft", Name: "Draft"}); err != nil {
		t.Fatal(err)
	}
	original := &ProtocolProfileRevision{ProfileID: "draft", Revision: 1, ContentJSON: "original"}
	if err := CreateProtocolProfileRevisionContext(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	replacement := &ProtocolProfileRevision{ProfileID: "draft", Revision: 1, ContentJSON: "replacement"}
	if err := CreateProtocolProfileRevisionContext(context.Background(), replacement); !errors.Is(err, ErrProfileRevisionConflict) {
		t.Fatalf("duplicate create error=%v", err)
	}
	stored, err := GetProtocolProfileRevision("draft", 1)
	if err != nil || stored.ContentJSON != "original" || stored.ID != original.ID {
		t.Fatalf("duplicate create changed draft: %+v, %v", stored, err)
	}
}

func TestCreateProtocolProfileRevisionAllocatesConcurrentNumbers(t *testing.T) {
	if err := InitDB(t.TempDir() + "/concurrent-revisions.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "parallel", Name: "Parallel"}); err != nil {
		t.Fatal(err)
	}
	const count = 24
	start := make(chan struct{})
	results := make(chan *ProtocolProfileRevision, count)
	errors := make(chan error, count)
	var workers sync.WaitGroup
	for i := 0; i < count; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			<-start
			draft := &ProtocolProfileRevision{ProfileID: "parallel", ContentJSON: fmt.Sprintf("draft-%d", i)}
			if err := CreateProtocolProfileRevisionContext(context.Background(), draft); err != nil {
				errors <- err
				return
			}
			results <- draft
		}(i)
	}
	close(start)
	workers.Wait()
	close(errors)
	close(results)
	for err := range errors {
		t.Errorf("concurrent create: %v", err)
	}
	seen := make(map[int]bool)
	for draft := range results {
		if draft.Revision < 1 || draft.Revision > count || seen[draft.Revision] {
			t.Errorf("duplicate/invalid revision: %+v", draft)
		}
		seen[draft.Revision] = true
		stored, err := GetProtocolProfileRevision("parallel", draft.Revision)
		if err != nil || stored.ContentJSON != draft.ContentJSON {
			t.Errorf("created draft was lost: %+v, %v", stored, err)
		}
	}
	profile, err := GetProtocolProfile("parallel")
	if err != nil || len(seen) != count || profile.LatestRevision != count {
		t.Fatalf("revision allocation count=%d profile=%+v err=%v", len(seen), profile, err)
	}
}

func TestCreateProtocolProfileRevisionRollsBackFailedAllocation(t *testing.T) {
	if err := InitDB(t.TempDir() + "/failed-revision.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfileWithInitialRevisionContext(context.Background(), &ProtocolProfile{ID: "atomic"}, &ProtocolProfileRevision{ProfileID: "atomic", Revision: 1}); err != nil {
		t.Fatal(err)
	}
	if err := DB.Exec(`CREATE TRIGGER reject_draft BEFORE INSERT ON protocol_profile_revisions WHEN NEW.content_digest = 'reject' BEGIN SELECT RAISE(ABORT, 'rejected draft'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Transaction(func(tx *gorm.DB) error {
		ctx := WithTx(context.Background(), tx)
		rejected := &ProtocolProfileRevision{ProfileID: "atomic", ContentDigest: "reject"}
		if err := CreateProtocolProfileRevisionContext(ctx, rejected); err == nil || rejected.Revision != 0 {
			return fmt.Errorf("failed create error=%v revision=%d", err, rejected.Revision)
		}
		accepted := &ProtocolProfileRevision{ProfileID: "atomic", ContentJSON: "accepted"}
		if err := CreateProtocolProfileRevisionContext(ctx, accepted); err != nil {
			return err
		}
		if accepted.Revision != 2 {
			return fmt.Errorf("failed insert consumed revision: %d", accepted.Revision)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateProtocolProfileRevisionRollsBackCanceledAllocation(t *testing.T) {
	if err := InitDB(t.TempDir() + "/canceled-revision.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "cancel"}); err != nil {
		t.Fatal(err)
	}
	if err := DB.Transaction(func(owner *gorm.DB) error {
		ctx, cancel := context.WithCancel(WithTx(context.Background(), owner))
		defer cancel()
		const callback = "test:cancel_revision_insert"
		if err := DB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
			if tx.Statement.Table == "protocol_profile_revisions" {
				cancel()
			}
		}); err != nil {
			return err
		}
		draft := &ProtocolProfileRevision{ProfileID: "cancel"}
		err := CreateProtocolProfileRevisionContext(ctx, draft)
		_ = DB.Callback().Create().Remove(callback)
		if !errors.Is(err, context.Canceled) || draft.Revision != 0 {
			return fmt.Errorf("canceled create error=%v draft=%+v", err, draft)
		}
		// The owner can recover and keep using its transaction without leaking
		// the canceled operation's revision allocation.
		accepted := &ProtocolProfileRevision{ProfileID: "cancel"}
		if err := CreateProtocolProfileRevisionContext(WithTx(context.Background(), owner), accepted); err != nil {
			return err
		}
		if accepted.Revision != 1 {
			return fmt.Errorf("canceled create consumed revision %d", accepted.Revision)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateProtocolProfileWithInitialRevisionRollsBackNestedFailure(t *testing.T) {
	if err := InitDB(t.TempDir() + "/nested-profile.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := DB.Exec(`CREATE TRIGGER reject_initial BEFORE INSERT ON protocol_profile_revisions BEGIN SELECT RAISE(ABORT, 'rejected draft'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Transaction(func(owner *gorm.DB) error {
		err := CreateProtocolProfileWithInitialRevisionContext(WithTx(context.Background(), owner), &ProtocolProfile{ID: "rejected"}, &ProtocolProfileRevision{ProfileID: "rejected"})
		if err == nil {
			return errors.New("initial revision should fail")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := GetProtocolProfile("rejected"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("failed initial revision left a profile: %v", err)
	}
}
