package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestMediaAssetMaterializationClaimCancellationRollsBackCallerSavepoint(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-claim-savepoint.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	asset := &MediaAsset{PublicID: "savepoint-claim", CapabilityHash: "hash", Kind: "image", Status: MediaAssetPending}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := CreateMediaMaterializationJob(&MediaMaterializationJob{AssetID: asset.ID, Status: MediaJobPending}); err != nil {
		t.Fatal(err)
	}
	const callbackName = "test:cancel_media_claim_after_job_update"
	var cancelChild context.CancelFunc
	jobUpdated := false
	if err := DB.Callback().Update().After("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == "media_materialization_jobs" && tx.Error == nil && tx.RowsAffected == 1 {
			jobUpdated = true
			cancelChild()
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = DB.Callback().Update().Remove(callbackName) })
	if err := DB.Transaction(func(ownerTx *gorm.DB) error {
		ownerCtx := WithTx(context.Background(), ownerTx)
		childCtx, cancel := context.WithCancel(ownerCtx)
		cancelChild = cancel
		defer cancel()
		_, err := ClaimMediaAssetMaterializationContext(childCtx, asset.ID, "sync", time.Minute)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("canceled claim error = %v, want context.Canceled", err)
		}
		// The owner handles the operation's failure and commits unrelated work.
		return ownerTx.Model(&MediaAsset{}).Where("id = ?", asset.ID).Update("display_name", "owner committed").Error
	}); err != nil {
		t.Fatal(err)
	}
	if !jobUpdated {
		t.Fatal("test did not cancel between the job and asset updates")
	}
	job, err := GetMediaMaterializationJob(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != MediaJobPending || job.Attempts != 0 || job.LeaseOwner != "" || job.LeaseExpiresAt != nil || loaded.Status != MediaAssetPending || loaded.StateVersion != 0 || loaded.DisplayName != "owner committed" {
		t.Fatalf("failed claim escaped its savepoint: job=%+v asset=%+v", job, loaded)
	}
}
