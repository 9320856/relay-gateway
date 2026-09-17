package db

import (
	"errors"
	"testing"
	"time"
)

func TestMaterializationRetryRequeuesAndPreservesActiveLease(t *testing.T) {
	if err := InitDB(t.TempDir() + "/retry.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	asset := &MediaAsset{PublicID: "retry", CapabilityHash: "hash", Kind: "image", Status: MediaAssetFailed}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := RetryMediaAssetMaterialization(asset.ID); err != nil {
		t.Fatal(err)
	}
	var job MediaMaterializationJob
	if err := DB.Where("asset_id = ?", asset.ID).First(&job).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != MediaJobPending {
		t.Fatalf("missing queued job: %#v", job)
	}
	future := time.Now().Add(time.Hour)
	if err := DB.Model(&job).Updates(map[string]any{"status": MediaJobFailed, "next_attempt_at": future, "deadline_at": time.Now().Add(-time.Hour)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := RetryMediaAssetMaterialization(asset.ID); err != nil {
		t.Fatal(err)
	}
	job = MediaMaterializationJob{}
	if err := DB.Where("asset_id = ?", asset.ID).First(&job).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != MediaJobPending || job.NextAttemptAt != nil || job.DeadlineAt != nil {
		t.Fatalf("retry remains blocked: %#v", job)
	}
	if err := DB.Model(&job).Updates(map[string]any{"status": MediaJobRunning, "lease_owner": "worker", "lease_expires_at": future}).Error; err != nil {
		t.Fatal(err)
	}
	if err := RetryMediaAssetMaterialization(asset.ID); err != nil {
		t.Fatal(err)
	}
	job = MediaMaterializationJob{}
	if err := DB.Where("asset_id = ?", asset.ID).First(&job).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != MediaJobRunning || job.LeaseOwner != "worker" {
		t.Fatalf("active worker lease was stolen: %#v", job)
	}
	if err := DB.Model(asset).Update("status", MediaAssetDeleteRequested).Error; err != nil {
		t.Fatal(err)
	}
	if err := RetryMediaAssetMaterialization(asset.ID); !errors.Is(err, ErrMediaAssetNotFound) {
		t.Fatalf("revoked asset accepted retry: %v", err)
	}
}

func TestDeletionRetryPreservesActiveLease(t *testing.T) {
	if err := InitDB(t.TempDir() + "/delete-retry.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	asset := &MediaAsset{PublicID: "delete-retry", CapabilityHash: "hash", Kind: "image", Status: MediaAssetDeleteRequested}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour)
	job := &MediaDeletionJob{AssetID: asset.ID, Status: MediaJobRunning, LeaseOwner: "worker", LeaseExpiresAt: &future}
	if err := CreateMediaDeletionJob(job); err != nil {
		t.Fatal(err)
	}
	if err := RetryMediaAssetDeletion(asset.ID); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaDeletionJob(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != MediaJobRunning || loaded.LeaseOwner != "worker" {
		t.Fatalf("active deletion lease changed: %#v", loaded)
	}
}
