package db

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

// CreateMediaAssetForMaterializationContext makes the asset and its exclusive
// first lease visible together. A synchronous caller must never publish a
// pending job before it starts consuming the provider response.
func CreateMediaAssetForMaterializationContext(ctx context.Context, asset *MediaAsset, owner string, lease time.Duration) (*MediaMaterializationJob, error) {
	conn, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	owner, err = mediaMaterializationOwner(owner)
	if err != nil {
		return nil, err
	}
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	expires := time.Now().Add(lease)
	job := &MediaMaterializationJob{Status: MediaJobRunning, LeaseOwner: owner, LeaseExpiresAt: &expires, Attempts: 1}
	err = conn.Transaction(func(tx *gorm.DB) error {
		txCtx := WithTx(ctx, tx)
		if err := CreateMediaAssetContext(txCtx, asset); err != nil {
			return err
		}
		job.AssetID = asset.ID
		if err := tx.Create(job).Error; err != nil {
			return err
		}
		return tx.Model(asset).Updates(map[string]any{"status": MediaAssetMaterializing, "state_version": gorm.Expr("state_version + 1")}).Error
	})
	return job, err
}

// ClaimMediaAssetMaterializationContext claims one existing logical asset.
// Synchronous batches use the same due-job and lease rules as background
// workers, so retries reuse assets and cannot download another worker's job.
func ClaimMediaAssetMaterializationContext(ctx context.Context, assetID uint, owner string, lease time.Duration) (*MediaMaterializationJob, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	owner, err = mediaMaterializationOwner(owner)
	if err != nil {
		return nil, err
	}
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	now := time.Now()
	expires := now.Add(lease)
	var job MediaMaterializationJob
	// Let an existing transaction's live context manage its savepoint, even
	// when the child operation is canceled after the job update. Business SQL
	// still inherits the child's cancellation through the callback session.
	if HasContextTransaction(ctx) {
		conn = DBForContext(ctx)
	}
	err = conn.Transaction(func(tx *gorm.DB) error {
		tx = tx.WithContext(ctx)
		if err := tx.Where("asset_id = ?", assetID).First(&job).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrMediaJobUnavailable
			}
			return err
		}
		result := tx.Model(&job).
			Where("status IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?) AND (lease_expires_at IS NULL OR lease_expires_at <= ?)", []string{MediaJobPending, MediaJobRunning, MediaJobFailed}, now, now).
			Where("EXISTS (SELECT 1 FROM media_assets WHERE id = ? AND status IN ?)", assetID, []string{MediaAssetPending, MediaAssetMaterializing, MediaAssetFailed}).
			Updates(map[string]any{"status": MediaJobRunning, "lease_owner": owner, "lease_expires_at": expires, "attempts": gorm.Expr("attempts + 1"), "updated_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrMediaJobUnavailable
		}
		return tx.Model(&MediaAsset{}).Where("id = ?", assetID).
			Updates(map[string]any{"status": MediaAssetMaterializing, "state_version": gorm.Expr("state_version + 1"), "updated_at": now}).Error
	})
	if err != nil {
		return nil, err
	}
	job.Status, job.LeaseOwner, job.LeaseExpiresAt = MediaJobRunning, owner, &expires
	job.Attempts++
	return &job, nil
}

func mediaMaterializationOwner(owner string) (string, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "", errors.New("media materialization owner is required")
	}
	token, err := newMediaToken(18)
	if err != nil {
		return "", err
	}
	if len(owner) > 90 {
		owner = owner[:90]
	}
	return owner + ":" + token, nil
}

func RenewMediaMaterializationLeaseContext(ctx context.Context, job *MediaMaterializationJob, lease time.Duration) error {
	conn, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if job == nil || lease <= 0 {
		return ErrMediaJobLeaseOwner
	}
	now := time.Now()
	result := conn.Model(&MediaMaterializationJob{}).
		Where("id = ? AND status = ? AND lease_owner = ? AND lease_expires_at > ?", job.ID, MediaJobRunning, job.LeaseOwner, now).
		Update("lease_expires_at", now.Add(lease))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrMediaJobLeaseOwner
	}
	return nil
}

func RenewMediaDeletionLeaseContext(ctx context.Context, job *MediaDeletionJob, lease time.Duration) error {
	conn, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if job == nil || lease <= 0 {
		return ErrMediaJobLeaseOwner
	}
	now := time.Now()
	result := conn.Model(&MediaDeletionJob{}).Where("id = ? AND status = ? AND lease_owner = ? AND lease_expires_at > ?", job.ID, MediaJobRunning, job.LeaseOwner, now).Update("lease_expires_at", now.Add(lease))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrMediaJobLeaseOwner
	}
	return nil
}

// CommitMediaMaterializationContext fences every metadata mutation and job
// completion in one transaction. The object key belongs to this lease alone,
// so an expired worker cannot overwrite the replacement worker's file either.
func CommitMediaMaterializationContext(ctx context.Context, job *MediaMaterializationJob, object *MediaObject) error {
	conn, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if job == nil || object == nil {
		return ErrMediaJobLeaseOwner
	}
	return conn.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&MediaMaterializationJob{}).
			Where("id = ? AND status = ? AND lease_owner = ? AND lease_expires_at > ?", job.ID, MediaJobRunning, job.LeaseOwner, time.Now()).
			Updates(map[string]any{"status": MediaJobSucceeded, "lease_owner": "", "lease_expires_at": nil, "last_error": "", "updated_at": time.Now()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrMediaJobLeaseOwner
		}
		txCtx := WithTx(ctx, tx)
		if err := CreateMediaObjectContext(txCtx, object); err != nil {
			return err
		}
		if err := MarkMediaAssetAvailableContext(txCtx, job.AssetID, object.ID, object.ContentType, object.SHA256, object.ByteSize); err != nil {
			return err
		}
		asset, err := GetMediaAssetByIDContext(txCtx, job.AssetID)
		if err != nil {
			return err
		}
		if strings.TrimSpace(asset.TaskRunID) != "" {
			// Task completion belongs to the same commit. A canceled response or
			// process exit cannot leave a succeeded media job with its task stuck
			// materializing. Imported assets may have no remaining TaskRun row.
			if err := CompleteTaskRunAfterMediaContext(txCtx, asset.TaskRunID, asset.Kind); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		return nil
	})
}

func FailMediaMaterializationWithAssetForLeaseContext(ctx context.Context, job *MediaMaterializationJob, failure string, next time.Time) error {
	conn, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if job == nil {
		return ErrMediaJobLeaseOwner
	}
	return conn.Transaction(func(tx *gorm.DB) error {
		txCtx := WithTx(ctx, tx)
		if err := FailMediaMaterializationJobForLeaseContext(txCtx, job.ID, job.LeaseOwner, failure, next); err != nil {
			return err
		}
		return MarkMediaAssetFailedContext(txCtx, job.AssetID, failure)
	})
}
