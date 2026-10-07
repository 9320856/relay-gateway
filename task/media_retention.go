package task

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
	"relay-gateway/db"
)

// MediaRetentionScanResult describes one idempotent retention scan.
type MediaRetentionScanResult struct {
	Scanned int
	Expired int
}

// MediaRetentionScanner marks expired assets as revoked and queues the
// existing local deletion job. It never deletes objects or contacts upstreams.
type MediaRetentionScanner struct {
	Now  func() time.Time
	Page int
}

func (s *MediaRetentionScanner) Scan(ctx context.Context) (MediaRetentionScanResult, error) {
	var result MediaRetentionScanResult
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	if s != nil && s.Now != nil {
		now = s.Now()
	}
	page := 200
	if s != nil && s.Page > 0 && s.Page <= 1000 {
		page = s.Page
	}
	g := db.SQLDBForContext(ctx)
	if g == nil {
		return result, errors.New("database is not initialized")
	}
	var assets []db.MediaAsset
	if err := g.Where("expires_at IS NOT NULL AND expires_at <= ?", now).
		Where("status IN ?", []string{db.MediaAssetPending, db.MediaAssetMaterializing, db.MediaAssetAvailable, db.MediaAssetFailed}).
		Order("id ASC").Limit(page).Find(&assets).Error; err != nil {
		return result, err
	}
	result.Scanned = len(assets)
	for i := range assets {
		expired, err := expireMediaAsset(ctx, assets[i].ID, now)
		if err != nil {
			return result, err
		}
		if expired {
			result.Expired++
		}
	}
	return result, nil
}

// ScanExpiredMediaAssets is a convenience entry point for scheduled workers.
func ScanExpiredMediaAssets(ctx context.Context) (MediaRetentionScanResult, error) {
	return (&MediaRetentionScanner{}).Scan(ctx)
}

func expireMediaAsset(ctx context.Context, assetID uint, now time.Time) (bool, error) {
	g := db.SQLDBForContext(ctx)
	expired := false
	err := g.Transaction(func(tx *gorm.DB) error {
		var asset db.MediaAsset
		if err := tx.First(&asset, "id = ?", assetID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if asset.ExpiresAt == nil || asset.ExpiresAt.After(now) || !retentionEligible(asset.Status) {
			return nil
		}
		expired = true
		// The state transition and refcount decrement share one transaction, so
		// a repeated scan cannot decrement a shared object twice.
		if asset.ObjectID != "" {
			var object db.MediaObject
			err := tx.First(&object, "id = ?", strings.TrimSpace(asset.ObjectID)).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if err == nil && object.State != db.MediaObjectDeleted && object.RefCount > 0 {
				state := db.MediaObjectDeletePending
				if object.RefCount > 1 {
					state = db.MediaObjectReady
				}
				if err := tx.Model(&object).Updates(map[string]any{
					"ref_count": gorm.Expr("CASE WHEN ref_count > 0 THEN ref_count - 1 ELSE 0 END"),
					"state":     state, "next_retry_at": nil, "last_error": "", "updated_at": now,
				}).Error; err != nil {
					return err
				}
			}
		}
		job := &db.MediaDeletionJob{AssetID: asset.ID, ObjectID: asset.ObjectID, Status: db.MediaJobPending}
		if asset.ObjectID != "" {
			var object db.MediaObject
			if err := tx.First(&object, "id = ?", asset.ObjectID).Error; err == nil {
				job.StorageKey = object.StorageKey
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		if err := tx.Where("asset_id = ?", asset.ID).FirstOrCreate(job).Error; err != nil {
			return err
		}
		return tx.Model(&asset).Updates(map[string]any{
			"status": db.MediaAssetExpired, "delete_requested_at": now,
			"delete_reason": "retention expired", "state_version": asset.StateVersion + 1,
			"updated_at": now,
		}).Error
	})
	return expired, err
}

func retentionEligible(status string) bool {
	return status == db.MediaAssetPending || status == db.MediaAssetMaterializing || status == db.MediaAssetAvailable || status == db.MediaAssetFailed
}
