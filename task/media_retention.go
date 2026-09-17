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
	g := db.DBForContext(ctx)
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
	g := db.DBForContext(ctx)
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

// MediaCapacityStats is a read-only snapshot of locally managed media.
type MediaCapacityStats struct {
	TotalObjectBytes         int64
	ReadyObjectBytes         int64
	StagingObjectBytes       int64
	DeletePendingObjectBytes int64
	AssetCount               int64
	ActiveAssetCount         int64
}

func GetMediaCapacityStatsContext(ctx context.Context) (MediaCapacityStats, error) {
	var out MediaCapacityStats
	g := db.DBForContext(ctx)
	if g == nil {
		return out, errors.New("database is not initialized")
	}
	if err := g.Model(&db.MediaObject{}).Where("state <> ?", db.MediaObjectDeleted).Select("COALESCE(SUM(byte_size), 0)").Scan(&out.TotalObjectBytes).Error; err != nil {
		return out, err
	}
	for state, target := range map[string]*int64{db.MediaObjectReady: &out.ReadyObjectBytes, db.MediaObjectStaging: &out.StagingObjectBytes, db.MediaObjectDeletePending: &out.DeletePendingObjectBytes} {
		if err := g.Model(&db.MediaObject{}).Where("state = ?", state).Select("COALESCE(SUM(byte_size), 0)").Scan(target).Error; err != nil {
			return out, err
		}
	}
	if err := g.Model(&db.MediaAsset{}).Count(&out.AssetCount).Error; err != nil {
		return out, err
	}
	if err := g.Model(&db.MediaAsset{}).Where("status NOT IN ?", []string{db.MediaAssetDeleted, db.MediaAssetExpired, db.MediaAssetDeleteRequested, db.MediaAssetDeleteFailed}).Count(&out.ActiveAssetCount).Error; err != nil {
		return out, err
	}
	return out, nil
}

func GetMediaCapacityStats() (MediaCapacityStats, error) {
	return GetMediaCapacityStatsContext(context.Background())
}

// MediaCapacityPolicy controls admission checks without coupling storage to a
// particular filesystem implementation. FreeBytes is optional and injectable.
type MediaCapacityPolicy struct {
	MaxObjectBytes int64
	MinFreeBytes   int64
	FreeBytes      func() (int64, error)
}

type MediaCapacityCheck struct {
	Stats     MediaCapacityStats
	FreeBytes int64
	Allowed   bool
	Reasons   []string
}

func CheckMediaCapacityContext(ctx context.Context, policy MediaCapacityPolicy) (MediaCapacityCheck, error) {
	stats, err := GetMediaCapacityStatsContext(ctx)
	if err != nil {
		return MediaCapacityCheck{}, err
	}
	out := MediaCapacityCheck{Stats: stats, Allowed: true}
	if policy.MaxObjectBytes > 0 && stats.TotalObjectBytes >= policy.MaxObjectBytes {
		out.Allowed = false
		out.Reasons = append(out.Reasons, "object capacity limit reached")
	}
	if policy.FreeBytes != nil {
		free, callErr := policy.FreeBytes()
		if callErr != nil {
			return out, callErr
		}
		out.FreeBytes = free
		if policy.MinFreeBytes > 0 && free < policy.MinFreeBytes {
			out.Allowed = false
			out.Reasons = append(out.Reasons, "minimum free space not available")
		}
	}
	return out, nil
}

func CheckMediaCapacity(policy MediaCapacityPolicy) (MediaCapacityCheck, error) {
	return CheckMediaCapacityContext(context.Background(), policy)
}
