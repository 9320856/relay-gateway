package db

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// RequestMediaAssetPurgeContext revokes reads and durably records physical
// cleanup before any metadata is removed. Purging happens only when the
// deletion worker has successfully removed the unshared object.
func RequestMediaAssetPurgeContext(ctx context.Context, id uint) (string, error) {
	conn, err := mediaDB(ctx)
	if err != nil {
		return "", err
	}
	status := MediaAssetDeleteRequested
	err = conn.Transaction(func(tx *gorm.DB) error {
		txCtx := WithTx(ctx, tx)
		asset, err := GetMediaAssetByIDContext(txCtx, id)
		if errors.Is(err, ErrMediaAssetNotFound) {
			status = MediaAssetDeleted
			return nil
		}
		if err != nil {
			return err
		}
		if asset.Status == MediaAssetDeleted {
			status = MediaAssetDeleted
			_, _, err := HardDeleteMediaAssetByIDContext(txCtx, id)
			return err
		}
		if err := RequestMediaAssetDeleteContext(txCtx, asset.PublicID, "deleted by administrator"); err != nil {
			return err
		}
		return tx.Model(&MediaDeletionJob{}).Where("asset_id = ?", id).Update("purge_after_delete", true).Error
	})
	return status, err
}
