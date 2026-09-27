package task

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
)

// MaterializeClaimedMedia is shared by synchronous HTTP responses and durable
// workers. It renews an exclusive claim while streaming, uses an immutable key
// per claim, and commits the object, asset and job under the same lease fence.
func MaterializeClaimedMedia(ctx context.Context, job *db.MediaMaterializationJob, asset *db.MediaAsset, store media.ObjectStore, fetcher media.SourceFetcher, lease time.Duration) (media.ObjectInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if job == nil || asset == nil || job.AssetID != asset.ID {
		return media.ObjectInfo{}, db.ErrMediaJobLeaseOwner
	}
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	workCtx, stopLease := keepLeaseAlive(ctx, lease, func(renewCtx context.Context) error {
		return db.RenewMediaMaterializationLeaseContext(renewCtx, job, lease)
	})
	defer stopLease()
	claimHash := sha256.Sum256([]byte(job.LeaseOwner))
	publicKey := fmt.Sprintf("%s-%d-%d-%x", asset.PublicID, job.ID, job.Attempts, claimHash[:8])
	result := media.MediaResult{SourceKind: asset.SourceKind, Locator: asset.SourceLocator, ContentType: asset.ContentType, MaxBytes: defaultMediaMaterializationMaxBytes}
	info, err := (&media.MaterializationWorker{Store: store, Fetcher: fetcher}).Materialize(workCtx, result, media.ProfileObjectKey(publicKey, asset.Kind, result.ContentType))
	if err != nil {
		return media.ObjectInfo{}, err
	}
	object := &db.MediaObject{ID: mediaObjectID(info.Key, info.SHA256), Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, ETag: info.SHA256, State: db.MediaObjectReady}
	if err := db.CommitMediaMaterializationContext(workCtx, job, object); err != nil {
		// No committed asset can reference this claim's private file. Delete it
		// without inheriting request cancellation; reconciliation is a fallback.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = store.Delete(cleanupCtx, info.Key)
		return media.ObjectInfo{}, err
	}
	return info, nil
}
