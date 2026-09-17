package task

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
)

// MediaDeletionPoller processes durable cleanup jobs after a media capability
// has been revoked. It never changes an asset back to an accessible state.
type MediaDeletionPoller struct {
	Owner      string
	Store      media.ObjectStore
	Workers    int
	Lease      time.Duration
	RetryAfter time.Duration
}

func NewMediaDeletionPoller(owner string, store media.ObjectStore) *MediaDeletionPoller {
	return &MediaDeletionPoller{Owner: owner, Store: store, Workers: 2, RetryAfter: 15 * time.Second}
}

func (p *MediaDeletionPoller) RunOnce(ctx context.Context) (bool, error) {
	if p == nil || p.Store == nil {
		return false, errors.New("media deletion store is required")
	}
	owner := strings.TrimSpace(p.Owner)
	if owner == "" {
		return false, errors.New("media deletion owner is required")
	}
	job, err := db.ClaimMediaDeletionJobContext(ctx, owner, p.lease())
	if errors.Is(err, db.ErrMediaJobUnavailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	asset, err := db.GetMediaAssetByIDContext(ctx, job.AssetID)
	if errors.Is(err, db.ErrMediaAssetNotFound) {
		return p.complete(job)
	}
	if err != nil {
		return p.fail(job, err)
	}
	if asset.Status == db.MediaAssetDeleted {
		return p.complete(job)
	}

	objectID := strings.TrimSpace(asset.ObjectID)
	if objectID == "" {
		objectID = strings.TrimSpace(job.ObjectID)
	}
	if objectID == "" {
		if err := db.CompleteMediaAssetDeleteContext(context.Background(), asset.ID); err != nil {
			return p.fail(job, err)
		}
		return p.complete(job)
	}

	object, err := db.GetMediaObjectByIDContext(ctx, objectID)
	if errors.Is(err, db.ErrMediaAssetNotFound) {
		if err := db.CompleteMediaAssetDeleteContext(context.Background(), asset.ID); err != nil {
			return p.fail(job, err)
		}
		return p.complete(job)
	}
	if err != nil {
		return p.fail(job, err)
	}
	if object.State == db.MediaObjectDeleted || object.RefCount > 0 {
		if err := db.CompleteMediaAssetDeleteContext(context.Background(), asset.ID); err != nil {
			return p.fail(job, err)
		}
		return p.complete(job)
	}

	key := strings.TrimSpace(object.StorageKey)
	if key == "" {
		key = strings.TrimSpace(job.StorageKey)
	}
	if key == "" {
		return p.fail(job, errors.New("media deletion storage key is required"))
	}
	prepared, err := db.PrepareMediaObjectDeletionContext(ctx, objectID)
	if err != nil {
		return p.fail(job, err)
	}
	if !prepared {
		// A concurrent retry may have changed the object while this worker was
		// reading it. Re-read before deciding whether the logical asset can be
		// finalized; an object still being deleted is safe to retry idempotently.
		object, err = db.GetMediaObjectByIDContext(ctx, objectID)
		if err != nil {
			return p.fail(job, err)
		}
		if object.State == db.MediaObjectDeleted || object.RefCount > 0 {
			if err := db.CompleteMediaAssetDeleteContext(context.Background(), asset.ID); err != nil {
				return p.fail(job, err)
			}
			return p.complete(job)
		}
	}
	if err := p.Store.Delete(ctx, key); err != nil && !errors.Is(err, media.ErrNotFound) {
		return p.fail(job, err)
	}
	if err := db.CompleteMediaAssetDeleteContext(context.Background(), asset.ID); err != nil {
		return p.fail(job, err)
	}
	return p.complete(job)
}

func (p *MediaDeletionPoller) complete(job *db.MediaDeletionJob) (bool, error) {
	if job == nil {
		return true, errors.New("media deletion job is required")
	}
	err := db.CompleteMediaDeletionJobForLeaseContext(context.Background(), job.ID, job.LeaseOwner)
	if errors.Is(err, db.ErrMediaJobLeaseOwner) {
		// A newer worker owns the job. Do not let the stale worker report or
		// overwrite state that belongs to that worker.
		return true, nil
	}
	return true, err
}

func (p *MediaDeletionPoller) fail(job *db.MediaDeletionJob, cause error) (bool, error) {
	if cause == nil {
		cause = errors.New("media deletion failed")
	}
	next := time.Now().Add(p.retryAfter())
	if job == nil {
		return true, cause
	}
	err := db.FailMediaDeletionJobForLeaseContext(context.Background(), job.ID, job.LeaseOwner, cause.Error(), next)
	if errors.Is(err, db.ErrMediaJobLeaseOwner) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	if err := db.MarkMediaAssetDeleteFailedContext(context.Background(), job.AssetID, cause.Error(), next); err != nil && !errors.Is(err, db.ErrMediaAssetNotFound) {
		return true, err
	}
	return true, cause
}

func (p *MediaDeletionPoller) Start(ctx context.Context) error {
	if p == nil || p.Store == nil {
		return errors.New("media deletion store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workers := p.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > 16 {
		workers = 16
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				claimed, err := p.RunOnce(ctx)
				if err != nil && !claimed {
					if !waitContext(ctx, time.Second) {
						return
					}
					continue
				}
				if !claimed && !waitContext(ctx, 250*time.Millisecond) {
					return
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (p *MediaDeletionPoller) lease() time.Duration {
	if p.Lease > 0 {
		return p.Lease
	}
	return 5 * time.Minute
}

func (p *MediaDeletionPoller) retryAfter() time.Duration {
	if p.RetryAfter > 0 {
		return p.RetryAfter
	}
	return 15 * time.Second
}

func (p *MediaDeletionPoller) String() string {
	if p == nil {
		return "MediaDeletionPoller(<nil>)"
	}
	return "MediaDeletionPoller(" + strings.TrimSpace(p.Owner) + ")"
}
