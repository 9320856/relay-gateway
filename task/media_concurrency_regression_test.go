package task

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
)

func TestSynchronousMaterializationKeepsExclusiveLeaseWhileFetching(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/sync-lease.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	asset := &db.MediaAsset{PublicID: "sync-lease", CapabilityHash: "hash", Kind: "image", SourceKind: media.SourceURL, SourceLocator: "https://provider.example/image.png"}
	const lease = 300 * time.Millisecond
	job, err := db.CreateMediaAssetForMaterializationContext(context.Background(), asset, "sync", lease)
	if err != nil {
		t.Fatal(err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer close(release)
	go func() {
		_, err := MaterializeClaimedMedia(ctx, job, asset, store, media.SourceFetcherFunc(func(ctx context.Context, _ media.MediaResult) (media.FetchedSource, error) {
			close(entered)
			select {
			case <-release:
				return media.FetchedSource{Body: io.NopCloser(strings.NewReader("image")), ContentType: "image/png"}, nil
			case <-ctx.Done():
				return media.FetchedSource{}, ctx.Err()
			}
		}), lease)
		done <- err
	}()
	<-entered
	// Wait for a real heartbeat in SQL rather than assuming a scheduled sleep.
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, err := db.GetMediaMaterializationJob(asset.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.LeaseExpiresAt.After(*job.LeaseExpiresAt) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease was not renewed during blocked download")
		}
		time.Sleep(10 * time.Millisecond)
	}
	claimed, err := NewMediaMaterializationPoller("background", store).RunOnce(context.Background())
	if err != nil || claimed {
		t.Fatalf("background claimed sync download: claimed=%v err=%v", claimed, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("download did not stop")
	}
}

func TestExpiredMaterializationCannotOverwriteReplacementObject(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/expired-lease.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	asset := &db.MediaAsset{PublicID: "expired-lease", CapabilityHash: "hash", Kind: "image", SourceKind: media.SourceURL, SourceLocator: "https://provider.example/image.png"}
	job, err := db.CreateMediaAssetForMaterializationContext(context.Background(), asset, "same-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := MaterializeClaimedMedia(context.Background(), job, asset, store, media.SourceFetcherFunc(func(context.Context, media.MediaResult) (media.FetchedSource, error) {
			close(entered)
			<-release
			return media.FetchedSource{Body: io.NopCloser(strings.NewReader("stale bytes")), ContentType: "image/png"}, nil
		}), time.Minute)
		done <- err
	}()
	<-entered
	if err := db.DB.Model(&db.MediaMaterializationJob{}).Where("id = ?", job.ID).Update("lease_expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		close(release)
		t.Fatal(err)
	}
	replacement, err := db.ClaimMediaMaterializationJob("same-worker", time.Minute)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	if replacement.LeaseOwner == job.LeaseOwner {
		close(release)
		t.Fatal("claim reused owner fencing token")
	}
	info, err := MaterializeClaimedMedia(context.Background(), replacement, asset, store, media.SourceFetcherFunc(func(context.Context, media.MediaResult) (media.FetchedSource, error) {
		return media.FetchedSource{Body: io.NopCloser(strings.NewReader("replacement bytes")), ContentType: "image/png"}, nil
	}), time.Minute)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, db.ErrMediaJobLeaseOwner) {
		t.Fatalf("old claim committed: %v", err)
	}
	current, err := db.GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	object, err := db.GetMediaObjectByID(current.ObjectID)
	if err != nil || object.StorageKey != info.Key {
		t.Fatalf("replacement metadata changed: %#v %v", object, err)
	}
	body, _, err := store.Open(context.Background(), object.StorageKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	bytes, err := io.ReadAll(body)
	if err != nil || string(bytes) != "replacement bytes" {
		t.Fatalf("replacement file overwritten: %q %v", bytes, err)
	}
}

func TestAdministratorPurgeRetriesBeforeRemovingMetadata(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/purge-retry.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	base, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := base.PutAtomic(context.Background(), strings.NewReader("keep until deleted"), media.PutMeta{Key: "image/purge.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	object := &db.MediaObject{ID: "purge-object", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady}
	if err := db.CreateMediaObject(object); err != nil {
		t.Fatal(err)
	}
	asset := &db.MediaAsset{PublicID: "purge-retry", CapabilityHash: "hash", Kind: "image", Status: db.MediaAssetAvailable, ObjectID: object.ID}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RequestMediaAssetPurgeContext(context.Background(), asset.ID); err != nil {
		t.Fatal(err)
	}
	store := &deletionTestStore{ObjectStore: base, deleteErr: errors.New("file is busy")}
	poller := NewMediaDeletionPoller("purge", store)
	if claimed, err := poller.RunOnce(context.Background()); !claimed || err == nil {
		t.Fatalf("deletion failure was hidden: %v %v", claimed, err)
	}
	current, err := db.GetMediaAssetByID(asset.ID)
	if err != nil || current.Status != db.MediaAssetDeleteFailed {
		t.Fatalf("failed purge lost revoked metadata: %#v %v", current, err)
	}
	job, err := db.GetMediaDeletionJob(asset.ID)
	if err != nil || !job.PurgeAfterDelete || job.Status != db.MediaJobPending {
		t.Fatalf("purge retry not durable: %#v %v", job, err)
	}
	body, _, err := base.Open(context.Background(), info.Key, nil)
	if err != nil {
		t.Fatal(err)
	}
	bytes, readErr := io.ReadAll(body)
	_ = body.Close()
	if readErr != nil || string(bytes) != "keep until deleted" {
		t.Fatalf("file changed on failed deletion: %q %v", bytes, readErr)
	}
	if err := db.RetryMediaAssetDeletion(asset.ID); err != nil {
		t.Fatal(err)
	}
	store.deleteErr = nil
	if claimed, err := poller.RunOnce(context.Background()); !claimed || err != nil {
		t.Fatalf("purge retry failed: %v %v", claimed, err)
	}
	if _, err := db.GetMediaAssetByID(asset.ID); !errors.Is(err, db.ErrMediaAssetNotFound) {
		t.Fatalf("purged asset retained: %v", err)
	}
	if _, err := db.GetMediaObjectByID(object.ID); !errors.Is(err, db.ErrMediaAssetNotFound) {
		t.Fatalf("purged object metadata retained: %v", err)
	}
	if _, err := base.Stat(context.Background(), info.Key); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("purged file retained: %v", err)
	}
}

func TestAdministratorPurgeKeepsSharedObjectAndDoesNotReviveRevokedReferences(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/purge-shared.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.PutAtomic(context.Background(), strings.NewReader("shared"), media.PutMeta{Key: "image/shared.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	object := &db.MediaObject{ID: "purge-shared", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady, RefCount: 3}
	if err := db.CreateMediaObject(object); err != nil {
		t.Fatal(err)
	}
	assets := make([]*db.MediaAsset, 3)
	for i, id := range []string{"shared-first", "shared-second", "shared-third"} {
		assets[i] = &db.MediaAsset{PublicID: id, CapabilityHash: id, TaskRunID: id, Kind: "image", Status: db.MediaAssetAvailable, ObjectID: object.ID}
		if err := db.CreateMediaAsset(assets[i]); err != nil {
			t.Fatal(err)
		}
	}
	poller := NewMediaDeletionPoller("shared-purge", store)
	if _, err := db.RequestMediaAssetPurgeContext(context.Background(), assets[0].ID); err != nil {
		t.Fatal(err)
	}
	if claimed, err := poller.RunOnce(context.Background()); !claimed || err != nil {
		t.Fatalf("first purge: %v %v", claimed, err)
	}
	if _, err := store.Stat(context.Background(), info.Key); err != nil {
		t.Fatalf("shared file prematurely deleted: %v", err)
	}
	for _, asset := range assets[1:] {
		if _, err := db.RequestMediaAssetPurgeContext(context.Background(), asset.ID); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if claimed, err := poller.RunOnce(context.Background()); !claimed || err != nil {
			t.Fatalf("revoked shared purge: %v %v", claimed, err)
		}
	}
	if _, err := store.Stat(context.Background(), info.Key); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("shared file survived final purge: %v", err)
	}
	if _, err := db.GetMediaObjectByID(object.ID); !errors.Is(err, db.ErrMediaAssetNotFound) {
		t.Fatalf("shared object metadata survived final purge: %v", err)
	}
}
