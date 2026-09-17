package task

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
)

type deletionTestStore struct {
	media.ObjectStore
	deleteErr error
	deleted   []string
}

func (s *deletionTestStore) Delete(ctx context.Context, key string) error {
	s.deleted = append(s.deleted, key)
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.ObjectStore.Delete(ctx, key)
}

func TestMediaDeletionPollerDeletesObjectAndCompletesJob(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-delete-poller.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.PutAtomic(context.Background(), strings.NewReader("delete-me"), media.PutMeta{Key: "video/delete.mp4", ContentType: "video/mp4"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaObject(&db.MediaObject{ID: "obj-poller-delete", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady, RefCount: 1}); err != nil {
		t.Fatal(err)
	}
	asset := &db.MediaAsset{PublicID: "poller-delete", CapabilityHash: "hash", Kind: "video", Status: db.MediaAssetAvailable, ObjectID: "obj-poller-delete"}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := db.RequestMediaAssetDelete(asset.PublicID, "test"); err != nil {
		t.Fatal(err)
	}
	poller := NewMediaDeletionPoller("delete-test", store)
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("run once claimed=%v err=%v", claimed, err)
	}
	if _, err := store.Stat(context.Background(), info.Key); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("deleted object stat error = %v", err)
	}
	loaded, err := db.GetMediaAssetByID(asset.ID)
	if err != nil || loaded.Status != db.MediaAssetDeleted {
		t.Fatalf("deleted asset = %#v, %v", loaded, err)
	}
	object, err := db.GetMediaObjectByID(asset.ObjectID)
	if err != nil || object.State != db.MediaObjectDeleted {
		t.Fatalf("deleted object = %#v, %v", object, err)
	}
	job, err := db.GetMediaDeletionJob(asset.ID)
	if err != nil || job.Status != db.MediaJobSucceeded {
		t.Fatalf("completed deletion job = %#v, %v", job, err)
	}
}

func TestMediaDeletionPollerKeepsSharedObjectUntilLastAsset(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-delete-shared-poller.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.PutAtomic(context.Background(), strings.NewReader("shared-delete"), media.PutMeta{Key: "video/shared-delete.mp4", ContentType: "video/mp4"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaObject(&db.MediaObject{ID: "obj-poller-shared", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady, RefCount: 2}); err != nil {
		t.Fatal(err)
	}
	first := &db.MediaAsset{PublicID: "poller-shared-first", CapabilityHash: "poller-shared-first-hash", TaskRunID: "poller-shared-run-1", Kind: "video", Ordinal: 0, Status: db.MediaAssetAvailable, ObjectID: "obj-poller-shared"}
	second := &db.MediaAsset{PublicID: "poller-shared-second", CapabilityHash: "poller-shared-second-hash", TaskRunID: "poller-shared-run-2", Kind: "video", Ordinal: 0, Status: db.MediaAssetAvailable, ObjectID: "obj-poller-shared"}
	if err := db.CreateMediaAsset(first); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaAsset(second); err != nil {
		t.Fatal(err)
	}
	if err := db.RequestMediaAssetDelete(first.PublicID, "test"); err != nil {
		t.Fatal(err)
	}
	poller := NewMediaDeletionPoller("delete-shared-test", store)
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("first run claimed=%v err=%v", claimed, err)
	}
	if _, err := store.Stat(context.Background(), info.Key); err != nil {
		t.Fatalf("shared object was deleted while still referenced: %v", err)
	}
	object, err := db.GetMediaObjectByID(first.ObjectID)
	if err != nil || object.RefCount != 1 || object.State != db.MediaObjectReady {
		t.Fatalf("shared object after first deletion = %#v, %v", object, err)
	}

	if err := db.RequestMediaAssetDelete(second.PublicID, "test"); err != nil {
		t.Fatal(err)
	}
	claimed, err = poller.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("last run claimed=%v err=%v", claimed, err)
	}
	if _, err := store.Stat(context.Background(), info.Key); !errors.Is(err, media.ErrNotFound) {
		t.Fatalf("last referenced object was not deleted: %v", err)
	}
}

func TestMediaDeletionPollerFailureAndRetry(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-delete-failure.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	baseStore, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := baseStore.PutAtomic(context.Background(), strings.NewReader("retry-me"), media.PutMeta{Key: "video/retry.mp4", ContentType: "video/mp4"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaObject(&db.MediaObject{ID: "obj-poller-retry", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady, RefCount: 1}); err != nil {
		t.Fatal(err)
	}
	asset := &db.MediaAsset{PublicID: "poller-retry", CapabilityHash: "hash", Kind: "video", Status: db.MediaAssetAvailable, ObjectID: "obj-poller-retry"}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := db.RequestMediaAssetDelete(asset.PublicID, "test"); err != nil {
		t.Fatal(err)
	}
	store := &deletionTestStore{ObjectStore: baseStore, deleteErr: errors.New("disk busy")}
	poller := NewMediaDeletionPoller("delete-failure-test", store)
	poller.RetryAfter = time.Millisecond
	claimed, err := poller.RunOnce(context.Background())
	if !claimed || err == nil {
		t.Fatalf("failure run claimed=%v err=%v", claimed, err)
	}
	failed, _ := db.GetMediaAssetByID(asset.ID)
	if failed.Status != db.MediaAssetDeleteFailed {
		t.Fatalf("failed asset = %#v", failed)
	}
	failedObject, _ := db.GetMediaObjectByID(asset.ObjectID)
	if failedObject.State != db.MediaObjectDeleteFailed || failedObject.DeleteAttempts != 1 || failedObject.LastError != "disk busy" {
		t.Fatalf("failed object = %#v", failedObject)
	}
	failedJob, _ := db.GetMediaDeletionJob(asset.ID)
	if failedJob.Status != db.MediaJobPending || failedJob.LastError != "disk busy" || failedJob.NextAttemptAt == nil {
		t.Fatalf("failed job = %#v", failedJob)
	}
	if err := db.RetryMediaAssetDeletion(asset.ID); err != nil {
		t.Fatal(err)
	}
	store.deleteErr = nil
	claimed, err = poller.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("retry run claimed=%v err=%v", claimed, err)
	}
	completed, _ := db.GetMediaAssetByID(asset.ID)
	if completed.Status != db.MediaAssetDeleted {
		t.Fatalf("retried asset = %#v", completed)
	}
}
