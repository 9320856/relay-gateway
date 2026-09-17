package db

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMediaCapabilityEncryptedAndRecoverable(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "media-capability-test-key")
	if err := InitDB(t.TempDir() + "/media-capability.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	publicID, capability, hash, err := NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := EncryptMediaCapability(capability)
	if err != nil || ciphertext == "" || ciphertext == capability || strings.Contains(ciphertext, capability) {
		t.Fatalf("capability was not encrypted: %q, %v", ciphertext, err)
	}
	asset := &MediaAsset{PublicID: publicID, CapabilityHash: hash, CapabilityCiphertext: ciphertext, Kind: "video", Status: MediaAssetAvailable}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := RecoverMediaAssetCapability(loaded)
	if err != nil || recovered != capability {
		t.Fatalf("recovered capability = %q, %v", recovered, err)
	}
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "wrong-media-key")
	if _, err := RecoverMediaAssetCapability(loaded); err == nil {
		t.Fatal("expected recovery to fail with wrong encryption key")
	}
}

func TestRecoverMediaCapabilityDoesNotRotateMissingCiphertext(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "media-missing-capability-key")
	if err := InitDB(t.TempDir() + "/media-missing-capability.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	publicID, capability, hash, err := NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	asset := &MediaAsset{PublicID: publicID, CapabilityHash: hash, Kind: "image", Status: MediaAssetAvailable}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RecoverMediaAssetCapability(loaded); !errors.Is(err, ErrMediaCapabilityUnavailable) {
		t.Fatalf("missing ciphertext recovery error = %v", err)
	}
	unchanged, err := GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.CapabilityHash != hash || unchanged.CapabilityCiphertext != "" {
		t.Fatalf("status read unexpectedly rotated link: %+v", unchanged)
	}
	if _, err := GetMediaAssetByLink(publicID, capability); err != nil {
		t.Fatalf("original link was invalidated: %v", err)
	}
}

func TestMediaAssetOrdinalIsScopedToTaskRun(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-ordinal.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	newAsset := func(publicID, taskRunID string) *MediaAsset {
		return &MediaAsset{PublicID: publicID, CapabilityHash: publicID, TaskRunID: taskRunID, Kind: "video", Ordinal: 0, Status: MediaAssetPending}
	}
	if err := CreateMediaAsset(newAsset("asset-a", "run-a")); err != nil {
		t.Fatal(err)
	}
	if err := CreateMediaAsset(newAsset("asset-b", "run-b")); err != nil {
		t.Fatalf("different task runs should allow the same ordinal: %v", err)
	}
	if err := CreateMediaAsset(newAsset("asset-c", "run-a")); err == nil {
		t.Fatal("duplicate task run ordinal should be rejected")
	}
}

func TestCompleteTaskRunAfterMediaLifecycle(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-task-completion.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	run := &TaskRun{ID: "media-complete-run", Engine: "profile", TaskKind: "video", TaskStatus: "processing", TaskOutcome: "pending"}
	if err := CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	assets := []*MediaAsset{
		{PublicID: "media-complete-a", CapabilityHash: "hash-a", TaskRunID: run.ID, Kind: "video", Ordinal: 0, Status: MediaAssetPending},
		{PublicID: "media-complete-b", CapabilityHash: "hash-b", TaskRunID: run.ID, Kind: "video", Ordinal: 1, Status: MediaAssetPending},
	}
	for _, asset := range assets {
		if err := CreateMediaAsset(asset); err != nil {
			t.Fatal(err)
		}
	}
	if err := CompleteTaskRunAfterMediaContext(nil, run.ID, "video"); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != "processing" || loaded.TaskOutcome != "pending" || loaded.EventSequence != 0 {
		t.Fatalf("pending media completed task unexpectedly: %+v", loaded)
	}

	if err := DB.Model(&MediaAsset{}).Where("id = ?", assets[0].ID).Updates(map[string]any{"status": MediaAssetAvailable, "object_id": "object-a"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := CompleteTaskRunAfterMediaContext(nil, run.ID, "video"); err != nil {
		t.Fatal(err)
	}
	loaded, err = GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != "processing" || loaded.EventSequence != 0 {
		t.Fatalf("partially materialized task completed unexpectedly: %+v", loaded)
	}

	if err := DB.Model(&MediaAsset{}).Where("id = ?", assets[1].ID).Updates(map[string]any{"status": MediaAssetAvailable, "object_id": "object-b"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := CompleteTaskRunAfterMediaContext(nil, run.ID, "video"); err != nil {
		t.Fatal(err)
	}
	loaded, err = GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != "completed" || loaded.TaskOutcome != "success" || loaded.CompletedAt == nil || loaded.EventSequence != 1 {
		t.Fatalf("fully materialized task was not completed: %+v", loaded)
	}
	var events []TaskEvent
	if err := DB.Where("task_run_id = ?", run.ID).Order("sequence ASC").Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Sequence != 1 || events[0].Type != "status_changed" || events[0].Data != "completed" {
		t.Fatalf("unexpected completion events: %#v", events)
	}
	stateVersion := loaded.StateVersion
	if err := CompleteTaskRunAfterMediaContext(nil, run.ID, "video"); err != nil {
		t.Fatal(err)
	}
	loaded, err = GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.StateVersion != stateVersion || loaded.EventSequence != 1 {
		t.Fatalf("repeated completion was not idempotent: before=%d after=%d run=%+v", stateVersion, loaded.StateVersion, loaded)
	}

	failedRun := &TaskRun{ID: "media-failed-run", Engine: "profile", TaskKind: "video", TaskStatus: "processing", TaskOutcome: "pending"}
	if err := CreateTaskRun(failedRun); err != nil {
		t.Fatal(err)
	}
	if err := CreateMediaAsset(&MediaAsset{PublicID: "media-failed", CapabilityHash: "hash-failed", TaskRunID: failedRun.ID, Kind: "video", Status: MediaAssetFailed}); err != nil {
		t.Fatal(err)
	}
	if err := CompleteTaskRunAfterMediaContext(nil, failedRun.ID, "video"); err != nil {
		t.Fatal(err)
	}
	failedLoaded, err := GetTaskRun(failedRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failedLoaded.TaskStatus != "processing" || failedLoaded.EventSequence != 0 {
		t.Fatalf("failed media completed task unexpectedly: %+v", failedLoaded)
	}
}

func TestListMediaAssetsFilteredPaginatesAfterFiltering(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-filter.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	for i, kind := range []string{"audio", "video", "video", "video"} {
		asset := &MediaAsset{PublicID: "filter-" + string(rune('a'+i)), CapabilityHash: "hash-" + string(rune('a'+i)), TaskRunID: "filter-run-" + string(rune('a'+i)), Kind: kind, Status: MediaAssetAvailable}
		if err := CreateMediaAsset(asset); err != nil {
			t.Fatal(err)
		}
	}
	assets, err := ListMediaAssetsFilteredContext(nil, 2, 1, "video", MediaAssetAvailable)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 2 || assets[0].Kind != "video" || assets[1].Kind != "video" {
		t.Fatalf("filtered page = %#v, want two video assets", assets)
	}
}

func TestMediaMaterializationJobLeaseAndRetry(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-job.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	job := &MediaMaterializationJob{AssetID: 42, Status: MediaJobPending}
	if err := CreateMediaMaterializationJob(job); err != nil {
		t.Fatal(err)
	}
	claimed, err := ClaimMediaMaterializationJob("worker-a", time.Minute)
	if err != nil || claimed.ID != job.ID || claimed.Status != MediaJobRunning || claimed.Attempts != 1 {
		t.Fatalf("claimed job = %+v, %v", claimed, err)
	}
	if _, err := ClaimMediaMaterializationJob("worker-b", time.Minute); !errors.Is(err, ErrMediaJobUnavailable) {
		t.Fatalf("active lease should block another worker: %v", err)
	}
	if err := FailMediaMaterializationJob(job.ID, "temporary fetch error", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	claimed, err = ClaimMediaMaterializationJob("worker-b", time.Minute)
	if err != nil || claimed.LeaseOwner != "worker-b" || claimed.Attempts != 2 {
		t.Fatalf("retried job = %+v, %v", claimed, err)
	}
	if err := CompleteMediaMaterializationJob(job.ID); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaMaterializationJob(job.AssetID)
	if err != nil || loaded.Status != MediaJobSucceeded || loaded.LeaseOwner != "" {
		t.Fatalf("completed job = %+v, %v", loaded, err)
	}
}

func TestMediaAssetLinkLifecycle(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "media-lifecycle-key")
	if err := InitDB(t.TempDir() + "/media.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	publicID, capability, hash, err := NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := EncryptMediaCapability(capability)
	if err != nil {
		t.Fatal(err)
	}
	asset := &MediaAsset{PublicID: publicID, CapabilityHash: hash, CapabilityCiphertext: ciphertext, Kind: "video", Status: MediaAssetAvailable, ContentType: "video/mp4"}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if _, err := GetMediaAssetByLink(publicID, capability); err != nil {
		t.Fatalf("valid media link rejected: %v", err)
	}
	if _, err := GetMediaAssetByLink(publicID, "wrong"); !errors.Is(err, ErrMediaAssetNotFound) {
		t.Fatalf("invalid capability error = %v", err)
	}
	loadedAsset, err := GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := RecoverMediaAssetCapability(loadedAsset)
	if err != nil || recovered != capability {
		t.Fatalf("recovered capability = %q, want %q, err = %v", recovered, capability, err)
	}
	// Verify that repeated reads return the exact same capability and don't change the asset
	recoveredSecond, err := RecoverMediaAssetCapability(loadedAsset)
	if err != nil || recoveredSecond != capability {
		t.Fatalf("second recovered capability = %q, want %q", recoveredSecond, capability)
	}
	if err := RequestMediaAssetDelete(publicID, "operator request"); err != nil {
		t.Fatal(err)
	}
	if _, err := GetMediaAssetByLink(publicID, capability); !errors.Is(err, ErrMediaAssetGone) {
		t.Fatalf("deleted media link error = %v", err)
	}
	if err := RequestMediaAssetDelete(publicID, "repeat"); err != nil {
		t.Fatalf("delete should be idempotent: %v", err)
	}
	// Also verify expired status returns ErrMediaAssetGone
	expiredAsset := &MediaAsset{PublicID: "expired-pub-id", CapabilityHash: hash, Kind: "image", Status: MediaAssetExpired, TaskRunID: "run-expired-test", Ordinal: 0}
	if err := CreateMediaAsset(expiredAsset); err != nil {
		t.Fatal(err)
	}
	if _, err := GetMediaAssetByLink(expiredAsset.PublicID, capability); !errors.Is(err, ErrMediaAssetGone) {
		t.Fatalf("expired media link error = %v, want ErrMediaAssetGone", err)
	}
}

func TestMediaDeletionJobLifecycleAndSharedObject(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-delete.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	object := &MediaObject{ID: "obj-delete", Backend: "local", StorageKey: "video/delete.mp4", SHA256: "sha", ByteSize: 4, ContentType: "video/mp4", State: MediaObjectReady, RefCount: 2}
	if err := CreateMediaObject(object); err != nil {
		t.Fatal(err)
	}
	publicID, capability, hash, err := NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	asset := &MediaAsset{PublicID: publicID, CapabilityHash: hash, Kind: "video", Status: MediaAssetAvailable, ObjectID: object.ID, ContentType: object.ContentType}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := RequestMediaAssetDelete(publicID, "operator"); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaAssetByID(asset.ID)
	if err != nil || loaded.Status != MediaAssetDeleteRequested {
		t.Fatalf("requested asset = %#v, %v", loaded, err)
	}
	loadedObject, err := GetMediaObjectByID(object.ID)
	if err != nil || loadedObject.RefCount != 1 || loadedObject.State != MediaObjectReady {
		t.Fatalf("requested object = %#v, %v", loadedObject, err)
	}
	job, err := GetMediaDeletionJob(asset.ID)
	if err != nil || job.Status != MediaJobPending || job.StorageKey != object.StorageKey {
		t.Fatalf("deletion job = %#v, %v", job, err)
	}
	if err := RequestMediaAssetDelete(publicID, "repeat"); err != nil {
		t.Fatal(err)
	}
	loadedObject, _ = GetMediaObjectByID(object.ID)
	if loadedObject.RefCount != 1 {
		t.Fatalf("repeat delete decremented shared refcount: %d", loadedObject.RefCount)
	}
	if _, err := GetMediaAssetByLink(publicID, capability); !errors.Is(err, ErrMediaAssetGone) {
		t.Fatalf("requested link error = %v", err)
	}
	if err := CompleteMediaAssetDelete(asset.ID); err != nil {
		t.Fatal(err)
	}
	loadedObject, _ = GetMediaObjectByID(object.ID)
	if loadedObject.State != MediaObjectReady || loadedObject.RefCount != 1 {
		t.Fatalf("shared object was deleted: %#v", loadedObject)
	}

	failing := &MediaAsset{PublicID: "delete-failed", CapabilityHash: "delete-failed-hash", TaskRunID: "delete-failed-run", Kind: "video", Status: MediaAssetDeleteRequested}
	if err := CreateMediaAsset(failing); err != nil {
		t.Fatal(err)
	}
	if err := MarkMediaAssetDeleteFailed(failing.ID, "late failure", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := GetMediaAssetByID(failing.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := GetMediaAssetByLink(publicID, capability); !errors.Is(err, ErrMediaAssetGone) {
		t.Fatalf("failed-delete link error = %v", err)
	}
}

func TestRetryMediaAssetDeletionRequeuesOnlyDeleteStates(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-delete-retry.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	asset := &MediaAsset{PublicID: "delete-retry", CapabilityHash: "hash", Kind: "video", Status: MediaAssetDeleteFailed}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := RetryMediaAssetDeletion(asset.ID); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaAssetByID(asset.ID)
	if err != nil || loaded.Status != MediaAssetDeleteRequested {
		t.Fatalf("retried asset = %#v, %v", loaded, err)
	}
	job, err := GetMediaDeletionJob(asset.ID)
	if err != nil || job.Status != MediaJobPending {
		t.Fatalf("retried job = %#v, %v", job, err)
	}
	if err := RetryMediaAssetDeletion(asset.ID); err != nil {
		t.Fatal(err)
	}
	available := &MediaAsset{PublicID: "available-no-delete-retry", CapabilityHash: "hash2", TaskRunID: "available-run", Kind: "video", Status: MediaAssetAvailable}
	if err := CreateMediaAsset(available); err != nil {
		t.Fatal(err)
	}
	if err := RetryMediaAssetDeletion(available.ID); !errors.Is(err, ErrMediaAssetNotFound) {
		t.Fatalf("available asset retry error = %v", err)
	}
}

func TestMarkMediaAssetFailedPreservesRetryableAsset(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-failed.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	asset := &MediaAsset{PublicID: "failed-asset", CapabilityHash: "hash", Kind: "video", Status: MediaAssetMaterializing}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := MarkMediaAssetFailed(asset.ID, "temporary fetch failure"); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaAssetByID(asset.ID)
	if err != nil || loaded.Status != MediaAssetFailed || loaded.DeleteReason != "temporary fetch failure" {
		t.Fatalf("failed asset = %#v, %v", loaded, err)
	}
}

func TestMarkMediaAssetAvailableRejectsDeleteFailed(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-delete-failed-available.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateMediaObject(&MediaObject{ID: "obj-delete-failed", Backend: "local", StorageKey: "delete-failed.mp4", SHA256: "sha-delete-failed", ContentType: "video/mp4", State: MediaObjectReady}); err != nil {
		t.Fatal(err)
	}
	asset := &MediaAsset{PublicID: "delete-failed-available", CapabilityHash: "delete-failed-available-hash", TaskRunID: "delete-failed-available-run", Kind: "video", Status: MediaAssetDeleteFailed}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := MarkMediaAssetAvailable(asset.ID, "obj-delete-failed", "video/mp4", "sha-delete-failed", 1); !errors.Is(err, ErrMediaAssetGone) {
		t.Fatalf("delete_failed asset became available: %v", err)
	}
}

func TestMarkMediaAssetAvailableIncrementsSharedObjectRefCount(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-shared-refcount.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateMediaObject(&MediaObject{ID: "obj-shared-refcount", Backend: "local", StorageKey: "shared-refcount.mp4", SHA256: "sha-shared-refcount", ContentType: "video/mp4", State: MediaObjectReady, RefCount: 1}); err != nil {
		t.Fatal(err)
	}
	first := &MediaAsset{PublicID: "shared-first", CapabilityHash: "shared-first-hash", TaskRunID: "shared-first-run", Kind: "video", Status: MediaAssetAvailable, ObjectID: "obj-shared-refcount"}
	if err := CreateMediaAsset(first); err != nil {
		t.Fatal(err)
	}
	second := &MediaAsset{PublicID: "shared-second", CapabilityHash: "shared-second-hash", TaskRunID: "shared-second-run", Kind: "video", Status: MediaAssetPending}
	if err := CreateMediaAsset(second); err != nil {
		t.Fatal(err)
	}
	if err := MarkMediaAssetAvailable(second.ID, "obj-shared-refcount", "video/mp4", "sha-shared-refcount", 1); err != nil {
		t.Fatal(err)
	}
	object, err := GetMediaObjectByID("obj-shared-refcount")
	if err != nil || object.RefCount != 2 || object.State != MediaObjectReady {
		t.Fatalf("shared object refcount/state = %#v, %v", object, err)
	}
}

func TestRetryMediaAssetDeletionDoesNotPendSharedObject(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-delete-shared-retry.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateMediaObject(&MediaObject{ID: "obj-shared-retry", Backend: "local", StorageKey: "shared-retry.mp4", SHA256: "sha-shared-retry", ContentType: "video/mp4", State: MediaObjectReady, RefCount: 2}); err != nil {
		t.Fatal(err)
	}
	asset := &MediaAsset{PublicID: "shared-retry-asset", CapabilityHash: "shared-retry-hash", TaskRunID: "shared-retry-run", Kind: "video", Status: MediaAssetDeleteFailed, ObjectID: "obj-shared-retry"}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := RetryMediaAssetDeletion(asset.ID); err != nil {
		t.Fatal(err)
	}
	object, err := GetMediaObjectByID("obj-shared-retry")
	if err != nil || object.RefCount != 2 || object.State != MediaObjectReady {
		t.Fatalf("shared object changed during retry = %#v, %v", object, err)
	}
}

func TestMediaDeletionJobLeaseOwnerPreventsStaleCompletion(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-delete-lease.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	job := &MediaDeletionJob{AssetID: 100, Status: MediaJobPending}
	if err := CreateMediaDeletionJob(job); err != nil {
		t.Fatal(err)
	}
	first, err := ClaimMediaDeletionJob("worker-a", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	second, err := ClaimMediaDeletionJob("worker-b", time.Minute)
	if err != nil || second.ID != first.ID {
		t.Fatalf("takeover claim = %#v, %v", second, err)
	}
	if err := CompleteMediaDeletionJobForLease(first.ID, "worker-a"); !errors.Is(err, ErrMediaJobLeaseOwner) {
		t.Fatalf("stale worker completed job: %v", err)
	}
	if err := CompleteMediaDeletionJobForLease(second.ID, "worker-b"); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaDeletionJob(100)
	if err != nil || loaded.Status != MediaJobSucceeded {
		t.Fatalf("completed job = %#v, %v", loaded, err)
	}
}

func TestMediaMaterializationJobLeaseOwnerPreventsStaleCompletion(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-materialize-lease.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	asset := &MediaAsset{PublicID: "materialize-lease-asset", CapabilityHash: "materialize-lease-hash", TaskRunID: "materialize-lease-run", Kind: "video", Status: MediaAssetPending}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	job := &MediaMaterializationJob{AssetID: asset.ID, Status: MediaJobPending}
	if err := CreateMediaMaterializationJob(job); err != nil {
		t.Fatal(err)
	}
	first, err := ClaimMediaMaterializationJob("worker-a", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	second, err := ClaimMediaMaterializationJob("worker-b", time.Minute)
	if err != nil || second.ID != first.ID {
		t.Fatalf("takeover claim = %#v, %v", second, err)
	}
	if err := CompleteMediaMaterializationJobForLease(first.ID, "worker-a"); !errors.Is(err, ErrMediaJobLeaseOwner) {
		t.Fatalf("stale worker completed job: %v", err)
	}
	if err := CompleteMediaMaterializationJobForLease(second.ID, "worker-b"); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetMediaMaterializationJob(asset.ID)
	if err != nil || loaded.Status != MediaJobSucceeded {
		t.Fatalf("completed job = %#v, %v", loaded, err)
	}
}

func TestHardDeleteMediaAssetRemovesRecordAndSharedObject(t *testing.T) {
	if err := InitDB(t.TempDir() + "/media-hard-delete.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })

	if err := CreateMediaObject(&MediaObject{ID: "obj-hard-del", Backend: "local", StorageKey: "hard-del.mp4", SHA256: "sha-hard", ContentType: "video/mp4", State: MediaObjectReady, RefCount: 1}); err != nil {
		t.Fatal(err)
	}
	asset := &MediaAsset{PublicID: "asset-hard-del", CapabilityHash: "hash-hard", Kind: "video", Status: MediaAssetAvailable, ObjectID: "obj-hard-del"}
	if err := CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}

	key, shouldDeleteFile, err := HardDeleteMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatalf("hard delete failed: %v", err)
	}
	if key != "hard-del.mp4" || !shouldDeleteFile {
		t.Fatalf("expected key=hard-del.mp4 shouldDeleteFile=true, got %s, %v", key, shouldDeleteFile)
	}

	if _, err := GetMediaAssetByID(asset.ID); !errors.Is(err, ErrMediaAssetNotFound) {
		t.Fatalf("asset still exists after hard delete: %v", err)
	}
	if _, err := GetMediaObjectByID("obj-hard-del"); !errors.Is(err, ErrMediaAssetNotFound) {
		t.Fatalf("object still exists after hard delete: %v", err)
	}
}
