package task

import (
	"context"
	"testing"
	"time"

	"relay-gateway/db"
)

func TestMediaRetentionScanRevokesAndQueuesDeletionIdempotently(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/retention.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Now().UTC().Truncate(time.Millisecond)
	object := &db.MediaObject{ID: "retention-object", Backend: "local", StorageKey: "retention/key", SHA256: "retention-sha", ByteSize: 42, ContentType: "video/mp4", State: db.MediaObjectReady, RefCount: 1}
	if err := db.DB.Create(object).Error; err != nil {
		t.Fatal(err)
	}
	asset := &db.MediaAsset{PublicID: "retention-asset", CapabilityHash: "retention-hash", Kind: "video", Status: db.MediaAssetAvailable, ObjectID: object.ID, ExpiresAt: ptrTime(now.Add(-time.Minute))}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	scanner := &MediaRetentionScanner{Now: func() time.Time { return now }, Page: 10}
	got, err := scanner.Scan(context.Background())
	if err != nil || got.Scanned != 1 || got.Expired != 1 {
		t.Fatalf("scan = %+v, %v", got, err)
	}
	loaded, _ := db.GetMediaAssetByID(asset.ID)
	if loaded.Status != db.MediaAssetExpired {
		t.Fatalf("status = %q", loaded.Status)
	}
	var job db.MediaDeletionJob
	if err := db.DB.Where("asset_id = ?", asset.ID).First(&job).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != db.MediaJobPending {
		t.Fatalf("job = %+v", job)
	}
	loadedObject, _ := db.GetMediaObjectByID(object.ID)
	if loadedObject.RefCount != 0 || loadedObject.State != db.MediaObjectDeletePending {
		t.Fatalf("object = %+v", loadedObject)
	}
	got, err = scanner.Scan(context.Background())
	if err != nil || got.Scanned != 0 || got.Expired != 0 {
		t.Fatalf("repeat scan = %+v, %v", got, err)
	}
	var jobs int64
	db.DB.Model(&db.MediaDeletionJob{}).Where("asset_id = ?", asset.ID).Count(&jobs)
	if jobs != 1 {
		t.Fatalf("deletion jobs = %d", jobs)
	}
}

func TestMediaCapacityStatsAndPolicy(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/capacity.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	objects := []*db.MediaObject{
		{ID: "capacity-ready", Backend: "local", StorageKey: "ready", SHA256: "ready", ByteSize: 100, ContentType: "x", State: db.MediaObjectReady, RefCount: 1},
		{ID: "capacity-stage", Backend: "local", StorageKey: "stage", SHA256: "stage", ByteSize: 20, ContentType: "x", State: db.MediaObjectStaging, RefCount: 1},
		{ID: "capacity-delete", Backend: "local", StorageKey: "delete", SHA256: "delete", ByteSize: 7, ContentType: "x", State: db.MediaObjectDeletePending, RefCount: 0},
		{ID: "capacity-gone", Backend: "local", StorageKey: "gone", SHA256: "gone", ByteSize: 999, ContentType: "x", State: db.MediaObjectDeleted, RefCount: 0},
	}
	for _, object := range objects {
		if err := db.DB.Create(object).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i, status := range []string{db.MediaAssetAvailable, db.MediaAssetDeleted} {
		asset := &db.MediaAsset{PublicID: "capacity-asset-" + string(rune('a'+i)), CapabilityHash: "capacity-hash-" + string(rune('a'+i)), TaskRunID: "capacity-run-" + string(rune('a'+i)), Kind: "video", Status: status}
		if err := db.CreateMediaAsset(asset); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := GetMediaCapacityStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalObjectBytes != 127 || stats.ReadyObjectBytes != 100 || stats.StagingObjectBytes != 20 || stats.DeletePendingObjectBytes != 7 || stats.AssetCount != 2 || stats.ActiveAssetCount != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	check, err := CheckMediaCapacity(MediaCapacityPolicy{MaxObjectBytes: 127, MinFreeBytes: 50, FreeBytes: func() (int64, error) { return 49, nil }})
	if err != nil || check.Allowed || len(check.Reasons) != 2 {
		t.Fatalf("check = %+v, %v", check, err)
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
