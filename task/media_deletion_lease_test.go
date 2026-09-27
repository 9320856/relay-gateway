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

type blockingDeletionStore struct {
	media.ObjectStore
	entered  chan struct{}
	finished chan struct{}
}

func (s *blockingDeletionStore) Delete(ctx context.Context, key string) error {
	close(s.entered)
	<-ctx.Done()
	close(s.finished)
	return ctx.Err()
}

func TestMediaDeletionRenewsExclusiveLeaseAndStopsOnCancellation(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/delete-renew.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	base, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := base.PutAtomic(context.Background(), strings.NewReader("file"), media.PutMeta{Key: "image/delete-renew.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaObject(&db.MediaObject{ID: "delete-renew", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady}); err != nil {
		t.Fatal(err)
	}
	asset := &db.MediaAsset{PublicID: "delete-renew", CapabilityHash: "hash", Kind: "image", Status: db.MediaAssetAvailable, ObjectID: "delete-renew"}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RequestMediaAssetPurgeContext(context.Background(), asset.ID); err != nil {
		t.Fatal(err)
	}
	store := &blockingDeletionStore{ObjectStore: base, entered: make(chan struct{}), finished: make(chan struct{})}
	poller := NewMediaDeletionPoller("same-worker", store)
	poller.Lease = 300 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- poller.Start(ctx) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("deletion did not start")
	}
	initial, err := db.GetMediaDeletionJob(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, err := db.GetMediaDeletionJob(asset.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.LeaseExpiresAt.After(*initial.LeaseExpiresAt) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deletion lease was not renewed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := db.ClaimMediaDeletionJob("same-worker", time.Minute); !errors.Is(err, db.ErrMediaJobUnavailable) {
		t.Fatalf("blocked deletion claimed twice: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("deletion did not stop")
	}
	select {
	case <-store.finished:
	default:
		t.Fatal("stop returned before deletion body finished")
	}
	job, err := db.GetMediaDeletionJob(asset.ID)
	if err != nil || job.LeaseOwner != "" || job.Status != db.MediaJobPending || !job.PurgeAfterDelete {
		t.Fatalf("canceled cleanup lost its durable retry: %#v %v", job, err)
	}
	if _, err := base.Stat(context.Background(), info.Key); err != nil {
		t.Fatalf("canceled deletion changed file: %v", err)
	}
}
