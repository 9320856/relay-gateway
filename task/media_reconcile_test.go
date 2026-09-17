package task

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
)

func TestMediaReconcileReportsWithoutDeletingByDefault(t *testing.T) {
	if err := db.InitDB(filepath.Join(t.TempDir(), "reconcile.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := t.TempDir()
	store, err := media.NewLocalObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutAtomic(context.Background(), strings.NewReader("known"), media.PutMeta{Key: "profile/known.media"}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaObject(&db.MediaObject{ID: "obj-known", Backend: "local", StorageKey: "profile/known.media", State: db.MediaObjectReady}); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(root, "orphan.bin")
	if err := os.WriteFile(orphan, []byte("orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join(root, ".media-stale.part")
	if err := os.WriteFile(part, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	_ = os.Chtimes(orphan, old, old)
	_ = os.Chtimes(part, old, old)

	report, err := NewMediaReconcilePoller(store).RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ScannedFiles != 3 || report.OrphanFiles != 1 || report.TemporaryFiles != 1 || report.RemovedFiles != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("default audit removed orphan: %v", err)
	}
}

func TestMediaReconcileDeletesOnlyStaleUntrackedFilesAndReportsMissing(t *testing.T) {
	if err := db.InitDB(filepath.Join(t.TempDir(), "reconcile-delete.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := t.TempDir()
	store, err := media.NewLocalObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	missing := &db.MediaObject{ID: "obj-missing", Backend: "local", StorageKey: "profile/missing.media", State: db.MediaObjectReady}
	if err := db.CreateMediaObject(missing); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(root, "orphan.bin")
	if err := os.WriteFile(orphan, []byte("orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	part := filepath.Join(root, ".media-stale.part")
	if err := os.WriteFile(part, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	_ = os.Chtimes(orphan, old, old)
	_ = os.Chtimes(part, old, old)

	poller := NewMediaReconcilePoller(store)
	poller.DeleteOrphans = true
	poller.SafetyWindow = time.Minute
	report, err := poller.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.MissingObjectFiles != 1 || report.RemovedFiles != 2 || report.Changed != 2 {
		t.Fatalf("unexpected cleanup report: %+v", report)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan remains or unexpected error: %v", err)
	}
	if _, err := os.Stat(part); !os.IsNotExist(err) {
		t.Fatalf("part file remains or unexpected error: %v", err)
	}
	second, err := poller.RunOnce(context.Background())
	if err != nil || second.RemovedFiles != 0 || second.Changed != 0 {
		t.Fatalf("cleanup is not idempotent: %+v, %v", second, err)
	}
}
