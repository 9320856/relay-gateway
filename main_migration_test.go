package main

import (
	"os"
	"path/filepath"
	"testing"

	"relay-gateway/db"
)

func TestMigrateLegacyMediaFiles_Success(t *testing.T) {
	tempDir := t.TempDir()
	profileDir := filepath.Join(tempDir, "media", "profile")
	if err := os.MkdirAll(profileDir, 0755); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(tempDir, "migration.db")
	if err := db.InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	oldFileName := "video_123.media"
	oldFilePath := filepath.Join(profileDir, oldFileName)
	if err := os.WriteFile(oldFilePath, []byte("dummy-media-content"), 0644); err != nil {
		t.Fatal(err)
	}

	obj := &db.MediaObject{
		ID:          "obj_video_123",
		Backend:     "local",
		StorageKey:  "profile/" + oldFileName,
		SHA256:      "dummy-sha",
		ByteSize:    19,
		ContentType: "video/mp4",
		State:       "available",
	}
	if err := db.DB.Create(obj).Error; err != nil {
		t.Fatal(err)
	}

	migrateLegacyMediaFiles(profileDir)

	newFilePath := filepath.Join(profileDir, "video_123.mp4")
	if _, err := os.Stat(oldFilePath); !os.IsNotExist(err) {
		t.Fatalf("old file %s should have been renamed", oldFilePath)
	}
	if _, err := os.Stat(newFilePath); err != nil {
		t.Fatalf("new file %s should exist: %v", newFilePath, err)
	}

	var updated db.MediaObject
	if err := db.DB.First(&updated, "id = ?", obj.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updated.StorageKey != "profile/video_123.mp4" {
		t.Fatalf("expected storage_key %q, got %q", "profile/video_123.mp4", updated.StorageKey)
	}
}

func TestMigrateLegacyMediaFiles_InterruptedReconciliation(t *testing.T) {
	tempDir := t.TempDir()
	profileDir := filepath.Join(tempDir, "media", "profile")
	if err := os.MkdirAll(profileDir, 0755); err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(tempDir, "reconcile.db")
	if err := db.InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Only .mp4 exists on disk; .media does not exist
	mp4FilePath := filepath.Join(profileDir, "interrupted_video.mp4")
	if err := os.WriteFile(mp4FilePath, []byte("interrupted-content"), 0644); err != nil {
		t.Fatal(err)
	}

	obj := &db.MediaObject{
		ID:          "obj_interrupted",
		Backend:     "local",
		StorageKey:  "profile/interrupted_video.media",
		SHA256:      "dummy-sha-2",
		ByteSize:    19,
		ContentType: "video/mp4",
		State:       "available",
	}
	if err := db.DB.Create(obj).Error; err != nil {
		t.Fatal(err)
	}

	migrateLegacyMediaFiles(profileDir)

	var reconciled db.MediaObject
	if err := db.DB.First(&reconciled, "id = ?", obj.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reconciled.StorageKey != "profile/interrupted_video.mp4" {
		t.Fatalf("expected reconciled storage_key %q, got %q", "profile/interrupted_video.mp4", reconciled.StorageKey)
	}
}
