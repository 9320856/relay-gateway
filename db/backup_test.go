package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func makeBackupTestDB(t *testing.T, version string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")
	conn, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.AutoMigrate(&SchemaMeta{}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Create(&SchemaMeta{Key: "schema_version", Value: version}).Error; err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := conn.DB()
	_ = sqlDB.Close()
	return path
}

func TestBackupDatabaseCreatesConsistentSnapshot(t *testing.T) {
	source := makeBackupTestDB(t, SchemaVersion)
	destination := filepath.Join(t.TempDir(), "backup.db")
	if err := BackupDatabase(context.Background(), source, destination); err != nil {
		t.Fatalf("BackupDatabase() error = %v", err)
	}
	if _, err := os.Stat(destination); err != nil {
		t.Fatalf("backup was not created: %v", err)
	}
	report, err := InspectDatabase(context.Background(), destination)
	if err != nil {
		t.Fatalf("InspectDatabase(backup) error = %v", err)
	}
	if report.SchemaVersion != SchemaVersion || report.Integrity != "ok" {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestBackupDatabaseRejectsExistingDestinationAndSamePath(t *testing.T) {
	source := makeBackupTestDB(t, SchemaVersion)
	destination := filepath.Join(t.TempDir(), "backup.db")
	if err := os.WriteFile(destination, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := BackupDatabase(context.Background(), source, destination); err == nil {
		t.Fatal("BackupDatabase() accepted an existing destination")
	}
	contents, _ := os.ReadFile(destination)
	if string(contents) != "keep" {
		t.Fatal("existing destination was modified")
	}
	if err := BackupDatabase(context.Background(), source, source); err == nil {
		t.Fatal("BackupDatabase() accepted source as destination")
	}
}

func TestInspectDatabaseRejectsIncompatibleMarkerAndCorruption(t *testing.T) {
	incompatible := makeBackupTestDB(t, "99")
	if err := CheckDatabase(context.Background(), incompatible); !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("CheckDatabase(incompatible) error = %v, want ErrIncompatibleSchema", err)
	}
	corrupt := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(corrupt, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckDatabase(context.Background(), corrupt); err == nil {
		t.Fatal("CheckDatabase(corrupt) unexpectedly succeeded")
	}
}

func TestBackupDatabaseHonorsCanceledContext(t *testing.T) {
	source := makeBackupTestDB(t, SchemaVersion)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := BackupDatabase(ctx, source, filepath.Join(t.TempDir(), "backup.db")); !errors.Is(err, context.Canceled) {
		t.Fatalf("BackupDatabase(canceled) error = %v, want context.Canceled", err)
	}
}
