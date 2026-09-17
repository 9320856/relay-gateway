package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DatabaseIntegrityReport is the result of inspecting a SQLite database.
// SchemaVersion may be either the current version or the immediately
// previous version, both of which InitDB knows how to handle.
type DatabaseIntegrityReport struct {
	SchemaVersion string
	Integrity     string
}

// BackupDatabase creates a consistent SQLite snapshot at destinationPath.
// VACUUM INTO takes a transactionally consistent snapshot and includes WAL
// content, while requiring the destination not to exist. The source is never
// modified (apart from SQLite's normal read-side bookkeeping).
func BackupDatabase(ctx context.Context, sourcePath, destinationPath string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	source, destination, err := databaseBackupPaths(sourcePath, destinationPath)
	if err != nil {
		return err
	}
	if _, err := os.Stat(source); err != nil {
		return fmt.Errorf("stat source database: %w", err)
	}
	if _, err := os.Lstat(destination); err == nil {
		return fmt.Errorf("backup destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect backup destination: %w", err)
	}
	parent := filepath.Dir(destination)
	if info, err := os.Stat(parent); err != nil {
		return fmt.Errorf("stat backup directory: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("backup destination parent is not a directory: %s", parent)
	}

	conn, err := gorm.Open(sqlite.Open(source), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		return fmt.Errorf("open source database: %w", err)
	}
	sqlDB, err := conn.DB()
	if err != nil {
		return fmt.Errorf("access source database: %w", err)
	}
	defer sqlDB.Close()
	// A bound parameter avoids SQL injection and works with paths containing
	// quotes; SQLite accepts an expression for VACUUM INTO.
	if err := conn.WithContext(ctx).Exec("VACUUM INTO ?", destination).Error; err != nil {
		if _, statErr := os.Lstat(destination); statErr == nil {
			_ = os.Remove(destination)
		}
		return fmt.Errorf("create database backup: %w", err)
	}
	return nil
}

// CheckDatabase verifies that path is a readable, structurally sound database
// with a schema marker understood by this application.
func CheckDatabase(ctx context.Context, path string) error {
	_, err := InspectDatabase(ctx, path)
	return err
}

// InspectDatabase runs integrity_check and reads the schema marker without
// migrating or otherwise changing the database.
func InspectDatabase(ctx context.Context, path string) (DatabaseIntegrityReport, error) {
	var report DatabaseIntegrityReport
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	cleanPath, err := cleanDatabasePath(path)
	if err != nil {
		return report, err
	}
	info, err := os.Stat(cleanPath)
	if err != nil {
		return report, fmt.Errorf("stat database: %w", err)
	}
	if !info.Mode().IsRegular() {
		return report, fmt.Errorf("database path is not a regular file: %s", cleanPath)
	}
	conn, err := gorm.Open(sqlite.Open(cleanPath), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		return report, fmt.Errorf("open database: %w", err)
	}
	sqlDB, err := conn.DB()
	if err != nil {
		return report, fmt.Errorf("access database: %w", err)
	}
	defer sqlDB.Close()
	var integrity string
	if err := conn.WithContext(ctx).Raw("PRAGMA integrity_check").Scan(&integrity).Error; err != nil {
		return report, fmt.Errorf("run integrity check: %w", err)
	}
	report.Integrity = integrity
	if !strings.EqualFold(strings.TrimSpace(integrity), "ok") {
		return report, fmt.Errorf("database integrity check failed: %s", integrity)
	}
	var marker SchemaMeta
	if err := conn.WithContext(ctx).Where("key = ?", "schema_version").First(&marker).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return report, fmt.Errorf("%w: schema marker is missing", ErrIncompatibleSchema)
		}
		return report, fmt.Errorf("read schema marker: %w", err)
	}
	report.SchemaVersion = marker.Value
	if marker.Value != SchemaVersion && marker.Value != previousSchemaVersion {
		return report, fmt.Errorf("%w: found version %q, need %q or %q", ErrIncompatibleSchema, marker.Value, previousSchemaVersion, SchemaVersion)
	}
	return report, nil
}

func databaseBackupPaths(sourcePath, destinationPath string) (string, string, error) {
	source, err := cleanDatabasePath(sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("invalid source database path: %w", err)
	}
	destination, err := cleanDatabasePath(destinationPath)
	if err != nil {
		return "", "", fmt.Errorf("invalid backup destination path: %w", err)
	}
	if source == destination {
		return "", "", errors.New("backup destination must differ from source database")
	}
	return source, destination, nil
}

func cleanDatabasePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("database path is required")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	return abs, nil
}
