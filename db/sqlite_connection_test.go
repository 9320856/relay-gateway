package db

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestSQLiteSettingsSurvivePhysicalConnectionReplacement(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "connection.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	pool, err := DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	assertSettings := func() {
		t.Helper()
		conn, err := pool.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		for pragma, want := range map[string]int{"busy_timeout": 5000, "synchronous": 1, "foreign_keys": 1} {
			var got int
			if err := conn.QueryRowContext(context.Background(), "PRAGMA "+pragma).Scan(&got); err != nil || got != want {
				t.Fatalf("PRAGMA %s = %d, want %d: %v", pragma, got, want, err)
			}
		}
		var journal string
		if err := conn.QueryRowContext(context.Background(), "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
			t.Fatalf("journal_mode = %q, want wal: %v", journal, err)
		}
	}
	assertSettings()
	// Lifetime expiry forces a replacement before any PRAGMA query. Holding
	// one sql.Conn during each assertion keeps all checks on the same session.
	pool.SetConnMaxLifetime(time.Nanosecond)
	time.Sleep(time.Millisecond)
	assertSettings()
	if pool.Stats().MaxLifetimeClosed == 0 {
		t.Fatal("test did not replace an expired connection")
	}
	pool.SetConnMaxLifetime(time.Hour)
	// An unusable connection may also be discarded after cancellation or a
	// driver failure. ErrBadConn deterministically exercises that replacement.
	conn, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("discard connection: %v", err)
	}
	_ = conn.Close()
	assertSettings()
}

func TestInitDBIncompatibleSchemaDoesNotApplyConnectionSettings(t *testing.T) {
	for _, marker := range []string{"", "99"} {
		t.Run("marker="+marker, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "incompatible.db")
			conn, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.Exec("CREATE TABLE preserved (value TEXT)").Error; err != nil {
				t.Fatal(err)
			}
			if err := conn.Exec("INSERT INTO preserved VALUES ('keep')").Error; err != nil {
				t.Fatal(err)
			}
			if marker != "" {
				if err := conn.AutoMigrate(&SchemaMeta{}); err != nil {
					t.Fatal(err)
				}
				if err := conn.Create(&SchemaMeta{Key: "schema_version", Value: marker}).Error; err != nil {
					t.Fatal(err)
				}
			}
			pool, err := conn.DB()
			if err != nil {
				t.Fatal(err)
			}
			if err := pool.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := InitDB(path); !errors.Is(err, ErrIncompatibleSchema) {
				t.Fatalf("InitDB error = %v, want incompatible schema", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("incompatible database changed: %v", err)
			}
			for _, suffix := range []string{"-wal", "-shm"} {
				if _, err := os.Stat(path + suffix); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("incompatible database acquired %s: %v", suffix, err)
				}
			}
		})
	}
}
