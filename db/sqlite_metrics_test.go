package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"relay-gateway/metrics"
)

type sqliteResultError int

func (e sqliteResultError) Error() string { return "SQLite result error" }
func (e sqliteResultError) Code() int     { return int(e) }

func TestSQLiteMetricsClassifiesCodesWithoutReadingSQL(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want uint64
	}{
		{name: "success"},
		{name: "busy", err: sqliteResultError(5), want: 1},
		{name: "locked", err: sqliteResultError(6), want: 1},
		{name: "busy snapshot", err: sqliteResultError(517), want: 1},
		{name: "wrapped shared cache lock", err: fmt.Errorf("query: %w", sqliteResultError(262)), want: 1},
		{name: "constraint", err: sqliteResultError(19)},
		{name: "record missing", err: gorm.ErrRecordNotFound},
		{name: "cancelled", err: context.Canceled},
		{name: "message without SQLite code", err: errors.New("database is locked")},
	} {
		t.Run(test.name, func(t *testing.T) {
			collector := metrics.New()
			observed := sqliteMetricsLogger{Interface: logger.Discard, collector: collector}.LogMode(logger.Silent)
			observed.Trace(context.Background(), time.Now(), func() (string, int64) {
				t.Fatal("metrics must not evaluate SQL text when logging is disabled")
				return "", 0
			}, test.err)
			if got := collector.Snapshot().SQLiteBusy; got != test.want {
				t.Fatalf("sqlite_busy = %d, want %d", got, test.want)
			}
		})
	}
}

func TestSQLiteBusyFailureReachesRuntimeMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy.db")
	if err := InitDB(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := DB.Exec("CREATE TABLE busy_probe (id INTEGER PRIMARY KEY)").Error; err != nil {
		t.Fatal(err)
	}
	// Fail quickly on a real competing writer, rather than waiting the normal
	// five seconds. InitDB keeps a single physical connection in this pool.
	if err := DB.Exec("PRAGMA busy_timeout = 1").Error; err != nil {
		t.Fatal(err)
	}
	other, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	otherPool, err := other.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = otherPool.Close() })
	lock := other.Begin()
	if lock.Error != nil {
		t.Fatal(lock.Error)
	}
	t.Cleanup(func() { _ = lock.Rollback().Error })
	if err := lock.Exec("INSERT INTO busy_probe (id) VALUES (1)").Error; err != nil {
		t.Fatal(err)
	}
	before := metrics.Default.Snapshot().SQLiteBusy
	if err := DB.Exec("INSERT INTO busy_probe (id) VALUES (2)").Error; err == nil {
		t.Fatal("competing writer unexpectedly succeeded")
	}
	recorder := httptest.NewRecorder()
	metrics.Default.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/api/metrics", nil))
	var snapshot metrics.Snapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.SQLiteBusy != before+1 {
		t.Fatalf("runtime sqlite_busy = %d, want %d", snapshot.SQLiteBusy, before+1)
	}
	if err := lock.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err := DB.Exec("INSERT INTO busy_probe (id) VALUES (2)").Error; err != nil {
		t.Fatal(err)
	}
	if got := metrics.Default.Snapshot().SQLiteBusy; got != before+1 {
		t.Fatalf("successful write changed sqlite_busy to %d", got)
	}
}
