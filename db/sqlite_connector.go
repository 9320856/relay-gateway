package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync/atomic"

	"github.com/glebarez/sqlite"
)

var sqliteConnectionPragmas = []string{
	"PRAGMA journal_mode = WAL",
	"PRAGMA busy_timeout = 5000",
	"PRAGMA synchronous = NORMAL",
	"PRAGMA foreign_keys = ON",
}

// sqliteConnector retains the registered driver's behavior while initializing
// every physical connection. Configuration remains disabled during the schema
// check so opening an incompatible database cannot change its journal mode.
type sqliteConnector struct {
	dsn       string
	driver    driver.Driver
	configure atomic.Bool
}

func openSQLitePool(dsn string) (*sql.DB, *sqliteConnector, error) {
	// sql.Open does not connect. Use its driver instance to preserve registered
	// functions and driver options without installing a process-global hook.
	driverPool, err := sql.Open(sqlite.DriverName, dsn)
	if err != nil {
		return nil, nil, err
	}
	connector := &sqliteConnector{dsn: dsn, driver: driverPool.Driver()}
	_ = driverPool.Close()
	return sql.OpenDB(connector), connector, nil
}

func (c *sqliteConnector) Driver() driver.Driver { return c.driver }

func (c *sqliteConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	if !c.configure.Load() {
		return conn, nil
	}
	executor, ok := conn.(driver.ExecerContext)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("SQLite driver does not support connection initialization")
	}
	for _, pragma := range sqliteConnectionPragmas {
		if _, err := executor.ExecContext(ctx, pragma, nil); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("initialize SQLite connection %q: %w", pragma, err)
		}
	}
	return conn, nil
}
