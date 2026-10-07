package db

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm/logger"
	"relay-gateway/metrics"
)

// sqliteMetricsLogger observes SQL failures even when statement logging is
// disabled. It never reads SQL text or parameters to classify an error.
type sqliteMetricsLogger struct {
	logger.Interface
	collector *metrics.Collector
}

func (l sqliteMetricsLogger) LogMode(level logger.LogLevel) logger.Interface {
	l.Interface = l.Interface.LogMode(level)
	return l
}

func (l sqliteMetricsLogger) Trace(ctx context.Context, begin time.Time, query func() (string, int64), err error) {
	var result interface{ Code() int }
	if errors.As(err, &result) {
		// The low byte is the primary result code, including extended BUSY and
		// LOCKED errors such as BUSY_SNAPSHOT and LOCKED_SHAREDCACHE.
		code := result.Code() & 0xff
		if code == 5 || code == 6 {
			l.collector.IncSQLiteBusy()
		}
	}
	l.Interface.Trace(ctx, begin, query, err)
}
