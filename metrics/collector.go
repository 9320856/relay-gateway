// Package metrics provides small, process-local runtime metrics for the gateway.
package metrics

import (
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

const defaultMaxLatencySamples = 10000

// Collector records runtime observations. It is safe for concurrent use.
type Collector struct {
	mu             sync.RWMutex
	requests       uint64
	successes      uint64
	errors         uint64
	sqliteBusy     uint64
	latencies      []time.Duration
	maxLatencies   int
	databaseBytes  int64
	databaseGrowth int64
	growthRate     float64
	databaseAt     time.Time
}

// Option configures a Collector.
type Option func(*Collector)

// WithMaxLatencySamples bounds memory used for the latency window. Values less
// than one are ignored.
func WithMaxLatencySamples(max int) Option {
	return func(c *Collector) {
		if max > 0 {
			c.maxLatencies = max
		}
	}
}

// New creates a process-local metrics collector.
func New(options ...Option) *Collector {
	c := &Collector{maxLatencies: defaultMaxLatencySamples}
	for _, option := range options {
		if option != nil {
			option(c)
		}
	}
	c.latencies = make([]time.Duration, 0, c.maxLatencies)
	return c
}

// ObserveRequest records one completed request. A false success value records
// an error. Negative durations are treated as zero.
func (c *Collector) ObserveRequest(duration time.Duration, success bool) {
	if duration < 0 {
		duration = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests++
	if success {
		c.successes++
	} else {
		c.errors++
	}
	if c.maxLatencies > 0 {
		if len(c.latencies) == c.maxLatencies {
			copy(c.latencies, c.latencies[1:])
			c.latencies[len(c.latencies)-1] = duration
		} else {
			c.latencies = append(c.latencies, duration)
		}
	}
}

// ObserveStatus records a request using an HTTP status code. 2xx and 3xx are
// considered successful; 4xx and 5xx are errors.
func (c *Collector) ObserveStatus(status int, duration time.Duration) {
	c.ObserveRequest(duration, status >= 200 && status < 400)
}

// IncSQLiteBusy records a transient SQLite busy/locked observation.
func (c *Collector) IncSQLiteBusy() {
	c.mu.Lock()
	c.sqliteBusy++
	c.mu.Unlock()
}

// SampleDatabase samples a SQLite database file's size and growth. The first
// sample establishes a baseline. Subsequent samples expose byte delta and
// bytes-per-second growth since the previous sample.
func (c *Collector) SampleDatabase(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.databaseAt.IsZero() {
		c.databaseGrowth = info.Size() - c.databaseBytes
		elapsed := now.Sub(c.databaseAt).Seconds()
		if elapsed > 0 {
			c.growthRate = float64(c.databaseGrowth) / elapsed
		}
	}
	c.databaseBytes = info.Size()
	c.databaseAt = now
	return nil
}

// Snapshot is an immutable copy of the current metrics.
type Snapshot struct {
	Requests            uint64        `json:"requests"`
	Successes           uint64        `json:"successes"`
	Errors              uint64        `json:"errors"`
	ErrorRate           float64       `json:"error_rate"`
	SQLiteBusy          uint64        `json:"sqlite_busy"`
	LatencySamples      int           `json:"latency_samples"`
	P95Latency          time.Duration `json:"p95_latency_ns"`
	DatabaseBytes       int64         `json:"database_bytes"`
	DatabaseGrowthBytes int64         `json:"database_growth_bytes"`
	DatabaseGrowthRate  float64       `json:"database_growth_bytes_per_second"`
	DatabaseSampleAt    time.Time     `json:"database_sample_at,omitempty"`
}

// Snapshot returns counters and percentile data calculated from the bounded
// latency window.
func (c *Collector) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	latencies := append([]time.Duration(nil), c.latencies...)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p95 time.Duration
	if len(latencies) > 0 {
		index := (95*len(latencies)+99)/100 - 1
		p95 = latencies[index]
	}
	rate := float64(0)
	if c.requests > 0 {
		rate = float64(c.errors) / float64(c.requests)
	}
	return Snapshot{Requests: c.requests, Successes: c.successes, Errors: c.errors,
		ErrorRate: rate, SQLiteBusy: c.sqliteBusy, LatencySamples: len(latencies),
		P95Latency: p95, DatabaseBytes: c.databaseBytes, DatabaseGrowthBytes: c.databaseGrowth,
		DatabaseGrowthRate: c.growthRate, DatabaseSampleAt: c.databaseAt}
}

// Handler returns a standard-library HTTP handler serving a JSON snapshot.
func (c *Collector) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(c.Snapshot()); err != nil {
			return
		}
	})
}
