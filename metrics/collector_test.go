package metrics

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSnapshotCountersAndP95(t *testing.T) {
	tests := []struct {
		name string
		feed []struct {
			duration time.Duration
			success  bool
		}
		wantP95 time.Duration
	}{
		{name: "empty"},
		{name: "five samples", feed: []struct {
			duration time.Duration
			success  bool
		}{
			{10 * time.Millisecond, true}, {20 * time.Millisecond, true}, {30 * time.Millisecond, false},
			{40 * time.Millisecond, true}, {50 * time.Millisecond, false}}, wantP95: 50 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New()
			for _, sample := range tt.feed {
				c.ObserveRequest(sample.duration, sample.success)
			}
			s := c.Snapshot()
			if s.Requests != uint64(len(tt.feed)) || s.Successes+s.Errors != s.Requests || s.P95Latency != tt.wantP95 {
				t.Fatalf("snapshot = %+v", s)
			}
			if len(tt.feed) > 0 && s.ErrorRate != float64(s.Errors)/float64(s.Requests) {
				t.Fatalf("error rate = %v", s.ErrorRate)
			}
		})
	}
}

func TestLatencyWindowAndStatus(t *testing.T) {
	c := New(WithMaxLatencySamples(2))
	c.ObserveStatus(200, time.Millisecond)
	c.ObserveStatus(503, 2*time.Millisecond)
	c.ObserveStatus(302, 3*time.Millisecond)
	s := c.Snapshot()
	if s.Requests != 3 || s.Successes != 2 || s.Errors != 1 || s.LatencySamples != 2 || s.P95Latency != 3*time.Millisecond {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestDatabaseSamplingAndHandler(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.db")
	if err := os.WriteFile(path, []byte("1234"), 0600); err != nil {
		t.Fatal(err)
	}
	c := New()
	if err := c.SampleDatabase(path); err != nil {
		t.Fatal(err)
	}
	if got := c.Snapshot(); got.DatabaseBytes != 4 || got.DatabaseGrowthBytes != 0 {
		t.Fatalf("first sample = %+v", got)
	}
	if err := os.WriteFile(path, []byte("1234567890"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.SampleDatabase(path); err != nil {
		t.Fatal(err)
	}
	if got := c.Snapshot(); got.DatabaseBytes != 10 || got.DatabaseGrowthBytes != 6 {
		t.Fatalf("second sample = %+v", got)
	}
	c.IncSQLiteBusy()
	r := httptest.NewRecorder()
	c.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	if r.Code != 200 || r.Header().Get("Content-Type") != "application/json" || len(r.Body.Bytes()) == 0 {
		t.Fatalf("handler response: %d %q", r.Code, r.Body.String())
	}
}

func TestConcurrentObservations(t *testing.T) {
	c := New(WithMaxLatencySamples(64))
	const workers, perWorker = 16, 250
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				c.ObserveRequest(time.Duration(j)*time.Microsecond, j%3 != 0)
				if j%7 == 0 {
					c.IncSQLiteBusy()
				}
			}
		}()
	}
	wg.Wait()
	s := c.Snapshot()
	if s.Requests != workers*perWorker || s.Successes+s.Errors != s.Requests || s.LatencySamples != 64 || s.SQLiteBusy != workers*((perWorker+6)/7) {
		t.Fatalf("snapshot = %+v", s)
	}
}
