package metrics

import (
	"sort"
	"sync"
	"testing"
	"time"
)

func TestLatencyWindowWrapsPreservingExactRecentSamplePercentile(t *testing.T) {
	for _, window := range []int{1, 2, 20, 64} {
		c := New(WithMaxLatencySamples(window))
		var history []time.Duration
		for index := 0; index < window*4+3; index++ {
			// Alternate high/low observations so an incorrect eviction order
			// changes P95 instead of hiding behind an increasing maximum.
			observation := time.Duration((index*971)%113) * time.Millisecond
			c.ObserveRequest(observation, index%3 != 0)
			history = append(history, observation)
			start := len(history) - window
			if start < 0 {
				start = 0
			}
			latest := append([]time.Duration(nil), history[start:]...)
			sort.Slice(latest, func(i, j int) bool { return latest[i] < latest[j] })
			want := latest[(95*len(latest)+99)/100-1]
			snapshot := c.Snapshot()
			if snapshot.P95Latency != want || snapshot.LatencySamples != len(latest) || snapshot.Requests != uint64(len(history)) {
				t.Fatalf("window=%d observation=%d: snapshot=%+v wantP95=%v samples=%d", window, index, snapshot, want, len(latest))
			}
			// Snapshot sorting must never reorder or mutate the stored window.
			if repeated := c.Snapshot(); repeated != snapshot {
				t.Fatalf("repeated snapshot mutated the window: first=%+v second=%+v", snapshot, repeated)
			}
		}
	}
}

func TestConcurrentSnapshotsAndObserversRemainConsistent(t *testing.T) {
	c := New(WithMaxLatencySamples(10000))
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for iteration := 0; iteration < 2000; iteration++ {
				c.ObserveRequest(time.Duration(worker+1)*time.Millisecond, iteration%2 == 0)
				if iteration%10 == 0 {
					snapshot := c.Snapshot()
					if snapshot.Requests != snapshot.Successes+snapshot.Errors || snapshot.LatencySamples > 10000 || snapshot.ErrorRate != float64(snapshot.Errors)/float64(snapshot.Requests) {
						t.Errorf("snapshot counters came from different observations: %+v", snapshot)
					}
				}
			}
		}(worker)
	}
	workers.Wait()
	snapshot := c.Snapshot()
	if snapshot.Requests != 16000 || snapshot.Successes != 8000 || snapshot.Errors != 8000 || snapshot.LatencySamples != 10000 {
		t.Fatalf("concurrent snapshots lost observations: %+v", snapshot)
	}
}
