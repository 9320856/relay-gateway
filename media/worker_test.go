package media

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWorkerMaterializesAndDeletesWithCallbacks(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var materialized, deleted []string
	worker := &Worker{
		Materializer: &MaterializationWorker{Store: store, Fetcher: InlineSourceFetcher{}},
		Concurrency:  2,
		Callbacks: WorkerCallbacksFunc{
			OnMaterialized: func(_ context.Context, j MaterializationJob, _ ObjectInfo) error {
				mu.Lock()
				materialized = append(materialized, j.ID)
				mu.Unlock()
				return nil
			},
			OnDeleted: func(_ context.Context, j DeleteJob) error {
				mu.Lock()
				deleted = append(deleted, j.ID)
				mu.Unlock()
				return nil
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	if err := worker.SubmitMaterialization(ctx, MaterializationJob{ID: "m1", Key: "one.txt", Result: MediaResult{SourceKind: SourceBinary, Body: strings.NewReader("one")}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(materialized) == 1 })
	if err := worker.SubmitDelete(ctx, DeleteJob{ID: "d1", Key: "one.txt"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(deleted) == 1 })
	if _, err := store.Stat(context.Background(), "one.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted object stat error = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("worker run error = %v", err)
	}
}

func TestWorkerReportsFetchFailureWithoutResubmitting(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	failures := make(chan error, 1)
	worker := &Worker{
		Materializer: &MaterializationWorker{Store: store, Fetcher: SourceFetcherFunc(func(context.Context, MediaResult) (FetchedSource, error) {
			return FetchedSource{}, errors.New("fetch failed")
		})},
		Callbacks: WorkerCallbacksFunc{OnMaterializationFailed: func(_ context.Context, _ MaterializationJob, err error) error {
			failures <- err
			return nil
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)
	if err := worker.SubmitMaterialization(ctx, MaterializationJob{ID: "m2", Key: "two", Result: MediaResult{SourceKind: SourceURL, Locator: "https://provider.invalid/two"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failures:
		if err.Error() != "fetch failed" {
			t.Fatalf("failure = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failure callback")
	}
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for worker")
}
