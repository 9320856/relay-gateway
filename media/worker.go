package media

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// MaterializationJob is a durable-result work item. Result must already be
// obtained from the provider; workers intentionally have no submit operation.
// Callers can re-enqueue the same ID after a process restart.
type MaterializationJob struct {
	ID      string
	Result  MediaResult
	Key     string
	Attempt int
}

// DeleteJob removes an object after its logical asset has been revoked. It is
// separate from MaterializationJob so deletion can never accidentally fetch or
// submit a generation request.
type DeleteJob struct {
	ID      string
	Key     string
	Attempt int
}

// WorkerCallbacks receives durable state transitions. Implementations usually
// update the asset/job rows here; returning an error records that the callback
// itself failed, allowing the caller to retry the persisted job later.
type WorkerCallbacks interface {
	Materialized(context.Context, MaterializationJob, ObjectInfo) error
	MaterializationFailed(context.Context, MaterializationJob, error) error
	Deleted(context.Context, DeleteJob) error
	DeleteFailed(context.Context, DeleteJob, error) error
}

// WorkerCallbacksFunc is a convenient adapter for applications that only need
// a subset of transitions. Nil functions are treated as no-ops.
type WorkerCallbacksFunc struct {
	OnMaterialized          func(context.Context, MaterializationJob, ObjectInfo) error
	OnMaterializationFailed func(context.Context, MaterializationJob, error) error
	OnDeleted               func(context.Context, DeleteJob) error
	OnDeleteFailed          func(context.Context, DeleteJob, error) error
}

func (f WorkerCallbacksFunc) Materialized(ctx context.Context, j MaterializationJob, info ObjectInfo) error {
	if f.OnMaterialized == nil {
		return nil
	}
	return f.OnMaterialized(ctx, j, info)
}
func (f WorkerCallbacksFunc) MaterializationFailed(ctx context.Context, j MaterializationJob, err error) error {
	if f.OnMaterializationFailed == nil {
		return nil
	}
	return f.OnMaterializationFailed(ctx, j, err)
}
func (f WorkerCallbacksFunc) Deleted(ctx context.Context, j DeleteJob) error {
	if f.OnDeleted == nil {
		return nil
	}
	return f.OnDeleted(ctx, j)
}
func (f WorkerCallbacksFunc) DeleteFailed(ctx context.Context, j DeleteJob, err error) error {
	if f.OnDeleteFailed == nil {
		return nil
	}
	return f.OnDeleteFailed(ctx, j, err)
}

// Worker runs materialization and deletion jobs with bounded concurrency. The
// queues are intentionally in-memory: durable job records are the source of
// truth, and callers recover work by submitting records again after restart.
type Worker struct {
	Materializer *MaterializationWorker
	Callbacks    WorkerCallbacks
	Concurrency  int
	QueueSize    int

	initMu      sync.Mutex
	once        sync.Once
	mat         chan MaterializationJob
	del         chan DeleteJob
	concurrency int
}

func (w *Worker) init() error {
	if w == nil {
		return errors.New("media worker object store is required")
	}
	w.initMu.Lock()
	defer w.initMu.Unlock()

	if w.Materializer == nil || w.Materializer.Store == nil {
		return errors.New("media worker object store is required")
	}
	if w.Concurrency <= 0 {
		w.Concurrency = 1
	}
	if w.QueueSize <= 0 {
		w.QueueSize = w.Concurrency * 2
		if w.QueueSize < 1 {
			w.QueueSize = 1
		}
	}
	w.once.Do(func() {
		w.mat = make(chan MaterializationJob, w.QueueSize)
		w.del = make(chan DeleteJob, w.QueueSize)
		w.concurrency = w.Concurrency
	})
	return nil
}

// SubmitMaterialization queues a result already returned by an executor.
func (w *Worker) SubmitMaterialization(ctx context.Context, job MaterializationJob) error {
	if err := w.init(); err != nil {
		return err
	}
	if job.ID == "" || job.Key == "" {
		return errors.New("materialization job id and key are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case w.mat <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) SubmitDelete(ctx context.Context, job DeleteJob) error {
	if err := w.init(); err != nil {
		return err
	}
	if job.ID == "" || job.Key == "" {
		return errors.New("delete job id and key are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case w.del <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run processes queued work until ctx is cancelled. It returns only after all
// worker goroutines have stopped. Queued jobs are left available to the caller
// only through its durable job store, so cancellation should trigger recovery.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.init(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	w.initMu.Lock()
	concurrency := w.concurrency
	w.initMu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

func (w *Worker) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.mat:
			w.materialize(ctx, job)
		case job := <-w.del:
			w.delete(ctx, job)
		}
	}
}

func (w *Worker) materialize(ctx context.Context, job MaterializationJob) {
	info, err := w.Materializer.Materialize(ctx, job.Result, job.Key)
	if err == nil {
		err = w.callbacks().Materialized(ctx, job, info)
		if err != nil {
			_ = w.callbacks().MaterializationFailed(ctx, job, fmt.Errorf("materialization callback: %w", err))
		}
		return
	}
	_ = w.callbacks().MaterializationFailed(ctx, job, err)
}

func (w *Worker) delete(ctx context.Context, job DeleteJob) {
	err := w.Materializer.Store.Delete(ctx, job.Key)
	if err == nil {
		err = w.callbacks().Deleted(ctx, job)
		if err != nil {
			_ = w.callbacks().DeleteFailed(ctx, job, fmt.Errorf("delete callback: %w", err))
		}
		return
	}
	_ = w.callbacks().DeleteFailed(ctx, job, err)
}

func (w *Worker) callbacks() WorkerCallbacks {
	if w.Callbacks == nil {
		return WorkerCallbacksFunc{}
	}
	return w.Callbacks
}
