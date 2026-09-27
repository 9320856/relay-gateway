package task

import (
	"context"
	"sync"
	"time"

	"relay-gateway/db"
)

// KeepTaskRunLeaseAlive is shared by background workers and client status
// requests; both must cancel the provider call when their claim is lost.
func KeepTaskRunLeaseAlive(ctx context.Context, run *db.TaskRun, lease time.Duration) (context.Context, func() error) {
	return keepLeaseAlive(ctx, lease, func(renewCtx context.Context) error {
		return db.RenewTaskRunLeaseContext(renewCtx, run.ID, run.LeaseOwner, lease)
	})
}

// keepLeaseAlive owns one cancellable heartbeat for network/storage work.
// Losing the lease cancels that work; stop joins the goroutine so a worker
// cannot leak a ticker or renew a later attempt after it has returned.
func keepLeaseAlive(ctx context.Context, lease time.Duration, renew func(context.Context) error) (context.Context, func() error) {
	if ctx == nil {
		ctx = context.Background()
	}
	workCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var renewErr error
	go func() {
		defer close(done)
		interval := lease / 3
		if interval <= 0 {
			interval = time.Nanosecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := renew(workCtx); err != nil {
					renewErr = err
					cancel()
					return
				}
			}
		}
	}()
	var once sync.Once
	return workCtx, func() error { once.Do(func() { cancel(); <-done }); return renewErr }
}
