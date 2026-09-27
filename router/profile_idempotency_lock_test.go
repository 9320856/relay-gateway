package router

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProfileIdempotencyLockCanceledWaitersLeaveOwnerAndOtherKeysUsable(t *testing.T) {
	var locks profileIdempotencyLocks
	owner, err := locks.acquire(context.Background(), "same-key")
	if err != nil {
		t.Fatal(err)
	}
	defer owner()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			unlock, err := locks.acquire(ctx, "same-key")
			if unlock != nil {
				unlock()
				t.Error("waiter acquired occupied lock")
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("wait did not cancel: %v", err)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled requests stayed blocked behind running task")
	}
	locks.mu.Lock()
	refs, count := locks.items["same-key"].refs, len(locks.items)
	locks.mu.Unlock()
	if refs != 1 || count != 1 {
		t.Fatalf("canceled waiters leaked refs: refs=%d items=%d", refs, count)
	}
	other, err := locks.acquire(context.Background(), "independent-key")
	if err != nil {
		t.Fatal(err)
	}
	other()
	owner()
	owner()
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if len(locks.items) != 0 {
		t.Fatal("released locks retained keys")
	}
}

func TestProfileIdempotencyLockExcludesConcurrentOwnersAndReleasesOnce(t *testing.T) {
	var locks profileIdempotencyLocks
	var active atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock, err := locks.acquire(context.Background(), "single-submit")
			if err != nil {
				t.Error(err)
				return
			}
			if active.Add(1) != 1 {
				t.Error("multiple owners could submit concurrently")
			}
			runtime.Gosched()
			active.Add(-1)
			unlock()
			unlock()
		}()
	}
	wg.Wait()
	if active.Load() != 0 || len(locks.items) != 0 {
		t.Fatal("lock ownership or references leaked")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := locks.acquire(ctx, "single-submit"); !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled acquisition: %v", err)
	}
}
