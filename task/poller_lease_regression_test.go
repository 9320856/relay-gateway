package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay-gateway/db"
)

func TestBackgroundPollerRenewsLeaseAndStopsOnCancellation(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/poller.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	run := &db.TaskRun{ID: "renewing-poll", TaskKind: "video", Operation: "video.create", ChannelID: "channel", ProviderTaskID: "provider-renewing-poll", PollingMode: "background", TaskStatus: "processing"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	const lease = 300 * time.Millisecond
	entered, done := make(chan *db.TaskRun, 1), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := &BackgroundPoller{Owner: "worker", Lease: lease, Poll: func(ctx context.Context, run *db.TaskRun) (Observation, error) {
		entered <- run
		<-ctx.Done()
		return Observation{}, ctx.Err()
	}}
	go func() { done <- poller.Start(ctx) }()
	claimed := <-entered
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, err := db.GetTaskRun(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.LeaseExpiresAt.After(*claimed.LeaseExpiresAt) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("blocked poll lease was not renewed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := db.ClaimDueTaskRun("worker", time.Minute); !errors.Is(err, db.ErrTaskLeaseUnavailable) {
		t.Fatalf("renewed poll claimed twice: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("poller did not stop after cancellation")
	}
	current, err := db.GetTaskRun(run.ID)
	if err != nil || current.LeaseOwner != "" || current.PollFailureCount != 1 {
		t.Fatalf("shutdown did not durably release current poll: %#v %v", current, err)
	}
}

func TestLeaseHeartbeatStopJoinsCanceledRenewal(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	workCtx, stop := keepLeaseAlive(context.Background(), 30*time.Millisecond, func(ctx context.Context) error { close(entered); <-ctx.Done(); close(exited); return ctx.Err() })
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("renewal did not start")
	}
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected stop result: %v", err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("stop returned before renewal goroutine finished")
	}
	if workCtx.Err() == nil {
		t.Fatal("stop did not cancel lease work")
	}
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("stop was not idempotent: %v", err)
	}
}
