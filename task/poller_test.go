package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay-gateway/db"
)

func TestBackgroundPollerClaimsAndReschedulesWithoutSubmit(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/poller.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Now()
	run := &db.TaskRun{ID: "background-run", TaskKind: "video", Operation: "video.create", ChannelID: "channel", PollingMode: "background", TaskStatus: "queued", NextPollAt: &now}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	var calls int
	poller := &BackgroundPoller{Owner: "worker-a", Lease: time.Minute, RetryAfter: time.Second, Poll: func(_ context.Context, got *db.TaskRun) (Observation, error) {
		calls++
		if got.ID != run.ID {
			t.Fatalf("wrong task %q", got.ID)
		}
		return Observation{Status: "processing", Success: true}, nil
	}}
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed || calls != 1 {
		t.Fatalf("first poll claimed=%v err=%v calls=%d", claimed, err, calls)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil || loaded.PollCount != 1 || loaded.LeaseOwner != "" || loaded.TaskStatus != "processing" {
		t.Fatalf("rescheduled task = %+v, %v", loaded, err)
	}
	claimed, err = poller.RunOnce(context.Background())
	if err != nil || claimed {
		t.Fatalf("task should not be due yet: claimed=%v err=%v", claimed, err)
	}
}

func TestBackgroundPollerDoesNotRepollMaterializingTask(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/materializing-poller.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Now().Add(-time.Second)
	run := &db.TaskRun{ID: "materializing-run", TaskKind: "image", Operation: "images.create", ChannelID: "channel", PollingMode: "background", TaskStatus: "materializing", TaskOutcome: "pending", NextPollAt: &now}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	var calls int
	claimed, err := (&BackgroundPoller{Owner: "worker-a", Poll: func(context.Context, *db.TaskRun) (Observation, error) {
		calls++
		return Observation{}, nil
	}}).RunOnce(context.Background())
	if err != nil || claimed || calls != 0 {
		t.Fatalf("materializing task was polled again: claimed=%v calls=%d err=%v", claimed, calls, err)
	}
}

func TestBackgroundPollerCancellationIsRecordedAndRescheduled(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/cancel.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Now()
	run := &db.TaskRun{ID: "cancel-run", TaskKind: "video", Operation: "video.create", ChannelID: "c", PollingMode: "background", NextPollAt: &now}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	claimed, err := (&BackgroundPoller{Owner: "worker-a", RetryAfter: time.Hour, Poll: func(ctx context.Context, _ *db.TaskRun) (Observation, error) {
		return Observation{Status: "processing"}, ctx.Err()
	}}).RunOnce(ctx)
	if !claimed || !errors.Is(err, context.Canceled) {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PollCount != 1 || loaded.PollFailureCount != 1 || loaded.LeaseOwner != "" {
		t.Fatalf("canceled poll state = %+v", loaded)
	}
}

func TestBackgroundPollerResumesTaskAfterRestart(t *testing.T) {
	dbPath := t.TempDir() + "/restart-poller.db"
	if err := db.InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Second)
	run := &db.TaskRun{
		ID: "restart-poller-run", TaskKind: "video", Operation: "video.create", ChannelID: "restart-channel",
		Engine: "profile", ProfileID: "restart-profile", ProfileRevision: 1, ProfileDigest: "restart-digest",
		ProviderTaskID: "provider-restart-task", PollingMode: "background", TaskStatus: "processing", TaskOutcome: "pending", NextPollAt: &now,
	}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitDB(dbPath); err != nil {
		t.Fatalf("restart database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var pollCalls int
	poller := &BackgroundPoller{Owner: "restart-worker", Lease: time.Minute, Poll: func(_ context.Context, got *db.TaskRun) (Observation, error) {
		pollCalls++
		if got.ProviderTaskID != "provider-restart-task" || got.Engine != "profile" {
			t.Fatalf("poll received task snapshot %+v", got)
		}
		return Observation{Status: "completed", Outcome: "success", Success: true, HTTPStatus: 200}, nil
	}}
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed || pollCalls != 1 {
		t.Fatalf("restart poll claimed=%v err=%v calls=%d", claimed, err, pollCalls)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != "completed" || loaded.TaskOutcome != "success" || loaded.PollCount != 1 || loaded.ProviderTaskID != run.ProviderTaskID {
		t.Fatalf("task after restart poll = %+v", loaded)
	}
	claimed, err = poller.RunOnce(context.Background())
	if err != nil || claimed || pollCalls != 1 {
		t.Fatalf("terminal task was polled again claimed=%v err=%v calls=%d", claimed, err, pollCalls)
	}
}
