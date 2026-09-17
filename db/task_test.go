package db

import (
	"errors"
	"testing"
	"time"
)

func TestTaskRunLifecycleIsMonotonicAndEventsAreSequenced(t *testing.T) {
	if err := InitDB(t.TempDir() + "/task.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	run := &TaskRun{ID: "run-1", TaskKind: "video", Operation: "video.create", ChannelID: "channel-1", PollingMode: "client"}
	if err := CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := RecordTaskAlias(&TaskAlias{TaskRunID: run.ID, LookupID: "public-1"}); err != nil {
		t.Fatal(err)
	}
	if resolved, err := GetTaskRunByAlias("public-1"); err != nil || resolved.ID != run.ID {
		t.Fatalf("alias lookup = %#v, %v", resolved, err)
	}
	if err := RecordTaskAlias(&TaskAlias{TaskRunID: run.ID, LookupID: "public-1"}); err != nil {
		t.Fatalf("idempotent alias insert: %v", err)
	}
	if err := UpdateTaskRunStatus(run.ID, "processing", "pending"); err != nil {
		t.Fatal(err)
	}
	if err := UpdateTaskRunStatus(run.ID, "queued", "pending"); !errors.Is(err, ErrTaskStateRegression) {
		t.Fatalf("regression error = %v", err)
	}
	if err := UpdateTaskRunStatus(run.ID, "completed", "success"); err != nil {
		t.Fatal(err)
	}
	if err := UpdateTaskRunStatus(run.ID, "failed", "failed"); !errors.Is(err, ErrTaskAlreadyTerminal) {
		t.Fatalf("terminal transition error = %v", err)
	}
	e1, err := AppendTaskEvent(nil, run.ID, "submitted", "{}")
	if err != nil {
		t.Fatal(err)
	}
	e2, err := AppendTaskEvent(nil, run.ID, "completed", "{}")
	if err != nil {
		t.Fatal(err)
	}
	if e1.Sequence != 1 || e2.Sequence != 2 {
		t.Fatalf("event sequence = %d, %d; want 1, 2", e1.Sequence, e2.Sequence)
	}
	loaded, err := GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != "completed" || loaded.EventSequence != 2 || loaded.StateVersion != 4 {
		t.Fatalf("unexpected task run: %+v", loaded)
	}
}

func TestExpiredTaskStatusIsTerminal(t *testing.T) {
	if err := InitDB(t.TempDir() + "/task-expired.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	run := &TaskRun{ID: "expired-run", TaskKind: "video", Operation: "video.create", ChannelID: "channel", TaskStatus: "processing"}
	if err := CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := UpdateTaskRunStatus(run.ID, "expired", "failed"); err != nil {
		t.Fatalf("processing -> expired: %v", err)
	}
	loaded, err := GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != "expired" || loaded.TaskOutcome != "failed" || loaded.CompletedAt == nil {
		t.Fatalf("expired task projection = %+v", loaded)
	}
	if err := UpdateTaskRunStatus(run.ID, "failed", "failed"); !errors.Is(err, ErrTaskAlreadyTerminal) {
		t.Fatalf("expired -> failed transition = %v, want terminal error", err)
	}
}

func TestTaskAliasCannotBeRepointedAcrossRuns(t *testing.T) {
	if err := InitDB(t.TempDir() + "/task-alias-conflict.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	for _, runID := range []string{"alias-run-a", "alias-run-b"} {
		if err := CreateTaskRun(&TaskRun{ID: runID, TaskKind: "video", Operation: "video.create", ChannelID: runID}); err != nil {
			t.Fatal(err)
		}
	}
	if err := RecordTaskAlias(&TaskAlias{TaskRunID: "alias-run-a", LookupID: "shared-provider-id"}); err != nil {
		t.Fatal(err)
	}
	if err := RecordTaskAlias(&TaskAlias{TaskRunID: "alias-run-b", LookupID: "shared-provider-id"}); !errors.Is(err, ErrTaskAliasConflict) {
		t.Fatalf("cross-run alias error = %v, want ErrTaskAliasConflict", err)
	}
	resolved, err := GetTaskRunByAlias("shared-provider-id")
	if err != nil || resolved.ID != "alias-run-a" {
		t.Fatalf("alias was repointed: run=%#v err=%v", resolved, err)
	}
}

func TestProfileTaskIdempotencyLookupRejectsFingerprintConflict(t *testing.T) {
	if err := InitDB(t.TempDir() + "/task-idempotency.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	run := &TaskRun{
		ID:                 "profile-idempotency-run",
		TaskKind:           "video",
		Operation:          "video.create",
		ChannelID:          "channel-idempotency",
		Engine:             "profile",
		IdempotencyKey:     "client-key",
		RequestFingerprint: "fingerprint-a",
		ProviderTaskID:     "provider-idempotency",
		SubmissionState:    "accepted",
		TaskStatus:         "queued",
	}
	if err := CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	found, err := FindProfileTaskRunByIdempotencyContext(nil, "client-key", "video.create", "fingerprint-a")
	if err != nil || found.ID != run.ID {
		t.Fatalf("idempotency lookup = %#v, %v", found, err)
	}
	if _, err := FindProfileTaskRunByIdempotencyContext(nil, "client-key", "video.create", "fingerprint-b"); !errors.Is(err, ErrTaskIdempotencyConflict) {
		t.Fatalf("fingerprint conflict = %v, want ErrTaskIdempotencyConflict", err)
	}
}

func TestTaskLeaseTakeoverRejectsStalePollUpdates(t *testing.T) {
	if err := InitDB(t.TempDir() + "/lease.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	now := time.Now()
	if err := CreateTaskRun(&TaskRun{ID: "lease-run", TaskKind: "video", Operation: "video.create", ChannelID: "c", PollingMode: "background", NextPollAt: &now}); err != nil {
		t.Fatal(err)
	}
	first, err := ClaimDueTaskRun("worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	second, err := ClaimDueTaskRun("worker-b", time.Minute)
	if err != nil || second.ID != first.ID {
		t.Fatalf("takeover = %+v, %v", second, err)
	}
	if err := RecordTaskPollForLease(first.ID, "worker-a", true, 200); !errors.Is(err, ErrTaskLeaseOwner) {
		t.Fatalf("stale poll error = %v", err)
	}
	if err := UpdateTaskRunStatusForLease(first.ID, "worker-a", "completed", "success"); !errors.Is(err, ErrTaskLeaseOwner) {
		t.Fatalf("stale status error = %v", err)
	}
	loaded, err := GetTaskRun(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PollCount != 0 || loaded.TaskStatus != "queued" || loaded.LeaseOwner != "worker-b" {
		t.Fatalf("stale worker mutated task: %+v", loaded)
	}
}

func TestClientPollLeasePreventsConcurrentProviderPolls(t *testing.T) {
	if err := InitDB(t.TempDir() + "/client-poll-lease.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	now := time.Now()
	if err := CreateTaskRun(&TaskRun{ID: "client-poll-lease-run", TaskKind: "video", Operation: "video.create", ChannelID: "channel", PollingMode: "client", TaskStatus: "processing", NextPollAt: &now}); err != nil {
		t.Fatal(err)
	}
	first, err := ClaimTaskRunPoll("client-poll-lease-run", "client-a", time.Minute)
	if err != nil || first.LeaseOwner != "client-a" {
		t.Fatalf("first client lease = %+v, %v", first, err)
	}
	if _, err := ClaimTaskRunPoll("client-poll-lease-run", "client-b", time.Minute); !errors.Is(err, ErrTaskLeaseUnavailable) {
		t.Fatalf("concurrent client lease error = %v, want ErrTaskLeaseUnavailable", err)
	}
	if err := ReleaseTaskRunLease("client-poll-lease-run", "client-a"); err != nil {
		t.Fatal(err)
	}
	second, err := ClaimTaskRunPoll("client-poll-lease-run", "client-b", time.Minute)
	if err != nil || second.LeaseOwner != "client-b" {
		t.Fatalf("released client lease = %+v, %v", second, err)
	}
}

func TestUpdateTaskRunProviderTaskID(t *testing.T) {
	if err := InitDB(t.TempDir() + "/update-provider-task-id.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	run := &TaskRun{ID: "run-provider-update", TaskKind: "video", Operation: "video.create", ChannelID: "channel-1", TaskStatus: "processing"}
	if err := CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := UpdateTaskRunProviderTaskID(run.ID, "real-upstream-id"); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ProviderTaskID != "real-upstream-id" {
		t.Fatalf("loaded ProviderTaskID = %q, want real-upstream-id", loaded.ProviderTaskID)
	}
}
