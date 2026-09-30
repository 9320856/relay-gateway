package task

import (
	"context"
	"testing"
	"time"

	"relay-gateway/db"
)

func TestProfileBackgroundPollerDoesNotConsumeUnacceptedTaskBudget(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/submission-budget.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	due := time.Now().Add(-time.Minute)
	for _, state := range []string{"submitting", "unknown", "rejected"} {
		run := &db.TaskRun{ID: state, Engine: "profile", TaskKind: "video", Operation: "video.create", PollingMode: "background", SubmissionState: state, TaskStatus: "queued", NextPollAt: &due}
		if err := db.CreateTaskRun(run); err != nil {
			t.Fatal(err)
		}
	}
	poller := NewProfileBackgroundPoller("submission-budget")
	for attempt := 0; attempt < 3; attempt++ {
		if claimed, err := poller.RunOnce(context.Background()); claimed || err != nil {
			t.Fatalf("unaccepted task reached ProfilePoll: claimed=%v err=%v", claimed, err)
		}
	}
	for _, state := range []string{"submitting", "unknown", "rejected"} {
		current, err := db.GetTaskRun(state)
		if err != nil {
			t.Fatal(err)
		}
		if current.SubmissionState != state || current.TaskStatus != "queued" || current.PollCount != 0 || current.PollFailureCount != 0 || current.StateVersion != 0 || current.LeaseOwner != "" || current.LeaseExpiresAt != nil || current.CompletedAt != nil {
			t.Fatalf("unaccepted task consumed polling budget: %#v", current)
		}
	}
	var attempts int64
	if err := db.DB.Model(&db.TaskAttempt{}).Count(&attempts).Error; err != nil || attempts != 0 {
		t.Fatalf("unaccepted tasks recorded attempts=%d err=%v", attempts, err)
	}
}
