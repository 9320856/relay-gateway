package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestSameOwnerTaskReclaimFencesEveryOldAttemptMutation(t *testing.T) {
	for _, mode := range []string{"background", "client"} {
		t.Run(mode, func(t *testing.T) {
			if err := InitDB(t.TempDir() + "/lease.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = Close() })
			run := &TaskRun{ID: "same-owner", TaskKind: "video", Operation: "video.create", ChannelID: "channel", ProviderTaskID: "provider-same-owner", PollingMode: mode, TaskStatus: "processing"}
			if err := CreateTaskRun(run); err != nil {
				t.Fatal(err)
			}
			claim := func() (*TaskRun, error) {
				if mode == "background" {
					return ClaimDueTaskRun("worker", time.Minute)
				}
				return ClaimTaskRunPoll(run.ID, "worker", time.Minute)
			}
			first, err := claim()
			if err != nil {
				t.Fatal(err)
			}
			if err := DB.Model(&TaskRun{}).Where("id = ?", run.ID).Update("lease_expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
				t.Fatal(err)
			}
			second, err := claim()
			if err != nil {
				t.Fatal(err)
			}
			if first.LeaseOwner == second.LeaseOwner {
				t.Fatal("same named worker reused its attempt fencing token")
			}
			if err := RecordTaskPollForLease(first.ID, first.LeaseOwner, true, 200); !errors.Is(err, ErrTaskLeaseOwner) {
				t.Fatalf("stale poll accepted: %v", err)
			}
			if err := UpdateTaskRunStatusForLease(first.ID, first.LeaseOwner, "completed", "success"); !errors.Is(err, ErrTaskLeaseOwner) {
				t.Fatalf("stale status accepted: %v", err)
			}
			called := false
			if err := WithTaskRunLeaseContext(context.Background(), first.ID, first.LeaseOwner, func(context.Context) error { called = true; return nil }); !errors.Is(err, ErrTaskLeaseOwner) || called {
				t.Fatalf("stale callback applied: called=%v err=%v", called, err)
			}
			if err := ReleaseTaskRunLease(first.ID, first.LeaseOwner); !errors.Is(err, ErrTaskLeaseOwner) {
				t.Fatalf("stale owner released replacement: %v", err)
			}
			if err := RescheduleTaskRunPoll(first.ID, first.LeaseOwner, time.Now().Add(time.Hour)); !errors.Is(err, ErrTaskLeaseOwner) {
				t.Fatalf("stale owner rescheduled replacement: %v", err)
			}
			if err := RenewTaskRunLeaseContext(context.Background(), first.ID, first.LeaseOwner, time.Minute); !errors.Is(err, ErrTaskLeaseOwner) {
				t.Fatalf("stale owner renewed replacement: %v", err)
			}
			current, err := GetTaskRun(first.ID)
			if err != nil || current.PollCount != 0 || current.TaskStatus != "processing" || current.LeaseOwner != second.LeaseOwner {
				t.Fatalf("replacement mutated: %#v %v", current, err)
			}
		})
	}
}

func TestTaskLeaseCallbackRollsBackRelatedWrites(t *testing.T) {
	if err := InitDB(t.TempDir() + "/lease.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	run := &TaskRun{ID: "atomic-poll", TaskKind: "video", Operation: "video.create", ChannelID: "channel", ProviderTaskID: "provider-atomic-poll", PollingMode: "background", TaskStatus: "processing"}
	if err := CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	claimed, err := ClaimDueTaskRun("worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("durable projection failed")
	err = WithTaskRunLeaseContext(context.Background(), claimed.ID, claimed.LeaseOwner, func(ctx context.Context) error {
		if err := RecordTaskPollForLeaseContext(ctx, claimed.ID, claimed.LeaseOwner, true, 200); err != nil {
			return err
		}
		if _, err := AppendTaskEvent(ctx, claimed.ID, "status_changed", "completed"); err != nil {
			return err
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	current, err := GetTaskRun(claimed.ID)
	if err != nil || current.PollCount != 0 || current.EventSequence != 0 || current.LeaseOwner != claimed.LeaseOwner {
		t.Fatalf("partial poll commit: %#v %v", current, err)
	}
}

func TestTaskLeaseFenceCanceledNestedOperationCannotLeakIntoOwnerCommit(t *testing.T) {
	if err := InitDB(t.TempDir() + "/lease.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	run := &TaskRun{ID: "nested-budget", TaskKind: "video", Operation: "video.create", ChannelID: "channel", ProviderTaskID: "provider-nested-budget", PollingMode: "background", TaskStatus: "processing"}
	if err := CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	claimed, err := ClaimDueTaskRun("worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	err = DB.Transaction(func(ownerTx *gorm.DB) error {
		budgetCtx, cancel := context.WithTimeout(WithTx(context.Background(), ownerTx), 30*time.Millisecond)
		defer cancel()
		err := WithTaskRunLeaseContext(budgetCtx, claimed.ID, claimed.LeaseOwner, func(operationCtx context.Context) error {
			return SQLDBForContext(operationCtx).Transaction(func(inner *gorm.DB) error {
				if err := UpdateTaskRunResultContext(WithTx(operationCtx, inner), claimed.ID, "partial result", false); err != nil {
					return err
				}
				<-operationCtx.Done()
				return operationCtx.Err()
			})
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			return errors.New("nested budget did not expire")
		}
		// The owner deliberately commits its independent work after the child
		// failed; partial child writes must already have been rolled back.
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := GetTaskRun(claimed.ID)
	if err != nil || current.ResultBody != "" {
		t.Fatalf("canceled child leaked into owner commit: %#v %v", current, err)
	}
}
