package db

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestBackgroundTaskClaimRequiresAcceptedProviderTask(t *testing.T) {
	for _, tc := range []struct {
		name, state, providerID string
		legacyBlank             bool
		wantClaim               bool
	}{
		{name: "accepted", state: "accepted", providerID: "provider-task", wantClaim: true},
		{name: "legacy blank state", providerID: "legacy-provider-task", legacyBlank: true, wantClaim: true},
		{name: "submitting", state: "submitting"},
		{name: "submitting with ID", state: "submitting", providerID: "provider-task"},
		{name: "unknown", state: "unknown"},
		{name: "unknown with ID", state: "unknown", providerID: "provider-task"},
		{name: "rejected", state: "rejected", providerID: "provider-task"},
		{name: "accepted without ID", state: "accepted"},
		{name: "accepted whitespace ID", state: "accepted", providerID: "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := InitDB(t.TempDir() + "/claim.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = Close() })
			due := time.Now().Add(-time.Second)
			run := &TaskRun{ID: "claim", Engine: "profile", PollingMode: "background", SubmissionState: tc.state, ProviderTaskID: tc.providerID, TaskStatus: "queued", NextPollAt: &due}
			if err := CreateTaskRun(run); err != nil {
				t.Fatal(err)
			}
			if tc.legacyBlank {
				// GORM fills accepted on new blank rows. Reproduce an older
				// persisted row whose submission state was never populated.
				if err := DB.Model(run).Update("submission_state", "").Error; err != nil {
					t.Fatal(err)
				}
			}
			claimed, err := ClaimDueTaskRun("worker", time.Minute)
			if tc.wantClaim {
				if err != nil || claimed == nil || claimed.ID != run.ID {
					t.Fatalf("accepted task not claimed: %#v %v", claimed, err)
				}
				return
			}
			if claimed != nil || !errors.Is(err, ErrTaskLeaseUnavailable) {
				t.Fatalf("unaccepted task claimed: %#v %v", claimed, err)
			}
			current, err := GetTaskRun(run.ID)
			if err != nil || current.LeaseOwner != "" || current.LeaseExpiresAt != nil || current.StateVersion != 0 {
				t.Fatalf("rejected claim changed task: %#v %v", current, err)
			}
		})
	}
}

func TestBackgroundTaskClaimRechecksAcceptanceBeforeUpdate(t *testing.T) {
	for _, column := range []string{"submission_state", "provider_task_id"} {
		t.Run(column, func(t *testing.T) {
			if err := InitDB(t.TempDir() + "/claim.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = Close() })
			run := &TaskRun{ID: "claim", PollingMode: "background", SubmissionState: "accepted", ProviderTaskID: "provider-task", TaskStatus: "queued"}
			if err := CreateTaskRun(run); err != nil {
				t.Fatal(err)
			}
			const callbackName = "test:change_task_acceptance"
			called := false
			if err := DB.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
				if called || tx.Statement.Table != run.TableName() {
					return
				}
				called = true
				value := ""
				if column == "submission_state" {
					value = "unknown"
				}
				// Raw SQL bypasses update callbacks, changing readiness after
				// selection but before the conditional lease update.
				if err := tx.Exec("UPDATE async_task_runs SET "+column+" = ? WHERE id = ?", value, run.ID).Error; err != nil {
					tx.AddError(err)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = DB.Callback().Update().Remove(callbackName) })
			claimed, err := ClaimDueTaskRun("worker", time.Minute)
			if !called || claimed != nil || !errors.Is(err, ErrTaskLeaseUnavailable) {
				t.Fatalf("readiness change bypassed claim fence: called=%v claimed=%#v err=%v", called, claimed, err)
			}
			current, err := GetTaskRun(run.ID)
			if err != nil || current.LeaseOwner != "" || current.StateVersion != 0 {
				t.Fatalf("failed claim retained lease: %#v %v", current, err)
			}
		})
	}
}
