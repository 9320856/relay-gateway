package router

import (
	"context"
	"fmt"
	"testing"
	"time"

	"relay-gateway/db"
)

func TestBackfillLegacyTaskLifecycleIsIdempotent(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/legacy-backfill-startup.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{ID: "legacy-startup-channel", Name: "OpenAI", Type: "openai", BaseURL: "https://openai.example/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	// Simulate a pre-v3 row: no TaskKind, TaskAlias, or TaskRun projection.
	if err := db.RecordTaskMapping(db.TaskMapping{TaskID: "historical-startup-video", ChannelID: channel.ID}); err != nil {
		t.Fatal(err)
	}
	first, err := BackfillLegacyTaskLifecycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Scanned != 1 || first.Projected != 1 || first.AlreadyReady != 0 || first.Skipped != 0 {
		t.Fatalf("first backfill report = %+v", first)
	}
	second, err := BackfillLegacyTaskLifecycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Scanned != 1 || second.Projected != 0 || second.AlreadyReady != 1 || second.Skipped != 0 {
		t.Fatalf("second backfill report = %+v", second)
	}
	run, err := db.GetTaskRunByAlias("historical-startup-video")
	if err != nil {
		t.Fatal(err)
	}
	if run.Engine != "legacy" || run.TaskKind != asyncTaskKindVideo || run.Operation != "video.create" {
		t.Fatalf("backfilled run = %+v", run)
	}
	var aliases int64
	if err := db.DB.Model(&db.TaskAlias{}).Where("task_run_id = ?", run.ID).Count(&aliases).Error; err != nil {
		t.Fatal(err)
	}
	if aliases != 1 {
		t.Fatalf("backfilled alias count = %d, want 1", aliases)
	}
}

func TestLegacyAsyncMappingProjectsIntoTaskLifecycle(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/legacy-lifecycle.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registration := asyncTaskMappingRegistration{
		ChannelID:       "channel-1",
		TaskKind:        asyncTaskKindVideo,
		TaskAlias:       "public-task",
		InitialStatus:   "queued",
		OriginRequestID: "request-1",
		TaskIDs:         []string{"public-task", "provider-task"},
	}
	if err := db.EnsureTaskMappings(registration.mappings()...); err != nil {
		t.Fatal(err)
	}
	ensureLegacyTaskRun(registration)
	runID := legacyTaskRunID(registration)
	run, err := db.GetTaskRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ChannelID != registration.ChannelID || run.OriginRequestID != registration.OriginRequestID || run.ProviderTaskID != "public-task" {
		t.Fatalf("task run projection = %+v", run)
	}
	recordLegacyTaskPoll("public-task", asyncTaskKindVideo, "processing", map[string]any{"status": "processing"})
	run, err = db.GetTaskRun(runID)
	if err != nil || run.TaskStatus != "processing" || run.PollCount != 1 || run.PollSuccessCount != 1 {
		t.Fatalf("processing projection = %+v, %v", run, err)
	}
	if err := db.UpdateTaskRunStatus(runID, "completed", "success"); err != nil {
		t.Fatal(err)
	}
	if err := db.AppendTaskAttempt(nil); err == nil {
		t.Fatal("nil attempt should be rejected")
	}
}

func TestLegacyPollBackfillsPreTaskKindMapping(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/legacy-backfill.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{ID: "legacy-backfill-channel", Name: "OpenAI", Type: "openai", BaseURL: "https://openai.example/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskMapping(db.TaskMapping{TaskID: "historical-video", ChannelID: channel.ID}); err != nil {
		t.Fatal(err)
	}
	recordLegacyTaskPoll("historical-video", asyncTaskKindVideo, "completed", map[string]any{"status": "completed"})
	run, err := db.GetTaskRunByAlias("historical-video")
	if err != nil {
		t.Fatal(err)
	}
	if run.Engine != "legacy" || run.TaskKind != asyncTaskKindVideo || run.TaskStatus != "completed" || run.TaskOutcome != "success" {
		t.Fatalf("backfilled legacy run = %#v", run)
	}
	var mapping db.TaskMapping
	if err := db.DB.First(&mapping, "task_id = ?", "historical-video").Error; err != nil {
		t.Fatal(err)
	}
	if mapping.TaskKind != asyncTaskKindVideo || mapping.TaskAlias != "historical-video" {
		t.Fatalf("historical mapping was not normalized: %#v", mapping)
	}
}

func TestLegacyTaskRunUsesCanonicalProviderIDForImageAlias(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/legacy-image-provider-id.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registration := asyncTaskMappingRegistration{
		ChannelID:     "legacy-image-channel",
		TaskKind:      asyncTaskKindImage,
		TaskAlias:     "provider-image-id",
		InitialStatus: "queued",
		TaskIDs:       []string{imageTaskIDPrefix + "provider-image-id", "provider-image-id"},
	}
	if err := db.EnsureTaskMappings(registration.mappings()...); err != nil {
		t.Fatal(err)
	}
	ensureLegacyTaskRun(registration)
	run, err := db.GetTaskRun(legacyTaskRunID(registration))
	if err != nil {
		t.Fatal(err)
	}
	if run.ProviderTaskID != "provider-image-id" {
		t.Fatalf("image legacy provider task ID = %q, want canonical alias", run.ProviderTaskID)
	}
}

func TestProfileImageMappingDoesNotCreateLegacyTaskProjection(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-image-no-legacy-projection.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registration := asyncTaskMappingRegistration{
		ChannelID:       "profile-image-channel",
		TaskKind:        asyncTaskKindImage,
		TaskAlias:       "profile-image-task",
		InitialStatus:   "queued",
		OriginRequestID: "profile-image-request",
		TaskIDs:         []string{imageTaskIDPrefix + "profile-image-task", "profile-image-task"},
	}
	if err := db.EnsureTaskMappings(registration.mappings()...); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "profile-image-run", OriginRequestID: registration.OriginRequestID, TaskKind: asyncTaskKindImage, Operation: "images.create", ChannelID: registration.ChannelID, Engine: "profile", ProviderTaskID: registration.TaskAlias, TaskStatus: "queued", TaskOutcome: "pending"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	for _, lookupID := range registration.TaskIDs {
		if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: lookupID, Source: "create"}); err != nil {
			t.Fatal(err)
		}
	}

	recordLegacyTaskPoll(imageTaskIDPrefix+registration.TaskAlias, asyncTaskKindImage, "completed", map[string]any{"status": "completed"})
	report, err := BackfillLegacyTaskLifecycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Projected != 0 {
		t.Fatalf("profile task was projected as legacy: %+v", report)
	}
	var legacyRuns int64
	if err := db.DB.Model(&db.TaskRun{}).Where("engine = ?", "legacy").Count(&legacyRuns).Error; err != nil {
		t.Fatal(err)
	}
	if legacyRuns != 0 {
		t.Fatalf("legacy task projections = %d, want 0", legacyRuns)
	}
	loaded, err := db.GetTaskRunByAlias(imageTaskIDPrefix + registration.TaskAlias)
	if err != nil || loaded.ID != run.ID || loaded.Engine != "profile" {
		t.Fatalf("profile alias changed: run=%#v err=%v", loaded, err)
	}
}

func TestBackfillLegacyTaskLifecycleRecoversCompletedStatusFromRequestLog(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/legacy-backfill-log-status.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{ID: "legacy-log-status-channel", Name: "OpenAI", Type: "openai", BaseURL: "https://openai.example/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskMapping(db.TaskMapping{TaskID: "historical-completed-video", ChannelID: channel.ID}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := db.DB.Create(&db.RequestLogModel{
		ID: "legacy-completed-request", Kind: "api_call", StartedAt: now.Add(-time.Minute),
		Method: "POST", Path: "/v1/videos", ChannelID: channel.ID,
		AsyncTaskKind: asyncTaskKindVideo, AsyncTaskID: "historical-completed-video",
		AsyncTaskStatus: "completed", AsyncCompletedAt: &now, Outcome: "success", StatusCode: 200,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := BackfillLegacyTaskLifecycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	run, err := db.GetTaskRunByAlias("historical-completed-video")
	if err != nil {
		t.Fatal(err)
	}
	if run.TaskStatus != "completed" || run.TaskOutcome != "success" || run.CompletedAt == nil {
		t.Fatalf("historical completed status was not recovered: %+v", run)
	}
}

func TestReconcileOrphanedAsyncPollLogs(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/reconcile-orphaned.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	parentID := "origin-req-999"
	now := time.Now().UTC()
	if err := db.DB.Create(&db.RequestLogModel{
		ID:              parentID,
		Kind:            "admin_action",
		StartedAt:       now.Add(-10 * time.Minute),
		Method:          "POST",
		Path:            "/api/playground/run",
		AsyncTaskKind:   asyncTaskKindVideo,
		AsyncTaskID:     "task-orphaned-1",
		AsyncTaskStatus: "queued",
		AsyncPollCount:  0,
		StatusCode:      200,
		Outcome:         "running",
	}).Error; err != nil {
		t.Fatal(err)
	}

	if err := db.RecordTaskMapping(db.TaskMapping{
		TaskID:          "task-orphaned-1",
		ChannelID:       "test-channel-1",
		OriginRequestID: parentID,
		TaskKind:        asyncTaskKindVideo,
		TaskAlias:       "task-orphaned-1",
	}); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		st := "processing"
		if i == 3 {
			st = "completed"
		}
		if err := db.DB.Create(&db.RequestLogModel{
			ID:           fmt.Sprintf("child-poll-%d", i),
			Kind:         "api_call",
			StartedAt:    now.Add(time.Duration(i) * time.Minute),
			Method:       "GET",
			Path:         "/api/playground/video-status?task_id=task-orphaned-1",
			StatusCode:   200,
			Outcome:      "success",
			ResponseBody: fmt.Sprintf(`{"task_id":"task-orphaned-1","task_status":"%s"}`, st),
		}).Error; err != nil {
			t.Fatal(err)
		}
	}

	coalesced, err := ReconcileOrphanedAsyncPollLogs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if coalesced != 3 {
		t.Fatalf("expected 3 coalesced logs, got %d", coalesced)
	}

	var parent db.RequestLogModel
	if err := db.DB.First(&parent, "id = ?", parentID).Error; err != nil {
		t.Fatal(err)
	}
	if parent.AsyncPollCount != 3 {
		t.Fatalf("expected parent AsyncPollCount=3, got %d", parent.AsyncPollCount)
	}
	if parent.AsyncTaskStatus != "completed" {
		t.Fatalf("expected parent AsyncTaskStatus=completed, got %s", parent.AsyncTaskStatus)
	}
	if parent.AsyncCompletedAt == nil {
		t.Fatal("expected parent AsyncCompletedAt to be set")
	}

	var remainingChildren int64
	db.DB.Model(&db.RequestLogModel{}).Where("path LIKE ?", "%/playground/video-status%").Count(&remainingChildren)
	if remainingChildren != 0 {
		t.Fatalf("expected 0 orphaned child logs remaining, got %d", remainingChildren)
	}
}
