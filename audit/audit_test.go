package audit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"relay-gateway/db"
)

func TestAuditAndMutationRollbackTogether(t *testing.T) {
	initAuditTestDB(t)
	tx := db.DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	entry, err := StartWithDB(tx, "admin_action", "127.0.0.1", "POST", "/api/channels", nil)
	if err != nil {
		t.Fatal(err)
	}
	channel := &db.ChannelModel{ID: "atomic", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true}
	if err := db.SaveChannelModelContext(db.WithTx(context.Background(), tx), channel); err != nil {
		t.Fatal(err)
	}
	if err := entry.AddEventWithDB(tx, "channel_health_updated", EventData{ChannelID: channel.ID, Message: "healthy"}); err != nil {
		t.Fatal(err)
	}
	entry.RecordResult(http.StatusOK, []byte(`{"status":"ok"}`), nil)
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetChannelModel("atomic"); err == nil {
		t.Fatal("channel unexpectedly committed after rollback")
	}
	if _, err := GetDetail(entry.ID); err == nil {
		t.Fatal("audit unexpectedly committed after rollback")
	}
}

func initAuditTestDB(t *testing.T) {
	t.Helper()
	if err := db.InitDB(t.TempDir() + "/audit.db"); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
}

func TestAuditLifecycleInSQLite(t *testing.T) {
	initAuditTestDB(t)
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	headers.Set("Authorization", "Bearer sk-secret-value")
	entry, err := Start("api_call", "127.0..1.1:9000", "POST", "/v1/chat/completions", headers)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	entry.SetReqBody([]byte(`{"model":"gpt-test","password":"do-not-store","messages":[{"role":"user","content":"hello"}]}`), "gpt-test")
	entry.RecordDispatch("openai-main", "openai", "https://api.example.com/v1?token=secret", "gpt-test-upstream")
	entry.RecordFailover("first credential rejected")
	entry.RecordResult(200, []byte(`{"usage":{"prompt_tokens":2,"completion_tokens":3},"choices":[]}`), nil)

	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatalf("GetDetail: %v", err)
	}
	if detail.Log.Outcome != "success" || detail.Log.InputTokens != 2 || detail.Log.OutputTokens != 3 {
		t.Fatalf("unexpected log: %+v", detail.Log)
	}
	if strings.Contains(detail.Log.RequestHeaders, "sk-secret") || !strings.Contains(detail.Log.RequestHeaders, "REDACTED") {
		t.Fatalf("authorization was not redacted: %s", detail.Log.RequestHeaders)
	}
	if strings.Contains(detail.Log.RequestBody, "do-not-store") || !strings.Contains(detail.Log.RequestBody, "REDACTED") {
		t.Fatalf("password was not redacted: %s", detail.Log.RequestBody)
	}
	if len(detail.Events) < 5 {
		t.Fatalf("expected lifecycle events, got %d", len(detail.Events))
	}

	rows, total, err := List(ListQuery{Page: 1, PageSize: 10, Model: "gpt-test"})
	if err != nil || total != 1 || len(rows) != 1 {
		t.Fatalf("List: total=%d rows=%d err=%v", total, len(rows), err)
	}
}

func TestGetDetailIncludesSynchronousMediaAssets(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "127.0.0.1", "POST", "/api/playground/run", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.RecordResult(http.StatusOK, []byte(`{"images":["/v1/media/image/[REDACTED]"]}`), nil)
	publicID, _, capabilityHash, err := db.NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	asset := &db.MediaAsset{PublicID: publicID, CapabilityHash: capabilityHash, OriginRequestID: entry.ID, Kind: "image", Ordinal: 0, Status: db.MediaAssetAvailable, SourceKind: "base64", ContentType: "image/png"}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}

	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskRun != nil {
		t.Fatalf("synchronous request unexpectedly has a task run: %+v", detail.TaskRun)
	}
	if len(detail.MediaAssets) != 1 || detail.MediaAssets[0].ID != asset.ID || detail.MediaAssets[0].OriginRequestID != entry.ID {
		t.Fatalf("synchronous media assets = %+v", detail.MediaAssets)
	}
}

func TestGetDetailIncludesDurableTaskLifecycle(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "127.0.0.1", "POST", "/v1/videos", nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	run := &db.TaskRun{
		ID:              "run-detail-origin",
		OriginRequestID: entry.ID,
		TaskKind:        "video",
		Operation:       "video.create",
		ChannelID:       "channel-detail",
		Engine:          "profile",
		ProfileID:       "profile-detail",
		ProfileRevision: 3,
		ProfileDigest:   "sha256:detail",
		PollingMode:     "background",
		ProviderTaskID:  "provider-detail",
		TaskStatus:      "processing",
		TaskOutcome:     "pending",
		PollCount:       7,
		LastPollAt:      &now,
	}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: "provider-detail", Source: "provider"}); err != nil {
		t.Fatal(err)
	}
	finished := now.Add(time.Second)
	if err := db.AppendTaskAttempt(&db.TaskAttempt{TaskRunID: run.ID, AttemptType: "poll", StartedAt: now, FinishedAt: &finished, HTTPStatus: http.StatusOK, Outcome: "success", ResponseMeta: `{"status":"processing"}`}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AppendTaskEvent(context.Background(), run.ID, "status_changed", `{"status":"processing"}`); err != nil {
		t.Fatal(err)
	}
	publicID, _, capabilityHash, err := db.NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Create(&db.MediaAsset{PublicID: publicID, CapabilityHash: capabilityHash, TaskRunID: run.ID, Kind: "video", Ordinal: 0, Status: db.MediaAssetAvailable, SourceKind: "url", SourceLocator: "https://cdn.example.test/video.mp4"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskPoll(run.ID, true, http.StatusOK); err != nil {
		t.Fatal(err)
	}
	before, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}

	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskRun == nil || detail.TaskRun.ID != run.ID || detail.TaskRun.ProfileRevision != 3 || detail.TaskRun.PollCount != before.PollCount {
		t.Fatalf("unexpected task run detail: %+v", detail.TaskRun)
	}
	if len(detail.Attempts) != 1 || detail.Attempts[0].AttemptType != "poll" {
		t.Fatalf("unexpected attempts: %+v", detail.Attempts)
	}
	if len(detail.TaskEvents) != 1 || detail.TaskEvents[0].Type != "status_changed" {
		t.Fatalf("unexpected task events: %+v", detail.TaskEvents)
	}
	if len(detail.MediaAssets) != 1 || detail.MediaAssets[0].PublicID != publicID {
		t.Fatalf("unexpected media assets: %+v", detail.MediaAssets)
	}
	after, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.PollCount != before.PollCount || after.TaskStatus != before.TaskStatus || after.StateVersion != before.StateVersion {
		t.Fatalf("GetDetail mutated task state: before=%+v after=%+v", before, after)
	}
}

func TestGetDetailFallsBackToTaskAliasAndPreservesLegacyLog(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "127.0.0.1", "GET", "/v1/images/generations/image-public", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.RecordResult(http.StatusOK, []byte(`{"id":"image-public","status":"completed"}`), nil)
	ctx := WithAudit(context.Background(), entry)
	if err := RecordAsyncTaskCreated(ctx, "image-public", "image", "completed"); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "run-detail-alias", TaskKind: "image", Operation: "image.create", ChannelID: "channel-image", ProviderTaskID: "image-public", TaskStatus: "completed", TaskOutcome: "success", PollCount: 2}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: db.ImageTaskMappingLookupPrefix + "image-public", Source: "provider"}); err != nil {
		t.Fatal(err)
	}
	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.TaskRun == nil || detail.TaskRun.ID != run.ID {
		t.Fatalf("image alias did not resolve task run: %+v", detail.TaskRun)
	}

	legacy, err := Start("api_call", "127.0.0.1", "GET", "/v1/videos/legacy-only", nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy.RecordResult(http.StatusOK, []byte(`{"status":"completed"}`), nil)
	legacyDetail, err := GetDetail(legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if legacyDetail.TaskRun != nil || legacyDetail.Attempts != nil || legacyDetail.TaskEvents != nil || legacyDetail.MediaAssets != nil {
		t.Fatalf("legacy detail unexpectedly contains durable task data: %+v", legacyDetail)
	}
}

func TestAuditErrorAndDelete(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "127.0.0.1", "POST", "/v1/images/generations", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.RecordResult(502, []byte(`{"error":"bad gateway"}`), errors.New("upstream failed"))
	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.Outcome != "error" {
		t.Fatalf("expected error, got %s", detail.Log.Outcome)
	}
	if err := DeleteAll(); err != nil {
		t.Fatal(err)
	}
	_, total, _ := List(ListQuery{})
	if total != 0 {
		t.Fatalf("expected zero logs after delete, got %d", total)
	}
}

func TestAuditCancellationKeepsTransferredStatusWithoutErrorText(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "127.0.0.1", "GET", "/v1/videos/task/content", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.RecordResult(http.StatusPartialContent, nil, errors.Join(errors.New("stream stopped"), context.Canceled))
	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.StatusCode != http.StatusPartialContent || detail.Log.Outcome != "cancelled" || detail.Log.ErrorMessage != "" {
		t.Fatalf("cancelled range audit = %+v", detail.Log)
	}

	notCancelled, err := Start("api_call", "127.0.0.1", "GET", "/v1/videos/task/content", nil)
	if err != nil {
		t.Fatal(err)
	}
	notCancelled.RecordResult(http.StatusBadGateway, nil, errors.New("provider canceled the render"))
	detail, err = GetDetail(notCancelled.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.Outcome != "error" || !strings.Contains(detail.Log.ErrorMessage, "provider canceled") {
		t.Fatalf("plain cancellation text was misclassified: %+v", detail.Log)
	}
}

func TestDeleteAllContextPreservesDeletingRequestAudit(t *testing.T) {
	initAuditTestDB(t)
	old, err := Start("admin_action", "127.0.0.1", "POST", "/api/channels", nil)
	if err != nil {
		t.Fatal(err)
	}
	old.RecordResult(http.StatusOK, []byte(`{"status":"ok"}`), nil)
	tx := db.DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	entry, err := StartWithDB(tx, "admin_action", "127.0.0.1", "DELETE", "/api/logs", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.RecordResult(http.StatusOK, []byte(`{"status":"ok"}`), nil)
	if err := DeleteAllContext(db.WithTx(context.Background(), tx), entry.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if _, err := GetDetail(entry.ID); err != nil {
		t.Fatalf("deleting request audit was removed: %v", err)
	}
	if _, err := GetDetail(old.ID); err == nil {
		t.Fatal("historical audit was not deleted")
	}
}

func TestPayloadTruncationAndBase64Summary(t *testing.T) {
	base64Value := strings.Repeat("QUJD", 400)
	value, truncated := sanitizePayload([]byte(`{"image":"` + base64Value + `"}`))
	if truncated {
		t.Fatal("small payload should not be truncated")
	}
	if !strings.Contains(value, "BASE64") || strings.Contains(value, base64Value) {
		t.Fatalf("base64 was not summarized: %s", value)
	}
	b64ImagePayload := `{"data":[{"b64_json":"` + base64Value + `"}]}`
	imgValue, imgTruncated := sanitizePayload([]byte(b64ImagePayload))
	if imgTruncated {
		t.Fatal("image payload should not be truncated")
	}
	if strings.Contains(imgValue, "BASE64") || !strings.Contains(imgValue, base64Value) {
		t.Fatalf("b64_json must be preserved for gallery rendering: %s", imgValue)
	}
	large, truncated := capture([]byte(strings.Repeat("x", MaxFieldBytes+1)))
	if !truncated || !strings.Contains(large, "TRUNCATED") {
		t.Fatal("large field should be marked truncated")
	}
}

func TestMaskSecret(t *testing.T) {
	if got := MaskSecret("Bearer sk-123456"); got != "Bearer sk-****456" {
		t.Fatalf("unexpected mask: %s", got)
	}
}

func TestAuditSanitizesBinaryAndInlineSecrets(t *testing.T) {
	binary, truncated := sanitizePayload([]byte{0xff, 0x00, 0x01, 0xfe})
	if truncated || !strings.Contains(binary, "BINARY length=4") || strings.Contains(binary, "\ufffd") {
		t.Fatalf("binary payload was not summarized safely: %q", binary)
	}
	text := sanitizeText(`upstream error api_key=sk-live-secret token:abc123`)
	if strings.Contains(text, "sk-live-secret") || strings.Contains(text, "abc123") || !strings.Contains(text, "REDACTED") {
		t.Fatalf("inline secret was not redacted: %q", text)
	}
	smallDataURL, _ := sanitizePayload([]byte(`{"image":"data:image/png;base64,QUJD"}`))
	if !strings.Contains(smallDataURL, "BASE64") || strings.Contains(smallDataURL, "QUJD") {
		t.Fatalf("small data URL was not summarized: %q", smallDataURL)
	}
	nested, _ := sanitizePayload([]byte(`{"error":"upstream token=inline-secret"}`))
	if strings.Contains(nested, "inline-secret") || !strings.Contains(nested, "REDACTED") {
		t.Fatalf("inline secret inside parsed JSON was not redacted: %q", nested)
	}
	mediaURL := "https://gateway.example.test/v1/media/public-id/capability-secret"
	clean, _ := sanitizePayload([]byte(`{"url":"` + mediaURL + `"}`))
	if strings.Contains(clean, "capability-secret") {
		t.Fatalf("media capability leaked into audit payload: %q", clean)
	}
}

func TestAuditRecordsCandidateChannels(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "127.0.0.1", "POST", "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.RecordCandidates([]string{"primary", "fallback"})
	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range detail.Events {
		if event.Phase == "candidate_channels" && strings.Contains(event.Data, "primary") && strings.Contains(event.Data, "fallback") {
			found = true
		}
	}
	if !found {
		t.Fatalf("candidate channel event missing: %+v", detail.Events)
	}
}

func TestAuditAggregatesStreamText(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "127.0.0.1", "POST", "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.SetReqBody([]byte(`{"model":"gpt-test","stream":true}`), "gpt-test")
	stream := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"lo\",\"reasoning_content\":\"plan\"}}]}\n\ndata: [DONE]\n\n")
	entry.RecordResult(200, stream, nil)
	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.StreamText != "hello"+"plan" {
		t.Fatalf("unexpected aggregate stream text: %q", detail.Log.StreamText)
	}
}

func TestCleanupAppliesRetentionWindow(t *testing.T) {
	initAuditTestDB(t)
	old, err := Start("api_call", "127.0.0.1", "POST", "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	old.RecordResult(http.StatusOK, []byte(`{"status":"old"}`), nil)
	recent, err := Start("api_call", "127.0.0.1", "POST", "/v1/messages", nil)
	if err != nil {
		t.Fatal(err)
	}
	recent.RecordResult(http.StatusOK, []byte(`{"status":"recent"}`), nil)

	oldTime := time.Now().UTC().Add(-31 * 24 * time.Hour)
	if err := db.DB.Model(&db.RequestLogModel{}).Where("id = ?", old.ID).Update("started_at", oldTime).Error; err != nil {
		t.Fatal(err)
	}
	if err := Cleanup(30); err != nil {
		t.Fatal(err)
	}
	if _, err := GetDetail(old.ID); err == nil {
		t.Fatal("request older than the retention window was not deleted")
	}
	if _, err := GetDetail(recent.ID); err != nil {
		t.Fatalf("recent request was deleted by retention cleanup: %v", err)
	}
}

func createAsyncTaskParent(t *testing.T, taskID, taskKind, initialStatus string) *AuditEntry {
	t.Helper()
	parent, err := Start("api_call", "127.0.0.1", "POST", "/v1/"+taskKind+"s", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskMapping(db.TaskMapping{
		TaskID:          taskID,
		ChannelID:       "async-channel",
		OriginRequestID: parent.ID,
		TaskKind:        taskKind,
		TaskAlias:       strings.TrimPrefix(taskID, "imgjob_"),
	}); err != nil {
		t.Fatal(err)
	}
	ctx := WithAudit(context.Background(), parent)
	if err := RecordAsyncTaskCreated(ctx, strings.TrimPrefix(taskID, "imgjob_"), taskKind, initialStatus); err != nil {
		t.Fatal(err)
	}
	parent.RecordResult(http.StatusAccepted, []byte(`{"id":"`+taskID+`","status":"`+initialStatus+`"}`), nil)
	return parent
}

func recordAsyncTaskPoll(t *testing.T, taskID, taskKind, status string, response any) {
	t.Helper()
	child, err := Start("api_call", "127.0.0.1", "GET", "/v1/"+taskKind+"s/"+taskID, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithAudit(context.Background(), child)
	if !MarkAsyncTaskPoll(ctx, taskID, taskKind, status, response, nil) {
		t.Fatal("async task poll was not marked")
	}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	child.RecordResult(http.StatusOK, body, nil)
	if err := CoalesceMarkedAsyncTaskPoll(child); err != nil {
		t.Fatalf("coalesce async task poll: %v", err)
	}
}

func TestCoalesceAsyncTaskPollsIntoCreationLog(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "imgjob_image-123", "image", "queued")

	// A repeated state only increments the summary counter; it must not make
	// the parent timeline noisy.
	recordAsyncTaskPoll(t, "image-123", "image", "queued", map[string]any{"id": "image-123", "status": "queued"})
	recordAsyncTaskPoll(t, "image-123", "image", "processing", map[string]any{"id": "image-123", "status": "processing"})
	recordAsyncTaskPoll(t, "image-123", "image", "succeeded", map[string]any{"id": "image-123", "status": "succeeded", "assets": []map[string]any{{"url": "https://cdn.example.test/image.png"}}})

	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.Outcome != "success" || detail.Log.StatusCode != http.StatusAccepted {
		t.Fatalf("creation HTTP result was overwritten: %+v", detail.Log)
	}
	if detail.Log.AsyncTaskKind != "image" || detail.Log.AsyncTaskID != "image-123" || detail.Log.AsyncTaskStatus != "completed" || detail.Log.AsyncPollCount != 3 || detail.Log.AsyncLastPolledAt == nil || detail.Log.AsyncCompletedAt == nil {
		t.Fatalf("unexpected async summary: %+v", detail.Log)
	}
	if !strings.Contains(detail.Log.AsyncResultBody, "cdn.example.test/image.png") || detail.Log.AsyncResultTruncated {
		t.Fatalf("terminal async result was not retained: %q", detail.Log.AsyncResultBody)
	}
	statusEvents := 0
	for _, event := range detail.Events {
		if event.Phase == "async_task_status_changed" {
			statusEvents++
		}
	}
	if statusEvents != 2 {
		t.Fatalf("status transitions = %d, want processing and completed only; events=%+v", statusEvents, detail.Events)
	}
	rows, total, err := List(ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].ID != parent.ID {
		t.Fatalf("coalesced list = rows=%+v total=%d err=%v", rows, total, err)
	}
}

func TestCoalescedAsyncTaskProgressIsCurrentAndTerminalStateCannotRegress(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "video-monotonic", "video", "queued")

	recordAsyncTaskPoll(t, "video-monotonic", "video", "processing", map[string]any{"status": "processing", "progress": 37})
	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncTaskStatus != "processing" || detail.Log.AsyncPollCount != 1 || !strings.Contains(detail.Log.AsyncResultBody, `"progress": 37`) || detail.Log.AsyncCompletedAt != nil {
		t.Fatalf("processing result was not retained immediately: %+v", detail.Log)
	}

	recordAsyncTaskPoll(t, "video-monotonic", "video", "completed", map[string]any{"status": "completed"})
	recordAsyncTaskPoll(t, "video-monotonic", "video", "completed", map[string]any{"status": "completed", "video_url": "https://cdn.example.test/late.mp4"})
	recordAsyncTaskPoll(t, "video-monotonic", "video", "processing", map[string]any{"status": "processing", "progress": 80})
	recordAsyncTaskPoll(t, "video-monotonic", "video", "failed", map[string]any{"status": "failed", "error": "stale failure"})

	detail, err = GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncTaskStatus != "completed" || detail.Log.AsyncPollCount != 5 || detail.Log.AsyncCompletedAt == nil {
		t.Fatalf("terminal async summary regressed: %+v", detail.Log)
	}
	if !strings.Contains(detail.Log.AsyncResultBody, "late.mp4") || strings.Contains(detail.Log.AsyncResultBody, "stale failure") || strings.Contains(detail.Log.AsyncResultBody, `"progress": 80`) {
		t.Fatalf("terminal result body was replaced by a stale poll: %q", detail.Log.AsyncResultBody)
	}
	statusEvents := 0
	for _, event := range detail.Events {
		if event.Phase == "async_task_status_changed" {
			statusEvents++
		}
	}
	if statusEvents != 2 {
		t.Fatalf("terminal guard emitted stale transitions: events=%+v", detail.Events)
	}

	failedParent := createAsyncTaskParent(t, "video-failed-terminal", "video", "queued")
	recordAsyncTaskPoll(t, "video-failed-terminal", "video", "failed", map[string]any{"status": "failed", "error": "render rejected"})
	recordAsyncTaskPoll(t, "video-failed-terminal", "video", "completed", map[string]any{"status": "completed", "video_url": "https://cdn.example.test/incorrect.mp4"})
	failedDetail, err := GetDetail(failedParent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failedDetail.Log.AsyncTaskStatus != "failed" || !strings.Contains(failedDetail.Log.AsyncResultBody, "render rejected") || strings.Contains(failedDetail.Log.AsyncResultBody, "incorrect.mp4") {
		t.Fatalf("failed terminal state was replaced by completed: %+v", failedDetail.Log)
	}
}

func TestCoalescedAsyncTaskNonTerminalStateAndProgressCannotRegress(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "video-progress-monotonic", "video", "queued")

	recordAsyncTaskPoll(t, "video-progress-monotonic", "video", "processing", map[string]any{"status": "processing", "progress": 80})
	recordAsyncTaskPoll(t, "video-progress-monotonic", "video", "processing", map[string]any{"status": "processing", "progress": 37})
	recordAsyncTaskPoll(t, "video-progress-monotonic", "video", "queued", map[string]any{"status": "queued", "progress": 0})
	recordAsyncTaskPoll(t, "video-progress-monotonic", "video", "processing", map[string]any{"status": "processing"})

	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncTaskStatus != "processing" || detail.Log.AsyncPollCount != 4 || !strings.Contains(detail.Log.AsyncResultBody, `"progress": 80`) {
		t.Fatalf("non-terminal task state or progress regressed: %+v", detail.Log)
	}
	statusEvents := 0
	for _, event := range detail.Events {
		if event.Phase == "async_task_status_changed" {
			statusEvents++
		}
	}
	if statusEvents != 1 {
		t.Fatalf("stale observations emitted status transitions: events=%+v", detail.Events)
	}
}

func TestCoalescedLegacyVideoMappingStillUpdatesCreationAudit(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "legacy-video-poll", "video", "queued")
	if err := db.DB.Model(&db.TaskMapping{}).Where("task_id = ?", "legacy-video-poll").Update("task_kind", "").Error; err != nil {
		t.Fatal(err)
	}

	recordAsyncTaskPoll(t, "legacy-video-poll", "video", "processing", map[string]any{"status": "processing", "progress": 25})
	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncTaskStatus != "processing" || detail.Log.AsyncPollCount != 1 || !strings.Contains(detail.Log.AsyncResultBody, `"progress": 25`) {
		t.Fatalf("legacy video mapping was not coalesced: %+v", detail.Log)
	}
}

func TestRecordAsyncTaskProgressRetainsNonTerminalResult(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "127.0.0.1", "POST", "/v1/images/generations", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithAudit(context.Background(), entry)
	if err := RecordAsyncTaskCreated(ctx, "private-poll", "image", "queued"); err != nil {
		t.Fatal(err)
	}
	if err := RecordAsyncTaskProgress(ctx, "private-poll", "image", "processing", map[string]any{"status": "processing", "progress": 42}, nil); err != nil {
		t.Fatal(err)
	}
	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncTaskStatus != "processing" || detail.Log.AsyncPollCount != 1 || !strings.Contains(detail.Log.AsyncResultBody, `"progress": 42`) {
		t.Fatalf("private poll progress was not retained: %+v", detail.Log)
	}
	if err := RecordAsyncTaskProgress(ctx, "private-poll", "image", "queued", map[string]any{"status": "queued", "progress": 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := RecordAsyncTaskProgress(ctx, "private-poll", "image", "processing", map[string]any{"status": "processing", "progress": 12}, nil); err != nil {
		t.Fatal(err)
	}
	detail, err = GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncTaskStatus != "processing" || detail.Log.AsyncPollCount != 3 || !strings.Contains(detail.Log.AsyncResultBody, `"progress": 42`) {
		t.Fatalf("private poll state or progress regressed: %+v", detail.Log)
	}
}

func TestCoalesceAsyncTaskFailureRetainsCreationSuccessAndSanitizesError(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "video-123", "video", "queued")
	recordAsyncTaskPoll(t, "video-123", "video", "failed", map[string]any{
		"id": "video-123", "status": "failed", "error": map[string]any{"message": "token=provider-secret render rejected"},
	})
	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.Outcome != "success" || detail.Log.AsyncTaskStatus != "failed" || detail.Log.AsyncCompletedAt == nil {
		t.Fatalf("unexpected failed async summary: %+v", detail.Log)
	}
	if strings.Contains(detail.Log.AsyncResultBody, "provider-secret") || strings.Contains(detail.Log.AsyncTaskError, "provider-secret") || !strings.Contains(detail.Log.AsyncTaskError, "REDACTED") {
		t.Fatalf("async task error was not sanitized: result=%q error=%q", detail.Log.AsyncResultBody, detail.Log.AsyncTaskError)
	}
}

func TestUnsuccessfulOrUnmappablePollRemainsStandalone(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "video-fallback", "video", "queued")

	child, err := Start("api_call", "127.0.0.1", "GET", "/v1/videos/video-fallback", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithAudit(context.Background(), child)
	if MarkAsyncTaskPoll(ctx, "video-fallback", "video", "processing", map[string]any{"status": "processing"}, errors.New("upstream unavailable")) {
		t.Fatal("failed upstream poll must not be marked for coalescing")
	}
	child.RecordResult(http.StatusBadGateway, []byte(`{"error":"upstream unavailable"}`), errors.New("upstream unavailable"))
	if err := CoalesceMarkedAsyncTaskPoll(child); err != nil {
		t.Fatal(err)
	}
	if _, err := GetDetail(child.ID); err != nil {
		t.Fatalf("failed poll child log was removed: %v", err)
	}
	if detail, err := GetDetail(parent.ID); err != nil || detail.Log.AsyncPollCount != 0 {
		t.Fatalf("parent unexpectedly changed after standalone failure: detail=%+v err=%v", detail, err)
	}

	unknown, err := Start("api_call", "127.0.0.1", "GET", "/v1/videos/unknown", nil)
	if err != nil {
		t.Fatal(err)
	}
	if MarkAsyncTaskPoll(WithAudit(context.Background(), unknown), "unknown", "video", "processing", map[string]any{"status": "processing"}, nil) {
		t.Fatal("unknown task must not be marked for coalescing")
	}
	unknown.RecordResult(http.StatusOK, []byte(`{"status":"processing"}`), nil)
	if err := CoalesceMarkedAsyncTaskPoll(unknown); err != nil {
		t.Fatal(err)
	}
	if _, err := GetDetail(unknown.ID); err != nil {
		t.Fatalf("unknown poll child log was removed: %v", err)
	}
}

func TestCoalesceAsyncTaskPollCountIsSafeUnderConcurrentPolls(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "video-concurrent", "video", "queued")
	const polls = 20
	var wg sync.WaitGroup
	errCh := make(chan error, polls)
	for i := 0; i < polls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			child, err := Start("api_call", "127.0.0.1", "GET", "/v1/videos/video-concurrent", nil)
			if err != nil {
				errCh <- err
				return
			}
			if !MarkAsyncTaskPoll(WithAudit(context.Background(), child), "video-concurrent", "video", "processing", map[string]any{"status": "processing"}, nil) {
				errCh <- errors.New("mark failed")
				return
			}
			child.RecordResult(http.StatusOK, []byte(`{"status":"processing"}`), nil)
			if err := CoalesceMarkedAsyncTaskPoll(child); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncPollCount != polls || detail.Log.AsyncTaskStatus != "processing" {
		t.Fatalf("concurrent poll summary = %+v, want count %d", detail.Log, polls)
	}
	rows, total, err := List(ListQuery{Page: 1, PageSize: polls + 5})
	if err != nil || total != 1 || len(rows) != 1 {
		t.Fatalf("concurrent coalesced logs = total %d rows %d err %v", total, len(rows), err)
	}
}
