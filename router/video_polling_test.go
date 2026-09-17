package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/audit"
	"relay-gateway/db"
	"relay-gateway/service"
)

func saveVideoPollingTestChannel(t *testing.T, id, baseURL string) *db.ChannelModel {
	t.Helper()
	channel := &db.ChannelModel{
		ID:        id,
		Name:      id,
		Type:      "newapi",
		BaseURL:   baseURL + "/v1",
		APIKey:    "upstream-test-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "video-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	return channel
}

func performJSONRequest(engine http.Handler, method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)
	return recorder
}

func TestPlaygroundVideoPollsUpdateCreationAuditAndRemoveChildLogs(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	var statusCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/videos/generations":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"playground-public","task_id":"playground-provider","status":"queued"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/playground-public":
			w.Header().Set("Content-Type", "application/json")
			if statusCalls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"id":"playground-public","task_id":"playground-provider","status":"processing","progress":37}`))
				return
			}
			_, _ = w.Write([]byte(`{"id":"playground-public","task_id":"playground-provider","status":"completed","progress":100,"video_url":"https://cdn.example.test/temporary.mp4"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/playground-provider/content":
			w.Header().Set("Content-Type", "video/mp4")
			w.Header().Set("Content-Range", "bytes 0-4/5")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("video"))
		default:
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
		}
	}))
	t.Cleanup(upstream.Close)
	channel := saveVideoPollingTestChannel(t, "playground-video-channel", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/api/playground/run", handlePlaygroundRun)
	engine.GET("/api/playground/video-status", handlePlaygroundVideoStatus)
	engine.GET("/api/playground/video-content/:id", handleGetVideoContent)

	create := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-model","prompt":"render"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create response = %d %s", create.Code, create.Body.String())
	}
	var createPayload map[string]any
	if err := json.Unmarshal(create.Body.Bytes(), &createPayload); err != nil {
		t.Fatal(err)
	}
	if createPayload["task_id"] != "playground-public" || createPayload["task_status"] != "queued" || createPayload["video_url"] != "" {
		t.Fatalf("unexpected create payload: %v", createPayload)
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 {
		t.Fatalf("creation logs = total %d rows %d err %v", total, len(rows), err)
	}
	parentID := rows[0].ID

	processing := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/video-status?task_id=playground-public&channel_id=wrong-channel", "")
	if processing.Code != http.StatusOK {
		t.Fatalf("processing response = %d %s", processing.Code, processing.Body.String())
	}
	var processingPayload map[string]any
	if err := json.Unmarshal(processing.Body.Bytes(), &processingPayload); err != nil {
		t.Fatal(err)
	}
	if processingPayload["task_status"] != "processing" || processingPayload["progress"] != float64(37) || processingPayload["video_url"] != "" {
		t.Fatalf("unexpected processing payload: %v", processingPayload)
	}
	detail, err := audit.GetDetail(parentID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncTaskStatus != "processing" || detail.Log.AsyncPollCount != 1 || !strings.Contains(detail.Log.AsyncResultBody, `"progress": 37`) {
		t.Fatalf("processing poll did not update creation audit: %+v", detail.Log)
	}
	if rows, total, err = audit.List(audit.ListQuery{Page: 1, PageSize: 10}); err != nil || total != 1 || len(rows) != 1 || rows[0].ID != parentID {
		t.Fatalf("processing child log was not coalesced: total %d rows %+v err %v", total, rows, err)
	}

	completed := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/video-status?task_id=playground-public&channel_id=wrong-channel", "")
	if completed.Code != http.StatusOK {
		t.Fatalf("completed response = %d %s", completed.Code, completed.Body.String())
	}
	var completedPayload map[string]any
	if err := json.Unmarshal(completed.Body.Bytes(), &completedPayload); err != nil {
		t.Fatal(err)
	}
	wantVideoURL := "http://gateway.test/api/playground/video-content/playground-provider"
	if completedPayload["task_status"] != "completed" || completedPayload["video_url"] != wantVideoURL {
		t.Fatalf("unexpected completed payload: %v", completedPayload)
	}
	detail, err = audit.GetDetail(parentID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncTaskStatus != "completed" || detail.Log.AsyncPollCount != 2 || detail.Log.AsyncCompletedAt == nil || !strings.Contains(detail.Log.AsyncResultBody, "/api/playground/video-content/playground-provider") {
		t.Fatalf("completed poll did not update creation audit: %+v", detail.Log)
	}
	if rows, total, err = audit.List(audit.ListQuery{Page: 1, PageSize: 10}); err != nil || total != 1 || len(rows) != 1 || rows[0].ID != parentID {
		t.Fatalf("completed child log was not coalesced: total %d rows %+v err %v", total, rows, err)
	}
	if statusCalls.Load() != 2 {
		t.Fatalf("status calls = %d, want 2", statusCalls.Load())
	}

	contentRequest := httptest.NewRequest(http.MethodGet, wantVideoURL, nil)
	contentRequest.Header.Set("Range", "bytes=0-4")
	content := httptest.NewRecorder()
	engine.ServeHTTP(content, contentRequest)
	if content.Code != http.StatusPartialContent || content.Body.String() != "video" {
		t.Fatalf("playground content response = %d %q", content.Code, content.Body.String())
	}
	rows, total, err = audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].ID != parentID {
		t.Fatalf("playground media preview should not create an extra audit log: total %d rows %+v err %v", total, rows, err)
	}
	detail, err = audit.GetDetail(parentID)
	if err != nil || detail.Log.AsyncPollCount != 2 || detail.Log.AsyncTaskStatus != "completed" {
		t.Fatalf("media request changed the creation audit: detail=%+v err=%v", detail, err)
	}
}

func TestPlaygroundImmediatelyCompletedVideoUsesMappedAdminContentURL(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost || r.URL.Path != "/v1/videos/generations" {
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"id":"immediate-public","task_id":"immediate-provider","status":"completed"}`))
	}))
	t.Cleanup(upstream.Close)
	channel := saveVideoPollingTestChannel(t, "immediate-video-channel", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/api/playground/run", handlePlaygroundRun)
	response := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-model","prompt":"render"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create response = %d %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["task_status"] != "completed" || payload["video_url"] != "http://gateway.test/api/playground/video-content/immediate-provider" {
		t.Fatalf("immediately completed payload = %v", payload)
	}
	if got := db.GetVideoTaskChannel("immediate-provider"); got != channel.ID {
		t.Fatalf("provider content ID mapped to %q", got)
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].AsyncTaskStatus != "completed" || rows[0].AsyncCompletedAt == nil {
		t.Fatalf("immediate completion audit = total %d rows %+v err %v", total, rows, err)
	}
}

func TestPlaygroundVideoStatusKeepsUnmappedUpstreamSuccessWithoutGatewayURL(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"unmapped-video","status":"completed"}`))
	}))
	t.Cleanup(upstream.Close)
	channel := saveVideoPollingTestChannel(t, "unmapped-playground-channel", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.GET("/api/playground/video-status", handlePlaygroundVideoStatus)
	response := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/video-status?task_id=unmapped-video&channel_id="+channel.ID, "")
	if response.Code != http.StatusOK {
		t.Fatalf("status response = %d %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["status"] != "ok" || payload["task_status"] != "completed" || payload["video_url"] != "" {
		t.Fatalf("unmapped upstream success was not preserved safely: %v", payload)
	}
	if response.Header().Get("X-Relay-Task-Mapping") != "missing" {
		t.Fatalf("mapping state header = %q", response.Header().Get("X-Relay-Task-Mapping"))
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].Path != "/api/playground/video-status" || rows[0].Outcome != "success" || rows[0].AsyncTaskKind != "" {
		t.Fatalf("unmapped poll should remain standalone: total=%d rows=%+v err=%v", total, rows, err)
	}
}

func TestPlaygroundVideoStatusDoesNotRouteMappedTaskThroughQueryChannel(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	var mappedCalls atomic.Int32
	mappedUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mappedCalls.Add(1)
		http.Error(w, "disabled mapped channel must not be called", http.StatusInternalServerError)
	}))
	t.Cleanup(mappedUpstream.Close)
	mappedChannel := saveVideoPollingTestChannel(t, "disabled-mapped-channel", mappedUpstream.URL)

	var queryCalls atomic.Int32
	queryUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"pinned-video","status":"completed"}`))
	}))
	t.Cleanup(queryUpstream.Close)
	queryChannel := saveVideoPollingTestChannel(t, "active-query-channel", queryUpstream.URL)

	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:    "pinned-video",
		ChannelID: mappedChannel.ID,
		TaskKind:  asyncTaskKindVideo,
		TaskAlias: "pinned-video",
	}); err != nil {
		t.Fatal(err)
	}
	enabled, err := db.ToggleChannelContext(context.Background(), mappedChannel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("mapped test channel was not disabled")
	}

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.GET("/api/playground/video-status", handlePlaygroundVideoStatus)
	response := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/video-status?task_id=pinned-video&channel_id="+queryChannel.ID, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "该渠道当前已停用") {
		t.Fatalf("disabled mapped channel response = %d %s", response.Code, response.Body.String())
	}
	if mappedCalls.Load() != 0 || queryCalls.Load() != 0 {
		t.Fatalf("mapped status request reached upstreams: mapped=%d query=%d", mappedCalls.Load(), queryCalls.Load())
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].Outcome != "error" || rows[0].Path != "/api/playground/video-status" {
		t.Fatalf("blocked reroute audit = total %d rows %+v err %v", total, rows, err)
	}
}

func TestVideoContentDoesNotRouteUnmappedTaskThroughQueryChannel(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	channel := saveVideoPollingTestChannel(t, "unmapped-content-channel", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.GET("/v1/videos/:id/content", handleGetVideoContent)
	response := performJSONRequest(engine, http.MethodGet,
		"http://gateway.test/v1/videos/unmapped-content-task/content?channel_id="+channel.ID, "")
	if response.Code == http.StatusOK || upstreamCalls.Load() != 0 {
		t.Fatalf("unmapped content request was routed through query channel: status=%d calls=%d body=%s", response.Code, upstreamCalls.Load(), response.Body.String())
	}
}

type cancelOnWriteRecorder struct {
	header http.Header
	status int
}

func (w *cancelOnWriteRecorder) Header() http.Header { return w.header }
func (w *cancelOnWriteRecorder) WriteHeader(status int) {
	w.status = status
}
func (w *cancelOnWriteRecorder) Write([]byte) (int, error) { return 0, context.Canceled }

func prepareVideoContentRoute(t *testing.T, channel *db.ChannelModel, taskID string) *gin.Engine {
	t.Helper()
	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:    taskID,
		ChannelID: channel.ID,
		TaskKind:  asyncTaskKindVideo,
		TaskAlias: taskID,
	}); err != nil {
		t.Fatal(err)
	}
	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.GET("/v1/videos/:id/content", handleGetVideoContent)
	engine.HEAD("/v1/videos/:id/content", handleGetVideoContent)
	return engine
}

func prepareAuditedStreamingRoute(upstreamURL string) *gin.Engine {
	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.GET("/v1/chat/stream", func(c *gin.Context) {
		req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, upstreamURL, nil)
		if err != nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		for k, v := range c.Request.Header {
			req.Header[k] = v
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			_ = c.Error(err)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			c.Writer.Header()[k] = v
		}
		c.Writer.WriteHeader(resp.StatusCode)
		_, copyErr := io.Copy(c.Writer, resp.Body)
		if copyErr != nil {
			_ = c.Error(copyErr)
		}
	})
	return engine
}

func TestVideoContentCancellationBeforeHeadersAuditsNoResponse(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	engine := prepareAuditedStreamingRoute(upstream.URL)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/v1/chat/stream", nil).WithContext(ctx)
	engine.ServeHTTP(httptest.NewRecorder(), request)
	if upstreamCalls.Load() != 0 {
		t.Fatalf("cancelled request reached upstream %d times", upstreamCalls.Load())
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 {
		t.Fatalf("cancel audit rows = total %d rows %+v err %v", total, rows, err)
	}
	if rows[0].StatusCode != 0 || rows[0].Outcome != "cancelled" || rows[0].ErrorMessage != "" || rows[0].ResponseBody != "" {
		t.Fatalf("pre-header cancellation audit = %+v", rows[0])
	}
}

func TestVideoContentCancellationAfter206KeepsTransferredStatus(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Range", "bytes 0-4/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("video"))
	}))
	t.Cleanup(upstream.Close)
	engine := prepareAuditedStreamingRoute(upstream.URL)

	w := &cancelOnWriteRecorder{header: make(http.Header)}
	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/v1/chat/stream", nil)
	engine.ServeHTTP(w, request)
	if w.status != http.StatusPartialContent {
		t.Fatalf("client status = %d, want 206", w.status)
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 {
		t.Fatalf("cancel audit rows = total %d rows %+v err %v", total, rows, err)
	}
	if rows[0].StatusCode != http.StatusPartialContent || rows[0].Outcome != "cancelled" || rows[0].ErrorMessage != "" {
		t.Fatalf("post-header cancellation audit = %+v", rows[0])
	}
}

func TestVideoContentNormal206RemainsSuccessful(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=5-9" {
			http.Error(w, "missing range", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Range", "bytes 5-9/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("video"))
	}))
	t.Cleanup(upstream.Close)
	channel := saveVideoPollingTestChannel(t, "successful-range", upstream.URL)
	engine := prepareVideoContentRoute(t, channel, "successful-range-task")

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/v1/videos/successful-range-task/content", nil)
	request.Header.Set("Range", "bytes=5-9")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusPartialContent || response.Body.String() != "video" || response.Header().Get("Content-Range") != "bytes 5-9/10" {
		t.Fatalf("range response = %d %q headers=%v", response.Code, response.Body.String(), response.Header())
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 0 || len(rows) != 0 {
		t.Fatalf("video content stream must not produce audit rows: total=%d rows=%+v", total, rows)
	}
}

func TestPlaygroundVideoContentHEADIsAudited(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	var methods []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	channel := saveVideoPollingTestChannel(t, "successful-head", upstream.URL)
	engine := prepareVideoContentRoute(t, channel, "successful-head-task")

	response := performJSONRequest(engine, http.MethodHead, "http://gateway.test/v1/videos/successful-head-task/content", "")
	if response.Code != http.StatusOK || response.Body.Len() != 0 {
		t.Fatalf("HEAD response = %d %q", response.Code, response.Body.String())
	}
	if strings.Join(methods, ",") != http.MethodHead {
		t.Fatalf("upstream methods = %v, want HEAD only", methods)
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 0 || len(rows) != 0 {
		t.Fatalf("HEAD on video content must not produce audit rows: total=%d rows=%+v", total, rows)
	}
}

func TestPlaygroundProfileVideoPollingCoalescesStatusRequests(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	channel := saveVideoPollingTestChannel(t, "profile-coalesce-channel", "http://upstream.test")

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.GET("/api/playground/video-status", handlePlaygroundVideoStatus)

	parentEntry, err := audit.Start("admin_action", "127.0.0.1", http.MethodPost, "/api/playground/run", nil)
	if err != nil {
		t.Fatal(err)
	}
	parentEntry.RecordResult(http.StatusOK, []byte(`{"status":"ok","task_id":"profile-task-1"}`), nil)
	parentID := parentEntry.ID

	mapping := db.TaskMapping{
		TaskID:          "profile-task-1",
		ChannelID:       channel.ID,
		OriginRequestID: parentID,
		TaskKind:        "video",
		TaskAlias:       "profile-task-1",
	}
	if err := db.RecordTaskMappingContext(context.Background(), mapping); err != nil {
		t.Fatal(err)
	}

	run := &db.TaskRun{
		ID:              "pr_test_coalesce_run",
		OriginRequestID: parentID,
		TaskKind:        "video",
		Operation:       "video.create",
		ChannelID:       channel.ID,
		Engine:          "profile",
		PollingMode:     "background",
		ProviderTaskID:  "profile-task-1",
		TaskStatus:      "processing",
		TaskOutcome:     "pending",
	}
	if err := db.CreateTaskRunContext(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAliasContext(context.Background(), &db.TaskAlias{
		TaskRunID: run.ID,
		LookupID:  "profile-task-1",
		Source:    "create",
	}); err != nil {
		t.Fatal(err)
	}

	poll1 := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/video-status?task_id=profile-task-1", "")
	if poll1.Code != http.StatusOK {
		t.Fatalf("poll1 code = %d %s", poll1.Code, poll1.Body.String())
	}

	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].ID != parentID {
		t.Fatalf("poll1 was not coalesced: total %d rows %+v err %v", total, rows, err)
	}

	_ = db.UpdateTaskRunStatusContext(context.Background(), run.ID, "completed", "success")

	poll2 := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/video-status?task_id=profile-task-1", "")
	if poll2.Code != http.StatusOK {
		t.Fatalf("poll2 code = %d %s", poll2.Code, poll2.Body.String())
	}

	rows, total, err = audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].ID != parentID {
		t.Fatalf("poll2 was not coalesced: total %d rows %+v err %v", total, rows, err)
	}

	detail, err := audit.GetDetail(parentID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncPollCount != 2 || detail.Log.AsyncTaskStatus != "completed" {
		t.Fatalf("unexpected parent detail: pollCount=%d status=%s", detail.Log.AsyncPollCount, detail.Log.AsyncTaskStatus)
	}
}

func TestLegacyPublicVideoContentSafeMIMERestrictionAndSandboxCSP(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	var upstreamContentType string
	var upstreamBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", upstreamContentType)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	t.Cleanup(upstream.Close)

	channel := saveVideoPollingTestChannel(t, "legacy-video-channel", upstream.URL)

	testCases := []struct {
		name             string
		upstreamType     string
		upstreamPayload  string
		expectedType     string
		expectAttachment bool
	}{
		{
			name:             "upstream returns HTML script injection",
			upstreamType:     "text/html; charset=utf-8",
			upstreamPayload:  "<html><script>alert(document.domain)</script></html>",
			expectedType:     "application/octet-stream",
			expectAttachment: true,
		},
		{
			name:             "upstream returns JavaScript",
			upstreamType:     "application/javascript",
			upstreamPayload:  "window.__injected = true;",
			expectedType:     "application/octet-stream",
			expectAttachment: true,
		},
		{
			name:             "upstream returns legitimate MP4",
			upstreamType:     "video/mp4",
			upstreamPayload:  "\x00\x00\x00\x18ftypmp42",
			expectedType:     "video/mp4",
			expectAttachment: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskID := fmt.Sprintf("vid-%s", strings.ReplaceAll(tc.name, " ", "-"))
			engine := prepareVideoContentRoute(t, channel, taskID)
			upstreamContentType = tc.upstreamType
			upstreamBody = tc.upstreamPayload

			resp := performJSONRequest(engine, http.MethodGet, "http://gateway.test/v1/videos/"+taskID+"/content", "")
			if resp.Code != http.StatusOK {
				t.Fatalf("expected 200 OK, got %d body=%s", resp.Code, resp.Body.String())
			}

			// Verify Sandbox CSP and security headers
			csp := resp.Header().Get("Content-Security-Policy")
			if !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "sandbox") {
				t.Fatalf("missing or weak Content-Security-Policy: %q", csp)
			}
			if got := resp.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Fatalf("missing X-Content-Type-Options nosniff: %q", got)
			}
			if got := resp.Header().Get("Cross-Origin-Resource-Policy"); got != "cross-origin" {
				t.Fatalf("missing Cross-Origin-Resource-Policy: %q", got)
			}

			// Verify MIME type sanitization
			if got := resp.Header().Get("Content-Type"); got != tc.expectedType {
				t.Fatalf("Content-Type = %q, want %q", got, tc.expectedType)
			}

			// Verify Content-Disposition
			disp := resp.Header().Get("Content-Disposition")
			if tc.expectAttachment {
				if !strings.Contains(disp, "attachment") {
					t.Fatalf("expected attachment disposition for unsafe MIME, got %q", disp)
				}
			}
		})
	}
}
