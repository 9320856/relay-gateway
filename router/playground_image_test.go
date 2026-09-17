package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/audit"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

func TestPlaygroundProfileImageTaskReturnsTaskThenPollsResult(t *testing.T) {
	t.Setenv("RELAY_ENABLE_PROFILE_IMAGE_ENGINE", "1")
	initAsyncTaskRecoveryTestDB(t)
	var submitted map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/images/jobs" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&submitted)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job":{"id":"playground-image-task","status":"queued"}}`))
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "playground-profile-image-channel", Name: "Profile image", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "profile-image-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "gpt-image-2"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Name: "playground image", Operations: []protocol.Operation{{
		Operation: "images.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingBackground, MediaRetention: protocol.MediaRetentionDisabled,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/images/jobs", BodyEncoding: "json"},
		Response: protocol.Response{TaskIDPaths: []string{"job.id"}},
		Poll:     &protocol.Poll{Method: http.MethodGet, Path: "/images/jobs/{task_id}", IntervalMS: 5000, MaxAttempts: 20, MaxDurationMS: 120000, StatusPath: "job.status", SuccessValues: []string{"succeeded"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"job.assets"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "playground-profile-image", Name: "Playground image", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "playground-profile-image", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "images.create", ModelPattern: "gpt-image-2", ProfileID: "playground-profile-image", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/api/playground/run", handlePlaygroundRun)
	engine.GET("/api/playground/image-status", handlePlaygroundImageStatus)
	create := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"image","channel_id":"`+channel.ID+`","model":"gpt-image-2","prompt":"puppy","size":"16:9"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create response = %d %s", create.Code, create.Body.String())
	}
	var created map[string]any
	if err := json.Unmarshal(create.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created["type"] != "image_task" || created["task_id"] != "playground-image-task" || created["task_status"] != "queued" {
		t.Fatalf("async image was not returned as a task: %#v", created)
	}
	if strings.Contains(created["response"].(string), "0 张") {
		t.Fatalf("async task was reported as an empty success: %#v", created)
	}
	if submitted["size"] != "1280x720" {
		t.Fatalf("profile image request skipped shared normalization: %#v", submitted)
	}

	run, err := db.GetTaskRunByAlias(imageTaskIDPrefix + "playground-image-task")
	if err != nil {
		t.Fatal(err)
	}
	resultBody := `{"job":{"id":"playground-image-task","status":"succeeded","assets":[{"thumbnail_url":"https://cdn.example/image-thumbnail.png"}]}}`
	if err := db.UpdateTaskRunResultContext(context.Background(), run.ID, resultBody, false); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateTaskRunStatusContext(context.Background(), run.ID, "completed", "success"); err != nil {
		t.Fatal(err)
	}
	poll := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/image-status?task_id=playground-image-task&channel_id=wrong-channel", "")
	if poll.Code != http.StatusOK {
		t.Fatalf("poll response = %d %s", poll.Code, poll.Body.String())
	}
	var polled map[string]any
	if err := json.Unmarshal(poll.Body.Bytes(), &polled); err != nil {
		t.Fatal(err)
	}
	images, _ := polled["images"].([]any)
	if polled["task_status"] != "completed" || len(images) != 1 || images[0] != "https://cdn.example/image-thumbnail.png" {
		t.Fatalf("completed image poll = %#v", polled)
	}
	rows, total, err := audit.List(audit.ListQuery{Page: 1, PageSize: 10})
	if err != nil || total != 1 || len(rows) != 1 || rows[0].AsyncPollCount != 1 || rows[0].AsyncTaskStatus != "completed" {
		t.Fatalf("image status poll was not coalesced: total=%d rows=%+v err=%v", total, rows, err)
	}
}

func TestPlaygroundProfileImagePollShowsPersistedPreviewWhileMaterializing(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "playground-preview-materializing-test-key")
	initAsyncTaskRecoveryTestDB(t)
	var pollCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pollCalls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/images/jobs/provider-preview-task" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job":{"id":"provider-preview-task","status":"succeeded","assets":[{"proxy_url":"https://cdn.example/image.png","thumbnail_url":"https://cdn.example/thumb.png"}]}}`))
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "playground-preview-channel", Name: "Preview image", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "image-model"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "images.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: protocol.MediaRetentionRequired,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/images/jobs", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"job.id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/images/jobs/{task_id}", IntervalMS: 1000, MaxAttempts: 5, MaxDurationMS: 60000, StatusPath: "job.status", SuccessValues: []string{"succeeded"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"job.assets"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "playground-preview-profile", Name: "Playground preview", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "playground-preview-profile", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	run := &db.TaskRun{ID: "playground-preview-run", TaskKind: asyncTaskKindImage, Operation: "images.create", ChannelID: channel.ID, Engine: "profile", ProfileID: "playground-preview-profile", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingClient, ProviderTaskID: "provider-preview-task", SubmissionState: "accepted", TaskStatus: "processing", TaskOutcome: "pending", DeadlineAt: &deadline}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: imageTaskIDPrefix + "preview-task"}); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.GET("/api/playground/image-status", handlePlaygroundImageStatus)
	poll := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/image-status?task_id=preview-task", "")
	if poll.Code != http.StatusOK {
		t.Fatalf("poll response = %d %s", poll.Code, poll.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(poll.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	images, _ := result["images"].([]any)
	if result["task_status"] != "processing" || len(images) != 1 || images[0] != "https://cdn.example/image.png" {
		t.Fatalf("materializing preview response = %#v", result)
	}
	if pollCalls != 1 {
		t.Fatalf("provider poll calls = %d, want 1", pollCalls)
	}
	repeated := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/image-status?task_id=preview-task", "")
	if repeated.Code != http.StatusOK || pollCalls != 1 {
		t.Fatalf("repeated materializing poll = %d %s, provider calls=%d", repeated.Code, repeated.Body.String(), pollCalls)
	}
	assets, err := db.ListMediaAssetsForTaskRun(run.ID, asyncTaskKindImage)
	if err != nil || len(assets) != 1 {
		t.Fatalf("required preview media assets = %+v, %v", assets, err)
	}
}

func TestMaterializeProfileImageResponseAcceptsDataURL(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-inline-image-test-key")
	if err := db.InitDB(t.TempDir() + "/profile-inline-image.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		SetMediaObjectStore(nil)
		_ = db.Close()
	})
	store, err := media.NewLocalObjectStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "http://gateway.test/v1/images/generations", nil)
	dataURL := "data:image/png;base64,iVBORw0KGgo="
	response := profileImageResponse(protocol.Result{JSON: map[string]any{"data": []any{map[string]any{"url": dataURL}}}, ResultURLs: []string{dataURL}}, false)
	if err := materializeProfileImageResponse(c, "profile-inline-image-run", response, []string{dataURL}); err != nil {
		t.Fatal(err)
	}
	data, ok := response["data"].([]map[string]string)
	if !ok || len(data) != 1 || !strings.Contains(data[0]["url"], "/v1/media/") {
		t.Fatalf("managed image response = %#v", response)
	}
	assets, err := db.ListMediaAssetsForTaskRunContext(context.Background(), "profile-inline-image-run", "image")
	if err != nil || len(assets) != 1 || assets[0].SourceKind != media.SourceBase64 || assets[0].Status != db.MediaAssetAvailable || assets[0].ContentType != "image/png" {
		t.Fatalf("inline media asset = %#v, %v", assets, err)
	}
}
