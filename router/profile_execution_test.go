package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/db"
	relaymedia "relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

func TestLoadPublishedProfileOperation(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-binding.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	source, err := protocol.BuiltinPreset(protocol.PresetOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	const profileID = "shared-loader-test"
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: profileID, Name: "Shared Loader", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	for _, revision := range []struct {
		number int
		state  string
	}{{1, db.ProfileRevisionPublished}, {2, db.ProfileRevisionDraft}} {
		if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{
			ProfileID: profileID, Revision: revision.number, State: revision.state,
			SchemaVersion: source.SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	binding := &db.ChannelProtocolBinding{ProfileID: profileID, ProfileRevision: 1, Operation: source.Operations[0].Operation}
	revision, loaded, op, err := loadPublishedProfileOperation(context.Background(), binding)
	if err != nil || revision.Revision != 1 || loaded.Digest() != compiled.Digest() || op.Operation != binding.Operation {
		t.Fatalf("published operation: revision=%+v operation=%q err=%v", revision, op.Operation, err)
	}
	binding.Operation = "missing.operation"
	if _, _, _, err := loadPublishedProfileOperation(context.Background(), binding); err == nil || !strings.Contains(err.Error(), "is missing") {
		t.Fatalf("missing operation error = %v", err)
	}
	binding.ProfileRevision = 2
	if _, _, _, err := loadPublishedProfileOperation(context.Background(), binding); err == nil || !strings.Contains(err.Error(), "is not published") {
		t.Fatalf("draft revision error = %v", err)
	}
	binding.ProfileRevision = 99
	if _, _, _, err := loadPublishedProfileOperation(context.Background(), binding); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("missing revision error = %v", err)
	}
}

func TestProfileClientPollBudgetStopsBeforeProviderRequest(t *testing.T) {
	op := protocol.Operation{Poll: &protocol.Poll{MaxAttempts: 2}}
	run := &db.TaskRun{PollCount: 2}
	if reason, exceeded := profileClientPollBudget(run, op); !exceeded || reason == "" {
		t.Fatalf("attempt budget reason=%q exceeded=%v", reason, exceeded)
	}
	deadline := time.Now().Add(-time.Second)
	run = &db.TaskRun{PollCount: 1, DeadlineAt: &deadline}
	if reason, exceeded := profileClientPollBudget(run, op); !exceeded || reason == "" {
		t.Fatalf("deadline budget reason=%q exceeded=%v", reason, exceeded)
	}
}

func TestProfileClientPollRequiredMediaWaitsForLocalAsset(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-client-media-key")
	if err := db.InitDB(t.TempDir() + "/profile-client-media.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var pollCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/generations/profile-media-provider-task" {
			t.Fatalf("unexpected poll request: %s %s", r.Method, r.URL.Path)
		}
		pollCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"completed","video_url":"https://cdn.example/profile-media.mp4"}`)
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-client-media-channel", Name: "Profile Client Media", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: protocol.MediaRetentionRequired,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-client-media", Name: "Profile Client Media", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-client-media", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	run := &db.TaskRun{ID: "profile-client-media-run", TaskKind: asyncTaskKindVideo, Operation: "video.create", ChannelID: channel.ID, Engine: "profile", ProfileID: "profile-client-media", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingClient, ProviderTaskID: "profile-media-provider-task", SubmissionState: "accepted", TaskStatus: model.VideoStatusProcessing, DeadlineAt: &deadline}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: run.ProviderTaskID}); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	statusContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	statusContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+run.ProviderTaskID, nil)
	status, handled, err := profileTaskStatus(statusContext, run.ProviderTaskID, asyncTaskKindVideo)
	if err != nil || !handled {
		t.Fatalf("required media client poll handled=%v err=%v", handled, err)
	}
	statusResponse, ok := status.(*model.VideoTaskResponse)
	if !ok || statusResponse.Status != "materializing" || statusResponse.VideoURL != "" || statusResponse.URL != "" {
		t.Fatalf("provider result leaked before local materialization: %#v", status)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != model.VideoStatusProcessing || loaded.TaskOutcome != "pending" || loaded.PollCount != 1 || loaded.PollSuccessCount != 1 || loaded.PollFailureCount != 0 {
		t.Fatalf("required media client lifecycle = %+v", loaded)
	}
	if pollCalls != 1 {
		t.Fatalf("expected one provider poll after completion, got %d", pollCalls)
	}
	assets, err := db.ListMediaAssetsForTaskRun(run.ID, asyncTaskKindVideo)
	if err != nil || len(assets) != 1 || assets[0].Status != db.MediaAssetPending {
		t.Fatalf("required media asset = %#v, %v", assets, err)
	}

	statusContext, _ = gin.CreateTestContext(httptest.NewRecorder())
	statusContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+run.ProviderTaskID, nil)
	status, handled, err = profileTaskStatus(statusContext, run.ProviderTaskID, asyncTaskKindVideo)
	if err != nil || !handled {
		t.Fatalf("materializing status handled=%v err=%v", handled, err)
	}
	if pollCalls != 1 {
		t.Fatalf("materializing status polled provider again: %d", pollCalls)
	}

	if err := db.DB.Model(&db.MediaAsset{}).Where("id = ?", assets[0].ID).Updates(map[string]any{"status": db.MediaAssetAvailable, "object_id": "profile-media-object"}).Error; err != nil {
		t.Fatal(err)
	}
	statusContext, _ = gin.CreateTestContext(httptest.NewRecorder())
	statusContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+run.ProviderTaskID, nil)
	status, handled, err = profileTaskStatus(statusContext, run.ProviderTaskID, asyncTaskKindVideo)
	if err != nil || !handled {
		t.Fatalf("available media status handled=%v err=%v", handled, err)
	}
	statusResponse, ok = status.(*model.VideoTaskResponse)
	if !ok || statusResponse.Status != model.VideoStatusCompleted || statusResponse.VideoURL == "" || statusResponse.URL == "" {
		t.Fatalf("available media status = %#v", status)
	}
	loaded, err = db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != model.VideoStatusCompleted || loaded.TaskOutcome != "success" || pollCalls != 1 {
		t.Fatalf("available media completion = %+v pollCalls=%d", loaded, pollCalls)
	}
}

func TestProfileClientPollErrorConsumesBudgetAndExpiresTask(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-client-poll-error.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/generations/provider-error-task" {
			t.Fatalf("unexpected poll request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `{"error":"temporary upstream failure"}`)
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-client-error-channel", Name: "Profile Client Error", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
		Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll:     &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-client-error", Name: "Profile Client Error", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-client-error", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	run := &db.TaskRun{ID: "profile-client-error-run", TaskKind: asyncTaskKindVideo, Operation: "video.create", ChannelID: channel.ID, Engine: "profile", ProfileID: "profile-client-error", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingClient, ProviderTaskID: "provider-error-task", SubmissionState: "accepted", TaskStatus: model.VideoStatusProcessing, DeadlineAt: &deadline}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: "provider-error-task"}); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	statusContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	statusContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/provider-error-task", nil)
	if _, handled, err := profileTaskStatus(statusContext, "provider-error-task", asyncTaskKindVideo); err == nil || !handled {
		t.Fatalf("client poll error handled=%v err=%v, want provider error", handled, err)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PollCount != 1 || loaded.PollFailureCount != 1 || loaded.TaskStatus != "expired" || loaded.TaskOutcome != "failed" {
		t.Fatalf("poll error lifecycle = %+v", loaded)
	}
	var attempts []db.TaskAttempt
	if err := db.DB.Where("task_run_id = ?", run.ID).Find(&attempts).Error; err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Outcome != "failed" || attempts[0].HTTPStatus != http.StatusBadGateway {
		t.Fatalf("poll error attempt = %+v", attempts)
	}
}

func TestProfileGatewayWaitPollFailurePersistsAcceptedTask(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-gateway-wait-error.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var submitCalls, pollCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/videos/generations":
			submitCalls++
			_, _ = io.WriteString(w, `{"id":"gateway-wait-error-task","status":"queued"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/gateway-wait-error-task":
			pollCalls++
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"error":"provider unavailable"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "gateway-wait-error-channel", Name: "Gateway Wait Error", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "gateway-wait-error-model"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingGatewayWait,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "gateway-wait-error-profile", Name: "Gateway Wait Error", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "gateway-wait-error-profile", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "video.create", ModelPattern: "gateway-wait-error-model", ProfileID: "gateway-wait-error-profile", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
	gin.SetMode(gin.TestMode)
	first, _ := gin.CreateTestContext(httptest.NewRecorder())
	first.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	first.Request.Header.Set("Idempotency-Key", "gateway-wait-error-key")
	if response, selected, handled, err := profileEngineVideoCreate(first, &model.VideoGenerationRequest{Model: "gateway-wait-error-model", Prompt: "retry"}); err == nil || !handled || selected == nil || response != nil {
		t.Fatalf("gateway wait error handled=%v selected=%v response=%#v err=%v", handled, selected, response, err)
	}
	if submitCalls != 1 || pollCalls != 1 {
		t.Fatalf("gateway wait error provider calls submit=%d poll=%d", submitCalls, pollCalls)
	}
	var runs []db.TaskRun
	if err := db.DB.Where("idempotency_key = ?", "gateway-wait-error-key").Find(&runs).Error; err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].SubmissionState != "accepted" || runs[0].ProviderTaskID != "gateway-wait-error-task" || runs[0].PollCount != 1 || runs[0].PollFailureCount != 1 || runs[0].LastHTTPStatus != http.StatusBadGateway {
		t.Fatalf("accepted task was not persisted: %+v", runs)
	}
	var attempts []db.TaskAttempt
	if err := db.DB.Where("task_run_id = ?", runs[0].ID).Find(&attempts).Error; err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Outcome != "failed" || attempts[0].HTTPStatus != http.StatusBadGateway {
		t.Fatalf("gateway wait attempt = %+v", attempts)
	}
	second, _ := gin.CreateTestContext(httptest.NewRecorder())
	second.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	second.Request.Header.Set("Idempotency-Key", "gateway-wait-error-key")
	response, selected, handled, err := profileEngineVideoCreate(second, &model.VideoGenerationRequest{Model: "gateway-wait-error-model", Prompt: "retry"})
	if err != nil || !handled || selected != nil || response == nil || response.TaskID != "gateway-wait-error-task" {
		t.Fatalf("idempotent gateway wait retry handled=%v selected=%v response=%#v err=%v", handled, selected, response, err)
	}
	if submitCalls != 1 || pollCalls != 1 {
		t.Fatalf("idempotent gateway wait retry submitted again: submit=%d poll=%d", submitCalls, pollCalls)
	}
}

func TestProfileGatewayWaitMediaFailureRemainsRetryable(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-gateway-wait-media.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var submitCalls, pollCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/videos/generations":
			submitCalls++
			_, _ = io.WriteString(w, `{"id":"gateway-wait-provider-task","status":"queued"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/gateway-wait-provider-task":
			pollCalls++
			_, _ = io.WriteString(w, `{"status":"completed","video_url":"https://cdn.example/gateway-wait.mp4"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "gateway-wait-media-channel", Name: "Gateway Wait Media", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "gateway-wait-model"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingGatewayWait, MediaRetention: protocol.MediaRetentionRequired,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "gateway-wait-media-profile", Name: "Gateway Wait Media", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "gateway-wait-media-profile", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "video.create", ModelPattern: "gateway-wait-model", ProfileID: "gateway-wait-media-profile", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	previousFetcher := profileMediaFetcherFactory
	profileMediaFetcherFactory = func() relaymedia.SourceFetcher {
		return relaymedia.HTTPSourceFetcher{URLValidator: func(string) error { return errors.New("materialization temporarily unavailable") }}
	}
	t.Cleanup(func() { profileMediaFetcherFactory = previousFetcher })
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	gin.SetMode(gin.TestMode)
	createContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	createContext.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	response, selected, handled, err := profileEngineVideoCreate(createContext, &model.VideoGenerationRequest{Model: "gateway-wait-model", Prompt: "retry"})
	if err != nil || !handled || selected == nil || response == nil {
		t.Fatalf("gateway wait create handled=%v selected=%v response=%#v err=%v", handled, selected, response, err)
	}
	if response.Status != "materializing" || response.VideoURL != "" || response.URL != "" {
		t.Fatalf("failed materialization leaked provider result: %#v", response)
	}
	if submitCalls != 1 || pollCalls != 1 {
		t.Fatalf("initial provider calls submit=%d poll=%d", submitCalls, pollCalls)
	}
	runID := profileTaskRunID(channel.ID, "gateway-wait-provider-task", "video.create")
	run, err := db.GetTaskRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.TaskStatus != model.VideoStatusProcessing || run.TaskOutcome != "pending" || !strings.Contains(run.ResultBody, "gateway-wait.mp4") {
		t.Fatalf("gateway wait retry state = %+v result=%q", run, run.ResultBody)
	}
	statusContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	statusContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/gateway-wait-provider-task", nil)
	status, handled, err := profileTaskStatus(statusContext, "gateway-wait-provider-task", asyncTaskKindVideo)
	if err != nil || !handled {
		t.Fatalf("gateway wait status handled=%v err=%v", handled, err)
	}
	statusResponse, ok := status.(*model.VideoTaskResponse)
	if !ok || statusResponse.VideoURL != "" || statusResponse.Status != "materializing" {
		t.Fatalf("retry status leaked provider result: %#v", status)
	}
	if submitCalls != 1 || pollCalls != 1 {
		t.Fatalf("retry provider calls submit=%d poll=%d", submitCalls, pollCalls)
	}
}

func TestPersistableProfileVideoResponseStripsManagedCapability(t *testing.T) {
	response := &model.VideoTaskResponse{TaskID: "task", VideoURL: "https://gateway.test/v1/media/public/capability", URL: "https://gateway.test/v1/media/public/capability", Data: []map[string]string{{"url": "https://gateway.test/v1/media/public/capability"}}}
	persisted := persistableProfileVideoResponse(response)
	if persisted == response {
		t.Fatal("expected a defensive copy")
	}
	if persisted.VideoURL != "" || persisted.URL != "" || persisted.Data != nil {
		t.Fatalf("managed capability remained in persisted response: %#v", persisted)
	}
	if response.VideoURL == "" || response.URL == "" {
		t.Fatal("original response was modified")
	}
}

func TestPersistableProfileImageResponseStripsNestedManagedCapability(t *testing.T) {
	managedURL := "https://gateway.test/v1/media/public/capability"
	response := map[string]any{
		"data":   []map[string]string{{"url": managedURL}},
		"raw":    map[string]any{"nested": managedURL},
		"status": "completed",
	}
	persisted := persistableProfileImageResponse(response)
	encoded, err := json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "capability") {
		t.Fatalf("managed capability remained in persisted image response: %s", encoded)
	}
	if response["raw"].(map[string]any)["nested"] != managedURL {
		t.Fatal("original image response was modified")
	}
}

func TestProfileEngineVideoCreateCapturesRevisionAndTaskMapping(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-execution.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/videos/generations" {
			t.Fatalf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "profile-provider-task", "status": "completed", "video_url": "https://cdn.example/submit-result.mp4"})
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-exec-channel", Name: "profile-exec-channel", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "profile-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "profile-video-model"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Name: "test", Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: protocol.MediaRetentionBestEffort,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
		Response: protocol.Response{TaskIDPaths: []string{"id", "task_id"}, ResultPaths: []string{"video_url"}},
		Poll:     &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-exec", Name: "Profile Exec", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-exec", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "video.create", ModelPattern: "profile-video-model", ProfileID: "profile-exec", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	c.Request.Header.Set("Idempotency-Key", "profile-video-idempotency")
	resp, selected, handled, err := profileEngineVideoCreate(c, &model.VideoGenerationRequest{Model: "profile-video-model", Prompt: "render"})
	if err != nil {
		t.Fatal(err)
	}
	if !handled || selected == nil || selected.ID != channel.ID {
		t.Fatalf("profile execution selection = handled:%v channel:%v", handled, selected)
	}
	if resp == nil || resp.TaskID != "profile-provider-task" || resp.Status != model.VideoStatusCompleted || resp.VideoURL != "https://cdn.example/submit-result.mp4" {
		t.Fatalf("unexpected profile response: %#v", resp)
	}
	createdRun, err := db.GetTaskRunByAlias("profile-provider-task")
	if err != nil {
		t.Fatal(err)
	}
	assets, err := db.ListMediaAssetsForTaskRun(createdRun.ID, asyncTaskKindVideo)
	if err != nil || len(assets) != 1 || assets[0].Status != db.MediaAssetPending || assets[0].SourceLocator != "https://cdn.example/submit-result.mp4" {
		t.Fatalf("submit-result media assets = %#v, err=%v", assets, err)
	}
	var jobs []db.MediaMaterializationJob
	if err := db.DB.Where("asset_id = ?", assets[0].ID).Find(&jobs).Error; err != nil || len(jobs) != 1 || jobs[0].Status != db.MediaJobPending {
		t.Fatalf("submit-result media jobs = %#v, err=%v", jobs, err)
	}
	channel.Enabled = false
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.RemoveChannel(channel.ID)
	second, _ := gin.CreateTestContext(httptest.NewRecorder())
	second.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	second.Request.Header.Set("Idempotency-Key", "profile-video-idempotency")
	secondResp, secondChannel, secondHandled, secondErr := profileEngineVideoCreate(second, &model.VideoGenerationRequest{Model: "profile-video-model", Prompt: "render"})
	if secondErr != nil || !secondHandled || secondChannel != nil {
		t.Fatalf("idempotent retry handled=%v channel=%v err=%v", secondHandled, secondChannel, secondErr)
	}
	if secondResp == nil || secondResp.TaskID != resp.TaskID || upstreamCalls != 1 {
		t.Fatalf("idempotent retry submitted again: response=%#v upstream_calls=%d", secondResp, upstreamCalls)
	}
	var runs []db.TaskRun
	if err := db.DB.Where("idempotency_key = ?", "profile-video-idempotency").Find(&runs).Error; err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected one durable idempotent TaskRun, got %d", len(runs))
	}
	run := &runs[0]
	if run.Engine != "profile" || run.ProfileID != "profile-exec" || run.ProfileRevision != 1 || run.ProfileDigest != compiled.Digest() {
		t.Fatalf("profile snapshot not captured: %#v", run)
	}
	if mapping := db.GetTaskMappingForKind("profile-provider-task", asyncTaskKindVideo); mapping == nil || mapping.ChannelID != channel.ID {
		t.Fatalf("profile task mapping missing: %#v", mapping)
	}
}

func TestProfileEngineVideoCreateRejectsAsyncResponseWithoutTaskID(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-execution-no-task.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/videos/generations" {
			t.Fatalf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"queued"}`)
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-no-task-channel", Name: "profile-no-task-channel", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "profile-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "profile-no-task-model"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Name: "no task", Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
		Response: protocol.Response{TaskIDPaths: []string{"id", "task_id"}},
		Poll:     &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-no-task", Name: "No Task", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-no-task", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "video.create", ModelPattern: "profile-no-task-model", ProfileID: "profile-no-task", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/videos", nil)
	response, selected, handled, err := profileEngineVideoCreate(c, &model.VideoGenerationRequest{Model: "profile-no-task-model", Prompt: "render"})
	if response != nil || selected == nil || selected.ID != channel.ID || !handled || err == nil || !strings.Contains(err.Error(), "no task ID") {
		t.Fatalf("missing task ID result=%#v selected=%#v handled=%v err=%v", response, selected, handled, err)
	}
}

func TestProfileEngineVideoCreateFailsClosedAfterAmbiguousSubmit(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-execution-ambiguous.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var upstreamCalls int
	var upstreamCallsMu sync.Mutex
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCallsMu.Lock()
		upstreamCalls++
		upstreamCallsMu.Unlock()
		if r.Method != http.MethodPost || r.URL.Path != "/v1/videos/generations" {
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
			return
		}
		// The provider has received the request but the connection closes before
		// a response. The executor must classify this as submission_unknown.
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijacking unavailable", http.StatusInternalServerError)
			return
		}
		conn, _, err := hijacker.Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-ambiguous-channel", Name: "profile-ambiguous-channel", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "profile-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "profile-ambiguous-model"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Name: "ambiguous submit", Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
		Response: protocol.Response{TaskIDPaths: []string{"id", "task_id"}},
		Poll:     &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-ambiguous", Name: "Ambiguous Submit", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-ambiguous", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "video.create", ModelPattern: "profile-ambiguous-model", ProfileID: "profile-ambiguous", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
	gin.SetMode(gin.TestMode)
	first, _ := gin.CreateTestContext(httptest.NewRecorder())
	first.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	first.Request.Header.Set("Idempotency-Key", "profile-ambiguous-key")
	response, selected, handled, err := profileEngineVideoCreate(first, &model.VideoGenerationRequest{Model: "profile-ambiguous-model", Prompt: "render"})
	if response != nil || selected == nil || selected.ID != channel.ID || !handled || err == nil {
		t.Fatalf("ambiguous submit result=%#v selected=%#v handled=%v err=%v", response, selected, handled, err)
	}
	var run db.TaskRun
	if err := db.DB.Where("idempotency_key = ?", "profile-ambiguous-key").First(&run).Error; err != nil {
		t.Fatal(err)
	}
	if run.SubmissionState != "submission_unknown" || run.ProviderTaskID != "" {
		t.Fatalf("ambiguous task state = %+v, want submission_unknown without provider ID", run)
	}
	second, _ := gin.CreateTestContext(httptest.NewRecorder())
	second.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	second.Request.Header.Set("Idempotency-Key", "profile-ambiguous-key")
	secondResponse, secondChannel, secondHandled, secondErr := profileEngineVideoCreate(second, &model.VideoGenerationRequest{Model: "profile-ambiguous-model", Prompt: "render"})
	if secondResponse != nil || secondChannel != nil || !secondHandled || secondErr == nil {
		t.Fatalf("ambiguous retry result=%#v selected=%#v handled=%v err=%v", secondResponse, secondChannel, secondHandled, secondErr)
	}
	upstreamCallsMu.Lock()
	gotUpstreamCalls := upstreamCalls
	upstreamCallsMu.Unlock()
	if gotUpstreamCalls != 1 {
		t.Fatalf("ambiguous submit was retried upstream %d times, want 1", gotUpstreamCalls)
	}
}

func TestProfileVideoContentUsesCapturedProfileWithoutLegacyAdapter(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-content.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var contentCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/videos/generations":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "profile-content-task", "status": "queued"})
		case (r.Method == http.MethodGet || r.Method == http.MethodHead) && r.URL.Path == "/v1/videos/generations/profile-content-task/content":
			contentCalls++
			w.Header().Set("Content-Type", "video/mp4")
			if r.Header.Get("Range") == "bytes=0-6" {
				w.Header().Set("Content-Range", "bytes 0-6/19")
				w.WriteHeader(http.StatusPartialContent)
			}
			_, _ = io.WriteString(w, "profile-video-bytes")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-content-channel", Name: "Profile Content", Type: "openai", BaseURL: upstream.URL + "/v1", APIKey: "profile-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "profile-content-model"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Name: "Profile Content", Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: protocol.MediaRetentionBestEffort,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
		Response: protocol.Response{TaskIDPaths: []string{"id", "task_id"}},
		Poll:     &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"url"}},
		Content:  &protocol.Content{Method: http.MethodGet, Path: "/videos/generations/{task_id}/content"},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-content", Name: "Profile Content", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-content", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "video.create", ModelPattern: "profile-content-model", ProfileID: "profile-content", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
	gin.SetMode(gin.TestMode)
	createContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	createContext.Request = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/videos", nil)
	response, _, handled, err := profileEngineVideoCreate(createContext, &model.VideoGenerationRequest{Model: "profile-content-model", Prompt: "a cat"})
	if err != nil || !handled || response == nil || response.TaskID != "profile-content-task" {
		t.Fatalf("profile content create handled=%v response=%#v err=%v", handled, response, err)
	}
	contentRecorder := httptest.NewRecorder()
	contentContext, _ := gin.CreateTestContext(contentRecorder)
	contentContext.Request = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/videos/profile-content-task/content", nil)
	if handled, err := profileTaskContent(contentContext, "profile-content-task"); err != nil || !handled {
		t.Fatalf("profile content handled=%v err=%v", handled, err)
	}
	if contentRecorder.Code != http.StatusOK || contentRecorder.Body.String() != "profile-video-bytes" {
		t.Fatalf("profile content status=%d body=%q", contentRecorder.Code, contentRecorder.Body.String())
	}
	if contentCalls != 1 {
		t.Fatalf("profile content calls=%d, want 1", contentCalls)
	}

	// HEAD must preserve the provider status/headers without copying a body.
	headRecorder := httptest.NewRecorder()
	headContext, _ := gin.CreateTestContext(headRecorder)
	headContext.Params = gin.Params{{Key: "id", Value: "profile-content-task"}}
	headContext.Request = httptest.NewRequestWithContext(context.Background(), http.MethodHead, "/v1/videos/profile-content-task/content", nil)
	if handled, err := profileTaskContent(headContext, "profile-content-task"); err != nil || !handled {
		t.Fatalf("profile HEAD content handled=%v err=%v", handled, err)
	}
	if headRecorder.Code != http.StatusOK || headRecorder.Body.Len() != 0 || headRecorder.Header().Get("Content-Type") != "video/mp4" {
		t.Fatalf("profile HEAD response status=%d body=%q headers=%v", headRecorder.Code, headRecorder.Body.String(), headRecorder.Header())
	}

	// Range is forwarded to the frozen Profile content operation and the
	// provider's partial response headers are preserved.
	rangeRecorder := httptest.NewRecorder()
	rangeContext, _ := gin.CreateTestContext(rangeRecorder)
	rangeContext.Params = gin.Params{{Key: "id", Value: "profile-content-task"}}
	rangeContext.Request = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/videos/profile-content-task/content", nil)
	rangeContext.Request.Header.Set("Range", "bytes=0-6")
	if handled, err := profileTaskContent(rangeContext, "profile-content-task"); err != nil || !handled {
		t.Fatalf("profile range content handled=%v err=%v", handled, err)
	}
	if rangeRecorder.Code != http.StatusPartialContent || rangeRecorder.Body.String() != "profile-video-bytes" || rangeRecorder.Header().Get("Content-Range") != "bytes 0-6/19" {
		t.Fatalf("profile range response status=%d body=%q headers=%v", rangeRecorder.Code, rangeRecorder.Body.String(), rangeRecorder.Header())
	}
	if contentCalls != 3 {
		t.Fatalf("profile content calls=%d, want 3 after GET/HEAD/range", contentCalls)
	}
}

func TestSafeProfileContentRedirectRejectsPrivateTargets(t *testing.T) {
	tests := []struct {
		name     string
		baseURL  string
		location string
		want     string
		wantErr  bool
	}{
		{name: "public", baseURL: "https://provider.example/v1", location: "https://cdn.example/video.mp4"},
		{name: "loopback", baseURL: "https://provider.example/v1", location: "http://127.0.0.1/video.mp4", wantErr: true},
		{name: "private", baseURL: "https://provider.example/v1", location: "http://10.0.0.4/video.mp4", wantErr: true},
		{name: "userinfo", baseURL: "https://provider.example/v1", location: "https://user:pass@cdn.example/video.mp4", wantErr: true},
		{name: "relative", baseURL: "https://provider.example/v1", location: "/video.mp4", want: "https://provider.example/video.mp4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := safeProfileContentRedirect(tt.baseURL, tt.location)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("redirect %q accepted as %q", tt.location, got)
				}
				return
			}
			want := tt.want
			if want == "" {
				want = tt.location
			}
			if err != nil || got != want {
				t.Fatalf("redirect = %q, err=%v; want %q", got, err, want)
			}
		})
	}
}
