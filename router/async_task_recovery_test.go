package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/audit"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/security"
	"relay-gateway/service"
)

func initAsyncTaskRecoveryTestDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "async-task-recovery.db")
	if err := db.InitDB(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		asyncTaskMappingRecoveries = asyncTaskMappingRecoveryQueue{}
	})
	return path
}

func TestDeferredAsyncTaskMappingIsCachedAndReplayedAfterRestart(t *testing.T) {
	dbPath := initAsyncTaskRecoveryTestDB(t)
	originalPersist := persistAsyncTaskMappingsFn
	persistAsyncTaskMappingsFn = func(*gin.Context, string, string, string, string, ...string) error {
		return errors.New("simulated mapping write failure")
	}
	t.Cleanup(func() { persistAsyncTaskMappingsFn = originalPersist })

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	registerAsyncTaskMappings(ctx, "recovery-channel", asyncTaskKindVideo, "public-video", "queued", "public-video", "provider-video")

	if got := recorder.Header().Get("X-Relay-Task-Mapping"); got != "pending" {
		t.Fatalf("deferred mapping header = %q, want pending", got)
	}
	if len(ctx.Errors) != 0 {
		t.Fatalf("deferred persistence must not turn the successful request into an error: %v", ctx.Errors)
	}
	if got := db.GetVideoTaskChannel("provider-video"); got != "recovery-channel" {
		t.Fatalf("cached provider alias routed to %q, want recovery-channel", got)
	}
	var count int64
	if err := db.DB.Model(&db.TaskMapping{}).Where("task_id IN ?", []string{"public-video", "provider-video"}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("simulated failed mappings unexpectedly reached SQLite: %d", count)
	}

	// Reset the process-local queue before replaying. The remaining journal is
	// the same input a newly started process receives.
	asyncTaskMappingRecoveries = asyncTaskMappingRecoveryQueue{}
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("replay deferred mappings: %v", err)
	}
	if err := db.DB.Model(&db.TaskMapping{}).Where("task_id IN ?", []string{"public-video", "provider-video"}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("replay persisted %d mappings, want both aliases", count)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if got := db.GetVideoTaskChannel("provider-video"); got != "recovery-channel" {
		t.Fatalf("replayed mapping did not survive restart: %q", got)
	}
}

func TestDeferredMappingIsNotCachedUntilRecoveryJournalIsDurable(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	originalPersist := persistAsyncTaskMappingsFn
	persistAsyncTaskMappingsFn = func(*gin.Context, string, string, string, string, ...string) error {
		return errors.New("simulated mapping write failure")
	}
	t.Cleanup(func() { persistAsyncTaskMappingsFn = originalPersist })

	journalPath := asyncTaskMappingRecoveryJournalPath()
	if err := os.Mkdir(journalPath, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(journalPath) })

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	registerAsyncTaskMappings(ctx, "journal-channel", asyncTaskKindVideo, "journal-video", "queued", "journal-video")

	if got := recorder.Header().Get("X-Relay-Task-Mapping"); got != "failed" {
		t.Fatalf("journal failure header = %q, want failed", got)
	}
	if got := db.GetVideoTaskChannel("journal-video"); got != "" {
		t.Fatalf("non-durable mapping was exposed from cache as %q", got)
	}

	if err := os.Remove(journalPath); err != nil {
		t.Fatal(err)
	}
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("retry journal and mapping after filesystem recovery: %v", err)
	}
	if got := db.GetVideoTaskChannel("journal-video"); got != "journal-channel" {
		t.Fatalf("recovered mapping routed to %q, want journal-channel", got)
	}
}

func TestUnreadableRecoveryJournalDoesNotAuthorizeCachedVideo(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	originalPersist := persistAsyncTaskMappingsFn
	persistAsyncTaskMappingsFn = func(*gin.Context, string, string, string, string, ...string) error {
		return errors.New("simulated mapping write failure")
	}
	t.Cleanup(func() { persistAsyncTaskMappingsFn = originalPersist })

	journalPath := asyncTaskMappingRecoveryJournalPath()
	// Scanner rejects this oversized record. The new valid record appended
	// after it cannot yet be guaranteed to replay on startup, so it must not
	// authorize public media through the in-memory mapping cache.
	if err := os.WriteFile(journalPath, []byte(strings.Repeat("x", 1024*1024+1)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)
	registerAsyncTaskMappings(ctx, "unreadable-journal-channel", asyncTaskKindVideo, "unreadable-journal-video", "queued", "unreadable-journal-video")

	if got := recorder.Header().Get("X-Relay-Task-Mapping"); got != "failed" {
		t.Fatalf("unreadable journal header = %q, want failed", got)
	}
	if got := db.GetVideoTaskChannel("unreadable-journal-video"); got != "" {
		t.Fatalf("unreadable journal exposed cached video route %q", got)
	}
}

func TestAsyncTaskRoutingDoesNotRollbackWhenCreationAuditFails(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	entry, err := audit.Start("api_call", "127.0.0.1", http.MethodPost, "/v1/videos", nil)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil).WithContext(audit.WithAudit(context.Background(), entry))

	originalAudit := recordAsyncTaskCreatedFn
	called := false
	recordAsyncTaskCreatedFn = func(context.Context, string, string, string) error {
		called = true
		return errors.New("simulated audit summary failure")
	}
	t.Cleanup(func() { recordAsyncTaskCreatedFn = originalAudit })

	if err := persistAsyncTaskMappings(ctx, "audit-channel", asyncTaskKindVideo, "audit-video", "queued", "audit-video", "audit-provider-video"); err != nil {
		t.Fatalf("audit failure must not roll back routing: %v", err)
	}
	if !called {
		t.Fatal("creation audit seam was not called")
	}
	for _, taskID := range []string{"audit-video", "audit-provider-video"} {
		mapping := db.GetTaskMapping(taskID)
		if mapping == nil || mapping.ChannelID != "audit-channel" || mapping.OriginRequestID != entry.ID {
			t.Fatalf("mapping %s = %+v, want preserved routing and origin audit ID", taskID, mapping)
		}
	}
}

func TestCreateVideoKeepsUpstreamSuccessWhenMappingIsDeferred(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/videos/generations" {
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"deferred-video","status":"queued"}`))
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "deferred-video-channel",
		Name:      "Deferred video test",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "upstream-test-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "deferred-video-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	originalPersist := persistAsyncTaskMappingsFn
	persistAsyncTaskMappingsFn = func(*gin.Context, string, string, string, string, ...string) error {
		return errors.New("simulated mapping write failure")
	}
	t.Cleanup(func() { persistAsyncTaskMappingsFn = originalPersist })

	engine := gin.New()
	engine.POST("/v1/videos", handleCreateVideo)
	request := httptest.NewRequest(http.MethodPost, "/v1/videos", strings.NewReader(`{"model":"deferred-video-model","prompt":"test"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("upstream-created video returned %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Relay-Task-Mapping") != "pending" {
		t.Fatalf("successful response did not disclose deferred mapping: headers=%v", response.Header())
	}
	if !strings.Contains(response.Body.String(), "deferred-video") {
		t.Fatalf("successful upstream task ID was not preserved: %s", response.Body.String())
	}
	if upstreamCalls != 1 {
		t.Fatalf("upstream create calls = %d, want exactly one", upstreamCalls)
	}
	if got := db.GetVideoTaskChannel("deferred-video"); got != channel.ID {
		t.Fatalf("immediate cached route = %q, want %q", got, channel.ID)
	}
}

func TestGetVideoKeepsUpstreamSuccessWhenAliasPersistenceIsDeferred(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/generations/status-public-video" {
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"status-public-video","task_id":"status-provider-video","status":"completed","video_url":"https://cdn.example/video.mp4?signature=temporary"}`))
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "status-alias-channel",
		Name:      "Deferred alias status test",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "upstream-test-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "status-alias-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	const (
		publicTaskID    = "status-public-video"
		providerTaskID  = "status-provider-video"
		originRequestID = "origin-status-video"
	)
	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:          publicTaskID,
		ChannelID:       channel.ID,
		OriginRequestID: originRequestID,
		TaskKind:        asyncTaskKindVideo,
		TaskAlias:       publicTaskID,
	}); err != nil {
		t.Fatal(err)
	}

	originalPersist := persistVideoTaskAliasRegistrationFn
	seenRegistration := false
	persistVideoTaskAliasRegistrationFn = func(registration asyncTaskMappingRegistration) error {
		seenRegistration = true
		if registration.ChannelID != channel.ID || registration.TaskKind != asyncTaskKindVideo || registration.TaskAlias != publicTaskID || registration.OriginRequestID != originRequestID || registration.InitialStatus != "" || len(registration.TaskIDs) != 1 || registration.TaskIDs[0] != providerTaskID {
			t.Errorf("unexpected deferred alias registration: %+v", registration)
		}
		return errors.New("simulated alias mapping write failure")
	}
	t.Cleanup(func() { persistVideoTaskAliasRegistrationFn = originalPersist })

	engine := gin.New()
	engine.GET("/v1/videos/:id", handleGetVideo)
	request := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID, nil)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("upstream-successful video status returned %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Relay-Task-Mapping") != "pending" {
		t.Fatalf("successful response did not disclose deferred alias mapping: headers=%v", response.Header())
	}
	if !seenRegistration {
		t.Fatal("alias persistence seam was not called")
	}
	if upstreamCalls != 1 {
		t.Fatalf("upstream status calls = %d, want exactly one", upstreamCalls)
	}

	var payload model.VideoTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Status != "completed" || !strings.Contains(payload.VideoURL, "/v1/videos/"+providerTaskID+"/content") || strings.Contains(payload.VideoURL, "cdn.example") {
		t.Fatalf("successful status response did not retain a stable video URL: %+v", payload)
	}

	if cached := db.GetTaskMapping(providerTaskID); cached == nil || cached.ChannelID != channel.ID || cached.OriginRequestID != originRequestID || cached.TaskAlias != publicTaskID || cached.TaskKind != asyncTaskKindVideo {
		t.Fatalf("journaled alias was not safely published to cache: %+v", cached)
	}
	var count int64
	if err := db.DB.Model(&db.TaskMapping{}).Where("task_id = ?", providerTaskID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed alias write unexpectedly reached SQLite: %d", count)
	}

	// Recovery uses the concrete persistence function, so it must not make a
	// second upstream request and must preserve the original creation context.
	persistVideoTaskAliasRegistrationFn = originalPersist
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("replay deferred video alias: %v", err)
	}
	var recovered db.TaskMapping
	if err := db.DB.First(&recovered, "task_id = ?", providerTaskID).Error; err != nil {
		t.Fatal(err)
	}
	if recovered.ChannelID != channel.ID || recovered.OriginRequestID != originRequestID || recovered.TaskAlias != publicTaskID || recovered.TaskKind != asyncTaskKindVideo {
		t.Fatalf("recovered alias lost original mapping provenance: %+v", recovered)
	}
	if upstreamCalls != 1 {
		t.Fatalf("recovery made %d upstream status calls, want exactly one", upstreamCalls)
	}
}

func TestCreateVideoCrossChannelProviderIDConflictDoesNotPublishStableContentURL(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	channelACalls := 0
	upstreamA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelACalls++
		http.Error(w, "channel A must not receive this create request", http.StatusInternalServerError)
	}))
	t.Cleanup(upstreamA.Close)

	channelBCalls := 0
	const (
		newPublicTaskID  = "new-public-video"
		sharedProviderID = "shared-provider-video"
		upstreamVideoURL = "https://cdn-b.example/video.mp4?signature=temporary"
	)
	upstreamB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelBCalls++
		if r.Method == http.MethodPost && r.URL.Path == "/v1/videos/generations" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"new-public-video","task_id":"shared-provider-video","status":"completed","video_url":"https://cdn-b.example/video.mp4?signature=temporary","url":"https://cdn-b.example/video.mp4?signature=temporary","data":[{"url":"https://cdn-b.example/video.mp4?signature=temporary","video_url":"https://cdn-b.example/video.mp4?signature=temporary"}]}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+newPublicTaskID {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"new-public-video","task_id":"shared-provider-video","status":"completed","video_url":"https://cdn-b.example/video.mp4?signature=temporary"}`))
			return
		}
		http.Error(w, "unexpected upstream request: "+r.Method+" "+r.URL.Path, http.StatusNotFound)
	}))
	t.Cleanup(upstreamB.Close)

	channelA := &db.ChannelModel{
		ID:        "conflict-channel-a",
		Name:      "Existing provider task channel",
		Type:      "newapi",
		BaseURL:   upstreamA.URL + "/v1",
		APIKey:    "channel-a-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "unrelated-model",
	}
	channelB := &db.ChannelModel{
		ID:        "conflict-channel-b",
		Name:      "New provider task channel",
		Type:      "newapi",
		BaseURL:   upstreamB.URL + "/v1",
		APIKey:    "channel-b-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "conflict-regression-model",
	}
	for _, channel := range []*db.ChannelModel{channelA, channelB} {
		if err := db.SaveChannelModel(channel); err != nil {
			t.Fatal(err)
		}
		service.DefaultDispatcher.ResetBreaker(channel.ID)
		channelID := channel.ID
		t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channelID) })
	}
	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:    sharedProviderID,
		ChannelID: channelA.ID,
		TaskKind:  asyncTaskKindVideo,
		TaskAlias: "existing-public-video",
	}); err != nil {
		t.Fatal(err)
	}

	engine := gin.New()
	engine.POST("/v1/videos", handleCreateVideo)
	engine.GET("/v1/videos/:id", handleGetVideo)
	request := httptest.NewRequest(http.MethodPost, "/v1/videos", strings.NewReader(`{"model":"conflict-regression-model","prompt":"test"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Host = "gateway.example.test"
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("already-created video must keep its upstream success response, got %d: %s", response.Code, response.Body.String())
	}
	if channelACalls != 0 || channelBCalls != 1 {
		t.Fatalf("upstream create routing = channelA:%d channelB:%d, want 0 and 1", channelACalls, channelBCalls)
	}

	var payload model.VideoTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	// With all public IDs covered in cross-channel collision check, the conflicting task_id
	// triggers gateway disambiguation into a gt_ ID.
	if !strings.HasPrefix(payload.ID, "gt_") || payload.TaskID != payload.ID {
		t.Fatalf("upstream task_id conflict was not disambiguated with gt_ prefix: %+v", payload)
	}
	if payload.ID == newPublicTaskID || payload.TaskID == sharedProviderID {
		t.Fatalf("conflicting IDs leaked into public task response: %+v", payload)
	}
	for _, candidate := range append([]string{payload.VideoURL, payload.URL}, dataURLs(payload.Data)...) {
		if strings.Contains(candidate, sharedProviderID) {
			t.Fatalf("cross-channel conflict exposed an unsafe provider ID in URL: %+v", payload)
		}
	}

	// Verify channel A's existing mapping remains intact
	shared := db.GetTaskMapping(sharedProviderID)
	if shared == nil || shared.ChannelID != channelA.ID {
		t.Fatalf("existing provider ID was overwritten by a conflicting create: %+v", shared)
	}

	// Verify channel B's disambiguated mapping was persisted
	gtMapping := db.GetTaskMapping(payload.ID)
	if gtMapping == nil || gtMapping.ChannelID != channelB.ID || gtMapping.TaskAlias != newPublicTaskID {
		t.Fatalf("disambiguated task mapping for channel B failed: %+v", gtMapping)
	}

	// Verify subsequent query routing for the disambiguated task ID routes to channel B
	getReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+payload.ID, nil)
	getReq.Host = "gateway.example.test"
	getResp := httptest.NewRecorder()
	engine.ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("subsequent query for disambiguated task failed, got %d: %s", getResp.Code, getResp.Body.String())
	}
	var getPayload model.VideoTaskResponse
	if err := json.Unmarshal(getResp.Body.Bytes(), &getPayload); err != nil {
		t.Fatal(err)
	}
	if getPayload.ID != payload.ID {
		t.Fatalf("query response ID = %q, want disambiguated %q", getPayload.ID, payload.ID)
	}
	if channelACalls != 0 || channelBCalls != 2 {
		t.Fatalf("subsequent query routing = channelA:%d channelB:%d, want 0 and 2", channelACalls, channelBCalls)
	}

	// Verify query for the colliding sharedProviderID routes to channel A
	getSharedReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+sharedProviderID, nil)
	getSharedReq.Host = "gateway.example.test"
	getSharedResp := httptest.NewRecorder()
	engine.ServeHTTP(getSharedResp, getSharedReq)
	if channelACalls != 1 {
		t.Fatalf("subsequent query for sharedProviderID did not route to channel A: channelACalls=%d", channelACalls)
	}
}

func TestGetVideoCrossChannelAliasConflictKeepsSuccessWithoutGatewayContentURL(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	channelACalls := 0
	channelBCalls := 0
	const (
		publicTaskID     = "conflict-status-public-video"
		newStatusAliasID = "conflict-status-new-alias-video"
		sharedProviderID = "conflict-status-provider-video"
		upstreamVideoURL = "https://cdn-b.example/status.mp4?signature=temporary"
	)
	upstreamA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelACalls++
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+sharedProviderID {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"` + sharedProviderID + `","status":"completed"}`))
			return
		}
		http.Error(w, "channel A unexpected request: "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
	}))
	t.Cleanup(upstreamA.Close)
	upstreamB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelBCalls++
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+publicTaskID {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"` + newStatusAliasID + `","task_id":"` + sharedProviderID + `","status":"completed","video_url":"https://cdn-b.example/status.mp4?signature=temporary","url":"https://cdn-b.example/status.mp4?signature=temporary","data":[{"url":"https://cdn-b.example/status.mp4?signature=temporary","video_url":"https://cdn-b.example/status.mp4?signature=temporary"}]}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+sharedProviderID+"/content" {
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = w.Write([]byte("mp4-content-bytes"))
			return
		}
		http.Error(w, "unexpected upstream request: "+r.Method+" "+r.URL.Path, http.StatusNotFound)
	}))
	t.Cleanup(upstreamB.Close)

	channelA := &db.ChannelModel{ID: "conflict-status-channel-a", Name: "Existing status provider task", Type: "newapi", BaseURL: upstreamA.URL + "/v1", APIKey: "channel-a-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "unrelated-model"}
	channelB := &db.ChannelModel{ID: "conflict-status-channel-b", Name: "Status lookup channel", Type: "newapi", BaseURL: upstreamB.URL + "/v1", APIKey: "channel-b-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "status-model"}
	for _, channel := range []*db.ChannelModel{channelA, channelB} {
		if err := db.SaveChannelModel(channel); err != nil {
			t.Fatal(err)
		}
		channelID := channel.ID
		t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channelID) })
	}
	if err := db.EnsureTaskMappings(
		db.TaskMapping{TaskID: publicTaskID, ChannelID: channelB.ID, OriginRequestID: "origin-conflict-status", TaskKind: asyncTaskKindVideo, TaskAlias: publicTaskID},
		db.TaskMapping{TaskID: sharedProviderID, ChannelID: channelA.ID, TaskKind: asyncTaskKindVideo, TaskAlias: "existing-provider-video"},
	); err != nil {
		t.Fatal(err)
	}

	engine := gin.New()
	engine.GET("/v1/videos/:id", handleGetVideo)
	engine.GET("/v1/videos/:id/content", handleGetVideoContent)
	request := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID, nil)
	request.Host = "gateway.example.test"
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("upstream-successful status must remain successful on an alias conflict, got %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Relay-Task-Mapping") != "conflict" {
		t.Fatalf("cross-channel alias conflict was not disclosed: headers=%v", response.Header())
	}
	if channelACalls != 0 || channelBCalls != 1 {
		t.Fatalf("upstream status routing = channelA:%d channelB:%d, want 0 and 1", channelACalls, channelBCalls)
	}

	var payload model.VideoTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	expectedStableURL := "http://gateway.example.test/v1/videos/" + publicTaskID + "/content"
	if payload.ID != publicTaskID || payload.TaskID != publicTaskID || payload.VideoURL != expectedStableURL || payload.URL != expectedStableURL {
		t.Fatalf("upstream status result was not safely sanitized and stabilized: %+v", payload)
	}
	for _, candidate := range append([]string{payload.VideoURL, payload.URL}, dataURLs(payload.Data)...) {
		if strings.Contains(candidate, sharedProviderID) {
			t.Fatalf("cross-channel alias conflict exposed an unsafe provider ID in URL: %+v", payload)
		}
		if strings.Contains(candidate, newStatusAliasID) {
			t.Fatalf("cross-channel alias conflict exposed an unpinned alias ID in URL: %+v", payload)
		}
		if candidate != expectedStableURL {
			t.Fatalf("candidate URL %q does not match expected stable URL %q", candidate, expectedStableURL)
		}
	}
	if got := db.GetVideoTaskChannel(publicTaskID); got != channelB.ID {
		t.Fatalf("public task mapping changed to %q, want %q", got, channelB.ID)
	}
	if got := db.GetVideoTaskChannel(sharedProviderID); got != channelA.ID {
		t.Fatalf("conflicting provider mapping changed to %q, want %q", got, channelA.ID)
	}

	mapping := db.GetTaskMapping(publicTaskID)
	if mapping == nil || mapping.TaskAlias != sharedProviderID || mapping.ChannelID != channelB.ID {
		t.Fatalf("public task mapping failed to retain real upstream provider ID: %+v", mapping)
	}

	// Subsequent query by returned id must route to channel B (not fail on unpinned newStatusAliasID)
	subsequentIDReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+payload.ID, nil)
	subsequentIDReq.Host = "gateway.example.test"
	subsequentIDResp := httptest.NewRecorder()
	engine.ServeHTTP(subsequentIDResp, subsequentIDReq)
	if subsequentIDResp.Code != http.StatusOK {
		t.Fatalf("subsequent query by returned id failed, got %d: %s", subsequentIDResp.Code, subsequentIDResp.Body.String())
	}
	if channelBCalls != 2 || channelACalls != 0 {
		t.Fatalf("subsequent query routing = channelA:%d channelB:%d, want 0 and 2", channelACalls, channelBCalls)
	}

	// Subsequent query by returned task_id must route to channel B
	subsequentTaskIDReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+payload.TaskID, nil)
	subsequentTaskIDReq.Host = "gateway.example.test"
	subsequentTaskIDResp := httptest.NewRecorder()
	engine.ServeHTTP(subsequentTaskIDResp, subsequentTaskIDReq)
	if subsequentTaskIDResp.Code != http.StatusOK {
		t.Fatalf("subsequent query by returned task_id failed, got %d: %s", subsequentTaskIDResp.Code, subsequentTaskIDResp.Body.String())
	}
	if channelBCalls != 3 || channelACalls != 0 {
		t.Fatalf("subsequent query routing = channelA:%d channelB:%d, want 0 and 3", channelACalls, channelBCalls)
	}

	// Query for sharedProviderID must route to channel A
	sharedReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+sharedProviderID, nil)
	sharedReq.Host = "gateway.example.test"
	sharedResp := httptest.NewRecorder()
	engine.ServeHTTP(sharedResp, sharedReq)
	if sharedResp.Code != http.StatusOK {
		t.Fatalf("query for sharedProviderID failed, got %d: %s", sharedResp.Code, sharedResp.Body.String())
	}
	if channelACalls != 1 || channelBCalls != 3 {
		t.Fatalf("query for sharedProviderID routing = channelA:%d channelB:%d, want 1 and 3", channelACalls, channelBCalls)
	}

	// Subsequent content query by returned stable URL must route to channel B and use sharedProviderID upstream
	contentReq := httptest.NewRequest(http.MethodGet, payload.VideoURL, nil)
	contentReq.Host = "gateway.example.test"
	contentResp := httptest.NewRecorder()
	engine.ServeHTTP(contentResp, contentReq)
	if contentResp.Code != http.StatusOK {
		t.Fatalf("content query failed, got %d: %s", contentResp.Code, contentResp.Body.String())
	}
	if channelBCalls != 4 || channelACalls != 1 {
		t.Fatalf("content routing = channelA:%d channelB:%d, want 1 and 4", channelACalls, channelBCalls)
	}
	if contentResp.Body.String() != "mp4-content-bytes" {
		t.Fatalf("content body = %q, want mp4-content-bytes", contentResp.Body.String())
	}
}

func TestGetVideoCrossChannelAliasConflictWriteFailureSuppressesStableURLAndRecovers(t *testing.T) {
	dbPath := initAsyncTaskRecoveryTestDB(t)

	channelACalls := 0
	channelBCalls := 0
	const (
		publicTaskID     = "conflict-fail-public-video"
		newStatusAliasID = "conflict-fail-new-alias-video"
		sharedProviderID = "conflict-fail-provider-video"
		upstreamVideoURL = "https://cdn-fail.example/status.mp4?signature=temporary"
	)

	upstreamA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelACalls++
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+sharedProviderID {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"` + sharedProviderID + `","status":"completed"}`))
			return
		}
		http.Error(w, "channel A unexpected request: "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
	}))
	t.Cleanup(upstreamA.Close)

	upstreamB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelBCalls++
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+publicTaskID {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"` + newStatusAliasID + `","task_id":"` + sharedProviderID + `","status":"completed","video_url":"` + upstreamVideoURL + `","url":"` + upstreamVideoURL + `","data":[{"url":"` + upstreamVideoURL + `","video_url":"` + upstreamVideoURL + `"}]}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+sharedProviderID+"/content" {
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = w.Write([]byte("mp4-content-bytes"))
			return
		}
		http.Error(w, "unexpected upstream request: "+r.Method+" "+r.URL.Path, http.StatusNotFound)
	}))
	t.Cleanup(upstreamB.Close)

	channelA := &db.ChannelModel{ID: "conflict-fail-channel-a", Name: "Channel A", Type: "newapi", BaseURL: upstreamA.URL + "/v1", APIKey: "key-a", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "unrelated-model"}
	channelB := &db.ChannelModel{ID: "conflict-fail-channel-b", Name: "Channel B", Type: "newapi", BaseURL: upstreamB.URL + "/v1", APIKey: "key-b", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "status-model"}
	for _, ch := range []*db.ChannelModel{channelA, channelB} {
		if err := db.SaveChannelModel(ch); err != nil {
			t.Fatal(err)
		}
		chID := ch.ID
		t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(chID) })
	}

	if err := db.EnsureTaskMappings(
		db.TaskMapping{TaskID: publicTaskID, ChannelID: channelB.ID, OriginRequestID: "origin-conflict-fail", TaskKind: asyncTaskKindVideo, TaskAlias: publicTaskID},
		db.TaskMapping{TaskID: sharedProviderID, ChannelID: channelA.ID, TaskKind: asyncTaskKindVideo, TaskAlias: "existing-provider-video"},
	); err != nil {
		t.Fatal(err)
	}

	// 1. Simulate DB write failure when persisting real provider ID
	originalConflictFn := recordVideoTaskConflictAliasFn
	recordVideoTaskConflictAliasFn = func(ctx context.Context, lookupTaskID, realProviderID string) error {
		return errors.New("simulated sqlite database write failure")
	}
	t.Cleanup(func() { recordVideoTaskConflictAliasFn = originalConflictFn })

	engine := gin.New()
	engine.GET("/v1/videos/:id", handleGetVideo)
	engine.GET("/v1/videos/:id/content", handleGetVideoContent)

	req1 := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID, nil)
	req1.Host = "gateway.example.test"
	resp1 := httptest.NewRecorder()
	engine.ServeHTTP(resp1, req1)

	// Upstream-successful status response must be preserved (200 OK, not 500 error)
	if resp1.Code != http.StatusOK {
		t.Fatalf("upstream-successful status must remain successful on write failure, got %d: %s", resp1.Code, resp1.Body.String())
	}
	// Explicitly records pending recovery state
	if resp1.Header().Get("X-Relay-Task-Mapping") != "pending" {
		t.Fatalf("mapping state header = %q, want pending", resp1.Header().Get("X-Relay-Task-Mapping"))
	}

	var payload1 model.VideoTaskResponse
	if err := json.Unmarshal(resp1.Body.Bytes(), &payload1); err != nil {
		t.Fatal(err)
	}
	gatewayStableURL := "http://gateway.example.test/v1/videos/" + publicTaskID + "/content"
	// Stable content URL must NOT be published when real upstream ID could not be saved
	if payload1.VideoURL == gatewayStableURL || payload1.URL == gatewayStableURL {
		t.Fatalf("stable content URL was published despite DB write failure: %+v", payload1)
	}
	// Direct provider URL is preserved
	if payload1.VideoURL != upstreamVideoURL {
		t.Fatalf("direct provider URL = %q, want %q", payload1.VideoURL, upstreamVideoURL)
	}
	// Public ID and task_id are sanitized (no unpinned alias or conflicting ID leaked)
	if payload1.ID != publicTaskID || payload1.TaskID != publicTaskID {
		t.Fatalf("IDs not sanitized: id=%q task_id=%q, want %q", payload1.ID, payload1.TaskID, publicTaskID)
	}
	// Verify DB was NOT updated due to simulated write failure
	mappingBefore := db.GetTaskMapping(publicTaskID)
	if mappingBefore == nil || mappingBefore.TaskAlias != publicTaskID {
		t.Fatalf("task mapping unexpectedly updated during simulated write failure: %+v", mappingBefore)
	}

	// 2. Simulate cache invalidation / process restart
	db.ClearTaskMappingCache()

	// Channel A mapping must be untouched
	if got := db.GetVideoTaskChannel(sharedProviderID); got != channelA.ID {
		t.Fatalf("channel A mapping altered to %q, want %q", got, channelA.ID)
	}
	// Subsequent query for sharedProviderID must route to channel A
	sharedReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+sharedProviderID, nil)
	sharedReq.Host = "gateway.example.test"
	sharedResp := httptest.NewRecorder()
	engine.ServeHTTP(sharedResp, sharedReq)
	if sharedResp.Code != http.StatusOK {
		t.Fatalf("sharedProviderID query failed, got %d: %s", sharedResp.Code, sharedResp.Body.String())
	}
	if channelACalls != 1 {
		t.Fatalf("channel A calls = %d, want 1", channelACalls)
	}

	// Channel B mapping must be untouched and subsequent query routes to channel B
	if got := db.GetVideoTaskChannel(publicTaskID); got != channelB.ID {
		t.Fatalf("channel B mapping altered to %q, want %q", got, channelB.ID)
	}
	subsequentReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+payload1.ID, nil)
	subsequentReq.Host = "gateway.example.test"
	subsequentResp := httptest.NewRecorder()
	engine.ServeHTTP(subsequentResp, subsequentReq)
	if subsequentResp.Code != http.StatusOK {
		t.Fatalf("subsequent query by returned ID failed, got %d: %s", subsequentResp.Code, subsequentResp.Body.String())
	}
	if channelBCalls != 2 {
		t.Fatalf("channel B calls = %d, want 2", channelBCalls)
	}

	// Confirm that attempting to fetch content while in unrecovered state would 404 upstream
	// (proving that suppressing gatewayStableURL was necessary)
	unrecoveredContentReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID+"/content", nil)
	unrecoveredContentReq.Host = "gateway.example.test"
	unrecoveredContentResp := httptest.NewRecorder()
	engine.ServeHTTP(unrecoveredContentResp, unrecoveredContentReq)
	if unrecoveredContentResp.Code != http.StatusNotFound {
		t.Fatalf("unrecovered content should fail with 404 upstream, got %d", unrecoveredContentResp.Code)
	}

	// 3. Simulate recovery: DB write is restored, journal replays pending mapping
	recordVideoTaskConflictAliasFn = originalConflictFn
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("ReconcilePendingAsyncTaskMappings failed: %v", err)
	}

	// In SQLite DB, TaskAlias should now be recovered to sharedProviderID
	mappingAfter := db.GetTaskMapping(publicTaskID)
	if mappingAfter == nil || mappingAfter.TaskAlias != sharedProviderID {
		t.Fatalf("recovered task mapping TaskAlias = %v, want %q", mappingAfter, sharedProviderID)
	}

	// Querying content now succeeds via the recovered real provider ID
	recoveredContentReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID+"/content", nil)
	recoveredContentReq.Host = "gateway.example.test"
	recoveredContentResp := httptest.NewRecorder()
	engine.ServeHTTP(recoveredContentResp, recoveredContentReq)
	if recoveredContentResp.Code != http.StatusOK {
		t.Fatalf("recovered content query failed, got %d: %s", recoveredContentResp.Code, recoveredContentResp.Body.String())
	}
	if recoveredContentResp.Body.String() != "mp4-content-bytes" {
		t.Fatalf("content body = %q, want mp4-content-bytes", recoveredContentResp.Body.String())
	}

	// 4. Subsequent status query after recovery now returns stable content URL and X-Relay-Task-Mapping: conflict
	finalStatusReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID, nil)
	finalStatusReq.Host = "gateway.example.test"
	finalStatusResp := httptest.NewRecorder()
	engine.ServeHTTP(finalStatusResp, finalStatusReq)
	if finalStatusResp.Code != http.StatusOK {
		t.Fatalf("final status query failed, got %d: %s", finalStatusResp.Code, finalStatusResp.Body.String())
	}
	if finalStatusResp.Header().Get("X-Relay-Task-Mapping") != "conflict" {
		t.Fatalf("final mapping state header = %q, want conflict", finalStatusResp.Header().Get("X-Relay-Task-Mapping"))
	}
	var finalPayload model.VideoTaskResponse
	if err := json.Unmarshal(finalStatusResp.Body.Bytes(), &finalPayload); err != nil {
		t.Fatal(err)
	}
	if finalPayload.VideoURL != gatewayStableURL {
		t.Fatalf("final status VideoURL = %q, want %q", finalPayload.VideoURL, gatewayStableURL)
	}

	// Confirm restart survives: close & reinit DB from disk
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if got := db.GetVideoTaskChannel(sharedProviderID); got != channelA.ID {
		t.Fatalf("channel A mapping altered after restart: got %q, want %q", got, channelA.ID)
	}
	if got := db.GetVideoTaskChannel(publicTaskID); got != channelB.ID {
		t.Fatalf("channel B mapping altered after restart: got %q, want %q", got, channelB.ID)
	}
	if got := db.GetCanonicalTaskIDForKind(publicTaskID, asyncTaskKindVideo); got != sharedProviderID {
		t.Fatalf("canonical provider ID after restart = %q, want %q", got, sharedProviderID)
	}
}

func TestGetVideoCrossChannelAliasConflictDatabaseAndJournalFailureSuppressesStableURLAndRecovers(t *testing.T) {
	dbPath := initAsyncTaskRecoveryTestDB(t)

	channelACalls := 0
	channelBCalls := 0
	const (
		publicTaskID     = "conflict-both-fail-public-video"
		newStatusAliasID = "conflict-both-fail-new-alias-video"
		sharedProviderID = "conflict-both-fail-provider-video"
		upstreamVideoURL = "https://cdn-both-fail.example/status.mp4?signature=temporary"
	)

	upstreamA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelACalls++
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+sharedProviderID {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"` + sharedProviderID + `","status":"completed"}`))
			return
		}
		http.Error(w, "channel A unexpected request: "+r.Method+" "+r.URL.Path, http.StatusInternalServerError)
	}))
	t.Cleanup(upstreamA.Close)

	upstreamB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelBCalls++
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+publicTaskID {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"` + newStatusAliasID + `","task_id":"` + sharedProviderID + `","status":"completed","video_url":"` + upstreamVideoURL + `","url":"` + upstreamVideoURL + `","data":[{"url":"` + upstreamVideoURL + `","video_url":"` + upstreamVideoURL + `"}]}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/"+sharedProviderID+"/content" {
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = w.Write([]byte("mp4-content-bytes"))
			return
		}
		http.Error(w, "unexpected upstream request: "+r.Method+" "+r.URL.Path, http.StatusNotFound)
	}))
	t.Cleanup(upstreamB.Close)

	channelA := &db.ChannelModel{ID: "conflict-both-fail-channel-a", Name: "Channel A", Type: "newapi", BaseURL: upstreamA.URL + "/v1", APIKey: "key-a", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "unrelated-model"}
	channelB := &db.ChannelModel{ID: "conflict-both-fail-channel-b", Name: "Channel B", Type: "newapi", BaseURL: upstreamB.URL + "/v1", APIKey: "key-b", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "status-model"}
	for _, ch := range []*db.ChannelModel{channelA, channelB} {
		if err := db.SaveChannelModel(ch); err != nil {
			t.Fatal(err)
		}
		chID := ch.ID
		t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(chID) })
	}

	if err := db.EnsureTaskMappings(
		db.TaskMapping{TaskID: publicTaskID, ChannelID: channelB.ID, OriginRequestID: "origin-conflict-both-fail", TaskKind: asyncTaskKindVideo, TaskAlias: publicTaskID},
		db.TaskMapping{TaskID: sharedProviderID, ChannelID: channelA.ID, TaskKind: asyncTaskKindVideo, TaskAlias: "existing-provider-video"},
	); err != nil {
		t.Fatal(err)
	}

	// 1. Simultaneously simulate DB write failure and Recovery Journal write failure
	originalConflictFn := recordVideoTaskConflictAliasFn
	recordVideoTaskConflictAliasFn = func(ctx context.Context, lookupTaskID, realProviderID string) error {
		return errors.New("simulated database write failure")
	}
	t.Cleanup(func() { recordVideoTaskConflictAliasFn = originalConflictFn })

	originalEnqueueFn := enqueueAsyncTaskMappingRecoveryFn
	enqueueAsyncTaskMappingRecoveryFn = func(registration asyncTaskMappingRegistration) (bool, error) {
		return false, errors.New("simulated recovery journal write failure")
	}
	t.Cleanup(func() { enqueueAsyncTaskMappingRecoveryFn = originalEnqueueFn })

	engine := gin.New()
	engine.GET("/v1/videos/:id", handleGetVideo)
	engine.GET("/v1/videos/:id/content", handleGetVideoContent)

	req1 := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID, nil)
	req1.Host = "gateway.example.test"
	resp1 := httptest.NewRecorder()
	engine.ServeHTTP(resp1, req1)

	// Upstream-successful status response must be preserved (200 OK, not 500 error)
	if resp1.Code != http.StatusOK {
		t.Fatalf("upstream-successful status must remain successful on dual failure, got %d: %s", resp1.Code, resp1.Body.String())
	}
	// Explicitly differentiates journal failure ("failed") from durable pending recovery ("pending")
	if resp1.Header().Get("X-Relay-Task-Mapping") != "failed" {
		t.Fatalf("mapping state header = %q, want failed", resp1.Header().Get("X-Relay-Task-Mapping"))
	}

	var payload1 model.VideoTaskResponse
	if err := json.Unmarshal(resp1.Body.Bytes(), &payload1); err != nil {
		t.Fatal(err)
	}
	gatewayStableURL := "http://gateway.example.test/v1/videos/" + publicTaskID + "/content"
	// Stable content URL must NOT be published when real upstream ID could not be saved
	if payload1.VideoURL == gatewayStableURL || payload1.URL == gatewayStableURL {
		t.Fatalf("stable content URL was published despite dual write failure: %+v", payload1)
	}
	// Direct provider URL is preserved
	if payload1.VideoURL != upstreamVideoURL {
		t.Fatalf("direct provider URL = %q, want %q", payload1.VideoURL, upstreamVideoURL)
	}
	// Public ID and task_id are sanitized (no unpinned alias or conflicting ID leaked)
	if payload1.ID != publicTaskID || payload1.TaskID != publicTaskID {
		t.Fatalf("IDs not sanitized: id=%q task_id=%q, want %q", payload1.ID, payload1.TaskID, publicTaskID)
	}
	// Verify DB was NOT updated due to simulated write failure
	mappingBefore := db.GetTaskMapping(publicTaskID)
	if mappingBefore == nil || mappingBefore.TaskAlias != publicTaskID {
		t.Fatalf("task mapping unexpectedly updated during simulated write failure: %+v", mappingBefore)
	}

	// 2. Clear cache and verify routing integrity
	db.ClearTaskMappingCache()

	// Channel A mapping must be untouched
	if got := db.GetVideoTaskChannel(sharedProviderID); got != channelA.ID {
		t.Fatalf("channel A mapping altered to %q, want %q", got, channelA.ID)
	}
	sharedReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+sharedProviderID, nil)
	sharedReq.Host = "gateway.example.test"
	sharedResp := httptest.NewRecorder()
	engine.ServeHTTP(sharedResp, sharedReq)
	if sharedResp.Code != http.StatusOK {
		t.Fatalf("sharedProviderID query failed, got %d: %s", sharedResp.Code, sharedResp.Body.String())
	}
	if channelACalls != 1 {
		t.Fatalf("channel A calls = %d, want 1", channelACalls)
	}

	// Channel B mapping must be untouched and subsequent query routes to channel B
	if got := db.GetVideoTaskChannel(publicTaskID); got != channelB.ID {
		t.Fatalf("channel B mapping altered to %q, want %q", got, channelB.ID)
	}
	subsequentReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+payload1.ID, nil)
	subsequentReq.Host = "gateway.example.test"
	subsequentResp := httptest.NewRecorder()
	engine.ServeHTTP(subsequentResp, subsequentReq)
	if subsequentResp.Code != http.StatusOK {
		t.Fatalf("subsequent query by returned ID failed, got %d: %s", subsequentResp.Code, subsequentResp.Body.String())
	}
	if channelBCalls != 2 {
		t.Fatalf("channel B calls = %d, want 2", channelBCalls)
	}

	// Attempting to fetch content in this failed state should 404 upstream
	unrecoveredContentReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID+"/content", nil)
	unrecoveredContentReq.Host = "gateway.example.test"
	unrecoveredContentResp := httptest.NewRecorder()
	engine.ServeHTTP(unrecoveredContentResp, unrecoveredContentReq)
	if unrecoveredContentResp.Code != http.StatusNotFound {
		t.Fatalf("unrecovered content should fail with 404 upstream, got %d", unrecoveredContentResp.Code)
	}

	// 3. Storage recovered: simulate subsequent query after storage recovery
	recordVideoTaskConflictAliasFn = originalConflictFn
	enqueueAsyncTaskMappingRecoveryFn = originalEnqueueFn

	recoveredStatusReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID, nil)
	recoveredStatusReq.Host = "gateway.example.test"
	recoveredStatusResp := httptest.NewRecorder()
	engine.ServeHTTP(recoveredStatusResp, recoveredStatusReq)
	if recoveredStatusResp.Code != http.StatusOK {
		t.Fatalf("recovered status query failed, got %d: %s", recoveredStatusResp.Code, recoveredStatusResp.Body.String())
	}
	// Once DB write succeeds on query, header is "conflict" and stable content URL is published
	if recoveredStatusResp.Header().Get("X-Relay-Task-Mapping") != "conflict" {
		t.Fatalf("recovered status mapping header = %q, want conflict", recoveredStatusResp.Header().Get("X-Relay-Task-Mapping"))
	}
	var recoveredPayload model.VideoTaskResponse
	if err := json.Unmarshal(recoveredStatusResp.Body.Bytes(), &recoveredPayload); err != nil {
		t.Fatal(err)
	}
	if recoveredPayload.VideoURL != gatewayStableURL {
		t.Fatalf("recovered status VideoURL = %q, want %q", recoveredPayload.VideoURL, gatewayStableURL)
	}

	// In SQLite DB, TaskAlias is now durably stored as sharedProviderID
	mappingAfter := db.GetTaskMapping(publicTaskID)
	if mappingAfter == nil || mappingAfter.TaskAlias != sharedProviderID {
		t.Fatalf("recovered task mapping TaskAlias = %v, want %q", mappingAfter, sharedProviderID)
	}

	// Requesting content through gateway stable URL now succeeds
	recoveredContentReq := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID+"/content", nil)
	recoveredContentReq.Host = "gateway.example.test"
	recoveredContentResp := httptest.NewRecorder()
	engine.ServeHTTP(recoveredContentResp, recoveredContentReq)
	if recoveredContentResp.Code != http.StatusOK {
		t.Fatalf("recovered content query failed, got %d: %s", recoveredContentResp.Code, recoveredContentResp.Body.String())
	}
	if recoveredContentResp.Body.String() != "mp4-content-bytes" {
		t.Fatalf("content body = %q, want mp4-content-bytes", recoveredContentResp.Body.String())
	}

	// 4. Confirm restart preserves mappings and canonical provider task ID
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if got := db.GetVideoTaskChannel(sharedProviderID); got != channelA.ID {
		t.Fatalf("channel A mapping altered after restart: got %q, want %q", got, channelA.ID)
	}
	if got := db.GetVideoTaskChannel(publicTaskID); got != channelB.ID {
		t.Fatalf("channel B mapping altered after restart: got %q, want %q", got, channelB.ID)
	}
	if got := db.GetCanonicalTaskIDForKind(publicTaskID, asyncTaskKindVideo); got != sharedProviderID {
		t.Fatalf("canonical provider ID after restart = %q, want %q", got, sharedProviderID)
	}
}

func TestDeferredFullLengthImageMappingReplaysAndRoutes(t *testing.T) {
	dbPath := initAsyncTaskRecoveryTestDB(t)
	const channelID = "full-length-image-channel"
	rawTaskID := strings.Repeat("i", 128)
	lookupIDs := imageJobLookupIDs([]string{rawTaskID})
	if len(lookupIDs) != 1 || len(lookupIDs[0]) != len(imageTaskIDPrefix)+len(rawTaskID) {
		t.Fatalf("full-length image lookup IDs = %v", lookupIDs)
	}

	imageStatusCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/images/jobs/"+rawTaskID {
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
			return
		}
		imageStatusCalls++
		w.Header().Set("Content-Type", "application/json")
		payload, _ := json.Marshal(map[string]string{"id": rawTaskID, "status": "completed"})
		_, _ = w.Write(payload)
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        channelID,
		Name:      "Full length image mapping test",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "image-test-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "gpt-image-2",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channelID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channelID) })

	originalPersist := persistAsyncTaskMappingsFn
	persistAsyncTaskMappingsFn = func(*gin.Context, string, string, string, string, ...string) error {
		return errors.New("simulated image mapping write failure")
	}
	t.Cleanup(func() { persistAsyncTaskMappingsFn = originalPersist })

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/images/jobs", nil)
	registerAsyncTaskMappings(ctx, channelID, asyncTaskKindImage, rawTaskID, "queued", lookupIDs...)

	if got := recorder.Header().Get("X-Relay-Task-Mapping"); got != "pending" {
		t.Fatalf("deferred full-length image mapping header = %q, want pending", got)
	}
	if cached := db.GetTaskMappingForKind(rawTaskID, asyncTaskKindImage); cached == nil || cached.TaskID != lookupIDs[0] || cached.TaskAlias != rawTaskID || cached.ChannelID != channelID {
		t.Fatalf("cached full-length image mapping = %+v", cached)
	}
	if got := db.GetVideoTaskChannel(rawTaskID); got != "" {
		t.Fatalf("full-length image mapping leaked into video channel %q", got)
	}
	var count int64
	if err := db.DB.Model(&db.TaskMapping{}).Where("task_id = ?", lookupIDs[0]).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed image mapping write unexpectedly reached SQLite: %d", count)
	}

	// The recovery journal validates the same 135-byte internal key, so replay
	// must survive both the failed initial write and a subsequent restart.
	persistAsyncTaskMappingsFn = originalPersist
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("replay full-length image mapping: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if got := db.GetImageTaskChannel(rawTaskID); got != channelID {
		t.Fatalf("full-length image mapping after restart = %q, want %q", got, channelID)
	}

	engine := gin.New()
	engine.GET("/v1/images/jobs/:id", handleGetImageJob)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/images/jobs/"+rawTaskID, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("full-length image status route returned %d: %s", response.Code, response.Body.String())
	}
	if imageStatusCalls != 1 {
		t.Fatalf("full-length image status upstream calls = %d, want 1", imageStatusCalls)
	}
}

func TestGetImageJobPersistsLateProviderAlias(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const (
		channelID       = "image-late-alias-channel"
		publicTaskID    = "image-late-public"
		providerTaskID  = "image-late-provider"
		originRequestID = "origin-image-late-alias"
	)
	upstreamPaths := make([]string, 0, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPaths = append(upstreamPaths, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/images/jobs/" + publicTaskID:
			_, _ = w.Write([]byte(`{"job":{"id":"image-late-public","task_id":"image-late-provider","status":"completed"}}`))
		case "/v1/images/jobs/" + providerTaskID:
			_, _ = w.Write([]byte(`{"job":{"id":"image-late-provider","status":"completed"}}`))
		default:
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
		}
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{ID: channelID, Name: "Late image alias", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "image-alias-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "gpt-image-2"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channelID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channelID) })

	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:          imageTaskIDPrefix + publicTaskID,
		ChannelID:       channelID,
		OriginRequestID: originRequestID,
		TaskKind:        asyncTaskKindImage,
		TaskAlias:       publicTaskID,
	}); err != nil {
		t.Fatal(err)
	}

	engine := gin.New()
	engine.GET("/v1/images/jobs/:id", handleGetImageJob)
	publicResponse := httptest.NewRecorder()
	engine.ServeHTTP(publicResponse, httptest.NewRequest(http.MethodGet, "/v1/images/jobs/"+publicTaskID, nil))
	if publicResponse.Code != http.StatusOK {
		t.Fatalf("public image status returned %d: %s", publicResponse.Code, publicResponse.Body.String())
	}

	providerMapping := db.GetTaskMappingForKind(imageTaskIDPrefix+providerTaskID, asyncTaskKindImage)
	if providerMapping == nil || providerMapping.ChannelID != channelID || providerMapping.OriginRequestID != originRequestID || providerMapping.TaskAlias != publicTaskID || providerMapping.TaskKind != asyncTaskKindImage {
		t.Fatalf("late image alias mapping = %+v", providerMapping)
	}
	if got := db.GetImageTaskChannel(providerTaskID); got != channelID {
		t.Fatalf("provider image alias routes to %q, want %q", got, channelID)
	}

	providerResponse := httptest.NewRecorder()
	engine.ServeHTTP(providerResponse, httptest.NewRequest(http.MethodGet, "/v1/images/jobs/"+providerTaskID, nil))
	if providerResponse.Code != http.StatusOK {
		t.Fatalf("provider image status returned %d: %s", providerResponse.Code, providerResponse.Body.String())
	}
	if got, want := strings.Join(upstreamPaths, ","), "GET /v1/images/jobs/"+publicTaskID+",GET /v1/images/jobs/"+providerTaskID; got != want {
		t.Fatalf("image alias upstream paths = %q, want %q", got, want)
	}
}

func TestGetImageJobKeepsUpstreamSuccessWhenLateAliasPersistenceIsDeferred(t *testing.T) {
	dbPath := initAsyncTaskRecoveryTestDB(t)

	const (
		channelID       = "deferred-image-alias-channel"
		publicTaskID    = "deferred-image-public"
		originRequestID = "origin-deferred-image-alias"
	)
	// A full public-size provider ID becomes a 135-byte imgjob_ lookup key.
	// Exercise the late-alias journal path at the same boundary as production.
	providerTaskID := strings.Repeat("p", 128)
	publicPayload, err := json.Marshal(map[string]any{
		"job": map[string]string{"id": publicTaskID, "task_id": providerTaskID, "status": "completed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	providerPayload, err := json.Marshal(map[string]any{
		"job": map[string]string{"id": providerTaskID, "status": "completed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if r.Method != http.MethodGet || (r.URL.Path != "/v1/images/jobs/"+publicTaskID && r.URL.Path != "/v1/images/jobs/"+providerTaskID) {
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/images/jobs/"+publicTaskID {
			_, _ = w.Write(publicPayload)
			return
		}
		_, _ = w.Write(providerPayload)
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{ID: channelID, Name: "Deferred image alias", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "image-alias-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "gpt-image-2"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channelID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channelID) })

	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:          imageTaskIDPrefix + publicTaskID,
		ChannelID:       channelID,
		OriginRequestID: originRequestID,
		TaskKind:        asyncTaskKindImage,
		TaskAlias:       publicTaskID,
	}); err != nil {
		t.Fatal(err)
	}

	originalPersist := persistImageTaskAliasRegistrationFn
	seenRegistration := false
	persistImageTaskAliasRegistrationFn = func(registration asyncTaskMappingRegistration) error {
		seenRegistration = true
		if registration.ChannelID != channelID || registration.TaskKind != asyncTaskKindImage || registration.TaskAlias != publicTaskID || registration.OriginRequestID != originRequestID || registration.InitialStatus != "" || len(registration.TaskIDs) != 1 || registration.TaskIDs[0] != imageTaskIDPrefix+providerTaskID {
			t.Errorf("unexpected deferred image alias registration: %+v", registration)
		}
		return errors.New("simulated image alias mapping write failure")
	}
	t.Cleanup(func() { persistImageTaskAliasRegistrationFn = originalPersist })

	engine := gin.New()
	engine.GET("/v1/images/jobs/:id", handleGetImageJob)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/images/jobs/"+publicTaskID, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("upstream-successful image status returned %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Relay-Task-Mapping") != "pending" {
		t.Fatalf("successful image response did not disclose deferred alias mapping: headers=%v", response.Header())
	}
	if !seenRegistration || upstreamCalls != 1 {
		t.Fatalf("deferred image alias registration=%v upstream calls=%d, want true and 1", seenRegistration, upstreamCalls)
	}
	if cached := db.GetTaskMappingForKind(imageTaskIDPrefix+providerTaskID, asyncTaskKindImage); cached == nil || cached.ChannelID != channelID || cached.OriginRequestID != originRequestID || cached.TaskAlias != publicTaskID {
		t.Fatalf("journaled image alias was not safely cached: %+v", cached)
	}
	var count int64
	if err := db.DB.Model(&db.TaskMapping{}).Where("task_id = ?", imageTaskIDPrefix+providerTaskID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed image alias write unexpectedly reached SQLite: %d", count)
	}

	persistImageTaskAliasRegistrationFn = originalPersist
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("replay deferred image alias: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	if got := db.GetImageTaskChannel(providerTaskID); got != channelID {
		t.Fatalf("recovered image alias after restart = %q, want %q", got, channelID)
	}
	if upstreamCalls != 1 {
		t.Fatalf("recovery made %d upstream calls, want 1", upstreamCalls)
	}
}

func TestGetImageJobCrossChannelLateAliasConflictKeepsSuccess(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const (
		channelAID      = "image-alias-conflict-a"
		channelBID      = "image-alias-conflict-b"
		publicTaskID    = "image-conflict-public"
		providerTaskID  = "image-conflict-provider"
		originRequestID = "origin-image-conflict"
	)
	channelACalls := 0
	upstreamA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelACalls++
		http.Error(w, "channel A must not receive the public status request", http.StatusInternalServerError)
	}))
	t.Cleanup(upstreamA.Close)

	channelBCalls := 0
	upstreamB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channelBCalls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/images/jobs/"+publicTaskID {
			http.Error(w, "unexpected upstream request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job":{"id":"image-conflict-public","task_id":"image-conflict-provider","status":"completed"}}`))
	}))
	t.Cleanup(upstreamB.Close)

	channelA := &db.ChannelModel{ID: channelAID, Name: "Existing image provider alias", Type: "newapi", BaseURL: upstreamA.URL + "/v1", APIKey: "channel-a-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "unrelated-model"}
	channelB := &db.ChannelModel{ID: channelBID, Name: "Image public task", Type: "newapi", BaseURL: upstreamB.URL + "/v1", APIKey: "channel-b-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "gpt-image-2"}
	for _, channel := range []*db.ChannelModel{channelA, channelB} {
		if err := db.SaveChannelModel(channel); err != nil {
			t.Fatal(err)
		}
		channelID := channel.ID
		t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channelID) })
	}
	if err := db.EnsureTaskMappings(
		db.TaskMapping{TaskID: imageTaskIDPrefix + publicTaskID, ChannelID: channelBID, OriginRequestID: originRequestID, TaskKind: asyncTaskKindImage, TaskAlias: publicTaskID},
		db.TaskMapping{TaskID: imageTaskIDPrefix + providerTaskID, ChannelID: channelAID, TaskKind: asyncTaskKindImage, TaskAlias: "existing-image-provider"},
	); err != nil {
		t.Fatal(err)
	}

	engine := gin.New()
	engine.GET("/v1/images/jobs/:id", handleGetImageJob)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/images/jobs/"+publicTaskID, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("image status must preserve upstream success on alias conflict, got %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Relay-Task-Mapping") != "conflict" {
		t.Fatalf("image alias conflict was not disclosed: headers=%v", response.Header())
	}
	if channelACalls != 0 || channelBCalls != 1 {
		t.Fatalf("image alias conflict routing = channelA:%d channelB:%d, want 0 and 1", channelACalls, channelBCalls)
	}
	if mapping := db.GetTaskMappingForKind(imageTaskIDPrefix+publicTaskID, asyncTaskKindImage); mapping == nil || mapping.ChannelID != channelBID || mapping.OriginRequestID != originRequestID {
		t.Fatalf("public image mapping changed after conflict: %+v", mapping)
	}
	if mapping := db.GetTaskMappingForKind(imageTaskIDPrefix+providerTaskID, asyncTaskKindImage); mapping == nil || mapping.ChannelID != channelAID {
		t.Fatalf("provider image mapping changed after conflict: %+v", mapping)
	}
}

func TestImageMappingCannotAuthorizeOrRouteRawVideoID(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const sharedTaskID = "shared-image-video-id"
	imageStatusCalls := 0
	unexpectedUpstreamCalls := 0
	imageUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/images/jobs/"+sharedTaskID {
			imageStatusCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"shared-image-video-id","status":"completed"}`))
			return
		}
		unexpectedUpstreamCalls++
		http.Error(w, "an image channel must not receive a video request", http.StatusNotFound)
	}))
	t.Cleanup(imageUpstream.Close)

	imageChannel := &db.ChannelModel{
		ID:        "typed-image-channel",
		Name:      "Typed image mapping test",
		Type:      "newapi",
		BaseURL:   imageUpstream.URL + "/v1",
		APIKey:    "image-channel-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "gpt-image-2",
	}
	if err := db.SaveChannelModel(imageChannel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(imageChannel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(imageChannel.ID) })

	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:    imageTaskIDPrefix + sharedTaskID,
		ChannelID: imageChannel.ID,
		TaskKind:  asyncTaskKindImage,
		TaskAlias: sharedTaskID,
	}); err != nil {
		t.Fatal(err)
	}
	if got := db.GetVideoTaskChannel(sharedTaskID); got != "" {
		t.Fatalf("raw video ID was routed through image mapping to %q", got)
	}
	if got := db.GetImageTaskChannel(sharedTaskID); got != imageChannel.ID {
		t.Fatalf("legacy image lookup routed to %q, want %q", got, imageChannel.ID)
	}

	_, gatewayToken, err := security.SetupAdmin("typed-isolation-admin", "correct horse battery", "127.0.0.1", "test")
	if err != nil {
		t.Fatal(err)
	}
	engine := Setup()

	// Anonymous browser playback is allowed only for registered video IDs. An
	// image's legacy namespace must not authorize a same-named video URL.
	noTokenContent := httptest.NewRecorder()
	engine.ServeHTTP(noTokenContent, httptest.NewRequest(http.MethodGet, "/v1/videos/"+sharedTaskID+"/content", nil))
	if noTokenContent.Code != http.StatusNotFound {
		t.Fatalf("anonymous video content through image mapping returned %d: %s", noTokenContent.Code, noTokenContent.Body.String())
	}

	for _, path := range []string{
		"/v1/videos/" + sharedTaskID,
		"/v1/videos/" + sharedTaskID + "/content",
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+gatewayToken)
		engine.ServeHTTP(response, request)
		if response.Code < http.StatusBadRequest {
			t.Fatalf("video request %s unexpectedly succeeded through image mapping: %d %s", path, response.Code, response.Body.String())
		}
	}
	if unexpectedUpstreamCalls != 0 {
		t.Fatalf("image channel received %d unexpected video request(s)", unexpectedUpstreamCalls)
	}

	imageResponse := httptest.NewRecorder()
	imageRequest := httptest.NewRequest(http.MethodGet, "/v1/images/jobs/"+sharedTaskID, nil)
	imageRequest.Header.Set("Authorization", "Bearer "+gatewayToken)
	engine.ServeHTTP(imageResponse, imageRequest)
	if imageResponse.Code != http.StatusOK {
		t.Fatalf("legacy image route returned %d: %s", imageResponse.Code, imageResponse.Body.String())
	}
	if imageStatusCalls != 1 || unexpectedUpstreamCalls != 0 {
		t.Fatalf("upstream routing image=%d unexpected=%d, want image=1 unexpected=0", imageStatusCalls, unexpectedUpstreamCalls)
	}
}

func dataURLs(data []map[string]string) []string {
	urls := make([]string, 0, len(data)*2)
	for _, item := range data {
		urls = append(urls, item["url"], item["video_url"])
	}
	return urls
}

func TestCrossChannelTaskIDCollisionDisambiguation(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/videos", nil)

	// Channel A registers task "provider-task-collision"
	taskA := "provider-task-collision"
	if err := registerAsyncTaskMappings(ctx, "channel-a", asyncTaskKindVideo, taskA, "queued", taskA); err != nil {
		t.Fatalf("channel-a registration failed: %v", err)
	}
	if got := db.GetVideoTaskChannel(taskA); got != "channel-a" {
		t.Fatalf("channel-a task routed to %q, want channel-a", got)
	}

	// Channel B receives the exact same task ID from upstream
	gtID, disambiguated := disambiguateAsyncTaskIDs("channel-b", asyncTaskKindVideo, taskA, []string{taskA})
	if !disambiguated {
		t.Fatalf("expected collision disambiguation for channel-b with task %s", taskA)
	}
	if !strings.HasPrefix(gtID, "gt_") {
		t.Fatalf("expected gateway ID prefix gt_, got %s", gtID)
	}

	// Channel B registers the disambiguated task
	if err := registerAsyncTaskMappings(ctx, "channel-b", asyncTaskKindVideo, taskA, "queued", gtID); err != nil {
		t.Fatalf("channel-b registration failed: %v", err)
	}

	// Verify both channels route independently
	if got := db.GetVideoTaskChannel(taskA); got != "channel-a" {
		t.Fatalf("channel-a task routing altered: got %q, want channel-a", got)
	}
	if got := db.GetVideoTaskChannel(gtID); got != "channel-b" {
		t.Fatalf("channel-b task routing failed: got %q, want channel-b", got)
	}

	// Verify canonical provider ID resolution
	if canonical := resolveCanonicalProviderTaskID(gtID, asyncTaskKindVideo); canonical != taskA {
		t.Fatalf("canonical provider ID for %s = %q, want %q", gtID, canonical, taskA)
	}
	if canonical := resolveCanonicalProviderTaskID(taskA, asyncTaskKindVideo); canonical != taskA {
		t.Fatalf("canonical provider ID for %s = %q, want %q", taskA, canonical, taskA)
	}

	// Verify image collision disambiguation
	imgTask := "image-shared-dup"
	if err := registerAsyncTaskMappings(ctx, "channel-img-1", asyncTaskKindImage, imgTask, "queued", imageJobLookupIDs([]string{imgTask})...); err != nil {
		t.Fatalf("image channel 1 registration failed: %v", err)
	}
	gtImgID, imgDisambiguated := disambiguateAsyncTaskIDs("channel-img-2", asyncTaskKindImage, imgTask, []string{imgTask})
	if !imgDisambiguated {
		t.Fatalf("expected image task collision disambiguation for channel-img-2")
	}
	if err := registerAsyncTaskMappings(ctx, "channel-img-2", asyncTaskKindImage, imgTask, "queued", imageJobLookupIDs([]string{gtImgID})...); err != nil {
		t.Fatalf("image channel 2 registration failed: %v", err)
	}
	if canonical := resolveCanonicalProviderTaskID(imageTaskIDPrefix+gtImgID, asyncTaskKindImage); canonical != imgTask {
		t.Fatalf("canonical provider ID for %s = %q, want %q", imageTaskIDPrefix+gtImgID, canonical, imgTask)
	}
}

func TestCreateVideoDatabaseAndJournalFailureReturnsFailedHeaderAndPreservesSuccess(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"fail-journal-video","status":"queued"}`)
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "fail-journal-video-channel",
		Name:      "Fail Journal video test",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "upstream-test-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "fail-journal-video-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	originalPersist := persistAsyncTaskMappingsFn
	persistAsyncTaskMappingsFn = func(*gin.Context, string, string, string, string, ...string) error {
		return errors.New("simulated mapping write failure")
	}
	t.Cleanup(func() { persistAsyncTaskMappingsFn = originalPersist })

	journalPath := asyncTaskMappingRecoveryJournalPath()
	if err := os.Mkdir(journalPath, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(journalPath) })

	engine := gin.New()
	engine.POST("/v1/videos", handleCreateVideo)
	request := httptest.NewRequest(http.MethodPost, "/v1/videos", strings.NewReader(`{"model":"fail-journal-video-model","prompt":"test"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	// 1. Upstream success must be preserved, HTTP 200, task ID in body
	if response.Code != http.StatusOK {
		t.Fatalf("upstream-created video returned %d: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "fail-journal-video") {
		t.Fatalf("successful upstream task ID was not preserved: %s", response.Body.String())
	}
	if upstreamCalls != 1 {
		t.Fatalf("upstream create calls = %d, want exactly one", upstreamCalls)
	}

	// 2. Mapping header must be "failed"
	if got := response.Header().Get("X-Relay-Task-Mapping"); got != "failed" {
		t.Fatalf("mapping header = %q, want failed", got)
	}

	// 3. Cache must NOT be published
	if got := db.GetVideoTaskChannel("fail-journal-video"); got != "" {
		t.Fatalf("non-durable mapping was exposed from cache as %q", got)
	}

	// 4. Simulate process restart before journal lands: must not pretend there is a recoverable record
	if err := os.Remove(journalPath); err != nil {
		t.Fatal(err)
	}
	asyncTaskMappingRecoveries = asyncTaskMappingRecoveryQueue{}
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("reconcile after restart without journal: %v", err)
	}
	if got := db.GetVideoTaskChannel("fail-journal-video"); got != "" {
		t.Fatalf("phantom mapping recovered after restart: %q", got)
	}
	var count int64
	if err := db.DB.Model(&db.TaskMapping{}).Where("task_id = ?", "fail-journal-video").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("sqlite count = %d, want 0", count)
	}
}

func TestGetVideoAliasDatabaseAndJournalFailureReturnsFailedHeaderAndPreservesSuccess(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const (
		channelID           = "fail-alias-video-channel"
		publicTaskID        = "public-video-task"
		providerTaskID      = "provider-video-alias"
		originRequestID     = "req-fail-alias-video"
		expectedProviderURL = "https://cdn.example.test/video.mp4"
	)

	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"`+providerTaskID+`","status":"completed","video_url":"`+expectedProviderURL+`","url":"`+expectedProviderURL+`"}`)
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        channelID,
		Name:      "Fail alias video test",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "upstream-test-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "fail-alias-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:          publicTaskID,
		ChannelID:       channelID,
		OriginRequestID: originRequestID,
		TaskKind:        asyncTaskKindVideo,
		TaskAlias:       publicTaskID,
	}); err != nil {
		t.Fatal(err)
	}

	originalPersist := persistVideoTaskAliasRegistrationFn
	persistVideoTaskAliasRegistrationFn = func(registration asyncTaskMappingRegistration) error {
		return errors.New("simulated alias write failure")
	}
	t.Cleanup(func() { persistVideoTaskAliasRegistrationFn = originalPersist })

	journalPath := asyncTaskMappingRecoveryJournalPath()
	if err := os.Mkdir(journalPath, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(journalPath) })

	engine := gin.New()
	engine.GET("/v1/videos/:id", handleGetVideo)
	request := httptest.NewRequest(http.MethodGet, "/v1/videos/"+publicTaskID, nil)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	// 1. Upstream status response preserved
	if response.Code != http.StatusOK {
		t.Fatalf("upstream status code = %d: %s", response.Code, response.Body.String())
	}
	if upstreamCalls != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstreamCalls)
	}

	// 2. Mapping header must be "failed"
	if got := response.Header().Get("X-Relay-Task-Mapping"); got != "failed" {
		t.Fatalf("mapping header = %q, want failed", got)
	}

	// 3. Cache must NOT be published
	if cached := db.GetTaskMapping(providerTaskID); cached != nil {
		t.Fatalf("failed alias mapping was published to cache: %+v", cached)
	}

	// 4. Cannot expose stable gateway URL when alias persistence failed and journal failed
	var payload model.VideoTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.VideoURL != expectedProviderURL || payload.URL != expectedProviderURL {
		t.Fatalf(
			"provider URL was not preserved after journal failure: video_url=%q url=%q",
			payload.VideoURL,
			payload.URL,
		)
	}

	// 5. Filesystem recovers: same process retries and succeeds
	if err := os.Remove(journalPath); err != nil {
		t.Fatal(err)
	}
	persistVideoTaskAliasRegistrationFn = originalPersist
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("retry after filesystem recovery failed: %v", err)
	}
	if cached := db.GetTaskMapping(providerTaskID); cached == nil || cached.ChannelID != channelID {
		t.Fatalf("recovered alias mapping not in cache: %+v", cached)
	}
}

func TestGetImageJobAliasDatabaseAndJournalFailureReturnsFailedHeaderAndPreservesSuccess(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const (
		channelID       = "fail-alias-img-channel"
		publicTaskID    = "public-image-task"
		providerTaskID  = "provider-image-alias"
		originRequestID = "req-fail-alias-img"
	)

	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"`+providerTaskID+`","status":"completed"}`)
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        channelID,
		Name:      "Fail alias image test",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "image-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "gpt-image-2",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channelID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channelID) })

	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:          imageTaskIDPrefix + publicTaskID,
		ChannelID:       channelID,
		OriginRequestID: originRequestID,
		TaskKind:        asyncTaskKindImage,
		TaskAlias:       publicTaskID,
	}); err != nil {
		t.Fatal(err)
	}

	originalPersist := persistImageTaskAliasRegistrationFn
	persistImageTaskAliasRegistrationFn = func(registration asyncTaskMappingRegistration) error {
		return errors.New("simulated image alias write failure")
	}
	t.Cleanup(func() { persistImageTaskAliasRegistrationFn = originalPersist })

	journalPath := asyncTaskMappingRecoveryJournalPath()
	if err := os.Mkdir(journalPath, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(journalPath) })

	engine := gin.New()
	engine.GET("/v1/images/jobs/:id", handleGetImageJob)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/images/jobs/"+publicTaskID, nil))

	// 1. Upstream status response preserved
	if response.Code != http.StatusOK {
		t.Fatalf("upstream-successful image status returned %d: %s", response.Code, response.Body.String())
	}
	if upstreamCalls != 1 {
		t.Fatalf("upstream status calls = %d, want 1", upstreamCalls)
	}

	// 2. Mapping header must be "failed"
	if got := response.Header().Get("X-Relay-Task-Mapping"); got != "failed" {
		t.Fatalf("mapping header = %q, want failed", got)
	}

	// 3. Cache must NOT be published
	if cached := db.GetTaskMappingForKind(imageTaskIDPrefix+providerTaskID, asyncTaskKindImage); cached != nil {
		t.Fatalf("non-durable image alias mapping was exposed from cache: %+v", cached)
	}

	// 4. Simulate process restart before journal lands: must not pretend there is a recoverable record
	if err := os.Remove(journalPath); err != nil {
		t.Fatal(err)
	}
	asyncTaskMappingRecoveries = asyncTaskMappingRecoveryQueue{}
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatalf("reconcile after restart without journal: %v", err)
	}
	if cached := db.GetTaskMappingForKind(imageTaskIDPrefix+providerTaskID, asyncTaskKindImage); cached != nil {
		t.Fatalf("phantom image alias recovered after restart: %+v", cached)
	}
}
