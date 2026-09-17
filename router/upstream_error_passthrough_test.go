package router

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

const real503Fixture = `{"code":"fail_to_fetch_task","message":"{\"error\":{\"code\":\"no_available_account\",\"message\":\"无可用账号，请稍后重试\",\"type\":\"server_error\"}}","data":null}`

func TestUpstream503ErrorPassthrough_V1Videos(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && (r.URL.Path == "/v1/videos/generations" || r.URL.Path == "/v1/videos") {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(real503Fixture))
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(upstream.Close)

	channel := saveVideoPollingTestChannel(t, "test-503-channel", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/videos", handleCreateVideo)

	res := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/videos", `{"model":"video-model","prompt":"draw something"}`)

	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503, got %d, body: %s", res.Code, res.Body.String())
	}
	if ct := res.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected Content-Type application/json, got %s", ct)
	}
	if ra := res.Header().Get("Retry-After"); ra != "120" {
		t.Fatalf("expected Retry-After 120, got %s", ra)
	}
	if res.Body.String() != real503Fixture {
		t.Fatalf("expected exact byte fixture, got %s", res.Body.String())
	}

	_ = channel
}

func TestUpstream503ErrorPassthrough_Playground(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(real503Fixture))
	}))
	t.Cleanup(upstream.Close)

	channel := saveVideoPollingTestChannel(t, "test-playground-503", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/api/playground/run", handlePlaygroundRun)

	res := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-model","prompt":"draw something"}`)

	if res.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for playground RPC, got %d, body: %s", res.Code, res.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, res.Body.String())
	}

	if resp["status"] != "error" {
		t.Fatalf("expected status error, got %v", resp["status"])
	}
	if resp["error"] != real503Fixture {
		t.Fatalf("expected error to contain raw fixture, got %v", resp["error"])
	}
	if resp["upstream_body"] != real503Fixture {
		t.Fatalf("expected upstream_body to contain raw fixture, got %v", resp["upstream_body"])
	}
	if resp["provider_message"] != "无可用账号，请稍后重试" {
		t.Fatalf("expected provider_message '无可用账号，请稍后重试', got %v", resp["provider_message"])
	}
	if resp["channel"] != channel.ID {
		t.Fatalf("expected channel %s, got %v", channel.ID, resp["channel"])
	}
	if statusNum, ok := resp["upstream_status"].(float64); !ok || int(statusNum) != 503 {
		t.Fatalf("expected upstream_status 503, got %v", resp["upstream_status"])
	}
}

func TestUpstream524PlaintextPassthrough_V1Videos(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(524)
		_, _ = w.Write([]byte("error code: 524"))
	}))
	t.Cleanup(upstream.Close)

	channel := saveVideoPollingTestChannel(t, "test-524-channel", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/videos", handleCreateVideo)

	res := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/videos", `{"model":"video-model","prompt":"draw something"}`)

	if res.Code != 524 {
		t.Fatalf("expected HTTP 524, got %d, body: %s", res.Code, res.Body.String())
	}
	if ct := res.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected Content-Type text/plain, got %s", ct)
	}
	if strings.TrimSpace(res.Body.String()) != "error code: 524" {
		t.Fatalf("expected plaintext 'error code: 524', got %q", res.Body.String())
	}

	_ = channel
}

func TestUpstream524PlaintextPassthrough_Playground(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(524)
		_, _ = w.Write([]byte("error code: 524"))
	}))
	t.Cleanup(upstream.Close)

	channel := saveVideoPollingTestChannel(t, "test-playground-524", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/api/playground/run", handlePlaygroundRun)

	res := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-model","prompt":"draw something"}`)

	if res.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for playground RPC, got %d, body: %s", res.Code, res.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, res.Body.String())
	}

	if resp["status"] != "error" {
		t.Fatalf("expected status error, got %v", resp["status"])
	}
	if resp["error"] != "error code: 524" {
		t.Fatalf("expected error 'error code: 524', got %v", resp["error"])
	}
	if resp["upstream_body"] != "error code: 524" {
		t.Fatalf("expected upstream_body 'error code: 524', got %v", resp["upstream_body"])
	}
	if statusNum, ok := resp["upstream_status"].(float64); !ok || int(statusNum) != 524 {
		t.Fatalf("expected upstream_status 524, got %v", resp["upstream_status"])
	}
}

func TestUpstreamHTTP200BusinessFailure_V1AndPlayground(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const businessFailJSON = `{"code":-1,"message":"账户余额不足，无法创建视频任务"}`

	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(businessFailJSON))
	}))
	t.Cleanup(upstream.Close)

	channel := saveVideoPollingTestChannel(t, "test-200-bizfail-channel", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/videos", handleCreateVideo)
	engine.POST("/api/playground/run", handlePlaygroundRun)

	// 1. /v1/videos
	resV1 := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/videos", `{"model":"video-model","prompt":"draw something"}`)
	if resV1.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 passthrough, got %d, body: %s", resV1.Code, resV1.Body.String())
	}
	if resV1.Body.String() != businessFailJSON {
		t.Fatalf("expected verbatim raw body %s, got %s", businessFailJSON, resV1.Body.String())
	}

	// Upstream must have received exactly 1 call (no retries / no key rotation)
	if calls := upstreamCalls.Load(); calls != 1 {
		t.Fatalf("expected exactly 1 upstream call, got %d", calls)
	}

	// 2. Playground
	resPG := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-model","prompt":"draw something"}`)
	if resPG.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for playground RPC, got %d, body: %s", resPG.Code, resPG.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(resPG.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, resPG.Body.String())
	}
	if resp["status"] != "error" {
		t.Fatalf("Playground must NOT report status ok for HTTP 200 business failure: %v", resp)
	}
	if resp["error"] != businessFailJSON {
		t.Fatalf("expected error to contain raw JSON, got %v", resp["error"])
	}
	if resp["provider_message"] != "账户余额不足，无法创建视频任务" {
		t.Fatalf("expected provider_message, got %v", resp["provider_message"])
	}
}

func TestSynchronousImagesGenerations_200OKWithoutTaskID(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const syncImageJSON = `{"created":1700000000,"data":[{"url":"https://cdn.example.test/generated.png"}]}`

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(syncImageJSON))
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "sync-image-channel",
		Name:      "sync-image",
		Type:      "openai",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "test-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "dall-e-3",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/images/generations", handleImagesGenerations)

	res := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/images/generations", `{"model":"dall-e-3","prompt":"a cute puppy"}`)
	if res.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d, body: %s", res.Code, res.Body.String())
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(res.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	data, ok := parsed["data"].([]interface{})
	if !ok || len(data) != 1 {
		t.Fatalf("expected data array with 1 item, got %v", parsed["data"])
	}
	item := data[0].(map[string]interface{})
	if item["url"] != "https://cdn.example.test/generated.png" {
		t.Fatalf("unexpected image url: %v", item["url"])
	}
}

func TestMultiKeyRotation_LastFailurePreserved(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	var keyCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := keyCalls.Add(1)
		auth := r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		if strings.Contains(auth, "key-1") {
			_, _ = w.Write([]byte(`{"error":{"code":"invalid_key_1","message":"key 1 expired"}}`))
			return
		}
		if strings.Contains(auth, "key-2") {
			_, _ = w.Write([]byte(`{"error":{"code":"invalid_key_2","message":"key 2 quota exceeded"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"error":{"code":"call_` + string(rune('0'+call)) + `","message":"unknown key"}}`))
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "multikey-channel",
		Name:      "multikey",
		Type:      "openai",
		BaseURL:   upstream.URL + "/v1",
		APIKeys:   []string{"key-1", "key-2"},
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "dall-e-3",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/images/generations", handleImagesGenerations)

	res := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/images/generations", `{"model":"dall-e-3","prompt":"a cute puppy"}`)

	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected HTTP 401, got %d, body: %s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "key 2 quota exceeded") {
		t.Fatalf("expected last key failure preserved, got %s", res.Body.String())
	}
	if keyCalls.Load() != 2 {
		t.Fatalf("expected both keys attempted (2 calls), got %d", keyCalls.Load())
	}
}

func TestMultiKeyRotation_NoRotationOn503(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	var attempts atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(real503Fixture))
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "multikey-503-channel",
		Name:      "multikey-503",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKeys:   []string{"key-1", "key-2"},
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

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/videos", handleCreateVideo)

	res := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/videos", `{"model":"video-model","prompt":"draw something"}`)

	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503, got %d, body: %s", res.Code, res.Body.String())
	}
	if attempts.Load() != 1 {
		t.Fatalf("single task submission safety: 503 must NOT rotate to key-2, attempts = %d", attempts.Load())
	}
}

func TestUpstream503ErrorPassthrough_ProfileVideo(t *testing.T) {
	t.Setenv("RELAY_ENABLE_PROFILE_VIDEO_ENGINE", "1")
	initAsyncTaskRecoveryTestDB(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(real503Fixture))
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "profile-video-channel",
		Name:      "Profile video",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "profile-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "video-profile-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	profile := protocol.Profile{
		SchemaVersion: protocol.CurrentSchemaVersion,
		Name:          "profile video",
		Operations: []protocol.Operation{{
			Operation:      "videos.create",
			ExecutionMode:  protocol.ExecutionAsync,
			PollingMode:    protocol.PollingBackground,
			MediaRetention: protocol.MediaRetentionDisabled,
			Submit:         protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
			Response:       protocol.Response{TaskIDPaths: []string{"id", "task_id"}},
			Poll: &protocol.Poll{
				Method: http.MethodGet, Path: "/videos/generations/{task_id}",
				IntervalMS: 5000, MaxAttempts: 20, MaxDurationMS: 120000,
				StatusPath: "status", SuccessValues: []string{"succeeded"}, FailureValues: []string{"failed"},
				ResultURLPaths: []string{"video_url"},
			},
		}},
	}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-video-prof", Name: "Profile video", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{
		ProfileID: "profile-video-prof", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion,
		ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "videos.create", ModelPattern: "video-profile-model",
		ProfileID: "profile-video-prof", ProfileRevision: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/videos", handleCreateVideo)
	engine.POST("/api/playground/run", handlePlaygroundRun)

	// 1. /v1/videos
	resV1 := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/videos", `{"model":"video-profile-model","prompt":"draw something"}`)
	if resV1.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503 from Profile, got %d, body: %s", resV1.Code, resV1.Body.String())
	}
	if strings.TrimSpace(resV1.Body.String()) != strings.TrimSpace(real503Fixture) {
		t.Fatalf("expected exact raw body from Profile, got %s", resV1.Body.String())
	}

	// 2. Playground
	resPG := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-profile-model","prompt":"draw something"}`)
	if resPG.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for playground RPC, got %d, body: %s", resPG.Code, resPG.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(resPG.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, resPG.Body.String())
	}
	if resp["status"] != "error" {
		t.Fatalf("Playground Profile must report status error, got %v", resp["status"])
	}
	if resp["error"] != real503Fixture {
		t.Fatalf("expected error to contain raw JSON fixture, got %v", resp["error"])
	}
	if resp["provider_message"] != "无可用账号，请稍后重试" {
		t.Fatalf("expected provider_message '无可用账号，请稍后重试', got %v", resp["provider_message"])
	}
}

func TestUpstreamHTTP200BusinessFailure_ProfileVideo(t *testing.T) {
	t.Setenv("RELAY_ENABLE_PROFILE_VIDEO_ENGINE", "1")
	initAsyncTaskRecoveryTestDB(t)

	const businessFailJSON = `{"code":-1,"message":"Profile 账户余额不足，无法创建视频任务"}`

	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(businessFailJSON))
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "profile-200-channel",
		Name:      "Profile 200 video",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "profile-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "video-profile-200",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	profile := protocol.Profile{
		SchemaVersion: protocol.CurrentSchemaVersion,
		Name:          "profile video 200",
		Operations: []protocol.Operation{{
			Operation:      "videos.create",
			ExecutionMode:  protocol.ExecutionAsync,
			PollingMode:    protocol.PollingBackground,
			MediaRetention: protocol.MediaRetentionDisabled,
			Submit:         protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
			Response:       protocol.Response{TaskIDPaths: []string{"id", "task_id"}},
			Poll: &protocol.Poll{
				Method: http.MethodGet, Path: "/videos/generations/{task_id}",
				IntervalMS: 5000, MaxAttempts: 20, MaxDurationMS: 120000,
				StatusPath: "status", SuccessValues: []string{"succeeded"}, FailureValues: []string{"failed"},
				ResultURLPaths: []string{"video_url"},
			},
		}},
	}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-vid-200-prof", Name: "Profile video 200", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{
		ProfileID: "profile-vid-200-prof", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion,
		ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "videos.create", ModelPattern: "video-profile-200",
		ProfileID: "profile-vid-200-prof", ProfileRevision: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/videos", handleCreateVideo)
	engine.POST("/api/playground/run", handlePlaygroundRun)

	// 1. /v1/videos
	resV1 := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/videos", `{"model":"video-profile-200","prompt":"draw something"}`)
	if resV1.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 passthrough from Profile, got %d, body: %s", resV1.Code, resV1.Body.String())
	}
	if resV1.Body.String() != businessFailJSON {
		t.Fatalf("expected verbatim raw body from Profile 200 business error, got %s", resV1.Body.String())
	}

	// Single submission check
	if calls := upstreamCalls.Load(); calls != 1 {
		t.Fatalf("expected exactly 1 upstream call, got %d", calls)
	}

	// 2. Playground
	resPG := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-profile-200","prompt":"draw something"}`)
	if resPG.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for playground RPC, got %d, body: %s", resPG.Code, resPG.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(resPG.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, resPG.Body.String())
	}
	if resp["status"] != "error" {
		t.Fatalf("Playground Profile must NOT report status ok for HTTP 200 business failure: %v", resp)
	}
	if resp["error"] != businessFailJSON {
		t.Fatalf("expected error to contain raw JSON, got %v", resp["error"])
	}
	if resp["provider_message"] != "Profile 账户余额不足，无法创建视频任务" {
		t.Fatalf("expected provider_message, got %v", resp["provider_message"])
	}
}

func TestPlaygroundPreservesWhitespaceAndMetadata(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const spacedFixture = "  \n{\"code\":\"custom_err\",\"message\":\"spaced error message\"}\n  \n"

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Retry-After", "90")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(spacedFixture))
	}))
	t.Cleanup(upstream.Close)

	channel := saveVideoPollingTestChannel(t, "test-spaced-playground", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/api/playground/run", handlePlaygroundRun)

	res := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-model","prompt":"draw something"}`)
	if res.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for playground RPC, got %d, body: %s", res.Code, res.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(res.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, res.Body.String())
	}

	if resp["status"] != "error" {
		t.Fatalf("expected status error, got %v", resp["status"])
	}
	// Assert exact equality without any trimming
	if resp["error"] != spacedFixture {
		t.Fatalf("expected exact unmodified error string, got %q, want %q", resp["error"], spacedFixture)
	}
	if resp["upstream_body"] != spacedFixture {
		t.Fatalf("expected exact unmodified upstream_body string, got %q, want %q", resp["upstream_body"], spacedFixture)
	}
	if statusNum, ok := resp["upstream_status"].(float64); !ok || int(statusNum) != 503 {
		t.Fatalf("expected upstream_status 503, got %v", resp["upstream_status"])
	}
	if ct, ok := resp["upstream_content_type"].(string); !ok || !strings.Contains(ct, "application/json") {
		t.Fatalf("expected upstream_content_type containing application/json, got %v", resp["upstream_content_type"])
	}
	if ra, ok := resp["upstream_retry_after"].(string); !ok || ra != "90" {
		t.Fatalf("expected upstream_retry_after '90', got %v", resp["upstream_retry_after"])
	}
	if resp["provider_message"] != "spaced error message" {
		t.Fatalf("expected provider_message 'spaced error message', got %v", resp["provider_message"])
	}
}

func TestUpstreamHTTP200PlaintextFailure_V1AndPlayground(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const plaintextFail = "provider error: quota balance empty"

	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(plaintextFail))
	}))
	t.Cleanup(upstream.Close)

	channel := saveVideoPollingTestChannel(t, "test-200-plaintext-channel", upstream.URL)

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/videos", handleCreateVideo)
	engine.POST("/api/playground/run", handlePlaygroundRun)

	// 1. /v1/videos
	resV1 := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/videos", `{"model":"video-model","prompt":"draw something"}`)
	if resV1.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 passthrough, got %d, body: %s", resV1.Code, resV1.Body.String())
	}
	if resV1.Body.String() != plaintextFail {
		t.Fatalf("expected verbatim plaintext body %q, got %q", plaintextFail, resV1.Body.String())
	}
	if ct := resV1.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected Content-Type containing text/plain, got %s", ct)
	}
	if ra := resV1.Header().Get("Retry-After"); ra != "30" {
		t.Fatalf("expected Retry-After 30, got %s", ra)
	}

	if calls := upstreamCalls.Load(); calls != 1 {
		t.Fatalf("expected exactly 1 upstream call, got %d", calls)
	}

	// 2. Playground
	resPG := performJSONRequest(engine, http.MethodPost, "http://gateway.test/api/playground/run", `{"kind":"video","channel_id":"`+channel.ID+`","model":"video-model","prompt":"draw something"}`)
	if resPG.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for playground RPC, got %d, body: %s", resPG.Code, resPG.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(resPG.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, resPG.Body.String())
	}
	if resp["status"] != "error" {
		t.Fatalf("expected status error, got %v", resp["status"])
	}
	if resp["error"] != plaintextFail {
		t.Fatalf("expected error %q, got %v", plaintextFail, resp["error"])
	}
	if resp["upstream_body"] != plaintextFail {
		t.Fatalf("expected upstream_body %q, got %v", plaintextFail, resp["upstream_body"])
	}
	if statusNum, ok := resp["upstream_status"].(float64); !ok || int(statusNum) != 200 {
		t.Fatalf("expected upstream_status 200, got %v", resp["upstream_status"])
	}
	if ct, ok := resp["upstream_content_type"].(string); !ok || !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected upstream_content_type text/plain, got %v", resp["upstream_content_type"])
	}
	if ra, ok := resp["upstream_retry_after"].(string); !ok || ra != "30" {
		t.Fatalf("expected upstream_retry_after '30', got %v", resp["upstream_retry_after"])
	}
}

func TestProfileVideoFailedPollingPreservesUpstreamError(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)

	const failReason = "prompt contains disallowed keywords"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/profile-poll-fail-task" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"status":"failed","error":{"code":"content_violation","message":"`+failReason+`"}}`)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "profile-poll-fail-channel",
		Name:      "Profile Poll Fail",
		Type:      "newapi",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "video-profile-fail",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}

	profile := protocol.Profile{
		SchemaVersion: protocol.CurrentSchemaVersion,
		Operations: []protocol.Operation{{
			Operation:      "video.create",
			ExecutionMode:  protocol.ExecutionAsync,
			PollingMode:    protocol.PollingClient,
			MediaRetention: protocol.MediaRetentionDisabled,
			Submit:         protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
			Response:       protocol.Response{TaskIDPaths: []string{"id"}},
			Poll: &protocol.Poll{
				Method:         http.MethodGet,
				Path:           "/videos/generations/{task_id}",
				IntervalMS:     10,
				MaxAttempts:    5,
				MaxDurationMS:  1000,
				StatusPath:     "status",
				SuccessValues:  []string{"completed"},
				FailureValues:  []string{"failed"},
				ResultURLPaths: []string{"video_url"},
			},
		}},
	}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-poll-fail-prof", Name: "Profile Poll Fail", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{
		ProfileID:     "profile-poll-fail-prof",
		Revision:      1,
		SchemaVersion: compiled.Profile().SchemaVersion,
		ContentJSON:   string(compiled.CanonicalJSON()),
		ContentDigest: compiled.Digest(),
		State:         db.ProfileRevisionPublished,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{
		ChannelID:       channel.ID,
		Operation:       "video.create",
		ModelPattern:    "video-profile-fail",
		ProfileID:       "profile-poll-fail-prof",
		ProfileRevision: 1,
		Enabled:         true,
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Minute)
	run := &db.TaskRun{
		ID:              "profile-poll-fail-run",
		TaskKind:        asyncTaskKindVideo,
		Operation:       "video.create",
		ChannelID:       channel.ID,
		Engine:          "profile",
		ProfileID:       "profile-poll-fail-prof",
		ProfileRevision: 1,
		ProfileDigest:   compiled.Digest(),
		PollingMode:     protocol.PollingClient,
		ProviderTaskID:  "profile-poll-fail-task",
		SubmissionState: "accepted",
		TaskStatus:      model.VideoStatusProcessing,
		DeadlineAt:      &deadline,
	}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: run.ProviderTaskID}); err != nil {
		t.Fatal(err)
	}

	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.GET("/api/playground/video-status", handlePlaygroundVideoStatus)

	res := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/video-status?task_id=profile-poll-fail-task", "")
	if res.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d body: %s", res.Code, res.Body.String())
	}

	var pollResult map[string]interface{}
	if err := json.Unmarshal(res.Body.Bytes(), &pollResult); err != nil {
		t.Fatalf("unmarshal poll result failed: %v", err)
	}

	if pollResult["status"] != "ok" {
		t.Fatalf("expected status 'ok', got %v", pollResult["status"])
	}
	if pollResult["task_status"] != "failed" {
		t.Fatalf("expected task_status 'failed', got %v", pollResult["task_status"])
	}
	errMsg, _ := pollResult["error"].(string)
	if !strings.Contains(errMsg, failReason) {
		t.Fatalf("expected error to contain %q, got %q", failReason, errMsg)
	}
	upBody, _ := pollResult["upstream_body"].(string)
	if !strings.Contains(upBody, failReason) {
		t.Fatalf("expected upstream_body to contain %q, got %q", failReason, upBody)
	}

	// Verify durable status query returns the same error
	resDurable := performJSONRequest(engine, http.MethodGet, "http://gateway.test/api/playground/video-status?task_id=profile-poll-fail-task", "")
	if resDurable.Code != http.StatusOK {
		t.Fatalf("durable status query failed: %d", resDurable.Code)
	}
	var durableResult map[string]interface{}
	if err := json.Unmarshal(resDurable.Body.Bytes(), &durableResult); err != nil {
		t.Fatal(err)
	}
	if durableResult["task_status"] != "failed" {
		t.Fatalf("expected durable task_status failed, got %v", durableResult["task_status"])
	}
	if dErr, _ := durableResult["error"].(string); !strings.Contains(dErr, failReason) {
		t.Fatalf("expected durable error to contain %q, got %q", failReason, dErr)
	}
}
