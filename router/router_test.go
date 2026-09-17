package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/service"
)

func init() {
	if os.Getenv("RELAY_DB_ENCRYPTION_KEY") == "" {
		_ = os.Setenv("RELAY_DB_ENCRYPTION_KEY", "default-test-encryption-key-for-unit-tests-entropy")
	}
}

func TestExtractModelFast(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Standard leading model",
			input:    `{"model": "gpt-4o", "messages": [{"role": "user", "content": "hello"}]}`,
			expected: "gpt-4o",
		},
		{
			name:     "Compact no whitespace",
			input:    `{"model":"claude-3-5-sonnet-20241022","stream":true}`,
			expected: "claude-3-5-sonnet-20241022",
		},
		{
			name:     "Trailing model after huge content",
			input:    `{"messages":[{"role":"user","content":"some very long text..."}],"model":"deepseek-chat"}`,
			expected: "deepseek-chat",
		},
		{
			name:     "Model keyword inside user text before actual model key",
			input:    `{"messages":[{"role":"user","content":"explain what \"model\": means"}],"model":"gemini-1.5-pro"}`,
			expected: "gemini-1.5-pro",
		},
		{
			name:     "Model with slash namespace",
			input:    `{"model": "black-forest-labs/FLUX.1-schnell", "prompt": "a dog"}`,
			expected: "black-forest-labs/FLUX.1-schnell",
		},
		{
			name:     "Missing model",
			input:    `{"prompt": "just prompt without model"}`,
			expected: "",
		},
		{
			name:     "Invalid json",
			input:    `{not valid json`,
			expected: "",
		},
		{
			name:     "Dangling backslash at end of string",
			input:    `{"model": "test\`,
			expected: "",
		},
		{
			name:     "Truncated right after model key",
			input:    `{"model"`,
			expected: "",
		},
		{
			name:     "Escaped quotes in model value",
			input:    `{"model": "model-\"pro\""}`,
			expected: `model-"pro"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := extractModelFast([]byte(tc.input))
			if actual != tc.expected {
				t.Fatalf("extractModelFast(%s) = %q, expected %q", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestIsValidTaskID(t *testing.T) {
	validCases := []string{
		"task_12345",
		"imgjob_abc-def",
		"c828d11c-2234-4567-89ab-cdef01234567",
		"task:123@abc",
		"model.job-99",
	}
	for _, id := range validCases {
		if !db.IsValidTaskID(id) {
			t.Errorf("IsValidTaskID(%q) expected true, got false", id)
		}
	}

	invalidCases := []string{
		"",
		"../etc/passwd",
		"task/123",
		"task\\123",
		"task?param=1",
		"task#anchor",
		"task&param=2",
		"task with space",
		"task\nnewline",
		"task\x00null",
		string(make([]byte, 129)), // > 128 chars
	}
	for _, id := range invalidCases {
		if db.IsValidTaskID(id) {
			t.Errorf("IsValidTaskID(%q) expected false, got true", id)
		}
	}
}

func TestAsyncTaskMappingHelpers(t *testing.T) {
	alias, ids := collectVideoTaskIDs(&model.VideoTaskResponse{ID: "video-public", TaskID: "provider-video-id"})
	if alias != "video-public" || len(ids) != 2 || ids[0] != "video-public" || ids[1] != "provider-video-id" {
		t.Fatalf("video aliases = alias %q ids %v", alias, ids)
	}

	for _, tc := range []struct {
		name       string
		response   interface{}
		wantID     string
		wantStatus string
	}{
		{
			name:       "nested job",
			response:   map[string]interface{}{"job": map[string]interface{}{"id": "image-job", "status": "queued"}},
			wantID:     "image-job",
			wantStatus: "queued",
		},
		{
			name:       "top-level task ID",
			response:   map[string]interface{}{"task_id": "image-task", "status": "processing"},
			wantID:     "image-task",
			wantStatus: "processing",
		},
		{
			name:       "nested data",
			response:   map[string]interface{}{"data": map[string]interface{}{"id": "image-data", "status": "succeeded"}},
			wantID:     "image-data",
			wantStatus: "succeeded",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotID, gotStatus := imageJobDetails(tc.response)
			if gotID != tc.wantID || gotStatus != tc.wantStatus {
				t.Fatalf("imageJobDetails(%#v) = (%q, %q), want (%q, %q)", tc.response, gotID, gotStatus, tc.wantID, tc.wantStatus)
			}
		})
	}

	imageResponse := map[string]interface{}{
		"id":      "wrapper-request-id",
		"task_id": "wrapper-task-id",
		"job": map[string]interface{}{
			"id":      "public-image-id",
			"task_id": "provider-image-id",
			"status":  "queued",
		},
	}
	alias, status, ids := collectImageJobTaskDetails(imageResponse)
	wantIDs := []string{"public-image-id", "provider-image-id", "wrapper-request-id", "wrapper-task-id"}
	if alias != wantIDs[0] || status != "queued" || len(ids) != len(wantIDs) {
		t.Fatalf("image aliases = alias %q status %q ids %v, want %v", alias, status, ids, wantIDs)
	}
	for i, want := range wantIDs {
		if ids[i] != want {
			t.Fatalf("image alias %d = %q, want %q; all=%v", i, ids[i], want, ids)
		}
	}
	lookupIDs := imageJobLookupIDs(ids)
	for i, want := range wantIDs {
		if lookupIDs[i] != imageTaskIDPrefix+want {
			t.Fatalf("image lookup alias %d = %q, want %q", i, lookupIDs[i], imageTaskIDPrefix+want)
		}
	}
}

func TestImageJobLookupIDsPreserveFullLengthExternalIDs(t *testing.T) {
	for _, rawLength := range []int{121, 122, 128} {
		rawID := strings.Repeat("i", rawLength)
		lookupIDs := imageJobLookupIDs([]string{rawID})
		if len(lookupIDs) != 1 {
			t.Fatalf("%d-byte image ID produced lookup IDs %v", rawLength, lookupIDs)
		}
		want := imageTaskIDPrefix + rawID
		if lookupIDs[0] != want {
			t.Fatalf("%d-byte image lookup ID = %q, want %q", rawLength, lookupIDs[0], want)
		}
	}
	if lookupIDs := imageJobLookupIDs([]string{strings.Repeat("x", 129)}); len(lookupIDs) != 0 {
		t.Fatalf("129-byte external image ID produced lookup IDs %v", lookupIDs)
	}
}

func TestNormalizeBaseURLScheme(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"https://api.openai.com/v1", "https://api.openai.com/v1"},
		{"http://localhost:8080", "http://localhost:8080"},
		{"api.deepseek.com/v1", "https://api.deepseek.com/v1"},
		{"localhost:11434", "http://localhost:11434"},
		{"127.0.0.1:8000/v1", "http://127.0.0.1:8000/v1"},
		{"192.168.1.100:8080", "http://192.168.1.100:8080"},
		{"", ""},
		{"   ", ""},
	}
	for _, tc := range cases {
		actual := normalizeBaseURLScheme(tc.input)
		if actual != tc.expected {
			t.Errorf("normalizeBaseURLScheme(%q) = %q, expected %q", tc.input, actual, tc.expected)
		}
	}
}

func TestWriteUpstreamErrorUsesChineseFallbackWithoutInternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	writeUpstreamError(c, fmt.Errorf("dial tcp 10.0.0.5:443: connection refused"), "上游请求失败")
	if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), "上游请求失败") || strings.Contains(recorder.Body.String(), "10.0.0.5") {
		t.Fatalf("upstream fallback response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestResolveAbsoluteURLIgnoresUntrustedForwardedHeaders(t *testing.T) {
	t.Setenv("RELAY_TRUST_PROXY", "")
	req := httptest.NewRequest(http.MethodGet, "http://gateway.test/api/playground/run", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "attacker.example")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = req
	if got, want := resolveAbsoluteURL(c, "/v1/videos/task/content"), "http://gateway.test/v1/videos/task/content"; got != want {
		t.Fatalf("untrusted forwarded headers produced %q, want %q", got, want)
	}

	t.Setenv("RELAY_TRUST_PROXY", "1")
	if got, want := resolveAbsoluteURL(c, "/v1/videos/task/content"), "https://attacker.example/v1/videos/task/content"; got != want {
		t.Fatalf("trusted forwarded headers were not honored: got %q, want %q", got, want)
	}
}

func TestIsStreamRequested(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "Standard stream true with space",
			input:    `{"model": "gpt-4o", "stream": true}`,
			expected: true,
		},
		{
			name:     "Compact stream true without space",
			input:    `{"model":"gpt-4o","stream":true}`,
			expected: true,
		},
		{
			name:     "Stream false",
			input:    `{"model": "gpt-4o", "stream": false}`,
			expected: false,
		},
		{
			name:     "Missing stream field",
			input:    `{"model": "gpt-4o"}`,
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual := isStreamRequested([]byte(tc.input))
			if actual != tc.expected {
				t.Fatalf("isStreamRequested(%s) = %v, expected %v", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestReadBodyAndModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	bodyJSON := `{"model": "gpt-4o-mini", "stream": true, "messages": [{"role": "user", "content": "hi"}]}`
	req, _ := http.NewRequest("POST", "/v1/chat/completions", io.NopCloser(bytes.NewBufferString(bodyJSON)))
	c.Request = req

	body, modelName, err := readBodyAndModel(c)
	if err != nil {
		t.Fatalf("readBodyAndModel returned error: %v", err)
	}
	if modelName != "gpt-4o-mini" {
		t.Fatalf("expected model gpt-4o-mini, got %s", modelName)
	}
	if string(body) != bodyJSON {
		t.Fatalf("expected body matches original, got %s", string(body))
	}
}

func BenchmarkExtractModelFast(b *testing.B) {
	payload := []byte(`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hello world this is a test"}]}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = extractModelFast(payload)
	}
}

func BenchmarkExtractModelSlowJSONUnmarshal(b *testing.B) {
	payload := []byte(`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hello world this is a test"}]}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var meta struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(payload, &meta)
	}
}

func TestParseVideoGenerationRequestMultipart(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("model", "grok-imagine-video")
	_ = writer.WriteField("prompt", "a cinematic drone shot of mountains")
	_ = writer.WriteField("duration", "6")
	_ = writer.WriteField("aspect_ratio", "16:9")
	_ = writer.WriteField("resolution_name", "720p")
	_ = writer.WriteField("first_frame_url", "https://example.com/first.png")
	_ = writer.Close()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req, _ := http.NewRequest("POST", "/v1/videos", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	c.Request = req

	parsed, err := parseVideoGenerationRequest(c)
	if err != nil {
		t.Fatalf("unexpected error parsing multipart video request: %v", err)
	}
	if parsed.Model != "grok-imagine-video" {
		t.Errorf("expected model grok-imagine-video, got %s", parsed.Model)
	}
	if parsed.Prompt != "a cinematic drone shot of mountains" {
		t.Errorf("expected prompt match, got %s", parsed.Prompt)
	}
	if parsed.Seconds != "6" {
		t.Errorf("expected seconds 6, got %s", parsed.Seconds)
	}
	if parsed.Size != "16:9" {
		t.Errorf("expected size 16:9, got %s", parsed.Size)
	}
	if parsed.Quality != "720p" {
		t.Errorf("expected quality 720p, got %s", parsed.Quality)
	}
	if parsed.FirstFrame != "https://example.com/first.png" {
		t.Errorf("expected first frame match, got %s", parsed.FirstFrame)
	}
}

func TestParseVideoGenerationRequestJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jsonPayload := `{"model":"sora-2","prompt":"snowy forest","duration":10,"aspect_ratio":"9:16","with_audio":true}`

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req, _ := http.NewRequest("POST", "/v1/videos", bytes.NewBufferString(jsonPayload))
	req.Header.Set("Content-Type", "application/json")
	c.Request = req

	parsed, err := parseVideoGenerationRequest(c)
	if err != nil {
		t.Fatalf("unexpected error parsing JSON video request: %v", err)
	}
	if parsed.Model != "sora-2" {
		t.Errorf("expected model sora-2, got %s", parsed.Model)
	}
	if parsed.Seconds != "10" {
		t.Errorf("expected numeric duration converted to string 10, got %s", parsed.Seconds)
	}
	if parsed.Size != "9:16" {
		t.Errorf("expected size 9:16, got %s", parsed.Size)
	}
	if !parsed.GenerateAudio {
		t.Errorf("expected GenerateAudio true")
	}
}

func TestFormatVideoTaskResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req, _ := http.NewRequest("GET", "/v1/videos/task_123", nil)
	req.Host = "127.0.0.1:8000"
	c.Request = req

	taskResp := &model.VideoTaskResponse{
		ID:       "task_123",
		Status:   "completed",
		VideoURL: "/v1/videos/task_123/content",
	}

	formatVideoTaskResponse(c, taskResp)

	if taskResp.TaskID != "task_123" {
		t.Errorf("expected TaskID task_123, got %s", taskResp.TaskID)
	}
	expectedURL := "http://127.0.0.1:8000/v1/videos/task_123/content"
	if taskResp.VideoURL != expectedURL {
		t.Errorf("expected VideoURL %s, got %s", expectedURL, taskResp.VideoURL)
	}
	if taskResp.URL != expectedURL {
		t.Errorf("expected URL %s, got %s", expectedURL, taskResp.URL)
	}
	if len(taskResp.Data) != 1 || taskResp.Data[0]["url"] != expectedURL {
		t.Errorf("expected Data array with url %s, got %+v", expectedURL, taskResp.Data)
	}
}

func TestFormatVideoTaskResponseReplacesExpiringUpstreamURL(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req, _ := http.NewRequest("GET", "/v1/videos/task_stable", nil)
	req.Host = "gateway.example.test"
	c.Request = req

	upstreamURL := "https://cdn.example.test/video.mp4?expires=123&signature=temporary"
	taskResp := &model.VideoTaskResponse{
		ID:     "public-request-id",
		TaskID: "provider-video-id",
		// Some providers make a playable but short-lived URL available before
		// they normalize the task status to completed.
		Status:   "processing",
		VideoURL: upstreamURL,
		URL:      upstreamURL,
		Data:     []map[string]string{{"url": upstreamURL, "video_url": upstreamURL}},
	}

	formatVideoTaskResponse(c, taskResp)
	want := "http://gateway.example.test/v1/videos/provider-video-id/content"
	if taskResp.VideoURL != want || taskResp.URL != want {
		t.Fatalf("stable video URLs = %q and %q, want %q", taskResp.VideoURL, taskResp.URL, want)
	}
	if len(taskResp.Data) != 1 || taskResp.Data[0]["url"] != want {
		t.Fatalf("stable video data = %+v, want URL %q", taskResp.Data, want)
	}
	if taskResp.Data[0]["video_url"] != want {
		t.Fatalf("stable video data.video_url = %q, want %q", taskResp.Data[0]["video_url"], want)
	}
}

func TestAuthMiddlewareContentBypass(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	config.SetAPIKey("sk-secret-key-123456")

	// 1. 普通请求未携带 key 应返回 401
	engine := Setup()
	rec1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("GET", "/v1/models", nil)
	engine.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauthenticated /models, got %d", rec1.Code)
	}

	// 2. 视频内容是公开播放资源，但仍必须命中已登记的视频映射。
	taskID := "task_test_123"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/videos/generations/"+taskID+"/content" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write([]byte("video-bytes"))
	}))
	t.Cleanup(upstream.Close)
	channel := saveVideoPollingTestChannel(t, "public-video-content-auth-test", upstream.URL)
	if err := db.EnsureTaskMappings(db.TaskMapping{
		TaskID:    taskID,
		ChannelID: channel.ID,
		TaskKind:  asyncTaskKindVideo,
		TaskAlias: taskID,
	}); err != nil {
		t.Fatal(err)
	}

	rec2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/v1/videos/"+taskID+"/content", nil)
	engine.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK || rec2.Body.String() != "video-bytes" {
		t.Errorf("expected public /content playback, got %d %q", rec2.Code, rec2.Body.String())
	}

	// 3. Task status remains protected even though its content is public.
	rec3 := httptest.NewRecorder()
	req3, _ := http.NewRequest("GET", "/v1/videos/"+taskID, nil)
	engine.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusUnauthorized {
		t.Errorf("expected video status to require auth, got %d", rec3.Code)
	}
}

func TestHandleTestChannelReportsCredentialConfigurationError(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "correct-channel-test-key")
	if err := db.InitDB(t.TempDir() + "/channel-test-key.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{
		ID: "credential-error-channel", Type: "openai",
		BaseURL: "https://example.invalid/v1", Enabled: true, APIKey: "provider-secret",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "wrong-channel-test-key")

	engine := gin.New()
	engine.POST("/api/channels/:id/test", handleTestChannel)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/channels/"+channel.ID+"/test", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("credential configuration error status = %d, want 500: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "RELAY_DB_ENCRYPTION_KEY") || strings.Contains(response.Body.String(), "渠道不存在") {
		t.Fatalf("credential configuration error was misreported: %s", response.Body.String())
	}
}

func TestSanitizedChannelModelMasksCustomCredentials(t *testing.T) {
	cm := db.ChannelModel{
		ID:               "masked-channel",
		Type:             "openai",
		APIKey:           "provider-key",
		HeadersRaw:       `{"Authorization":"Bearer header-secret","X-Trace":"keep"}`,
		LastErrorMessage: "upstream-secret-response",
	}
	result := sanitizedChannelModel(cm)
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "header-secret") || strings.Contains(string(encoded), "upstream-secret-response") {
		t.Fatalf("sanitized channel leaked sensitive data: %s", encoded)
	}
	headers, ok := result["headers_raw"].(string)
	if !ok || !strings.Contains(headers, `"Authorization":"********"`) || !strings.Contains(headers, `"X-Trace":"keep"`) {
		t.Fatalf("sanitized headers lost masking or safe values: %s", encoded)
	}
}

func TestAdminAuthMiddleware(t *testing.T) {
	config.SetAPIKey("test-admin-secret-key-888")
	engine := Setup()

	// 1. 未授权请求 /api/channels 应返回 401
	rec1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("GET", "/api/channels", nil)
	engine.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauthenticated /api/channels, got %d", rec1.Code)
	}

	// 2. 管理员 API 不接受 gateway Bearer；必须使用管理员 Session Cookie。
	rec2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/api/channels", nil)
	req2.Header.Set("Authorization", "Bearer test-admin-secret-key-888")
	engine.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Errorf("expected Bearer to be rejected for admin API, got %d", rec2.Code)
	}

	// 3. x-api-key 同样只用于 /v1，不可冒充管理员 Session。
	rec3 := httptest.NewRecorder()
	req3, _ := http.NewRequest("GET", "/api/settings", nil)
	req3.Header.Set("x-api-key", "test-admin-secret-key-888")
	engine.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusUnauthorized {
		t.Errorf("expected x-api-key to be rejected for admin API, got %d", rec3.Code)
	}
}

func TestExtractModelFastNestedAndPromptCollision(t *testing.T) {
	// 用户 prompt 内容中包含 `"model": "fake-model"`，实际模型在根级别 `"model": "real-deepseek-r1"`
	promptCollision := `{"messages":[{"role":"user","content":"what is \"model\": \"gpt-4o\" in your prompt?"}],"model":"real-deepseek-r1"}`
	model := extractModelFast([]byte(promptCollision))
	if model != "real-deepseek-r1" {
		t.Errorf("expected 'real-deepseek-r1', but got %q (vulnerable to prompt collision!)", model)
	}

	// 根级别 model 在前，nested object 里也带 model
	nestedObj := `{"model":"primary-model","extra":{"model":"sub-model","nested":{"model":"inner"}}}`
	model2 := extractModelFast([]byte(nestedObj))
	if model2 != "primary-model" {
		t.Errorf("expected 'primary-model', got %q", model2)
	}
}

func TestCORSLocalhostVsExternal(t *testing.T) {
	engine := Setup()

	// 1. /v1 使用无凭据 wildcard CORS，不反射 Origin。
	recLocal := httptest.NewRecorder()
	reqLocal, _ := http.NewRequest("OPTIONS", "/v1/models", nil)
	reqLocal.Header.Set("Origin", "http://localhost:5173")
	engine.ServeHTTP(recLocal, reqLocal)

	if recLocal.Header().Get("Access-Control-Allow-Credentials") != "" {
		t.Errorf("/v1 must not allow credentials, got %s", recLocal.Header().Get("Access-Control-Allow-Credentials"))
	}
	if recLocal.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected wildcard Origin, got %s", recLocal.Header().Get("Access-Control-Allow-Origin"))
	}

	// 2. 外部恶意 origin 不得反射 Credentials: true
	recExt := httptest.NewRecorder()
	reqExt, _ := http.NewRequest("OPTIONS", "/v1/models", nil)
	reqExt.Header.Set("Origin", "http://malicious-site.com")
	engine.ServeHTTP(recExt, reqExt)

	if recExt.Header().Get("Access-Control-Allow-Credentials") == "true" {
		t.Errorf("malicious external site MUST NOT receive Access-Control-Allow-Credentials: true")
	}
	if recExt.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("expected wildcard Origin for external non-credentialed request, got %s", recExt.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestIsSafeProbeURL(t *testing.T) {
	// Keep this test independent of the host/network resolver. CI and sandbox
	// DNS providers may synthesize private IPv6 answers for otherwise public
	// names, which would make the result environment-dependent.
	publicResolver := func(host string) ([]string, error) {
		return map[string][]string{
			"api.openai.com":           {"104.18.33.45"},
			"api.deepseek.com":         {"104.18.7.190"},
			"api.siliconflow.cn":       {"104.21.32.1"},
			"api.external-transit.com": {"93.184.216.34"},
		}[host], nil
	}
	unsafeURLs := []string{
		"http://169.254.169.254/latest/meta-data/",
		"http://169.254.1.1/computeMetadata/v1",
		"http://metadata.google.internal/computeMetadata/v1",
		"ftp://api.openai.com/v1",
		"file:///etc/passwd",
	}
	for _, u := range unsafeURLs {
		if err := isSafeProbeURLWithResolver(u, publicResolver); err == nil {
			t.Errorf("isSafeProbeURL(%q) expected error for unsafe URL, but got nil", u)
		}
	}

	safeURLs := []string{
		"https://api.openai.com/v1",
		"https://api.deepseek.com/v1",
		"https://api.siliconflow.cn/v1",
		"http://api.external-transit.com:8080/v1",
		"http://localhost:11434/v1",
	}
	for _, u := range safeURLs {
		if err := isSafeProbeURLWithResolver(u, publicResolver); err != nil {
			t.Errorf("isSafeProbeURL(%q) expected safe URL, but got err: %v", u, err)
		}
	}
}

func TestIsSafeProbeURLWithResolverRejectsRebindingAndFailures(t *testing.T) {
	if err := isSafeProbeURLWithResolver("https://public.example/v1", func(string) ([]string, error) {
		return []string{"93.184.216.34", "10.0.0.1"}, nil
	}); err == nil {
		t.Fatal("expected mixed public/private DNS answers to be rejected")
	}

	if err := isSafeProbeURLWithResolver("https://public.example/v1", func(string) ([]string, error) {
		return nil, fmt.Errorf("lookup failed")
	}); err == nil {
		t.Fatal("expected DNS failures to be rejected")
	}

	if err := isSafeProbeURLWithResolver("https://public.example/v1", nil); err == nil {
		t.Fatal("expected unavailable resolver to be rejected")
	}
}

func TestNormalizeVideoStatus(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"succeeded", model.VideoStatusCompleted},
		{"SUCCEEDED", model.VideoStatusCompleted},
		{"success", model.VideoStatusCompleted},
		{"complete", model.VideoStatusCompleted},
		{"completed", model.VideoStatusCompleted},
		{"COMPLETED", model.VideoStatusCompleted},
		{"running", model.VideoStatusProcessing},
		{"Running", model.VideoStatusProcessing},
		{"in_progress", model.VideoStatusProcessing},
		{"generating", model.VideoStatusProcessing},
		{"queued", model.VideoStatusQueued},
		{"Pending", model.VideoStatusQueued},
		{"failed", model.VideoStatusFailed},
		{"FAILED", model.VideoStatusFailed},
		{"error", model.VideoStatusFailed},
		{"custom_unknown", "custom_unknown"},
	}

	for _, tc := range cases {
		got := model.NormalizeVideoStatus(tc.input)
		if got != tc.expected {
			t.Errorf("NormalizeVideoStatus(%q) = %q, expected %q", tc.input, got, tc.expected)
		}
	}
}

func TestFormatVideoTaskResponseWithTaskIdOnly(t *testing.T) {
	// 测试当上游仅返回 task_id 时，formatVideoTaskResponse 能正确将 task_id 填补到 ID 字段
	resp := &model.VideoTaskResponse{
		TaskID:   "task_from_upstream_999",
		Status:   "completed",
		VideoURL: "/v1/videos/task_from_upstream_999/content",
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/v1/videos/task_from_upstream_999", nil)
	c.Request.Host = "localhost:8000"

	formatVideoTaskResponse(c, resp)

	if resp.ID != "task_from_upstream_999" {
		t.Errorf("expected resp.ID to be populated from TaskID, got %q", resp.ID)
	}
	if resp.TaskID != "task_from_upstream_999" {
		t.Errorf("expected resp.TaskID to remain populated, got %q", resp.TaskID)
	}
	if resp.VideoURL != "http://localhost:8000/v1/videos/task_from_upstream_999/content" {
		t.Errorf("unexpected VideoURL: %s", resp.VideoURL)
	}
}

func TestComprehensiveOfficialEndpointsRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := Setup()

	endpoints := []string{
		"/v1/messages/count_tokens",
		"/v1/moderations",
		"/v1/audio/transcriptions",
		"/v1/audio/translations",
	}

	for _, ep := range endpoints {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, ep, strings.NewReader("{}"))
		r.ServeHTTP(w, req)
		// Should require authorization or return valid HTTP status, definitely not 404
		if w.Code == http.StatusNotFound {
			t.Errorf("endpoint %s returned 404 Not Found, expected it to be registered", ep)
		}
	}
}

func TestAnthropicCountTokensAddsDefaultModel(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	var upstreamBody []byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages/count_tokens" {
			http.NotFound(w, r)
			return
		}
		upstreamBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"input_tokens":3}`))
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "anthropic-count-tokens-default-model",
		Name:      "anthropic-count-tokens-default-model",
		Type:      "anthropic",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "test-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "claude-3-7-sonnet",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	engine := gin.New()
	engine.POST("/v1/messages/count_tokens", handleAnthropicCountTokens)
	response := performJSONRequest(engine, http.MethodPost, "http://gateway.test/v1/messages/count_tokens", `{"messages":[{"role":"user","content":"hello"}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	var forwarded map[string]any
	if err := json.Unmarshal(upstreamBody, &forwarded); err != nil {
		t.Fatalf("decode forwarded request: %v", err)
	}
	if forwarded["model"] != "claude-3-7-sonnet" {
		t.Fatalf("expected default model to be forwarded, got %v", forwarded["model"])
	}
}
