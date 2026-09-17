package protocol

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func testExecutorProfile() Profile {
	return Profile{SchemaVersion: 1, Operations: []Operation{
		{
			Operation: "video.create", ExecutionMode: ExecutionAsync, PollingMode: PollingClient,
			Submit:   Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json", Body: map[string]any{"preset": "default"}},
			Response: Response{TaskIDPaths: []string{"id"}},
			Poll:     &Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 3, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"url"}},
			Content:  &Content{Method: http.MethodGet, Path: "/videos/{task_id}/content"},
		},
	}}
}

func TestResultStringsUsesThumbnailAsImageFallback(t *testing.T) {
	thumbnailOnly := resultStrings(map[string]any{"thumbnail_url": "https://cdn.example/thumb.png"})
	if len(thumbnailOnly) != 1 || thumbnailOnly[0] != "https://cdn.example/thumb.png" {
		t.Fatalf("thumbnail-only result = %v", thumbnailOnly)
	}
	withProxy := resultStrings(map[string]any{"proxy_url": "https://cdn.example/image.png", "thumbnail_url": "https://cdn.example/thumb.png"})
	if len(withProxy) != 1 || withProxy[0] != "https://cdn.example/image.png" {
		t.Fatalf("proxy result should win over thumbnail: %v", withProxy)
	}
}

func TestHTTPExecutorAppliesOperationHeaders(t *testing.T) {
	profile := testExecutorProfile()
	profile.Operations[0].Submit.Headers = map[string]string{"X-Operation": "submit"}
	profile.Operations[0].Poll.Headers = map[string]string{"X-Operation": "poll"}
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "poll"
		if r.URL.Path == "/videos" {
			want = "submit"
		}
		if got := r.Header.Get("X-Operation"); got != want {
			t.Errorf("operation header = %q, want %q", got, want)
		}
		if r.URL.Path == "/videos" {
			_, _ = io.WriteString(w, `{"id":"task"}`)
			return
		}
		_, _ = io.WriteString(w, `{"status":"completed","url":"https://cdn.example/video"}`)
	}))
	defer server.Close()
	executor := NewHTTPExecutor(server.Client())
	result, err := executor.Submit(context.Background(), compiled, "video.create", Request{BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.PollOnce(context.Background(), compiled, "video.create", Request{BaseURL: server.URL, TaskID: result.TaskID})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHTTPExecutorSubmitPollAndContent(t *testing.T) {
	var submitKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/videos":
			if got := r.Header.Get("Idempotency-Key"); got != "stable-key" {
				t.Errorf("idempotency key = %q", got)
			}
			submitKey = r.Header.Get("Authorization")
			if submitKey == "Bearer bad" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, `{"error":"bad key"}`)
				return
			}
			if r.Method != http.MethodPost {
				t.Fatalf("submit method = %s", r.Method)
			}
			_, _ = io.WriteString(w, `{"id":"provider-1"}`)
		case "/videos/provider-1":
			if r.Header.Get("Authorization") != "Bearer good" {
				t.Errorf("poll authorization = %q", r.Header.Get("Authorization"))
			}
			_, _ = io.WriteString(w, `{"status":"completed","url":"https://cdn.example/video.mp4"}`)
		case "/videos/provider-1/content":
			if r.Header.Get("Authorization") != "Bearer good" {
				t.Errorf("content authorization = %q", r.Header.Get("Authorization"))
			}
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = io.WriteString(w, "video-bytes")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	compiled, err := Compile(testExecutorProfile())
	if err != nil {
		t.Fatal(err)
	}
	executor := NewHTTPExecutor(server.Client())
	result, err := executor.Submit(context.Background(), compiled, "video.create", Request{BaseURL: server.URL, APIKeys: []string{"bad", "good"}, Headers: map[string]string{"X-Test": "yes"}, Body: map[string]any{"prompt": "hello"}, IdempotencyKey: "stable-key"})
	if err != nil || result.TaskID != "provider-1" || submitKey != "Bearer good" {
		t.Fatalf("submit result = %+v, %v auth=%q", result, err, submitKey)
	}
	poll, err := executor.PollOnce(context.Background(), compiled, "video.create", Request{BaseURL: server.URL, APIKeys: []string{"good"}, TaskID: result.TaskID})
	if err != nil || poll.Status != "completed" || len(poll.ResultURLs) != 1 {
		t.Fatalf("poll result = %+v, %v", poll, err)
	}
	content, err := executor.FetchContent(context.Background(), compiled, "video.create", Request{BaseURL: server.URL, APIKeys: []string{"good"}, TaskID: result.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	defer content.Body.Close()
	body, _ := io.ReadAll(content.Body)
	if string(body) != "video-bytes" || content.HTTPStatus != http.StatusOK {
		t.Fatalf("content = %d %q", content.HTTPStatus, body)
	}
}

func TestHTTPExecutorExtractsURLsFromObjectArrays(t *testing.T) {
	profile := Profile{SchemaVersion: 1, Operations: []Operation{{
		Operation: "images.create", ExecutionMode: ExecutionDirect, PollingMode: PollingOff,
		Submit:   Submit{Method: http.MethodPost, Path: "/images/generations", BodyEncoding: "json"},
		Response: Response{ResultPaths: []string{"data"}},
	}}}
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"url":"https://cdn.example/one.png"},{"url":"https://cdn.example/two.png"}]}`)
	}))
	defer server.Close()
	result, err := NewHTTPExecutor(server.Client()).Submit(context.Background(), compiled, "images.create", Request{BaseURL: server.URL, Body: map[string]any{"prompt": "two"}})
	if err != nil || len(result.ResultURLs) != 2 || result.ResultURLs[0] != "https://cdn.example/one.png" || result.ResultURLs[1] != "https://cdn.example/two.png" {
		t.Fatalf("result URLs = %#v, err=%v", result.ResultURLs, err)
	}
}

func TestHTTPExecutorExtractsBase64ImagesFromObjectArrays(t *testing.T) {
	profile := Profile{SchemaVersion: 1, Operations: []Operation{{
		Operation: "images.create", ExecutionMode: ExecutionDirect, PollingMode: PollingOff,
		Submit:   Submit{Method: http.MethodPost, Path: "/images/generations", BodyEncoding: "json"},
		Response: Response{ResultPaths: []string{"data"}},
	}}}
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"b64_json":"iVBORw0KGgo="}]}`)
	}))
	defer server.Close()
	result, err := NewHTTPExecutor(server.Client()).Submit(context.Background(), compiled, "images.create", Request{BaseURL: server.URL})
	if err != nil || len(result.ResultURLs) != 1 || result.ResultURLs[0] != "iVBORw0KGgo=" {
		t.Fatalf("base64 image results = %#v, err=%v", result.ResultURLs, err)
	}
}

func TestHTTPExecutorSupportsNestedAsyncJobEnvelope(t *testing.T) {
	profile := Profile{SchemaVersion: 1, Operations: []Operation{{
		Operation: "images.create", ExecutionMode: ExecutionAsync, PollingMode: PollingBackground,
		Submit:   Submit{Method: http.MethodPost, Path: "/images/jobs", BodyEncoding: "json"},
		Response: Response{TaskIDPaths: []string{"job.id"}},
		Poll:     &Poll{Method: http.MethodGet, Path: "/images/jobs/{task_id}", IntervalMS: 5000, MaxAttempts: 360, MaxDurationMS: 1800000, StatusPath: "job.status", SuccessValues: []string{"succeeded"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"job.assets.0.proxy_url", "job.assets.1.proxy_url"}},
	}}}
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/images/jobs":
			_, _ = io.WriteString(w, `{"job":{"id":"task_nested_image","status":"queued"}}`)
		case "/images/jobs/task_nested_image":
			_, _ = io.WriteString(w, `{"job":{"id":"task_nested_image","status":"succeeded","assets":[{"proxy_url":"https://cdn.example/one.png"},{"proxy_url":"https://cdn.example/two.png"}]}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	executor := NewHTTPExecutor(server.Client())
	accepted, err := executor.Submit(context.Background(), compiled, "images.create", Request{BaseURL: server.URL, Body: map[string]any{"prompt": "nested"}})
	if err != nil || accepted.TaskID != "task_nested_image" {
		t.Fatalf("nested submit result = %+v, err=%v", accepted, err)
	}
	poll, err := executor.PollOnce(context.Background(), compiled, "images.create", Request{BaseURL: server.URL, TaskID: accepted.TaskID})
	if err != nil || poll.Status != "succeeded" || len(poll.ResultURLs) != 2 || poll.ResultURLs[0] != "https://cdn.example/one.png" || poll.ResultURLs[1] != "https://cdn.example/two.png" {
		t.Fatalf("nested poll result = %+v, err=%v", poll, err)
	}
}

func TestHTTPExecutorPreservesBaseURLPathPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/videos" {
			t.Fatalf("request path = %q, want /v1/videos", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"id":"prefixed-task"}`)
	}))
	defer server.Close()
	compiled, err := Compile(testExecutorProfile())
	if err != nil {
		t.Fatal(err)
	}
	executor := NewHTTPExecutor(server.Client())
	result, err := executor.Submit(context.Background(), compiled, "video.create", Request{BaseURL: server.URL + "/v1"})
	if err != nil || result.TaskID != "prefixed-task" {
		t.Fatalf("submit result = %+v, %v", result, err)
	}
}

func TestHTTPExecutorRejectsMissingPathVariableAndOversizedResponse(t *testing.T) {
	compiled, err := Compile(testExecutorProfile())
	if err != nil {
		t.Fatal(err)
	}
	executor := NewHTTPExecutor(nil)
	if _, err := executor.PollOnce(context.Background(), compiled, "video.create", Request{BaseURL: "http://127.0.0.1", TaskID: ""}); err == nil {
		t.Fatal("missing task ID should be rejected")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, strings.Repeat("x", 20)) }))
	defer server.Close()
	executor = NewHTTPExecutor(server.Client())
	if _, err := executor.Submit(context.Background(), compiled, "video.create", Request{BaseURL: server.URL, MaxResponseBytes: 4, Body: map[string]any{}}); err == nil {
		t.Fatal("oversized response should be rejected")
	}
}

func testDirectProfile() Profile {
	return Profile{SchemaVersion: 1, Operations: []Operation{
		{Operation: "chat.completions", ExecutionMode: ExecutionDirect, PollingMode: PollingOff,
			Submit:   Submit{Method: http.MethodPost, Path: "/chat/completions", BodyEncoding: "json"},
			Response: Response{ResultPaths: []string{"choices"}}},
	}}
}

func TestHTTPExecutorExecuteRawJSONAndCredentialHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" || r.Header.Get("x-api-key") != "anthropic-secret" {
			t.Fatalf("request path/header = %q/%q", r.URL.Path, r.Header.Get("x-api-key"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","content":[{"type":"text","text":"ok"}]}`)
	}))
	defer server.Close()
	profile := testDirectProfile()
	profile.Operations[0].Operation = "messages.create"
	profile.Operations[0].Submit.Path = "/messages"
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	result, err := NewHTTPExecutor(server.Client()).ExecuteRaw(context.Background(), compiled, "messages.create", Request{BaseURL: server.URL + "/v1", APIKeys: []string{"anthropic-secret"}, APIKeyHeader: "x-api-key", Body: map[string]any{"model": "claude"}}, recorder, false)
	if err != nil || result.HTTPStatus != http.StatusOK || recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"msg_1"`) {
		t.Fatalf("raw JSON result=%+v err=%v code=%d body=%q", result, err, recorder.Code, recorder.Body.String())
	}
}

func TestHTTPExecutorExecuteRawStreamsSSE(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"delta":{"content":"hello"}}
data: [DONE]

`)
	}))
	defer server.Close()
	compiled, err := Compile(testDirectProfile())
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	_, err = NewHTTPExecutor(server.Client()).ExecuteRaw(context.Background(), compiled, "chat.completions", Request{BaseURL: server.URL, APIKeys: []string{"key"}, Body: map[string]any{"model": "m", "stream": true}}, recorder, true)
	if err != nil || recorder.Code != http.StatusOK || recorder.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(recorder.Body.String(), "[DONE]") {
		t.Fatalf("raw SSE err=%v code=%d content-type=%q body=%q", err, recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String())
	}
}

func TestHTTPExecutorExecuteRawRejectsOversizedJSONBeforeWrite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, strings.Repeat("x", 32)) }))
	defer server.Close()
	compiled, err := Compile(testDirectProfile())
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	result, err := NewHTTPExecutor(server.Client()).ExecuteRaw(context.Background(), compiled, "chat.completions", Request{BaseURL: server.URL, MaxResponseBytes: 8, Body: map[string]any{}}, recorder, false)
	if err == nil || !errors.Is(err, ErrResponseTooLarge) || len(result.RawBody) == 0 || recorder.Body.Len() != 0 {
		t.Fatalf("oversized raw result=%+v err=%v body=%q", result, err, recorder.Body.String())
	}
}

func TestHTTPExecutorExecuteRawPreservesRawMultipartPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Type"); got != "multipart/form-data; boundary=test-boundary" {
			t.Fatalf("content type = %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "--test-boundary--\r\n" {
			t.Fatalf("raw body = %q", body)
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer server.Close()
	compiled, err := Compile(testDirectProfile())
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	result, err := NewHTTPExecutor(server.Client()).ExecuteRaw(context.Background(), compiled, "chat.completions", Request{
		BaseURL: server.URL, RawBody: []byte("--test-boundary--\r\n"), RawContentType: "multipart/form-data; boundary=test-boundary",
	}, recorder, false)
	if err != nil || result.HTTPStatus != http.StatusOK || recorder.Code != http.StatusOK {
		t.Fatalf("raw multipart result=%+v err=%v code=%d", result, err, recorder.Code)
	}
}

func TestHTTPExecutorAsyncPostNetworkErrorDoesNotRotateKey(t *testing.T) {
	var key1Calls, key2Calls int
	var callsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "Bearer key1" {
			callsMu.Lock()
			key1Calls++
			callsMu.Unlock()
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				_ = conn.Close()
				return
			}
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		if auth == "Bearer key2" {
			callsMu.Lock()
			key2Calls++
			callsMu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"id":"task-key2"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	compiled, err := Compile(testExecutorProfile())
	if err != nil {
		t.Fatal(err)
	}
	executor := NewHTTPExecutor(server.Client())
	_, err = executor.Submit(context.Background(), compiled, "video.create", Request{
		BaseURL: server.URL,
		APIKeys: []string{"key1", "key2"},
	})
	if err == nil {
		t.Fatal("expected network error, got nil")
	}
	var execErr *ExecutorError
	if !errors.As(err, &execErr) {
		t.Fatalf("expected ExecutorError, got %T: %v", err, err)
	}
	if !execErr.MayHaveSubmitted {
		t.Fatalf("expected MayHaveSubmitted=true, got false")
	}
	callsMu.Lock()
	gotKey1Calls, gotKey2Calls := key1Calls, key2Calls
	callsMu.Unlock()
	if gotKey1Calls != 1 {
		t.Errorf("key1Calls = %d, want 1", gotKey1Calls)
	}
	if gotKey2Calls != 0 {
		t.Errorf("key2Calls = %d, want 0 (must not rotate key on mutating network error)", gotKey2Calls)
	}
}
