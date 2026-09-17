package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"relay-gateway/config"
	"relay-gateway/model"
)

func TestBaseAdapterInheritance(t *testing.T) {
	base := NewOpenAIAdapter()
	var _ Adapter = base
	Register(AdapterMeta{Type: "custom_transit", Name: "Custom Transit"}, base)
	if Get("custom_transit") != nil {
		t.Fatal("v2 must reject custom adapter registrations")
	}
}

func TestGetFailsClosedWhenLegacyAdaptersAreDisabled(t *testing.T) {
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	if Get("openai") != nil {
		t.Fatal("global Legacy disable switch returned an adapter")
	}
}

func TestReadLimitedResponseBody(t *testing.T) {
	body, err := readLimitedResponseBody(strings.NewReader("1234"), 4)
	if err != nil || string(body) != "1234" {
		t.Fatalf("read at limit = %q, %v", body, err)
	}
	if _, err := readLimitedResponseBody(strings.NewReader("12345"), 4); err == nil {
		t.Fatal("oversized response body was accepted")
	}
}

func TestExecuteWithKeyRotationSuccess(t *testing.T) {
	var attempts atomic.Int32
	var usedKeys []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		auth := r.Header.Get("Authorization")
		usedKeys = append(usedKeys, auth)

		if att == 1 {
			// First attempt returns 429 Too Many Requests
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}

		// Second key succeeds
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	}))
	defer server.Close()

	ch := &config.UpstreamChannel{
		ID:      "test-rot",
		Type:    "openai",
		BaseURL: server.URL + "/v1",
		APIKeys: []string{"key-first-429", "key-second-success"},
	}

	adp := NewOpenAIAdapter()
	var finalBody string

	err := adp.ExecuteWithKeyRotation(context.Background(), ch, func(key string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/v1/test", nil)
		if err != nil {
			return nil, err
		}
		adp.SetHeadersWithKey(req, ch, key)
		return req, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		finalBody = string(b)
		return nil
	})

	if err != nil {
		t.Fatalf("ExecuteWithKeyRotation failed: %v", err)
	}

	if attempts.Load() != 2 {
		t.Errorf("expected 2 attempts after 429, got %d", attempts.Load())
	}

	if finalBody != `{"result":"ok"}` {
		t.Errorf("unexpected body: %s", finalBody)
	}
}

func TestExecuteWithKeyRotationKeepsStableIdempotencyKey(t *testing.T) {
	var keys []string
	var idempotency []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, r.Header.Get("Authorization"))
		idempotency = append(idempotency, r.Header.Get("Idempotency-Key"))
		if len(keys) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	channel := &config.UpstreamChannel{ID: "stable-idempotency", Type: "openai", BaseURL: server.URL, APIKeys: []string{"first", "second"}}
	adapter := NewOpenAIAdapter()
	ctx := context.WithValue(context.Background(), CtxIdempotencyKey, "client-request-123")
	err := adapter.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/tasks", bytes.NewReader([]byte(`{"model":"video"}`)))
		if err != nil {
			return nil, err
		}
		adapter.SetHeadersWithKey(req, channel, key)
		return req, nil
	}, func(resp *http.Response, _ string) error {
		defer resp.Body.Close()
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteWithKeyRotation failed: %v", err)
	}
	if len(idempotency) != 2 || idempotency[0] == "" || idempotency[0] != idempotency[1] || idempotency[0] != "client-request-123" {
		t.Fatalf("expected one stable caller idempotency key, got %v", idempotency)
	}
}

func TestRewriteJSONModelFastPath(t *testing.T) {
	// 1. Same model: should return identical slice
	original := []byte(`{"model": "gpt-4o", "messages": [{"role":"user","content":"hi"}]}`)
	result := RewriteJSONModel(original, "gpt-4o")
	if string(result) != string(original) {
		t.Errorf("expected original slice returned when model matches, got: %s", string(result))
	}

	// 2. Different model: should rewrite model correctly
	diff := RewriteJSONModel(original, "deepseek-chat")
	if !bytes.Contains(diff, []byte(`"model":"deepseek-chat"`)) {
		t.Errorf("expected model to be rewritten, got: %s", string(diff))
	}
	if !bytes.Contains(diff, []byte(`"content":"hi"`)) {
		t.Errorf("expected content preserved, got: %s", string(diff))
	}
}

func TestForwardStreamBufferPool(t *testing.T) {
	sourceData := "data: hello\n\ndata: world\n\ndata: [DONE]\n\n"
	src := bytes.NewReader([]byte(sourceData))
	recorder := httptest.NewRecorder()

	err := ForwardStream(context.Background(), src, recorder)
	if err != nil {
		t.Fatalf("ForwardStream failed: %v", err)
	}

	if recorder.Body.String() != sourceData {
		t.Errorf("expected %q, got %q", sourceData, recorder.Body.String())
	}
}

func TestExecuteWithKeyRotation401And403(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempts.Add(1)
		if att == 1 {
			// Key 1 is expired / unauthorized (401)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_api_key"}`))
			return
		}
		if att == 2 {
			// Key 2 has insufficient balance (403)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"quota_exhausted"}`))
			return
		}
		// Key 3 succeeds
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"ok-key3"}`))
	}))
	defer server.Close()

	ch := &config.UpstreamChannel{
		ID:      "test-auth-rot",
		Type:    "openai",
		BaseURL: server.URL + "/v1",
		APIKeys: []string{"key-401", "key-403", "key-success"},
	}

	adp := NewOpenAIAdapter()
	var finalBody string

	err := adp.ExecuteWithKeyRotation(context.Background(), ch, func(key string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/v1/test", nil)
		if err != nil {
			return nil, err
		}
		adp.SetHeadersWithKey(req, ch, key)
		return req, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		finalBody = string(b)
		return nil
	})

	if err != nil {
		t.Fatalf("ExecuteWithKeyRotation failed: %v", err)
	}

	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts (401 -> 403 -> 200), got %d", attempts.Load())
	}

	if finalBody != `{"result":"ok-key3"}` {
		t.Errorf("unexpected body: %s", finalBody)
	}
}

func TestHopByHopHeaderFilter(t *testing.T) {
	src := http.Header{}
	src.Set("Connection", "keep-alive")
	src.Set("Transfer-Encoding", "chunked")
	src.Set("Upgrade", "websocket")
	src.Set("Content-Type", "application/json")
	src.Set("X-Custom-Header", "test-val")

	rec := httptest.NewRecorder()
	copyHeader(rec, src)

	res := rec.Header()
	if res.Get("Connection") != "" {
		t.Errorf("expected Connection header to be stripped, got %s", res.Get("Connection"))
	}
	if res.Get("Transfer-Encoding") != "" {
		t.Errorf("expected Transfer-Encoding header to be stripped, got %s", res.Get("Transfer-Encoding"))
	}
	if res.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type to be preserved, got %s", res.Get("Content-Type"))
	}
	if res.Get("X-Custom-Header") != "test-val" {
		t.Errorf("expected X-Custom-Header to be preserved, got %s", res.Get("X-Custom-Header"))
	}
}

func TestStrictV2AdapterRegistration(t *testing.T) {
	expectedPresets := []string{"openai", "anthropic", "newapi", "sub2api"}
	metas := ListMetas()
	if len(metas) != len(expectedPresets) {
		t.Fatalf("expected exactly five adapter types, got %d: %+v", len(metas), metas)
	}
	for _, p := range expectedPresets {
		meta, ok := GetMeta(p)
		if !ok {
			t.Errorf("expected preset %s to be registered in metas", p)
		}
		if meta.DefaultURL == "" {
			t.Errorf("expected preset %s to have default_url", p)
		}
		adp := Get(p)
		if adp == nil {
			t.Errorf("expected preset %s to have an adapter registered", p)
		}
	}
	for _, removed := range []string{"oneapi", "siliconflow", "deepseek", "openrouter", "moonshot", "ollama", "groq", "unknown"} {
		if Get(removed) != nil {
			t.Errorf("removed/unknown type %s must not silently fall back", removed)
		}
	}
}

func TestAnthropicHeaderAutoDetection(t *testing.T) {
	adp := NewOpenAIAdapter()

	// 1. Channel type is anthropic
	ch := &config.UpstreamChannel{
		ID:   "anthropic-node",
		Type: "anthropic",
	}
	req, _ := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", nil)
	adp.SetHeadersWithKey(req, ch, "sk-ant-test")

	if req.Header.Get("x-api-key") != "sk-ant-test" {
		t.Errorf("expected x-api-key to be set, got %s", req.Header.Get("x-api-key"))
	}
	if req.Header.Get("Authorization") != "" {
		t.Errorf("expected Authorization header to NOT be set for official anthropic channel, got %s", req.Header.Get("Authorization"))
	}
	if req.Header.Get("anthropic-version") != "2023-06-01" {
		t.Errorf("expected anthropic-version 2023-06-01, got %s", req.Header.Get("anthropic-version"))
	}

	// 2. Propagation from context for anthropic-beta and custom anthropic-version
	ctx := context.WithValue(context.Background(), CtxAnthropicBeta, "prompt-caching-2024-07-31")
	ctx = context.WithValue(ctx, CtxAnthropicVersion, "2023-01-01")
	req2, _ := http.NewRequestWithContext(ctx, "POST", "https://api.anthropic.com/v1/messages", nil)
	adp.SetHeadersWithKey(req2, ch, "sk-ant-test")

	if req2.Header.Get("anthropic-beta") != "prompt-caching-2024-07-31" {
		t.Errorf("expected anthropic-beta to be propagated, got %s", req2.Header.Get("anthropic-beta"))
	}
	if req2.Header.Get("anthropic-version") != "2023-01-01" {
		t.Errorf("expected anthropic-version 2023-01-01, got %s", req2.Header.Get("anthropic-version"))
	}
}

func TestExecuteWithKeyRotation5xxNoSpurLoop(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"bad gateway upstream dead"}`))
	}))
	defer server.Close()

	ch := &config.UpstreamChannel{
		ID:      "test-502-node",
		Type:    "openai",
		BaseURL: server.URL + "/v1",
		APIKeys: []string{"key-1", "key-2", "key-3"},
	}

	adp := NewOpenAIAdapter()
	err := adp.ExecuteWithKeyRotation(context.Background(), ch, func(key string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/v1/test", nil)
		if err != nil {
			return nil, err
		}
		adp.SetHeadersWithKey(req, ch, key)
		return req, nil
	}, func(resp *http.Response, key string) error {
		return nil
	})

	if err == nil {
		t.Fatalf("expected error on 502, got nil")
	}

	// 5xx 服务器宕机应立即返回，触发调度器渠道级切换，绝不可在同一死节点上空转 3 次轮换 Key
	if attempts.Load() != 1 {
		t.Errorf("expected exactly 1 attempt on 5xx before fast return, but got %d", attempts.Load())
	}
}

func TestForwardHTTPRequestStreamingHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", "13") // 上游附带 Content-Length
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: hello\n\n"))
	}))
	defer server.Close()

	ch := &config.UpstreamChannel{
		ID:      "test-stream-headers",
		Type:    "openai",
		BaseURL: server.URL + "/v1",
		APIKey:  "key-1",
	}

	adp := NewOpenAIAdapter()
	recorder := httptest.NewRecorder()

	err := adp.ForwardHTTPRequest(context.Background(), ch, http.MethodPost, server.URL+"/v1/chat/completions", []byte(`{}`), nil, true, recorder)
	if err != nil {
		t.Fatalf("ForwardHTTPRequest failed: %v", err)
	}

	// 检查流式标头：必须剥离 Content-Length，并设置 X-Accel-Buffering: no 与 Cache-Control: no-cache
	if recorder.Header().Get("Content-Length") != "" {
		t.Errorf("expected Content-Length to be deleted on stream, got %s", recorder.Header().Get("Content-Length"))
	}
	if recorder.Header().Get("X-Accel-Buffering") != "no" {
		t.Errorf("expected X-Accel-Buffering: no, got %s", recorder.Header().Get("X-Accel-Buffering"))
	}
	if !strings.Contains(recorder.Header().Get("Cache-Control"), "no-cache") {
		t.Errorf("expected Cache-Control to contain no-cache, got %s", recorder.Header().Get("Cache-Control"))
	}
}

func TestNewAPIVideoLifecycle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// New-API video generation route must be /v1/videos/generations
		if r.Method == http.MethodPost && r.URL.Path == "/v1/videos/generations" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"id":     "newapi_vid_123",
				"status": "queued",
			})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/newapi_vid_123" {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"id":     "newapi_vid_123",
				"status": "completed",
			})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/newapi_vid_123/content" {
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = w.Write([]byte("mp4-binary"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	ch := &config.UpstreamChannel{
		ID:      "newapi-video-test",
		Type:    "newapi",
		BaseURL: server.URL + "/v1",
		APIKey:  "key-newapi",
	}

	adp := Get("newapi")
	if adp == nil {
		t.Fatalf("expected newapi adapter to be registered")
	}

	createResp, err := adp.CreateVideo(context.Background(), ch, &model.VideoGenerationRequest{
		Model:  "luma-dream-machine",
		Prompt: "a soaring eagle",
	})
	if err != nil {
		t.Fatalf("NewAPI CreateVideo failed: %v", err)
	}
	if createResp.ID != "newapi_vid_123" || createResp.Status != "queued" {
		t.Errorf("unexpected create response: %+v", createResp)
	}

	getResp, err := adp.GetVideo(context.Background(), ch, "newapi_vid_123")
	if err != nil {
		t.Fatalf("NewAPI GetVideo failed: %v", err)
	}
	if getResp.Status != "completed" {
		t.Errorf("expected completed, got %s", getResp.Status)
	}
	if !strings.Contains(getResp.VideoURL, "/v1/videos/newapi_vid_123/content") {
		t.Errorf("expected video_url to contain /v1/videos/newapi_vid_123/content, got %s", getResp.VideoURL)
	}

	rec := httptest.NewRecorder()
	httpReq := httptest.NewRequest(http.MethodGet, "/v1/videos/newapi_vid_123/content", nil)
	err = adp.GetVideoContent(context.Background(), ch, "newapi_vid_123", rec, httpReq)
	if err != nil {
		t.Fatalf("NewAPI GetVideoContent failed: %v", err)
	}
	if rec.Body.String() != "mp4-binary" {
		t.Errorf("expected 'mp4-binary', got '%s'", rec.Body.String())
	}
}

func TestNewAPIVideoContentRelaysSignedRedirect(t *testing.T) {
	var gotRange, gotAuthorization string
	var signedEndpointRequested atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/videos/generations/provider-video-id/content":
			gotRange = r.Header.Get("Range")
			gotAuthorization = r.Header.Get("Authorization")
			w.Header().Set("Location", "https://cdn.example/signed/video.mp4?sig=fresh")
			w.WriteHeader(http.StatusFound)
		case "/signed/video.mp4":
			signedEndpointRequested.Store(true)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	channel := &config.UpstreamChannel{
		ID:      "newapi-video-redirect",
		Type:    "newapi",
		BaseURL: server.URL + "/v1",
		APIKey:  "upstream-secret",
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/videos/provider-video-id/content", nil)
	request.Header.Set("Range", "bytes=128-")

	err := NewNewAPIAdapter().GetVideoContent(context.Background(), channel, "provider-video-id", recorder, request)
	if err != nil {
		t.Fatalf("GetVideoContent returned error: %v", err)
	}
	if recorder.Code != http.StatusFound {
		t.Fatalf("content redirect status = %d, want %d", recorder.Code, http.StatusFound)
	}
	if got, want := recorder.Header().Get("Location"), "https://cdn.example/signed/video.mp4?sig=fresh"; got != want {
		t.Fatalf("redirect Location = %q, want %q", got, want)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("redirect Cache-Control = %q, want no-store", got)
	}
	if gotRange != "bytes=128-" {
		t.Fatalf("upstream Range = %q, want bytes=128-", gotRange)
	}
	if gotAuthorization != "Bearer upstream-secret" {
		t.Fatalf("upstream Authorization = %q", gotAuthorization)
	}
	if signedEndpointRequested.Load() {
		t.Fatal("gateway followed the signed redirect instead of relaying it to the browser")
	}
}

func TestNewAPIVideoContentPreservesRangeResponse(t *testing.T) {
	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/videos/generations/provider-video-id/content" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", "bytes 4-7/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("4567"))
	}))
	defer server.Close()

	channel := &config.UpstreamChannel{ID: "newapi-video-range", Type: "newapi", BaseURL: server.URL + "/v1", APIKey: "upstream-secret"}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/videos/provider-video-id/content", nil)
	request.Header.Set("Range", "bytes=4-7")

	err := NewNewAPIAdapter().GetVideoContent(context.Background(), channel, "provider-video-id", recorder, request)
	if err != nil {
		t.Fatalf("GetVideoContent returned error: %v", err)
	}
	if recorder.Code != http.StatusPartialContent || recorder.Body.String() != "4567" {
		t.Fatalf("range response = %d %q, want %d %q", recorder.Code, recorder.Body.String(), http.StatusPartialContent, "4567")
	}
	if gotRange != "bytes=4-7" {
		t.Fatalf("upstream Range = %q, want bytes=4-7", gotRange)
	}
	if recorder.Header().Get("Content-Range") != "bytes 4-7/10" || recorder.Header().Get("Accept-Ranges") != "bytes" {
		t.Fatalf("range headers not preserved: %+v", recorder.Header())
	}
}

type cancelOnReadBody struct {
	cancel context.CancelFunc
	body   []byte
	done   bool
}

func (b *cancelOnReadBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	b.done = true
	b.cancel()
	return copy(p, b.body), io.EOF
}

func (b *cancelOnReadBody) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestExecuteWithKeyRotationStopsWhenContextIsCancelledDuringRejectedResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	adp := NewOpenAIAdapter()
	adp.Client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return &http.Response{
			StatusCode: http.StatusUnauthorized,
			Header:     make(http.Header),
			Body:       &cancelOnReadBody{cancel: cancel, body: []byte(`{"error":"expired"}`)},
			Request:    req,
		}, nil
	})}
	channel := &config.UpstreamChannel{ID: "cancel-rotation", APIKeys: []string{"first", "second"}}
	err := adp.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, "https://example.test/video", nil)
	}, func(*http.Response, string) error {
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("rotation error = %v, want context.Canceled", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("upstream attempts = %d, want 1 after cancellation", attempts.Load())
	}
}

func TestVideoContentHEADFallbackIsLimitedToUnsupportedStatuses(t *testing.T) {
	for _, fallbackStatus := range []int{http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented} {
		t.Run(http.StatusText(fallbackStatus), func(t *testing.T) {
			var methods []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				methods = append(methods, r.Method)
				if r.Method == http.MethodHead {
					w.WriteHeader(fallbackStatus)
					return
				}
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("video"))
			}))
			defer server.Close()

			adp := NewOpenAIAdapter()
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodHead, "/content", nil)
			err := adp.ForwardVideoContent(context.Background(), &config.UpstreamChannel{ID: "head-fallback", APIKey: "key"}, server.URL, request, recorder)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(methods, ",") != "HEAD,GET" {
				t.Fatalf("methods = %v, want HEAD then GET", methods)
			}
			if recorder.Code != http.StatusPartialContent || recorder.Body.String() != "video" {
				t.Fatalf("fallback response = %d %q", recorder.Code, recorder.Body.String())
			}
		})
	}

	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusBadGateway,
	} {
		t.Run("no fallback "+http.StatusText(status), func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.WriteHeader(status)
			}))
			defer server.Close()
			err := NewOpenAIAdapter().ForwardVideoContent(
				context.Background(),
				&config.UpstreamChannel{ID: "head-no-fallback", APIKey: "key"},
				server.URL,
				httptest.NewRequest(http.MethodHead, "/content", nil),
				httptest.NewRecorder(),
			)
			var upstreamErr *UpstreamHTTPError
			if !errors.As(err, &upstreamErr) || upstreamErr.StatusCode != status {
				t.Fatalf("HEAD %d error = %v", status, err)
			}
			if attempts.Load() != 1 {
				t.Fatalf("HEAD %d attempts = %d, want no GET fallback", status, attempts.Load())
			}
		})
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := NewOpenAIAdapter().ForwardVideoContent(
		cancelledCtx,
		&config.UpstreamChannel{ID: "head-cancelled", APIKey: "key"},
		"https://example.test/content",
		httptest.NewRequest(http.MethodHead, "/content", nil),
		httptest.NewRecorder(),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled HEAD = err %v, want cancellation before upstream", err)
	}
}

type cancellingResponseWriter struct {
	header http.Header
	status int
}

func (w *cancellingResponseWriter) Header() http.Header { return w.header }
func (w *cancellingResponseWriter) WriteHeader(status int) {
	w.status = status
}
func (w *cancellingResponseWriter) Write([]byte) (int, error) { return 0, context.Canceled }

func TestVideoContentCancellationAfterPartialContentHeadersIsPreserved(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-4/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("video"))
	}))
	defer server.Close()

	w := &cancellingResponseWriter{header: make(http.Header)}
	err := NewOpenAIAdapter().ForwardVideoContent(
		context.Background(),
		&config.UpstreamChannel{ID: "cancelled-range", APIKey: "key"},
		server.URL,
		httptest.NewRequest(http.MethodGet, "/content", nil),
		w,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("stream error = %v, want context.Canceled", err)
	}
	if w.status != http.StatusPartialContent || w.header.Get("Content-Range") != "bytes 0-4/10" {
		t.Fatalf("partial response metadata was lost: status=%d headers=%v", w.status, w.header)
	}
}

func TestReadUpstreamErrorBodyLimit(t *testing.T) {
	// Exactly 2 MiB
	exact2MiB := bytes.Repeat([]byte("a"), int(maxUpstreamErrorBodyBytes))
	data, err := readUpstreamErrorBody(bytes.NewReader(exact2MiB))
	if err != nil {
		t.Fatalf("readUpstreamErrorBody exact 2 MiB failed: %v", err)
	}
	if len(data) != int(maxUpstreamErrorBodyBytes) {
		t.Fatalf("expected len %d, got %d", maxUpstreamErrorBodyBytes, len(data))
	}

	// 2 MiB + 1 byte
	oversized := bytes.Repeat([]byte("a"), int(maxUpstreamErrorBodyBytes)+1)
	_, err = readUpstreamErrorBody(bytes.NewReader(oversized))
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected ErrResponseTooLarge, got %v", err)
	}
}

func TestLegacyAndNewAPIErrorBodyLimit400And404(t *testing.T) {
	// Test HTTP 400 with exact 2 MiB
	exact2MiB := strings.Repeat("x", int(maxUpstreamErrorBodyBytes))
	server400 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Retry-After", "45")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, exact2MiB)
	}))
	defer server400.Close()

	ch400 := &config.UpstreamChannel{ID: "ch-400", BaseURL: server400.URL + "/v1", APIKey: "test-key"}
	openaiAdp := NewOpenAIAdapter()
	_, err := openaiAdp.CreateVideo(context.Background(), ch400, &model.VideoGenerationRequest{Prompt: "test"})
	var upErr *UpstreamHTTPError
	if !errors.As(err, &upErr) {
		t.Fatalf("expected UpstreamHTTPError for HTTP 400, got %v", err)
	}
	if upErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", upErr.StatusCode)
	}
	if len(upErr.Body) != int(maxUpstreamErrorBodyBytes) {
		t.Fatalf("expected 2 MiB body, got length %d", len(upErr.Body))
	}
	if upErr.RetryAfter != "45" {
		t.Fatalf("expected retry after 45, got %s", upErr.RetryAfter)
	}

	err = openaiAdp.ForwardHTTPRequest(context.Background(), ch400, http.MethodPost, server400.URL+"/v1/moderations", []byte(`{}`), nil, false, httptest.NewRecorder())
	if !errors.As(err, &upErr) {
		t.Fatalf("ForwardHTTPRequest expected UpstreamHTTPError for HTTP 400, got %v", err)
	}
	if len(upErr.Body) != int(maxUpstreamErrorBodyBytes) || upErr.ContentType != "text/plain; charset=utf-8" {
		t.Fatalf("ForwardHTTPRequest lost upstream error metadata: body=%d content-type=%q", len(upErr.Body), upErr.ContentType)
	}

	newapiAdp := NewNewAPIAdapter()
	_, err = newapiAdp.CreateVideo(context.Background(), ch400, &model.VideoGenerationRequest{Prompt: "test"})
	if !errors.As(err, &upErr) || upErr.StatusCode != http.StatusBadRequest || len(upErr.Body) != int(maxUpstreamErrorBodyBytes) {
		t.Fatalf("newapi CreateVideo 400 error = %v", err)
	}

	// Test HTTP 404 with oversized (> 2 MiB) body -> returns ErrResponseTooLarge
	oversized := strings.Repeat("y", int(maxUpstreamErrorBodyBytes)+1)
	server404 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, oversized)
	}))
	defer server404.Close()

	ch404 := &config.UpstreamChannel{ID: "ch-404", BaseURL: server404.URL + "/v1", APIKey: "test-key"}
	_, err = openaiAdp.CreateVideo(context.Background(), ch404, &model.VideoGenerationRequest{Prompt: "test"})
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected ErrResponseTooLarge for >2MiB 404 response, got %v", err)
	}

	_, err = newapiAdp.CreateVideo(context.Background(), ch404, &model.VideoGenerationRequest{Prompt: "test"})
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("newapi expected ErrResponseTooLarge for >2MiB 404 response, got %v", err)
	}
}
