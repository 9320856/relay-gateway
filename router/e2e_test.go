package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"relay-gateway/audit"
	"relay-gateway/db"
	relaymedia "relay-gateway/media"
	"relay-gateway/security"
	"relay-gateway/service"
	"relay-gateway/task"
)

type e2eUpstream struct {
	t        *testing.T
	kind     string
	apiKey   string
	server   *httptest.Server
	mu       sync.Mutex
	paths    []string
	idemKeys []string
}

func newE2EUpstream(t *testing.T, kind string) *e2eUpstream {
	t.Helper()
	u := &e2eUpstream{t: t, kind: kind, apiKey: "upstream-" + kind}
	u.server = httptest.NewServer(http.HandlerFunc(u.serveHTTP))
	t.Cleanup(u.server.Close)
	return u
}

func (u *e2eUpstream) serveHTTP(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	u.paths = append(u.paths, r.Method+" "+r.URL.Path)
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		u.idemKeys = append(u.idemKeys, key)
	}
	u.mu.Unlock()
	if u.kind == "newapi" && r.Method == http.MethodGet && r.URL.Path == "/assets/video.mp4" {
		// Required media retention fetches the returned asset URL without the
		// upstream API credential; keep this fixture endpoint public.
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write([]byte("mock-video"))
		return
	}

	if u.kind == "anthropic" {
		if got := r.Header.Get("x-api-key"); got != u.apiKey {
			http.Error(w, "missing anthropic key", http.StatusUnauthorized)
			return
		}
		if got := r.Header.Get("anthropic-version"); got != "2023-06-01" {
			http.Error(w, "missing anthropic version", http.StatusBadRequest)
			return
		}
	} else if got := r.Header.Get("Authorization"); got != "Bearer "+u.apiKey {
		http.Error(w, "missing bearer key", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	switch {
	case u.kind == "openai" && r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		_, _ = w.Write([]byte(`{"id":"chatcmpl-e2e","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"openai ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2}}`))
	case u.kind == "sub2api" && r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		_, _ = w.Write([]byte(`{"id":"chatcmpl-sub2","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"sub2 ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}`))
	case u.kind == "anthropic" && r.Method == http.MethodPost && r.URL.Path == "/v1/messages":
		_, _ = w.Write([]byte(`{"id":"msg-e2e","type":"message","role":"assistant","model":"e2e-anthropic","content":[{"type":"text","text":"anthropic ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":4}}`))
	case u.kind == "newapi" && r.Method == http.MethodPost && r.URL.Path == "/v1/videos/generations":
		_, _ = w.Write([]byte(`{"id":"video-e2e-public","status":"queued"}`))
	case u.kind == "newapi" && r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/video-e2e-public":
		_, _ = w.Write([]byte(fmt.Sprintf(`{"id":"video-e2e-public","task_id":"video-e2e-provider","status":"completed","video_url":%q}`, u.server.URL+"/assets/video.mp4")))
	case u.kind == "newapi" && r.Method == http.MethodGet && r.URL.Path == "/v1/videos/generations/video-e2e-provider/content":
		w.Header().Set("Location", "https://cdn.example/video.mp4?signature=fresh")
		w.WriteHeader(http.StatusFound)
	default:
		http.Error(w, fmt.Sprintf("unexpected %s upstream request: %s %s", u.kind, r.Method, r.URL.Path), http.StatusNotFound)
	}
}

func (u *e2eUpstream) assertCalled(t *testing.T, want string, requireIdempotency bool) {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, got := range u.paths {
		if got == want {
			if requireIdempotency && len(u.idemKeys) == 0 {
				t.Fatalf("%s request did not carry an Idempotency-Key", want)
			}
			return
		}
	}
	t.Fatalf("upstream %s did not receive %s; calls=%v", u.kind, want, u.paths)
}

func e2eRequest(t *testing.T, handler http.Handler, method, target, body string, cookie *http.Cookie, csrf, gatewayKey string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	req.RemoteAddr = "127.0.0.1:45678"
	if cookie != nil {
		req.AddCookie(cookie)
	}
	if csrf != "" {
		req.Header.Set("X-CSRF-Token", csrf)
	}
	if gatewayKey != "" {
		req.Header.Set("Authorization", "Bearer "+gatewayKey)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestLocalMockEndToEndLifecycle(t *testing.T) {
	allowProfileMediaTestURLs(t)
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "e2e-encryption-key-with-sufficient-entropy")
	initAuthTestDB(t)
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	bootstrapProfilesForRouterTest(t)
	engine := Setup()

	setup := e2eRequest(t, engine, http.MethodPost, "/api/auth/setup", `{"username":"administrator","password":"correct horse battery"}`, nil, "", "")
	if setup.Code != http.StatusCreated {
		t.Fatalf("setup returned %d: %s", setup.Code, setup.Body.String())
	}
	var setupPayload struct {
		CSRF string `json:"csrf_token"`
		Key  string `json:"gateway_api_key"`
	}
	if err := json.Unmarshal(setup.Body.Bytes(), &setupPayload); err != nil {
		t.Fatal(err)
	}
	if setupPayload.CSRF == "" || setupPayload.Key == "" || len(setup.Result().Cookies()) != 1 {
		t.Fatalf("setup did not return the one-time credentials and session: %s", setup.Body.String())
	}
	adminCookie := setup.Result().Cookies()[0]

	upstreams := map[string]*e2eUpstream{}
	models := map[string]string{
		"openai":    "e2e-openai",
		"anthropic": "e2e-anthropic",
		"newapi":    "e2e-newapi-video",
		"sub2api":   "e2e-sub2api",
	}
	for _, kind := range []string{"openai", "anthropic", "newapi", "sub2api"} {
		upstream := newE2EUpstream(t, kind)
		upstreams[kind] = upstream
		payload, err := json.Marshal(map[string]any{
			"id":           "e2e-" + kind,
			"name":         "E2E " + kind,
			"type":         kind,
			"base_url":     upstream.server.URL + "/v1",
			"enabled":      true,
			"priority":     1,
			"weight":       1,
			"fetch_models": false,
			"models_raw":   models[kind],
			"headers_raw":  "",
			"api_key":      upstream.apiKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		saved := e2eRequest(t, engine, http.MethodPost, "/api/channels", string(payload), adminCookie, setupPayload.CSRF, "")
		if saved.Code != http.StatusOK {
			t.Fatalf("save %s channel returned %d: %s", kind, saved.Code, saved.Body.String())
		}
		if strings.Contains(saved.Body.String(), upstream.apiKey) {
			t.Fatalf("save %s response exposed the upstream key: %s", kind, saved.Body.String())
		}
		service.DefaultDispatcher.ResetBreaker("e2e-" + kind)
	}

	apiCalls := []struct {
		method string
		path   string
		body   string
		want   string
		status int
	}{
		{http.MethodPost, "/v1/chat/completions", `{"model":"e2e-openai","messages":[{"role":"user","content":"hello"}]}`, "openai ok", http.StatusOK},
		{http.MethodPost, "/v1/chat/completions", `{"model":"e2e-sub2api","messages":[{"role":"user","content":"hello"}]}`, "sub2 ok", http.StatusOK},
		{http.MethodPost, "/v1/messages", `{"model":"e2e-anthropic","max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`, "anthropic ok", http.StatusOK},
		{http.MethodPost, "/v1/videos", `{"model":"e2e-newapi-video","prompt":"a local mock"}`, "video-e2e-public", http.StatusOK},
	}
	var videoStatusResponse *httptest.ResponseRecorder
	for _, call := range apiCalls {
		resp := e2eRequest(t, engine, call.method, call.path, call.body, nil, "", setupPayload.Key)
		if resp.Code != call.status || !strings.Contains(resp.Body.String(), call.want) {
			t.Fatalf("%s %s returned %d %s", call.method, call.path, resp.Code, resp.Body.String())
		}
		if call.path == "/v1/videos/video-e2e-public" {
			videoStatusResponse = resp
		}
	}
	firstVideoStatus := e2eRequest(t, engine, http.MethodGet, "/v1/videos/video-e2e-public", "", nil, "", setupPayload.Key)
	if firstVideoStatus.Code != http.StatusOK || !strings.Contains(firstVideoStatus.Body.String(), `"status":"materializing"`) || strings.Contains(firstVideoStatus.Body.String(), "video.mp4") {
		t.Fatalf("video status should hide the provider URL while materializing: %d %s", firstVideoStatus.Code, firstVideoStatus.Body.String())
	}
	store, err := getMediaObjectStore()
	if err != nil {
		t.Fatalf("load e2e media store: %v", err)
	}
	materializer := task.NewMediaMaterializationPoller("e2e-media", store)
	materializer.Fetcher = relaymedia.HTTPSourceFetcher{URLValidator: func(string) error { return nil }}
	claimed, err := materializer.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("materialize e2e video asset claimed=%v err=%v", claimed, err)
	}
	if assets, assetErr := db.ListMediaAssetsForTaskRun(profileTaskRunID("e2e-newapi", "video-e2e-public", "video.create"), asyncTaskKindVideo); assetErr != nil {
		t.Fatalf("inspect e2e video asset: %v", assetErr)
	} else if len(assets) == 0 || assets[0].Status != db.MediaAssetAvailable {
		t.Fatalf("e2e video asset not available after materialization: %+v", assets)
	}
	videoStatusResponse = e2eRequest(t, engine, http.MethodGet, "/v1/videos/video-e2e-public", "", nil, "", setupPayload.Key)
	if videoStatusResponse.Code != http.StatusOK || strings.Contains(videoStatusResponse.Body.String(), "cdn.example") || !strings.Contains(videoStatusResponse.Body.String(), "/v1/media/") || strings.Contains(videoStatusResponse.Body.String(), "video.mp4") {
		t.Fatalf("video status did not expose the stable managed URL: %d %s", videoStatusResponse.Code, videoStatusResponse.Body.String())
	}
	denied := e2eRequest(t, engine, http.MethodGet, "/v1/models", "", nil, "", "")
	if denied.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated API request returned %d: %s", denied.Code, denied.Body.String())
	}
	var videoPayload struct {
		VideoURL string `json:"video_url"`
	}
	if err := json.Unmarshal(videoStatusResponse.Body.Bytes(), &videoPayload); err != nil {
		t.Fatalf("decode managed video status: %v", err)
	}
	managedVideoURL, err := url.Parse(videoPayload.VideoURL)
	if err != nil || managedVideoURL.Path == "" || !strings.HasPrefix(managedVideoURL.Path, "/v1/media/") {
		t.Fatalf("invalid managed video URL: %q", videoPayload.VideoURL)
	}
	videoContent := e2eRequest(t, engine, http.MethodGet, managedVideoURL.Path, "", nil, "", "")
	if videoContent.Code != http.StatusOK || videoContent.Body.String() != "mock-video" {
		t.Fatalf("managed video content returned %d body=%q", videoContent.Code, videoContent.Body.String())
	}

	upstreams["openai"].assertCalled(t, "POST /v1/chat/completions", false)
	upstreams["sub2api"].assertCalled(t, "POST /v1/chat/completions", false)
	upstreams["anthropic"].assertCalled(t, "POST /v1/messages", false)
	upstreams["newapi"].assertCalled(t, "POST /v1/videos/generations", true)
	upstreams["newapi"].assertCalled(t, "GET /v1/videos/generations/video-e2e-public", false)
	if run, runErr := db.GetTaskRunByAlias("video-e2e-public"); runErr != nil || run.ChannelID != "e2e-newapi" || run.ProviderTaskID != "video-e2e-public" {
		t.Fatalf("video TaskRun was not pinned to its creation channel: run=%+v err=%v", run, runErr)
	}

	logs := e2eRequest(t, engine, http.MethodGet, "/api/logs?kind=api_call&page_size=100", "", adminCookie, "", "")
	if logs.Code != http.StatusOK {
		t.Fatalf("list logs returned %d: %s", logs.Code, logs.Body.String())
	}
	var listPayload struct {
		Data  []db.RequestLogModel `json:"data"`
		Total int                  `json:"total"`
	}
	if err := json.Unmarshal(logs.Body.Bytes(), &listPayload); err != nil {
		t.Fatal(err)
	}
	// Video status reads are coalesced into their creation row.
	statusPollCount := 2
	// Includes the terminal video read and denied models request. Static media
	// downloads (/v1/media/*) are excluded from model call audit rows.
	wantLogCount := len(apiCalls) + 2 + 1 - statusPollCount
	if listPayload.Total != wantLogCount {
		t.Fatalf("expected %d API audit rows after poll coalescing, got %d", wantLogCount, listPayload.Total)
	}
	seenPaths := make(map[string]bool)
	logsByPath := make(map[string]db.RequestLogModel)
	for _, row := range listPayload.Data {
		if strings.HasPrefix(row.Path, "/v1/media/") {
			t.Fatalf("media download must not be audited as an API call: %+v", row)
		}
		if row.Path == "/v1/models" {
			if row.Outcome != "error" || row.StatusCode != http.StatusUnauthorized {
				t.Fatalf("authentication failure was not audited correctly: %+v", row)
			}
		} else if row.Outcome != "success" || (row.StatusCode != http.StatusOK && row.StatusCode != http.StatusAccepted) {
			t.Fatalf("unexpected API audit outcome: %+v", row)
		}
		seenPaths[row.Path] = true
		if previous, exists := logsByPath[row.Path]; !exists || previous.AsyncTaskID == "" {
			logsByPath[row.Path] = row
		}
	}
	for _, path := range []string{"/v1/models", "/v1/chat/completions", "/v1/messages", "/v1/videos"} {
		if !seenPaths[path] {
			t.Fatalf("audit list is missing %s: %+v", path, seenPaths)
		}
	}

	videoLog := logsByPath["/v1/videos"]
	if videoLog.ID == "" {
		t.Fatal("video create audit row not found")
	}
	if videoLog.AsyncTaskKind != "video" || videoLog.AsyncTaskID != "video-e2e-public" || videoLog.AsyncTaskStatus != "completed" || videoLog.AsyncPollCount != 2 || videoLog.AsyncLastPolledAt == nil || videoLog.AsyncCompletedAt == nil {
		t.Fatalf("video async task summary is incomplete: %+v", videoLog)
	}

	detailResp := e2eRequest(t, engine, http.MethodGet, "/api/logs/"+videoLog.ID, "", adminCookie, "", "")
	if detailResp.Code != http.StatusOK {
		t.Fatalf("log detail returned %d: %s", detailResp.Code, detailResp.Body.String())
	}
	var detail audit.Detail
	if err := json.Unmarshal(detailResp.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.Log.ChannelID != "e2e-newapi" || !strings.Contains(detail.Log.RequestBody, "e2e-newapi-video") || !strings.Contains(detail.Log.ResponseBody, "video-e2e") || !strings.Contains(detail.Log.AsyncResultBody, "/v1/media/") {
		t.Fatalf("video log detail is incomplete: %+v", detail.Log)
	}
	if len(detail.MediaAssets) != 1 || detail.MediaAssets[0].PublicURL != videoPayload.VideoURL || detail.MediaAssets[0].PublicURLError != "" {
		t.Fatalf("log media link did not match the API response: assets=%+v response=%q", detail.MediaAssets, videoPayload.VideoURL)
	}
	mediaListResp := e2eRequest(t, engine, http.MethodGet, "/api/media-assets?kind=video&status=available&limit=100", "", adminCookie, "", "")
	if mediaListResp.Code != http.StatusOK {
		t.Fatalf("media list returned %d: %s", mediaListResp.Code, mediaListResp.Body.String())
	}
	var mediaList struct {
		Data []struct {
			ID             uint   `json:"id"`
			PublicURL      string `json:"public_url"`
			PublicURLError string `json:"public_url_error"`
		} `json:"data"`
	}
	if err := json.Unmarshal(mediaListResp.Body.Bytes(), &mediaList); err != nil {
		t.Fatal(err)
	}
	foundPublicMediaURL := false
	for _, asset := range mediaList.Data {
		if asset.PublicURL == videoPayload.VideoURL && asset.PublicURLError == "" {
			foundPublicMediaURL = true
			break
		}
	}
	if !foundPublicMediaURL {
		t.Fatalf("media management link did not match the API response: assets=%+v response=%q", mediaList.Data, videoPayload.VideoURL)
	}
	phases := make(map[string]bool)
	for _, event := range detail.Events {
		phases[event.Phase] = true
	}
	for _, phase := range []string{"request_received", "auth_succeeded", "candidate_channels", "attempt_started", "response_completed", "async_task_created", "async_task_status_changed"} {
		if !phases[phase] {
			t.Fatalf("video audit timeline is missing %s: %+v", phase, phases)
		}
	}

	// A public content URL must stay pinned to the task's creation channel even
	// if an untrusted caller appends a different channel_id query parameter.
	videoContentWithOverride := e2eRequest(t, engine, http.MethodGet, "/v1/videos/video-e2e-public/content?channel_id=e2e-openai", "", nil, "", setupPayload.Key)
	if videoContentWithOverride.Code != http.StatusOK || videoContentWithOverride.Body.String() != "mock-video" {
		t.Fatalf("channel override changed public video routing: %d, headers=%+v", videoContentWithOverride.Code, videoContentWithOverride.Header())
	}

	credentials := e2eRequest(t, engine, http.MethodPut, "/api/account/credentials", `{"current_password":"correct horse battery","new_username":"administrator2","new_password":"new correct horse battery"}`, adminCookie, setupPayload.CSRF, "")
	if credentials.Code != http.StatusOK || len(credentials.Result().Cookies()) != 1 {
		t.Fatalf("credential update returned %d: %s", credentials.Code, credentials.Body.String())
	}
	newCookie := credentials.Result().Cookies()[0]
	if newCookie.Name != security.SessionCookieName || newCookie.Value == adminCookie.Value {
		t.Fatalf("credential update did not issue a replacement session: %+v", newCookie)
	}
	oldSession := e2eRequest(t, engine, http.MethodGet, "/api/channels", "", adminCookie, "", "")
	if oldSession.Code != http.StatusUnauthorized {
		t.Fatalf("old session remained valid after credential update: %d", oldSession.Code)
	}
	newSession := e2eRequest(t, engine, http.MethodGet, "/api/channels", "", newCookie, "", "")
	if newSession.Code != http.StatusOK {
		t.Fatalf("replacement session is invalid: %d %s", newSession.Code, newSession.Body.String())
	}
}
