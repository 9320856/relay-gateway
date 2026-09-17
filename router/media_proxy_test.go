package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMediaProxySSRFProtection(t *testing.T) {
	blockedURLs := []string{
		"http://localhost:8000/secret",
		"http://127.0.0.1:8000/api",
		"http://169.254.169.254/latest/meta-data/",
		"http://metadata.google.internal/computeMetadata/v1/",
		"http://10.0.0.1/admin",
		"http://192.168.1.1/router",
		"http://172.16.0.1/internal",
		"http://100.64.0.1/test.png",
		"http://100.127.255.254/test.png",
		"http://192.0.2.1/test.png",
		"http://198.51.100.1/test.png",
		"http://203.0.113.1/test.png",
		"http://198.18.0.1/test.png",
		"ftp://example.com/test.jpg",
		"file:///etc/passwd",
		"http://example.internal/test.png",
		"http://test.localhost/test.png",
	}

	for _, u := range blockedURLs {
		_, err := isSafeMediaURL(u)
		if err == nil {
			t.Errorf("expected URL %q to be blocked by isSafeMediaURL, but got nil error", u)
		}
	}

	resolver := func(host string) ([]string, error) {
		if host == "media.example.org" {
			return []string{"8.8.8.8", "2001:4860:4860::8888"}, nil
		}
		return nil, fmt.Errorf("unexpected lookup for %s", host)
	}
	parsed, err := isSafeMediaURLWithResolver("https://media.example.org/photo.webp", resolver)
	if err != nil {
		t.Fatalf("expected public hostname to be allowed, got error: %v", err)
	}
	if parsed == nil || parsed.Host == "" {
		t.Fatal("parsed public URL is invalid")
	}

	_, err = isSafeMediaURLWithResolver("https://media.example.org/private.png", func(string) ([]string, error) {
		return []string{"8.8.8.8", "10.0.0.8"}, nil
	})
	if err == nil {
		t.Fatal("hostname with a private resolved address was allowed")
	}

	for _, u := range []string{"https://8.8.8.8/image.jpg", "https://[2001:4860:4860::8888]/image.jpg"} {
		parsed, err := isSafeMediaURLWithResolver(u, resolver)
		if err != nil {
			t.Errorf("expected URL %q to be allowed, got error: %v", u, err)
		}
		if parsed == nil || parsed.Host == "" {
			t.Errorf("parsed URL for %q is invalid", u)
		}
	}
}

func TestMediaProxyHandler(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()

	// 1. Unauthenticated request -> 401
	req, _ := http.NewRequest(http.MethodGet, "/api/media-proxy?url=https://example.com/image.jpg", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated request, got %d", w.Code)
	}

	// 2. Setup admin to get session cookie & gateway key
	setup := e2eRequest(t, engine, http.MethodPost, "/api/auth/setup", `{"username":"adminmedia","password":"correct horse battery"}`, nil, "", "")
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
	adminCookie := setup.Result().Cookies()[0]

	// 3. Test missing url param with admin cookie -> 400
	req, _ = http.NewRequest(http.MethodGet, "/api/media-proxy", nil)
	req.AddCookie(adminCookie)
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "缺少 url 参数") {
		t.Fatalf("expected 400 for missing url param, got %d: %s", w.Code, w.Body.String())
	}

	// 4. Test SSRF loopback block with admin cookie -> 400
	req, _ = http.NewRequest(http.MethodGet, "/api/media-proxy?url=http://127.0.0.1:8000/test.png", nil)
	req.AddCookie(adminCookie)
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "媒体地址无效或不允许访问") {
		t.Fatalf("expected SSRF block 400 for 127.0.0.1 upstream, got code %d body %s", w.Code, w.Body.String())
	}

	// 5. Query-string credentials are rejected; use the header form instead.
	req, _ = http.NewRequest(http.MethodGet, "/api/media-proxy?token="+setupPayload.Key+"&url=http://127.0.0.1:8000/test.png", nil)
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected query token to be rejected, got code %d body %s", w.Code, w.Body.String())
	}
	req, _ = http.NewRequest(http.MethodGet, "/api/media-proxy?url=http://127.0.0.1:8000/test.png", nil)
	req.Header.Set("x-api-key", setupPayload.Key)
	w = httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "媒体地址无效或不允许访问") {
		t.Fatalf("expected SSRF block 400 with gateway header, got code %d body %s", w.Code, w.Body.String())
	}
}

func TestMediaProxyErrorsAreChineseAndDoNotExposeBlockedURLDetails(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()
	setup := e2eRequest(t, engine, http.MethodPost, "/api/auth/setup", `{"username":"adminproxy","password":"correct horse battery"}`, nil, "", "")
	if setup.Code != http.StatusCreated {
		t.Fatalf("setup returned %d: %s", setup.Code, setup.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/api/media-proxy?url=http://127.0.0.1:8765/private", nil)
	req.AddCookie(setup.Result().Cookies()[0])
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, req)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "媒体地址无效或不允许访问") || strings.Contains(response.Body.String(), "127.0.0.1") {
		t.Fatalf("unsafe proxy response = %d %s", response.Code, response.Body.String())
	}
}

func TestMediaProxyRedirectsInternalGatewayMedia(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()
	setup := e2eRequest(t, engine, http.MethodPost, "/api/auth/setup", `{"username":"adminmediaredirect","password":"correct horse battery"}`, nil, "", "")
	if setup.Code != http.StatusCreated {
		t.Fatalf("setup returned %d: %s", setup.Code, setup.Body.String())
	}
	cookie := setup.Result().Cookies()[0]

	// 1. Loopback URL to /v1/media/... redirects directly with 307
	req := httptest.NewRequest(http.MethodGet, "/api/media-proxy?url=http://localhost:8000/v1/media/asset-123/cap-456", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 redirect for localhost gateway media, got %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/media/asset-123/cap-456" {
		t.Fatalf("expected Location /v1/media/asset-123/cap-456, got %q", loc)
	}

	// 2. Relative URL to /v1/media/... redirects directly with 307
	req = httptest.NewRequest(http.MethodGet, "/api/media-proxy?url=/v1/media/asset-789/cap-000", nil)
	req.AddCookie(cookie)
	rec = httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 redirect for relative gateway media, got %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/media/asset-789/cap-000" {
		t.Fatalf("expected Location /v1/media/asset-789/cap-000, got %q", loc)
	}

	// 3. /api/media-assets/.../content redirects directly with 307
	req = httptest.NewRequest(http.MethodGet, "/api/media-proxy?url=http://127.0.0.1:8000/api/media-assets/42/content", nil)
	req.AddCookie(cookie)
	rec = httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected 307 redirect for media-assets content, got %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/api/media-assets/42/content" {
		t.Fatalf("expected Location /api/media-assets/42/content, got %q", loc)
	}

	// 4. Directory traversal attempt must NOT redirect; must be blocked by SSRF check
	req = httptest.NewRequest(http.MethodGet, "/api/media-proxy?url=http://localhost:8000/v1/media/../admin", nil)
	req.AddCookie(cookie)
	rec = httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "媒体地址无效或不允许访问") {
		t.Fatalf("expected SSRF block for path traversal attempt, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAllowedMediaContentType(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{name: "image", in: "image/png", want: "image/png", ok: true},
		{name: "video with parameters", in: "video/mp4; charset=binary", want: "video/mp4", ok: true},
		{name: "audio", in: "audio/mpeg", want: "audio/mpeg", ok: true},
		{name: "unsupported image", in: "image/tiff", ok: false},
		{name: "html", in: "text/html; charset=utf-8", ok: false},
		{name: "javascript", in: "application/javascript", ok: false},
		{name: "malformed", in: "image/png; bad", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := isAllowedMediaContentType(tt.in)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("isAllowedMediaContentType(%q) = (%q, %v), want (%q, %v)", tt.in, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestMediaProxyDisablesEnvironmentAndSystemProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9999")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:9999")
	t.Setenv("ALL_PROXY", "socks5://127.0.0.1:9999")

	client := getMediaProxyClient()
	if client == nil {
		t.Fatal("getMediaProxyClient returned nil")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("expected transport.Proxy to be nil to prevent proxy DNS re-resolution and internal bypass")
	}
}

func TestDialSafeMediaTargetRejectsPrivateAddresses(t *testing.T) {
	ctx := context.Background()
	privateAddresses := []string{
		"127.0.0.1:80",
		"10.0.0.1:80",
		"192.168.1.1:80",
		"172.16.0.1:80",
		"169.254.169.254:80",
		"100.64.0.1:80",
		"100.127.255.254:80",
		"192.0.2.1:80",
	}

	for _, addr := range privateAddresses {
		conn, err := dialSafeMediaTarget(ctx, "tcp", addr)
		if err == nil {
			if conn != nil {
				_ = conn.Close()
			}
			t.Errorf("expected dialSafeMediaTarget to reject %q, but got nil error", addr)
		}
	}
}
