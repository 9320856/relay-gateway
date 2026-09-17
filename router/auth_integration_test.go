package router

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/security"
)

// newAuthRequest gives httptest requests an explicit peer address.  The setup
// endpoint must distinguish a real loopback client from a forwarded or remote
// address; relying on httptest.NewRequest's default peer would make this
// important security property untested.
func newAuthRequest(method, target, body string, remote string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.RemoteAddr = remote
	req.Header.Set("Content-Type", "application/json")
	return req
}

func initAuthTestDB(t *testing.T) {
	t.Helper()
	// Keep host/proxy-sensitive setup assertions deterministic even when the
	// developer shell exports deployment-only proxy settings.
	t.Setenv("RELAY_TRUST_PROXY", "")
	t.Setenv("RELAY_SETUP_SECRET", "")
	if err := db.InitDB(t.TempDir() + "/auth-integration.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
}

func TestAuthSetupLoopbackAndCredentialIsolation(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()

	status := httptest.NewRecorder()
	engine.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/auth/status", nil))
	if status.Code != http.StatusOK || strings.Contains(status.Body.String(), `"initialized":true`) {
		t.Fatalf("fresh auth status = %d %s", status.Code, status.Body.String())
	}

	remote := httptest.NewRecorder()
	engine.ServeHTTP(remote, newAuthRequest(http.MethodPost, "/api/auth/setup", `{"username":"administrator","password":"correct horse battery"}`, "203.0.113.10:1234"))
	if remote.Code != http.StatusForbidden {
		t.Fatalf("non-loopback setup returned %d: %s", remote.Code, remote.Body.String())
	}

	setup := httptest.NewRecorder()
	setupReq := newAuthRequest(http.MethodPost, "/api/auth/setup", `{"username":"administrator","password":"correct horse battery"}`, "127.0.0.1:4321")
	engine.ServeHTTP(setup, setupReq)
	if setup.Code != http.StatusCreated {
		t.Fatalf("loopback setup returned %d: %s", setup.Code, setup.Body.String())
	}
	var setupPayload struct {
		CSRF string `json:"csrf_token"`
		Key  string `json:"gateway_api_key"`
	}
	if err := json.Unmarshal(setup.Body.Bytes(), &setupPayload); err != nil {
		t.Fatal(err)
	}
	if setupPayload.CSRF == "" || setupPayload.Key == "" {
		t.Fatalf("setup did not return one-time credentials: %+v", setupPayload)
	}
	if !strings.HasPrefix(setupPayload.Key, "sk-gw-") {
		t.Fatalf("unexpected gateway key format %q", setupPayload.Key)
	}
	setCookie := setup.Result().Cookies()
	if len(setCookie) != 1 || setCookie[0].Name != security.SessionCookieName || !setCookie[0].HttpOnly || setCookie[0].SameSite != http.SameSiteStrictMode || setCookie[0].Secure {
		t.Fatalf("setup session cookie has unsafe attributes: %+v", setCookie)
	}
	cookie := setCookie[0]

	second := httptest.NewRecorder()
	engine.ServeHTTP(second, newAuthRequest(http.MethodPost, "/api/auth/setup", `{"username":"administrator","password":"correct horse battery"}`, "127.0.0.1:4321"))
	if second.Code != http.StatusConflict {
		t.Fatalf("second setup returned %d: %s", second.Code, second.Body.String())
	}

	adminGet := httptest.NewRecorder()
	adminReq := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	adminReq.AddCookie(cookie)
	engine.ServeHTTP(adminGet, adminReq)
	if adminGet.Code != http.StatusOK {
		t.Fatalf("session did not authenticate admin GET: %d %s", adminGet.Code, adminGet.Body.String())
	}
	// Force an idle session and verify the seven-day sliding renewal path.
	var sessionRow db.AdminSessionModel
	if err := db.DB.First(&sessionRow).Error; err != nil {
		t.Fatal(err)
	}
	oldExpiry := time.Now().UTC().Add(time.Hour)
	if err := db.DB.Model(&sessionRow).Updates(map[string]any{"last_seen_at": time.Now().UTC().Add(-2 * time.Hour), "expires_at": oldExpiry}).Error; err != nil {
		t.Fatal(err)
	}
	sliding := httptest.NewRecorder()
	slidingReq := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	slidingReq.AddCookie(cookie)
	engine.ServeHTTP(sliding, slidingReq)
	if sliding.Code != http.StatusOK {
		t.Fatalf("idle session was rejected during sliding renewal: %d", sliding.Code)
	}
	if err := db.DB.First(&sessionRow, "token_hash = ?", sessionRow.TokenHash).Error; err != nil {
		t.Fatal(err)
	}
	if !sessionRow.ExpiresAt.After(oldExpiry.Add(24 * time.Hour)) {
		t.Fatalf("session expiry was not slid forward: old=%s new=%s", oldExpiry, sessionRow.ExpiresAt)
	}

	adminWithGateway := httptest.NewRecorder()
	adminGatewayReq := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	adminGatewayReq.Header.Set("Authorization", "Bearer "+setupPayload.Key)
	engine.ServeHTTP(adminWithGateway, adminGatewayReq)
	if adminWithGateway.Code != http.StatusUnauthorized {
		t.Fatalf("gateway key crossed into admin API: %d %s", adminWithGateway.Code, adminWithGateway.Body.String())
	}

	v1WithoutKey := httptest.NewRecorder()
	engine.ServeHTTP(v1WithoutKey, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if v1WithoutKey.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /v1 request returned %d", v1WithoutKey.Code)
	}
	v1WithKey := httptest.NewRecorder()
	v1Req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	v1Req.Header.Set("Authorization", "Bearer "+setupPayload.Key)
	engine.ServeHTTP(v1WithKey, v1Req)
	if v1WithKey.Code != http.StatusOK {
		t.Fatalf("gateway key did not authenticate /v1: %d %s", v1WithKey.Code, v1WithKey.Body.String())
	}
}

func TestAuthCSRFRotationGatewayRotationAndLogout(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()

	setup := httptest.NewRecorder()
	engine.ServeHTTP(setup, newAuthRequest(http.MethodPost, "/api/auth/setup", `{"username":"administrator","password":"correct horse battery"}`, "127.0.0.1:4321"))
	if setup.Code != http.StatusCreated {
		t.Fatalf("setup returned %d: %s", setup.Code, setup.Body.String())
	}
	var setupPayload struct {
		CSRF string `json:"csrf_token"`
		Key  string `json:"gateway_api_key"`
	}
	_ = json.Unmarshal(setup.Body.Bytes(), &setupPayload)
	cookie := setup.Result().Cookies()[0]

	// /api/auth/me rotates the CSRF token.  The old one must stop working,
	// while the newly returned token remains valid for the current session.
	me := httptest.NewRecorder()
	meReq := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	meReq.AddCookie(cookie)
	engine.ServeHTTP(me, meReq)
	if me.Code != http.StatusOK {
		t.Fatalf("auth/me returned %d: %s", me.Code, me.Body.String())
	}
	var mePayload struct {
		CSRF string `json:"csrf_token"`
	}
	if err := json.Unmarshal(me.Body.Bytes(), &mePayload); err != nil || mePayload.CSRF == "" {
		t.Fatalf("auth/me did not return a CSRF token: %s", me.Body.String())
	}

	rotate := func(csrf, key string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/gateway-token/rotate", nil)
		req.AddCookie(cookie)
		if csrf != "" {
			req.Header.Set("X-CSRF-Token", csrf)
		}
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		engine.ServeHTTP(rec, req)
		return rec
	}
	if got := rotate("", ""); got.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF accepted with status %d", got.Code)
	}
	if got := rotate("wrong-token", ""); got.Code != http.StatusForbidden {
		t.Fatalf("invalid CSRF accepted with status %d", got.Code)
	}

	if got := rotate(setupPayload.CSRF, ""); got.Code != http.StatusForbidden {
		t.Fatalf("CSRF token returned by setup remained valid after /me rotation: %d", got.Code)
	}
	rotated := rotate(mePayload.CSRF, "")
	if rotated.Code != http.StatusOK {
		t.Fatalf("valid CSRF rotation returned %d: %s", rotated.Code, rotated.Body.String())
	}
	var rotatedPayload struct {
		Key string `json:"gateway_api_key"`
	}
	_ = json.Unmarshal(rotated.Body.Bytes(), &rotatedPayload)
	if rotatedPayload.Key == "" || rotatedPayload.Key == setupPayload.Key {
		t.Fatalf("gateway key was not rotated: old=%q new=%q", setupPayload.Key, rotatedPayload.Key)
	}

	oldKey := httptest.NewRecorder()
	oldReq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	oldReq.Header.Set("Authorization", "Bearer "+setupPayload.Key)
	engine.ServeHTTP(oldKey, oldReq)
	if oldKey.Code != http.StatusUnauthorized {
		t.Fatalf("old gateway key remained valid: %d", oldKey.Code)
	}
	newKey := httptest.NewRecorder()
	newReq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	newReq.Header.Set("x-api-key", rotatedPayload.Key)
	engine.ServeHTTP(newKey, newReq)
	if newKey.Code != http.StatusOK {
		t.Fatalf("rotated gateway key failed /v1 auth: %d %s", newKey.Code, newKey.Body.String())
	}

	logout := httptest.NewRecorder()
	logoutReq := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	logoutReq.AddCookie(cookie)
	logoutReq.Header.Set("X-CSRF-Token", mePayload.CSRF)
	engine.ServeHTTP(logout, logoutReq)
	if logout.Code != http.StatusOK {
		t.Fatalf("logout returned %d: %s", logout.Code, logout.Body.String())
	}
	after := httptest.NewRecorder()
	afterReq := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	afterReq.AddCookie(cookie)
	engine.ServeHTTP(after, afterReq)
	if after.Code != http.StatusUnauthorized {
		t.Fatalf("logged-out session remained valid: %d", after.Code)
	}
}

func TestAuthLoginAndRateLimit(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()
	username := fmt.Sprintf("rate-%d", time.Now().UnixNano())
	remoteIP := fmt.Sprintf("198.51.100.%d:9876", 20+time.Now().UnixNano()%200)
	setup := httptest.NewRecorder()
	engine.ServeHTTP(setup, newAuthRequest(http.MethodPost, "/api/auth/setup", fmt.Sprintf(`{"username":%q,"password":"correct horse battery"}`, username), "127.0.0.1:4321"))
	if setup.Code != http.StatusCreated {
		t.Fatalf("setup returned %d: %s", setup.Code, setup.Body.String())
	}
	setupCookie := setup.Result().Cookies()[0]
	logout := httptest.NewRecorder()
	logoutReq := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	logoutReq.AddCookie(setupCookie)
	var payload struct {
		CSRF string `json:"csrf_token"`
	}
	_ = json.Unmarshal(setup.Body.Bytes(), &payload)
	logoutReq.Header.Set("X-CSRF-Token", payload.CSRF)
	engine.ServeHTTP(logout, logoutReq)

	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := newAuthRequest(http.MethodPost, "/api/auth/login", fmt.Sprintf(`{"username":%q,"password":"wrong password"}`, username), remoteIP)
		engine.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("failed login %d returned %d, want 401", i+1, rec.Code)
		}
	}
	blocked := httptest.NewRecorder()
	engine.ServeHTTP(blocked, newAuthRequest(http.MethodPost, "/api/auth/login", fmt.Sprintf(`{"username":%q,"password":"correct horse battery"}`, username), remoteIP))
	if blocked.Code != http.StatusTooManyRequests {
		t.Fatalf("sixth login was not rate-limited: %d %s", blocked.Code, blocked.Body.String())
	}

	good := httptest.NewRecorder()
	engine.ServeHTTP(good, newAuthRequest(http.MethodPost, "/api/auth/login", fmt.Sprintf(`{"username":%q,"password":"correct horse battery"}`, username), "127.0.0.1:4321"))
	if good.Code != http.StatusOK {
		t.Fatalf("valid login from another peer was unexpectedly blocked: %d %s", good.Code, good.Body.String())
	}
}

func TestAuthenticationErrorsUseChineseMessagesWithoutCredentialDetails(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()

	adminRequest := httptest.NewRecorder()
	engine.ServeHTTP(adminRequest, httptest.NewRequest(http.MethodGet, "/api/channels", nil))
	if adminRequest.Code != http.StatusUnauthorized || !strings.Contains(adminRequest.Body.String(), "需要管理员登录") {
		t.Fatalf("admin authentication error = %d %s", adminRequest.Code, adminRequest.Body.String())
	}

	gatewayRequest := httptest.NewRecorder()
	gatewayRequestReq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	gatewayRequestReq.Header.Set("Authorization", "Bearer definitely-not-a-real-key")
	engine.ServeHTTP(gatewayRequest, gatewayRequestReq)
	if gatewayRequest.Code != http.StatusUnauthorized || !strings.Contains(gatewayRequest.Body.String(), "API 密钥无效") || strings.Contains(gatewayRequest.Body.String(), "definitely-not-a-real-key") {
		t.Fatalf("gateway authentication error = %d %s", gatewayRequest.Code, gatewayRequest.Body.String())
	}
}

func TestProfilesPageRoute(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()

	// When no admin is setup, accessing /profiles redirects to /setup
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/profiles", nil))
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/setup" {
		t.Fatalf("uninitialized /profiles returned %d location %q", rec.Code, rec.Header().Get("Location"))
	}

	// Setup admin
	setup := httptest.NewRecorder()
	engine.ServeHTTP(setup, newAuthRequest(http.MethodPost, "/api/auth/setup", `{"username":"admin","password":"password-123456"}`, "127.0.0.1:1234"))
	if setup.Code != http.StatusCreated {
		t.Fatalf("setup admin failed: %d %s", setup.Code, setup.Body.String())
	}
	cookie := setup.Result().Cookies()[0]

	// Access without session cookie redirects to /login
	unauth := httptest.NewRecorder()
	engine.ServeHTTP(unauth, httptest.NewRequest(http.MethodGet, "/profiles", nil))
	if unauth.Code != http.StatusFound || unauth.Header().Get("Location") != "/login" {
		t.Fatalf("unauthenticated /profiles returned %d location %q", unauth.Code, unauth.Header().Get("Location"))
	}

	// Access with session cookie serves profiles.html with HTTP 200
	authed := httptest.NewRecorder()
	authReq := httptest.NewRequest(http.MethodGet, "/profiles", nil)
	authReq.AddCookie(cookie)
	engine.ServeHTTP(authed, authReq)
	if authed.Code != http.StatusOK {
		t.Fatalf("authenticated /profiles returned %d", authed.Code)
	}
	body := authed.Body.String()
	if !strings.Contains(body, `data-page="profiles"`) || !strings.Contains(body, "Profile 管理") {
		t.Fatalf("profiles page body missing expected markup: %s", body[:min(200, len(body))])
	}
}
