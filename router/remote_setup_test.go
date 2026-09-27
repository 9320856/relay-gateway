package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"relay-gateway/db"
	"relay-gateway/security"
)

func TestSetupAcceptsAnySourceBeforeInitialization(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()
	for _, remote := range []string{"127.0.0.1:4321", "192.168.58.1:4321", "203.0.113.10:4321", "[2001:db8::10]:4321"} {
		for _, proxyMode := range []string{"", "0", "1"} {
			t.Setenv("RELAY_TRUST_PROXY", proxyMode)
			request := newAuthRequest(http.MethodPost, "http://gateway.example:8000/api/auth/setup", "{", remote)
			request.Header.Set("Origin", "http://gateway.example:8000")
			request.Header.Set("X-Forwarded-For", "127.0.0.1")
			recorder := httptest.NewRecorder()
			engine.ServeHTTP(recorder, request)
			// The malformed body must reach parsing, regardless of network source
			// or proxy configuration, without creating a real administrator.
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "初始化请求格式不正确") {
				t.Fatalf("remote=%s proxy=%q returned %d: %s", remote, proxyMode, recorder.Code, recorder.Body.String())
			}
		}
	}
	if security.HasAdmin() {
		t.Fatal("invalid initialization requests created an administrator")
	}
	status := httptest.NewRecorder()
	engine.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/auth/status", nil))
	var payload struct {
		Initialized         bool `json:"initialized"`
		SetupSecretRequired bool `json:"setup_secret_required"`
	}
	if err := json.Unmarshal(status.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Initialized || payload.SetupSecretRequired {
		t.Fatalf("default first-time setup status = %+v", payload)
	}
}

func TestRemoteSetupOptionalSecretAndRepeatProtection(t *testing.T) {
	initAuthTestDB(t)
	t.Setenv("RELAY_SETUP_SECRET", "configured-bootstrap-secret")
	engine := Setup()
	status := httptest.NewRecorder()
	engine.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/auth/status", nil))
	if !strings.Contains(status.Body.String(), `"setup_secret_required":true`) || strings.Contains(status.Body.String(), "configured-bootstrap-secret") {
		t.Fatalf("protected setup status = %s", status.Body.String())
	}
	for _, remote := range []string{"127.0.0.1:4321", "192.168.58.1:4321", "203.0.113.10:4321"} {
		for _, supplied := range []string{"", "wrong-bootstrap-secret"} {
			request := newAuthRequest(http.MethodPost, "/api/auth/setup?setup_secret=configured-bootstrap-secret", `{"username":"admin","password":"correct horse battery","setup_secret":"configured-bootstrap-secret"}`, remote)
			request.Header.Set("X-Relay-Setup-Secret", supplied)
			recorder := httptest.NewRecorder()
			engine.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusForbidden || len(recorder.Result().Cookies()) != 0 {
				t.Fatalf("missing/wrong setup secret from %s returned %d", remote, recorder.Code)
			}
		}
	}
	if security.HasAdmin() {
		t.Fatal("incorrect initialization secret created an administrator")
	}
	t.Setenv("RELAY_TRUST_PROXY", "1")
	request := newAuthRequest(http.MethodPost, "http://gateway.example/api/auth/setup", `{"username":"admin","password":"correct horse battery"}`, "203.0.113.10:4321")
	request.Header.Set("Origin", "https://gateway.example")
	request.Header.Set("X-Relay-Setup-Secret", "configured-bootstrap-secret")
	request.Header.Set("X-Forwarded-Proto", "https")
	created := httptest.NewRecorder()
	engine.ServeHTTP(created, request)
	if created.Code != http.StatusCreated {
		t.Fatalf("protected remote setup returned %d: %s", created.Code, created.Body.String())
	}
	var payload struct {
		Key string `json:"gateway_api_key"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	cookies := created.Result().Cookies()
	if payload.Key == "" || len(cookies) != 1 || !cookies[0].Secure || !security.ValidateGatewayToken(payload.Key) {
		t.Fatal("protected remote setup did not return a valid key and HTTPS session")
	}
	var originalToken db.GatewayTokenModel
	if err := db.DB.First(&originalToken, 1).Error; err != nil {
		t.Fatal(err)
	}
	status = httptest.NewRecorder()
	engine.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/auth/status", nil))
	if !strings.Contains(status.Body.String(), `"initialized":true`) || !strings.Contains(status.Body.String(), `"setup_secret_required":false`) {
		t.Fatalf("completed setup status = %s", status.Body.String())
	}
	for _, body := range []string{"{", `{"username":"another-admin","password":"another strong password"}`} {
		repeated := httptest.NewRecorder()
		// Even a missing secret or malformed body cannot reopen setup.
		engine.ServeHTTP(repeated, newAuthRequest(http.MethodPost, "/api/auth/setup", body, "198.51.100.20:4321"))
		if repeated.Code != http.StatusConflict || len(repeated.Result().Cookies()) != 0 || strings.Contains(repeated.Body.String(), "gateway_api_key") {
			t.Fatalf("repeat setup returned %d: %s", repeated.Code, repeated.Body.String())
		}
	}
	var afterToken db.GatewayTokenModel
	if err := db.DB.First(&afterToken, 1).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(originalToken, afterToken) || !security.ValidateGatewayToken(payload.Key) {
		t.Fatal("repeated initialization changed the gateway token")
	}
	var admins, sessions int64
	if err := db.DB.Model(&db.AdminUserModel{}).Count(&admins).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Model(&db.AdminSessionModel{}).Count(&sessions).Error; err != nil {
		t.Fatal(err)
	}
	if admins != 1 || sessions != 1 {
		t.Fatalf("repeat setup created extra records: admins=%d sessions=%d", admins, sessions)
	}
	if _, err := security.ValidateSession(cookies[0].Value); err != nil {
		t.Fatalf("original session no longer valid: %v", err)
	}
}

func TestSetupRejectsCrossOriginBrowserRequests(t *testing.T) {
	initAuthTestDB(t)
	engine := Setup()
	for _, origin := range []string{"https://attacker.example", "null", "file://gateway.example"} {
		request := newAuthRequest(http.MethodPost, "https://gateway.example/api/auth/setup", `{"username":"admin","password":"correct horse battery"}`, "203.0.113.10:4321")
		request.Header.Set("Origin", origin)
		recorder := httptest.NewRecorder()
		engine.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("cross-origin setup returned %d for %q", recorder.Code, origin)
		}
	}
	if security.HasAdmin() {
		t.Fatal("cross-origin initialization created an administrator")
	}
}
