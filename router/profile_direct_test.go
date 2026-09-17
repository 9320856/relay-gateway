package router

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

func TestProfileEngineDirectChatUsesPublishedBinding(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-direct.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer profile-key" {
			t.Fatalf("authorization header = %q", r.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil || payload["model"] != "provider-chat" {
			t.Fatalf("request body = %s, err=%v", body, err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{\"id\":\"chatcmpl-profile\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"ok\"}}]}")
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-direct-channel", Name: "profile-direct-channel", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "profile-key", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "chat-model", ModelMapRaw: "{\"chat-model\":\"provider-chat\"}"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Name: "direct-chat", Operations: []protocol.Operation{{
		Operation: "chat.completions", ExecutionMode: protocol.ExecutionDirect, PollingMode: protocol.PollingOff,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/chat/completions", BodyEncoding: "json"},
		Response: protocol.Response{ResultPaths: []string{"choices"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-direct", Name: "Direct Chat", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-direct", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "chat.completions", ModelPattern: "chat-model", ProfileID: "profile-direct", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_ENABLE_PROFILE_DIRECT_ENGINE", "1")
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("{\"model\":\"chat-model\",\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}]}"))
	handled, err := profileEngineDirect(c, "chat.completions", "")
	if err != nil || !handled {
		t.Fatalf("profile direct handled=%v err=%v", handled, err)
	}
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "chatcmpl-profile") {
		t.Fatalf("response code=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestProfileDirectFailoverRejectsPossiblySubmittedMutation(t *testing.T) {
	if profileDirectFailoverEligible(&protocol.ExecutorError{HTTPStatus: http.StatusInternalServerError, MayHaveSubmitted: true}) {
		t.Fatal("a possibly submitted mutation must not fail over")
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusRequestTimeout, http.StatusTooManyRequests} {
		if !profileDirectFailoverEligible(&protocol.ExecutorError{HTTPStatus: status}) {
			t.Fatalf("status %d should remain eligible when submission is known not to have occurred", status)
		}
	}
	if profileDirectFailoverEligible(&protocol.ExecutorError{HTTPStatus: http.StatusInternalServerError}) {
		t.Fatal("5xx mutation should not be eligible without an explicit non-submission proof")
	}
}

func TestProfileEngineDirectRequiresExplicitBinding(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-direct-no-binding.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{
		ID: "profile-direct-unbound", Name: "Unbound", Type: "openai",
		BaseURL: "https://openai.example/v1", APIKey: "profile-key", Enabled: true,
		Priority: 1, Weight: 1, ModelsRaw: "chat-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_ENABLE_PROFILE_DIRECT_ENGINE", "1")
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"chat-model","messages":[]}`))
	handled, err := profileEngineDirect(c, "chat.completions", "")
	if err != nil || handled {
		t.Fatalf("unbound profile should fall back to legacy routing: handled=%v err=%v", handled, err)
	}
}

func TestProfileEngineDirectLeavesAsyncBindingToTaskBridge(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-direct-async.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{
		ID: "profile-direct-async", Name: "Async", Type: "openai",
		BaseURL: "https://openai.example/v1", APIKey: "profile-key", Enabled: true,
		Priority: 1, Weight: 1, ModelsRaw: "chat-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Name: "async-chat", Operations: []protocol.Operation{{
		Operation: "chat.completions", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingOff,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/chat/completions", BodyEncoding: "json"},
		Response: protocol.Response{TaskIDPaths: []string{"id"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-direct-async", Name: "Async Chat", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-direct-async", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "chat.completions", ModelPattern: "chat-model", ProfileID: "profile-direct-async", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RELAY_ENABLE_PROFILE_DIRECT_ENGINE", "1")
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"chat-model","messages":[]}`))
	handled, err := profileEngineDirect(c, "chat.completions", "")
	if err != nil || handled {
		t.Fatalf("async profile must be left to its task bridge: handled=%v err=%v", handled, err)
	}
}
