package router

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/protocol"
)

func TestChannelProtocolBindingRoutesLifecycle(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bindings.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "binding-channel", Name: "channel", Type: "newapi", BaseURL: "http://example.test", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "binding-profile", Name: "profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "binding-profile", Revision: 1, SchemaVersion: 1, ContentJSON: bindingProfileContent(t, "video.create", "chat"), ContentDigest: "d", State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/bindings", handleListChannelProtocolBindings)
	r.POST("/bindings", handleCreateChannelProtocolBinding)
	r.GET("/bindings/:id", handleGetChannelProtocolBinding)
	r.PUT("/bindings/:id", handleUpdateChannelProtocolBinding)
	r.PATCH("/bindings/:id/toggle", handleToggleChannelProtocolBinding)
	r.DELETE("/bindings/:id", handleDeleteChannelProtocolBinding)
	payload := `{"channel_id":"binding-channel","operation":"video.create","model_pattern":"model-*","profile_id":"binding-profile","profile_revision":1,"precedence":4}`
	w := requestBinding(t, r, http.MethodPost, "/bindings", payload)
	if w.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", w.Code, w.Body.String())
	}
	var created struct {
		Binding db.ChannelProtocolBinding `json:"binding"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || created.Binding.ID == 0 || !created.Binding.Enabled {
		t.Fatalf("create response=%s", w.Body.String())
	}
	id := created.Binding.ID
	w = requestBinding(t, r, http.MethodGet, "/bindings", "")
	if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte("video.create")) {
		t.Fatalf("list response=%d %s", w.Code, w.Body.String())
	}
	w = requestBinding(t, r, http.MethodPatch, "/bindings/"+itoa(id)+"/toggle", "")
	if w.Code != http.StatusOK || bytes.Contains(w.Body.Bytes(), []byte(`"enabled":true`)) {
		t.Fatalf("toggle response=%d %s", w.Code, w.Body.String())
	}
	w = requestBinding(t, r, http.MethodPut, "/bindings/"+itoa(id), `{"channel_id":"binding-channel","operation":"chat","model_pattern":"*","profile_id":"binding-profile","profile_revision":1}`)
	if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"operation":"chat"`)) {
		t.Fatalf("update response=%d %s", w.Code, w.Body.String())
	}
	w = requestBinding(t, r, http.MethodDelete, "/bindings/"+itoa(id), "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d", w.Code)
	}
}

func TestChannelProtocolBindingRoutesRejectUnpublishedAndMissingReferences(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bindings-invalid.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "binding-channel", Name: "channel", Type: "newapi", BaseURL: "http://example.test", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "binding-profile", Name: "profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "binding-profile", Revision: 1, SchemaVersion: 1, ContentJSON: bindingProfileContent(t, "video.create"), ContentDigest: "d", State: db.ProfileRevisionDraft}); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/bindings", handleCreateChannelProtocolBinding)
	for _, payload := range []string{
		`{"channel_id":"missing","operation":"x","model_pattern":"*","profile_id":"binding-profile","profile_revision":1}`,
		`{"channel_id":"binding-channel","operation":"x","model_pattern":"*","profile_id":"binding-profile","profile_revision":1}`,
	} {
		w := requestBinding(t, r, http.MethodPost, "/bindings", payload)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
		}
	}
}

func TestChannelProtocolBindingRoutesRejectOperationMissingFromPublishedProfile(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bindings-operation.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "binding-channel", Name: "channel", Type: "newapi", BaseURL: "http://example.test", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "binding-profile", Name: "profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "binding-profile", Revision: 1, SchemaVersion: 1, ContentJSON: bindingProfileContent(t, "chat.completions"), ContentDigest: "d", State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/bindings", handleCreateChannelProtocolBinding)
	w := requestBinding(t, r, http.MethodPost, "/bindings", `{"channel_id":"binding-channel","operation":"video.create","model_pattern":"*","profile_id":"binding-profile","profile_revision":1}`)
	if w.Code != http.StatusBadRequest || !bytes.Contains(w.Body.Bytes(), []byte("未定义")) {
		t.Fatalf("missing operation response=%d %s", w.Code, w.Body.String())
	}
}

func TestChannelProtocolBindingRoutesRejectSamePrecedenceOverlap(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bindings-conflict.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "binding-channel", Name: "channel", Type: "newapi", BaseURL: "http://example.test", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "binding-profile", Name: "profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "binding-profile", Revision: 1, SchemaVersion: 1, ContentJSON: bindingProfileContent(t, "chat.completions"), ContentDigest: "d", State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/bindings", handleCreateChannelProtocolBinding)
	first := `{"channel_id":"binding-channel","operation":"chat.completions","model_pattern":"model-*","profile_id":"binding-profile","profile_revision":1,"precedence":4}`
	if w := requestBinding(t, r, http.MethodPost, "/bindings", first); w.Code != http.StatusCreated {
		t.Fatalf("first binding response=%d %s", w.Code, w.Body.String())
	}
	second := `{"channel_id":"binding-channel","operation":"chat.completions","model_pattern":"model-fast","profile_id":"binding-profile","profile_revision":1,"precedence":4}`
	if w := requestBinding(t, r, http.MethodPost, "/bindings", second); w.Code != http.StatusConflict {
		t.Fatalf("overlap response=%d %s", w.Code, w.Body.String())
	}
}

func TestBindChannelProfileAppliesAllPublishedOperations(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bind-profile.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "profile-channel", Name: "channel", Type: "newapi", BaseURL: "http://example.test", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "published-profile", Name: "Published", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{
		ProfileID: "published-profile", Revision: 1, SchemaVersion: 1,
		ContentJSON:   bindingProfileContent(t, "chat.completions", "video.create"),
		ContentDigest: "d", State: db.ProfileRevisionPublished,
	}); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/channels/:id/bind-profile", handleBindChannelProfile)
	w := requestBinding(t, r, http.MethodPost, "/channels/profile-channel/bind-profile", `{"profile_id":"published-profile","profile_revision":1}`)
	if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"profile_revision":1`)) {
		t.Fatalf("bind profile response=%d %s", w.Code, w.Body.String())
	}
	var bindings []db.ChannelProtocolBinding
	if err := db.DB.Where("channel_id = ?", "profile-channel").Find(&bindings).Error; err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 2 {
		t.Fatalf("expected one wildcard binding per operation, got %d: %+v", len(bindings), bindings)
	}
	for _, binding := range bindings {
		if binding.ProfileID != "published-profile" || binding.ProfileRevision != 1 || binding.ModelPattern != "*" || binding.Precedence != 0 || !binding.Enabled {
			t.Fatalf("unexpected applied binding: %+v", binding)
		}
	}
	// Reapplying the same Profile updates the existing slots instead of
	// creating duplicate bindings.
	w = requestBinding(t, r, http.MethodPost, "/channels/profile-channel/bind-profile", `{"profile_id":"published-profile"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("rebind profile response=%d %s", w.Code, w.Body.String())
	}
	var count int64
	if err := db.DB.Model(&db.ChannelProtocolBinding{}).Where("channel_id = ?", "profile-channel").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("rebind created duplicate bindings: %d", count)
	}
}

func TestBindChannelProfileRejectsUnpublishedRevision(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bind-profile-invalid.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "profile-channel", Name: "channel", Type: "newapi", BaseURL: "http://example.test", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "draft-profile", Name: "Draft", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "draft-profile", Revision: 1, SchemaVersion: 1, ContentJSON: bindingProfileContent(t, "chat.completions"), ContentDigest: "d", State: db.ProfileRevisionDraft}); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/channels/:id/bind-profile", handleBindChannelProfile)
	w := requestBinding(t, r, http.MethodPost, "/channels/profile-channel/bind-profile", `{"profile_id":"draft-profile","profile_revision":1}`)
	if w.Code != http.StatusBadRequest || !bytes.Contains(w.Body.Bytes(), []byte("必须先发布")) {
		t.Fatalf("draft profile response=%d %s", w.Code, w.Body.String())
	}
}

func TestChannelProtocolBindingRouteDoesNotEnableRetiredRevision(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bindings-retired.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "binding-channel", Name: "channel", Type: "newapi", BaseURL: "http://example.test", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "binding-profile", Name: "profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "binding-profile", Revision: 1, SchemaVersion: 1, ContentJSON: bindingProfileContent(t, "chat.completions"), ContentDigest: "d", State: db.ProfileRevisionRetired}); err != nil {
		t.Fatal(err)
	}
	binding := &db.ChannelProtocolBinding{ChannelID: "binding-channel", Operation: "chat.completions", ModelPattern: "*", ProfileID: "binding-profile", ProfileRevision: 1, Enabled: false}
	if err := db.SaveChannelProtocolBinding(binding); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.PATCH("/bindings/:id/toggle", handleToggleChannelProtocolBinding)
	w := requestBinding(t, r, http.MethodPatch, "/bindings/"+itoa(binding.ID)+"/toggle", "")
	if w.Code != http.StatusConflict || !bytes.Contains(w.Body.Bytes(), []byte("已停用")) {
		t.Fatalf("retired toggle response=%d %s", w.Code, w.Body.String())
	}
	var stored db.ChannelProtocolBinding
	if err := db.DB.First(&stored, binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Enabled {
		t.Fatalf("retired revision binding was re-enabled: %+v", stored)
	}
}

func TestChannelProfileBindReplacementAndUnbind(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bind-replace.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.SaveChannelModel(&db.ChannelModel{ID: "ch-switch", Name: "channel-switch", Type: "openai", BaseURL: "http://example.test", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	// Profile A has 3 operations
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "prof-3op", Name: "3-op profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{
		ProfileID: "prof-3op", Revision: 1, SchemaVersion: 1,
		ContentJSON:   bindingProfileContent(t, "chat.completions", "images.create", "video.create"),
		ContentDigest: "d1", State: db.ProfileRevisionPublished,
	}); err != nil {
		t.Fatal(err)
	}

	// Profile B has only 1 operation
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "prof-1op", Name: "1-op profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{
		ProfileID: "prof-1op", Revision: 1, SchemaVersion: 1,
		ContentJSON:   bindingProfileContent(t, "chat.completions"),
		ContentDigest: "d2", State: db.ProfileRevisionPublished,
	}); err != nil {
		t.Fatal(err)
	}

	r := gin.New()
	r.POST("/channels/:id/bind-profile", handleBindChannelProfile)

	// 1. Bind Profile A (3 operations)
	w := requestBinding(t, r, http.MethodPost, "/channels/ch-switch/bind-profile", `{"profile_id":"prof-3op","profile_revision":1}`)
	if w.Code != http.StatusOK {
		t.Fatalf("bind 3-op status=%d body=%s", w.Code, w.Body.String())
	}
	var bindings []db.ChannelProtocolBinding
	if err := db.DB.Where("channel_id = ? AND precedence = 0", "ch-switch").Find(&bindings).Error; err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 3 {
		t.Fatalf("expected 3 bindings, got %d", len(bindings))
	}

	// 2. Switch to Profile B (1 operation) -> the other 2 operations must be deleted!
	w = requestBinding(t, r, http.MethodPost, "/channels/ch-switch/bind-profile", `{"profile_id":"prof-1op","profile_revision":1}`)
	if w.Code != http.StatusOK {
		t.Fatalf("bind 1-op status=%d body=%s", w.Code, w.Body.String())
	}
	bindings = nil
	if err := db.DB.Where("channel_id = ? AND precedence = 0", "ch-switch").Find(&bindings).Error; err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 1 {
		t.Fatalf("expected exactly 1 binding after switching to 1-op profile, got %d", len(bindings))
	}
	if bindings[0].Operation != "chat.completions" || bindings[0].ProfileID != "prof-1op" {
		t.Fatalf("unexpected binding: %+v", bindings[0])
	}

	// 3. Switch back to built-in protocol (profile_id = "") -> all precedence=0 bindings must be deleted!
	w = requestBinding(t, r, http.MethodPost, "/channels/ch-switch/bind-profile", `{"profile_id":""}`)
	if w.Code != http.StatusOK {
		t.Fatalf("unbind status=%d body=%s", w.Code, w.Body.String())
	}
	bindings = nil
	if err := db.DB.Where("channel_id = ? AND precedence = 0", "ch-switch").Find(&bindings).Error; err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 0 {
		t.Fatalf("expected 0 bindings after unbind, got %d", len(bindings))
	}
}

func bindingProfileContent(t *testing.T, operations ...string) string {
	t.Helper()
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion}
	for _, name := range operations {
		profile.Operations = append(profile.Operations, protocol.Operation{
			Operation: name, ExecutionMode: protocol.ExecutionDirect, PollingMode: protocol.PollingOff,
			Submit:   protocol.Submit{Method: http.MethodPost, Path: "/v1/test", BodyEncoding: "json"},
			Response: protocol.Response{ResultPaths: []string{"id"}},
		})
	}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatalf("compile binding test profile: %v", err)
	}
	return string(compiled.CanonicalJSON())
}

func requestBinding(t *testing.T, r http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func itoa(id uint) string { return strconv.FormatUint(uint64(id), 10) }
