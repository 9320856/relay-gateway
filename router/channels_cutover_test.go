package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/migration"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

func TestAllChannelsCutoverReadinessWithReconcile(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/cutover-readiness.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bootstrapProfilesForRouterTest(t)

	t.Setenv("RELAY_DISABLE_LEGACY", "1")

	channelTypes := []string{
		protocol.PresetOpenAI,
		protocol.PresetAnthropic,
		protocol.PresetNewAPI,
		protocol.PresetSub2API,
	}

	for _, chType := range channelTypes {
		ch := &db.ChannelModel{
			ID:       "cutover-" + chType,
			Name:     "Cutover " + chType,
			Type:     chType,
			BaseURL:  "https://" + chType + ".example/v1",
			Enabled:  true,
			Priority: 1,
			Weight:   1,
		}
		if err := db.SaveChannelModel(ch); err != nil {
			t.Fatalf("save channel %s: %v", chType, err)
		}
		service.DefaultDispatcher.ResetBreaker(ch.ID)
		t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(ch.ID) })
	}

	// Before reconcile, channels have no default bindings.
	preAudit, err := migration.AuditChannels(context.Background())
	if err != nil {
		t.Fatalf("pre-reconcile audit failed: %v", err)
	}
	if preAudit.Ready {
		t.Fatalf("expected not ready before reconcile, got ready")
	}

	// Run ReconcileAllChannelsProfileBindings
	report, err := ReconcileAllChannelsProfileBindings(context.Background(), true)
	if err != nil {
		t.Fatalf("reconcile all channels failed: %v", err)
	}
	if len(report.ReconciledChannels) != len(channelTypes) {
		t.Fatalf("expected %d reconciled channels, got %d", len(channelTypes), len(report.ReconciledChannels))
	}

	// Now AuditChannels must report ready: true and 0 blockers
	postAudit, err := migration.AuditChannels(context.Background())
	if err != nil {
		t.Fatalf("post-reconcile audit failed: %v", err)
	}
	if !postAudit.Ready {
		t.Fatalf("expected audit ready: true, got blockers: %v", postAudit.Blockers)
	}
	if postAudit.OperationsMissingBinding != 0 {
		t.Fatalf("expected 0 operations missing binding, got %d", postAudit.OperationsMissingBinding)
	}
}

func TestBindPresetHTTPEndpoint(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/bind-preset-http.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bootstrapProfilesForRouterTest(t)

	channelID := "http-test-openai"
	ch := &db.ChannelModel{
		ID: channelID, Name: "HTTP OpenAI", Type: protocol.PresetOpenAI,
		BaseURL: "https://openai.example/v1", Enabled: true,
	}
	if err := db.SaveChannelModel(ch); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Params = gin.Params{{Key: "id", Value: channelID}}
	c.Request = httptest.NewRequest(http.MethodPost, "/api/channels/"+channelID+"/bind-preset", nil)

	handleReconcileChannelBindings(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("bind-preset status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if resp["channel_id"] != channelID {
		t.Fatalf("unexpected response: %#v", resp)
	}

	preset, err := protocol.BuiltinPreset(protocol.PresetOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.DB.Model(&db.ChannelProtocolBinding{}).Where("channel_id = ?", channelID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != int64(len(preset.Operations)) {
		t.Fatalf("expected %d bindings created, got %d", len(preset.Operations), count)
	}
}

func TestRetiredAdapterTypesSuppressedAndRepublished(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/retired-adapter-types.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bootstrapProfilesForRouterTest(t)
	upstream := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(upstream.Close)

	r := Setup()

	setup := e2eRequest(t, r, http.MethodPost, "/api/auth/setup", `{"username":"administrator","password":"correct horse battery"}`, nil, "", "")
	if setup.Code != http.StatusCreated {
		t.Fatalf("setup returned %d: %s", setup.Code, setup.Body.String())
	}
	cookie := setup.Result().Cookies()[0]
	var setupResp struct {
		CSRFToken string `json:"csrf_token"`
	}
	if err := json.Unmarshal(setup.Body.Bytes(), &setupResp); err != nil {
		t.Fatal(err)
	}
	csrf := setupResp.CSRFToken

	// Initially, newapi should be in available adapter types
	checkHasType := func(targetType string) bool {
		rec := e2eRequest(t, r, http.MethodGet, "/api/adapter-types", "", cookie, csrf, "")
		var resp struct {
			Types []struct {
				Type string `json:"type"`
			} `json:"types"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal adapter types: %v", err)
		}
		for _, item := range resp.Types {
			if item.Type == targetType {
				return true
			}
		}
		return false
	}

	if !checkHasType("newapi") {
		t.Fatal("expected newapi to be in available adapter types initially")
	}

	// Retire builtin-newapi revision 1
	if err := db.RetireProtocolProfileRevision("builtin-newapi", 1); err != nil {
		t.Fatalf("retire builtin-newapi: %v", err)
	}

	// Now newapi must be suppressed from available adapter types
	if checkHasType("newapi") {
		t.Fatal("expected retired newapi to be suppressed from available adapter types")
	}

	// Attempting to create a new channel with retired type must fail
	createPayload, err := json.Marshal(map[string]any{
		"name": "Test Retired NewAPI", "type": "newapi", "base_url": upstream.URL + "/v1",
		"priority": 1, "weight": 1, "enabled": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	createBody := string(createPayload)
	recCreate := e2eRequest(t, r, http.MethodPost, "/api/channels", createBody, cookie, csrf, "")
	if recCreate.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for retired channel type, got %d: %s", recCreate.Code, recCreate.Body.String())
	}
	var rejectedCount int64
	if err := db.DB.Model(&db.ChannelModel{}).Where("type = ?", "newapi").Count(&rejectedCount).Error; err != nil {
		t.Fatal(err)
	}
	if rejectedCount != 0 {
		t.Fatalf("retired channel create left %d channel rows behind", rejectedCount)
	}

	// Re-publish builtin-newapi revision 1
	if err := db.PublishProtocolProfileRevision("builtin-newapi", 1); err != nil {
		t.Fatalf("re-publish builtin-newapi: %v", err)
	}

	// Now newapi must be available again
	if !checkHasType("newapi") {
		t.Fatal("expected republished newapi to appear in available adapter types")
	}

	// Creating a new channel with republished type must now succeed
	recCreate2 := e2eRequest(t, r, http.MethodPost, "/api/channels", createBody, cookie, csrf, "")
	if recCreate2.Code != http.StatusOK {
		t.Fatalf("expected 200 for republished channel type, got %d: %s", recCreate2.Code, recCreate2.Body.String())
	}
}
