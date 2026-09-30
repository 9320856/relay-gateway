package router

import (
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/profilebootstrap"
	"relay-gateway/service"
)

func selectionRequestContext(method, path, body string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, path, strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	return c, recorder
}

func TestChannelSelectionSaveAndCancellation(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "channel-selection-http-test-key")
	initAuthTestDB(t)
	if _, err := profilebootstrap.EnsureBuiltinProfiles(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/models" {
			items := make([]map[string]string, 100)
			for i := range items {
				items[i] = map[string]string{"id": fmt.Sprintf("model-%03d", i)}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": items})
			return
		}
		if r.URL.Path == "/v1/chat/completions" {
			var request struct {
				Model string `json:"model"`
			}
			_ = json.NewDecoder(r.Body).Decode(&request)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "selected-chat", "model": request.Model, "choices": []any{}})
			return
		}
		http.Error(w, "unexpected upstream request", http.StatusNotFound)
	}))
	defer upstream.Close()

	// The form's probe shows every candidate without enabling any of them.
	probe, probeRecorder := selectionRequestContext(http.MethodPost, "/api/channels/probe-models", fmt.Sprintf(`{"type":"openai","base_url":%q,"api_key":"test-key"}`, upstream.URL+"/v1"))
	handleProbeModels(probe)
	if probeRecorder.Code != http.StatusOK || !strings.Contains(probeRecorder.Body.String(), `"count":100`) {
		t.Fatalf("probe = %d %s", probeRecorder.Code, probeRecorder.Body.String())
	}
	selected := []string{"model-000", "model-001", "model-002", "model-003", "model-004"}
	save := func(models []string, includeSelection bool) {
		t.Helper()
		payload := map[string]any{"id": "selected", "type": "openai", "base_url": upstream.URL + "/v1", "api_key": "test-key", "enabled": true, "fetch_models": false, "models_raw": "*", "model_map_raw": `{"alias":"model-000","unselected-alias":"model-099"}`}
		if includeSelection {
			payload["selected_models"] = models
		}
		body, _ := json.Marshal(payload)
		c, recorder := selectionRequestContext(http.MethodPost, "/api/channels", string(body))
		handleSaveChannel(c)
		if recorder.Code != http.StatusOK {
			t.Fatalf("save = %d %s", recorder.Code, recorder.Body.String())
		}
		db.NotifyChannelsChanged("selected")
	}
	save(selected, true)
	defer service.DefaultDispatcher.RemoveChannel("selected")
	refresh, refreshRecorder := selectionRequestContext(http.MethodGet, "/api/channels/selected/models?refresh=true", "")
	refresh.Params = gin.Params{{Key: "id", Value: "selected"}}
	handleGetChannelModels(refresh)
	if refreshRecorder.Code != http.StatusOK {
		t.Fatalf("refresh = %d %s", refreshRecorder.Code, refreshRecorder.Body.String())
	}
	assertModels := func(want int) {
		t.Helper()
		models := service.DefaultDispatcher.ListModels().Data
		if len(models) != want {
			t.Fatalf("published models = %v, want %d", models, want)
		}
		for _, item := range models {
			if item.ID == "model-099" || item.ID == "unselected-alias" {
				t.Fatalf("unselected model leaked: %+v", item)
			}
		}
	}
	assertModels(6) // Five selected IDs and the enabled alias.
	chat := func(name string, allowed bool) {
		t.Helper()
		before := calls.Load()
		c, recorder := selectionRequestContext(http.MethodPost, "/v1/chat/completions", fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello"}]}`, name))
		handleChatCompletions(c)
		if allowed {
			if recorder.Code != http.StatusOK || calls.Load() != before+1 {
				t.Fatalf("allowed %s = %d %s calls=%d", name, recorder.Code, recorder.Body.String(), calls.Load()-before)
			}
		} else if recorder.Code != http.StatusBadRequest || calls.Load() != before {
			t.Fatalf("blocked %s = %d %s calls=%d", name, recorder.Code, recorder.Body.String(), calls.Load()-before)
		}
	}
	chat("model-004", true)
	chat("alias", true)
	chat("model-099", false)
	chat("unselected-alias", false)
	save(selected[:3], true)
	assertModels(4)
	chat("model-003", false)
	chat("model-004", false)
	chat("model-002", true)
	save(nil, false) // An older client must not accidentally remove the restriction.
	chat("model-004", false)
	row, err := db.GetChannelModel("selected")
	if err != nil {
		t.Fatal(err)
	}
	response := sanitizedChannelModel(*row)
	got, ok := response["selected_models"].([]string)
	if !ok || len(got) != 3 {
		t.Fatalf("saved selection = %#v", response["selected_models"])
	}
	if !strings.Contains(row.ModelsSyncedRaw, "model-099") {
		t.Fatal("selection edit discarded reselectable candidates")
	}
	save([]string{}, true)
	assertModels(0)
	chat("model-000", false)
}

func TestChannelSelectionRejectsInvalidAPIPayloads(t *testing.T) {
	for _, selected := range []string{`null`, `{}`, `"model"`, `[123]`, `["*"]`, `[""]`} {
		t.Run(selected, func(t *testing.T) {
			c, recorder := selectionRequestContext(http.MethodPost, "/api/channels", `{"selected_models":`+selected+`}`)
			handleSaveChannel(c)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("invalid selection = %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestPinnedProfileSelectionCannotFallBack(t *testing.T) {
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
	pinned := &config.UpstreamChannel{ID: "pinned", SelectedModels: []string{"allowed"}}
	t.Run("direct", func(t *testing.T) {
		c, _ := selectionRequestContext(http.MethodPost, "/v1/chat/completions", `{"model":"blocked"}`)
		handled, _, err := profileEngineDirectForChannelResult(c, "chat.completions", "", pinned, c.Writer, c.Writer.Written)
		if !handled || err == nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
	})
	t.Run("multipart", func(t *testing.T) {
		c, _ := selectionRequestContext(http.MethodPost, "/v1/images/edits", "")
		handled, err := profileEngineMultipartDirect(c, "images.edits", &multipart.Form{Value: map[string][]string{"model": {"blocked"}}}, pinned)
		if !handled || err == nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
	})
	t.Run("image", func(t *testing.T) {
		c, _ := selectionRequestContext(http.MethodPost, "/v1/images/generations", "")
		_, _, handled, err := profileEngineImageCreateForChannel(c, &model.ImageGenerationRequest{Model: "blocked"}, pinned)
		if !handled || err == nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
	})
	t.Run("video", func(t *testing.T) {
		c, _ := selectionRequestContext(http.MethodPost, "/v1/videos", "")
		_, _, handled, err := profileEngineVideoCreateForChannel(c, &model.VideoGenerationRequest{Model: "blocked"}, pinned)
		if !handled || err == nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
	})
}

func TestSelectionRejectionWithLegacyDisabledAndPinnedPlayground(t *testing.T) {
	initAuthTestDB(t)
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	row := &db.ChannelModel{ID: "empty-selection", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true, SelectedModelsRaw: `[]`}
	if err := db.SaveChannelModel(row); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/embeddings", "/v1/audio/speech", "/v1/images/generations", "/v1/images/jobs", "/v1/videos"} {
		t.Run(endpoint, func(t *testing.T) {
			engine := gin.New()
			registerV1Routes(engine.Group("/v1"))
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(`{"model":"blocked","prompt":"hello","input":"hello","voice":"alloy","messages":[{"role":"user","content":"hello"}]}`))
			request.Header.Set("Content-Type", "application/json")
			engine.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "model_not_available") {
				t.Fatalf("rejected request = %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
	for _, kind := range []string{"chat", "image", "video"} {
		t.Run("playground-"+kind, func(t *testing.T) {
			c, recorder := selectionRequestContext(http.MethodPost, "/api/playground/run", fmt.Sprintf(`{"channel_id":%q,"kind":%q,"model":"blocked"}`, row.ID, kind))
			handlePlaygroundRun(c)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "渠道未保存模型") {
				t.Fatalf("pinned playground = %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestCancelledSelectionKeepsSubmittedProfileTaskReadable(t *testing.T) {
	initAuthTestDB(t)
	row := &db.ChannelModel{ID: "existing-task-channel", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true, SelectedModelsRaw: `[]`}
	if err := db.SaveChannelModel(row); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "existing-selected-task", ChannelID: row.ID, Engine: "profile", TaskKind: asyncTaskKindVideo, Operation: "video.create", TaskStatus: "completed", TaskOutcome: "success", ProviderTaskID: "provider-selected-task", ResultBody: `{"id":"provider-selected-task","status":"completed"}`}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{LookupID: run.ProviderTaskID, TaskRunID: run.ID}); err != nil {
		t.Fatal(err)
	}
	c, _ := selectionRequestContext(http.MethodGet, "/v1/videos/"+run.ProviderTaskID, "")
	response, handled, err := profileTaskStatus(c, run.ProviderTaskID, asyncTaskKindVideo)
	if !handled || err != nil || response == nil {
		t.Fatalf("existing task response=%#v handled=%v err=%v", response, handled, err)
	}
}
