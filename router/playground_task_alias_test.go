package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/service"
)

func TestPlaygroundStatusResolvesGatewayTaskIDsAfterCacheReset(t *testing.T) {
	initAsyncTaskRecoveryTestDB(t)
	for _, kind := range []string{asyncTaskKindVideo, asyncTaskKindImage} {
		t.Run(kind, func(t *testing.T) {
			const providerID = "shared-provider-task"
			gatewayID := "gt_playground_" + kind
			path := "/v1/videos/" + providerID
			if kind == asyncTaskKindImage {
				path = "/v1/images/jobs/" + providerID
			}
			upstreamCalls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls++
				if r.Method != http.MethodGet || r.URL.Path != path {
					t.Errorf("upstream request = %s %s, want GET %s", r.Method, r.URL.Path, path)
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if kind == asyncTaskKindImage {
					_, _ = w.Write([]byte(`{"job":{"id":"shared-provider-task","status":"completed","assets":[{"url":"https://cdn.example/image.png"}]}}`))
				} else {
					_, _ = w.Write([]byte(`{"id":"shared-provider-task","task_id":"shared-provider-task","status":"completed"}`))
				}
			}))
			defer upstream.Close()
			channel := &db.ChannelModel{ID: "playground-alias-" + kind, Name: "alias", Type: "openai", BaseURL: upstream.URL + "/v1", Enabled: true, Priority: 1, Weight: 1, ModelsRaw: "alias-model"}
			if err := db.SaveChannelModel(channel); err != nil {
				t.Fatal(err)
			}
			defer service.DefaultDispatcher.RemoveChannel(channel.ID)
			lookupID, conflictingID := gatewayID, providerID
			if kind == asyncTaskKindImage {
				lookupID, conflictingID = imageTaskIDPrefix+lookupID, imageTaskIDPrefix+conflictingID
			}
			if err := db.EnsureTaskMappings(
				db.TaskMapping{TaskID: conflictingID, ChannelID: "other-channel", TaskKind: kind, TaskAlias: providerID},
				db.TaskMapping{TaskID: lookupID, ChannelID: channel.ID, TaskKind: kind, TaskAlias: providerID},
			); err != nil {
				t.Fatal(err)
			}
			db.ClearTaskMappingCache()
			engine := gin.New()
			if kind == asyncTaskKindVideo {
				engine.GET("/status", handlePlaygroundVideoStatus)
			} else {
				engine.GET("/status", handlePlaygroundImageStatus)
			}
			for attempt := 0; attempt < 2; attempt++ {
				recorder := httptest.NewRecorder()
				engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/status?task_id="+gatewayID+"&channel_id=other-channel", nil))
				var result map[string]any
				if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if recorder.Code != http.StatusOK || result["status"] != "ok" || result["task_status"] != "completed" || result["task_id"] != gatewayID || result["channel_id"] != channel.ID {
					t.Fatalf("unexpected status response: %d %s", recorder.Code, recorder.Body.String())
				}
				if kind == asyncTaskKindVideo && !strings.HasSuffix(result["video_url"].(string), "/api/playground/video-content/"+gatewayID) {
					t.Fatalf("video content link lost gateway ID: %v", result)
				}
				if kind == asyncTaskKindImage {
					response := result["response"].(map[string]any)
					if response["job"].(map[string]any)["id"] != gatewayID {
						t.Fatalf("image response exposed conflicting provider ID: %v", result)
					}
				}
			}
			if upstreamCalls != 2 {
				t.Fatalf("upstream calls = %d, want 2", upstreamCalls)
			}
			if mapping := db.GetTaskMappingForKind(conflictingID, kind); mapping == nil || mapping.ChannelID != "other-channel" {
				t.Fatalf("conflicting task mapping changed: %+v", mapping)
			}
			if mapping := db.GetTaskMappingForKind(lookupID, kind); mapping == nil || mapping.TaskAlias != providerID || mapping.ChannelID != channel.ID {
				t.Fatalf("gateway task mapping changed: %+v", mapping)
			}
		})
	}
}
