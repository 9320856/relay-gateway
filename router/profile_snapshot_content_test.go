package router

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

func TestArchivedProfileContentAndRetentionSurviveCatalogReuseAndRestart(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, retention := range []string{protocol.MediaRetentionBestEffort, protocol.MediaRetentionRequired} {
		for _, deleteProfile := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_delete_profile_%t", retention, deleteProfile), func(t *testing.T) {
				t.Setenv("RELAY_DB_ENCRYPTION_KEY", "archived-profile-content-test-key")
				path := filepath.Join(t.TempDir(), "archive-content.db")
				if err := db.InitDB(path); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.URL.Path != "/v1/original/provider-archived/content" || r.Header.Get("Authorization") != "Bearer archived-key" {
						t.Errorf("historical task changed its content operation: %s auth matches=%v", r.URL.Path, r.Header.Get("Authorization") == "Bearer archived-key")
						http.Error(w, "wrong content operation", http.StatusBadRequest)
						return
					}
					w.Header().Set("Content-Type", "video/mp4")
					_, _ = io.WriteString(w, "original-provider-video")
				}))
				t.Cleanup(upstream.Close)
				channel := &db.ChannelModel{ID: "archive-content-channel", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "archived-key", Enabled: true}
				if err := db.SaveChannelModel(channel); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
				profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
					Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: retention,
					Submit: protocol.Submit{Method: http.MethodPost, Path: "/original", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
					Poll:    &protocol.Poll{Method: http.MethodGet, Path: "/original/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
					Content: &protocol.Content{Method: http.MethodGet, Path: "/original/{task_id}/content"},
				}}}
				compiled, err := protocol.Compile(profile)
				if err != nil {
					t.Fatal(err)
				}
				const profileID = "reusable-profile-id"
				createProfile := func() {
					t.Helper()
					if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: profileID, Source: db.ProfileSourceCustom}); err != nil {
						t.Fatal(err)
					}
				}
				saveRevision := func(compiled protocol.CompiledProfile) {
					t.Helper()
					if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: profileID, Revision: 1, SchemaVersion: protocol.CurrentSchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
						t.Fatal(err)
					}
				}
				createProfile()
				saveRevision(compiled)
				run := &db.TaskRun{ID: "archive-content-run", TaskKind: asyncTaskKindVideo, Engine: "profile", ChannelID: channel.ID, Operation: "video.create", ProfileID: profileID, ProfileRevision: 1, ProfileDigest: compiled.Digest(), ProviderTaskID: "provider-archived", PollingMode: protocol.PollingClient, TaskStatus: model.VideoStatusCompleted, TaskOutcome: "success", ResultBody: `{"id":"provider-archived","status":"completed","video_url":"https://signed.example/private-provider.mp4"}`}
				if err := db.CreateTaskRun(run); err != nil {
					t.Fatal(err)
				}
				if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: run.ProviderTaskID}); err != nil {
					t.Fatal(err)
				}
				if deleteProfile {
					err = db.DeleteProtocolProfile(profileID)
				} else {
					err = db.DeleteProtocolProfileRevision(profileID, 1)
				}
				if err != nil {
					t.Fatal(err)
				}
				// Reusing the same catalog identity must not replace historical
				// content paths or weaken the task's captured retention policy.
				if deleteProfile {
					createProfile()
				}
				profile.Operations[0].Content.Path = "/replacement/{task_id}/content"
				profile.Operations[0].MediaRetention = protocol.MediaRetentionDisabled
				replacement, err := protocol.Compile(profile)
				if err != nil {
					t.Fatal(err)
				}
				saveRevision(replacement)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				if err := db.InitDB(path); err != nil {
					t.Fatal(err)
				}
				engine := gin.New()
				registerPublicVideoContentRoutes(engine.Group("/v1"))
				content := func() {
					t.Helper()
					rec := httptest.NewRecorder()
					engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/videos/provider-archived/content", nil))
					if retention == protocol.MediaRetentionRequired {
						if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "materializing") {
							t.Errorf("required content escaped retention: %d %s", rec.Code, rec.Body.String())
						}
					} else if rec.Code != http.StatusOK || rec.Body.String() != "original-provider-video" {
						t.Errorf("historical content = %d %q", rec.Code, rec.Body.String())
					}
				}
				content()
				if retention == protocol.MediaRetentionBestEffort {
					var wg sync.WaitGroup
					for range 16 {
						wg.Go(content)
					}
					wg.Wait()
				} else {
					if calls.Load() != 0 {
						t.Fatal("required content directly contacted provider")
					}
					statusContext, _ := gin.CreateTestContext(httptest.NewRecorder())
					statusContext.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/provider-archived", nil)
					payload, handled, err := profileTaskStatus(statusContext, run.ProviderTaskID, asyncTaskKindVideo)
					if err != nil || !handled {
						t.Fatalf("historical status: handled=%v, %v", handled, err)
					}
					encoded, _ := json.Marshal(payload)
					if strings.Contains(string(encoded), "private-provider.mp4") || !strings.Contains(string(encoded), "materializing") {
						t.Fatalf("historical status exposed required provider media: %s", encoded)
					}
					if err := db.DB.Model(&db.ProtocolProfileSnapshot{}).Where("content_digest = ?", compiled.Digest()).Update("content_json", `{}`).Error; err != nil {
						t.Fatal(err)
					}
					payload, handled, err = profileTaskStatus(statusContext, run.ProviderTaskID, asyncTaskKindVideo)
					if err != nil || !handled {
						t.Fatalf("corrupt snapshot status: handled=%v, %v", handled, err)
					}
					encoded, _ = json.Marshal(payload)
					if strings.Contains(string(encoded), "private-provider.mp4") {
						t.Fatalf("unknown retention exposed provider media: %s", encoded)
					}
				}
			})
		}
	}
}
