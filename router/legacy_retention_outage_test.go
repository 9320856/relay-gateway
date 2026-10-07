package router

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/media"
)

func TestLegacyRetentionCreatePreservesAcceptanceDuringProjectionOutage(t *testing.T) {
	for _, policy := range []string{media.RetentionRequired, media.RetentionBestEffort} {
		for _, kind := range []string{"video", "image"} {
			for _, journaled := range []bool{false, true} {
				name := policy + "/" + kind + "/journal-failed"
				if journaled {
					name = policy + "/" + kind + "/journal-durable"
				}
				t.Run(name, func(t *testing.T) {
					initLegacyRetentionIntegration(t)
					var submissions atomic.Int32
					const providerURL = "https://cdn.provider.net/generated-result.mp4"
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						submissions.Add(1)
						wantPath := "/v1/videos"
						if kind == "image" {
							wantPath = "/v1/images/jobs"
						}
						if r.Method != http.MethodPost || r.URL.Path != wantPath {
							t.Errorf("unexpected provider operation %s %s", r.Method, r.URL.Path)
						}
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"outage-provider-task","task_id":"outage-provider-task","status":"completed","video_url":"`+providerURL+`","url":"`+providerURL+`","data":[{"url":"`+providerURL+`"}]}`)
					}))
					t.Cleanup(upstream.Close)
					saveLegacyRetentionChannel(t, "outage-channel", upstream.URL, policy)
					previousPersist, previousEnqueue := persistAsyncTaskMappingsFn, enqueueAsyncTaskMappingRecoveryFn
					persistAsyncTaskMappingsFn = func(*gin.Context, string, string, string, string, ...string) error {
						return errors.New("simulated mapping outage")
					}
					if !journaled {
						enqueueAsyncTaskMappingRecoveryFn = func(asyncTaskMappingRegistration) (bool, error) {
							return false, errors.New("simulated journal outage")
						}
					}
					t.Cleanup(func() {
						persistAsyncTaskMappingsFn, enqueueAsyncTaskMappingRecoveryFn = previousPersist, previousEnqueue
					})
					// Keep TaskRun writes unavailable after a durable journal publishes
					// its cache entry, too. Read and audit operations still succeed.
					const callback = "test:retention_projection_outage"
					if err := db.DB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
						if tx.Statement.Table == "async_task_runs" {
							tx.AddError(errors.New("simulated task projection outage"))
						}
					}); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = db.DB.Callback().Create().Remove(callback) })
					path, wantCode := "/v1/videos", http.StatusOK
					if kind == "image" {
						path, wantCode = "/v1/images/jobs", http.StatusAccepted
					}
					response := legacyRetentionRequest(t, legacyRetentionEngine(), http.MethodPost, "http://gateway.test"+path, `{"model":"legacy-retention-model","prompt":"accepted once"}`, "application/json")
					if response.Code != wantCode || submissions.Load() != 1 {
						t.Fatalf("accepted create became a failed or repeated submit: status=%d submits=%d body=%s", response.Code, submissions.Load(), response.Body.String())
					}
					payload := legacyRetentionObject(t, response)
					if payload["id"] != "outage-provider-task" {
						t.Fatalf("accepted task identifier was lost: %s", response.Body.String())
					}
					wantHeader := "failed"
					if journaled {
						wantHeader = "pending"
					}
					if response.Header().Get("X-Relay-Task-Mapping") != wantHeader {
						t.Fatalf("mapping recovery state = %q, want %q", response.Header().Get("X-Relay-Task-Mapping"), wantHeader)
					}
					if policy == media.RetentionRequired {
						if payload["status"] != "materializing" || len(legacyRetentionResponseURLs(payload)) != 0 {
							t.Fatalf("required media escaped before local storage: %s", response.Body.String())
						}
					} else if payload["status"] != "completed" || !strings.Contains(response.Body.String(), providerURL) {
						t.Fatalf("best-effort changed the accepted provider result: %s", response.Body.String())
					}
					var runCount int64
					if err := db.DB.Model(&db.TaskRun{}).Count(&runCount).Error; err != nil || runCount != 0 {
						t.Fatalf("outage did not keep TaskRun projection unavailable: count=%d err=%v", runCount, err)
					}
				})
			}
		}
	}
}

func TestLegacyRetentionBestEffortPreservesNonJSONAndOverflowResponses(t *testing.T) {
	for _, body := range []string{"provider returned a successful non-JSON payload", `["successful","provider","array"]`, `null`} {
		t.Run(body, func(t *testing.T) {
			for _, limit := range []int64{4, 1024} {
				recorder := httptest.NewRecorder()
				ctx, _ := gin.CreateTestContext(recorder)
				ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", nil)
				captured := newBoundedMediaResponseWriter(limit)
				captured.fallback = ctx.Writer
				captured.Header().Set("Content-Type", "text/plain")
				captured.Header().Set("X-Provider-Meta", "preserve-me")
				captured.WriteHeader(http.StatusCreated)
				// Splitting writes exercises replay of the already buffered prefix.
				if _, err := captured.Write([]byte(body[:2])); err != nil {
					t.Fatal(err)
				}
				if _, err := captured.Write([]byte(body[2:])); err != nil {
					t.Fatalf("best-effort capture overflow became a provider error: %v", err)
				}
				channel := (&db.ChannelModel{ID: "capture-channel", MediaRetention: media.RetentionBestEffort}).ToUpstreamChannel()
				if err := finishLegacyCapturedMedia(ctx, captured, &channel, "image"); err != nil {
					t.Fatalf("best-effort capture finalization failed at limit=%d: %v", limit, err)
				}
				if recorder.Code != http.StatusCreated || recorder.Body.String() != body || recorder.Header().Get("X-Provider-Meta") != "preserve-me" {
					t.Fatalf("successful response changed at limit=%d: status=%d body=%q headers=%v", limit, recorder.Code, recorder.Body.String(), recorder.Header())
				}
			}
		})
	}
}
