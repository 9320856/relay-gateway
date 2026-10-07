package router

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/service"
	"relay-gateway/task"
)

const legacyRetentionImageBytes = "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRlegacy retained image"
const legacyRetentionVideoBytes = "legacy retained video bytes"

func initLegacyRetentionIntegration(t *testing.T) media.ObjectStore {
	t.Helper()
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "legacy-media-retention-integration-key")
	for _, flag := range []string{"RELAY_DISABLE_LEGACY", "RELAY_ENABLE_PROFILE_ENGINE", "RELAY_ENABLE_PROFILE_IMAGE_ENGINE", "RELAY_ENABLE_PROFILE_DIRECT_ENGINE"} {
		t.Setenv(flag, "0")
	}
	initAsyncTaskRecoveryTestDB(t)
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	t.Cleanup(func() { SetMediaObjectStore(nil) })
	allowProfileMediaTestURLs(t)
	return store
}

func legacyRetentionEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(auditMiddleware())
	engine.POST("/v1/images/generations", handleImagesGenerations)
	engine.POST("/v1/images/edits", handleImagesEdits)
	engine.POST("/v1/images/jobs", handleCreateImageJob)
	engine.GET("/v1/images/jobs/:id", handleGetImageJob)
	engine.POST("/v1/videos", handleCreateVideo)
	engine.GET("/v1/videos/:id", handleGetVideo)
	engine.GET("/v1/videos/:id/content", handleGetVideoContent)
	engine.HEAD("/v1/videos/:id/content", handleGetVideoContent)
	engine.POST("/api/playground/run", handlePlaygroundRun)
	engine.GET("/api/playground/image-status", handlePlaygroundImageStatus)
	engine.GET("/api/playground/video-status", handlePlaygroundVideoStatus)
	engine.GET("/v1/media/:public_id/:capability", handleGetMediaAsset)
	engine.HEAD("/v1/media/:public_id/:capability", handleGetMediaAsset)
	return engine
}

func saveLegacyRetentionChannel(t *testing.T, id, base, policy string) *db.ChannelModel {
	t.Helper()
	channel := &db.ChannelModel{ID: id, Name: id, Type: "openai", BaseURL: base + "/v1", APIKey: "legacy-retention-upstream-key", Enabled: true, Priority: 100, Weight: 1, ModelsRaw: "legacy-retention-model", MediaRetention: policy}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	return channel
}

func legacyRetentionRequest(t *testing.T, engine http.Handler, method, target, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)
	return recorder
}

func legacyRetentionObject(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid JSON response: %d %s: %v", response.Code, response.Body.String(), err)
	}
	return payload
}

func legacyRetentionResponseURLs(payload map[string]any) []string {
	seen := make(map[string]bool)
	var urls []string
	var visit func(any)
	visit = func(value any) {
		switch current := value.(type) {
		case []any:
			for _, child := range current {
				visit(child)
			}
		case map[string]any:
			for key, child := range current {
				switch key {
				case "url", "video_url", "image_url", "b64_json":
					if raw, ok := child.(string); ok && raw != "" && !seen[raw] {
						urls = append(urls, raw)
						seen[raw] = true
					}
				default:
					visit(child)
				}
			}
		}
	}
	visit(payload)
	return urls
}

func assertLegacyRetainedContent(t *testing.T, engine http.Handler, rawURL, expected, mime string) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil || !strings.HasPrefix(parsed.Path, "/v1/media/") {
		t.Fatalf("expected managed media URL, got %q (%v)", rawURL, err)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		response := legacyRetentionRequest(t, engine, method, rawURL, "", "")
		if response.Code != http.StatusOK || response.Header().Get("Content-Type") != mime {
			t.Fatalf("managed %s = %d %s, headers=%v", method, response.Code, response.Body.String(), response.Header())
		}
		if method == http.MethodGet && response.Body.String() != expected {
			t.Fatalf("managed bytes = %q, want %q", response.Body.String(), expected)
		}
		if method == http.MethodHead && response.Body.Len() != 0 {
			t.Fatalf("managed HEAD wrote %d bytes", response.Body.Len())
		}
	}
	req := httptest.NewRequest(http.MethodGet, rawURL, nil)
	req.Header.Set("Range", "bytes=1-4")
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusPartialContent || recorder.Body.String() != expected[1:5] || recorder.Header().Get("Content-Range") != fmt.Sprintf("bytes 1-4/%d", len(expected)) {
		t.Fatalf("managed Range = %d %q, headers=%v", recorder.Code, recorder.Body.String(), recorder.Header())
	}
}

func TestLegacyChannelRetentionSynchronousImages(t *testing.T) {
	for _, policy := range []string{media.RetentionDisabled, media.RetentionBestEffort, media.RetentionRequired} {
		for _, endpoint := range []string{"generations", "playground", "json-edits", "multipart-edits"} {
			t.Run(policy+"/"+endpoint, func(t *testing.T) {
				initLegacyRetentionIntegration(t)
				var submits, downloads atomic.Int32
				inline := base64.StdEncoding.EncodeToString([]byte(legacyRetentionImageBytes))
				var first, second string
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet && (r.URL.Path == "/first.png" || r.URL.Path == "/second.png") {
						downloads.Add(1)
						if r.Header.Get("Authorization") != "" {
							t.Error("public image download received upstream credentials")
						}
						w.Header().Set("Content-Type", "image/png")
						_, _ = io.WriteString(w, legacyRetentionImageBytes)
						return
					}
					if r.Method != http.MethodPost || (r.URL.Path != "/v1/images/generations" && r.URL.Path != "/v1/images/edits") {
						t.Errorf("unexpected provider request: %s %s", r.Method, r.URL.Path)
						http.NotFound(w, r)
						return
					}
					submits.Add(1)
					if r.Header.Get("Authorization") != "Bearer legacy-retention-upstream-key" {
						t.Error("provider submission omitted selected channel credentials")
					}
					if endpoint == "multipart-edits" {
						if err := r.ParseMultipartForm(1 << 20); err != nil || r.FormValue("model") != "legacy-retention-model" {
							t.Errorf("multipart edit not forwarded: %v", err)
						}
						file, _, err := r.FormFile("image")
						if err != nil {
							t.Errorf("multipart image missing: %v", err)
						} else {
							contents, _ := io.ReadAll(file)
							_ = file.Close()
							if string(contents) != "input image" {
								t.Errorf("multipart input bytes = %q", contents)
							}
						}
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{"created": 12345, "data": []map[string]string{{"url": first, "revised_prompt": "keep prompt metadata"}, {"b64_json": inline}, {"url": second}}, "provider_metadata": map[string]any{"request_id": "preserve-provider-metadata", "preview": map[string]string{"url": first}}})
				}))
				t.Cleanup(upstream.Close)
				first, second = upstream.URL+"/first.png", upstream.URL+"/second.png"
				channel := saveLegacyRetentionChannel(t, "legacy-sync-channel", upstream.URL, policy)
				engine := legacyRetentionEngine()
				path, body, contentType := "/v1/images/generations", `{"model":"legacy-retention-model","prompt":"retain all images","n":3}`, "application/json"
				if endpoint == "playground" {
					path = "/api/playground/run"
					body = `{"kind":"image","channel_id":"` + channel.ID + `","model":"legacy-retention-model","prompt":"retain all images","n":3}`
				} else if strings.HasSuffix(endpoint, "edits") {
					path = "/v1/images/edits"
					if endpoint == "multipart-edits" {
						var buffer bytes.Buffer
						writer := multipart.NewWriter(&buffer)
						_ = writer.WriteField("model", "legacy-retention-model")
						_ = writer.WriteField("prompt", "edit all images")
						part, err := writer.CreateFormFile("image", "input.png")
						if err != nil {
							t.Fatal(err)
						}
						_, _ = io.WriteString(part, "input image")
						_ = writer.Close()
						body, contentType = buffer.String(), writer.FormDataContentType()
					}
				}
				response := legacyRetentionRequest(t, engine, http.MethodPost, "http://gateway.test"+path, body, contentType)
				if response.Code != http.StatusOK {
					t.Fatalf("image response = %d %s", response.Code, response.Body.String())
				}
				payload := legacyRetentionObject(t, response)
				var urls []string
				if endpoint == "playground" {
					if payload["status"] != "ok" || payload["type"] != "image" {
						t.Fatalf("Playground image result = %s", response.Body.String())
					}
					for _, item := range payload["images"].([]any) {
						urls = append(urls, item.(string))
					}
				} else {
					if payload["created"] != float64(12345) {
						t.Fatalf("created metadata changed: %s", response.Body.String())
					}
					data := payload["data"].([]any)
					if len(data) != 3 || data[0].(map[string]any)["revised_prompt"] != "keep prompt metadata" {
						t.Fatalf("image result or prompt metadata lost: %s", response.Body.String())
					}
					for _, item := range data {
						if raw, ok := item.(map[string]any)["url"].(string); ok {
							urls = append(urls, raw)
							continue
						}
						urls = append(urls, "data:image/png;base64,"+item.(map[string]any)["b64_json"].(string))
					}
					if strings.HasSuffix(endpoint, "edits") && payload["provider_metadata"].(map[string]any)["request_id"] != "preserve-provider-metadata" {
						t.Fatalf("provider metadata was lost: %s", response.Body.String())
					}
				}
				if len(urls) != 3 || submits.Load() != 1 {
					t.Fatalf("image count/submission count = %v/%d", urls, submits.Load())
				}
				var assets []db.MediaAsset
				if err := db.DB.Order("ordinal").Find(&assets).Error; err != nil {
					t.Fatal(err)
				}
				if policy == media.RetentionDisabled {
					if len(assets) != 0 || downloads.Load() != 0 || urls[0] != first || urls[1] != "data:image/png;base64,"+inline || urls[2] != second {
						t.Fatalf("disabled result = %v, assets=%+v downloads=%d", urls, assets, downloads.Load())
					}
					return
				}
				if len(assets) != 3 {
					t.Fatalf("retained assets = %+v, want three distinct media sources", assets)
				}
				if policy == media.RetentionBestEffort {
					if downloads.Load() != 0 || urls[0] != first || urls[2] != second || !strings.Contains(urls[1], inline) {
						t.Fatalf("best effort did not immediately return original sources: %v, downloads=%d", urls, downloads.Load())
					}
					for _, asset := range assets {
						if asset.Status != db.MediaAssetPending || asset.ObjectID != "" {
							t.Fatalf("best effort did synchronous materialization: %+v", asset)
						}
					}
					return
				}
				if downloads.Load() != 2 || strings.Contains(response.Body.String(), first) || strings.Contains(response.Body.String(), second) || strings.Contains(response.Body.String(), inline) {
					t.Fatalf("required response leaked a source or missed a download: %s, downloads=%d", response.Body.String(), downloads.Load())
				}
				for _, stable := range urls {
					assertLegacyRetainedContent(t, engine, stable, legacyRetentionImageBytes, "image/png")
				}
			})
		}
	}
}

func TestLegacyRequiredImageFetchFailureNeverResubmits(t *testing.T) {
	for _, endpoint := range []string{"generations", "json-edits", "multipart-edits", "playground"} {
		t.Run(endpoint, func(t *testing.T) {
			initLegacyRetentionIntegration(t)
			var primaryPosts, fallbackPosts, downloads atomic.Int32
			var source string
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					downloads.Add(1)
					http.Error(w, "temporary media failure", http.StatusServiceUnavailable)
					return
				}
				primaryPosts.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]string{{"url": source}}})
			}))
			t.Cleanup(primary.Close)
			source = primary.URL + "/unavailable.png"
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fallbackPosts.Add(1)
				http.Error(w, "fallback must not receive an accepted request", http.StatusInternalServerError)
			}))
			t.Cleanup(fallback.Close)
			channel := saveLegacyRetentionChannel(t, "legacy-required-primary", primary.URL, media.RetentionRequired)
			secondary := saveLegacyRetentionChannel(t, "legacy-required-fallback", fallback.URL, media.RetentionRequired)
			channel.Priority, secondary.Priority = 1, 100
			if err := db.SaveChannelModel(channel); err != nil {
				t.Fatal(err)
			}
			if err := db.SaveChannelModel(secondary); err != nil {
				t.Fatal(err)
			}
			path, body, contentType := "/v1/images/generations", `{"model":"legacy-retention-model","prompt":"one paid request"}`, "application/json"
			if strings.HasSuffix(endpoint, "edits") {
				path = "/v1/images/edits"
				if endpoint == "multipart-edits" {
					var buffer bytes.Buffer
					writer := multipart.NewWriter(&buffer)
					_ = writer.WriteField("model", "legacy-retention-model")
					part, _ := writer.CreateFormFile("image", "input.png")
					_, _ = io.WriteString(part, "input image")
					_ = writer.Close()
					body, contentType = buffer.String(), writer.FormDataContentType()
				}
			} else if endpoint == "playground" {
				path = "/api/playground/run"
				body = `{"kind":"image","channel_id":"` + channel.ID + `","model":"legacy-retention-model","prompt":"one paid request"}`
			}
			response := legacyRetentionRequest(t, legacyRetentionEngine(), http.MethodPost, "http://gateway.test"+path, body, contentType)
			if endpoint == "playground" {
				if legacyRetentionObject(t, response)["status"] != "error" {
					t.Fatalf("failed retention reported success: %s", response.Body.String())
				}
			} else if response.Code != http.StatusBadGateway {
				t.Fatalf("failed retention status = %d %s", response.Code, response.Body.String())
			}
			if primaryPosts.Load() != 1 || fallbackPosts.Load() != 0 || downloads.Load() != 1 || strings.Contains(response.Body.String(), source) {
				t.Fatalf("media failure retried provider submission or leaked source: primary=%d fallback=%d download=%d body=%s", primaryPosts.Load(), fallbackPosts.Load(), downloads.Load(), response.Body.String())
			}
		})
	}
}

func TestLegacyChannelRetentionAsyncTasksFreezeAndServeLocalMedia(t *testing.T) {
	for _, policy := range []string{media.RetentionDisabled, media.RetentionBestEffort, media.RetentionRequired} {
		for _, kind := range []string{"image", "video-url", "video-content"} {
			for _, retryFailure := range []bool{false, true} {
				if retryFailure && policy != media.RetentionRequired {
					continue
				}
				name := policy + "/" + kind
				if retryFailure {
					name += "/retry-download"
				}
				t.Run(name, func(t *testing.T) {
					store := initLegacyRetentionIntegration(t)
					var submits, providerPolls, downloads atomic.Int32
					var failNextDownload atomic.Bool
					failNextDownload.Store(retryFailure)
					const taskID = "legacy-retention-async-task"
					var source string
					isImage := kind == "image"
					providerPath := "/v1/videos"
					if isImage {
						providerPath = "/v1/images/jobs"
					}
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method == http.MethodGet && r.URL.Path == "/asset" {
							downloads.Add(1)
							if failNextDownload.Swap(false) {
								http.Error(w, "temporary media download failure", http.StatusServiceUnavailable)
								return
							}
							if r.Header.Get("Authorization") != "" {
								t.Error("public media request received channel API key")
							}
							if isImage {
								w.Header().Set("Content-Type", "image/png")
								_, _ = io.WriteString(w, legacyRetentionImageBytes)
							} else {
								w.Header().Set("Content-Type", "video/mp4")
								_, _ = io.WriteString(w, legacyRetentionVideoBytes)
							}
							return
						}
						if r.Header.Get("Authorization") != "Bearer legacy-retention-upstream-key" {
							t.Error("task request omitted pinned upstream credentials")
						}
						if r.Method == http.MethodGet && r.URL.Path == providerPath+"/"+taskID+"/content" {
							downloads.Add(1)
							if failNextDownload.Swap(false) {
								http.Error(w, "temporary provider content failure", http.StatusServiceUnavailable)
								return
							}
							if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
								t.Error("materializer forwarded a partial content request")
							}
							w.Header().Set("Content-Type", "video/mp4")
							_, _ = io.WriteString(w, legacyRetentionVideoBytes)
							return
						}
						w.Header().Set("Content-Type", "application/json")
						if r.Method == http.MethodPost && r.URL.Path == providerPath {
							submits.Add(1)
							_ = json.NewEncoder(w).Encode(map[string]string{"id": taskID, "status": "queued"})
							return
						}
						if r.Method == http.MethodGet && r.URL.Path == providerPath+"/"+taskID {
							providerPolls.Add(1)
							result := map[string]any{"id": taskID, "status": "completed"}
							if isImage {
								result["data"] = []map[string]string{{"url": source, "revised_prompt": "keep async metadata"}, {"b64_json": base64.StdEncoding.EncodeToString([]byte(legacyRetentionImageBytes))}}
							} else if kind == "video-url" {
								result["video_url"] = source
							}
							_ = json.NewEncoder(w).Encode(result)
							return
						}
						t.Errorf("unexpected async provider request: %s %s", r.Method, r.URL.Path)
						http.NotFound(w, r)
					}))
					t.Cleanup(upstream.Close)
					source = upstream.URL + "/asset"
					channel := saveLegacyRetentionChannel(t, "legacy-async-retention", upstream.URL, policy)
					engine := legacyRetentionEngine()
					created := legacyRetentionRequest(t, engine, http.MethodPost, "http://gateway.test"+providerPath, `{"model":"legacy-retention-model","prompt":"retain asynchronous result"}`, "application/json")
					wantCreate := http.StatusOK
					lookupID, taskKind := taskID, "video"
					if isImage {
						wantCreate, lookupID, taskKind = http.StatusAccepted, imageTaskIDPrefix+taskID, "image"
					}
					if created.Code != wantCreate || legacyRetentionObject(t, created)["status"] != "queued" {
						t.Fatalf("create = %d %s", created.Code, created.Body.String())
					}
					run, err := db.GetTaskRunByAlias(lookupID)
					if err != nil || run.MediaRetention != policy || run.Engine != "legacy" || run.ChannelID != channel.ID {
						t.Fatalf("creation did not freeze selected policy/channel: run=%+v err=%v", run, err)
					}
					channel.MediaRetention = media.RetentionDisabled
					if err := db.SaveChannelModel(channel); err != nil {
						t.Fatal(err)
					}
					pollURL := "http://gateway.test" + providerPath + "/" + taskID
					polled := legacyRetentionRequest(t, engine, http.MethodGet, pollURL, "", "")
					if polled.Code != http.StatusOK {
						t.Fatalf("completed provider status = %d %s", polled.Code, polled.Body.String())
					}
					payload := legacyRetentionObject(t, polled)
					assets, err := db.ListMediaAssetsForTaskRun(run.ID, taskKind)
					if err != nil {
						t.Fatal(err)
					}
					if policy == media.RetentionDisabled {
						if len(assets) != 0 || payload["status"] != "completed" || downloads.Load() != 0 {
							t.Fatalf("disabled async result=%s assets=%+v downloads=%d", polled.Body.String(), assets, downloads.Load())
						}
						return
					}
					wantAssets := 1
					if isImage {
						wantAssets = 2
					}
					if len(assets) != wantAssets || downloads.Load() != 0 {
						t.Fatalf("async result did not queue all media: assets=%+v downloads=%d", assets, downloads.Load())
					}
					if policy == media.RetentionRequired {
						if payload["status"] != "materializing" || len(legacyRetentionResponseURLs(payload)) != 0 || strings.Contains(polled.Body.String(), source) {
							t.Fatalf("required pending response exposed completed media: %s", polled.Body.String())
						}
						if !isImage {
							pendingContent := legacyRetentionRequest(t, engine, http.MethodGet, pollURL+"/content", "", "")
							if pendingContent.Code != http.StatusAccepted || downloads.Load() != 0 {
								t.Fatalf("pending required content = %d %s, downloads=%d", pendingContent.Code, pendingContent.Body.String(), downloads.Load())
							}
						}
					} else if payload["status"] != "completed" || (kind != "video-content" && !strings.Contains(polled.Body.String(), source)) {
						t.Fatalf("best effort did not return original completed result: %s", polled.Body.String())
					}
					poller := task.NewMediaMaterializationPoller("legacy-retention-worker", store)
					poller.Fetcher = media.HTTPSourceFetcher{URLValidator: func(string) error { return nil }}
					if retryFailure {
						if claimed, err := poller.RunOnce(context.Background()); !claimed || err == nil {
							t.Fatalf("controlled media download failure = %v %v", claimed, err)
						}
						failedStatus := legacyRetentionRequest(t, engine, http.MethodGet, pollURL, "", "")
						failedPayload := legacyRetentionObject(t, failedStatus)
						if failedStatus.Code != http.StatusOK || failedPayload["status"] != "materializing" || len(legacyRetentionResponseURLs(failedPayload)) != 0 || submits.Load() != 1 || providerPolls.Load() != 1 {
							t.Fatalf("download failure leaked media or repeated provider work: %d %s submit=%d polls=%d", failedStatus.Code, failedStatus.Body.String(), submits.Load(), providerPolls.Load())
						}
						failedAssets, err := db.ListMediaAssetsForTaskRun(run.ID, taskKind)
						if err != nil {
							t.Fatal(err)
						}
						failedCount := 0
						for _, asset := range failedAssets {
							if asset.Status == db.MediaAssetFailed {
								failedCount++
								if err := db.RetryMediaAssetMaterialization(asset.ID); err != nil {
									t.Fatal(err)
								}
							}
						}
						if failedCount != 1 {
							t.Fatalf("failed media asset was not durably retryable: %+v", failedAssets)
						}
					}
					for index := 0; index < wantAssets; index++ {
						claimed, err := poller.RunOnce(context.Background())
						if err != nil || !claimed {
							t.Fatalf("materialization step %d = %v, %v", index, claimed, err)
						}
						if index == 0 && isImage && policy == media.RetentionRequired {
							partial := legacyRetentionRequest(t, engine, http.MethodGet, pollURL, "", "")
							partialPayload := legacyRetentionObject(t, partial)
							if partial.Code != http.StatusOK || partialPayload["status"] != "materializing" || len(legacyRetentionResponseURLs(partialPayload)) != 0 {
								t.Fatalf("partial image batch exposed media: %d %s", partial.Code, partial.Body.String())
							}
						}
					}
					if claimed, err := poller.RunOnce(context.Background()); claimed || err != nil {
						t.Fatalf("completed jobs remained claimable: %v %v", claimed, err)
					}
					complete := legacyRetentionRequest(t, engine, http.MethodGet, pollURL, "", "")
					completed := legacyRetentionObject(t, complete)
					if complete.Code != http.StatusOK || completed["status"] != "completed" || strings.Contains(complete.Body.String(), source) {
						t.Fatalf("managed completed task = %d %s", complete.Code, complete.Body.String())
					}
					if completed["id"] != taskID || (!isImage && completed["task_id"] != taskID) {
						t.Fatalf("managed result changed task identity: %s", complete.Body.String())
					}
					stableURLs := legacyRetentionResponseURLs(completed)
					if len(stableURLs) != wantAssets {
						t.Fatalf("completed result URLs = %v, want %d; body=%s", stableURLs, wantAssets, complete.Body.String())
					}
					for _, stable := range stableURLs {
						if isImage {
							assertLegacyRetainedContent(t, engine, stable, legacyRetentionImageBytes, "image/png")
						} else {
							assertLegacyRetainedContent(t, engine, stable, legacyRetentionVideoBytes, "video/mp4")
						}
					}
					playgroundPath := "video-status"
					if isImage {
						playgroundPath = "image-status"
					}
					playground := legacyRetentionRequest(t, engine, http.MethodGet, "http://gateway.test/api/playground/"+playgroundPath+"?task_id="+taskID+"&channel_id=wrong-channel", "", "")
					if playground.Code != http.StatusOK || legacyRetentionObject(t, playground)["task_status"] != "completed" || strings.Contains(playground.Body.String(), source) || !strings.Contains(playground.Body.String(), "/v1/media/") {
						t.Fatalf("Playground managed status = %d %s", playground.Code, playground.Body.String())
					}
					if !isImage {
						content := legacyRetentionRequest(t, engine, http.MethodGet, pollURL+"/content", "", "")
						if content.Code != http.StatusOK || content.Body.String() != legacyRetentionVideoBytes {
							t.Fatalf("mapped local content = %d %q", content.Code, content.Body.String())
						}
					}
					wantDownloads := int32(1)
					if retryFailure {
						wantDownloads = 2
					}
					if submits.Load() != 1 || providerPolls.Load() != 1 || downloads.Load() != wantDownloads {
						t.Fatalf("local media/status repeated provider work: submit=%d poll=%d download=%d", submits.Load(), providerPolls.Load(), downloads.Load())
					}
				})
			}
		}
	}
}

func TestLegacyChannelRetentionRecoveryJournalFreezesPolicy(t *testing.T) {
	initLegacyRetentionIntegration(t)
	channel := saveLegacyRetentionChannel(t, "legacy-journal-retention", "http://127.0.0.1:1", media.RetentionRequired)
	previousPersist := persistAsyncTaskMappingsFn
	persistAsyncTaskMappingsFn = func(*gin.Context, string, string, string, string, ...string) error {
		return errors.New("controlled mapping write outage")
	}
	t.Cleanup(func() { persistAsyncTaskMappingsFn = previousPersist })
	ctx, recorder := selectionRequestContext(http.MethodPost, "/v1/videos", "")
	upstream := channel.ToUpstreamChannel()
	freezeLegacyChannelPolicy(ctx, &upstream)
	if err := registerAsyncTaskMappings(ctx, channel.ID, "video", "retained-journal-task", "queued", "retained-journal-task"); err != nil {
		t.Fatal(err)
	}
	if recorder.Header().Get("X-Relay-Task-Mapping") != "pending" {
		t.Fatalf("recovery journal was not durable: headers=%v", recorder.Header())
	}
	journal, err := os.ReadFile(asyncTaskMappingRecoveryJournalPath())
	if err != nil || !bytes.Contains(journal, []byte(`"media_retention":"required"`)) {
		t.Fatalf("journal omitted selected retention policy: %s, %v", journal, err)
	}
	persistAsyncTaskMappingsFn = previousPersist
	channel.MediaRetention = media.RetentionDisabled
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	asyncTaskMappingRecoveries = asyncTaskMappingRecoveryQueue{}
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatal(err)
	}
	run, err := legacyMediaRun(context.Background(), "retained-journal-task", "video")
	if err != nil || run.MediaRetention != media.RetentionRequired {
		t.Fatalf("replayed task lost frozen retention: %+v, %v", run, err)
	}
	// Journals written before the field existed retain disabled behavior even
	// when the channel is later switched to required retention.
	channel.MediaRetention = media.RetentionRequired
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	old := asyncTaskMappingRegistration{ChannelID: channel.ID, TaskKind: "video", TaskAlias: "historical-journal-task", InitialStatus: "queued", TaskIDs: []string{"historical-journal-task"}}
	if journaled, err := enqueueAsyncTaskMappingRecovery(old); err != nil || !journaled {
		t.Fatalf("enqueue historical journal: %v %v", journaled, err)
	}
	asyncTaskMappingRecoveries = asyncTaskMappingRecoveryQueue{}
	if err := ReconcilePendingAsyncTaskMappings(context.Background()); err != nil {
		t.Fatal(err)
	}
	historical, err := legacyMediaRun(context.Background(), old.TaskAlias, "video")
	if err != nil || historical.MediaRetention != media.RetentionDisabled {
		t.Fatalf("historical task adopted current channel retention: %+v, %v", historical, err)
	}
}
