package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
	"relay-gateway/task"
)

func saveChannelRetentionProfile(t *testing.T, channel *db.ChannelModel, op protocol.Operation) protocol.CompiledProfile {
	t.Helper()
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	compiled, err := protocol.Compile(protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{op}})
	if err != nil {
		t.Fatal(err)
	}
	profileID := "channel-retention-" + channel.ID
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: profileID, Name: profileID, Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: profileID, Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: op.Operation, ModelPattern: "media-model", ProfileID: profileID, ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestProfileDirectMediaUsesChannelPolicy(t *testing.T) {
	for _, flow := range []string{"image", "video", "edits_json", "edits_multipart", "edits_playground"} {
		for _, policy := range []string{protocol.MediaRetentionDisabled, protocol.MediaRetentionBestEffort, protocol.MediaRetentionRequired} {
			t.Run(flow+"/"+policy, func(t *testing.T) {
				t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-channel-retention-test-key")
				t.Setenv("RELAY_PROFILE_MEDIA_DISABLED", "1")
				t.Setenv("RELAY_PROFILE_MEDIA_REQUIRED", "1")
				if err := db.InitDB(t.TempDir() + "/direct.db"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { SetMediaObjectStore(nil); _ = db.Close() })
				store, err := media.NewLocalObjectStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				SetMediaObjectStore(store)
				kind, operation, resultPath := "image", "image.create", "data"
				responseBody := `{"data":[{"b64_json":"iVBORw0KGgo=","revised_prompt":"preserve metadata"}]}`
				if flow == "video" {
					kind, operation, resultPath = "video", "video.create", "video_url"
					responseBody = `{"status":"completed","video_url":"https://cdn.future-provider.net/result.mp4"}`
				} else if strings.HasPrefix(flow, "edits_") {
					operation = "images.edits"
				}
				submits := 0
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					submits++
					if r.Method != http.MethodPost || r.URL.Path != "/v1/media" {
						t.Errorf("unexpected provider submit: %s %s", r.Method, r.URL.Path)
					}
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Length", fmt.Sprint(len(responseBody)))
					_, _ = io.WriteString(w, responseBody)
				}))
				t.Cleanup(upstream.Close)
				channel := &db.ChannelModel{ID: "direct-media", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "media-model", MediaRetention: policy}
				oldPolicy := protocol.MediaRetentionRequired
				if policy == protocol.MediaRetentionRequired {
					oldPolicy = protocol.MediaRetentionDisabled
				}
				op := protocol.Operation{Operation: operation, ExecutionMode: protocol.ExecutionDirect, PollingMode: protocol.PollingOff, MediaRetention: oldPolicy,
					Submit: protocol.Submit{Method: http.MethodPost, Path: "/media", BodyEncoding: "json"}, Response: protocol.Response{ResultPaths: []string{resultPath}}}
				compiled := saveChannelRetentionProfile(t, channel, op)
				fetchCalls := 0
				previousFetcher := profileMediaFetcherFactory
				profileMediaFetcherFactory = func(string, string) media.SourceFetcher {
					return media.SourceFetcherFunc(func(context.Context, media.MediaResult) (media.FetchedSource, error) {
						fetchCalls++
						return media.FetchedSource{Body: io.NopCloser(strings.NewReader("video bytes")), ContentType: "video/mp4"}, nil
					})
				}
				t.Cleanup(func() { profileMediaFetcherFactory = previousFetcher })
				gin.SetMode(gin.TestMode)
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "http://gateway.example/v1/images/edits", strings.NewReader(`{"model":"media-model","prompt":"edit"}`))
				c.Request.Header.Set("Idempotency-Key", "direct-media-key")
				selected := channel.ToUpstreamChannel()
				var encoded []byte
				switch flow {
				case "image":
					response, _, handled, execErr := profileEngineImageCreateForChannel(c, &model.ImageGenerationRequest{Model: "media-model", Prompt: "test"}, &selected)
					if execErr != nil || !handled {
						t.Fatalf("image handled=%v err=%v", handled, execErr)
					}
					encoded, _ = json.Marshal(response)
				case "video":
					response, _, handled, execErr := profileEngineVideoCreateForChannel(c, &model.VideoGenerationRequest{Model: "media-model", Prompt: "test"}, &selected)
					if execErr != nil || !handled {
						t.Fatalf("video handled=%v err=%v", handled, execErr)
					}
					encoded, _ = json.Marshal(response)
				case "edits_multipart":
					var input bytes.Buffer
					mw := multipart.NewWriter(&input)
					_ = mw.WriteField("model", "media-model")
					part, _ := mw.CreateFormFile("image", "original.png")
					_, _ = io.WriteString(part, "original image bytes")
					_ = mw.Close()
					c.Request = httptest.NewRequest(http.MethodPost, "http://gateway.example/v1/images/edits", &input)
					c.Request.Header.Set("Content-Type", mw.FormDataContentType())
					if err := c.Request.ParseMultipartForm(1 << 20); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = c.Request.MultipartForm.RemoveAll() })
					handled, execErr := profileEngineMultipartDirect(c, operation, c.Request.MultipartForm, &selected)
					if execErr != nil || !handled {
						t.Fatalf("multipart handled=%v err=%v", handled, execErr)
					}
					encoded = recorder.Body.Bytes()
				default:
					writer := http.ResponseWriter(c.Writer)
					if flow == "edits_playground" {
						writer = recorder
					}
					handled, _, execErr := profileEngineDirectForChannelResult(c, operation, "", &selected, writer, func() bool { return recorder.Body.Len() > 0 })
					if execErr != nil || !handled {
						t.Fatalf("JSON handled=%v err=%v", handled, execErr)
					}
					encoded = recorder.Body.Bytes()
				}
				if submits != 1 {
					t.Fatalf("paid submits=%d", submits)
				}
				var assets []db.MediaAsset
				if err := db.DB.Find(&assets).Error; err != nil {
					t.Fatal(err)
				}
				if policy == protocol.MediaRetentionDisabled {
					if len(assets) != 0 || fetchCalls != 0 || strings.Contains(string(encoded), "/v1/media/") {
						t.Fatalf("disabled retained media: assets=%+v fetch=%d body=%s", assets, fetchCalls, encoded)
					}
				} else if len(assets) != 1 || assets[0].Kind != kind {
					t.Fatalf("media assets=%+v", assets)
				} else if policy == protocol.MediaRetentionBestEffort {
					if assets[0].Status != db.MediaAssetPending || fetchCalls != 0 || strings.Contains(string(encoded), "/v1/media/") {
						t.Fatalf("best effort blocked/rewrote response: assets=%+v fetch=%d body=%s", assets, fetchCalls, encoded)
					}
				} else if assets[0].Status != db.MediaAssetAvailable || !strings.Contains(string(encoded), "/v1/media/") || strings.Contains(string(encoded), "b64_json") || strings.Contains(string(encoded), "cdn.future-provider.net") {
					t.Fatalf("required response not managed: assets=%+v body=%s", assets, encoded)
				}
				if strings.HasPrefix(flow, "edits_") && !strings.Contains(string(encoded), "preserve metadata") {
					t.Fatalf("edit metadata lost: %s", encoded)
				}
				revision, err := db.GetProtocolProfileRevision("channel-retention-"+channel.ID, 1)
				if err != nil || revision.ContentDigest != compiled.Digest() || revision.ContentJSON != string(compiled.CanonicalJSON()) {
					t.Fatalf("channel override mutated immutable profile: revision=%+v err=%v", revision, err)
				}
			})
		}
	}
}

func TestProfileEditsRequiredMediaFailureDoesNotFailOver(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-edit-failure-test-key")
	if err := db.InitDB(t.TempDir() + "/failure.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetMediaObjectStore(nil); _ = db.Close() })
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	submits := [2]int{}
	for index := range submits {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			submits[index]++
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"data":[{"url":"https://cdn.provider.net/result.png"}]}`)
		}))
		t.Cleanup(upstream.Close)
		channel := &db.ChannelModel{ID: fmt.Sprint("edit-", index), Type: "newapi", BaseURL: upstream.URL, Enabled: true, ModelsRaw: "media-model", Priority: 10 - index, MediaRetention: protocol.MediaRetentionRequired}
		saveChannelRetentionProfile(t, channel, protocol.Operation{Operation: "images.edits", ExecutionMode: protocol.ExecutionDirect, PollingMode: protocol.PollingOff, Submit: protocol.Submit{Method: http.MethodPost, Path: "/edits", BodyEncoding: "json"}})
	}
	previousFetcher := profileMediaFetcherFactory
	fetchCalls := 0
	profileMediaFetcherFactory = func(string, string) media.SourceFetcher {
		return media.SourceFetcherFunc(func(context.Context, media.MediaResult) (media.FetchedSource, error) {
			fetchCalls++
			return media.FetchedSource{}, &protocol.ExecutorError{HTTPStatus: http.StatusTooManyRequests, Cause: errors.New("media download unavailable")}
		})
	}
	t.Cleanup(func() { profileMediaFetcherFactory = previousFetcher })
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{"model":"media-model","prompt":"edit"}`))
	handled, err := profileEngineDirect(c, "images.edits", "")
	var mediaErr *protocol.ExecutorError
	if !handled || !errors.As(err, &mediaErr) || submits[0]+submits[1] != 1 || fetchCalls != 1 || recorder.Body.Len() != 0 {
		t.Fatalf("required media re-entered submit failover: handled=%v err=%v submits=%v fetch_calls=%d body=%s", handled, err, submits, fetchCalls, recorder.Body.String())
	}
}

func TestProfileAsyncChannelRetentionIsFrozenForStatusContentAndReplay(t *testing.T) {
	for _, polling := range []string{protocol.PollingClient, protocol.PollingBackground} {
		for _, policy := range []string{protocol.MediaRetentionDisabled, protocol.MediaRetentionBestEffort, protocol.MediaRetentionRequired} {
			t.Run(polling+"/"+policy, func(t *testing.T) {
				t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-frozen-retention-test-key")
				t.Setenv("RELAY_PROFILE_MEDIA_DISABLED", "1")
				t.Setenv("RELAY_PROFILE_MEDIA_REQUIRED", "1")
				if err := db.InitDB(t.TempDir() + "/frozen.db"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				submits, polls, contents := 0, 0, 0
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == http.MethodPost:
						submits++
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"frozen-provider-task","status":"queued"}`)
					case strings.HasSuffix(r.URL.Path, "/content"):
						contents++
						w.Header().Set("Content-Type", "video/mp4")
						_, _ = io.WriteString(w, "provider video bytes")
					default:
						polls++
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"frozen-provider-task","status":"completed","video_url":"https://cdn.future-provider.net/frozen.mp4"}`)
					}
				}))
				t.Cleanup(upstream.Close)
				channel := &db.ChannelModel{ID: "frozen-media", Type: "newapi", BaseURL: upstream.URL + "/v1", Enabled: true, ModelsRaw: "media-model", MediaRetention: policy}
				snapshotPolicy := protocol.MediaRetentionRequired
				if policy == protocol.MediaRetentionRequired {
					snapshotPolicy = protocol.MediaRetentionDisabled
				}
				op := protocol.Operation{Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: polling, MediaRetention: snapshotPolicy,
					Submit: protocol.Submit{Method: http.MethodPost, Path: "/tasks", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
					Poll:    &protocol.Poll{Method: http.MethodGet, Path: "/tasks/{task_id}", IntervalMS: 1, MaxAttempts: 10, MaxDurationMS: 60000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
					Content: &protocol.Content{Method: http.MethodGet, Path: "/tasks/{task_id}/content"}}
				compiled := saveChannelRetentionProfile(t, channel, op)
				create := mediaRegressionContext(http.MethodPost, "http://gateway.example", "/v1/videos", "frozen-media-key")
				req := &model.VideoGenerationRequest{Model: "media-model", Prompt: "test"}
				response, _, handled, err := profileEngineVideoCreate(create, req)
				if !handled || err != nil || response.TaskID != "frozen-provider-task" {
					t.Fatalf("create handled=%v response=%+v err=%v", handled, response, err)
				}
				run, err := db.GetTaskRunByAlias(response.TaskID)
				if err != nil || run.MediaRetention != policy || run.ProfileDigest != compiled.Digest() {
					t.Fatalf("frozen task=%+v err=%v", run, err)
				}
				channel.MediaRetention = protocol.MediaRetentionRequired
				if policy == protocol.MediaRetentionRequired {
					channel.MediaRetention = protocol.MediaRetentionDisabled
				}
				if err := db.SaveChannelModel(channel); err != nil {
					t.Fatal(err)
				}
				if polling == protocol.PollingBackground {
					if err := db.DB.Model(&db.TaskRun{}).Where("id = ?", run.ID).Update("next_poll_at", time.Now().Add(-time.Second)).Error; err != nil {
						t.Fatal(err)
					}
					claimed, err := task.NewProfileBackgroundPoller("frozen-policy-test").RunOnce(context.Background())
					if !claimed || err != nil {
						t.Fatalf("background claimed=%v err=%v", claimed, err)
					}
				}
				statusContext := mediaRegressionContext(http.MethodGet, "http://gateway.example", "/v1/videos/"+response.TaskID, "")
				status, handled, err := profileTaskStatus(statusContext, response.TaskID, asyncTaskKindVideo)
				if !handled || err != nil {
					t.Fatalf("status handled=%v err=%v", handled, err)
				}
				statusResponse := status.(*model.VideoTaskResponse)
				originalURL := statusResponse.VideoURL
				formatVideoTaskResponse(statusContext, statusResponse)
				if policy != protocol.MediaRetentionRequired && (statusResponse.VideoURL != originalURL || playgroundVideoResultURL(statusContext, statusResponse, true) != originalURL) {
					t.Fatalf("optional frozen policy rewrote original result: response=%+v original=%q", statusResponse, originalURL)
				}
				assets, err := db.ListMediaAssetsForTaskRun(run.ID, asyncTaskKindVideo)
				if err != nil {
					t.Fatal(err)
				}
				if policy == protocol.MediaRetentionRequired {
					if statusResponse.Status != "materializing" || statusResponse.VideoURL != "" || len(assets) != 1 {
						t.Fatalf("required frozen policy lost: response=%+v assets=%+v", statusResponse, assets)
					}
				} else if statusResponse.Status != model.VideoStatusCompleted || statusResponse.VideoURL == "" || (policy == protocol.MediaRetentionDisabled && len(assets) != 0) || (policy == protocol.MediaRetentionBestEffort && len(assets) != 1) {
					t.Fatalf("frozen policy changed: response=%+v assets=%+v", statusResponse, assets)
				}
				contentContext := mediaRegressionContext(http.MethodGet, "http://gateway.example", "/v1/videos/"+response.TaskID+"/content", "")
				contentHandled, contentErr := profileTaskContent(contentContext, response.TaskID)
				if !contentHandled || (policy == protocol.MediaRetentionRequired && (contentErr == nil || contents != 0)) || (policy != protocol.MediaRetentionRequired && (contentErr != nil || contents != 1)) {
					t.Fatalf("content ignored frozen policy: handled=%v err=%v provider_calls=%d", contentHandled, contentErr, contents)
				}
				replay, _, handled, err := profileEngineVideoCreate(mediaRegressionContext(http.MethodPost, "http://gateway.example", "/v1/videos", "frozen-media-key"), req)
				if !handled || err != nil || submits != 1 || polls != 1 || replay.Status != statusResponse.Status {
					t.Fatalf("replay changed contract/submitted again: handled=%v err=%v response=%+v submits=%d polls=%d", handled, err, replay, submits, polls)
				}
			})
		}
	}
}

func TestProfileAsyncRequiredMissingMediaStaysMaterializing(t *testing.T) {
	for _, kind := range []string{asyncTaskKindImage, asyncTaskKindVideo} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-missing-media-test-key")
			if err := db.InitDB(t.TempDir() + "/missing.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			submits := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				submits++
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"missing-provider-task","status":"completed"}`)
			}))
			t.Cleanup(upstream.Close)
			channel := &db.ChannelModel{ID: "missing-media", Type: "newapi", BaseURL: upstream.URL, Enabled: true, ModelsRaw: "media-model", MediaRetention: protocol.MediaRetentionRequired}
			op := protocol.Operation{Operation: kind + ".create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient,
				Submit: protocol.Submit{Method: http.MethodPost, Path: "/tasks", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
				Poll: &protocol.Poll{Method: http.MethodGet, Path: "/tasks/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"data"}}}
			saveChannelRetentionProfile(t, channel, op)
			create := mediaRegressionContext(http.MethodPost, "http://gateway.example", "/v1/"+kind, "missing-media-key")
			var response any
			if kind == asyncTaskKindImage {
				payload, _, handled, err := profileEngineImageCreate(create, &model.ImageGenerationRequest{Model: "media-model", Prompt: "test"})
				if !handled || err != nil {
					t.Fatalf("create handled=%v err=%v", handled, err)
				}
				response = payload
			} else {
				payload, _, handled, err := profileEngineVideoCreate(create, &model.VideoGenerationRequest{Model: "media-model", Prompt: "test"})
				if !handled || err != nil {
					t.Fatalf("create handled=%v err=%v", handled, err)
				}
				response = payload
			}
			_, status, _, encoded := mediaRegressionPayload(t, response)
			if status != "materializing" || !strings.Contains(encoded, "no source URL or content endpoint") {
				t.Fatalf("missing required media completed: %s", encoded)
			}
			lookupID := "missing-provider-task"
			if kind == asyncTaskKindImage {
				lookupID = imageTaskIDPrefix + lookupID
			}
			statusContext := mediaRegressionContext(http.MethodGet, "http://gateway.example", "/v1/status", "")
			payload, handled, err := profileTaskStatus(statusContext, lookupID, kind)
			if !handled || err != nil || submits != 1 {
				t.Fatalf("missing source retried provider: handled=%v err=%v provider_calls=%d", handled, err, submits)
			}
			_, status, _, encoded = mediaRegressionPayload(t, payload)
			if status != "materializing" || !strings.Contains(encoded, "no source URL or content endpoint") {
				t.Fatalf("missing media status lost contract: %s", encoded)
			}
		})
	}
}

func TestProfilePendingPreviewDoesNotEnqueueOrSkipProviderPoll(t *testing.T) {
	for _, policy := range []string{protocol.MediaRetentionBestEffort, protocol.MediaRetentionRequired} {
		t.Run(policy, func(t *testing.T) {
			t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-preview-media-test-key")
			if err := db.InitDB(t.TempDir() + "/preview.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			polls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				status := "queued"
				if r.Method == http.MethodGet {
					polls++
					status = "processing"
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{"id": "preview-provider-task", "status": status, "video_url": "https://cdn.provider.net/preview.mp4"})
			}))
			t.Cleanup(upstream.Close)
			channel := &db.ChannelModel{ID: "preview-media", Type: "newapi", BaseURL: upstream.URL, Enabled: true, ModelsRaw: "media-model", MediaRetention: policy}
			op := protocol.Operation{Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient,
				Submit: protocol.Submit{Method: http.MethodPost, Path: "/tasks", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}, ResultPaths: []string{"video_url"}},
				Poll: &protocol.Poll{Method: http.MethodGet, Path: "/tasks/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}}}
			op.Content = &protocol.Content{Method: http.MethodGet, Path: "/tasks/{task_id}/content"}
			saveChannelRetentionProfile(t, channel, op)
			response, _, handled, err := profileEngineVideoCreate(mediaRegressionContext(http.MethodPost, "http://gateway.example", "/v1/videos", ""), &model.VideoGenerationRequest{Model: "media-model", Prompt: "test"})
			if !handled || err != nil || response.Status != model.VideoStatusQueued || (policy == protocol.MediaRetentionRequired && response.VideoURL != "") {
				t.Fatalf("pending preview response=%+v handled=%v err=%v", response, handled, err)
			}
			status, handled, err := profileTaskStatus(mediaRegressionContext(http.MethodGet, "http://gateway.example", "/v1/videos/preview-provider-task", ""), "preview-provider-task", asyncTaskKindVideo)
			if !handled || err != nil || polls != 1 || status.(*model.VideoTaskResponse).Status != model.VideoStatusProcessing || (policy == protocol.MediaRetentionRequired && status.(*model.VideoTaskResponse).VideoURL != "") {
				t.Fatalf("preview skipped provider poll: status=%+v handled=%v err=%v polls=%d", status, handled, err, polls)
			}
			if policy == protocol.MediaRetentionRequired {
				contentHandled, contentErr := profileTaskContent(mediaRegressionContext(http.MethodGet, "http://gateway.example", "/v1/videos/preview-provider-task/content", ""), "preview-provider-task")
				if !contentHandled || contentErr == nil || polls != 1 {
					t.Fatalf("pending content dispatched upstream: handled=%v err=%v polls=%d", contentHandled, contentErr, polls)
				}
			}
			var count int64
			if err := db.DB.Model(&db.MediaAsset{}).Count(&count).Error; err != nil || count != 0 {
				t.Fatalf("pending preview was enqueued: count=%d err=%v", count, err)
			}
		})
	}
}

func TestProfileRequiredProjectionSeesMaterializerCompletion(t *testing.T) {
	for _, kind := range []string{asyncTaskKindImage, asyncTaskKindVideo} {
		for _, completion := range []string{"submit", "submit_partial", "client_poll"} {
			t.Run(kind+"/"+completion, func(t *testing.T) {
				t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-projection-race-test-key")
				if err := db.InitDB(t.TempDir() + "/race.db"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				polls := 0
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					status := "completed"
					if r.Method == http.MethodGet {
						polls++
					} else if completion == "client_poll" {
						status = "queued"
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "race-provider-task", "status": status, "data": []map[string]string{{"url": "https://cdn.example/one"}, {"url": "https://cdn.example/two"}}})
				}))
				t.Cleanup(upstream.Close)
				channel := &db.ChannelModel{ID: "race-media", Type: "newapi", BaseURL: upstream.URL, Enabled: true, ModelsRaw: "media-model", MediaRetention: protocol.MediaRetentionRequired}
				op := protocol.Operation{Operation: kind + ".create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient,
					Submit: protocol.Submit{Method: http.MethodPost, Path: "/tasks", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}, ResultPaths: []string{"data"}},
					Poll: &protocol.Poll{Method: http.MethodGet, Path: "/tasks/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"data"}}}
				saveChannelRetentionProfile(t, channel, op)
				previous := ensureProfileTaskResultMedia
				ensureProfileTaskResultMedia = func(ctx context.Context, runID, mediaKind string, urls []string) ([]db.MediaAsset, error) {
					assets, err := previous(ctx, runID, mediaKind, urls)
					if err != nil {
						return nil, err
					}
					// Finish after enqueue but return its earlier pending snapshot,
					// reproducing a materializer that wins the projection race.
					for _, asset := range assets {
						if completion == "submit_partial" && asset.Ordinal > 0 {
							continue
						}
						if err := db.DBForContext(ctx).WithContext(ctx).Model(&db.MediaAsset{}).Where("id = ?", asset.ID).Updates(map[string]any{"status": db.MediaAssetAvailable, "object_id": fmt.Sprint("race-object-", asset.Ordinal)}).Error; err != nil {
							return nil, err
						}
					}
					return assets, db.CompleteTaskRunAfterMediaContext(ctx, runID, mediaKind)
				}
				t.Cleanup(func() { ensureProfileTaskResultMedia = previous })
				create := mediaRegressionContext(http.MethodPost, "http://gateway.example", "/v1/create", "")
				var response any
				if kind == asyncTaskKindImage {
					payload, _, handled, err := profileEngineImageCreate(create, &model.ImageGenerationRequest{Model: "media-model", Prompt: "test"})
					if !handled || err != nil {
						t.Fatalf("create handled=%v err=%v", handled, err)
					}
					response = payload
				} else {
					payload, _, handled, err := profileEngineVideoCreate(create, &model.VideoGenerationRequest{Model: "media-model", Prompt: "test"})
					if !handled || err != nil {
						t.Fatalf("create handled=%v err=%v", handled, err)
					}
					response = payload
				}
				lookupID := "race-provider-task"
				if kind == asyncTaskKindImage {
					lookupID = imageTaskIDPrefix + lookupID
				}
				if completion == "client_poll" {
					payload, handled, err := profileTaskStatus(mediaRegressionContext(http.MethodGet, "http://gateway.example", "/v1/status", ""), lookupID, kind)
					if !handled || err != nil {
						t.Fatalf("completed worker caused projection regression: handled=%v err=%v", handled, err)
					}
					response = payload
				}
				_, status, mediaURL, body := mediaRegressionPayload(t, response)
				wantStatus, wantOutcome := model.VideoStatusCompleted, "success"
				if completion == "submit_partial" {
					wantStatus, wantOutcome = "materializing", "pending"
				}
				if status != wantStatus || (completion == "submit_partial") != (mediaURL == "") || strings.Contains(body, "cdn.example") {
					t.Fatalf("completed batch was regressed or exposed provider media: %s", body)
				}
				run, err := db.GetTaskRunByAlias(lookupID)
				if err != nil || run.TaskStatus != wantStatus || run.TaskOutcome != wantOutcome {
					t.Fatalf("completed lifecycle=%+v err=%v", run, err)
				}
				assets, err := db.ListMediaAssetsForTaskRun(run.ID, kind)
				if err != nil || len(assets) != 2 {
					t.Fatalf("initial completion did not retain the complete batch: assets=%+v err=%v", assets, err)
				}
				wantPolls := 0
				if completion == "client_poll" {
					wantPolls = 1
				}
				if polls != wantPolls {
					t.Fatalf("provider polls=%d want=%d", polls, wantPolls)
				}
			})
		}
	}
}

func TestProfileEditsRetentionCaptureResponseLimit(t *testing.T) {
	const limit = 8 << 20
	for _, flow := range []string{"json", "multipart"} {
		for _, oversized := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/oversized_%t", flow, oversized), func(t *testing.T) {
				t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-capture-limit-test-key")
				if err := db.InitDB(t.TempDir() + "/capture.db"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { SetMediaObjectStore(nil); _ = db.Close() })
				store, err := media.NewLocalObjectStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				SetMediaObjectStore(store)
				prefix, suffix := `{"data":[{"b64_json":"iVBORw0KGgo="}],"metadata":"`, `"}`
				size := limit
				if oversized {
					size++
				}
				body := prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
				submits := 0
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					submits++
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
					_, _ = io.WriteString(w, body)
				}))
				t.Cleanup(upstream.Close)
				channel := &db.ChannelModel{ID: "capture-media", Type: "newapi", BaseURL: upstream.URL, Enabled: true, ModelsRaw: "media-model", MediaRetention: protocol.MediaRetentionRequired}
				saveChannelRetentionProfile(t, channel, protocol.Operation{Operation: "images.edits", ExecutionMode: protocol.ExecutionDirect, PollingMode: protocol.PollingOff, Submit: protocol.Submit{Method: http.MethodPost, Path: "/edits", BodyEncoding: "json"}})
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", strings.NewReader(`{"model":"media-model"}`))
				selected := channel.ToUpstreamChannel()
				var handled bool
				if flow == "multipart" {
					var input bytes.Buffer
					mw := multipart.NewWriter(&input)
					_ = mw.WriteField("model", "media-model")
					_ = mw.Close()
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", &input)
					c.Request.Header.Set("Content-Type", mw.FormDataContentType())
					if err := c.Request.ParseMultipartForm(1 << 20); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = c.Request.MultipartForm.RemoveAll() })
					handled, err = profileEngineMultipartDirect(c, "images.edits", c.Request.MultipartForm, &selected)
				} else {
					handled, _, err = profileEngineDirectForChannelResult(c, "images.edits", "", &selected, recorder, func() bool { return recorder.Body.Len() > 0 })
				}
				if !handled || submits != 1 {
					t.Fatalf("capture repeated or skipped provider: handled=%v submits=%d err=%v", handled, submits, err)
				}
				if oversized {
					if !errors.Is(err, protocol.ErrResponseTooLarge) || recorder.Body.Len() != 0 {
						t.Fatalf("oversized response escaped capture: err=%v bytes=%d", err, recorder.Body.Len())
					}
					return
				}
				if err != nil || !strings.Contains(recorder.Body.String(), "/v1/media/") || strings.Contains(recorder.Body.String(), "b64_json") || recorder.Header().Get("Content-Length") != "" {
					t.Fatalf("response at limit was not safely rewritten: err=%v bytes=%d stale_content_length=%q", err, recorder.Body.Len(), recorder.Header().Get("Content-Length"))
				}
			})
		}
	}
}
