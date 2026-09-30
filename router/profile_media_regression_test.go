package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func saveRequiredMediaRegressionProfile(t *testing.T, kind string) string {
	t.Helper()
	operation := kind + ".create"
	resultPath := "video_url"
	if kind == asyncTaskKindImage {
		resultPath = "data"
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: operation, ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingGatewayWait, MediaRetention: protocol.MediaRetentionRequired,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/tasks", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/tasks/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{resultPath}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	profileID := "required-media-" + kind
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: profileID, Name: profileID, Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: profileID, Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	return profileID
}

func mediaRegressionContext(method, origin, path, idempotencyKey string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(method, origin+path, nil)
	if idempotencyKey != "" {
		c.Request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return c
}

func mediaRegressionPayload(t *testing.T, payload any) (id, status, mediaURL, encoded string) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		t.Fatal(err)
	}
	id, _ = object["task_id"].(string)
	status, _ = object["status"].(string)
	mediaURL, _ = object["video_url"].(string)
	if mediaURL == "" {
		if data, ok := object["data"].([]any); ok && len(data) > 0 {
			mediaURL, _ = data[0].(map[string]any)["url"].(string)
		}
	}
	return id, status, mediaURL, string(body)
}

func TestProfileRequiredMediaIdempotentReplay(t *testing.T) {
	for _, kind := range []string{asyncTaskKindVideo, asyncTaskKindImage} {
		for _, failDownload := range []bool{true, false} {
			name := kind + "/success"
			if failDownload {
				name = kind + "/failure"
			}
			t.Run(name, func(t *testing.T) {
				t.Setenv("RELAY_DB_ENCRYPTION_KEY", "required-media-replay-test-key")
				t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
				t.Setenv("RELAY_ENABLE_PROFILE_IMAGE_ENGINE", "1")
				if err := db.InitDB(t.TempDir() + "/replay.db"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { SetMediaObjectStore(nil); _ = db.Close() })
				store, err := media.NewLocalObjectStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				SetMediaObjectStore(store)
				const providerID = "colliding-provider-task"
				source := "https://provider.example/result." + kind
				providerBody := map[string]any{"id": providerID, "status": "completed"}
				if kind == asyncTaskKindVideo {
					providerBody["video_url"] = source
				} else {
					providerBody["data"] = []map[string]string{{"url": source}}
				}
				submitCalls, pollCalls, downloadCalls := 0, 0, 0
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.Method == http.MethodPost && r.URL.Path == "/v1/tasks" {
						submitCalls++
						_ = json.NewEncoder(w).Encode(map[string]string{"id": providerID, "status": "queued"})
					} else if r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/"+providerID {
						pollCalls++
						_ = json.NewEncoder(w).Encode(providerBody)
					} else {
						http.NotFound(w, r)
					}
				}))
				t.Cleanup(upstream.Close)
				previousFetcher := profileMediaFetcherFactory
				profileMediaFetcherFactory = func(string, string) media.SourceFetcher {
					return media.SourceFetcherFunc(func(context.Context, media.MediaResult) (media.FetchedSource, error) {
						downloadCalls++
						if failDownload {
							return media.FetchedSource{}, errors.New("provider media temporarily unavailable")
						}
						return media.FetchedSource{Body: io.NopCloser(strings.NewReader("media bytes")), ContentType: kind + "/" + kind}, nil
					})
				}
				t.Cleanup(func() { profileMediaFetcherFactory = previousFetcher })
				channel := &db.ChannelModel{ID: "media-replay-channel", Name: "media replay", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "media-model"}
				if err := db.SaveChannelModel(channel); err != nil {
					t.Fatal(err)
				}
				service.DefaultDispatcher.ResetBreaker(channel.ID)
				t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
				profileID := saveRequiredMediaRegressionProfile(t, kind)
				if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: kind + ".create", ModelPattern: "media-model", ProfileID: profileID, ProfileRevision: 1, Enabled: true}); err != nil {
					t.Fatal(err)
				}
				conflictingID := providerID
				path := "/v1/videos"
				if kind == asyncTaskKindImage {
					conflictingID = imageTaskIDPrefix + providerID
					path = "/v1/images/jobs"
				}
				if err := db.EnsureTaskMappings(db.TaskMapping{TaskID: conflictingID, ChannelID: "other-channel", TaskKind: kind, TaskAlias: providerID}); err != nil {
					t.Fatal(err)
				}
				submit := func(c *gin.Context) any {
					if kind == asyncTaskKindVideo {
						response, _, handled, err := profileEngineVideoCreate(c, &model.VideoGenerationRequest{Model: "media-model", Prompt: "test"})
						if err != nil || !handled {
							t.Fatalf("video handled=%v err=%v", handled, err)
						}
						return response
					}
					response, _, handled, err := profileEngineImageCreate(c, &model.ImageGenerationRequest{Model: "media-model", Prompt: "test"})
					if err != nil || !handled {
						t.Fatalf("image handled=%v err=%v", handled, err)
					}
					return response
				}
				first := submit(mediaRegressionContext(http.MethodPost, "https://first.example", path, "media-replay-key"))
				firstID, firstStatus, firstURL, firstBody := mediaRegressionPayload(t, first)
				if !strings.HasPrefix(firstID, "gt_") || strings.Contains(firstBody, source) {
					t.Fatalf("initial response leaked provider identity/media: %s", firstBody)
				}
				if failDownload {
					if firstStatus != "materializing" || firstURL != "" {
						t.Fatalf("failed media response = %s", firstBody)
					}
				} else {
					if firstStatus != "completed" || !strings.HasPrefix(firstURL, "https://first.example/v1/media/") {
						t.Fatalf("successful media response = %s", firstBody)
					}
					// A later provider result can overwrite the original public IDs
					// and carry a stale payload status; aliases and TaskRun state win.
					run, err := db.FindProfileTaskRunByIdempotencyKeyContext(context.Background(), "media-replay-key", kind+".create")
					if err != nil {
						t.Fatal(err)
					}
					providerBody["status"] = "processing"
					encoded, _ := json.Marshal(providerBody)
					if err := db.UpdateTaskRunResult(run.ID, string(encoded), false); err != nil {
						t.Fatal(err)
					}
				}
				replay := submit(mediaRegressionContext(http.MethodPost, "https://current.example", path, "media-replay-key"))
				replayID, replayStatus, replayURL, replayBody := mediaRegressionPayload(t, replay)
				if replayID != firstID || strings.Contains(replayBody, source) {
					t.Fatalf("replay lost public alias or leaked provider media: %s", replayBody)
				}
				if failDownload {
					if replayStatus != "materializing" || replayURL != "" {
						t.Fatalf("failed-media replay = %s", replayBody)
					}
				} else {
					firstParsed, _ := url.Parse(firstURL)
					replayParsed, _ := url.Parse(replayURL)
					if replayStatus != "completed" || replayParsed.Host != "current.example" || replayParsed.Path != firstParsed.Path || strings.Contains(replayBody, "first.example") {
						t.Fatalf("successful-media replay = %s", replayBody)
					}
				}
				if submitCalls != 1 || pollCalls != 1 || downloadCalls != 1 {
					t.Fatalf("replay repeated work: submit=%d poll=%d downloads=%d", submitCalls, pollCalls, downloadCalls)
				}
			})
		}
	}
}

func TestProfileMultiImageMaterializationRetriesOnlyMissingAssets(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "multi-image-materialization-test-key")
	if err := db.InitDB(t.TempDir() + "/multi-image.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetMediaObjectStore(nil); _ = db.Close() })
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	profileID := saveRequiredMediaRegressionProfile(t, asyncTaskKindImage)
	inline := "data:image/png;base64,iVBORw0KGgo="
	remote := "https://provider.example/second.png"
	run := &db.TaskRun{ID: "multi-image-run", TaskKind: asyncTaskKindImage, Operation: "image.create", Engine: "profile", ProfileID: profileID, ProfileRevision: 1, SubmissionState: "accepted", ProviderTaskID: "provider-images", TaskStatus: "processing", TaskOutcome: "pending", ResultBody: `{"id":"provider-images","status":"processing","data":[{"url":"` + inline + `"},{"url":"` + remote + `"}]}`}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	previousFetcher := profileMediaFetcherFactory
	t.Cleanup(func() { profileMediaFetcherFactory = previousFetcher })
	failRemote, downloads := true, 0
	profileMediaFetcherFactory = func(string, string) media.SourceFetcher {
		return media.SourceFetcherFunc(func(context.Context, media.MediaResult) (media.FetchedSource, error) {
			downloads++
			if failRemote {
				return media.FetchedSource{}, errors.New("second source unavailable")
			}
			return media.FetchedSource{Body: io.NopCloser(strings.NewReader("second image")), ContentType: "image/png"}, nil
		})
	}
	c := mediaRegressionContext(http.MethodPost, "https://gateway.example", "/v1/images/jobs", "")
	response := map[string]any{}
	if err := materializeProfileImageResponse(c, run.ID, "", response, []string{inline, remote}); err == nil {
		t.Fatal("expected second image materialization failure")
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	assets, err := db.ListMediaAssetsForTaskRun(run.ID, asyncTaskKindImage)
	if err != nil || len(assets) != 2 || assets[0].Status != db.MediaAssetAvailable || assets[1].Status != db.MediaAssetFailed || loaded.TaskStatus != "processing" || loaded.TaskOutcome != "pending" {
		t.Fatalf("partial result became terminal or lost assets: run=%+v assets=%+v err=%v", loaded, assets, err)
	}
	_, status, mediaURL, body := mediaRegressionPayload(t, profileDurableStatus(c, loaded, asyncTaskKindImage))
	if status != "materializing" || mediaURL != "" || strings.Contains(body, remote) || strings.Contains(body, inline) {
		t.Fatalf("partial required-media status leaked results: %s", body)
	}
	failRemote = false
	if err := db.RetryMediaAssetMaterialization(assets[1].ID); err != nil {
		t.Fatal(err)
	}
	if err := materializeProfileImageResponse(c, run.ID, "", response, []string{inline, remote}); err != nil {
		t.Fatal(err)
	}
	completed, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	retriedAssets, err := db.ListMediaAssetsForTaskRun(run.ID, asyncTaskKindImage)
	if err != nil || len(retriedAssets) != 2 || retriedAssets[0].ID != assets[0].ID || retriedAssets[1].ID != assets[1].ID || completed.TaskStatus != "completed" || completed.TaskOutcome != "success" {
		t.Fatalf("retry did not reuse and complete result set: run=%+v assets=%+v err=%v", completed, retriedAssets, err)
	}
	firstJob, err := db.GetMediaMaterializationJob(assets[0].ID)
	if err != nil || firstJob.Attempts != 1 || downloads != 2 {
		t.Fatalf("retry downloaded available first image again: job=%+v downloads=%d err=%v", firstJob, downloads, err)
	}
}

func TestProfileMultiImageSynchronousAndBackgroundMaterializationShareAssets(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "multi-image-concurrency-test-key")
	if err := db.InitDB(t.TempDir() + "/multi-image-concurrency.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetMediaObjectStore(nil); _ = db.Close() })
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	run := &db.TaskRun{ID: "multi-image-concurrency", TaskKind: asyncTaskKindImage, Operation: "image.create", Engine: "profile", TaskStatus: "processing", TaskOutcome: "pending"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	previous := profileMediaFetcherFactory
	t.Cleanup(func() { profileMediaFetcherFactory = previous })
	profileMediaFetcherFactory = func(string, string) media.SourceFetcher {
		return media.SourceFetcherFunc(func(ctx context.Context, _ media.MediaResult) (media.FetchedSource, error) {
			close(entered)
			select {
			case <-release:
				return media.FetchedSource{Body: io.NopCloser(strings.NewReader("first image")), ContentType: "image/png"}, nil
			case <-ctx.Done():
				return media.FetchedSource{}, ctx.Err()
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := mediaRegressionContext(http.MethodPost, "https://gateway.example", "/v1/images/jobs", "")
	c.Request = c.Request.WithContext(ctx)
	go func() {
		done <- materializeProfileImageResponse(c, run.ID, "", map[string]any{}, []string{"https://provider.example/first.png", "data:image/png;base64,iVBORw0KGgo="})
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("synchronous materialization failed before fetch: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("synchronous materialization did not start")
	}
	claimed, backgroundErr := task.NewMediaMaterializationPoller("background", store).RunOnce(context.Background())
	loaded, loadErr := db.GetTaskRun(run.ID)
	assets, assetsErr := db.ListMediaAssetsForTaskRun(run.ID, asyncTaskKindImage)
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("synchronous materialization did not complete")
	}
	if backgroundErr != nil || !claimed || loadErr != nil || loaded.TaskStatus != "processing" || assetsErr != nil || len(assets) != 2 || assets[1].Status != db.MediaAssetAvailable {
		t.Fatalf("background did not share the complete pending batch: claimed=%v run=%+v assets=%+v errors=%v/%v/%v", claimed, loaded, assets, backgroundErr, loadErr, assetsErr)
	}
	loaded, err = db.GetTaskRun(run.ID)
	if err != nil || loaded.TaskStatus != "completed" {
		t.Fatalf("combined result did not complete task: run=%+v err=%v", loaded, err)
	}
	for _, asset := range assets {
		job, err := db.GetMediaMaterializationJob(asset.ID)
		if err != nil || job.Attempts != 1 || job.Status != db.MediaJobSucceeded {
			t.Fatalf("materialization duplicated between workers: job=%+v err=%v", job, err)
		}
	}
}
