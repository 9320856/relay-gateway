package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	relaymedia "relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

func allowProfileMediaTestURLs(t *testing.T) {
	t.Helper()
	previous := profileMediaFetcherFactory
	profileMediaFetcherFactory = func() relaymedia.SourceFetcher {
		return relaymedia.HTTPSourceFetcher{URLValidator: func(string) error { return nil }}
	}
	t.Cleanup(func() { profileMediaFetcherFactory = previous })
}

func TestProfileImageResponseKeepsTaskEnvelopeAndRawPayload(t *testing.T) {
	raw := map[string]any{"id": "provider-image-1", "status": "queued", "extra": true}
	got := profileImageResponse(protocol.Result{JSON: raw, TaskID: "provider-image-1", ResultURLs: []string{"https://cdn.test/image.png"}}, true)
	if got["id"] != "provider-image-1" || got["task_id"] != "provider-image-1" || got["status"] != "queued" {
		t.Fatalf("unexpected image envelope: %#v", got)
	}
	if got["raw"] == nil || got["raw"].(map[string]any)["extra"] != true {
		t.Fatalf("raw payload was not retained: %#v", got["raw"])
	}
	data, ok := got["data"].([]map[string]string)
	if !ok || len(data) != 1 || data[0]["url"] != "https://cdn.test/image.png" {
		t.Fatalf("unexpected image data: %#v", got["data"])
	}
}

func TestProfileEngineImageCreateRequiresExplicitOptIn(t *testing.T) {
	t.Setenv("RELAY_ENABLE_PROFILE_IMAGE_ENGINE", "")
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/images/jobs", nil)
	got, channel, handled, err := profileEngineImageCreate(c, &model.ImageGenerationRequest{Model: "image-model", Prompt: "test"})
	if err != nil || got != nil || channel != nil || handled {
		t.Fatalf("disabled profile image engine should be a no-op: got=%#v channel=%v handled=%v err=%v", got, channel, handled, err)
	}
}

func TestProfileEngineImageSubmitResultCreatesMediaContext(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-image-submit-media-key")
	t.Setenv("RELAY_ENABLE_PROFILE_IMAGE_ENGINE", "1")
	if err := db.InitDB(t.TempDir() + "/profile-image-submit-media.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/images" {
			t.Fatalf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     "profile-image-submit-result",
			"status": "completed",
			"data":   []map[string]string{{"url": "https://cdn.example/submit-result.png"}},
		})
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-image-submit-channel", Name: "Profile Image Submit", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "profile-image-submit-model"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "image.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: protocol.MediaRetentionBestEffort,
		Submit:   protocol.Submit{Method: http.MethodPost, Path: "/images", BodyEncoding: "json"},
		Response: protocol.Response{TaskIDPaths: []string{"id"}, ResultPaths: []string{"data"}},
		Poll:     &protocol.Poll{Method: http.MethodGet, Path: "/images/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"data"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-image-submit", Name: "Profile Image Submit", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-image-submit", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "image.create", ModelPattern: "profile-image-submit-model", ProfileID: "profile-image-submit", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images", nil)
	response, selected, handled, err := profileEngineImageCreate(c, &model.ImageGenerationRequest{Model: "profile-image-submit-model", Prompt: "render"})
	if err != nil || !handled || selected == nil || response == nil {
		t.Fatalf("image submit handled=%v selected=%v response=%#v err=%v", handled, selected, response, err)
	}
	run, err := db.GetTaskRunByAlias(imageTaskIDPrefix + "profile-image-submit-result")
	if err != nil {
		t.Fatal(err)
	}
	assets, err := db.ListMediaAssetsForTaskRun(run.ID, asyncTaskKindImage)
	if err != nil || len(assets) != 1 || assets[0].Status != db.MediaAssetPending || assets[0].SourceLocator != "https://cdn.example/submit-result.png" {
		t.Fatalf("image media assets = %#v, err=%v", assets, err)
	}
	var jobs []db.MediaMaterializationJob
	if err := db.DB.Where("asset_id = ?", assets[0].ID).Find(&jobs).Error; err != nil || len(jobs) != 1 || jobs[0].Status != db.MediaJobPending {
		t.Fatalf("image media jobs = %#v, err=%v", jobs, err)
	}
}

func TestNormalizeProfileImageStatus(t *testing.T) {
	for _, tc := range []struct{ in, want string }{{"succeeded", "completed"}, {"running", "processing"}, {"error", "failed"}, {"", "queued"}} {
		if got := normalizeProfileImageStatus(tc.in); got != tc.want {
			t.Errorf("normalizeProfileImageStatus(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestClearProfileMediaPayloadRemovesTemporaryURLs(t *testing.T) {
	video := &model.VideoTaskResponse{
		VideoURL: "https://provider.example/video.mp4",
		URL:      "https://provider.example/video.mp4",
		Data:     []map[string]string{{"url": "https://provider.example/video.mp4"}},
	}
	clearProfileMediaPayload(video)
	if video.VideoURL != "" || video.URL != "" || video.Data != nil {
		t.Fatalf("video media was not cleared: %#v", video)
	}

	image := map[string]any{
		"data":        []map[string]string{{"url": "https://provider.example/image.png"}},
		"raw":         map[string]any{"url": "https://provider.example/image.png"},
		"raw_payload": map[string]any{"url": "https://provider.example/image.png"},
	}
	clearProfileMediaPayload(image)
	if _, ok := image["data"]; ok || image["raw"] != nil || image["raw_payload"] != nil {
		t.Fatalf("image media was not cleared: %#v", image)
	}
}
