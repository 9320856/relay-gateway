package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

func TestProfileResultURLsRecoversFanrenImageEnvelope(t *testing.T) {
	body := `{"job":{"status":"succeeded","assets":[{"proxy_url":"https://cdn.example/one.png","thumbnail_url":"https://cdn.example/thumb.png"},{"proxy_url":"https://cdn.example/two.png"}]}}`
	got := profileResultURLs(body, asyncTaskKindImage)
	if len(got) != 2 || got[0] != "https://cdn.example/one.png" || got[1] != "https://cdn.example/two.png" {
		t.Fatalf("profile image result URLs=%v", got)
	}
}

func TestProfileResultURLsUsesThumbnailWhenFullImageIsAbsent(t *testing.T) {
	body := `{"job":{"status":"succeeded","assets":[{"thumbnail_url":"https://cdn.example/thumb.png"}]}}`
	got := profileResultURLs(body, asyncTaskKindImage)
	if len(got) != 1 || got[0] != "https://cdn.example/thumb.png" {
		t.Fatalf("profile thumbnail result URLs=%v", got)
	}
}

func TestProfileResultURLsRecoversVideoDataEnvelope(t *testing.T) {
	body := `{"status":"completed","data":[{"url":"https://cdn.example/one.mp4"},{"url":"https://cdn.example/two.mp4"}]}`
	got := profileResultURLs(body, asyncTaskKindVideo)
	if len(got) != 2 || got[0] != "https://cdn.example/one.mp4" || got[1] != "https://cdn.example/two.mp4" {
		t.Fatalf("profile video result URLs=%v", got)
	}
}

func TestProfileContentMediaEligibleOnlyForVideoContentOperation(t *testing.T) {
	video := protocol.Operation{Content: &protocol.Content{Path: "/videos/{task_id}/content"}}
	if !profileContentMediaEligible(video, asyncTaskKindVideo) {
		t.Fatal("video content operation should be eligible")
	}
	if profileContentMediaEligible(video, asyncTaskKindImage) {
		t.Fatal("image content operation should not use video fallback")
	}
}

func TestProfileStatusRecoversHistoricalBestEffortResultMediaWithoutPolling(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-status-recovery-key")
	if err := db.InitDB(t.TempDir() + "/profile-status-recovery.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		http.Error(w, "completed tasks must not poll upstream", http.StatusInternalServerError)
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "profile-status-recovery-channel", Name: "Profile Status Recovery", Type: "newapi", BaseURL: upstream.URL, APIKey: "key", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: protocol.MediaRetentionBestEffort,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-status-recovery", Name: "Profile Status Recovery", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-status-recovery", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "profile-status-recovery-run", TaskKind: asyncTaskKindVideo, Operation: "video.create", ChannelID: channel.ID, Engine: "profile", ProfileID: "profile-status-recovery", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingClient, ProviderTaskID: "historical-provider-task", SubmissionState: "accepted", TaskStatus: model.VideoStatusCompleted, TaskOutcome: "success", ResultBody: `{"id":"historical-provider-task","status":"completed","video_url":"https://cdn.example/historical.mp4"}`}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: run.ProviderTaskID}); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/"+run.ProviderTaskID, nil)
	status, handled, err := profileTaskStatus(context, run.ProviderTaskID, asyncTaskKindVideo)
	if err != nil || !handled {
		t.Fatalf("historical status handled=%v err=%v", handled, err)
	}
	response, ok := status.(*model.VideoTaskResponse)
	if !ok || response.Status != model.VideoStatusCompleted || response.VideoURL != "https://cdn.example/historical.mp4" {
		t.Fatalf("historical status response = %#v", status)
	}
	assets, err := db.ListMediaAssetsForTaskRun(run.ID, asyncTaskKindVideo)
	if err != nil || len(assets) != 1 || assets[0].Status != db.MediaAssetPending || assets[0].SourceLocator != "https://cdn.example/historical.mp4" {
		t.Fatalf("recovered media assets = %#v, err=%v", assets, err)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PollCount != 0 || upstreamCalls != 0 {
		t.Fatalf("historical completed status polled upstream: poll_count=%d upstream_calls=%d", loaded.PollCount, upstreamCalls)
	}
}
