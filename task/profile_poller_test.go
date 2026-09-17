package task

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

func init() {
	if os.Getenv("RELAY_DB_ENCRYPTION_KEY") == "" {
		_ = os.Setenv("RELAY_DB_ENCRYPTION_KEY", "default-test-encryption-key-for-unit-tests-entropy")
	}
}

func TestProfilePollBudgetStopsExpiredBackgroundTask(t *testing.T) {
	op := protocol.Operation{Poll: &protocol.Poll{MaxAttempts: 2}}
	run := &db.TaskRun{PollCount: 2}
	observation, exceeded := profilePollBudget(run, op)
	if !exceeded || observation.Status != "expired" || observation.Success || !observation.SkipPollAccounting {
		t.Fatalf("attempt budget observation=%+v exceeded=%v", observation, exceeded)
	}
	deadline := time.Now().Add(-time.Second)
	run = &db.TaskRun{PollCount: 1, DeadlineAt: &deadline}
	observation, exceeded = profilePollBudget(run, op)
	if !exceeded || observation.Status != "expired" || observation.Outcome != "failed" || !observation.SkipPollAccounting {
		t.Fatalf("deadline budget observation=%+v exceeded=%v", observation, exceeded)
	}
}

func TestProfilePollBudgetDoesNotCountWithoutProviderRequest(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-poll-budget.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingBackground,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-poll-budget", Name: "Profile Poll Budget", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-poll-budget", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	run := &db.TaskRun{ID: "profile-poll-budget-run", TaskKind: "video", Operation: "video.create", ChannelID: "missing-channel", Engine: "profile", ProfileID: "profile-poll-budget", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingBackground, ProviderTaskID: "provider-budget", TaskStatus: "processing", TaskOutcome: "pending", PollCount: 1, NextPollAt: &now}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	claimed, err := (&BackgroundPoller{Owner: "budget-worker", Poll: ProfilePoll}).RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("budget worker claimed=%v err=%v", claimed, err)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TaskStatus != "expired" || loaded.TaskOutcome != "failed" || loaded.PollCount != 1 || loaded.LeaseOwner != "" {
		t.Fatalf("budget task = %+v", loaded)
	}
	var attempts []db.TaskAttempt
	if err := db.DB.Where("task_run_id = ?", run.ID).Find(&attempts).Error; err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 0 {
		t.Fatalf("budget check recorded provider attempt: %+v", attempts)
	}
}

func TestProfilePollRequiredMediaEnqueueFailureStaysRetryable(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-poll-required-media.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var pollCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pollCalls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/provider-required" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"completed","video_url":"https://cdn.example/video.mp4"}`))
	}))
	t.Cleanup(server.Close)
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "profile-required-channel", Name: "profile-required-channel", Type: "newapi", BaseURL: server.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "video-model"}); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingBackground,
		MediaRetention: protocol.MediaRetentionRequired,
		Submit:         protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-required", Name: "Profile Required", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-required", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	run := &db.TaskRun{ID: "profile-required-run", TaskKind: "video", Operation: "video.create", ChannelID: "profile-required-channel", Engine: "profile", ProfileID: "profile-required", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingBackground, ProviderTaskID: "provider-required", TaskStatus: "processing", TaskOutcome: "pending", NextPollAt: &now}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	previousEnsure := ensureTaskResultMedia
	ensureTaskResultMedia = func(context.Context, string, string, []string) ([]db.MediaAsset, error) {
		return nil, errors.New("materialization queue unavailable")
	}
	t.Cleanup(func() { ensureTaskResultMedia = previousEnsure })
	poller := &BackgroundPoller{Owner: "required-media-test", Lease: time.Minute, RetryAfter: time.Second, Poll: ProfilePoll}
	claimed, pollErr := poller.RunOnce(context.Background())
	if !claimed || pollErr == nil {
		t.Fatalf("required media failure claimed=%v err=%v", claimed, pollErr)
	}
	persisted, err := db.GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.TaskStatus != model.VideoStatusProcessing || persisted.TaskOutcome != "pending" {
		t.Fatalf("required media failure made task terminal: %+v", persisted)
	}
	if persisted.PollFailureCount != 1 || persisted.PollSuccessCount != 0 {
		t.Fatalf("poll counters = %+v", persisted)
	}
	if persisted.ResultBody == "" || !strings.Contains(persisted.ResultBody, "video.mp4") {
		t.Fatalf("provider result was not persisted: %q", persisted.ResultBody)
	}
	if pollCalls != 1 {
		t.Fatalf("provider poll calls = %d, want 1", pollCalls)
	}
}

func TestProfilePollUsesCapturedRevisionAndProviderTask(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-poll.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/videos/provider-task" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"completed","video_url":"https://cdn.example/video.mp4"}`))
	}))
	t.Cleanup(server.Close)
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "profile-poll-channel", Name: "profile-poll-channel", Type: "newapi", BaseURL: server.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "video-model"}); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingBackground,
		MediaRetention: protocol.MediaRetentionBestEffort,
		Submit:         protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-poll", Name: "Profile Poll", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-poll", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	// Publish a changed revision before the poller runs. The task row below
	// captures revision 1, so polling must continue using revision 1's path and
	// digest; revision 2 is only for newly-created tasks.
	profileV2 := profile
	pollV2 := *profileV2.Operations[0].Poll
	pollV2.Path = "/videos/new/{task_id}"
	profileV2.Operations[0].Poll = &pollV2
	compiledV2, err := protocol.Compile(profileV2)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-poll", Revision: 2, SchemaVersion: 1, ContentJSON: string(compiledV2.CanonicalJSON()), ContentDigest: compiledV2.Digest(), State: db.ProfileRevisionDraft}); err != nil {
		t.Fatal(err)
	}
	if err := db.PublishProtocolProfileRevision("profile-poll", 2); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "profile-poll-run", TaskKind: "video", Operation: "video.create", ChannelID: "profile-poll-channel", Engine: "profile", ProfileID: "profile-poll", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingBackground, ProviderTaskID: "provider-task", TaskStatus: "processing", TaskOutcome: "pending"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	observation, err := ProfilePoll(nil, run)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Status != "completed" || observation.Outcome != "success" || !observation.Success || observation.HTTPStatus != http.StatusOK {
		t.Fatalf("observation = %+v", observation)
	}
	assets, err := db.ListMediaAssets(10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 1 || assets[0].TaskRunID != run.ID || assets[0].SourceLocator != "https://cdn.example/video.mp4" {
		t.Fatalf("assets = %+v", assets)
	}
	if _, err := db.GetMediaMaterializationJob(assets[0].ID); err != nil {
		t.Fatalf("materialization job: %v", err)
	}
	if persisted, err := db.GetTaskRun(run.ID); err != nil || persisted.ResultBody == "" || !strings.Contains(persisted.ResultBody, "video_url") {
		t.Fatalf("persisted result = %+v, %v", persisted, err)
	}
	// A repeated terminal poll must not create another asset or job.
	if _, err := ProfilePoll(nil, run); err != nil {
		t.Fatal(err)
	}
	assets, err = db.ListMediaAssets(10, 0)
	if err != nil || len(assets) != 1 {
		t.Fatalf("repeated assets = %+v, %v", assets, err)
	}
}

func TestProfilePollRequiredVideoUsesAuthenticatedContentEndpoint(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-poll-content.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var pollCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/provider-content-task" {
			t.Fatalf("unexpected provider request: %s %s", r.Method, r.URL.Path)
		}
		pollCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	t.Cleanup(server.Close)
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "profile-content-poll-channel", Name: "Profile Content Poll", Type: "newapi", BaseURL: server.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "video-model"}); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingBackground,
		MediaRetention: protocol.MediaRetentionRequired,
		Submit:         protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll:    &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
		Content: &protocol.Content{Method: http.MethodGet, Path: "/videos/{task_id}/content"},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-content-poll", Name: "Profile Content Poll", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-content-poll", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	run := &db.TaskRun{ID: "profile-content-poll-run", TaskKind: "video", Operation: "video.create", ChannelID: "profile-content-poll-channel", Engine: "profile", ProfileID: "profile-content-poll", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingBackground, ProviderTaskID: "provider-content-task", TaskStatus: "processing", TaskOutcome: "pending", NextPollAt: &now}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	observation, err := ProfilePoll(context.Background(), run)
	if err != nil || !observation.PausePolling || observation.Status != "materializing" || observation.Outcome != "pending" {
		t.Fatalf("content-only required observation=%+v err=%v", observation, err)
	}
	if pollCalls != 1 {
		t.Fatalf("provider poll calls=%d, want 1", pollCalls)
	}
	assets, err := db.ListMediaAssetsForTaskRun(run.ID, "video")
	if err != nil || len(assets) != 1 || assets[0].SourceKind != "provider_content" || assets[0].SourceLocator != run.ProviderTaskID {
		t.Fatalf("content media assets=%+v err=%v", assets, err)
	}
	if _, err := db.GetMediaMaterializationJob(assets[0].ID); err != nil {
		t.Fatalf("content media job: %v", err)
	}
}
