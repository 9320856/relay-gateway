package task

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
)

func TestMediaMaterializationPollerMaterializesDurableURLJob(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-materializer.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write([]byte("video-bytes"))
	}))
	t.Cleanup(server.Close)
	asset := &db.MediaAsset{PublicID: "asset-materializer", CapabilityHash: "hash", TaskRunID: "run-materializer", Kind: "video", Ordinal: 0, Status: db.MediaAssetPending, SourceKind: media.SourceURL, SourceLocator: server.URL}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaMaterializationJob(&db.MediaMaterializationJob{AssetID: asset.ID, Status: db.MediaJobPending}); err != nil {
		t.Fatal(err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	poller := NewMediaMaterializationPoller("materializer-test", store)
	poller.Fetcher = media.SourceFetcherFunc(func(_ context.Context, result media.MediaResult) (media.FetchedSource, error) {
		return media.FetchedSource{Body: io.NopCloser(strings.NewReader("video-bytes")), ContentType: "video/mp4"}, nil
	})
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("run once claimed=%v err=%v", claimed, err)
	}
	got, err := db.GetMediaAssetByID(asset.ID)
	if err != nil || got.Status != db.MediaAssetAvailable || got.ObjectID == "" {
		t.Fatalf("asset=%+v err=%v", got, err)
	}
	object, err := db.GetMediaObjectByID(got.ObjectID)
	if err != nil || object.State != db.MediaObjectReady {
		t.Fatalf("object=%+v err=%v", object, err)
	}
	job, err := db.GetMediaMaterializationJob(asset.ID)
	if err != nil || job.Status != db.MediaJobSucceeded {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}

func TestMediaMaterializationPollerMaterializesDurableBase64Job(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "base64-materializer-test-key")
	if err := db.InitDB(t.TempDir() + "/base64-media-materializer.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	run := &db.TaskRun{ID: "run-base64-materializer", TaskKind: "image", Operation: "images.create", ChannelID: "channel", Engine: "profile", PollingMode: "background", TaskStatus: "processing", TaskOutcome: "pending"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	assets, err := db.EnsureTaskResultMediaContext(context.Background(), run.ID, "image", []string{"data:image/png;base64,iVBORw0KGgo="})
	if err != nil || len(assets) != 1 {
		t.Fatalf("ensure inline asset = %#v, %v", assets, err)
	}
	if assets[0].SourceKind != media.SourceBase64 || assets[0].ContentType != "image/png" {
		t.Fatalf("inline asset source = %#v", assets[0])
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := NewMediaMaterializationPoller("base64-materializer-test", store).RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("run once claimed=%v err=%v", claimed, err)
	}
	asset, err := db.GetMediaAssetByID(assets[0].ID)
	if err != nil || asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" || asset.ContentType != "image/png" {
		t.Fatalf("materialized inline asset = %#v, %v", asset, err)
	}
}

func TestMediaMaterializationPollerCompletesStaleJobWithoutRefetch(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-materializer-stale.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	asset := &db.MediaAsset{PublicID: "asset-materialized", CapabilityHash: "hash", TaskRunID: "run-materialized", Kind: "video", Ordinal: 0, Status: db.MediaAssetAvailable, ObjectID: "obj_existing", SourceKind: media.SourceURL, SourceLocator: "https://provider.invalid/already-done"}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaMaterializationJob(&db.MediaMaterializationJob{AssetID: asset.ID, Status: db.MediaJobPending}); err != nil {
		t.Fatal(err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	poller := NewMediaMaterializationPoller("materializer-stale-test", store)
	poller.Fetcher = media.SourceFetcherFunc(func(context.Context, media.MediaResult) (media.FetchedSource, error) {
		called = true
		return media.FetchedSource{}, errors.New("must not refetch")
	})
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed || called {
		t.Fatalf("run once claimed=%v err=%v refetched=%v", claimed, err, called)
	}
	job, err := db.GetMediaMaterializationJob(asset.ID)
	if err != nil || job.Status != db.MediaJobSucceeded {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}

func TestMediaMaterializationPollerFetchesProfileContent(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-content-materializer-key")
	if err := db.InitDB(t.TempDir() + "/profile-content-materializer.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var contentCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/provider-content-materializer/content" {
			t.Fatalf("unexpected content request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer profile-content-key" {
			t.Fatalf("content authorization=%q", r.Header.Get("Authorization"))
		}
		contentCalls++
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = io.WriteString(w, "profile-content-bytes")
	}))
	t.Cleanup(server.Close)
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "profile-content-materializer-channel", Name: "Profile Content Materializer", Type: "newapi", BaseURL: server.URL + "/v1", APIKey: "profile-content-key", Enabled: true}); err != nil {
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
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-content-materializer", Name: "Profile Content Materializer", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-content-materializer", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	run := &db.TaskRun{ID: "profile-content-materializer-run", TaskKind: "video", Operation: "video.create", ChannelID: "profile-content-materializer-channel", Engine: "profile", ProfileID: "profile-content-materializer", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingBackground, ProviderTaskID: "provider-content-materializer", TaskStatus: "processing", TaskOutcome: "pending", NextPollAt: &now}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	assets, err := db.EnsureTaskContentMediaContext(context.Background(), run.ID, "video")
	if err != nil || len(assets) != 1 {
		t.Fatalf("ensure content assets=%+v err=%v", assets, err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	poller := NewMediaMaterializationPoller("profile-content-materializer", store)
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("run once claimed=%v err=%v", claimed, err)
	}
	if contentCalls != 1 {
		t.Fatalf("content calls=%d, want 1", contentCalls)
	}
	asset, err := db.GetMediaAssetByID(assets[0].ID)
	if err != nil || asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" || asset.ContentType != "video/mp4" {
		t.Fatalf("materialized asset=%+v err=%v", asset, err)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil || loaded.TaskStatus != "completed" || loaded.TaskOutcome != "success" {
		t.Fatalf("task lifecycle=%+v err=%v", loaded, err)
	}
}

func TestDefaultURLFetcherForProfileAssetScopesFakeIPTrust(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "fake-ip-materializer-key")
	if err := db.InitDB(t.TempDir() + "/fake-ip-materializer.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{ID: "fake-ip-channel", Name: "Fake IP Channel", Type: "newapi", BaseURL: "https://api.fanrenapi.com/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "fake-ip-run", TaskKind: "video", Operation: "video.create", ChannelID: channel.ID, Engine: "profile", PollingMode: protocol.PollingBackground, TaskStatus: "materializing", TaskOutcome: "pending"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}

	fetcher := defaultURLFetcherForAsset(context.Background(), &db.MediaAsset{TaskRunID: run.ID, SourceKind: media.SourceURL, SourceLocator: "https://CDN.fanrenapi.com./video.mp4"})
	if _, ok := fetcher.TrustedFakeIPHosts["cdn.fanrenapi.com"]; !ok || len(fetcher.TrustedFakeIPHosts) != 1 {
		t.Fatalf("trusted Fake-IP hosts = %#v", fetcher.TrustedFakeIPHosts)
	}

	crossDomain := defaultURLFetcherForAsset(context.Background(), &db.MediaAsset{TaskRunID: run.ID, SourceKind: media.SourceURL, SourceLocator: "https://cdn.attacker.example/video.mp4"})
	if len(crossDomain.TrustedFakeIPHosts) != 0 {
		t.Fatalf("cross-domain trusted Fake-IP hosts = %#v", crossDomain.TrustedFakeIPHosts)
	}

	legacy := *run
	legacy.ID = "fake-ip-legacy-run"
	legacy.Engine = "legacy"
	if err := db.CreateTaskRun(&legacy); err != nil {
		t.Fatal(err)
	}
	legacyFetcher := defaultURLFetcherForAsset(context.Background(), &db.MediaAsset{TaskRunID: legacy.ID, SourceKind: media.SourceURL, SourceLocator: "https://cdn.fanrenapi.com/video.mp4"})
	if len(legacyFetcher.TrustedFakeIPHosts) != 0 {
		t.Fatalf("legacy trusted Fake-IP hosts = %#v", legacyFetcher.TrustedFakeIPHosts)
	}
}
