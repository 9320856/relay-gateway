package task

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
)

// This fixture starts as an attached, completed Profile task with a compiled
// required-retention contract. Its durable content job deliberately has not
// finished yet, as can happen when older task history is retained.
type retainedTaskFixture struct {
	dbPath   string
	storeDir string
	run      *db.TaskRun
	assetID  uint
	compiled protocol.CompiledProfile
}

func newRetainedTaskFixture(t *testing.T, baseURL string) *retainedTaskFixture {
	t.Helper()
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "retained-task-content-test-key")
	t.Setenv("RELAY_PROFILE_MEDIA_DISABLED", "0")
	f := &retainedTaskFixture{dbPath: filepath.Join(t.TempDir(), "retained-task.db"), storeDir: t.TempDir()}
	if err := db.InitDB(f.dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "retained-channel", Name: "Retained Channel", Type: "newapi", BaseURL: baseURL + "/v1", APIKey: "captured-channel-key", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingBackground, MediaRetention: protocol.MediaRetentionRequired,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", Headers: map[string]string{"X-Captured-Operation": "poll"}, IntervalMS: 1, MaxAttempts: 10, MaxDurationMS: 60000,
			StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
		Content: &protocol.Content{Method: http.MethodGet, Path: "/videos/{task_id}/content", Headers: map[string]string{"X-Captured-Operation": "content"}},
	}}}
	var err error
	f.compiled, err = protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "retained-profile", Name: "Retained Profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	saveRetainedTaskRevision(t, f.compiled)
	f.run = &db.TaskRun{ID: "retained-run", TaskKind: "video", Operation: "video.create", ChannelID: "retained-channel", Engine: "profile",
		ProfileID: "retained-profile", ProfileRevision: 1, ProfileDigest: f.compiled.Digest(), PollingMode: protocol.PollingBackground,
		ProviderTaskID: "captured-provider-task", TaskStatus: "completed", TaskOutcome: "success", ResultBody: `{"id":"captured-provider-task","status":"completed"}`}
	if err := db.CreateTaskRun(f.run); err != nil {
		t.Fatal(err)
	}
	assets, err := db.EnsureTaskContentMediaContext(context.Background(), f.run.ID, "video")
	if err != nil || len(assets) != 1 || assets[0].SourceKind != media.SourceProviderContent || assets[0].Status != db.MediaAssetPending {
		t.Fatalf("enqueue completed attached task content: assets=%+v err=%v", assets, err)
	}
	f.assetID = assets[0].ID
	return f
}

func saveRetainedTaskRevision(t *testing.T, compiled protocol.CompiledProfile) {
	t.Helper()
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "retained-profile", Revision: 1,
		SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
}

func (f *retainedTaskFixture) deleteCatalog(t *testing.T, kind string) {
	t.Helper()
	var err error
	if kind == "profile" {
		err = db.DeleteProtocolProfile(f.run.ProfileID)
	} else {
		err = db.DeleteProtocolProfileRevision(f.run.ProfileID, f.run.ProfileRevision)
	}
	if err != nil {
		t.Fatalf("delete %s with completed pending content: %v", kind, err)
	}
}

func (f *retainedTaskFixture) restart(t *testing.T) *media.LocalObjectStore {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitDB(f.dbPath); err != nil {
		t.Fatalf("reopen retained history database: %v", err)
	}
	run, err := db.GetTaskRun(f.run.ID)
	if err != nil || run.ProfileID != "" || run.ProfileRevision != 0 || run.ProfileDigest != f.compiled.Digest() || run.ChannelID != f.run.ChannelID || run.ProviderTaskID != f.run.ProviderTaskID || run.TaskStatus != "completed" {
		t.Fatalf("detached task after restart: run=%+v err=%v", run, err)
	}
	store, err := media.NewLocalObjectStore(f.storeDir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func (f *retainedTaskFixture) replaceCatalog(t *testing.T, kind string) {
	t.Helper()
	if kind == "profile" {
		if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: f.run.ProfileID, Name: "Replacement Profile", Source: db.ProfileSourceCustom}); err != nil {
			t.Fatal(err)
		}
	}
	source := f.compiled.Profile()
	source.Operations[0].Content = &protocol.Content{Method: http.MethodGet, Path: "/replacement/{task_id}/content"}
	source.Operations[0].Poll.Path = "/replacement/{task_id}"
	replacement, err := protocol.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Digest() == f.compiled.Digest() {
		t.Fatal("replacement catalog must have a different digest")
	}
	saveRetainedTaskRevision(t, replacement)
}

func TestRetainedProfileContentMaterializesAfterDeletionAndRestart(t *testing.T) {
	for _, kind := range []string{"profile", "revision"} {
		for _, initial := range []string{"pending", "failed"} {
			t.Run(kind+"/"+initial, func(t *testing.T) {
				var contentCalls, unexpectedCalls atomic.Int32
				var failContent atomic.Bool
				failContent.Store(initial == "failed")
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/captured-provider-task/content" || r.Header.Get("Authorization") != "Bearer captured-channel-key" || r.Header.Get("X-Captured-Operation") != "content" {
						unexpectedCalls.Add(1)
						t.Errorf("request escaped captured Content operation: %s %s authorization=%q operation=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Captured-Operation"))
						http.Error(w, "unexpected operation", http.StatusBadRequest)
						return
					}
					contentCalls.Add(1)
					if failContent.Load() {
						http.Error(w, "retry later", http.StatusServiceUnavailable)
						return
					}
					w.Header().Set("Content-Type", "video/mp4")
					_, _ = io.WriteString(w, "retained-video-bytes")
				}))
				t.Cleanup(server.Close)
				fixture := newRetainedTaskFixture(t, server.URL)
				if initial == "failed" {
					store, err := media.NewLocalObjectStore(fixture.storeDir, 1024)
					if err != nil {
						t.Fatal(err)
					}
					claimed, err := NewMediaMaterializationPoller("before-restart", store).RunOnce(context.Background())
					if !claimed || err == nil {
						t.Fatalf("initial content failure: claimed=%v err=%v", claimed, err)
					}
					asset, err := db.GetMediaAssetByID(fixture.assetID)
					if err != nil || asset.Status != db.MediaAssetFailed {
						t.Fatalf("failed durable content asset: %+v %v", asset, err)
					}
				}
				fixture.deleteCatalog(t, kind)
				store := fixture.restart(t)
				fixture.replaceCatalog(t, kind)
				failContent.Store(false)
				if initial == "failed" {
					if err := db.RetryMediaAssetMaterialization(fixture.assetID); err != nil {
						t.Fatalf("retry detached historical content: %v", err)
					}
				}
				claimed, err := NewMediaMaterializationPoller("after-restart", store).RunOnce(context.Background())
				if err != nil || !claimed {
					t.Fatalf("materialize retained content: claimed=%v err=%v", claimed, err)
				}
				asset, err := db.GetMediaAssetByID(fixture.assetID)
				if err != nil || asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" || asset.ContentType != "video/mp4" {
					t.Fatalf("retained asset: %+v %v", asset, err)
				}
				object, err := db.GetMediaObjectByID(asset.ObjectID)
				if err != nil || object.State != db.MediaObjectReady {
					t.Fatalf("retained object: %+v %v", object, err)
				}
				body, _, err := store.Open(context.Background(), object.StorageKey, nil)
				if err != nil {
					t.Fatal(err)
				}
				stored, readErr := io.ReadAll(body)
				_ = body.Close()
				if readErr != nil || string(stored) != "retained-video-bytes" {
					t.Fatalf("retained object bytes=%q err=%v", stored, readErr)
				}
				job, err := db.GetMediaMaterializationJob(fixture.assetID)
				if err != nil || job.Status != db.MediaJobSucceeded {
					t.Fatalf("retained content job: %+v %v", job, err)
				}
				wantCalls := int32(1)
				if initial == "failed" {
					wantCalls++
				}
				if contentCalls.Load() != wantCalls || unexpectedCalls.Load() != 0 {
					t.Fatalf("content calls=%d want=%d unexpected=%d", contentCalls.Load(), wantCalls, unexpectedCalls.Load())
				}
			})
		}
	}
}

func TestRetainedProfileSnapshotMissingOrCorruptFailsClosed(t *testing.T) {
	for _, invalid := range []string{"missing", "corrupt"} {
		t.Run(invalid, func(t *testing.T) {
			var providerCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				providerCalls.Add(1)
				http.Error(w, "snapshot must be validated first", http.StatusInternalServerError)
			}))
			t.Cleanup(server.Close)
			fixture := newRetainedTaskFixture(t, server.URL)
			fixture.deleteCatalog(t, "profile")
			store := fixture.restart(t)
			if invalid == "missing" {
				if err := db.DB.Where("content_digest = ?", fixture.compiled.Digest()).Delete(&db.ProtocolProfileSnapshot{}).Error; err != nil {
					t.Fatal(err)
				}
			} else {
				source := fixture.compiled.Profile()
				source.Operations[0].Content = &protocol.Content{Method: http.MethodGet, Path: "/tampered/{task_id}/content"}
				corrupt, err := protocol.Compile(source)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.DB.Model(&db.ProtocolProfileSnapshot{}).Where("content_digest = ?", fixture.compiled.Digest()).Update("content_json", string(corrupt.CanonicalJSON())).Error; err != nil {
					t.Fatal(err)
				}
			}
			run, err := db.GetTaskRun(fixture.run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ProfilePoll(context.Background(), run); err == nil {
				t.Fatal("poll accepted an invalid historical snapshot")
			}
			claimed, err := NewMediaMaterializationPoller("invalid-snapshot", store).RunOnce(context.Background())
			if !claimed || err == nil {
				t.Fatalf("materializer accepted invalid snapshot: claimed=%v err=%v", claimed, err)
			}
			asset, err := db.GetMediaAssetByID(fixture.assetID)
			if err != nil || asset.Status != db.MediaAssetFailed || asset.ObjectID != "" || providerCalls.Load() != 0 {
				t.Fatalf("invalid snapshot escaped validation: asset=%+v err=%v providerCalls=%d", asset, err, providerCalls.Load())
			}
		})
	}
}

func TestRetainedProfilePollUsesCapturedOperationAfterDeletion(t *testing.T) {
	var pollCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/captured-provider-task" || r.Header.Get("Authorization") != "Bearer captured-channel-key" || r.Header.Get("X-Captured-Operation") != "poll" {
			t.Errorf("unexpected retained poll: %s %s authorization=%q operation=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Captured-Operation"))
			http.Error(w, "unexpected operation", http.StatusBadRequest)
			return
		}
		pollCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	t.Cleanup(server.Close)
	fixture := newRetainedTaskFixture(t, server.URL)
	fixture.deleteCatalog(t, "revision")
	fixture.restart(t)
	fixture.replaceCatalog(t, "revision")
	run, err := db.GetTaskRun(fixture.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := ProfilePoll(context.Background(), run)
	if err != nil || observation.Status != "materializing" || !observation.PausePolling || pollCalls.Load() != 1 {
		t.Fatalf("historical poll: observation=%+v err=%v calls=%d", observation, err, pollCalls.Load())
	}
}

func TestRetainedProfileContentDoesNotForwardCredentialsOnRedirect(t *testing.T) {
	var redirectCalls atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectCalls.Add(1)
		t.Errorf("provider redirect followed with authorization=%q", r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, "redirected-video")
	}))
	t.Cleanup(redirectTarget.Close)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/videos/captured-provider-task/content" || r.Header.Get("Authorization") != "Bearer captured-channel-key" {
			t.Errorf("unexpected declared content request: %s authorization=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	t.Cleanup(server.Close)
	fixture := newRetainedTaskFixture(t, server.URL)
	fixture.deleteCatalog(t, "profile")
	store := fixture.restart(t)
	claimed, err := NewMediaMaterializationPoller("retained-redirect", store).RunOnce(context.Background())
	if !claimed || err == nil || redirectCalls.Load() != 0 {
		t.Fatalf("redirect escaped declared Content operation: claimed=%v err=%v targetCalls=%d", claimed, err, redirectCalls.Load())
	}
}
