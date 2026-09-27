package task

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/protocol"
)

func TestProfilePollStaleResponseCannotPersistResultOrEnqueueMedia(t *testing.T) {
	t.Setenv("RELAY_PROFILE_MEDIA_DISABLED", "0")
	if err := db.InitDB(t.TempDir() + "/profile-stale.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"completed","video_url":"https://cdn.example/old.mp4"}`)
	}))
	t.Cleanup(server.Close)
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "stale-profile-channel", Name: "Stale Profile", Type: "newapi", BaseURL: server.URL + "/v1", APIKey: "key", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingBackground, MediaRetention: protocol.MediaRetentionRequired,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 10, MaxDurationMS: 60000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "stale-profile", Name: "Stale Profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "stale-profile", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "stale-profile-run", TaskKind: "video", Operation: "video.create", ChannelID: "stale-profile-channel", Engine: "profile", ProfileID: "stale-profile", ProfileRevision: 1, PollingMode: "background", ProviderTaskID: "provider-task", TaskStatus: "processing", TaskOutcome: "pending"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	old, err := db.ClaimDueTaskRun("same-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := ProfilePoll(context.Background(), old); done <- err }()
	select {
	case <-entered:
	case err := <-done:
		close(release)
		t.Fatalf("poll did not reach server: %v", err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("poll did not reach server")
	}
	if err := db.DB.Model(&db.TaskRun{}).Where("id = ?", run.ID).Update("lease_expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		close(release)
		t.Fatal(err)
	}
	newClaim, err := db.ClaimDueTaskRun("same-worker", time.Minute)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	const latestResult = `{"status":"processing","generation":"new"}`
	if err := db.WithTaskRunLeaseContext(context.Background(), run.ID, newClaim.LeaseOwner, func(ctx context.Context) error {
		return db.UpdateTaskRunResultContext(ctx, run.ID, latestResult, false)
	}); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, db.ErrTaskLeaseOwner) {
		t.Fatalf("old profile response accepted: %v", err)
	}
	current, err := db.GetTaskRun(run.ID)
	if err != nil || current.ResultBody != latestResult || current.LeaseOwner != newClaim.LeaseOwner {
		t.Fatalf("old response overwrote current result: %#v %v", current, err)
	}
	assets, err := db.ListMediaAssetsForTaskRun(run.ID, "video")
	if err != nil || len(assets) != 0 {
		t.Fatalf("old response enqueued media: %#v %v", assets, err)
	}
}
