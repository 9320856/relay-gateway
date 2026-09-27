package router

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

func TestProfileClientStalePollCannotWriteOrReleaseReplacementLease(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "client-stale-poll-test-key")
	t.Setenv("RELAY_PROFILE_MEDIA_DISABLED", "0")
	if err := db.InitDB(t.TempDir() + "/client-stale.db"); err != nil {
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
	channel := &db.ChannelModel{ID: "stale-client-channel", Name: "Stale Client", Type: "newapi", BaseURL: server.URL + "/v1", APIKey: "key", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: protocol.MediaRetentionRequired,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 10, MaxDurationMS: 60000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "stale-client-profile", Name: "Stale Client", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "stale-client-profile", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "stale-client-run", TaskKind: "video", Operation: "video.create", ChannelID: channel.ID, Engine: "profile", ProfileID: "stale-client-profile", ProfileRevision: 1, PollingMode: "client", ProviderTaskID: "provider-task", TaskStatus: "processing", TaskOutcome: "pending"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: run.ProviderTaskID}); err != nil {
		t.Fatal(err)
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/provider-task", nil)
	type outcome struct {
		response any
		handled  bool
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		response, handled, err := profileTaskStatus(c, run.ProviderTaskID, asyncTaskKindVideo)
		done <- outcome{response, handled, err}
	}()
	select {
	case <-entered:
	case got := <-done:
		close(release)
		t.Fatalf("client poll did not reach upstream: %+v", got)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("client poll did not reach upstream")
	}
	if err := db.DB.Model(&db.TaskRun{}).Where("id = ?", run.ID).Update("lease_expires_at", time.Now().Add(-time.Minute)).Error; err != nil {
		close(release)
		t.Fatal(err)
	}
	newClaim, err := db.ClaimTaskRunPoll(run.ID, "profile-client", time.Minute)
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
	got := <-done
	response, ok := got.response.(*model.VideoTaskResponse)
	if got.err != nil || !got.handled || !ok || response.Status != "processing" || response.VideoURL != "" {
		t.Fatalf("client exposed stale response: %+v", got)
	}
	current, err := db.GetTaskRun(run.ID)
	if err != nil || current.ResultBody != latestResult || current.LeaseOwner != newClaim.LeaseOwner || current.PollCount != 0 {
		t.Fatalf("old client changed or released replacement: %#v %v", current, err)
	}
	assets, err := db.ListMediaAssetsForTaskRun(run.ID, "video")
	if err != nil || len(assets) != 0 {
		t.Fatalf("old client enqueued media: %#v %v", assets, err)
	}
}
