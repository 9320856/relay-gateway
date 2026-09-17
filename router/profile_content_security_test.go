package router

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/adapter"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

func TestProfileContentExecutorErrorPreservesUpstreamResponse(t *testing.T) {
	source := &protocol.ExecutorError{
		HTTPStatus:  http.StatusServiceUnavailable,
		ContentType: "application/json",
		Body:        `{"error":"temporarily unavailable"}`,
		RetryAfter:  "30",
	}
	converted := profileContentExecutorError(source)
	var upstreamErr *adapter.UpstreamHTTPError
	if !errors.As(converted, &upstreamErr) {
		t.Fatalf("expected UpstreamHTTPError, got %T", converted)
	}
	if upstreamErr.StatusCode != source.HTTPStatus || upstreamErr.ContentType != source.ContentType || upstreamErr.Body != source.Body || upstreamErr.RetryAfter != source.RetryAfter {
		t.Fatalf("upstream response metadata changed: %+v", upstreamErr)
	}
}

func TestProfileVideoContentRejectsActiveHTMLAndSetsSandboxCSP(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-security.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, "<script>fetch('/api/auth/me')</script>")
	}))
	t.Cleanup(upstream.Close)

	channel := &db.ChannelModel{
		ID:        "sec-content-channel",
		Name:      "Security Content Channel",
		Type:      "openai",
		BaseURL:   upstream.URL + "/v1",
		APIKey:    "profile-key",
		Enabled:   true,
		Priority:  1,
		Weight:    1,
		ModelsRaw: "sec-content-model",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	profile := protocol.Profile{
		SchemaVersion: protocol.CurrentSchemaVersion,
		Name:          "Security Content Profile",
		Operations: []protocol.Operation{{
			Operation:      "video.create",
			ExecutionMode:  protocol.ExecutionAsync,
			PollingMode:    protocol.PollingClient,
			MediaRetention: protocol.MediaRetentionBestEffort,
			Submit:         protocol.Submit{Method: http.MethodPost, Path: "/videos/generations", BodyEncoding: "json"},
			Response:       protocol.Response{TaskIDPaths: []string{"id", "task_id"}},
			Poll:           &protocol.Poll{Method: http.MethodGet, Path: "/videos/generations/{task_id}", IntervalMS: 1, MaxAttempts: 1, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"url"}},
			Content:        &protocol.Content{Method: http.MethodGet, Path: "/videos/generations/{task_id}/content"},
		}},
	}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "sec-profile", Name: "Security Profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{
		ProfileID: "sec-profile", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion,
		ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "video.create", ModelPattern: "sec-content-model",
		ProfileID: "sec-profile", ProfileRevision: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	run := &db.TaskRun{
		ID:              "sec-task-run",
		TaskKind:        asyncTaskKindVideo,
		Operation:       "video.create",
		ChannelID:       channel.ID,
		Engine:          "profile",
		ProfileID:       "sec-profile",
		ProfileRevision: 1,
		ProfileDigest:   compiled.Digest(),
		PollingMode:     protocol.PollingClient,
		ProviderTaskID:  "sec-provider-task",
		SubmissionState: "accepted",
		TaskStatus:      model.VideoStatusProcessing,
	}
	if err := db.CreateTaskRunContext(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: run.ProviderTaskID}); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/videos/sec-provider-task/content", nil)

	handled, err := profileTaskContent(ctx, "sec-provider-task")
	if err != nil || !handled {
		t.Fatalf("profileTaskContent handled=%v err=%v", handled, err)
	}

	csp := rec.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "sandbox") || !strings.Contains(csp, "default-src 'none'") {
		t.Fatalf("missing required CSP sandbox header: %q", csp)
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("missing X-Content-Type-Options: nosniff header: %v", rec.Header())
	}
	if got := rec.Header().Get("Cross-Origin-Resource-Policy"); got != "cross-origin" {
		t.Fatalf("public profile content must support cross-origin embedding, got %q", got)
	}
	contentType := rec.Header().Get("Content-Type")
	if strings.Contains(contentType, "text/html") {
		t.Fatalf("upstream text/html was reflected in response Content-Type: %q", contentType)
	}
	if contentType != "application/octet-stream" {
		t.Fatalf("expected application/octet-stream fallback, got %q", contentType)
	}
	disposition := rec.Header().Get("Content-Disposition")
	if !strings.HasPrefix(disposition, "attachment") {
		t.Fatalf("expected attachment disposition for non-video MIME, got %q", disposition)
	}
}
