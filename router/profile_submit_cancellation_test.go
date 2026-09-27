package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

func TestProfileCreateCancelsWhileWaitingForIdempotencyOwner(t *testing.T) {
	for _, kind := range []string{"video", "image"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
			t.Setenv("RELAY_ENABLE_PROFILE_IMAGE_ENGINE", "1")
			if err := db.InitDB(t.TempDir() + "/lock.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			operation := kind + ".create"
			channel := &db.ChannelModel{ID: "canceled-submit", Name: "cancel", Type: "newapi", BaseURL: "http://127.0.0.1:1/v1", Enabled: true}
			if err := db.SaveChannelModel(channel); err != nil {
				t.Fatal(err)
			}
			profile, err := protocol.Compile(protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
				Operation: operation, ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, MediaRetention: protocol.MediaRetentionDisabled,
				Submit: protocol.Submit{Method: http.MethodPost, Path: "/jobs", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
				Poll: &protocol.Poll{Method: http.MethodGet, Path: "/jobs/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"url"}},
			}}})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "canceled-submit", Name: "cancel", Source: db.ProfileSourceCustom}); err != nil {
				t.Fatal(err)
			}
			if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "canceled-submit", Revision: 1, SchemaVersion: protocol.CurrentSchemaVersion, State: db.ProfileRevisionPublished, ContentJSON: string(profile.CanonicalJSON()), ContentDigest: profile.Digest()}); err != nil {
				t.Fatal(err)
			}
			if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, ProfileID: "canceled-submit", ProfileRevision: 1, Operation: operation, ModelPattern: "test-model", Enabled: true}); err != nil {
				t.Fatal(err)
			}
			const key = "cancel-waiting-submit"
			lockKey := key + "\x00" + operation
			unlock, err := profileSubmitLocks.acquire(context.Background(), lockKey)
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/jobs", nil)
			c.Request.Header.Set("Idempotency-Key", key)
			upstream := channel.ToUpstreamChannel()
			done := make(chan error, 1)
			go func() {
				if kind == "video" {
					_, _, handled, err := profileEngineVideoCreateForChannel(c, &model.VideoGenerationRequest{Model: "test-model", Prompt: "test"}, &upstream)
					if !handled {
						err = errors.New("video route was not handled")
					}
					done <- err
				} else {
					_, _, handled, err := profileEngineImageCreateForChannel(c, &model.ImageGenerationRequest{Model: "test-model", Prompt: "test"}, &upstream)
					if !handled {
						err = errors.New("image route was not handled")
					}
					done <- err
				}
			}()
			deadline := time.Now().Add(time.Second)
			for {
				profileSubmitLocks.mu.Lock()
				waiting := profileSubmitLocks.items[lockKey].refs == 2
				profileSubmitLocks.mu.Unlock()
				if waiting {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("request did not wait on submit lock: %v", err)
				default:
				}
				if time.Now().After(deadline) {
					cancel()
					unlock()
					<-done
					t.Fatal("request did not reach submit lock")
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled submit returned %v", err)
				}
			case <-time.After(time.Second):
				unlock()
				<-done
				t.Fatal("canceled request waited for running submit")
			}
			var reservations int64
			if err := db.DB.Model(&db.TaskRun{}).Count(&reservations).Error; err != nil || reservations != 0 {
				t.Fatalf("canceled waiter reserved task: count=%d err=%v", reservations, err)
			}
		})
	}
}
