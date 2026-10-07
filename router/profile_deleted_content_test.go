package router

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/model"
)

func TestDeletedProfileManagedVideoRemainsReadable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, deleteProfile := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_profile_%t", deleteProfile), func(t *testing.T) {
			initAuthTestDB(t)
			store, err := media.NewLocalObjectStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			SetMediaObjectStore(store)
			t.Cleanup(func() { SetMediaObjectStore(nil) })
			profileID := saveRequiredMediaRegressionProfile(t, asyncTaskKindVideo)
			run := &db.TaskRun{ID: "archived-video-run", ChannelID: "removed-channel", TaskKind: asyncTaskKindVideo, Operation: "video.create", Engine: "profile", ProfileID: profileID, ProfileRevision: 1, ProviderTaskID: "archived-video", TaskStatus: model.VideoStatusCompleted, TaskOutcome: "succeeded"}
			if err := db.CreateTaskRunContext(context.Background(), run); err != nil {
				t.Fatal(err)
			}
			if err := db.RecordTaskAlias(&db.TaskAlias{TaskRunID: run.ID, LookupID: run.ProviderTaskID}); err != nil {
				t.Fatal(err)
			}
			const body = "archived-video-bytes"
			info, err := store.PutAtomic(context.Background(), strings.NewReader(body), media.PutMeta{Key: "video/archived.mp4", ContentType: "video/mp4"})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.CreateMediaObject(&db.MediaObject{ID: "archived-object", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady}); err != nil {
				t.Fatal(err)
			}
			if err := db.CreateMediaAsset(&db.MediaAsset{PublicID: "archived-asset", CapabilityHash: "private-test-hash", TaskRunID: run.ID, Kind: asyncTaskKindVideo, Status: db.MediaAssetAvailable, ObjectID: "archived-object", ContentType: info.ContentType, DisplayName: "archive.mp4"}); err != nil {
				t.Fatal(err)
			}
			if deleteProfile {
				err = db.DeleteProtocolProfile(profileID)
			} else {
				err = db.DeleteProtocolProfileRevision(profileID, 1)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.GetProtocolProfileRevision(profileID, 1); err != gorm.ErrRecordNotFound {
				t.Fatalf("revision was not deleted: %v", err)
			}
			engine := gin.New()
			registerPublicVideoContentRoutes(engine.Group("/v1"))
			check := func(method, rangeHeader, etag string, status int, want string) {
				t.Helper()
				request := httptest.NewRequest(method, "/v1/videos/archived-video/content?channel_id=attacker", nil)
				request.Header.Set("Range", rangeHeader)
				request.Header.Set("If-None-Match", etag)
				rec := httptest.NewRecorder()
				engine.ServeHTTP(rec, request)
				if rec.Code != status || rec.Body.String() != want {
					t.Errorf("%s range=%q = %d %q, want %d %q", method, rangeHeader, rec.Code, rec.Body.String(), status, want)
				}
				if rec.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(rec.Header().Get("Content-Security-Policy"), "sandbox") {
					t.Errorf("missing media security headers: %v", rec.Header())
				}
				if status == http.StatusPartialContent && rec.Header().Get("Content-Range") != fmt.Sprintf("bytes 2-5/%d", len(body)) {
					t.Errorf("range response headers: %v", rec.Header())
				}
				if status == http.StatusOK && rec.Header().Get("Content-Length") != fmt.Sprint(len(body)) {
					t.Errorf("content length: %v", rec.Header())
				}
			}
			check(http.MethodGet, "", "", http.StatusOK, body)
			check(http.MethodHead, "", "", http.StatusOK, "")
			check(http.MethodGet, "bytes=2-5", "", http.StatusPartialContent, body[2:6])
			check(http.MethodGet, "", `"`+info.SHA256+`"`, http.StatusNotModified, "")
			var wg sync.WaitGroup
			for range 32 {
				wg.Go(func() { check(http.MethodGet, "", "", http.StatusOK, body) })
			}
			wg.Wait()
		})
	}
}
