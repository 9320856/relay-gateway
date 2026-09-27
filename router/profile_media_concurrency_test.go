package router

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/task"
)

func TestProfileSynchronousMediaDoesNotPublishClaimableJob(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "sync-media-concurrency-test-key")
	if err := db.InitDB(t.TempDir() + "/profile-sync-media.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { SetMediaObjectStore(nil); _ = db.Close() })
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldFactory := profileMediaFetcherFactory
	t.Cleanup(func() { profileMediaFetcherFactory = oldFactory })
	profileMediaFetcherFactory = func(string, string) media.SourceFetcher {
		return media.SourceFetcherFunc(func(ctx context.Context, _ media.MediaResult) (media.FetchedSource, error) {
			close(entered)
			select {
			case <-release:
				return media.FetchedSource{Body: io.NopCloser(strings.NewReader("image-bytes")), ContentType: "image/png"}, nil
			case <-ctx.Done():
				return media.FetchedSource{}, ctx.Err()
			}
		})
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "http://gateway.example/v1/images/generations", nil).WithContext(ctx)
	go func() {
		_, err := materializeProfileMediaURL(c, "sync-run", "image", 0, "https://provider.example/image.png", "https://provider.example")
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("synchronous materializer did not reach fetch: %v", err)
	case <-time.After(time.Second):
		t.Fatal("synchronous materializer did not reach fetch")
	}
	claimed, err := task.NewMediaMaterializationPoller("background", store).RunOnce(context.Background())
	close(release)
	if err != nil || claimed {
		t.Fatalf("background stole synchronous job: claimed=%v err=%v", claimed, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("synchronous materializer did not finish")
	}
	assets, err := db.ListMediaAssetsForTaskRun("sync-run", "image")
	if err != nil || len(assets) != 1 || assets[0].Status != db.MediaAssetAvailable {
		t.Fatalf("sync result asset=%#v err=%v", assets, err)
	}
	job, err := db.GetMediaMaterializationJob(assets[0].ID)
	if err != nil || job.Attempts != 1 || job.Status != db.MediaJobSucceeded {
		t.Fatalf("sync completion job=%#v err=%v", job, err)
	}
}
