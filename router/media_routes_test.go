package router

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/media"
)

func TestMediaPublicURLIgnoresUntrustedForwardedHost(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Request.Host = "gateway.example.test"
	c.Request.Header.Set("X-Forwarded-Host", "attacker.example.test")
	if got := mediaPublicURL(c, "asset", "cap"); got != "http://gateway.example.test/v1/media/asset/cap" {
		t.Fatalf("untrusted forwarded host changed media URL: %q", got)
	}
	t.Setenv("RELAY_TRUST_PROXY", "1")
	if got := mediaPublicURL(c, "asset", "cap"); got != "http://attacker.example.test/v1/media/asset/cap" {
		t.Fatalf("trusted forwarded host was not honored: %q", got)
	}
}

func TestMediaPublicURLTrustsForwardedProtoOnlyForTrustedProxy(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Request.Host = "gateway.example.test"
	c.Request.Header.Set("X-Forwarded-Proto", "https")
	if got := mediaPublicURL(c, "asset", "cap"); got != "http://gateway.example.test/v1/media/asset/cap" {
		t.Fatalf("untrusted forwarded proto changed media URL: %q", got)
	}
	t.Setenv("RELAY_TRUST_PROXY", "1")
	if got := mediaPublicURL(c, "asset", "cap"); got != "https://gateway.example.test/v1/media/asset/cap" {
		t.Fatalf("trusted forwarded proto was not honored: %q", got)
	}
}

func TestGetMediaAssetLinkIsReadOnlyAndStable(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "media-link-route-test-key")
	if err := db.InitDB(t.TempDir() + "/media-link-route.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		SetMediaObjectStore(nil)
		_ = db.Close()
	})
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	info, err := store.PutAtomic(context.Background(), bytes.NewReader([]byte("stable media")), media.PutMeta{Key: "image/stable.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaObject(&db.MediaObject{ID: "stable-object", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady}); err != nil {
		t.Fatal(err)
	}
	publicID, capability, hash, err := db.NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := db.EncryptMediaCapability(capability)
	if err != nil || ciphertext == "" {
		t.Fatalf("encrypt capability: %q, %v", ciphertext, err)
	}
	asset := &db.MediaAsset{PublicID: publicID, CapabilityHash: hash, CapabilityCiphertext: ciphertext, Kind: "image", Status: db.MediaAssetAvailable, ObjectID: "stable-object", ContentType: info.ContentType, ByteSize: info.Size, SHA256: info.SHA256}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}

	getLink := func() string {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets/"+strconv.FormatUint(uint64(asset.ID), 10)+"/link", nil)
		c.Request.Host = "gateway.example.test"
		c.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(asset.ID), 10)}}
		handleGetMediaAssetLink(c)
		if recorder.Code != http.StatusOK {
			t.Fatalf("get link response = %d %q", recorder.Code, recorder.Body.String())
		}
		var response struct {
			ID       uint   `json:"id"`
			PublicID string `json:"public_id"`
			URL      string `json:"url"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.ID != asset.ID || response.PublicID != publicID || response.URL == "" {
			t.Fatalf("unexpected link response: %#v", response)
		}
		return response.URL
	}

	firstURL := getLink()
	afterFirstRead, err := db.GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondURL := getLink()
	afterSecondRead, err := db.GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if firstURL != secondURL {
		t.Fatalf("link changed across reads: first=%q second=%q", firstURL, secondURL)
	}
	if afterFirstRead.CapabilityHash != hash || afterFirstRead.CapabilityCiphertext != ciphertext || afterFirstRead.StateVersion != asset.StateVersion || afterSecondRead.CapabilityHash != hash || afterSecondRead.CapabilityCiphertext != ciphertext || afterSecondRead.StateVersion != asset.StateVersion {
		t.Fatalf("get link mutated asset: first=%#v second=%#v", afterFirstRead, afterSecondRead)
	}
	parsed, err := url.Parse(firstURL)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	Setup().ServeHTTP(response, httptest.NewRequest(http.MethodGet, parsed.RequestURI(), nil))
	if response.Code != http.StatusOK || response.Body.String() != "stable media" {
		t.Fatalf("stable public URL response = %d %q", response.Code, response.Body.String())
	}
}

func TestGetMediaAssetLinkRejectsUnavailableCapability(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-link-unavailable.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	asset := &db.MediaAsset{PublicID: "unrecoverable-link", CapabilityHash: "hash", Kind: "image", Status: db.MediaAssetAvailable}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets/1/link", nil)
	c.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(asset.ID), 10)}}
	handleGetMediaAssetLink(c)
	if recorder.Code != http.StatusConflict || !bytes.Contains(recorder.Body.Bytes(), []byte("媒体公开链接不可恢复")) {
		t.Fatalf("unavailable capability response = %d %q", recorder.Code, recorder.Body.String())
	}
}

func TestGetMediaAssetLinkRejectsDeletedAsset(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-link-deleted.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	asset := &db.MediaAsset{PublicID: "deleted-link", CapabilityHash: "hash", Kind: "image", Status: db.MediaAssetAvailable}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := db.RequestMediaAssetDelete(asset.PublicID, "test"); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets/1/link", nil)
	c.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(asset.ID), 10)}}
	handleGetMediaAssetLink(c)
	if recorder.Code != http.StatusGone {
		t.Fatalf("deleted asset response = %d %q", recorder.Code, recorder.Body.String())
	}

	// Also test expired asset returns StatusGone
	expiredAsset := &db.MediaAsset{PublicID: "expired-link", CapabilityHash: "hash", Kind: "image", Status: db.MediaAssetExpired, TaskRunID: "run-expired-route-test", Ordinal: 0}
	if err := db.CreateMediaAsset(expiredAsset); err != nil {
		t.Fatal(err)
	}
	recorderExp := httptest.NewRecorder()
	cExp, _ := gin.CreateTestContext(recorderExp)
	cExp.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets/"+strconv.FormatUint(uint64(expiredAsset.ID), 10)+"/link", nil)
	cExp.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(expiredAsset.ID), 10)}}
	handleGetMediaAssetLink(cExp)
	if recorderExp.Code != http.StatusGone {
		t.Fatalf("expired asset response = %d %q, want 410", recorderExp.Code, recorderExp.Body.String())
	}
}

func TestRotateMediaAssetLinkRouteIsRemoved(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := Setup()
	request := httptest.NewRequest(http.MethodPost, "/api/media-assets/1/rotate-link", nil)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound && response.Code != http.StatusUnauthorized {
		// Even before auth or after, route should not exist (404)
	}
	// Verify directly on router that no handler is registered for rotate-link
	for _, route := range engine.Routes() {
		if route.Path == "/api/media-assets/:id/rotate-link" {
			t.Fatalf("rotate-link route must not be registered on router: %+v", route)
		}
	}
}

func TestMediaContentRouteSupportsRangeHeadAndRevocation(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-route.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		SetMediaObjectStore(nil)
		_ = db.Close()
	})
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	info, err := store.PutAtomic(context.Background(), bytes.NewReader([]byte("0123456789")), media.PutMeta{Key: "video/task.mp4", ContentType: "video/mp4"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaObject(&db.MediaObject{ID: "obj-1", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady}); err != nil {
		t.Fatal(err)
	}
	publicID, capability, hash, err := db.NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaAsset(&db.MediaAsset{PublicID: publicID, CapabilityHash: hash, Kind: "video", Status: db.MediaAssetAvailable, ObjectID: "obj-1", ContentType: "video/mp4", ByteSize: info.Size, SHA256: info.SHA256, DisplayName: "sample.mp4"}); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	engine := Setup()

	request := httptest.NewRequest(http.MethodGet, "/v1/media/"+publicID+"/"+capability, nil)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "0123456789" || response.Header().Get("ETag") == "" {
		t.Fatalf("GET response = %d %q headers=%v", response.Code, response.Body.String(), response.Header())
	}
	if got := response.Header().Get("Cross-Origin-Resource-Policy"); got != "cross-origin" {
		t.Fatalf("public media must support cross-origin embedding, got %q", got)
	}
	if got := response.Header().Get("X-Frame-Options"); got != "" {
		t.Fatalf("public media must not be blocked by X-Frame-Options, got %q", got)
	}
	csp := response.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "media-src") || !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "sandbox allow-scripts allow-same-origin") {
		t.Fatalf("public media CSP must allow media playback and script sandbox, got %q", csp)
	}

	// Test suffix .mp4 on capability
	reqDotMP4 := httptest.NewRequest(http.MethodGet, "/v1/media/"+publicID+"/"+capability+".mp4", nil)
	recDotMP4 := httptest.NewRecorder()
	engine.ServeHTTP(recDotMP4, reqDotMP4)
	if recDotMP4.Code != http.StatusOK || recDotMP4.Body.String() != "0123456789" {
		t.Fatalf("GET with .mp4 suffix response = %d %q", recDotMP4.Code, recDotMP4.Body.String())
	}

	// Test wildcard filename path /video.mp4
	reqWildcard := httptest.NewRequest(http.MethodGet, "/v1/media/"+publicID+"/"+capability+"/video.mp4", nil)
	recWildcard := httptest.NewRecorder()
	engine.ServeHTTP(recWildcard, reqWildcard)
	if recWildcard.Code != http.StatusOK || recWildcard.Body.String() != "0123456789" {
		t.Fatalf("GET with wildcard filename response = %d %q", recWildcard.Code, recWildcard.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/media/"+publicID+"/"+capability, nil)
	request.Header.Set("Range", "bytes=2-5")
	response = httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusPartialContent || response.Body.String() != "2345" || response.Header().Get("Content-Range") != "bytes 2-5/10" {
		t.Fatalf("range response = %d %q range=%q", response.Code, response.Body.String(), response.Header().Get("Content-Range"))
	}

	request = httptest.NewRequest(http.MethodHead, "/v1/media/"+publicID+"/"+capability, nil)
	response = httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.Len() != 0 || response.Header().Get("Content-Length") != "10" {
		t.Fatalf("HEAD response = %d body=%d length=%q", response.Code, response.Body.Len(), response.Header().Get("Content-Length"))
	}

	if err := db.RequestMediaAssetDelete(publicID, "test"); err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/media/"+publicID+"/"+capability, nil)
	response = httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusGone {
		t.Fatalf("revoked media response = %d, want 410", response.Code)
	}
}

func TestMediaAdminContentStreamsAvailableAssetOnly(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-admin-content.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		SetMediaObjectStore(nil)
		_ = db.Close()
	})
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	info, err := store.PutAtomic(context.Background(), bytes.NewReader([]byte("preview")), media.PutMeta{Key: "image/preview.png", ContentType: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaObject(&db.MediaObject{ID: "obj-admin-preview", Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, State: db.MediaObjectReady}); err != nil {
		t.Fatal(err)
	}
	asset := &db.MediaAsset{PublicID: "admin-preview", CapabilityHash: "hash", Kind: "image", Status: db.MediaAssetAvailable, ObjectID: "obj-admin-preview", ContentType: "image/png", ByteSize: info.Size, DisplayName: "preview.png"}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets/1/content", nil)
	c.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(asset.ID), 10)}}
	handleGetMediaAssetAdminContent(c)
	if recorder.Code != http.StatusOK || recorder.Body.String() != "preview" || recorder.Header().Get("Content-Type") != "image/png" || recorder.Header().Get("Cache-Control") != "private, no-cache" {
		t.Fatalf("admin preview response = %d %q headers=%v", recorder.Code, recorder.Body.String(), recorder.Header())
	}
	if got := recorder.Header().Get("Cross-Origin-Resource-Policy"); got != "same-origin" {
		t.Fatalf("admin preview must remain restricted to the gateway origin, got %q", got)
	}

	if err := db.RequestMediaAssetDelete(asset.PublicID, "test"); err != nil {
		t.Fatal(err)
	}
	if revoked, err := db.GetMediaAssetByID(asset.ID); err != nil || revoked.Status != db.MediaAssetDeleteRequested {
		t.Fatalf("revoked asset = %#v, %v", revoked, err)
	}
	recorder = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets/1/content", nil)
	c.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(asset.ID), 10)}}
	handleGetMediaAssetAdminContent(c)
	if c.Writer.Status() != http.StatusNotFound {
		t.Fatalf("revoked admin preview status = %d, want %d", c.Writer.Status(), http.StatusNotFound)
	}
	if recorder.Header().Get("Cache-Control") != "private, no-cache" {
		t.Fatalf("revoked admin preview cache-control = %q, want private, no-cache", recorder.Header().Get("Cache-Control"))
	}
}

func TestDeleteMediaAssetReturnsActualIdempotentStatus(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-delete-status.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	asset := &db.MediaAsset{PublicID: "already-deleted", CapabilityHash: "hash", Kind: "image", Status: db.MediaAssetDeleted}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodDelete, "/api/media-assets/1", nil)
	c.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(asset.ID), 10)}}
	handleDeleteMediaAsset(c)
	if recorder.Code != http.StatusAccepted || !bytes.Contains(recorder.Body.Bytes(), []byte(`"status":"deleted"`)) {
		t.Fatalf("idempotent delete response = %d %q", recorder.Code, recorder.Body.String())
	}
}

func TestRetryMediaAssetRouteDispatchesDeletionRetry(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-retry-route.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	asset := &db.MediaAsset{PublicID: "route-delete-failed", CapabilityHash: "hash", Kind: "video", Status: db.MediaAssetDeleteFailed}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(asset.ID), 10)}}
	handleRetryMediaAsset(c)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("retry response = %d %q", recorder.Code, recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"status":"delete_requested"`)) {
		t.Fatalf("retry response did not report queued deletion: %q", recorder.Body.String())
	}
	loaded, err := db.GetMediaAssetByID(asset.ID)
	if err != nil || loaded.Status != db.MediaAssetDeleteRequested {
		t.Fatalf("retried asset = %#v, %v", loaded, err)
	}
	job, err := db.GetMediaDeletionJob(asset.ID)
	if err != nil || job.Status != db.MediaJobPending {
		t.Fatalf("retried deletion job = %#v, %v", job, err)
	}
}

func TestListMediaAssetsRouteFiltersAndSearches(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-search-route.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.CreateMediaAsset(&db.MediaAsset{PublicID: "p1", CapabilityHash: "h1", Kind: "video", Status: db.MediaAssetAvailable, TaskRunID: "task_video_123", DisplayName: "cat_animation.mp4"}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaAsset(&db.MediaAsset{PublicID: "p2", CapabilityHash: "h2", Kind: "image", Status: db.MediaAssetAvailable, TaskRunID: "task_img_456", DisplayName: "dog_portrait.png"}); err != nil {
		t.Fatal(err)
	}

	// 1. Search by task_run_id
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets?q=video_123", nil)
	handleListMediaAssets(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("search response = %d %q", recorder.Code, recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte("cat_animation.mp4")) || bytes.Contains(recorder.Body.Bytes(), []byte("dog_portrait.png")) {
		t.Fatalf("search did not filter properly: %s", recorder.Body.String())
	}

	// 2. Search by display_name
	recorder = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets?q=dog", nil)
	handleListMediaAssets(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("search response = %d %q", recorder.Code, recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte("dog_portrait.png")) || bytes.Contains(recorder.Body.Bytes(), []byte("cat_animation.mp4")) {
		t.Fatalf("search did not filter properly: %s", recorder.Body.String())
	}
}

func TestListMediaAssetsRouteIncludesRecoverablePublicURLOnly(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "media-list-public-url-test-key")
	if err := db.InitDB(t.TempDir() + "/media-list-public-url.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	publicID, capability, hash, err := db.NewMediaLinkIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := db.EncryptMediaCapability(capability)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaAsset(&db.MediaAsset{PublicID: publicID, CapabilityHash: hash, CapabilityCiphertext: ciphertext, Kind: "image", Status: db.MediaAssetAvailable, ObjectID: "object-ready", TaskRunID: "recoverable-run"}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaAsset(&db.MediaAsset{PublicID: "legacy-unrecoverable", CapabilityHash: "hash", Kind: "image", Status: db.MediaAssetAvailable, ObjectID: "object-ready", TaskRunID: "legacy-run"}); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/media-assets", nil)
	c.Request.Host = "gateway.example.test"
	handleListMediaAssets(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list response = %d %q", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data []struct {
			PublicID       string `json:"public_id"`
			PublicURL      string `json:"public_url"`
			PublicURLError string `json:"public_url_error"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data) != 2 {
		t.Fatalf("unexpected list data: %#v", response.Data)
	}
	var foundRecoverable, foundUnrecoverable bool
	for _, item := range response.Data {
		switch item.PublicID {
		case publicID:
			parsed, parseErr := url.Parse(item.PublicURL)
			foundRecoverable = parseErr == nil && parsed.Scheme == "http" && parsed.Host == "gateway.example.test" && strings.HasPrefix(parsed.Path, "/v1/media/"+publicID+"/") && item.PublicURLError == ""
		case "legacy-unrecoverable":
			foundUnrecoverable = item.PublicURL == "" && item.PublicURLError != ""
		}
	}
	if !foundRecoverable || !foundUnrecoverable {
		t.Fatalf("unexpected public URL projection: %#v", response.Data)
	}
}
