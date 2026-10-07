package router

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/audit"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
	"relay-gateway/security"
)

// Exercise the deployment boundary: TLS terminates at the proxy, while the
// gateway sees an ordinary HTTP request from its actual loopback peer.
func TestMediaURLsThroughHTTPSReverseProxy(t *testing.T) {
	initAuthTestDB(t)
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "reverse-proxy-media-test-key")
	store, err := media.NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	SetMediaObjectStore(store)
	t.Cleanup(func() { SetMediaObjectStore(nil) })
	session, _, err := security.SetupAdmin("proxyadmin", "correct horse battery", "127.0.0.1", "proxy-test")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := audit.Start("api_call", "127.0.0.1", http.MethodPost, "/v1/images/generations", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.RecordResult(http.StatusOK, []byte(`{"data":[]}`), nil)
	imageBytes := []byte("\x89PNG\r\n\x1a\n")
	imageSource := "data:image/png;base64," + base64.StdEncoding.EncodeToString(imageBytes)
	videoBytes := []byte("proxy video bytes")
	videoSource := "data:video/mp4;base64," + base64.StdEncoding.EncodeToString(videoBytes)
	engine := Setup()
	// Small test-only endpoints call the same retention path as the
	// generation handlers, without depending on a provider or external fetch.
	engine.GET("/test/generated-image", func(c *gin.Context) {
		c.Request = c.Request.WithContext(audit.WithAudit(c.Request.Context(), entry))
		response := profileImageResponse(protocol.Result{JSON: map[string]any{"data": []any{map[string]any{"url": imageSource}}}, ResultURLs: []string{imageSource}}, false)
		if err := retainMediaResponse(c, "proxy-image-run", "image", "", media.RetentionRequired, response, []string{imageSource}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, response)
	})
	engine.GET("/test/generated-video", func(c *gin.Context) {
		response := gin.H{"url": videoSource}
		if err := retainMediaResponse(c, "proxy-video-run", "video", "", media.RetentionRequired, response, []string{videoSource}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, response)
	})
	backend := httptest.NewServer(engine)
	t.Cleanup(backend.Close)
	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	reverseProxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := reverseProxy.Director
	reverseProxy.Director = func(r *http.Request) {
		externalHost := r.Host
		originalDirector(r)
		r.Host = externalHost
		// Overwrite client-supplied forwarding values at the trusted boundary.
		r.Header.Set("X-Forwarded-Host", externalHost)
		r.Header.Set("X-Forwarded-Proto", "https")
	}
	proxy := httptest.NewTLSServer(reverseProxy)
	t.Cleanup(proxy.Close)
	client := proxy.Client()
	requestJSON := func(path string, authenticated bool, output any) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, proxy.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if authenticated {
			req.AddCookie(&http.Cookie{Name: security.SessionCookieName, Value: session.Token})
		}
		// These must not determine the emitted origin when the proxy overwrites them.
		req.Header.Set("X-Forwarded-Host", "attacker.example.test")
		req.Header.Set("X-Forwarded-Proto", "http")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(response.Body)
			t.Fatalf("%s returned %d: %s", path, response.StatusCode, body)
		}
		if err := json.NewDecoder(response.Body).Decode(output); err != nil {
			t.Fatal(err)
		}
	}
	var generated struct {
		Data []struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	requestJSON("/test/generated-image", false, &generated)
	if len(generated.Data) != 1 {
		t.Fatalf("generated image response = %+v", generated)
	}
	assets, err := db.ListMediaAssetsForTaskRunContext(context.Background(), "proxy-image-run", "image")
	if err != nil || len(assets) != 1 {
		t.Fatalf("generated image assets = %+v, %v", assets, err)
	}
	asset := assets[0]
	capability, err := db.DecryptMediaCapability(asset.CapabilityCiphertext)
	if err != nil || capability == "" {
		t.Fatalf("recover fixture capability: %v", err)
	}
	wantURL := proxy.URL + "/v1/media/" + asset.PublicID + "/" + capability
	if generated.Data[0].URL != wantURL {
		t.Fatalf("generated image URL = %q, want %q", generated.Data[0].URL, wantURL)
	}
	var list struct {
		Data []struct {
			ID        uint   `json:"id"`
			PublicURL string `json:"public_url"`
		} `json:"data"`
	}
	requestJSON("/api/media-assets?kind=image&status=available", true, &list)
	if len(list.Data) != 1 || list.Data[0].ID != asset.ID || list.Data[0].PublicURL != wantURL {
		t.Fatalf("admin media list = %+v, want %q", list, wantURL)
	}
	var link struct {
		URL string `json:"url"`
	}
	linkPath := "/api/media-assets/" + strconv.FormatUint(uint64(asset.ID), 10) + "/link"
	requestJSON(linkPath, true, &link)
	if link.URL != wantURL {
		t.Fatalf("admin media link = %q, want %q", link.URL, wantURL)
	}
	var detail audit.Detail
	requestJSON("/api/logs/"+entry.ID, true, &detail)
	if len(detail.MediaAssets) != 1 || detail.MediaAssets[0].PublicURL != wantURL {
		t.Fatalf("log media assets = %+v, want %q", detail.MediaAssets, wantURL)
	}
	for _, path := range []string{"/api/media-assets", linkPath, "/api/logs/" + entry.ID} {
		response, err := client.Get(proxy.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthenticated %s returned %d", path, response.StatusCode)
		}
	}
	assertProxyMediaDownload(t, client, link.URL, imageBytes)
	var video struct {
		URL string `json:"url"`
	}
	requestJSON("/test/generated-video", false, &video)
	videoURL, err := url.Parse(video.URL)
	if err != nil || videoURL.Scheme+"://"+videoURL.Host != proxy.URL {
		t.Fatalf("generated video URL = %q, error %v", video.URL, err)
	}
	assertProxyMediaDownload(t, client, video.URL, videoBytes)

	for _, host := range []string{"first.example.test:8443", "second.example.test:9443"} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodGet, linkPath, nil)
		c.Request.RemoteAddr = "127.0.0.1:12345"
		c.Request.Host = "gateway.internal:8000"
		c.Request.Header.Set("X-Forwarded-Host", host)
		c.Request.Header.Set("X-Forwarded-Proto", "https")
		c.Params = gin.Params{{Key: "id", Value: strconv.FormatUint(uint64(asset.ID), 10)}}
		handleGetMediaAssetLink(c)
		if recorder.Code != http.StatusOK {
			t.Fatalf("domain link returned %d: %s", recorder.Code, recorder.Body.String())
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &link); err != nil {
			t.Fatal(err)
		}
		want := "https://" + host + "/v1/media/" + asset.PublicID + "/" + capability
		if link.URL != want {
			t.Fatalf("domain link = %q, want %q", link.URL, want)
		}
	}
	after, err := db.GetMediaAssetByID(asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(asset, *after) {
		t.Fatalf("changing the external domain mutated the media asset: before=%+v after=%+v", asset, after)
	}
}

func assertProxyMediaDownload(t *testing.T, client *http.Client, mediaURL string, payload []byte) {
	t.Helper()
	for _, test := range []struct {
		method      string
		rangeHeader string
		status      int
		body        string
	}{
		{http.MethodGet, "", http.StatusOK, string(payload)},
		{http.MethodHead, "", http.StatusOK, ""},
		{http.MethodGet, "bytes=1-3", http.StatusPartialContent, string(payload[1:4])},
	} {
		req, err := http.NewRequest(test.method, mediaURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if test.rangeHeader != "" {
			req.Header.Set("Range", test.rangeHeader)
		}
		// No session cookie or gateway API key: this is the actual capability URL.
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil || response.StatusCode != test.status || string(body) != test.body {
			t.Fatalf("public %s %q: status=%d body=%q error=%v", test.method, test.rangeHeader, response.StatusCode, body, readErr)
		}
		if test.rangeHeader != "" && response.Header.Get("Content-Range") != "bytes 1-3/"+strconv.Itoa(len(payload)) {
			t.Fatalf("range response Content-Range = %q", response.Header.Get("Content-Range"))
		}
	}
}
