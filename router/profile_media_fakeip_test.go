package router

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	relaymedia "relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

type profileMediaStaticDNS struct {
	ip    string
	calls atomic.Int32
}

func (r *profileMediaStaticDNS) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.calls.Add(1)
	return []net.IPAddr{{IP: net.ParseIP(r.ip)}}, nil
}

func TestProfileDirectRequiredImageMediaFakeIP(t *testing.T) {
	for _, tc := range []struct {
		name, ip, trusted string
		wantError         bool
	}{
		{"trusted_fake_ip", "198.18.0.160", "cdn.media-provider.com", false},
		{"trusted_public_ip", "8.8.8.8", "cdn.media-provider.com", false},
		{"untrusted_fake_ip", "198.18.0.160", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RELAY_DB_ENCRYPTION_KEY", "direct-image-fake-ip-test-key")
			t.Setenv("RELAY_ENABLE_PROFILE_IMAGE_ENGINE", "1")
			t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", tc.trusted)
			if err := db.InitDB(t.TempDir() + "/image.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			store, err := relaymedia.NewLocalObjectStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			SetMediaObjectStore(store)
			t.Cleanup(func() { SetMediaObjectStore(nil) })
			imageBytes := "\x89PNG\r\n\x1a\nprofile-image-bytes"
			var mediaRequests, dialCalls atomic.Int32
			mediaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mediaRequests.Add(1)
				if r.Host != "cdn.media-provider.com" || r.URL.Path != "/result.png" {
					t.Errorf("unexpected media request: %s %s", r.Host, r.URL.Path)
				}
				w.Header().Set("Content-Type", "image/png")
				_, _ = io.WriteString(w, imageBytes)
			}))
			t.Cleanup(mediaServer.Close)
			const sourceURL = "http://cdn.media-provider.com/result.png"
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/images" {
					t.Errorf("unexpected submit: %s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]string{{"url": sourceURL}}})
			}))
			t.Cleanup(upstream.Close)
			channel := &db.ChannelModel{ID: "direct-image", Name: "Direct Image", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "direct-image-model"}
			if err := db.SaveChannelModel(channel); err != nil {
				t.Fatal(err)
			}
			service.DefaultDispatcher.ResetBreaker(channel.ID)
			t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
			compiled, err := protocol.Compile(protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{Operation: "image.create", ExecutionMode: protocol.ExecutionDirect, PollingMode: protocol.PollingOff, MediaRetention: protocol.MediaRetentionRequired, Submit: protocol.Submit{Method: http.MethodPost, Path: "/images", BodyEncoding: "json"}, Response: protocol.Response{ResultPaths: []string{"data"}}}}})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "direct-image-profile", Name: "Direct Image", Source: db.ProfileSourceCustom}); err != nil {
				t.Fatal(err)
			}
			if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "direct-image-profile", Revision: 1, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
				t.Fatal(err)
			}
			if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "image.create", ModelPattern: "direct-image-model", ProfileID: "direct-image-profile", ProfileRevision: 1, Enabled: true}); err != nil {
				t.Fatal(err)
			}
			resolver := &profileMediaStaticDNS{ip: tc.ip}
			previous := profileMediaFetcherFactory
			profileMediaFetcherFactory = func(source, base string) relaymedia.SourceFetcher {
				if source != sourceURL || base != channel.BaseURL {
					t.Errorf("fetcher inputs = %q, %q", source, base)
				}
				fetcher := relaymedia.NewHTTPSourceFetcher(source, base)
				fetcher.Resolver = resolver
				fetcher.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					dialCalls.Add(1)
					if address != net.JoinHostPort(tc.ip, "80") {
						t.Errorf("dial did not pin validated IP: %q", address)
					}
					return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(mediaServer.URL, "http://"))
				}
				return fetcher
			}
			t.Cleanup(func() { profileMediaFetcherFactory = previous })
			gin.SetMode(gin.TestMode)
			engine := gin.New()
			engine.POST("/v1/images", func(c *gin.Context) {
				response, _, handled, err := profileEngineImageCreate(c, &model.ImageGenerationRequest{Model: "direct-image-model", Prompt: "render"})
				if !handled {
					t.Error("profile image request was not handled")
				}
				if err != nil {
					c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusOK, response)
			})
			engine.GET("/v1/media/:public_id/:capability", handleGetMediaAsset)
			gateway := httptest.NewServer(engine)
			t.Cleanup(gateway.Close)
			response, err := gateway.Client().Post(gateway.URL+"/v1/images", "application/json", strings.NewReader("{}"))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resolver.calls.Load() == 0 {
				t.Fatal("real fetcher DNS validation did not run")
			}
			if tc.wantError {
				if response.StatusCode != http.StatusBadGateway || !strings.Contains(string(body), "non-public address") || mediaRequests.Load() != 0 || dialCalls.Load() != 0 {
					t.Fatalf("untrusted media result: status=%d body=%s requests=%d dials=%d", response.StatusCode, body, mediaRequests.Load(), dialCalls.Load())
				}
				return
			}
			if response.StatusCode != http.StatusOK || mediaRequests.Load() != 1 || dialCalls.Load() != 1 {
				t.Fatalf("materialization result: status=%d body=%s requests=%d dials=%d", response.StatusCode, body, mediaRequests.Load(), dialCalls.Load())
			}
			var generated struct {
				Data []struct {
					URL string `json:"url"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &generated); err != nil {
				t.Fatal(err)
			}
			if len(generated.Data) != 1 {
				t.Fatalf("generated response: %s", body)
			}
			managed, err := url.Parse(generated.Data[0].URL)
			if err != nil || !strings.HasPrefix(managed.Path, "/v1/media/") || managed.Host != strings.TrimPrefix(gateway.URL, "http://") {
				t.Fatalf("invalid gateway media URL: %q", generated.Data[0].URL)
			}
			assetResponse, err := gateway.Client().Get(generated.Data[0].URL)
			if err != nil {
				t.Fatal(err)
			}
			defer assetResponse.Body.Close()
			assetBytes, err := io.ReadAll(assetResponse.Body)
			if err != nil || assetResponse.StatusCode != http.StatusOK || string(assetBytes) != imageBytes {
				t.Fatalf("managed media: status=%d bytes=%q err=%v", assetResponse.StatusCode, assetBytes, err)
			}
		})
	}
}
