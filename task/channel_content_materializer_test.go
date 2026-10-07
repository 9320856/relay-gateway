package task

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
)

func channelContentWorkerFixture(t *testing.T, engine, adapterType, baseURL string) (*db.TaskRun, db.MediaAsset, media.ObjectStore) {
	t.Helper()
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "channel-content-worker-test-key")
	if err := db.InitDB(t.TempDir() + "/content-worker.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{
		ID: "pinned-content-channel", Name: "Pinned content channel", Type: adapterType, BaseURL: baseURL,
		APIKey: "pinned-provider-key", Enabled: true, MediaRetention: media.RetentionDisabled,
		HeadersRaw: `{"X-Provider-Account":"account-42","Range":"bytes=2-4","If-Range":"old-etag"}`,
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	// A changed default and changed retention policy must not repoint an existing job.
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "later-content-channel", Name: "Later", Type: adapterType, BaseURL: "https://must-not-request.invalid/v1", APIKey: "wrong-key", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	run := &db.TaskRun{ID: "channel-content-run", TaskKind: "video", Operation: "video.create", ChannelID: channel.ID, Engine: engine,
		MediaRetention: media.RetentionRequired, PollingMode: protocol.PollingClient, ProviderTaskID: "pinned-provider-task", SubmissionState: "accepted", TaskStatus: "materializing", TaskOutcome: "pending"}
	if engine == "profile" {
		profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
			Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient,
			Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
			Poll:    &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
			Content: &protocol.Content{Method: http.MethodGet, Path: "/videos/{task_id}/content", Headers: map[string]string{"range": "bytes=2-4", "if-range": "snapshot-etag"}},
		}}}
		compiled, err := protocol.Compile(profile)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "content-worker-profile", Name: "Content worker", Source: db.ProfileSourceCustom}); err != nil {
			t.Fatal(err)
		}
		if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "content-worker-profile", Revision: 1, SchemaVersion: protocol.CurrentSchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
			t.Fatal(err)
		}
		run.ProfileID, run.ProfileRevision, run.ProfileDigest = "content-worker-profile", 1, compiled.Digest()
	}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	assets, err := db.EnsureTaskContentMediaContext(context.Background(), run.ID, "video")
	if err != nil || len(assets) != 1 {
		t.Fatalf("ensure provider content = %+v, %v", assets, err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	return run, assets[0], store
}

func TestProfileContentWorkerRelativeRedirectUsesExpandedContentEndpoint(t *testing.T) {
	var contentCalls, downloadCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/videos/pinned-provider-task/content":
			contentCalls.Add(1)
			if r.Header.Get("Authorization") != "Bearer pinned-provider-key" {
				t.Errorf("authenticated endpoint lost its credential: %v", r.Header)
			}
			w.Header().Set("Location", "../files/result.mp4")
			w.WriteHeader(http.StatusFound)
		case "/v1/videos/files/result.mp4":
			downloadCalls.Add(1)
			if r.Header.Get("Authorization") != "" || r.Header.Get("X-Provider-Account") != "" {
				t.Errorf("public media download inherited provider headers: %v", r.Header)
			}
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = io.WriteString(w, "relative-redirect-video")
		default:
			t.Errorf("relative content redirect resolved to %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(provider.Close)
	run, asset, store := channelContentWorkerFixture(t, "profile", "openai", provider.URL+"/v1")
	previous := mediaURLFetcherFactory
	mediaURLFetcherFactory = func(source, base string) media.HTTPSourceFetcher {
		if source != provider.URL+"/v1/videos/files/result.mp4" || base != provider.URL+"/v1" {
			t.Errorf("relative content redirect provenance source=%q base=%q", source, base)
		}
		return media.HTTPSourceFetcher{Client: provider.Client(), URLValidator: func(string) error { return nil }}
	}
	t.Cleanup(func() { mediaURLFetcherFactory = previous })
	claimed, err := NewMediaMaterializationPoller("relative-content-worker", store).RunOnce(context.Background())
	if !claimed || err != nil || contentCalls.Load() != 1 || downloadCalls.Load() != 1 {
		t.Fatalf("relative content worker claimed=%v endpoint=%d media=%d err=%v", claimed, contentCalls.Load(), downloadCalls.Load(), err)
	}
	assertChannelContentStored(t, run.ID, asset.ID, store, "relative-redirect-video")
}

func assertChannelContentStored(t *testing.T, runID string, assetID uint, store media.ObjectStore, want string) {
	t.Helper()
	asset, err := db.GetMediaAssetByID(assetID)
	if err != nil || asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" {
		t.Fatalf("stored content asset = %+v, %v", asset, err)
	}
	object, err := db.GetMediaObjectByID(asset.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	body, _, err := store.Open(context.Background(), object.StorageKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	contents, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil || closeErr != nil || string(contents) != want {
		t.Fatalf("retained bytes = %q, read=%v close=%v", contents, readErr, closeErr)
	}
	run, err := db.GetTaskRun(runID)
	if err != nil || run.TaskStatus != "completed" || run.TaskOutcome != "success" {
		t.Fatalf("retained task = %+v, %v", run, err)
	}
}

func assertChannelContentRetryable(t *testing.T, runID string, assetID uint) {
	t.Helper()
	asset, err := db.GetMediaAssetByID(assetID)
	if err != nil || asset.Status != db.MediaAssetFailed || asset.ObjectID != "" {
		t.Fatalf("partial content asset = %+v, %v", asset, err)
	}
	job, err := db.GetMediaMaterializationJob(assetID)
	if err != nil || job.Status != db.MediaJobPending || job.NextAttemptAt == nil {
		t.Fatalf("partial content retry = %+v, %v", job, err)
	}
	run, err := db.GetTaskRun(runID)
	if err != nil || run.TaskStatus != "materializing" || run.TaskOutcome != "pending" {
		t.Fatalf("partial content task = %+v, %v", run, err)
	}
}

func TestChannelContentWorkerPinsAuthenticatedGETAndRequiresCompleteBody(t *testing.T) {
	for _, engine := range []string{"legacy", "profile"} {
		for _, kind := range []string{"openai", "newapi"} {
			if engine == "profile" && kind != "openai" {
				continue
			}
			for _, responseKind := range []string{"complete", "partial_206", "partial_200"} {
				t.Run(engine+"/"+kind+"/"+responseKind, func(t *testing.T) {
					var calls atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						wantPath := "/v1/videos/pinned-provider-task/content"
						if engine == "legacy" && kind == "newapi" {
							wantPath = "/v1/videos/generations/pinned-provider-task/content"
						}
						if r.Method != http.MethodGet || r.URL.Path != wantPath || r.Header.Get("Authorization") != "Bearer pinned-provider-key" || r.Header.Get("X-Provider-Account") != "account-42" {
							t.Errorf("content request did not use pinned operation and credentials: %s %s %v", r.Method, r.URL.Path, r.Header)
						}
						if r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
							t.Errorf("worker requested partial media: %v", r.Header)
						}
						if responseKind != "complete" && calls.Load() == 1 {
							w.Header().Set("Content-Type", "video/mp4")
							w.Header().Set("Content-Range", "bytes 2-4/23")
							if responseKind == "partial_206" {
								w.WriteHeader(http.StatusPartialContent)
							}
							_, _ = io.WriteString(w, "deo")
							return
						}
						// Stream multiple chunks and omit MIME as some compatible video APIs do.
						w.WriteHeader(http.StatusOK)
						_, _ = io.WriteString(w, "complete-")
						w.(http.Flusher).Flush()
						_, _ = io.WriteString(w, "video-content")
					}))
					t.Cleanup(server.Close)
					run, asset, store := channelContentWorkerFixture(t, engine, kind, server.URL+"/v1")
					poller := NewMediaMaterializationPoller("channel-content-test", store)
					claimed, err := poller.RunOnce(context.Background())
					if !claimed || calls.Load() != 1 {
						t.Fatalf("worker claimed=%v requests=%d err=%v", claimed, calls.Load(), err)
					}
					if responseKind != "complete" {
						if err == nil {
							t.Fatal("worker accepted partial content")
						}
						assertChannelContentRetryable(t, run.ID, asset.ID)
						if err := db.RetryMediaAssetMaterializationContext(context.Background(), asset.ID); err != nil {
							t.Fatal(err)
						}
						claimed, err = poller.RunOnce(context.Background())
						if !claimed || err != nil || calls.Load() != 2 {
							t.Fatalf("download-only retry claimed=%v requests=%d err=%v", claimed, calls.Load(), err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					assertChannelContentStored(t, run.ID, asset.ID, store, "complete-video-content")
				})
			}
		}
	}
}

func TestChannelContentWorkerCDNRedirectDropsCredentialsAndRejectsPartial(t *testing.T) {
	for _, engine := range []string{"legacy", "profile"} {
		for _, partial := range []bool{false, true} {
			name := engine + "/complete"
			if partial {
				name = engine + "/partial"
			}
			t.Run(name, func(t *testing.T) {
				var providerCalls, cdnCalls atomic.Int32
				const cdnURL = "http://cdn.content-provider.net/result.mp4"
				cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					cdnCalls.Add(1)
					if r.Method != http.MethodGet || r.URL.Path != "/result.mp4" || r.Header.Get("Authorization") != "" || r.Header.Get("X-Provider-Account") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
						t.Errorf("CDN received provider credentials or a partial request: %s %s %v", r.Method, r.URL.Path, r.Header)
					}
					w.Header().Set("Content-Type", "video/mp4")
					if partial {
						w.Header().Set("Content-Range", "bytes 2-4/23")
						w.WriteHeader(http.StatusPartialContent)
					}
					_, _ = io.WriteString(w, "cdn-video-content")
				}))
				t.Cleanup(cdn.Close)
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					providerCalls.Add(1)
					if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer pinned-provider-key" {
						t.Errorf("provider request = %s %v", r.Method, r.Header)
					}
					http.Redirect(w, r, cdnURL, http.StatusFound)
				}))
				t.Cleanup(provider.Close)
				run, asset, store := channelContentWorkerFixture(t, engine, "openai", provider.URL+"/v1")
				previous := mediaURLFetcherFactory
				var factoryCalls int
				transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					if address != "cdn.content-provider.net:80" {
						t.Errorf("unexpected public fetch address %q", address)
					}
					return (&net.Dialer{}).DialContext(ctx, network, cdn.Listener.Addr().String())
				}}
				t.Cleanup(transport.CloseIdleConnections)
				mediaURLFetcherFactory = func(source, base string) media.HTTPSourceFetcher {
					factoryCalls++
					if source != cdnURL || base != provider.URL+"/v1" {
						t.Errorf("public redirect provenance source=%q base=%q", source, base)
					}
					return media.HTTPSourceFetcher{Client: &http.Client{Transport: transport, Timeout: time.Second}, URLValidator: func(string) error { return nil }}
				}
				t.Cleanup(func() { mediaURLFetcherFactory = previous })
				claimed, err := NewMediaMaterializationPoller("channel-redirect-test", store).RunOnce(context.Background())
				if !claimed || providerCalls.Load() != 1 || cdnCalls.Load() != 1 || factoryCalls != 1 {
					t.Fatalf("redirect worker claimed=%v provider=%d cdn=%d factory=%d err=%v", claimed, providerCalls.Load(), cdnCalls.Load(), factoryCalls, err)
				}
				if partial {
					if err == nil {
						t.Fatal("worker accepted partial CDN content")
					}
					assertChannelContentRetryable(t, run.ID, asset.ID)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				assertChannelContentStored(t, run.ID, asset.ID, store, "cdn-video-content")
			})
		}
	}
}
