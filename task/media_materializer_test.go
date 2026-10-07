package task

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
)

func TestMediaMaterializationPollerMaterializesDurableURLJob(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-materializer.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write([]byte("video-bytes"))
	}))
	t.Cleanup(server.Close)
	asset := &db.MediaAsset{PublicID: "asset-materializer", CapabilityHash: "hash", TaskRunID: "run-materializer", Kind: "video", Ordinal: 0, Status: db.MediaAssetPending, SourceKind: media.SourceURL, SourceLocator: server.URL}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaMaterializationJob(&db.MediaMaterializationJob{AssetID: asset.ID, Status: db.MediaJobPending}); err != nil {
		t.Fatal(err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	poller := NewMediaMaterializationPoller("materializer-test", store)
	poller.Fetcher = media.SourceFetcherFunc(func(_ context.Context, result media.MediaResult) (media.FetchedSource, error) {
		return media.FetchedSource{Body: io.NopCloser(strings.NewReader("video-bytes")), ContentType: "video/mp4"}, nil
	})
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("run once claimed=%v err=%v", claimed, err)
	}
	got, err := db.GetMediaAssetByID(asset.ID)
	if err != nil || got.Status != db.MediaAssetAvailable || got.ObjectID == "" {
		t.Fatalf("asset=%+v err=%v", got, err)
	}
	object, err := db.GetMediaObjectByID(got.ObjectID)
	if err != nil || object.State != db.MediaObjectReady {
		t.Fatalf("object=%+v err=%v", object, err)
	}
	job, err := db.GetMediaMaterializationJob(asset.ID)
	if err != nil || job.Status != db.MediaJobSucceeded {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}

func TestMediaMaterializationPollerMaterializesDurableBase64Job(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "base64-materializer-test-key")
	if err := db.InitDB(t.TempDir() + "/base64-media-materializer.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	run := &db.TaskRun{ID: "run-base64-materializer", TaskKind: "image", Operation: "images.create", ChannelID: "channel", Engine: "profile", PollingMode: "background", TaskStatus: "processing", TaskOutcome: "pending"}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	assets, err := db.EnsureTaskResultMediaContext(context.Background(), run.ID, "image", []string{"data:image/png;base64,iVBORw0KGgo="})
	if err != nil || len(assets) != 1 {
		t.Fatalf("ensure inline asset = %#v, %v", assets, err)
	}
	if assets[0].SourceKind != media.SourceBase64 || assets[0].ContentType != "image/png" {
		t.Fatalf("inline asset source = %#v", assets[0])
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := NewMediaMaterializationPoller("base64-materializer-test", store).RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("run once claimed=%v err=%v", claimed, err)
	}
	asset, err := db.GetMediaAssetByID(assets[0].ID)
	if err != nil || asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" || asset.ContentType != "image/png" {
		t.Fatalf("materialized inline asset = %#v, %v", asset, err)
	}
}

func TestMediaMaterializationPollerCompletesStaleJobWithoutRefetch(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/media-materializer-stale.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	asset := &db.MediaAsset{PublicID: "asset-materialized", CapabilityHash: "hash", TaskRunID: "run-materialized", Kind: "video", Ordinal: 0, Status: db.MediaAssetAvailable, ObjectID: "obj_existing", SourceKind: media.SourceURL, SourceLocator: "https://provider.invalid/already-done"}
	if err := db.CreateMediaAsset(asset); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaMaterializationJob(&db.MediaMaterializationJob{AssetID: asset.ID, Status: db.MediaJobPending}); err != nil {
		t.Fatal(err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	poller := NewMediaMaterializationPoller("materializer-stale-test", store)
	poller.Fetcher = media.SourceFetcherFunc(func(context.Context, media.MediaResult) (media.FetchedSource, error) {
		called = true
		return media.FetchedSource{}, errors.New("must not refetch")
	})
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed || called {
		t.Fatalf("run once claimed=%v err=%v refetched=%v", claimed, err, called)
	}
	job, err := db.GetMediaMaterializationJob(asset.ID)
	if err != nil || job.Status != db.MediaJobSucceeded {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}

func TestMediaMaterializationPollerFetchesProfileContent(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "profile-content-materializer-key")
	if err := db.InitDB(t.TempDir() + "/profile-content-materializer.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var contentCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/videos/provider-content-materializer/content" {
			t.Fatalf("unexpected content request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer profile-content-key" {
			t.Fatalf("content authorization=%q", r.Header.Get("Authorization"))
		}
		contentCalls++
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = io.WriteString(w, "profile-content-bytes")
	}))
	t.Cleanup(server.Close)
	if err := db.SaveChannelModel(&db.ChannelModel{ID: "profile-content-materializer-channel", Name: "Profile Content Materializer", Type: "newapi", BaseURL: server.URL + "/v1", APIKey: "profile-content-key", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	profile := protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingBackground,
		MediaRetention: protocol.MediaRetentionRequired,
		Submit:         protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}},
		Poll:    &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", IntervalMS: 1, MaxAttempts: 2, MaxDurationMS: 1000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"video_url"}},
		Content: &protocol.Content{Method: http.MethodGet, Path: "/videos/{task_id}/content"},
	}}}
	compiled, err := protocol.Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "profile-content-materializer", Name: "Profile Content Materializer", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "profile-content-materializer", Revision: 1, SchemaVersion: 1, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	run := &db.TaskRun{ID: "profile-content-materializer-run", TaskKind: "video", Operation: "video.create", ChannelID: "profile-content-materializer-channel", Engine: "profile", ProfileID: "profile-content-materializer", ProfileRevision: 1, ProfileDigest: compiled.Digest(), PollingMode: protocol.PollingBackground, ProviderTaskID: "provider-content-materializer", TaskStatus: "processing", TaskOutcome: "pending", NextPollAt: &now}
	if err := db.CreateTaskRun(run); err != nil {
		t.Fatal(err)
	}
	assets, err := db.EnsureTaskContentMediaContext(context.Background(), run.ID, "video")
	if err != nil || len(assets) != 1 {
		t.Fatalf("ensure content assets=%+v err=%v", assets, err)
	}
	store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	poller := NewMediaMaterializationPoller("profile-content-materializer", store)
	claimed, err := poller.RunOnce(context.Background())
	if err != nil || !claimed {
		t.Fatalf("run once claimed=%v err=%v", claimed, err)
	}
	if contentCalls != 1 {
		t.Fatalf("content calls=%d, want 1", contentCalls)
	}
	asset, err := db.GetMediaAssetByID(assets[0].ID)
	if err != nil || asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" || asset.ContentType != "video/mp4" {
		t.Fatalf("materialized asset=%+v err=%v", asset, err)
	}
	loaded, err := db.GetTaskRun(run.ID)
	if err != nil || loaded.TaskStatus != "completed" || loaded.TaskOutcome != "success" {
		t.Fatalf("task lifecycle=%+v err=%v", loaded, err)
	}
}

type materializerStaticDNS struct {
	hosts map[string]struct{}
	ips   []string
	calls atomic.Int32
}

func (r *materializerStaticDNS) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	r.calls.Add(1)
	if _, ok := r.hosts[host]; !ok {
		return nil, errors.New("unexpected media DNS host: " + host)
	}
	addresses := make([]net.IPAddr, 0, len(r.ips))
	for _, ip := range r.ips {
		addresses = append(addresses, net.IPAddr{IP: net.ParseIP(ip)})
	}
	return addresses, nil
}

func TestMediaMaterializationPollerFetchesUpstreamFakeIPURLs(t *testing.T) {
	for _, tc := range []struct {
		name, engine, baseURL, host, redirectHost string
		ips                                       []string
		missingRun, missingChannel, noChannelID   bool
		wantError                                 bool
	}{
		{name: "profile_ipv4", engine: "profile", host: "download.xmimage2.cc.cd", ips: []string{"198.18.0.97"}},
		{name: "profile_ipv6", engine: "profile", host: "download.xmimage2.cc.cd", ips: []string{"2001:2::60"}},
		{name: "profile_dual_stack", engine: "profile", host: "download.xmimage2.cc.cd", ips: []string{"198.18.0.97", "2001:2::60"}},
		{name: "future_provider", engine: "profile", baseURL: "https://api.future-relay.net/v2", host: "media.another-provider.net", ips: []string{"198.19.1.8"}},
		{name: "private_public_suffix", engine: "profile", host: "media.customer.github.io", ips: []string{"198.18.0.97"}},
		{name: "legacy_provider", engine: "legacy", baseURL: "https://api.next-relay.net/v1", host: "assets.different-provider.com", ips: []string{"2001:2::60"}},
		{name: "redirect_new_domain", engine: "profile", host: "download.xmimage2.cc.cd", redirectHost: "edge.customer.github.io", ips: []string{"198.18.0.97", "2001:2::60"}},
		{name: "private_ipv4", engine: "profile", host: "media.other-provider.net", ips: []string{"192.168.1.9"}, wantError: true},
		{name: "private_ipv6", engine: "profile", host: "media.other-provider.net", ips: []string{"fd00::9"}, wantError: true},
		{name: "mixed_fake_public", engine: "profile", host: "media.other-provider.net", ips: []string{"198.18.0.97", "8.8.8.8"}, wantError: true},
		{name: "missing_task", engine: "profile", host: "media.other-provider.net", ips: []string{"198.18.0.97"}, missingRun: true, wantError: true},
		{name: "missing_channel", engine: "profile", host: "media.other-provider.net", ips: []string{"198.18.0.97"}, missingChannel: true, wantError: true},
		{name: "missing_channel_id", engine: "profile", host: "media.other-provider.net", ips: []string{"198.18.0.97"}, noChannelID: true, wantError: true},
		{name: "unknown_engine", engine: "damaged", host: "media.other-provider.net", ips: []string{"198.18.0.97"}, wantError: true},
		{name: "invalid_upstream", engine: "profile", baseURL: "file:///tmp/provider", host: "media.other-provider.net", ips: []string{"198.18.0.97"}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", "")
			t.Setenv("RELAY_DB_ENCRYPTION_KEY", "fake-ip-materializer-key")
			if err := db.InitDB(t.TempDir() + "/fake-ip-materializer.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if tc.baseURL == "" {
				tc.baseURL = "https://api.original-relay.com/v1"
			}
			channel := &db.ChannelModel{ID: "pinned-media-channel", Name: "Pinned Media Channel", Type: "newapi", BaseURL: tc.baseURL, Enabled: true}
			if !tc.missingChannel {
				if err := db.SaveChannelModel(channel); err != nil {
					t.Fatal(err)
				}
			}
			// A later provider configuration must never replace the channel
			// selected in the persisted task that owns this media result.
			if err := db.SaveChannelModel(&db.ChannelModel{ID: "new-media-channel", Name: "New Media Channel", Type: "newapi", BaseURL: "https://api.unrelated-relay.net/v1", Enabled: true}); err != nil {
				t.Fatal(err)
			}
			if err := db.SQLDBForContext(context.Background()).Create(&db.RequestLogModel{ID: "other-origin", Kind: "api_call", ChannelID: "new-media-channel", StartedAt: time.Now(), Method: http.MethodPost, Path: "/v1/images"}).Error; err != nil {
				t.Fatal(err)
			}
			run := &db.TaskRun{ID: "fake-ip-run", TaskKind: "video", Operation: "video.create", ChannelID: channel.ID, Engine: tc.engine, PollingMode: protocol.PollingBackground, TaskStatus: "materializing", TaskOutcome: "pending"}
			if tc.noChannelID {
				run.ChannelID = ""
			}
			if !tc.missingRun {
				if err := db.CreateTaskRun(run); err != nil {
					t.Fatal(err)
				}
			}
			const videoBytes = "upstream-video-bytes"
			var mediaRequests, dialCalls atomic.Int32
			mediaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mediaRequests.Add(1)
				if r.Host == tc.host && r.URL.Path == "/video.mp4" && tc.redirectHost != "" {
					http.Redirect(w, r, "http://"+tc.redirectHost+"/final.mp4", http.StatusFound)
					return
				}
				if (r.Host != tc.host || r.URL.Path != "/video.mp4") && (r.Host != tc.redirectHost || r.URL.Path != "/final.mp4") {
					t.Errorf("unexpected media request: %s %s", r.Host, r.URL.Path)
				}
				w.Header().Set("Content-Type", "video/mp4")
				_, _ = io.WriteString(w, videoBytes)
			}))
			t.Cleanup(mediaServer.Close)
			sourceURL := "http://" + tc.host + "/video.mp4"
			assets, err := db.EnsureTaskResultMediaContext(context.Background(), run.ID, "video", []string{sourceURL})
			if err != nil || len(assets) != 1 {
				t.Fatalf("ensure URL media = %#v, %v", assets, err)
			}
			// A valid origin log cannot replace an existing damaged TaskRun.
			// Missing task rows without a matching origin log remain strict too.
			originID := "other-origin"
			if tc.missingRun {
				originID = "missing-origin"
			}
			if err := db.SQLDBForContext(context.Background()).Model(&db.MediaAsset{}).Where("id = ?", assets[0].ID).Update("origin_request_id", originID).Error; err != nil {
				t.Fatal(err)
			}
			resolver := &materializerStaticDNS{hosts: map[string]struct{}{tc.host: {}}, ips: tc.ips}
			if tc.redirectHost != "" {
				resolver.hosts[tc.redirectHost] = struct{}{}
			}
			previous := mediaURLFetcherFactory
			var configuredCalls int
			mediaURLFetcherFactory = func(source, base string) media.HTTPSourceFetcher {
				if source != sourceURL {
					t.Errorf("media source = %q, want %q", source, sourceURL)
				}
				if base != "" {
					configuredCalls++
					if base != tc.baseURL {
						t.Errorf("media upstream = %q, want persisted channel %q", base, tc.baseURL)
					}
				}
				fetcher := media.NewHTTPSourceFetcher(source, base)
				fetcher.Resolver = resolver
				fetcher.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					dialCalls.Add(1)
					pinned := false
					for _, ip := range tc.ips {
						pinned = pinned || address == net.JoinHostPort(ip, "80")
					}
					if !pinned {
						t.Errorf("dial did not pin a validated DNS address: %q", address)
					}
					return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(mediaServer.URL, "http://"))
				}
				return fetcher
			}
			t.Cleanup(func() { mediaURLFetcherFactory = previous })
			store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
			if err != nil {
				t.Fatal(err)
			}
			claimed, runErr := NewMediaMaterializationPoller("fake-ip-materializer", store).RunOnce(context.Background())
			if !claimed || resolver.calls.Load() == 0 {
				t.Fatalf("default URL worker: claimed=%v DNS=%d err=%v", claimed, resolver.calls.Load(), runErr)
			}
			wantConfigured := !tc.missingRun && !tc.missingChannel && !tc.noChannelID && (tc.engine == "profile" || tc.engine == "legacy")
			if (configuredCalls > 0) != wantConfigured {
				t.Fatalf("configured upstream factory calls=%d, want provenance=%v", configuredCalls, wantConfigured)
			}
			asset, err := db.GetMediaAssetByID(assets[0].ID)
			if err != nil {
				t.Fatal(err)
			}
			job, err := db.GetMediaMaterializationJob(asset.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantError {
				if runErr == nil || mediaRequests.Load() != 0 || dialCalls.Load() != 0 || asset.ObjectID != "" || asset.Status != db.MediaAssetFailed || job.Status != db.MediaJobPending || job.NextAttemptAt == nil || job.LastError != runErr.Error() {
					t.Fatalf("blocked URL: err=%v requests=%d dials=%d asset=%+v job=%+v", runErr, mediaRequests.Load(), dialCalls.Load(), asset, job)
				}
				return
			}
			wantRequests := int32(1)
			if tc.redirectHost != "" {
				wantRequests = 2
			}
			if runErr != nil || mediaRequests.Load() != wantRequests || dialCalls.Load() != wantRequests || resolver.calls.Load() != wantRequests || asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" || job.Status != db.MediaJobSucceeded {
				t.Fatalf("materialized URL: err=%v requests=%d dials=%d DNS=%d asset=%+v job=%+v", runErr, mediaRequests.Load(), dialCalls.Load(), resolver.calls.Load(), asset, job)
			}
			object, err := db.GetMediaObjectByID(asset.ObjectID)
			if err != nil {
				t.Fatal(err)
			}
			body, _, err := store.Open(context.Background(), object.StorageKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer body.Close()
			contents, err := io.ReadAll(body)
			if err != nil || string(contents) != videoBytes {
				t.Fatalf("stored media bytes=%q err=%v", contents, err)
			}
		})
	}
}

func TestDefaultURLFetcherForAssetUsesExplicitCDNTrustWithoutTask(t *testing.T) {
	t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", "cdn.provider.net,other.provider.net")
	fetcher := defaultURLFetcherForAsset(context.Background(), &db.MediaAsset{SourceKind: media.SourceURL, SourceLocator: "https://cdn.provider.net/image.png"})
	if _, ok := fetcher.TrustedFakeIPHosts["cdn.provider.net"]; !ok || len(fetcher.TrustedFakeIPHosts) != 1 {
		t.Fatalf("explicit source trust = %#v", fetcher.TrustedFakeIPHosts)
	}
	unlisted := defaultURLFetcherForAsset(context.Background(), &db.MediaAsset{SourceKind: media.SourceURL, SourceLocator: "https://unlisted.provider.net/image.png"})
	if len(unlisted.TrustedFakeIPHosts) != 0 {
		t.Fatalf("unlisted source trusted = %#v", unlisted.TrustedFakeIPHosts)
	}
	if got := defaultURLFetcherForAsset(context.Background(), nil); len(got.TrustedFakeIPHosts) != 0 {
		t.Fatalf("nil source trusted = %#v", got.TrustedFakeIPHosts)
	}
}

func TestMediaMaterializationPollerRetriesSynchronousMediaWithPersistedOrigin(t *testing.T) {
	for _, tc := range []struct {
		name, taskRunID, baseURL, host, redirectHost, logKind string
		ips                                                   []string
		noOrigin, missingLog, noLogChannel, missingChannel    bool
		wantError                                             bool
	}{
		{name: "origin_ipv4", ips: []string{"198.18.0.97"}},
		{name: "origin_ipv6", ips: []string{"2001:2::60"}},
		{name: "synthetic_task_ipv4", taskRunID: "pr_unpersisted-image-ipv4", ips: []string{"198.18.0.97"}},
		{name: "synthetic_task_ipv6", taskRunID: "pr_unpersisted-image-ipv6", ips: []string{"2001:2::60"}},
		{name: "future_provider_cdn", baseURL: "https://api.future-provider.net/v2", host: "images.unrelated-cdn.com", ips: []string{"198.19.1.8"}},
		{name: "private_public_suffix", host: "images.customer.github.io", ips: []string{"198.18.0.97"}},
		{name: "redirect_new_cdn", redirectHost: "images.future-cdn.net", ips: []string{"2001:2::60"}},
		{name: "real_private_ip", ips: []string{"192.168.1.9"}, wantError: true},
		{name: "without_origin", ips: []string{"198.18.0.97"}, noOrigin: true, wantError: true},
		{name: "missing_origin_log", ips: []string{"198.18.0.97"}, missingLog: true, wantError: true},
		{name: "missing_log_channel", ips: []string{"198.18.0.97"}, noLogChannel: true, wantError: true},
		{name: "non_api_call_log", ips: []string{"198.18.0.97"}, logKind: "admin_action", wantError: true},
		{name: "missing_channel", ips: []string{"198.18.0.97"}, missingChannel: true, wantError: true},
		{name: "invalid_upstream", ips: []string{"198.18.0.97"}, baseURL: "file:///tmp/provider", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", "")
			if err := db.InitDB(t.TempDir() + "/sync-retry.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if tc.baseURL == "" {
				tc.baseURL = "https://api.original-provider.com/v1"
			}
			if tc.host == "" {
				tc.host = "download.xmimage2.cc.cd"
			}
			if tc.logKind == "" {
				tc.logKind = "api_call"
			}
			channel := &db.ChannelModel{ID: "sync-origin-channel", Name: "Sync Origin", Type: "newapi", BaseURL: tc.baseURL, Enabled: true}
			if !tc.missingChannel {
				if err := db.SaveChannelModel(channel); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.SaveChannelModel(&db.ChannelModel{ID: "later-channel", Name: "Later Channel", Type: "newapi", BaseURL: "https://api.later-provider.net/v1", Enabled: true}); err != nil {
				t.Fatal(err)
			}
			conn := db.SQLDBForContext(context.Background())
			if err := conn.Create(&db.RequestLogModel{ID: "different-api-call", Kind: "api_call", ChannelID: "later-channel", StartedAt: time.Now(), Method: http.MethodPost, Path: "/v1/images"}).Error; err != nil {
				t.Fatal(err)
			}
			originID := "sync-origin"
			if tc.noOrigin {
				originID = ""
			}
			if !tc.missingLog {
				log := &db.RequestLogModel{ID: "sync-origin", Kind: tc.logKind, ChannelID: channel.ID, StartedAt: time.Now(), Method: http.MethodPost, Path: "/v1/images"}
				if tc.noLogChannel {
					log.ChannelID = ""
				}
				if err := conn.Create(log).Error; err != nil {
					t.Fatal(err)
				}
			}
			const imageBytes = "\x89PNG\r\n\x1a\nretried-image-bytes"
			var mediaRequests, dialCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mediaRequests.Add(1)
				if r.Host == tc.host && r.URL.Path == "/image.png" && tc.redirectHost != "" {
					http.Redirect(w, r, "http://"+tc.redirectHost+"/final.png", http.StatusFound)
					return
				}
				if (r.Host != tc.host || r.URL.Path != "/image.png") && (r.Host != tc.redirectHost || r.URL.Path != "/final.png") {
					t.Errorf("unexpected retry media request: %s %s", r.Host, r.URL.Path)
				}
				w.Header().Set("Content-Type", "image/png")
				_, _ = io.WriteString(w, imageBytes)
			}))
			t.Cleanup(server.Close)
			sourceURL := "http://" + tc.host + "/image.png"
			resolver := &materializerStaticDNS{hosts: map[string]struct{}{tc.host: {}}, ips: tc.ips}
			if tc.redirectHost != "" {
				resolver.hosts[tc.redirectHost] = struct{}{}
			}
			var configuredCalls int
			factory := func(source, base string) media.HTTPSourceFetcher {
				if source != sourceURL {
					t.Errorf("retry source = %q, want %q", source, sourceURL)
				}
				if base != "" {
					configuredCalls++
					if base != tc.baseURL {
						t.Errorf("retry upstream = %q, want originating API-call channel %q", base, tc.baseURL)
					}
				}
				fetcher := media.NewHTTPSourceFetcher(source, base)
				fetcher.Resolver = resolver
				fetcher.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					dialCalls.Add(1)
					if address != net.JoinHostPort(tc.ips[0], "80") {
						t.Errorf("retry dial did not pin the validated IP: %q", address)
					}
					return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "http://"))
				}
				return fetcher
			}
			store, err := media.NewLocalObjectStore(t.TempDir(), 1024)
			if err != nil {
				t.Fatal(err)
			}
			asset := &db.MediaAsset{PublicID: "sync-retry-image", CapabilityHash: "test-capability-hash", TaskRunID: tc.taskRunID, OriginRequestID: originID, Kind: "image", SourceKind: media.SourceURL, SourceLocator: sourceURL}
			job, err := db.CreateMediaAssetForMaterializationContext(context.Background(), asset, "sync-response", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// Reproduce a synchronous download that failed under the old strict
			// policy, then persist that failure before using the public retry API.
			_, initialErr := MaterializeClaimedMedia(context.Background(), job, asset, store, factory(sourceURL, ""), time.Minute)
			if initialErr == nil || !strings.Contains(initialErr.Error(), "non-public address") || mediaRequests.Load() != 0 || dialCalls.Load() != 0 {
				t.Fatalf("initial sync failure: err=%v requests=%d dials=%d", initialErr, mediaRequests.Load(), dialCalls.Load())
			}
			if err := db.FailMediaMaterializationWithAssetForLeaseContext(context.Background(), job, initialErr.Error(), time.Now().Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			failed, err := db.GetMediaAssetByID(asset.ID)
			if err != nil || failed.Status != db.MediaAssetFailed || failed.OriginRequestID != originID || failed.TaskRunID != tc.taskRunID {
				t.Fatalf("durable synchronous failure = %+v, %v", failed, err)
			}
			if err := db.RetryMediaAssetMaterializationContext(context.Background(), asset.ID); err != nil {
				t.Fatal(err)
			}
			previous := mediaURLFetcherFactory
			mediaURLFetcherFactory = factory
			t.Cleanup(func() { mediaURLFetcherFactory = previous })
			resolver.calls.Store(0)
			claimed, runErr := NewMediaMaterializationPoller("sync-retry-worker", store).RunOnce(context.Background())
			if !claimed || resolver.calls.Load() == 0 {
				t.Fatalf("retry worker: claimed=%v DNS=%d err=%v", claimed, resolver.calls.Load(), runErr)
			}
			wantConfigured := !tc.noOrigin && !tc.missingLog && !tc.noLogChannel && !tc.missingChannel && tc.logKind == "api_call"
			if (configuredCalls > 0) != wantConfigured {
				t.Fatalf("origin factory calls=%d, want provenance=%v", configuredCalls, wantConfigured)
			}
			stored, err := db.GetMediaAssetByID(asset.ID)
			if err != nil {
				t.Fatal(err)
			}
			job, err = db.GetMediaMaterializationJob(asset.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantError {
				if runErr == nil || mediaRequests.Load() != 0 || dialCalls.Load() != 0 || stored.ObjectID != "" || stored.Status != db.MediaAssetFailed || job.Status != db.MediaJobPending || job.Attempts != 2 || job.LastError != runErr.Error() {
					t.Fatalf("blocked origin retry: err=%v requests=%d dials=%d asset=%+v job=%+v", runErr, mediaRequests.Load(), dialCalls.Load(), stored, job)
				}
				return
			}
			wantRequests := int32(1)
			if tc.redirectHost != "" {
				wantRequests = 2
			}
			if runErr != nil || mediaRequests.Load() != wantRequests || dialCalls.Load() != wantRequests || resolver.calls.Load() != wantRequests || stored.Status != db.MediaAssetAvailable || stored.ObjectID == "" || job.Status != db.MediaJobSucceeded || job.Attempts != 2 {
				t.Fatalf("successful origin retry: err=%v requests=%d dials=%d asset=%+v job=%+v", runErr, mediaRequests.Load(), dialCalls.Load(), stored, job)
			}
			object, err := db.GetMediaObjectByID(stored.ObjectID)
			if err != nil {
				t.Fatal(err)
			}
			body, _, err := store.Open(context.Background(), object.StorageKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer body.Close()
			contents, err := io.ReadAll(body)
			if err != nil || string(contents) != imageBytes {
				t.Fatalf("retried image bytes=%q err=%v", contents, err)
			}
		})
	}
}

func TestDefaultURLFetcherForAssetKeepsTaskReadErrorsStrict(t *testing.T) {
	t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", "")
	if err := db.InitDB(t.TempDir() + "/task-read-error.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{ID: "origin-channel", Name: "Origin Channel", Type: "newapi", BaseURL: "https://api.provider.net/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.SQLDBForContext(context.Background()).Create(&db.RequestLogModel{ID: "valid-origin", Kind: "api_call", ChannelID: channel.ID, StartedAt: time.Now(), Method: http.MethodPost, Path: "/v1/images"}).Error; err != nil {
		t.Fatal(err)
	}
	// Inject a SQL-layer task read failure while leaving origin/channel reads
	// available. Only ErrRecordNotFound may recover a synchronous origin.
	query := db.DB.Callback().Query()
	const callback = "test:media_task_read_error"
	if err := query.Before("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "async_task_runs" {
			tx.AddError(errors.New("task row query failed"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = query.Remove(callback) })
	asset := &db.MediaAsset{TaskRunID: "unavailable-task", OriginRequestID: "valid-origin", SourceKind: media.SourceURL, SourceLocator: "http://download.xmimage2.cc.cd/image.png"}
	fetcher := defaultURLFetcherForAsset(context.Background(), asset)
	resolver := &materializerStaticDNS{hosts: map[string]struct{}{"download.xmimage2.cc.cd": {}}, ips: []string{"198.18.0.97"}}
	fetcher.Resolver = resolver
	var dialCalls atomic.Int32
	fetcher.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dialCalls.Add(1)
		return nil, errors.New("must not dial without durable provenance")
	}
	_, err := fetcher.Fetch(context.Background(), media.MediaResult{SourceKind: media.SourceURL, Locator: asset.SourceLocator})
	if err == nil || !strings.Contains(err.Error(), "non-public address") || resolver.calls.Load() != 1 || dialCalls.Load() != 0 {
		t.Fatalf("task SQL error provenance: err=%v DNS=%d dials=%d", err, resolver.calls.Load(), dialCalls.Load())
	}
}
