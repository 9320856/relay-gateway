package media

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestUpstreamMediaFollowsChangingCDNDomainsAndPinsEachHop(t *testing.T) {
	t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", "")
	for _, initialIP := range []string{"198.18.0.97", "2001:2::60", "8.8.8.8"} {
		t.Run(initialIP, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
					t.Error("media redirect carried upstream credentials")
				}
				switch r.URL.Path {
				case "/initial":
					if r.Host != "download.xmimage2.cc.cd" {
						t.Errorf("initial Host=%q", r.Host)
					}
					http.Redirect(w, r, "http://future-cdn.example.net/result.png", http.StatusFound)
				case "/result.png":
					if r.Host != "future-cdn.example.net" {
						t.Errorf("redirect Host=%q", r.Host)
					}
					w.Header().Set("Content-Type", "image/png")
					_, _ = io.WriteString(w, "provider-image")
				default:
					t.Errorf("unexpected media path=%q", r.URL.Path)
				}
			}))
			defer server.Close()
			fetcher := NewHTTPSourceFetcher("http://download.xmimage2.cc.cd/initial", "https://new-intermediary.example.org/v1")
			fetcher.Resolver = staticResolver(map[string][]string{
				"download.xmimage2.cc.cd": {initialIP}, "future-cdn.example.net": {"2001:2::61"},
			})
			var addresses []string
			fetcher.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				addresses = append(addresses, address)
				return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "http://"))
			}
			result, err := fetcher.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: "http://download.xmimage2.cc.cd/initial"})
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(result.Body)
			_ = result.Body.Close()
			if err != nil || string(body) != "provider-image" || calls.Load() != 2 {
				t.Fatalf("media bytes=%q requests=%d err=%v", body, calls.Load(), err)
			}
			if len(addresses) != 2 || addresses[0] != net.JoinHostPort(initialIP, "80") || addresses[1] != "[2001:2::61]:80" {
				t.Fatalf("redirect did not pin independently validated DNS addresses: %v", addresses)
			}
		})
	}
}

func TestUpstreamMediaStillRejectsActualPrivateAddressesAndInvalidOrigins(t *testing.T) {
	t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", "")
	const source = "https://download.xmimage2.cc.cd/image.png"
	for _, base := range []string{"", "ftp://api.example.org", "not a URL", "https://", "https://user:password@api.example.org"} {
		fetcher := NewHTTPSourceFetcher(source, base)
		fetcher.Resolver = staticResolver(map[string][]string{"download.xmimage2.cc.cd": {"198.18.0.97"}})
		if _, err := fetcher.validateURL(context.Background(), source); err == nil || fetcher.AllowUpstreamFakeIP {
			t.Errorf("invalid origin %q authorized a Fake-IP download: %v", base, err)
		}
	}
	for _, tc := range []struct {
		url string
		ips []string
	}{
		{source, []string{"127.0.0.1"}},
		{source, []string{"10.1.2.3"}},
		{source, []string{"169.254.169.254"}},
		{source, []string{"fd00::1"}},
		{source, []string{"198.18.0.97", "10.1.2.3"}},
		{source, []string{"198.18.0.97", "8.8.8.8"}},
		{"http://198.18.0.97/image.png", nil},
		{"http://[2001:2::60]/image.png", nil},
		{"http://metadata.google.internal/image.png", nil},
		{"file:///image.png", nil},
	} {
		fetcher := NewHTTPSourceFetcher(tc.url, "https://another-intermediary.example.net/v1")
		fetcher.Resolver = staticResolver(map[string][]string{"download.xmimage2.cc.cd": tc.ips})
		if _, err := fetcher.validateURL(context.Background(), tc.url); err == nil {
			t.Errorf("unsafe media target was accepted: %s %v", tc.url, tc.ips)
		}
	}
}

func TestUpstreamMediaRejectsRedirectToPrivateNetworkBeforeDial(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, "http://another-cdn.example.net/private.png", http.StatusFound)
	}))
	defer server.Close()
	const source = "http://download.xmimage2.cc.cd/image.png"
	fetcher := NewHTTPSourceFetcher(source, "https://api.example.org/v1")
	fetcher.Resolver = staticResolver(map[string][]string{
		"download.xmimage2.cc.cd": {"198.18.0.97"}, "another-cdn.example.net": {"192.168.1.1"},
	})
	var dials atomic.Int32
	fetcher.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "http://"))
	}
	if result, err := fetcher.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: source}); err == nil {
		_ = result.Body.Close()
		t.Fatal("media redirect into private network was accepted")
	}
	if calls.Load() != 1 || dials.Load() != 1 {
		t.Fatalf("private redirect dialed before address validation: requests=%d dials=%d", calls.Load(), dials.Load())
	}
}

func TestUpstreamMediaFetcherCanBeReusedConcurrently(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = io.WriteString(w, "concurrent-media")
	}))
	defer server.Close()
	const source = "http://download.xmimage2.cc.cd/image.png"
	fetcher := NewHTTPSourceFetcher(source, "https://future-intermediary.example.org/v1")
	fetcher.Resolver = staticResolver(map[string][]string{"download.xmimage2.cc.cd": {"198.18.0.97"}})
	fetcher.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "198.18.0.97:80" {
			t.Errorf("unvalidated dial=%q", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "http://"))
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			result, err := fetcher.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: source})
			if err != nil {
				t.Error(err)
				return
			}
			body, err := io.ReadAll(result.Body)
			_ = result.Body.Close()
			if err != nil || string(body) != "concurrent-media" {
				t.Errorf("media bytes=%q err=%v", body, err)
			}
		})
	}
	wg.Wait()
}
