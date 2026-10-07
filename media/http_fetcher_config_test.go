package media

import (
	"context"
	"strings"
	"testing"
)

func TestNewHTTPSourceFetcherWithoutUpstreamRequiresExactConfiguration(t *testing.T) {
	const sourceHost = "cdn.example.net"
	const sourceURL = "https://cdn.example.net/image.png"
	tests := []struct {
		name       string
		configured string
		wantTrust  bool
	}{
		{"absent cross domain", "", false},
		{"exact cross domain", sourceHost, true},
		{"normalized exact cross domain", " CDN.EXAMPLE.NET. ", true},
		{"exact entry among others", "assets.example.org," + sourceHost + ",cdn.example.com", true},
		{"parent domain", "example.net", false},
		{"other subdomain", "other.example.net", false},
		{"wildcard", "*.example.net", false},
		{"suffix pattern", "+.example.net", false},
		{"url", "https://" + sourceHost, false},
		{"ip literal", "198.18.0.160", false},
		{"port", sourceHost + ":443", false},
		{"path", sourceHost + "/", false},
		{"user info", "user@" + sourceHost, false},
		{"malformed list", "localhost,metadata.google.internal,198.18.0.160,*.example.net,https://" + sourceHost, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", tc.configured)
			fetcher := NewHTTPSourceFetcher(sourceURL, "")
			_, trusted := fetcher.TrustedFakeIPHosts[sourceHost]
			if trusted != tc.wantTrust || (trusted && len(fetcher.TrustedFakeIPHosts) != 1) {
				t.Fatalf("trusted hosts=%v, want trust=%v", fetcher.TrustedFakeIPHosts, tc.wantTrust)
			}
			fetcher.Resolver = staticResolver(map[string][]string{sourceHost: {"198.18.0.160"}})
			target, err := fetcher.validateURL(context.Background(), sourceURL)
			if (err == nil) != tc.wantTrust || (err == nil && !target.fakeIP) {
				t.Fatalf("validateURL() target=%+v err=%v, want trust=%v", target, err, tc.wantTrust)
			}
		})
	}
}

func TestNewHTTPSourceFetcherSupportsFutureUpstreamsAndCDNs(t *testing.T) {
	t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", "unrelated.example.com")
	for _, base := range []string{"https://api.example.com/v1", "https://new-relay.example.org/v1", "http://127.0.0.1:8001/v1"} {
		for _, host := range []string{"cdn.example.com", "different-cdn.example.net", "download.xmimage2.cc.cd", "user.github.io", "github.io", "r2.dev", "xn--bcher-kva.example.org"} {
			fetcher := NewHTTPSourceFetcher("https://"+host+"/image.png", base)
			if !fetcher.AllowUpstreamFakeIP {
				t.Fatalf("upstream %q media %q requires a manual domain list", base, host)
			}
			fetcher.Resolver = staticResolver(map[string][]string{host: {"198.18.0.97", "2001:2::60"}})
			if target, err := fetcher.validateURL(context.Background(), "https://"+host+"/image.png"); err != nil || !target.fakeIP {
				t.Fatalf("upstream %q media %q was blocked: target=%+v err=%v", base, host, target, err)
			}
		}
	}
}

func TestConfiguredPublicMediaHostRejectsMalformedAndNonPublicEntries(t *testing.T) {
	for _, entry := range []string{
		"", "localhost", "metadata.google.internal", "127.0.0.1", "::1", "198.18.0.160",
		"com", "service.local", "service.home.arpa", "test.invalid",
		"*.example.com", "https://example.com", "example.com:443", "user@example.com",
		"example.com/path", "example.com?query", "example.com#fragment",
		"a..example.com", "-a.example.com", "a-.example.com", "a_b.example.com",
		strings.Repeat("a", 64) + ".example.com", "example.com..",
	} {
		t.Run(entry, func(t *testing.T) {
			if host, ok := configuredPublicMediaHost(entry); ok {
				t.Fatalf("configuredPublicMediaHost(%q) accepted %q", entry, host)
			}
			t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", entry)
			fetcher := NewHTTPSourceFetcher("https://"+entry+"/image.png", "https://api.example.org")
			if len(fetcher.TrustedFakeIPHosts) != 0 {
				t.Fatalf("invalid source or config trusted hosts=%v", fetcher.TrustedFakeIPHosts)
			}
		})
	}
}
