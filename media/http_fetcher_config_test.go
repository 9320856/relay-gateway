package media

import (
	"context"
	"strings"
	"testing"
)

func TestNewHTTPSourceFetcherTrustConfiguration(t *testing.T) {
	const sourceHost = "cdn.example.net"
	const sourceURL = "https://cdn.example.net/image.png"
	const baseURL = "https://api.example.org/v1"
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
			fetcher := NewHTTPSourceFetcher(sourceURL, baseURL)
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

func TestNewHTTPSourceFetcherPreservesSameSiteTrust(t *testing.T) {
	t.Setenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS", "unrelated.example.com")
	fetcher := NewHTTPSourceFetcher("https://CDN.example.com./image.png", "https://api.example.com/v1")
	if _, ok := fetcher.TrustedFakeIPHosts["cdn.example.com"]; !ok || len(fetcher.TrustedFakeIPHosts) != 1 {
		t.Fatalf("trusted hosts=%v", fetcher.TrustedFakeIPHosts)
	}
}

func TestConfiguredPublicMediaHostRejectsMalformedAndNonPublicEntries(t *testing.T) {
	for _, entry := range []string{
		"", "localhost", "metadata.google.internal", "127.0.0.1", "::1", "198.18.0.160",
		"com", "co.uk", "service.local", "service.home.arpa", "test.invalid", "user.github.io",
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
