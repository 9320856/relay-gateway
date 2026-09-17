package media

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type resolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f resolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, host)
}

func staticResolver(records map[string][]string) IPResolver {
	return resolverFunc(func(_ context.Context, host string) ([]net.IPAddr, error) {
		values, ok := records[host]
		if !ok {
			return nil, fmt.Errorf("unexpected DNS lookup for %q", host)
		}
		result := make([]net.IPAddr, 0, len(values))
		for _, value := range values {
			result = append(result, net.IPAddr{IP: net.ParseIP(value)})
		}
		return result, nil
	})
}

func permissiveURL(string) error { return nil }

func TestHTTPSourceFetcherFetchesAllowedMedia(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png; charset=binary")
		_, _ = io.WriteString(w, "payload")
	}))
	defer srv.Close()
	f := HTTPSourceFetcher{URLValidator: permissiveURL}
	got, err := f.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: srv.URL})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	defer got.Body.Close()
	body, err := io.ReadAll(got.Body)
	if err != nil || string(body) != "payload" {
		t.Fatalf("body = %q, err = %v", body, err)
	}
	if got.ContentType != "image/png" {
		t.Fatalf("ContentType = %q", got.ContentType)
	}
}

func TestHTTPSourceFetcherRejectsStatusAndMIME(t *testing.T) {
	cases := []struct {
		name string
		code int
		mime string
	}{
		{"status", http.StatusNotFound, "image/png"},
		{"mime", http.StatusOK, "text/html"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.mime)
				w.WriteHeader(tc.code)
				_, _ = io.WriteString(w, "body")
			}))
			defer srv.Close()
			_, err := (HTTPSourceFetcher{URLValidator: permissiveURL}).Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: srv.URL})
			if err == nil {
				t.Fatal("Fetch() unexpectedly succeeded")
			}
		})
	}
}

func TestHTTPSourceFetcherRedirectPolicy(t *testing.T) {
	redirect := httptest.NewServer(http.RedirectHandler("http://redirect.invalid/final", http.StatusFound))
	defer redirect.Close()
	_, err := (HTTPSourceFetcher{URLValidator: func(raw string) error {
		if strings.Contains(raw, "redirect.invalid") {
			return nil
		}
		return nil
	}}).Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: redirect.URL})
	if err == nil {
		t.Fatal("Fetch() unexpectedly followed unreachable redirect")
	}
}

func TestHTTPSourceFetcherEnforcesStreamingLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Transfer-Encoding", "chunked")
		_, _ = io.WriteString(w, "123456")
	}))
	defer srv.Close()
	f := HTTPSourceFetcher{URLValidator: permissiveURL, MaxBytes: 5}
	got, err := f.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: srv.URL})
	if err != nil {
		if !strings.Contains(err.Error(), "exceeds 5 bytes") {
			t.Fatalf("Fetch() error = %v", err)
		}
		return
	}
	defer got.Body.Close()
	_, err = io.ReadAll(got.Body)
	if err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("ReadAll() err = %v, want size limit error", err)
	}
}

func TestValidatePublicHTTPURLRejectsPrivateAddresses(t *testing.T) {
	for _, raw := range []string{
		"http://127.0.0.1/x",
		"http://localhost/x",
		"http://10.0.0.1/x",
		"ftp://example.com/x",
		"http://100.64.0.1/x",
		"http://100.127.255.254/x",
		"http://192.0.2.1/x",
		"http://198.51.100.1/x",
		"http://203.0.113.1/x",
		"http://198.18.0.1/x",
		"http://169.254.169.254/x",
		"http://[::1]/x",
		"http://[fc00::1]/x",
		"http://[fe80::1]/x",
	} {
		if err := validatePublicHTTPURL(raw); err == nil {
			t.Errorf("validatePublicHTTPURL(%q) succeeded, want error", raw)
		}
	}
}

func TestValidatePublicIP(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1",
		"100.64.0.1", "100.100.1.1", "100.127.255.255",
		"169.254.169.254", "192.0.0.1", "192.0.2.1",
		"198.18.0.1", "198.19.255.255", "198.51.100.1",
		"203.0.113.1", "240.0.0.1", "255.255.255.255",
		"0.0.0.0", "::", "::1", "fc00::1", "fe80::1",
	}
	for _, ipStr := range blocked {
		ip := net.ParseIP(ipStr)
		if err := ValidatePublicIP(ip); err == nil {
			t.Errorf("ValidatePublicIP(%s) expected error, got nil", ipStr)
		}
	}

	allowed := []string{
		"8.8.8.8", "1.1.1.1", "142.250.190.46", "2606:4700:4700::1111",
	}
	for _, ipStr := range allowed {
		ip := net.ParseIP(ipStr)
		if err := ValidatePublicIP(ip); err != nil {
			t.Errorf("ValidatePublicIP(%s) unexpected error: %v", ipStr, err)
		}
	}
}

func TestHTTPSourceFetcherRejectsPrivateDirectDial(t *testing.T) {
	// A fetcher without custom URLValidator should reject attempts to dial private addresses at dial time
	f := HTTPSourceFetcher{}
	_, err := f.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: "http://100.64.0.1:8080/image.png"})
	if err == nil {
		t.Fatal("Fetch() expected error for CGNAT IP, got nil")
	}
}

func TestHTTPSourceFetcherTrustedFakeIPHost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = io.WriteString(w, "fake-ip-media")
	}))
	t.Cleanup(server.Close)
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(serverURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	var dialed string
	fetcher := HTTPSourceFetcher{
		TrustedFakeIPHosts: map[string]struct{}{"cdn.fanrenapi.com": {}},
		Resolver:           staticResolver(map[string][]string{"cdn.fanrenapi.com": {"198.18.12.34"}}),
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			dialed = address
			return (&net.Dialer{}).DialContext(ctx, network, serverURL.Host)
		},
	}
	got, err := fetcher.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: "http://cdn.fanrenapi.com:" + port + "/video.mp4"})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	defer got.Body.Close()
	body, err := io.ReadAll(got.Body)
	if err != nil || string(body) != "fake-ip-media" {
		t.Fatalf("body = %q, err = %v", body, err)
	}
	if dialed != "198.18.12.34:"+port {
		t.Fatalf("dialed %q, want pinned Fake-IP", dialed)
	}
}

func TestHTTPSourceFetcherFakeIPPolicyRejectsUnsafeTargets(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		trusted map[string]struct{}
		records map[string][]string
	}{
		{
			name:    "untrusted fake ip host",
			rawURL:  "https://cdn.fanrenapi.com/video.mp4",
			records: map[string][]string{"cdn.fanrenapi.com": {"198.18.1.1"}},
		},
		{
			name:    "fake ip literal",
			rawURL:  "https://198.18.1.1/video.mp4",
			trusted: map[string]struct{}{"198.18.1.1": {}},
		},
		{
			name:    "mixed public and fake ip",
			rawURL:  "https://cdn.fanrenapi.com/video.mp4",
			trusted: map[string]struct{}{"cdn.fanrenapi.com": {}},
			records: map[string][]string{"cdn.fanrenapi.com": {"198.18.1.1", "8.8.8.8"}},
		},
		{
			name:    "trusted host other private address",
			rawURL:  "https://cdn.fanrenapi.com/video.mp4",
			trusted: map[string]struct{}{"cdn.fanrenapi.com": {}},
			records: map[string][]string{"cdn.fanrenapi.com": {"10.0.0.1"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := HTTPSourceFetcher{TrustedFakeIPHosts: tc.trusted, Resolver: staticResolver(tc.records)}
			if _, err := fetcher.validateURL(context.Background(), tc.rawURL); err == nil {
				t.Fatalf("validateURL(%q) succeeded", tc.rawURL)
			}
		})
	}
}

func TestHTTPSourceFetcherAllowsNormalPublicTarget(t *testing.T) {
	fetcher := HTTPSourceFetcher{Resolver: staticResolver(map[string][]string{"media.example.net": {"8.8.8.8", "1.1.1.1"}})}
	target, err := fetcher.validateURL(context.Background(), "https://media.example.net/video.mp4")
	if err != nil || len(target.validatedIPs) != 2 {
		t.Fatalf("validateURL() target=%+v err=%v", target, err)
	}
}

func TestHTTPSourceFetcherRejectsRedirectToDifferentFakeIPHost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://assets.fanrenapi.com"+r.URL.RequestURI(), http.StatusFound)
	}))
	t.Cleanup(server.Close)
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(serverURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	fetcher := HTTPSourceFetcher{
		TrustedFakeIPHosts: map[string]struct{}{"cdn.fanrenapi.com": {}},
		Resolver: staticResolver(map[string][]string{
			"cdn.fanrenapi.com":    {"198.18.1.1"},
			"assets.fanrenapi.com": {"198.18.1.2"},
		}),
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, serverURL.Host)
		},
	}
	_, err = fetcher.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: "http://cdn.fanrenapi.com:" + port + "/video.mp4"})
	if err == nil || !strings.Contains(err.Error(), "unsafe media redirect") {
		t.Fatalf("Fetch() err = %v, want unsafe redirect", err)
	}
}

func TestTrustedFakeIPHost(t *testing.T) {
	tests := []struct {
		name   string
		source string
		base   string
		want   string
	}{
		{"same registrable domain", "https://CDN.fanrenapi.com./video", "https://api.fanrenapi.com/v1", "cdn.fanrenapi.com"},
		{"multi-label public suffix", "https://cdn.example.co.uk/video", "https://api.example.co.uk/v1", "cdn.example.co.uk"},
		{"different domain", "https://cdn.attacker.com/video", "https://api.fanrenapi.com/v1", ""},
		{"ip source", "https://198.18.1.1/video", "https://api.fanrenapi.com/v1", ""},
		{"private suffix", "https://cdn.service.internal/video", "https://api.service.internal/v1", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := TrustedFakeIPHost(tc.source, tc.base)
			if got != tc.want || ok != (tc.want != "") {
				t.Fatalf("TrustedFakeIPHost() = %q, %v; want %q", got, ok, tc.want)
			}
		})
	}
}
