package media

import (
	"context"
	"net"
	"testing"
)

func TestHTTPSourceFetcherIPv6FakeIPPolicy(t *testing.T) {
	const host = "cdn.example.net"
	tests := []struct {
		name        string
		trusted     bool
		ips         []string
		wantAllowed bool
		wantFake    bool
	}{
		{"trusted ipv6 fake", true, []string{"2001:2::9d"}, true, true},
		{"trusted dual family fake", true, []string{"198.18.0.160", "2001:2::9d"}, true, true},
		{"trusted dual family fake reversed", true, []string{"2001:2::9d", "198.18.0.160"}, true, true},
		{"untrusted ipv6 fake", false, []string{"2001:2::9d"}, false, false},
		{"untrusted dual family fake", false, []string{"198.18.0.160", "2001:2::9d"}, false, false},
		{"trusted ipv6 fake and public ipv6", true, []string{"2001:2::9d", "2606:4700:4700::1111"}, false, false},
		{"trusted ipv6 public and fake reversed", true, []string{"2606:4700:4700::1111", "2001:2::9d"}, false, false},
		{"trusted ipv4 public and ipv6 fake", true, []string{"8.8.8.8", "2001:2::9d"}, false, false},
		{"trusted ipv4 fake and ipv6 public", true, []string{"198.18.0.160", "2606:4700:4700::1111"}, false, false},
		{"trusted ula", true, []string{"fdfe:dcba:9876::1"}, false, false},
		{"trusted dual family public", true, []string{"8.8.8.8", "2606:4700:4700::1111"}, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := HTTPSourceFetcher{Resolver: staticResolver(map[string][]string{host: tc.ips})}
			if tc.trusted {
				fetcher.TrustedFakeIPHosts = map[string]struct{}{host: {}}
			}
			target, err := fetcher.validateURL(context.Background(), "https://"+host+"/image.png")
			if (err == nil) != tc.wantAllowed {
				t.Fatalf("validateURL() target=%+v err=%v, want allowed=%v", target, err, tc.wantAllowed)
			}
			if err == nil && (target.fakeIP != tc.wantFake || len(target.validatedIPs) != len(tc.ips)) {
				t.Fatalf("validated target=%+v, want fake=%v with %d pinned addresses", target, tc.wantFake, len(tc.ips))
			}
		})
	}
}

func TestHTTPSourceFetcherRejectsIPv6FakeIPLiteral(t *testing.T) {
	fetcher := HTTPSourceFetcher{TrustedFakeIPHosts: map[string]struct{}{"2001:2::9d": {}}}
	if _, err := fetcher.validateURL(context.Background(), "https://[2001:2::9d]/image.png"); err == nil {
		t.Fatal("IPv6 benchmark literal unexpectedly allowed")
	}
}

func TestIsFakeIPIPv6Range(t *testing.T) {
	for _, raw := range []string{"2001:2::", "2001:2::9d", "2001:2:0:ffff:ffff:ffff:ffff:ffff"} {
		if !isFakeIP(net.ParseIP(raw)) {
			t.Errorf("isFakeIP(%s) = false", raw)
		}
	}
	for _, raw := range []string{"2001:1:ffff:ffff:ffff:ffff:ffff:ffff", "2001:2:1::", "2001:3::", "fdfe:dcba:9876::1", "2606:4700:4700::1111", "invalid"} {
		if isFakeIP(net.ParseIP(raw)) {
			t.Errorf("isFakeIP(%s) = true", raw)
		}
	}
}
