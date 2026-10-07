package media

import (
	"net"
	"net/url"
	"os"
	"strings"
)

// NewHTTPSourceFetcher handles media returned by an upstream and its CDN
// redirects without requiring a provider/CDN domain list. A nonempty valid
// HTTP(S) baseURL identifies upstream response media; callers must pass that
// origin rather than a client-supplied hint. Downloads without this origin use
// the exact environment host list. Neither mode admits actual private IPs.
func NewHTTPSourceFetcher(sourceURL, baseURL string) HTTPSourceFetcher {
	fetcher := HTTPSourceFetcher{}
	sourceHost, ok := registrableHTTPHost(sourceURL)
	if !ok {
		return fetcher
	}
	if _, ok := configuredPublicMediaHost(sourceHost); !ok {
		return fetcher
	}
	if upstream, err := url.Parse(strings.TrimSpace(baseURL)); err == nil && upstream.Hostname() != "" && upstream.User == nil && (upstream.Scheme == "http" || upstream.Scheme == "https") {
		fetcher.AllowUpstreamFakeIP = true
		return fetcher
	}
	for _, entry := range strings.Split(os.Getenv("RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS"), ",") {
		host, ok := configuredPublicMediaHost(entry)
		if ok && host == sourceHost {
			fetcher.TrustedFakeIPHosts = map[string]struct{}{sourceHost: {}}
			break
		}
	}
	return fetcher
}

func configuredPublicMediaHost(entry string) (string, bool) {
	host, err := normalizeMediaHostname(entry)
	if err != nil || net.ParseIP(host) != nil || len(host) > 253 || !strings.Contains(host, ".") {
		return "", false
	}
	for _, suffix := range []string{".invalid", ".example", ".test", ".localhost", ".local", ".internal", ".home.arpa"} {
		if host == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(host, suffix) {
			return "", false
		}
	}
	// IDNA normalization alone does not enforce DNS hostname label syntax.
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", false
		}
		for _, ch := range label {
			if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-') {
				return "", false
			}
		}
	}
	// A public-suffix classification describes domain registration boundaries,
	// not network reachability. Address validation decides where we may dial.
	return host, true
}
