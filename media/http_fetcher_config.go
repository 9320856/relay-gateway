package media

import (
	"net"
	"os"
	"strings"

	"golang.org/x/net/publicsuffix"
)

// NewHTTPSourceFetcher permits Fake-IP only for this source's hostname when it
// shares the provider's registrable domain or is explicitly configured by the
// deployer. The environment list accepts exact public domain names, not URLs,
// IP literals, wildcards, or suffix patterns. All other fetch policies remain
// the strict defaults.
func NewHTTPSourceFetcher(sourceURL, baseURL string) HTTPSourceFetcher {
	fetcher := HTTPSourceFetcher{}
	if host, ok := TrustedFakeIPHost(sourceURL, baseURL); ok {
		fetcher.TrustedFakeIPHosts = map[string]struct{}{host: {}}
		return fetcher
	}
	sourceHost, ok := registrableHTTPHost(sourceURL)
	if !ok {
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
	if err != nil || net.ParseIP(host) != nil || len(host) > 253 {
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
	_, icann := publicsuffix.PublicSuffix(host)
	if _, err := publicsuffix.EffectiveTLDPlusOne(host); err != nil || !icann {
		return "", false
	}
	return host, true
}
