package media

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
)

// HTTPSourceFetcher fetches URL media while enforcing network and content
// policy. URLValidator may be supplied by tests or deployments with a custom
// egress policy; nil uses the built-in public-address validator.
type HTTPSourceFetcher struct {
	Client             *http.Client
	MaxBytes           int64
	MaxRedirects       int
	AllowedContentType map[string]struct{}
	URLValidator       func(string) error
	TrustedFakeIPHosts map[string]struct{}
	Resolver           IPResolver
	DialContext        func(context.Context, string, string) (net.Conn, error)
}

// IPResolver is the DNS surface used by the media fetcher's validation and
// dial policy. net.DefaultResolver is used when Resolver is nil.
type IPResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

const defaultHTTPFetchMaxBytes int64 = 50 * 1024 * 1024

var defaultHTTPAllowedTypes = map[string]struct{}{
	"image/jpeg": {}, "image/png": {}, "image/webp": {}, "image/gif": {},
	"image/avif": {}, "image/apng": {}, "image/svg+xml": {},
	"video/mp4": {}, "video/webm": {}, "video/ogg": {}, "video/quicktime": {},
	"audio/mpeg": {}, "audio/mp4": {}, "audio/ogg": {}, "audio/wav": {}, "audio/webm": {},
}

// mediaTargetHostKey carries the addresses approved during URL validation into
// the transport, closing DNS rebinding gaps.
type mediaTargetHostKey struct{}

type mediaInitialTargetKey struct{}

type mediaTarget struct {
	host            string
	validatedIPs    []net.IP
	customValidator bool
	fakeIP          bool
}

var defaultMediaDialer = &net.Dialer{
	Timeout:   15 * time.Second,
	KeepAlive: 30 * time.Second,
}

func (f HTTPSourceFetcher) newDefaultMediaTransport() *http.Transport {
	return &http.Transport{
		Proxy: nil, // Protected media fetching prioritizes direct connection to verified public IPs
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return f.dialSafeMediaAddress(ctx, network, address)
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
}

func (f HTTPSourceFetcher) dialSafeMediaAddress(ctx context.Context, network, address string) (net.Conn, error) {
	dial := f.DialContext
	if dial == nil {
		dial = defaultMediaDialer.DialContext
	}
	target, ok := ctx.Value(mediaTargetHostKey{}).(mediaTarget)
	if target.customValidator {
		return dial(ctx, network, address)
	}
	if !ok || target.host == "" || len(target.validatedIPs) == 0 {
		return nil, fmt.Errorf("media dial target was not validated")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid media address: %w", err)
	}
	dialHost, err := normalizeMediaHostname(host)
	if err != nil || dialHost != target.host {
		return nil, fmt.Errorf("media dial host does not match validated target")
	}
	var lastErr error
	for _, ip := range target.validatedIPs {
		dialTarget := net.JoinHostPort(ip.String(), port)
		conn, dialErr := dial(ctx, network, dialTarget)
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	return nil, fmt.Errorf("dial media address for %s: %w", target.host, lastErr)
}

func (f HTTPSourceFetcher) Fetch(ctx context.Context, result MediaResult) (FetchedSource, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !strings.EqualFold(strings.TrimSpace(result.SourceKind), SourceURL) {
		return FetchedSource{}, fmt.Errorf("http source fetcher does not support kind %q", result.SourceKind)
	}
	raw := strings.TrimSpace(result.Locator)
	if raw == "" {
		return FetchedSource{}, fmt.Errorf("media URL is required")
	}
	target, err := f.validateURL(ctx, raw)
	if err != nil {
		return FetchedSource{}, err
	}
	max := f.MaxBytes
	if max <= 0 {
		max = defaultHTTPFetchMaxBytes
	}
	redirects := f.MaxRedirects
	if redirects <= 0 {
		redirects = 5
	}
	client := f.Client
	if client == nil {
		client = &http.Client{
			Transport: f.newDefaultMediaTransport(),
			Timeout:   60 * time.Second,
		}
	} else {
		clone := *client
		if clone.Transport == nil {
			clone.Transport = f.newDefaultMediaTransport()
		}
		client = &clone
	}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= redirects {
			return fmt.Errorf("too many media redirects")
		}
		redirectTarget, err := f.validateURL(req.Context(), req.URL.String())
		if err != nil {
			return fmt.Errorf("unsafe media redirect: %w", err)
		}
		if initial, ok := req.Context().Value(mediaInitialTargetKey{}).(mediaTarget); ok && initial.fakeIP && redirectTarget.fakeIP && redirectTarget.host != initial.host {
			return fmt.Errorf("unsafe media redirect: Fake-IP host changed")
		}
		*req = *req.WithContext(context.WithValue(req.Context(), mediaTargetHostKey{}, redirectTarget))
		return nil
	}
	reqCtx := context.WithValue(ctx, mediaTargetHostKey{}, target)
	reqCtx = context.WithValue(reqCtx, mediaInitialTargetKey{}, target)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, raw, nil)
	if err != nil {
		return FetchedSource{}, fmt.Errorf("create media request: %w", err)
	}
	req.Header.Set("Accept", "image/*,video/*,audio/*")
	resp, err := client.Do(req)
	if err != nil {
		return FetchedSource{}, fmt.Errorf("fetch media URL: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return FetchedSource{}, fmt.Errorf("upstream returned status %d", resp.StatusCode)
	}
	if resp.ContentLength > max {
		resp.Body.Close()
		return FetchedSource{}, fmt.Errorf("upstream media exceeds %d bytes", max)
	}
	contentType, ok := allowedContentType(resp.Header.Get("Content-Type"), f.AllowedContentType)
	if !ok {
		resp.Body.Close()
		return FetchedSource{}, fmt.Errorf("unsupported upstream media type")
	}
	return FetchedSource{Body: &limitedBody{ReadCloser: resp.Body, remaining: max}, ContentType: contentType}, nil
}

func allowedContentType(raw string, allowed map[string]struct{}) (string, bool) {
	mt, _, err := mime.ParseMediaType(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	mt = strings.ToLower(mt)
	if allowed == nil {
		allowed = defaultHTTPAllowedTypes
	}
	_, ok := allowed[mt]
	return mt, ok
}

type limitedBody struct {
	io.ReadCloser
	remaining int64
}

func (r *limitedBody) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		var one [1]byte
		n, err := r.ReadCloser.Read(one[:])
		if n > 0 {
			return 0, fmt.Errorf("upstream media exceeds configured size limit")
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.ReadCloser.Read(p)
	r.remaining -= int64(n)
	return n, err
}

func validatePublicHTTPURL(raw string) error {
	_, err := (HTTPSourceFetcher{}).validateURL(context.Background(), raw)
	return err
}

func (f HTTPSourceFetcher) validateURL(ctx context.Context, raw string) (mediaTarget, error) {
	if f.URLValidator != nil {
		if err := f.URLValidator(raw); err != nil {
			return mediaTarget{}, err
		}
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return mediaTarget{}, fmt.Errorf("invalid media URL")
		}
		host, err := normalizeMediaHostname(u.Hostname())
		if err != nil {
			return mediaTarget{}, fmt.Errorf("invalid media URL")
		}
		return mediaTarget{host: host, customValidator: true}, nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return mediaTarget{}, fmt.Errorf("invalid media URL")
	}
	host, err := normalizeMediaHostname(u.Hostname())
	if err != nil {
		return mediaTarget{}, fmt.Errorf("invalid media URL")
	}
	if host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "169.254.169.254" || strings.HasPrefix(host, "169.254.") || host == "metadata.google.internal" {
		return mediaTarget{}, fmt.Errorf("access to private media address is prohibited")
	}
	for _, suffix := range []string{".invalid", ".example", ".test", ".localhost", ".local", ".internal", ".home.arpa"} {
		if host == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(host, suffix) {
			return mediaTarget{}, fmt.Errorf("media host uses a reserved non-public domain")
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		if err := ValidatePublicIP(ip); err != nil {
			return mediaTarget{}, err
		}
		return mediaTarget{host: host, validatedIPs: []net.IP{cloneIP(ip)}}, nil
	}
	resolver := f.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, err := resolver.LookupIPAddr(ctx, host)
	if err != nil || len(ips) == 0 {
		return mediaTarget{}, fmt.Errorf("unable to resolve media host")
	}
	allFakeIP := f.isTrustedFakeIPHost(host)
	validated := make([]net.IP, 0, len(ips))
	for _, ipAddr := range ips {
		ip := ipAddr.IP
		if ValidatePublicIP(ip) != nil {
			if !allFakeIP || !isFakeIP(ip) {
				return mediaTarget{}, fmt.Errorf("media host resolved to non-public address %s: access to private or special-use address is prohibited", ip.String())
			}
		} else if allFakeIP {
			// A trusted Fake-IP host must resolve exclusively into 198.18.0.0/15.
			// Mixed public/Fake-IP answers fail closed.
			return mediaTarget{}, fmt.Errorf("trusted Fake-IP media host returned a non-Fake-IP address")
		}
		validated = append(validated, cloneIP(ip))
	}
	return mediaTarget{host: host, validatedIPs: validated, fakeIP: allFakeIP}, nil
}

func (f HTTPSourceFetcher) isTrustedFakeIPHost(host string) bool {
	if net.ParseIP(host) != nil {
		return false
	}
	for candidate := range f.TrustedFakeIPHosts {
		normalized, err := normalizeMediaHostname(candidate)
		if err == nil && net.ParseIP(normalized) == nil && normalized == host {
			return true
		}
	}
	return false
}

func normalizeMediaHostname(host string) (string, error) {
	host = strings.TrimSuffix(strings.TrimSpace(host), ".")
	if host == "" {
		return "", fmt.Errorf("empty host")
	}
	if ip := net.ParseIP(host); ip != nil {
		return strings.ToLower(ip.String()), nil
	}
	ascii, err := idna.Lookup.ToASCII(host)
	if err != nil || ascii == "" {
		return "", fmt.Errorf("invalid host")
	}
	return strings.ToLower(ascii), nil
}

func cloneIP(ip net.IP) net.IP {
	return append(net.IP(nil), ip...)
}

func isFakeIP(ip net.IP) bool {
	v4 := ip.To4()
	return v4 != nil && v4[0] == 198 && (v4[1] == 18 || v4[1] == 19)
}

// TrustedFakeIPHost returns the normalized source hostname when sourceURL and
// baseURL are HTTP(S) URLs under the same registrable domain. IP literals and
// hosts without an ICANN/public-suffix registrable domain are never returned.
func TrustedFakeIPHost(sourceURL, baseURL string) (string, bool) {
	sourceHost, ok := registrableHTTPHost(sourceURL)
	if !ok {
		return "", false
	}
	baseHost, ok := registrableHTTPHost(baseURL)
	if !ok {
		return "", false
	}
	sourceDomain, err := publicsuffix.EffectiveTLDPlusOne(sourceHost)
	_, sourceICANN := publicsuffix.PublicSuffix(sourceHost)
	if err != nil || !sourceICANN {
		return "", false
	}
	baseDomain, err := publicsuffix.EffectiveTLDPlusOne(baseHost)
	_, baseICANN := publicsuffix.PublicSuffix(baseHost)
	if err != nil || !baseICANN || sourceDomain != baseDomain {
		return "", false
	}
	return sourceHost, true
}

func registrableHTTPHost(raw string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return "", false
	}
	host, err := normalizeMediaHostname(u.Hostname())
	if err != nil || net.ParseIP(host) != nil {
		return "", false
	}
	return host, true
}

// ValidatePublicIP verifies that ip is a valid, globally routable public address,
// rejecting loopback, private, link-local, multicast, unspecified, carrier-grade NAT
// (100.64.0.0/10), and documentation/special-use subnets.
func ValidatePublicIP(ip net.IP) error {
	if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || !ip.IsGlobalUnicast() {
		return fmt.Errorf("access to private or non-global media address is prohibited")
	}
	if v4 := ip.To4(); v4 != nil {
		if (v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127) ||
			(v4[0] == 192 && v4[1] == 0 && (v4[2] == 0 || v4[2] == 2)) ||
			(v4[0] == 198 && (v4[1] == 18 || v4[1] == 19)) ||
			(v4[0] == 198 && v4[1] == 51 && v4[2] == 100) ||
			(v4[0] == 203 && v4[1] == 0 && v4[2] == 113) ||
			v4[0] >= 240 {
			return fmt.Errorf("access to private or special-use address is prohibited")
		}
	}
	return nil
}
