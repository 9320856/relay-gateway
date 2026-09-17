package router

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	relaymedia "relay-gateway/media"
	"relay-gateway/security"
)

var (
	mediaClientOnce sync.Once
	mediaClient     *http.Client
)

const maxMediaProxyBytes int64 = 50 * 1024 * 1024

// mediaTargetHostKey identifies the URL host that a direct transport dial is
// allowed to connect to. It lets the transport re-resolve and pin that host at
// connection time, closing the gap between initial validation and dialing.
type mediaTargetHostKey struct{}

var mediaDialer = &net.Dialer{
	Timeout:   15 * time.Second,
	KeepAlive: 30 * time.Second,
}

func getMediaProxyClient() *http.Client {
	mediaClientOnce.Do(func() {
		transport := &http.Transport{
			Proxy:                 nil, // Media proxying strictly connects directly to verified public IPs, preventing bypass where environment or system proxies re-resolve domains to internal networks.
			DialContext:           dialSafeMediaTarget,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		}
		mediaClient = &http.Client{
			Transport: transport,
			Timeout:   60 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return fmt.Errorf("too many media redirects")
				}
				if _, err := isSafeMediaURL(req.URL.String()); err != nil {
					return fmt.Errorf("unsafe media redirect: %w", err)
				}
				*req = *req.WithContext(context.WithValue(req.Context(), mediaTargetHostKey{}, strings.ToLower(req.URL.Hostname())))
				return nil
			},
		}
	})
	return mediaClient
}

// isSafeMediaURL ensures the requested media URL is a valid remote public HTTP(S) resource,
// blocking loopback, internal networks, and cloud metadata to prevent SSRF.
func isSafeMediaURL(rawURL string) (*url.URL, error) {
	return isSafeMediaURLWithResolver(rawURL, net.LookupHost)
}

// isSafeMediaURLWithResolver is kept separate for deterministic tests. Every
// resolved address must be public; accepting a mixed public/private result
// would let an attacker rely on the transport picking the private address.
func isSafeMediaURLWithResolver(rawURL string, lookupHost func(string) ([]string, error)) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("invalid URL scheme: must be http or https")
	}
	if u.User != nil {
		return nil, fmt.Errorf("URL userinfo is not allowed")
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return nil, fmt.Errorf("access to localhost is prohibited")
	}
	if host == "169.254.169.254" || strings.HasPrefix(host, "169.254.") || host == "metadata.google.internal" {
		return nil, fmt.Errorf("access to metadata address is prohibited")
	}
	for _, suffix := range []string{".invalid", ".example", ".test", ".localhost", ".local", ".internal", ".home.arpa"} {
		if host == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(host, suffix) {
			return nil, fmt.Errorf("media host uses a reserved non-public domain")
		}
	}
	if ip := net.ParseIP(host); ip != nil {
		if err := validatePublicMediaIP(ip); err != nil {
			return nil, err
		}
		return u, nil
	}
	if lookupHost == nil {
		return nil, fmt.Errorf("media resolver is unavailable")
	}
	ips, err := lookupHost(host)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve media host: %w", err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("unable to resolve media host: no IP address returned")
	}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return nil, fmt.Errorf("unable to resolve media host: invalid IP address")
		}
		if err := validatePublicMediaIP(ip); err != nil {
			return nil, err
		}
	}
	return u, nil
}

func validatePublicMediaIP(ip net.IP) error {
	return relaymedia.ValidatePublicIP(ip)
}

// dialSafeMediaTarget revalidates the target host at the instant a direct connection
// is opened and dials the validated address instead of an unvalidated hostname.
// Environment or system proxies are disabled so target domains cannot be re-resolved to internal networks.
func dialSafeMediaTarget(ctx context.Context, network, address string) (net.Conn, error) {
	targetHost, _ := ctx.Value(mediaTargetHostKey{}).(string)
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid media address: %w", err)
	}
	checkHost := host
	if targetHost != "" && strings.EqualFold(host, targetHost) {
		checkHost = targetHost
	}
	if ip := net.ParseIP(checkHost); ip != nil {
		if err := validatePublicMediaIP(ip); err != nil {
			return nil, err
		}
		return mediaDialer.DialContext(ctx, network, address)
	}
	ips, err := net.DefaultResolver.LookupHost(ctx, checkHost)
	if err != nil {
		return nil, fmt.Errorf("resolve media host for dial: %w", err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolve media host for dial: no IP address returned")
	}
	var validIPs []string
	for _, ipStr := range ips {
		parsed := net.ParseIP(ipStr)
		if parsed == nil {
			return nil, fmt.Errorf("resolve media host for dial: invalid IP address")
		}
		if err := validatePublicMediaIP(parsed); err != nil {
			return nil, err
		}
		validIPs = append(validIPs, ipStr)
	}
	var lastErr error
	for _, ipStr := range validIPs {
		conn, dialErr := mediaDialer.DialContext(ctx, network, net.JoinHostPort(ipStr, port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	return nil, fmt.Errorf("dial validated media addresses for %s: %w", host, lastErr)
}

func detectMediaType(path string) string {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".jpg") || strings.HasSuffix(lower, ".jpeg"):
		return "image/jpeg"
	case strings.HasSuffix(lower, ".png"):
		return "image/png"
	case strings.HasSuffix(lower, ".webp"):
		return "image/webp"
	case strings.HasSuffix(lower, ".gif"):
		return "image/gif"
	case strings.HasSuffix(lower, ".svg"):
		return "image/svg+xml"
	case strings.HasSuffix(lower, ".mp4"):
		return "video/mp4"
	case strings.HasSuffix(lower, ".webm"):
		return "video/webm"
	default:
		return ""
	}
}

// isAllowedMediaContentType prevents the proxy from reflecting an arbitrary
// upstream Content-Type (most importantly text/html or script content) into a
// browser response.  Parameters such as charset are ignored for the allowlist
// decision; callers should use the parsed media type when writing headers.
func isAllowedMediaContentType(contentType string) (string, bool) {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil {
		return "", false
	}
	mediaType = strings.ToLower(mediaType)
	if _, ok := allowedMediaContentTypes[mediaType]; ok {
		return mediaType, true
	}
	return "", false
}

var allowedMediaContentTypes = map[string]struct{}{
	"image/jpeg": {}, "image/png": {}, "image/webp": {}, "image/gif": {},
	"image/avif": {}, "image/apng": {}, "image/svg+xml": {},
	"video/mp4": {}, "video/webm": {}, "video/ogg": {}, "video/quicktime": {},
	"audio/mpeg": {}, "audio/mp4": {}, "audio/ogg": {}, "audio/wav": {}, "audio/webm": {},
}

func setMediaProxySecurityHeaders(c *gin.Context) {
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Content-Security-Policy", safeMediaContentSecurityPolicy)
	c.Header("Cross-Origin-Resource-Policy", "same-origin")
	c.Header("Referrer-Policy", "no-referrer")
}

// handleMediaProxy fetches upstream images/media on behalf of the user,
// bypassing CORS, referrer restrictions, and foreign network blocking.
func handleMediaProxy(c *gin.Context) {
	setMediaProxySecurityHeaders(c)
	// Authentication: admin session cookie OR gateway Bearer token/header.
	sess, err := currentSession(c)
	if err != nil || sess == nil {
		token := extractBearerToken(c)
		if token == "" || !security.ValidateGatewayToken(token) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未授权访问"})
			return
		}
	}

	rawURL := strings.TrimSpace(c.Query("url"))
	if rawURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "缺少 url 参数"})
		return
	}

	// Internal gateway-managed media endpoints (/v1/media/*, /api/media-assets/*):
	// If the URL targets this gateway's own media endpoints, redirect directly
	// to avoid SSRF loopback dial restrictions and serve immediately.
	if parsed, err := url.Parse(rawURL); err == nil {
		cleanPath := path.Clean(parsed.Path)
		if !strings.Contains(parsed.Path, "..") && (strings.HasPrefix(cleanPath, "/v1/media/") || strings.HasPrefix(cleanPath, "/api/media-assets/")) {
			host := strings.ToLower(parsed.Hostname())
			if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.EqualFold(parsed.Host, c.Request.Host) {
				c.Redirect(http.StatusTemporaryRedirect, parsed.RequestURI())
				return
			}
		}
	}

	targetURL, err := isSafeMediaURL(rawURL)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "媒体地址无效或不允许访问"})
		return
	}

	requestCtx := context.WithValue(c.Request.Context(), mediaTargetHostKey{}, strings.ToLower(targetURL.Hostname()))
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		requestError(c, err, "媒体请求创建失败，请检查地址后重试")
		return
	}

	// Browser-like headers, omit Referer to prevent anti-hotlinking 403
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "image/avif,image/webp,image/apng,image/svg+xml,image/*,video/*,*/*;q=0.8")

	client := getMediaProxyClient()
	resp, err := client.Do(req)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "获取上游媒体失败，请稍后重试"})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		c.JSON(resp.StatusCode, gin.H{"error": fmt.Sprintf("上游媒体服务返回状态码 %d", resp.StatusCode)})
		return
	}

	if resp.ContentLength > maxMediaProxyBytes {
		c.JSON(http.StatusBadGateway, gin.H{"error": "上游媒体超过 50 MB 代理限制"})
		return
	}

	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if contentType == "" || strings.EqualFold(contentType, "application/octet-stream") {
		contentType = detectMediaType(targetURL.Path)
	}
	parsedContentType, allowed := isAllowedMediaContentType(contentType)
	if !allowed {
		c.JSON(http.StatusBadGateway, gin.H{"error": "上游响应不是支持的媒体类型"})
		return
	}

	c.Header("Content-Type", parsedContentType)
	c.Header("Cache-Control", "private, max-age=86400")
	if cl := resp.Header.Get("Content-Length"); cl != "" && resp.ContentLength >= 0 && resp.ContentLength <= maxMediaProxyBytes {
		c.Header("Content-Length", cl)
	}

	// Stream max 50MB
	lr := io.LimitReader(resp.Body, maxMediaProxyBytes)
	_, _ = io.Copy(c.Writer, lr)
}
