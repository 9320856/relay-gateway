package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/model"
)

// CtxKey is an unexported type for context keys defined in this package,
// preventing collisions with keys defined in other packages.
type CtxKey string

const (
	// CtxAnthropicBeta carries the anthropic-beta header value through context.
	CtxAnthropicBeta CtxKey = "anthropic-beta"
	// CtxAnthropicVersion carries the anthropic-version header value through context.
	CtxAnthropicVersion CtxKey = "anthropic-version"
	// CtxIdempotencyKey carries the caller's idempotency key through the
	// router/adapter boundary.  A single stable key is reused for key rotation
	// and channel failover so task-creation POSTs cannot create duplicates.
	CtxIdempotencyKey CtxKey = "idempotency-key"
)

var GlobalTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          1000,
	MaxIdleConnsPerHost:   200,
	MaxConnsPerHost:       400,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	ResponseHeaderTimeout: 180 * time.Second, // 预留 3 分钟思考时间，完美适配 DeepSeek-R1 / o1 / Claude 3.7 深度思考
}

// DefaultClientTimeout is an upper bound for a single upstream request.  The
// transport's ResponseHeaderTimeout protects connection establishment, but it
// does not limit a stalled response body (for example a video download or an
// SSE stream).  Keep this generous enough for long-running model requests;
// callers can still impose a shorter deadline through context.
const DefaultClientTimeout = 10 * time.Minute

type UpstreamHTTPError struct {
	StatusCode  int
	ContentType string
	Body        string
	RetryAfter  string
}

func (e *UpstreamHTTPError) Error() string {
	if e == nil {
		return "upstream HTTP error"
	}
	return fmt.Sprintf("upstream returned HTTP %d", e.StatusCode)
}

// ErrStreamAborted 表示响应 Header 已下发给客户端后流传输中断，
// 此时 ResponseWriter 不可复用，Dispatcher 绝不能 Failover 重试
type ErrStreamAborted struct {
	Err error
}

func (e *ErrStreamAborted) Error() string {
	return fmt.Sprintf("stream aborted after headers sent: %v", e.Err)
}

func (e *ErrStreamAborted) Unwrap() error {
	return e.Err
}

var channelKeyCounters sync.Map // channelID (string) -> *atomic.Uint64 (无锁高并发安全)

// RemoveChannelCounter 清理已删除渠道的 Key 轮询计数器，防止内存泄漏
func RemoveChannelCounter(channelID string) {
	channelKeyCounters.Delete(channelID)
}

func getOrderedKeys(channel *config.UpstreamChannel) []string {
	keys := channel.GetEffectiveKeys()
	if len(keys) == 0 {
		return []string{""}
	}
	if len(keys) == 1 {
		return keys
	}

	val, ok := channelKeyCounters.Load(channel.ID)
	if !ok {
		val, _ = channelKeyCounters.LoadOrStore(channel.ID, &atomic.Uint64{})
	}
	counter := val.(*atomic.Uint64)

	startIdx := int(counter.Add(1)-1) % len(keys)
	ordered := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		ordered[i] = keys[(startIdx+i)%len(keys)]
	}
	return ordered
}

func maskKey(k string) string {
	k = strings.TrimSpace(k)
	if k == "" {
		return "<empty>"
	}
	if len(k) <= 6 {
		return "******"
	}
	return k[:3] + "..." + k[len(k)-2:]
}

type OpenAIAdapter struct {
	Client *http.Client
}

func init() {
	Register(AdapterMeta{
		Type:        "openai",
		Name:        "OpenAI / OpenAI-compatible",
		Description: "OpenAI 官方及采用 OpenAI 请求格式的官方服务",
		Protocols:   []string{"chat", "responses", "images", "audio", "video", "embeddings", "models", "moderations"},
		DefaultURL:  "https://api.openai.com/v1",
	}, NewOpenAIAdapter())
}

func NewOpenAIAdapter() *OpenAIAdapter {
	return &OpenAIAdapter{
		Client: &http.Client{
			Transport: GlobalTransport,
			Timeout:   DefaultClientTimeout,
			// API base URLs should be configured at their canonical endpoint.
			// Refusing redirects prevents credentials and non-idempotent request
			// bodies from being forwarded or replayed to another location.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (a *OpenAIAdapter) NormalizeURL(baseURL, endpoint string) string {
	base := strings.TrimRight(baseURL, "/")
	if !strings.HasSuffix(base, "/v1") && !strings.Contains(base, "/v1/") {
		base = base + "/v1"
	}
	return base + endpoint
}

func (a *OpenAIAdapter) SetHeadersWithKey(req *http.Request, channel *config.UpstreamChannel, key string) {
	// GET/HEAD requests have no JSON body.  Avoid advertising a body that does
	// not exist; some strict OpenAI-compatible gateways reject this combination.
	if req.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if req.Method == http.MethodPost && req.Header.Get("Idempotency-Key") == "" {
		if requestKey := contextIdempotencyKey(req.Context()); requestKey != "" {
			req.Header.Set("Idempotency-Key", requestKey)
		}
	}

	isAnthropicRequest := channel != nil && channel.Type == "anthropic"
	if req.URL != nil && (strings.HasSuffix(strings.TrimRight(req.URL.Path, "/"), "/messages") || strings.Contains(req.URL.Path, "/messages/")) {
		isAnthropicRequest = true
	}
	if key != "" {
		// Anthropic 官方直连端点严格仅接收 x-api-key，若附带 Authorization: Bearer 会报 400/401 鉴权格式错误
		if channel != nil && channel.Type == "anthropic" {
			req.Header.Set("x-api-key", key)
		} else {
			req.Header.Set("Authorization", "Bearer "+key)
			// Anthropic Messages 接口同时附带 x-api-key 保证向后兼容 (如 Sub2API 等)
			if isAnthropicRequest {
				req.Header.Set("x-api-key", key)
			}
		}
	}

	if isAnthropicRequest || (channel != nil && channel.AnthropicVersion != "") {
		v := "2023-06-01"
		if channel != nil && channel.AnthropicVersion != "" {
			v = channel.AnthropicVersion
		}
		if ver, ok := req.Context().Value(CtxAnthropicVersion).(string); ok && ver != "" {
			v = ver
		}
		req.Header.Set("anthropic-version", v)
	}

	if beta, ok := req.Context().Value(CtxAnthropicBeta).(string); ok && beta != "" {
		req.Header.Set("anthropic-beta", beta)
	}

	if channel != nil {
		for k, v := range channel.Headers {
			if isProtectedRequestHeader(k) {
				continue
			}
			req.Header.Set(k, v)
		}
	}
}

func isProtectedRequestHeader(name string) bool {
	switch http.CanonicalHeaderKey(strings.TrimSpace(name)) {
	case "Authorization", "X-Api-Key", "Proxy-Authorization", "Cookie", "Host", "Content-Length", "Transfer-Encoding", "Connection", "Idempotency-Key":
		return true
	default:
		return false
	}
}

func contextIdempotencyKey(ctx context.Context) string {
	if ctx != nil {
		if value, ok := ctx.Value(CtxIdempotencyKey).(string); ok {
			value = strings.TrimSpace(value)
			if value != "" && len(value) <= 255 && !strings.ContainsAny(value, "\r\n") {
				return value
			}
		}
		if requestID := audit.RequestID(ctx); requestID != "" {
			return requestID
		}
	}
	return ""
}

// Hop-by-hop headers RFC 7230, section 6.1
var hopByHopHeaders = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailers":            true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

var sensitiveResponseHeaders = map[string]bool{
	"Set-Cookie":                          true,
	"Set-Cookie2":                         true,
	"WWW-Authenticate":                    true,
	"Proxy-Authenticate":                  true,
	"Proxy-Authorization":                 true,
	"Content-Security-Policy":             true,
	"Content-Security-Policy-Report-Only": true,
}

// copyHeader 标准代理标头透传函数，自动剥离 hop-by-hop 标头与上游 CORS 标头（避免下游浏览器收到重复 CORS 标头导致阻断）
func copyHeader(dst http.ResponseWriter, src http.Header) {
	for k, vv := range src {
		canonicalKey := http.CanonicalHeaderKey(k)
		if hopByHopHeaders[canonicalKey] || sensitiveResponseHeaders[canonicalKey] || strings.HasPrefix(canonicalKey, "Access-Control-") {
			continue
		}
		for _, v := range vv {
			dst.Header().Add(k, v)
		}
	}
}

var streamBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

// ForwardStream 使用复用内存缓冲池将流式响应高效推向客户端，大幅减少 GC 开销
func ForwardStream(ctx context.Context, src io.Reader, dst http.ResponseWriter) error {
	flusher, ok := dst.(http.Flusher)
	bufPtr := streamBufPool.Get().(*[]byte)
	defer streamBufPool.Put(bufPtr)
	buf := *bufPtr

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, rErr := src.Read(buf)
		if n > 0 {
			if _, wErr := dst.Write(buf[:n]); wErr != nil {
				return wErr
			}
			if ok {
				flusher.Flush()
			}
		}
		if rErr != nil {
			if rErr == io.EOF {
				return nil
			}
			return rErr
		}
	}
}

// CopyWithPool 使用复用缓冲池进行流数据零堆内存拷贝，避免频繁触发 GC
func CopyWithPool(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := streamBufPool.Get().(*[]byte)
	defer streamBufPool.Put(bufPtr)
	return io.CopyBuffer(dst, src, *bufPtr)
}

// ForwardHTTPRequest 统一的高性能 HTTP 代理转发管线，整合多 Key 容灾轮询、标头剥离与缓冲池无拷贝流式传输
func (a *OpenAIAdapter) ForwardHTTPRequest(
	ctx context.Context,
	channel *config.UpstreamChannel,
	method string,
	targetURL string,
	body []byte,
	extraHeaders map[string]string,
	isStream bool,
	w http.ResponseWriter,
) error {
	return a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		var r io.Reader
		if len(body) > 0 {
			r = bytes.NewReader(body)
		}
		httpReq, err := http.NewRequestWithContext(ctx, method, targetURL, r)
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		for k, v := range extraHeaders {
			httpReq.Header.Set(k, v)
		}
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			bodyBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(bodyBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}

		copyHeader(w, resp.Header)
		// 流式传输必须剥离 Content-Length，防止因上游标头导致客户端提前关闭流
		if isStream {
			audit.AddEvent(ctx, "stream_started", audit.EventData{ChannelID: channel.ID, StatusCode: resp.StatusCode, Message: "streaming response started"})
			w.Header().Del("Content-Length")
			w.Header().Set("X-Accel-Buffering", "no")
			w.Header().Set("Cache-Control", "no-cache, no-transform")
		}
		w.WriteHeader(resp.StatusCode)

		if method == http.MethodHead {
			return nil
		}

		// Header 已下发给客户端，后续传输出错不可重试，必须包装为 ErrStreamAborted
		if isStream {
			if err := ForwardStream(ctx, resp.Body, w); err != nil {
				return &ErrStreamAborted{Err: err}
			}
			return nil
		}
		if _, err := CopyWithPool(w, resp.Body); err != nil {
			return &ErrStreamAborted{Err: err}
		}
		return nil
	})
}

// ForwardVideoContent proxies a video content endpoint while preserving the
// gateway URL as the client's durable source. Video providers commonly answer
// this endpoint with a newly minted, short-lived CDN URL. The shared client
// intentionally does not follow redirects because it must never forward an
// upstream credential to a redirected host; for video content we instead
// relay a safe HTTP(S) redirect to the browser.
func (a *OpenAIAdapter) ForwardVideoContent(
	ctx context.Context,
	channel *config.UpstreamChannel,
	targetURL string,
	req *http.Request,
	w http.ResponseWriter,
) error {
	var extraHeaders map[string]string
	if req != nil {
		if rangeHdr := req.Header.Get("Range"); rangeHdr != "" {
			extraHeaders = map[string]string{"Range": rangeHdr}
		}
	}
	method := http.MethodGet
	if req != nil && req.Method == http.MethodHead {
		method = http.MethodHead
	}

	err := a.forwardVideoContent(ctx, channel, method, targetURL, extraHeaders, w)
	// Some providers only implement GET. Fall back only when HEAD is explicitly
	// unsupported; transport failures, cancellations, auth errors, and upstream
	// 5xx responses must not trigger a second media request.
	if method == http.MethodHead && shouldFallbackVideoHEAD(ctx, err) {
		err = a.forwardVideoContent(ctx, channel, http.MethodGet, targetURL, extraHeaders, w)
	}
	return err
}

func shouldFallbackVideoHEAD(ctx context.Context, err error) bool {
	if err == nil || ctx == nil || ctx.Err() != nil {
		return false
	}
	var upstreamErr *UpstreamHTTPError
	return errors.As(err, &upstreamErr) &&
		(upstreamErr.StatusCode == http.StatusNotFound ||
			upstreamErr.StatusCode == http.StatusMethodNotAllowed ||
			upstreamErr.StatusCode == http.StatusNotImplemented)
}

func (a *OpenAIAdapter) forwardVideoContent(
	ctx context.Context,
	channel *config.UpstreamChannel,
	method string,
	targetURL string,
	extraHeaders map[string]string,
	w http.ResponseWriter,
) error {
	return a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		for k, v := range extraHeaders {
			httpReq.Header.Set(k, v)
		}
		return httpReq, nil
	}, func(resp *http.Response, _ string) error {
		defer resp.Body.Close()

		if isVideoContentRedirect(resp.StatusCode) {
			location, err := resolveVideoContentRedirect(resp)
			if err != nil {
				return err
			}
			// Do not let a browser cache the redirect itself. Each later play of
			// the durable gateway URL must ask the provider for a fresh signed URL.
			w.Header().Set("Location", location)
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(resp.StatusCode)
			return nil
		}

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
			if err != nil {
				return err
			}
			return &UpstreamHTTPError{
				StatusCode: resp.StatusCode,
				Body:       truncateBody(bodyBytes, 4096),
				RetryAfter: resp.Header.Get("Retry-After"),
			}
		}

		copyHeader(w, resp.Header)
		w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox allow-scripts allow-same-origin; media-src 'self' https: http: blob: data: *; img-src 'self' https: http: blob: data: *; style-src 'unsafe-inline'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
		w.Header().Set("Referrer-Policy", "no-referrer")

		rawType := strings.TrimSpace(w.Header().Get("Content-Type"))
		safeType, disposition := sanitizeVideoContentType(rawType, "video.mp4")
		w.Header().Set("Content-Type", safeType)
		if disposition != "" {
			w.Header().Set("Content-Disposition", disposition)
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(resp.StatusCode)
		if method == http.MethodHead {
			return nil
		}
		if _, err := CopyWithPool(w, resp.Body); err != nil {
			return &ErrStreamAborted{Err: err}
		}
		return nil
	})
}

var safeVideoContentTypes = map[string]struct{}{
	"video/mp4":       {},
	"video/webm":      {},
	"video/ogg":       {},
	"video/quicktime": {},
}

func sanitizeVideoContentType(rawType, fallbackFilename string) (string, string) {
	rawType = strings.TrimSpace(rawType)
	if rawType == "" || strings.EqualFold(rawType, "application/octet-stream") {
		return "video/mp4", ""
	}
	mediaType, _, err := mime.ParseMediaType(rawType)
	if err == nil {
		mediaType = strings.ToLower(mediaType)
		if _, ok := safeVideoContentTypes[mediaType]; ok {
			return mediaType, ""
		}
	}
	if fallbackFilename == "" {
		fallbackFilename = "video.bin"
	}
	return "application/octet-stream", fmt.Sprintf(`attachment; filename=%q`, fallbackFilename)
}

func isVideoContentRedirect(statusCode int) bool {
	switch statusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func resolveVideoContentRedirect(resp *http.Response) (string, error) {
	if resp == nil || resp.Request == nil || resp.Request.URL == nil {
		return "", errors.New("upstream video content redirect has no request URL")
	}
	location := strings.TrimSpace(resp.Header.Get("Location"))
	if location == "" {
		return "", errors.New("upstream video content redirect is missing Location")
	}
	redirectURL, err := resp.Request.URL.Parse(location)
	if err != nil {
		return "", fmt.Errorf("invalid upstream video content redirect: %w", err)
	}
	if !strings.EqualFold(redirectURL.Scheme, "http") && !strings.EqualFold(redirectURL.Scheme, "https") {
		return "", fmt.Errorf("unsupported upstream video content redirect scheme %q", redirectURL.Scheme)
	}
	if redirectURL.User != nil {
		return "", errors.New("upstream video content redirect must not contain userinfo")
	}
	host := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(redirectURL.Hostname())), ".")
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") || host == "metadata.google.internal" {
		return "", fmt.Errorf("upstream video content redirect host %q is not public", host)
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsLinkLocalMulticast() {
			return "", fmt.Errorf("upstream video content redirect host %q is not public", host)
		}
	}
	return redirectURL.String(), nil
}

func truncateBody(body []byte, max int) string {
	if max <= 0 || len(body) <= max {
		return string(body)
	}
	return string(body[:max]) + "..."
}

const maxUpstreamJSONBodyBytes int64 = 64 << 20
const maxUpstreamErrorBodyBytes int64 = 2 << 20

var ErrResponseTooLarge = errors.New("upstream response exceeds limit")

func readUpstreamErrorBody(body io.Reader) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	data, err := io.ReadAll(io.LimitReader(body, maxUpstreamErrorBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxUpstreamErrorBodyBytes {
		return nil, ErrResponseTooLarge
	}
	return data, nil
}

// readLimitedResponseBody bounds JSON/control-plane responses from upstreams.
// Binary and streaming endpoints are forwarded without buffering elsewhere;
// every adapter that needs to decode a complete JSON response uses this helper.
func readLimitedResponseBody(body io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, errors.New("response body limit must be positive")
	}
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: upstream response exceeds %d MiB limit", ErrResponseTooLarge, maxBytes>>20)
	}
	return data, nil
}

func readUpstreamJSONBody(body io.Reader) ([]byte, error) {
	return readLimitedResponseBody(body, maxUpstreamJSONBodyBytes)
}

func hasLegacyAsyncBusinessError(respBytes []byte, status string, errField any) bool {
	if errField != nil {
		return true
	}
	normStatus := strings.ToLower(strings.TrimSpace(status))
	if normStatus == "failed" || normStatus == "error" || normStatus == "rejected" {
		return true
	}
	var raw map[string]any
	if err := json.Unmarshal(respBytes, &raw); err == nil {
		if val, exists := raw["error"]; exists && val != nil {
			return true
		}
		if val, exists := raw["success"]; exists {
			if b, ok := val.(bool); ok && !b {
				return true
			}
		}
		if val, exists := raw["code"]; exists && val != nil {
			switch c := val.(type) {
			case string:
				cLower := strings.ToLower(strings.TrimSpace(c))
				if cLower != "" && cLower != "0" && cLower != "success" && cLower != "ok" {
					return true
				}
			case float64:
				if c != 0 && c != 200 {
					return true
				}
			case json.Number:
				numStr := c.String()
				if numStr != "" && numStr != "0" && numStr != "200" {
					return true
				}
			}
		}
	}
	return false
}

func extractImageJobTaskDetails(resp any) (string, string) {
	if m, ok := resp.(map[string]any); ok {
		for _, key := range []string{"id", "task_id"} {
			if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
				status, _ := m["status"].(string)
				return strings.TrimSpace(v), status
			}
		}
		if job, ok := m["job"].(map[string]any); ok {
			for _, key := range []string{"id", "task_id"} {
				if v, ok := job[key].(string); ok && strings.TrimSpace(v) != "" {
					status, _ := job["status"].(string)
					return strings.TrimSpace(v), status
				}
			}
		}
		if data, ok := m["data"].(map[string]any); ok {
			for _, key := range []string{"id", "task_id"} {
				if v, ok := data[key].(string); ok && strings.TrimSpace(v) != "" {
					status, _ := data["status"].(string)
					return strings.TrimSpace(v), status
				}
			}
		}
	}
	return "", ""
}

// RewriteJSONModel 高性能重写 JSON 请求体中的 model 字段 (含零内存分配 Fast-Path)
func RewriteJSONModel(rawBody []byte, targetModel string) []byte {
	if targetModel == "" || len(rawBody) == 0 {
		return rawBody
	}

	// Parse the top-level object instead of searching the raw text.  A prompt
	// or nested tool schema may contain a string such as `"model":"..."` and
	// must never prevent rewriting the actual request model.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rawBody, &raw); err != nil {
		return rawBody
	}

	if current, ok := raw["model"]; ok {
		var cur string
		if json.Unmarshal(current, &cur) == nil && cur == targetModel {
			return rawBody
		}
	}

	encoded, err := json.Marshal(targetModel)
	if err != nil {
		return rawBody
	}
	raw["model"] = encoded
	newBytes, err := json.Marshal(raw)
	if err != nil {
		return rawBody
	}
	return newBytes
}

func isIdempotentMethod(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodDelete:
		return true
	default:
		return false
	}
}

// ExecuteWithKeyRotation 统一的多 Key 并发轮询与 401/403/429/5xx 容灾执行器
func (a *OpenAIAdapter) ExecuteWithKeyRotation(
	ctx context.Context,
	channel *config.UpstreamChannel,
	reqFactory func(key string) (*http.Request, error),
	handleResp func(resp *http.Response, key string) error,
) error {
	orderedKeys := getOrderedKeys(channel)
	var lastErr error
	// Compute once per logical request.  The same value must be sent on every
	// retry, including retries that use another API key.
	stableIdempotencyKey := contextIdempotencyKey(ctx)
	if stableIdempotencyKey == "" {
		stableIdempotencyKey = fmt.Sprintf("rg-%d", time.Now().UnixNano())
	}

	for keyIdx, currentKey := range orderedKeys {
		if err := ctx.Err(); err != nil {
			return err
		}
		httpReq, err := reqFactory(currentKey)
		if err != nil {
			return err
		}
		if httpReq == nil || httpReq.URL == nil {
			return fmt.Errorf("request factory returned an incomplete request")
		}
		if httpReq != nil && httpReq.Method == http.MethodPost && httpReq.Header.Get("Idempotency-Key") == "" {
			httpReq.Header.Set("Idempotency-Key", stableIdempotencyKey)
		}
		audit.AddEvent(ctx, "upstream_attempt", audit.EventData{ChannelID: channel.ID, TargetURL: httpReq.URL.String(), Message: "upstream request sent"})

		resp, err := a.Client.Do(httpReq)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			lastErr = err
			audit.AddEvent(ctx, "upstream_failed", audit.EventData{ChannelID: channel.ID, TargetURL: httpReq.URL.String(), Message: err.Error()})
			// A request body may have been accepted before a network error was
			// observed.  Never replay non-idempotent methods with another key.
			if !isIdempotentMethod(httpReq.Method) {
				return err
			}
			continue
		}
		audit.AddEvent(ctx, "upstream_headers", audit.EventData{ChannelID: channel.ID, TargetURL: httpReq.URL.String(), StatusCode: resp.StatusCode, Message: "upstream response headers received"})
		if ctxErr := ctx.Err(); ctxErr != nil {
			resp.Body.Close()
			return ctxErr
		}

		// 上游 5xx 服务器宕机/网关错误：立即返回错误触发调度器渠道级故障转移，切勿在已宕机的单节点上空转轮换 Key
		if resp.StatusCode >= 500 {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}

		// 429 (限流) 或 401/403 (Token 失效 / 欠费余额耗尽) 自动切 Key 机制
		isAuthOrQuotaError := resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden
		isRateLimit := resp.StatusCode == http.StatusTooManyRequests

		if isRateLimit || isAuthOrQuotaError {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			resp.Body.Close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if readErr != nil {
				return readErr
			}
			reason := "429 Too Many Requests"
			if isAuthOrQuotaError {
				reason = fmt.Sprintf("HTTP %d authentication/permission error", resp.StatusCode)
			}
			lastErr = &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
			// 401/403/429 are explicit pre-execution rejection classes. POST
			// requests also carry a stable Idempotency-Key, so changing a key is
			// safe for these statuses but never for transport errors or 5xx.
			canRotate := isAuthOrQuotaError || isRateLimit || isIdempotentMethod(httpReq.Method)
			if canRotate && keyIdx < len(orderedKeys)-1 {
				log.Printf("[Key Rotation] Channel [%s] Key [%s] hit %s, switching to next key...", channel.ID, maskKey(currentKey), reason)
				continue
			}
			return lastErr
		}

		return handleResp(resp, currentKey)
	}
	return lastErr
}

func (a *OpenAIAdapter) ChatCompletions(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, isStream bool, w http.ResponseWriter) error {
	if mapped, ok := channel.ModelMap[targetModel]; ok && mapped != "" {
		targetModel = mapped
	}
	bodyToSend := RewriteJSONModel(rawBody, targetModel)
	targetURL := a.NormalizeURL(channel.BaseURL, "/chat/completions")
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, bodyToSend, nil, isStream, w)
}

func (a *OpenAIAdapter) AnthropicMessages(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, isStream bool, w http.ResponseWriter) error {
	targetURL := a.NormalizeURL(channel.BaseURL, "/messages")
	bodyToSend := rawBody
	if len(channel.ModelMap) > 0 {
		var meta struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(rawBody, &meta); err == nil && meta.Model != "" {
			if mapped, ok := channel.ModelMap[meta.Model]; ok && mapped != "" {
				bodyToSend = RewriteJSONModel(rawBody, mapped)
			}
		}
	}
	var extraHeaders map[string]string
	if beta, ok := ctx.Value(CtxAnthropicBeta).(string); ok && beta != "" {
		extraHeaders = map[string]string{"anthropic-beta": beta}
	}
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, bodyToSend, extraHeaders, isStream, w)
}

func (a *OpenAIAdapter) Embeddings(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, w http.ResponseWriter) error {
	if mapped, ok := channel.ModelMap[targetModel]; ok && mapped != "" {
		targetModel = mapped
	}
	bodyToSend := RewriteJSONModel(rawBody, targetModel)
	targetURL := a.NormalizeURL(channel.BaseURL, "/embeddings")
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, bodyToSend, nil, false, w)
}

func (a *OpenAIAdapter) ImagesGenerations(ctx context.Context, channel *config.UpstreamChannel, req *model.ImageGenerationRequest) (*model.ImageResponse, error) {
	reqCopy := *req
	if target, ok := channel.ModelMap[reqCopy.Model]; ok && target != "" {
		reqCopy.Model = target
	}

	payload := NormalizeImageRequest(&reqCopy, channel.Type)

	// 针对同步 ImagesGenerations 接口，显式剔除 stream 与 partial_images，避免部分上游强制返回 SSE 事件流
	delete(payload, "stream")
	delete(payload, "partial_images")

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	targetURL := a.NormalizeURL(channel.BaseURL, "/images/generations")
	var imageResp model.ImageResponse

	err = a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		respBytes, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(respBytes, &imageResp); err != nil {
			// 若上游返回了 SSE 格式数据（event: / data:），尝试从中提取图片信息
			if sseResp, sseErr := parseSSEImageResponse(respBytes); sseErr == nil && len(sseResp.Data) > 0 {
				imageResp = *sseResp
				return nil
			}
			return fmt.Errorf("unmarshal image response failed: %w (body: %s)", err, truncateBody(respBytes, 200))
		}
		if len(imageResp.Data) == 0 {
			var raw map[string]any
			if err := json.Unmarshal(respBytes, &raw); err == nil {
				if raw["error"] != nil {
					return &UpstreamHTTPError{
						StatusCode:  resp.StatusCode,
						ContentType: resp.Header.Get("Content-Type"),
						Body:        string(respBytes),
						RetryAfter:  resp.Header.Get("Retry-After"),
					}
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &imageResp, nil
}

// parseSSEImageResponse 解析包含 event: / data: 的流式图片生成响应
func parseSSEImageResponse(body []byte) (*model.ImageResponse, error) {
	imgResp := &model.ImageResponse{
		Created: time.Now().Unix(),
	}
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "data:") {
			dataStr := strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
			if dataStr == "" || dataStr == "[DONE]" {
				continue
			}

			// 1. 尝试解析为标准 ImageResponse
			var fullResp model.ImageResponse
			if err := json.Unmarshal([]byte(dataStr), &fullResp); err == nil && len(fullResp.Data) > 0 {
				imgResp.Data = append(imgResp.Data, fullResp.Data...)
				continue
			}

			// 2. 尝试从事件流单帧中提取
			var frame struct {
				URL     string `json:"url"`
				B64JSON string `json:"b64_json"`
			}
			if err := json.Unmarshal([]byte(dataStr), &frame); err == nil && (frame.URL != "" || frame.B64JSON != "") {
				imgResp.Data = append(imgResp.Data, model.ImageData{
					URL:     frame.URL,
					B64JSON: frame.B64JSON,
				})
			}
		}
	}
	if len(imgResp.Data) == 0 {
		return nil, errors.New("no valid image found in SSE stream")
	}
	return imgResp, nil
}

func (a *OpenAIAdapter) ImagesEdits(ctx context.Context, channel *config.UpstreamChannel, body []byte, contentType string, w http.ResponseWriter) error {
	targetURL := a.NormalizeURL(channel.BaseURL, "/images/edits")
	extraHeaders := make(map[string]string)
	if contentType != "" {
		extraHeaders["Content-Type"] = contentType
	}
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, body, extraHeaders, false, w)
}

func (a *OpenAIAdapter) AudioSpeech(ctx context.Context, channel *config.UpstreamChannel, req *model.AudioSpeechRequest, w http.ResponseWriter) error {
	reqCopy := *req
	if target, ok := channel.ModelMap[reqCopy.Model]; ok && target != "" {
		reqCopy.Model = target
	}

	bodyBytes, err := json.Marshal(&reqCopy)
	if err != nil {
		return err
	}

	targetURL := a.NormalizeURL(channel.BaseURL, "/audio/speech")
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, bodyBytes, nil, false, w)
}

// CreateVideo 提交 OpenAI 官方规范的视频生成任务 POST /v1/videos
func (a *OpenAIAdapter) CreateVideo(ctx context.Context, channel *config.UpstreamChannel, req *model.VideoGenerationRequest) (*model.VideoTaskResponse, error) {
	reqCopy := *req
	if target, ok := channel.ModelMap[reqCopy.Model]; ok && target != "" {
		reqCopy.Model = target
	}

	bodyBytes, err := json.Marshal(&reqCopy)
	if err != nil {
		return nil, err
	}

	targetURL := a.NormalizeURL(channel.BaseURL, "/videos")
	var taskResp model.VideoTaskResponse

	err = a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		respBytes, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(respBytes, &taskResp); err != nil {
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(respBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		if taskResp.ID == "" && taskResp.TaskID == "" {
			if hasLegacyAsyncBusinessError(respBytes, taskResp.Status, taskResp.Error) {
				return &UpstreamHTTPError{
					StatusCode:  resp.StatusCode,
					ContentType: resp.Header.Get("Content-Type"),
					Body:        string(respBytes),
					RetryAfter:  resp.Header.Get("Retry-After"),
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &taskResp, nil
}

func (a *OpenAIAdapter) GetVideo(ctx context.Context, channel *config.UpstreamChannel, taskID string) (*model.VideoTaskResponse, error) {
	targetURL := a.NormalizeURL(channel.BaseURL, "/videos/"+taskID)
	var taskResp model.VideoTaskResponse

	err := a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		respBytes, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(respBytes, &taskResp); err != nil {
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(respBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if taskResp.ID == "" {
		taskResp.ID = taskID
	}
	taskResp.Status = model.NormalizeVideoStatus(taskResp.Status)
	if taskResp.Status == model.VideoStatusCompleted && taskResp.VideoURL == "" {
		taskResp.VideoURL = "/v1/videos/" + taskID + "/content"
	}
	if taskResp.URL == "" {
		taskResp.URL = taskResp.VideoURL
	}
	return &taskResp, nil
}

func (a *OpenAIAdapter) FetchModels(ctx context.Context, channel *config.UpstreamChannel) ([]string, error) {
	targetURL := a.NormalizeURL(channel.BaseURL, "/models")
	var raw struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}

	err := a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		respBytes, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		return json.Unmarshal(respBytes, &raw)
	})

	if err != nil {
		return nil, err
	}

	models := make([]string, 0, len(raw.Data))
	for _, m := range raw.Data {
		if m.ID != "" {
			models = append(models, m.ID)
		}
	}
	return models, nil
}

func (a *OpenAIAdapter) GetVideoContent(ctx context.Context, channel *config.UpstreamChannel, taskID string, w http.ResponseWriter, req *http.Request) error {
	targetURL := a.NormalizeURL(channel.BaseURL, "/videos/"+taskID+"/content")
	return a.ForwardVideoContent(ctx, channel, targetURL, req, w)
}

func (a *OpenAIAdapter) CreateImageJob(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte) (interface{}, error) {
	bodyToSend := rawBody
	var req model.ImageGenerationRequest
	if err := json.Unmarshal(rawBody, &req); err == nil {
		if target, ok := channel.ModelMap[req.Model]; ok && target != "" {
			req.Model = target
		}
		normPayload := NormalizeImageRequest(&req, channel.Type)
		if b, err := json.Marshal(normPayload); err == nil {
			bodyToSend = b
		}
	}

	targetURL := a.NormalizeURL(channel.BaseURL, "/images/jobs")
	var parsed interface{}

	err := a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyToSend))
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		respBytes, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(respBytes, &parsed); err != nil {
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(respBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		taskID, status := extractImageJobTaskDetails(parsed)
		if taskID == "" && hasLegacyAsyncBusinessError(respBytes, status, nil) {
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(respBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		return nil
	})

	return parsed, err
}

func (a *OpenAIAdapter) GetImageJob(ctx context.Context, channel *config.UpstreamChannel, taskID string) (interface{}, error) {
	targetURL := a.NormalizeURL(channel.BaseURL, "/images/jobs/"+taskID)
	var parsed interface{}

	err := a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		respBytes, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(respBytes, &parsed); err != nil {
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(respBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		return nil
	})

	return parsed, err
}

func (a *OpenAIAdapter) Moderations(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, w http.ResponseWriter) error {
	if mapped, ok := channel.ModelMap[targetModel]; ok && mapped != "" {
		targetModel = mapped
	}
	bodyToSend := RewriteJSONModel(rawBody, targetModel)
	targetURL := a.NormalizeURL(channel.BaseURL, "/moderations")
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, bodyToSend, nil, false, w)
}

func (a *OpenAIAdapter) AudioTranscriptions(ctx context.Context, channel *config.UpstreamChannel, endpoint string, body []byte, contentType string, w http.ResponseWriter) error {
	targetURL := a.NormalizeURL(channel.BaseURL, endpoint)
	extraHeaders := make(map[string]string)
	if contentType != "" {
		extraHeaders["Content-Type"] = contentType
	}
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, body, extraHeaders, false, w)
}

func (a *OpenAIAdapter) CountTokens(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, w http.ResponseWriter) error {
	targetURL := a.NormalizeURL(channel.BaseURL, "/messages/count_tokens")
	bodyToSend := rawBody
	if len(channel.ModelMap) > 0 {
		var meta struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(rawBody, &meta); err == nil && meta.Model != "" {
			if mapped, ok := channel.ModelMap[meta.Model]; ok && mapped != "" {
				bodyToSend = RewriteJSONModel(rawBody, mapped)
			}
		}
	}
	var extraHeaders map[string]string
	if beta, ok := ctx.Value(CtxAnthropicBeta).(string); ok && beta != "" {
		extraHeaders = map[string]string{"anthropic-beta": beta}
	}
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, bodyToSend, extraHeaders, false, w)
}
