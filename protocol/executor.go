package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	ErrOperationNotFound = errors.New("profile operation not found")
	ErrResponseTooLarge  = errors.New("upstream response exceeds limit")
	ErrInvalidBaseURL    = errors.New("invalid upstream base URL")
)

// Request is the provider-independent input to an operation. Body is usually
// a map decoded from the public request; profile submit defaults are merged
// into it without mutating either caller-owned value.
type Request struct {
	BaseURL string
	APIKeys []string
	// APIKeyHeader selects the header used for provider credentials. Empty
	// preserves the OpenAI-compatible Authorization: Bearer default.
	APIKeyHeader string
	Headers      map[string]string
	Body         any
	// RawBody/RawContentType allow protocol-aware callers to preserve a
	// non-JSON payload (for example multipart image edits) while still using
	// the same Profile path, credential rotation, timeout and response rules.
	// When RawBody is non-nil ExecuteRaw sends it verbatim and does not apply
	// JSON body merging or encoding.
	RawBody          []byte
	RawContentType   string
	TaskID           string
	PathVars         map[string]string
	IdempotencyKey   string
	MaxResponseBytes int64
}

// ExecuteRaw submits a direct Profile operation and writes the provider
// response to w without interpreting its JSON envelope. This is used for
// chat/messages/responses where the public contract may be JSON or SSE.
// Exactly one submit is attempted per credential; a 401/403 may rotate to the
// next configured key before any response bytes are written.
func (e *HTTPExecutor) ExecuteRaw(ctx context.Context, profile CompiledProfile, operation string, req Request, w http.ResponseWriter, stream bool) (Result, error) {
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()
	if w == nil {
		return Result{}, errors.New("raw response writer is required")
	}
	op, err := operationFrom(profile, operation)
	if err != nil {
		return Result{}, err
	}
	if op.ExecutionMode != ExecutionDirect {
		return Result{}, errors.New("raw execution requires a direct operation")
	}
	var encoded []byte
	var contentType string
	if req.RawBody != nil {
		encoded = append([]byte(nil), req.RawBody...)
		contentType = strings.TrimSpace(req.RawContentType)
	} else {
		body, bodyErr := mergeOperationBody(op.Submit.Body, req.Body)
		if bodyErr != nil {
			return Result{}, bodyErr
		}
		body, bodyErr = applyModelPolicy(op.Operation, op.Policy, body)
		if bodyErr != nil {
			return Result{}, bodyErr
		}
		encoded, contentType, err = encodeBody(body, op.Submit.BodyEncoding)
		if err != nil {
			return Result{}, err
		}
	}
	path, err := expandPath(op.Submit.Path, requestPathVars(req))
	if err != nil {
		return Result{}, err
	}
	keys := normalizedKeys(req.APIKeys)
	if len(keys) == 0 {
		keys = []string{""}
	}
	var lastAuthErr error
	for i, key := range keys {
		httpReq, reqErr := e.newRequest(ctx, op.Submit.Method, path, req, encoded, contentType)
		if reqErr != nil {
			return Result{}, reqErr
		}
		applyHeaders(httpReq, op.Submit.Headers)
		applyAPIKeyHeader(httpReq, key, req.APIKeyHeader)
		resp, doErr := e.client().Do(httpReq)
		if doErr != nil {
			return Result{}, executorError("submit", 0, mutatingMethod(op.Submit.Method), false, doErr)
		}
		if (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) && i+1 < len(keys) {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, e.maxResponseBytes(req)+1))
			_ = resp.Body.Close()
			lastAuthErr = executorErrorWithResponse("submit", resp.StatusCode, resp.Header.Get("Content-Type"), string(bodyBytes), resp.Header.Get("Retry-After"), false, true, fmt.Errorf("upstream returned status %d", resp.StatusCode))
			continue
		}
		result := Result{HTTPStatus: resp.StatusCode, Headers: resp.Header.Clone()}
		if resp.StatusCode >= 400 {
			result.RawBody, _ = io.ReadAll(io.LimitReader(resp.Body, e.maxResponseBytes(req)+1))
			_ = resp.Body.Close()
			if int64(len(result.RawBody)) > e.maxResponseBytes(req) {
				return result, ErrResponseTooLarge
			}
			return result, executorErrorWithResponse("submit", resp.StatusCode, resp.Header.Get("Content-Type"), string(result.RawBody), resp.Header.Get("Retry-After"), mutatingMethod(op.Submit.Method) && resp.StatusCode >= 500, false, fmt.Errorf("upstream returned status %d", resp.StatusCode))
		}
		if stream {
			copyResponseHeaders(w, resp.Header)
			w.WriteHeader(resp.StatusCode)
			written, copyErr := io.Copy(w, io.LimitReader(resp.Body, e.maxResponseBytes(req)+1))
			_ = resp.Body.Close()
			result.RawBody = nil
			if copyErr != nil {
				return result, executorError("submit", resp.StatusCode, false, true, copyErr)
			}
			if written > e.maxResponseBytes(req) {
				return result, ErrResponseTooLarge
			}
			return result, nil
		}
		result.RawBody, err = io.ReadAll(io.LimitReader(resp.Body, e.maxResponseBytes(req)+1))
		_ = resp.Body.Close()
		if err != nil {
			return result, err
		}
		if int64(len(result.RawBody)) > e.maxResponseBytes(req) {
			return result, ErrResponseTooLarge
		}
		copyResponseHeaders(w, resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, err := w.Write(result.RawBody); err != nil {
			return result, executorError("submit", resp.StatusCode, false, true, err)
		}
		return result, nil
	}
	if lastAuthErr != nil {
		return Result{}, lastAuthErr
	}
	return Result{}, executorError("submit", http.StatusUnauthorized, true, false, errors.New("all upstream credentials were rejected"))
}

type Result struct {
	HTTPStatus int
	Headers    http.Header
	RawBody    []byte
	JSON       any
	TaskID     string
	Status     string
	Progress   string
	ResultURLs []string
}

type ContentResult struct {
	HTTPStatus int
	Headers    http.Header
	Body       io.ReadCloser
}

type OperationExecutor interface {
	Submit(context.Context, CompiledProfile, string, Request) (Result, error)
}

type StatusQuerier interface {
	PollOnce(context.Context, CompiledProfile, string, Request) (Result, error)
}

type ContentFetcher interface {
	FetchContent(context.Context, CompiledProfile, string, Request) (ContentResult, error)
}

// HTTPExecutor is the generic JSON/HTTP implementation. It intentionally has
// no provider-name branches; provider-specific semantics belong in profiles or
// a narrow policy layer above it.
type HTTPExecutor struct {
	Client           *http.Client
	MaxResponseBytes int64
	RequestTimeout   time.Duration
}

func NewHTTPExecutor(client *http.Client) *HTTPExecutor {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &HTTPExecutor{Client: client, MaxResponseBytes: 8 << 20}
}

func (e *HTTPExecutor) Submit(ctx context.Context, profile CompiledProfile, operation string, req Request) (Result, error) {
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()
	op, err := operationFrom(profile, operation)
	if err != nil {
		return Result{}, err
	}
	body, err := mergeOperationBody(op.Submit.Body, req.Body)
	if err != nil {
		return Result{}, err
	}
	body, err = applyModelPolicy(op.Operation, op.Policy, body)
	if err != nil {
		return Result{}, err
	}
	result, err := e.doJSON(ctx, op.Submit.Method, op.Submit.Path, req, body, op.Submit.BodyEncoding, op.Submit.Headers, true, requestPathVars(req))
	if err != nil {
		return result, err
	}
	result.TaskID = firstString(result.JSON, op.Response.TaskIDPaths)
	result.ResultURLs = allStrings(result.JSON, op.Response.ResultPaths)
	if op.ExecutionMode == ExecutionAsync && strings.TrimSpace(result.TaskID) == "" {
		if hasAsyncBusinessError(result.JSON, result.RawBody) {
			contentType := ""
			if result.Headers != nil {
				contentType = result.Headers.Get("Content-Type")
			}
			retryAfter := ""
			if result.Headers != nil {
				retryAfter = result.Headers.Get("Retry-After")
			}
			return result, &ExecutorError{
				Phase:            "submit",
				RetryClass:       "none",
				HTTPStatus:       result.HTTPStatus,
				ContentType:      contentType,
				MayHaveSubmitted: true,
				AllowFailover:    false,
				Message:          "upstream async task submission rejected",
				Cause:            errors.New("upstream rejected async task submission without task id"),
				Body:             string(result.RawBody),
				RetryAfter:       retryAfter,
			}
		}
	}
	return result, nil
}

func hasAsyncBusinessError(jsonVal any, rawBody []byte) bool {
	if jsonMap, ok := jsonVal.(map[string]any); ok {
		if val, exists := jsonMap["error"]; exists && val != nil {
			return true
		}
		if val, exists := jsonMap["success"]; exists {
			if b, ok := val.(bool); ok && !b {
				return true
			}
		}
		if val, exists := jsonMap["code"]; exists && val != nil {
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
		if statusVal, exists := jsonMap["status"]; exists {
			if s, ok := statusVal.(string); ok {
				sLower := strings.ToLower(strings.TrimSpace(s))
				if sLower == "failed" || sLower == "error" || sLower == "rejected" {
					return true
				}
			}
		}
	}
	return false
}

func (e *HTTPExecutor) PollOnce(ctx context.Context, profile CompiledProfile, operation string, req Request) (Result, error) {
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()
	op, err := operationFrom(profile, operation)
	if err != nil {
		return Result{}, err
	}
	if op.Poll == nil {
		return Result{}, errors.New("operation has no poll definition")
	}
	vars := requestPathVars(req)
	result, err := e.doJSON(ctx, op.Poll.Method, op.Poll.Path, req, nil, "", op.Poll.Headers, false, vars)
	if err != nil {
		return result, err
	}
	statusSelectors := []string{op.Poll.StatusPath}
	if op.Poll.StatusPath != "" && !strings.Contains(op.Poll.StatusPath, ".") {
		statusSelectors = append(statusSelectors, "job."+op.Poll.StatusPath, "data."+op.Poll.StatusPath)
	}
	result.Status = firstString(result.JSON, statusSelectors)
	if op.Poll.ProgressPath != "" {
		result.Progress = valueString(selectJSON(result.JSON, op.Poll.ProgressPath))
	}
	result.ResultURLs = allStrings(result.JSON, op.Poll.ResultURLPaths)
	return result, nil
}

func (e *HTTPExecutor) FetchContent(ctx context.Context, profile CompiledProfile, operation string, req Request) (ContentResult, error) {
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()
	op, err := operationFrom(profile, operation)
	if err != nil {
		return ContentResult{}, err
	}
	if op.Content == nil {
		return ContentResult{}, errors.New("operation has no content definition")
	}
	path, err := expandPath(op.Content.Path, requestPathVars(req))
	if err != nil {
		return ContentResult{}, err
	}
	httpReq, err := e.newRequest(ctx, op.Content.Method, path, req, nil, "")
	if err != nil {
		return ContentResult{}, err
	}
	applyHeaders(httpReq, op.Content.Headers)
	keys := normalizedKeys(req.APIKeys)
	if len(keys) > 0 {
		applyAPIKeyHeader(httpReq, keys[0], req.APIKeyHeader)
	}
	resp, err := e.client().Do(httpReq)
	if err != nil {
		return ContentResult{}, executorError("content", 0, false, false, err)
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, e.maxResponseBytes(req)+1))
		return ContentResult{HTTPStatus: resp.StatusCode, Headers: resp.Header}, executorErrorWithResponse("content", resp.StatusCode, resp.Header.Get("Content-Type"), string(body), resp.Header.Get("Retry-After"), false, resp.StatusCode >= 500, fmt.Errorf("upstream returned status %d", resp.StatusCode))
	}
	return ContentResult{HTTPStatus: resp.StatusCode, Headers: resp.Header, Body: resp.Body}, nil
}

func (e *HTTPExecutor) doJSON(ctx context.Context, method, path string, req Request, body any, encoding string, operationHeaders map[string]string, allowKeyRotation bool, vars map[string]string) (Result, error) {
	if vars != nil {
		var err error
		path, err = expandPath(path, vars)
		if err != nil {
			return Result{}, err
		}
	}
	encoded, contentType, err := encodeBody(body, encoding)
	if err != nil {
		return Result{}, err
	}
	keys := normalizedKeys(req.APIKeys)
	if len(keys) == 0 {
		keys = []string{""}
	}
	var lastExecutorErr error
	for i, key := range keys {
		httpReq, reqErr := e.newRequest(ctx, method, path, req, encoded, contentType)
		if reqErr != nil {
			return Result{}, reqErr
		}
		applyHeaders(httpReq, operationHeaders)
		applyAPIKeyHeader(httpReq, key, req.APIKeyHeader)
		resp, doErr := e.client().Do(httpReq)
		if doErr != nil {
			phase := "poll"
			if allowKeyRotation {
				phase = "submit"
			}
			mayHaveSubmitted := mutatingMethod(method)
			lastExecutorErr = executorError(phase, 0, mayHaveSubmitted, false, doErr)
			if allowKeyRotation && !mayHaveSubmitted && i+1 < len(keys) {
				continue
			}
			return Result{}, lastExecutorErr
		}
		result, readErr := readJSONResponse(resp, e.maxResponseBytes(req))
		if readErr != nil {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			return result, readErr
		}
		if (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) && allowKeyRotation && i+1 < len(keys) {
			lastExecutorErr = executorErrorWithResponse("submit", resp.StatusCode, resp.Header.Get("Content-Type"), string(result.RawBody), resp.Header.Get("Retry-After"), false, true, fmt.Errorf("upstream returned status %d", resp.StatusCode))
			continue
		}
		if resp.StatusCode >= 400 {
			phase := "poll"
			if allowKeyRotation {
				phase = "submit"
			}
			return result, executorErrorWithResponse(phase, resp.StatusCode, resp.Header.Get("Content-Type"), string(result.RawBody), resp.Header.Get("Retry-After"), mutatingMethod(method) && resp.StatusCode >= 500, false, fmt.Errorf("upstream returned status %d", resp.StatusCode))
		}
		return result, nil
	}
	if lastExecutorErr != nil {
		return Result{}, lastExecutorErr
	}
	return Result{}, executorError("submit", http.StatusUnauthorized, false, false, errors.New("all upstream credentials were rejected"))
}

func mutatingMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func (e *HTTPExecutor) newRequest(ctx context.Context, method, path string, req Request, body []byte, contentType string) (*http.Request, error) {
	base, err := parseBaseURL(req.BaseURL)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(path)
	if err != nil || u.IsAbs() || u.Host != "" {
		return nil, ErrInvalidBaseURL
	}
	// Profile paths are rooted at the channel base path rather than the host
	// root. This preserves prefixes such as /v1 when a channel is configured
	// with https://provider.example/v1 and the profile operation uses /videos.
	basePath := strings.TrimRight(base.Path, "/")
	u.Path = basePath + "/" + strings.TrimLeft(u.Path, "/")
	u.RawPath = ""
	u.Scheme = base.Scheme
	u.Host = base.Host
	u.User = base.User
	if ctx == nil {
		ctx = context.Background()
	}
	httpReq, err := http.NewRequestWithContext(ctx, strings.ToUpper(strings.TrimSpace(method)), u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	applyHeaders(httpReq, req.Headers)
	if len(body) > 0 && contentType != "" {
		httpReq.Header.Set("Content-Type", contentType)
	}
	if req.IdempotencyKey != "" {
		httpReq.Header.Set("Idempotency-Key", req.IdempotencyKey)
	}
	return httpReq, nil
}

func (e *HTTPExecutor) client() *http.Client {
	if e != nil && e.Client != nil {
		return e.Client
	}
	return http.DefaultClient
}

func (e *HTTPExecutor) maxResponseBytes(req Request) int64 {
	if req.MaxResponseBytes > 0 {
		return req.MaxResponseBytes
	}
	if e != nil && e.MaxResponseBytes > 0 {
		return e.MaxResponseBytes
	}
	return 8 << 20
}

func operationFrom(profile CompiledProfile, name string) (Operation, error) {
	for _, op := range profile.Profile().Operations {
		if op.Operation == name {
			return op, nil
		}
	}
	return Operation{}, fmt.Errorf("%w: %s", ErrOperationNotFound, name)
}

func parseBaseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || strings.Contains(u.Path, "..") {
		return nil, ErrInvalidBaseURL
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/"
	return u, nil
}

func expandPath(path string, vars map[string]string) (string, error) {
	path = strings.TrimSpace(path)
	for start := strings.IndexByte(path, '{'); start >= 0; {
		end := strings.IndexByte(path[start:], '}')
		if end < 0 {
			return "", fmt.Errorf("unclosed path variable")
		}
		end += start
		name := path[start+1 : end]
		value, ok := vars[name]
		if !ok || strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("missing path variable %q", name)
		}
		path = path[:start] + url.PathEscape(value) + path[end+1:]
		start = strings.IndexByte(path, '{')
	}
	return path, nil
}

func encodeBody(body any, encoding string) ([]byte, string, error) {
	if body == nil {
		return nil, "", nil
	}
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "json":
		b, err := json.Marshal(body)
		if err != nil {
			return nil, "", fmt.Errorf("encode request body: %w", err)
		}
		return b, "application/json", nil
	case "raw":
		switch value := body.(type) {
		case []byte:
			return append([]byte(nil), value...), "application/octet-stream", nil
		case string:
			return []byte(value), "application/octet-stream", nil
		default:
			return nil, "", errors.New("raw body must be a string or byte slice")
		}
	case "form":
		values := url.Values{}
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, "", fmt.Errorf("encode form body: %w", err)
		}
		var fields map[string]any
		if err := json.Unmarshal(encoded, &fields); err != nil {
			return nil, "", errors.New("form body must be an object")
		}
		for key, value := range fields {
			values.Set(key, valueString(value))
		}
		return []byte(values.Encode()), "application/x-www-form-urlencoded", nil
	default:
		return nil, "", fmt.Errorf("unsupported body encoding %q", encoding)
	}
}

func requestPathVars(req Request) map[string]string {
	vars := make(map[string]string, len(req.PathVars)+1)
	for key, value := range req.PathVars {
		vars[key] = value
	}
	if req.TaskID != "" {
		vars["task_id"] = req.TaskID
	}
	return vars
}

func mergeOperationBody(defaults map[string]any, body any) (any, error) {
	if len(defaults) == 0 {
		return body, nil
	}
	merged := cloneExecutorMap(defaults)
	if body == nil {
		return merged, nil
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode request body: %w", err)
	}
	var incoming map[string]any
	if err := json.Unmarshal(encoded, &incoming); err != nil {
		return body, nil
	}
	for key, value := range incoming {
		merged[key] = value
	}
	return merged, nil
}

func cloneExecutorMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func applyHeaders(req *http.Request, headers map[string]string) {
	for name, value := range headers {
		req.Header.Set(name, value)
	}
}

func applyAPIKeyHeader(req *http.Request, key, header string) {
	if req == nil || strings.TrimSpace(key) == "" {
		return
	}
	header = strings.TrimSpace(header)
	if header == "" {
		header = "Authorization"
	}
	if strings.EqualFold(header, "authorization") {
		req.Header.Set("Authorization", "Bearer "+key)
		return
	}
	req.Header.Set(header, key)
}

func copyResponseHeaders(dst http.ResponseWriter, src http.Header) {
	if dst == nil {
		return
	}
	for name, values := range src {
		for _, value := range values {
			dst.Header().Add(name, value)
		}
	}
}

func normalizedKeys(keys []string) []string {
	out := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func readJSONResponse(resp *http.Response, maxBytes int64) (Result, error) {
	defer resp.Body.Close()
	if maxBytes <= 0 {
		maxBytes = 8 << 20
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	result := Result{HTTPStatus: resp.StatusCode, Headers: resp.Header.Clone(), RawBody: body}
	if err != nil {
		return result, executorError("response", resp.StatusCode, false, false, err)
	}
	if int64(len(body)) > maxBytes {
		return result, fmt.Errorf("%w: %d bytes", ErrResponseTooLarge, len(body))
	}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &result.JSON); err != nil && resp.StatusCode < 400 {
			contentType := resp.Header.Get("Content-Type")
			retryAfter := resp.Header.Get("Retry-After")
			return result, executorErrorWithResponse("response", resp.StatusCode, contentType, string(body), retryAfter, false, false, fmt.Errorf("decode JSON response: %w", err))
		}
	}
	return result, nil
}

func (e *HTTPExecutor) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if e != nil && e.RequestTimeout > 0 {
		return context.WithTimeout(ctx, e.RequestTimeout)
	}
	return ctx, func() {}
}

func selectJSON(value any, selector string) any {
	if value == nil || selector == "" {
		return nil
	}
	current := value
	for _, part := range strings.Split(selector, ".") {
		switch typed := current.(type) {
		case map[string]any:
			current = typed[part]
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil
			}
			current = typed[index]
		default:
			return nil
		}
	}
	return current
}

func firstString(value any, selectors []string) string {
	for _, selector := range selectors {
		if candidate := valueString(selectJSON(value, selector)); candidate != "" {
			return candidate
		}
	}
	return ""
}

func allStrings(value any, selectors []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		candidate := selectJSON(value, selector)
		for _, text := range resultStrings(candidate) {
			if _, exists := seen[text]; !exists {
				seen[text] = struct{}{}
				out = append(out, text)
			}
		}
	}
	return out
}

// resultStrings extracts URL-like strings from response selectors. Profiles
// commonly point at an array-valued field such as images.data, whose elements
// are objects rather than strings; handling the conventional URL keys here
// keeps result extraction provider-neutral and avoids provider branches.
func resultStrings(value any) []string {
	if text := valueString(value); text != "" {
		return []string{text}
	}
	switch typed := value.(type) {
	case []any:
		var out []string
		for _, item := range typed {
			out = append(out, resultStrings(item)...)
		}
		return out
	case map[string]any:
		var out []string
		for _, key := range []string{"url", "image_url", "video_url", "audio_url", "proxy_url", "b64_json"} {
			if candidate, ok := typed[key]; ok {
				out = append(out, resultStrings(candidate)...)
			}
		}
		if len(out) == 0 {
			out = append(out, resultStrings(typed["thumbnail_url"])...)
		}
		return out
	default:
		return nil
	}
}

func valueString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

type ExecutorError struct {
	Phase            string
	RetryClass       string
	HTTPStatus       int
	ContentType      string
	MayHaveSubmitted bool
	AllowFailover    bool
	Message          string
	Cause            error
	Body             string
	RetryAfter       string
}

func (e *ExecutorError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *ExecutorError) Unwrap() error { return e.Cause }

func executorError(phase string, status int, mayHaveSubmitted, allowFailover bool, cause error) error {
	return executorErrorWithResponse(phase, status, "", "", "", mayHaveSubmitted, allowFailover, cause)
}

func executorErrorWithResponse(phase string, status int, contentType, body, retryAfter string, mayHaveSubmitted, allowFailover bool, cause error) error {
	retryClass := "none"
	if allowFailover {
		retryClass = "credential_rotation"
	}
	return &ExecutorError{
		Phase:            phase,
		RetryClass:       retryClass,
		HTTPStatus:       status,
		ContentType:      contentType,
		MayHaveSubmitted: mayHaveSubmitted,
		AllowFailover:    allowFailover,
		Message:          "upstream request failed",
		Cause:            cause,
		Body:             body,
		RetryAfter:       retryAfter,
	}
}
