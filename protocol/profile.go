package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	CurrentSchemaVersion = 1
	ExecutionDirect      = "direct"
	ExecutionAsync       = "async"

	PollingOff         = "off"
	PollingClient      = "client"
	PollingGatewayWait = "gateway_wait"
	PollingBackground  = "background"

	// Media retention controls whether a successful media result is persisted
	// as a gateway-managed asset. An empty value is retained for backwards
	// compatibility with profiles written before this field existed and has
	// the same effective behavior as disabled.
	MediaRetentionRequired   = "required"
	MediaRetentionBestEffort = "best_effort"
	MediaRetentionDisabled   = "disabled"
)

// Profile is a versioned collection of operation definitions. A profile may
// contain more than one operation, keyed by the Operation field.
type Profile struct {
	SchemaVersion int    `json:"schema_version"`
	Name          string `json:"name,omitempty"`
	// ModelDefaults are used only when a provider does not expose the
	// conventional /models endpoint. They are profile data rather than
	// Adapter logic, so model discovery remains available after Legacy Adapter
	// retirement.
	ModelDefaults []string    `json:"model_defaults,omitempty"`
	Operations    []Operation `json:"operations"`
}

type Operation struct {
	Operation      string   `json:"operation"`
	ExecutionMode  string   `json:"execution_mode"`
	PollingMode    string   `json:"polling_mode"`
	MediaRetention string   `json:"media_retention,omitempty"`
	Submit         Submit   `json:"submit"`
	Response       Response `json:"response"`
	Poll           *Poll    `json:"poll,omitempty"`
	Content        *Content `json:"content,omitempty"`
	Policy         string   `json:"policy,omitempty"`
}

type Submit struct {
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	BodyEncoding string            `json:"body_encoding,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Body         map[string]any    `json:"body,omitempty"`
}

type Response struct {
	TaskIDPaths []string `json:"task_id_paths,omitempty"`
	ResultPaths []string `json:"result_paths,omitempty"`
}

type Poll struct {
	Method         string            `json:"method"`
	Path           string            `json:"path"`
	Headers        map[string]string `json:"headers,omitempty"`
	IntervalMS     int64             `json:"interval_ms"`
	BackoffMode    string            `json:"backoff_mode,omitempty"`
	BackoffBaseMS  int64             `json:"backoff_base_ms,omitempty"`
	BackoffMaxMS   int64             `json:"backoff_max_ms,omitempty"`
	JitterMS       int64             `json:"jitter_ms,omitempty"`
	MaxAttempts    int               `json:"max_attempts"`
	MaxDurationMS  int64             `json:"max_duration_ms"`
	StatusPath     string            `json:"status_path"`
	SuccessValues  []string          `json:"success_values"`
	FailureValues  []string          `json:"failure_values"`
	ProgressPath   string            `json:"progress_path,omitempty"`
	ResultURLPaths []string          `json:"result_url_paths"`
}

type Content struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
}

// CompiledProfile is an immutable, validated profile. The source JSON is
// canonicalized once at compile time and can be persisted with its digest.
type CompiledProfile struct {
	profile Profile
	json    []byte
	digest  string
}

func (p Profile) Validate() error {
	if p.SchemaVersion < 0 || p.SchemaVersion > CurrentSchemaVersion {
		return fmt.Errorf("unsupported schema_version %d", p.SchemaVersion)
	}
	if len(p.Operations) == 0 {
		return errors.New("operations must not be empty")
	}
	seen := make(map[string]struct{}, len(p.Operations))
	for i := range p.Operations {
		op := p.Operations[i]
		if err := op.Validate(); err != nil {
			return fmt.Errorf("operations[%d]: %w", i, err)
		}
		if _, ok := seen[op.Operation]; ok {
			return fmt.Errorf("operations[%d]: duplicate operation %q", i, op.Operation)
		}
		seen[op.Operation] = struct{}{}
	}
	return nil
}

func (o Operation) Validate() error {
	if !validOperationName(o.Operation) {
		return errors.New("operation is required")
	}
	if strings.TrimSpace(o.Policy) != "" {
		if _, err := ResolveModelPolicy(o.Policy); err != nil {
			return fmt.Errorf("policy: %w", err)
		}
	}
	if o.ExecutionMode != ExecutionDirect && o.ExecutionMode != ExecutionAsync {
		return fmt.Errorf("invalid execution_mode %q", o.ExecutionMode)
	}
	if !validPollingMode(o.PollingMode) {
		return fmt.Errorf("invalid polling_mode %q", o.PollingMode)
	}
	if o.MediaRetention != "" && !validMediaRetention(o.MediaRetention) {
		return fmt.Errorf("invalid media_retention %q", o.MediaRetention)
	}
	if o.ExecutionMode == ExecutionDirect && o.PollingMode != PollingOff {
		return errors.New("direct execution requires polling_mode off")
	}
	if err := o.Submit.Validate(); err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	if err := validateHeaders(o.Submit.Headers); err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	if err := o.Response.Validate(o.ExecutionMode == ExecutionAsync); err != nil {
		return fmt.Errorf("response: %w", err)
	}
	if o.ExecutionMode == ExecutionAsync {
		if o.PollingMode == PollingOff {
			if o.Poll != nil {
				if err := o.Poll.Validate(); err != nil {
					return fmt.Errorf("poll: %w", err)
				}
			}
		} else {
			if o.Poll == nil {
				return errors.New("poll is required when polling is enabled")
			}
			if err := o.Poll.Validate(); err != nil {
				return fmt.Errorf("poll: %w", err)
			}
		}
	} else if o.Poll != nil {
		if err := o.Poll.Validate(); err != nil {
			return fmt.Errorf("poll: %w", err)
		}
	}
	if o.Content != nil {
		if err := o.Content.Validate(); err != nil {
			return fmt.Errorf("content: %w", err)
		}
	}
	return nil
}

// EffectiveMediaRetention returns the policy to use at execution time. Empty
// values represent profiles created before media_retention was introduced.
func (o Operation) EffectiveMediaRetention() string {
	if o.MediaRetention == "" {
		return MediaRetentionDisabled
	}
	return o.MediaRetention
}

func (s Submit) Validate() error { return validateRequest(s.Method, s.Path, s.BodyEncoding) }

func (r Response) Validate(async bool) error {
	if async && len(nonEmpty(r.TaskIDPaths)) == 0 {
		return errors.New("task_id_paths must not be empty for async execution")
	}
	for _, selector := range append(append([]string{}, r.TaskIDPaths...), r.ResultPaths...) {
		if err := validateSelector(selector); err != nil {
			return err
		}
	}
	return nil
}

func (p Poll) Validate() error {
	if err := validateRequest(p.Method, p.Path, ""); err != nil {
		return err
	}
	if p.IntervalMS < 0 || p.IntervalMS > int64((24*time.Hour)/time.Millisecond) {
		return errors.New("interval_ms must be between 0 and 86400000")
	}
	if p.BackoffMode != "" && p.BackoffMode != "fixed" && p.BackoffMode != "linear" && p.BackoffMode != "exponential" {
		return fmt.Errorf("invalid backoff_mode %q", p.BackoffMode)
	}
	if p.BackoffBaseMS < 0 || p.BackoffBaseMS > int64((24*time.Hour)/time.Millisecond) {
		return errors.New("backoff_base_ms must be between 0 and 86400000")
	}
	if p.BackoffMaxMS < 0 || p.BackoffMaxMS > int64((7*24*time.Hour)/time.Millisecond) {
		return errors.New("backoff_max_ms must be between 0 and 604800000")
	}
	if p.JitterMS < 0 || p.JitterMS > int64((24*time.Hour)/time.Millisecond) {
		return errors.New("jitter_ms must be between 0 and 86400000")
	}
	if p.MaxAttempts <= 0 || p.MaxAttempts > 100000 {
		return errors.New("max_attempts must be between 1 and 100000")
	}
	if p.MaxDurationMS <= 0 || p.MaxDurationMS > int64((7*24*time.Hour)/time.Millisecond) {
		return errors.New("max_duration_ms must be between 1 and 604800000")
	}
	if err := validateSelector(p.StatusPath); err != nil {
		return fmt.Errorf("status_path: %w", err)
	}
	if len(nonEmpty(p.SuccessValues)) == 0 || len(nonEmpty(p.SuccessValues)) != len(p.SuccessValues) {
		return errors.New("success_values must not be empty")
	}
	if len(nonEmpty(p.FailureValues)) == 0 || len(nonEmpty(p.FailureValues)) != len(p.FailureValues) {
		return errors.New("failure_values must not be empty")
	}
	if len(nonEmpty(p.ResultURLPaths)) == 0 || len(nonEmpty(p.ResultURLPaths)) != len(p.ResultURLPaths) {
		return errors.New("result_url_paths must not be empty")
	}
	if p.ProgressPath != "" {
		if err := validateSelector(p.ProgressPath); err != nil {
			return fmt.Errorf("progress_path: %w", err)
		}
	}
	for _, selector := range p.ResultURLPaths {
		if err := validateSelector(selector); err != nil {
			return err
		}
	}
	return validateHeaders(p.Headers)
}

func (c Content) Validate() error {
	if err := validateRequest(c.Method, c.Path, ""); err != nil {
		return err
	}
	return validateHeaders(c.Headers)
}

func validateRequest(method, path, encoding string) error {
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		return errors.New("method is required")
	}
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return fmt.Errorf("method %q is not allowed", method)
	}
	if path == "" || path[0] != '/' || strings.Contains(path, "\\") || strings.Contains(path, "//") || strings.Contains(path, "..") || strings.ContainsAny(path, "\r\n\x00") || strings.Contains(path, "#") {
		return fmt.Errorf("unsafe path %q", path)
	}
	if strings.Contains(path, "://") {
		return fmt.Errorf("unsafe path %q", path)
	}
	if encoding != "" && encoding != "json" && encoding != "form" && encoding != "raw" {
		return fmt.Errorf("invalid body_encoding %q", encoding)
	}
	return nil
}

var operationName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var selectorName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func validOperationName(name string) bool { return operationName.MatchString(name) }

func validateSelector(selector string) error {
	if selector == "" || len(selector) > 256 || strings.TrimSpace(selector) != selector {
		return fmt.Errorf("invalid selector %q", selector)
	}
	parts := strings.Split(selector, ".")
	if len(parts) > 32 {
		return fmt.Errorf("selector %q exceeds maximum depth", selector)
	}
	for _, part := range parts {
		if !selectorName.MatchString(part) {
			return fmt.Errorf("invalid selector %q", selector)
		}
	}
	return nil
}

var headerName = regexp.MustCompile(`^[!#$%&'*+.^_` + "`" + `|~0-9A-Za-z-]+$`)

func validateHeaders(headers map[string]string) error {
	for name := range headers {
		if !headerName.MatchString(name) {
			return fmt.Errorf("invalid header name %q", name)
		}
		switch strings.ToLower(name) {
		case "authorization", "proxy-authorization", "cookie", "set-cookie", "host", "content-length", "transfer-encoding", "connection":
			return fmt.Errorf("header %q is not allowed", name)
		}
		if strings.HasPrefix(strings.ToLower(name), "proxy-") {
			return fmt.Errorf("header %q is not allowed", name)
		}
	}
	return nil
}

func validPollingMode(mode string) bool {
	return mode == PollingOff || mode == PollingClient || mode == PollingGatewayWait || mode == PollingBackground
}

func validMediaRetention(policy string) bool {
	return policy == MediaRetentionRequired || policy == MediaRetentionBestEffort || policy == MediaRetentionDisabled
}

func nonEmpty(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			result = append(result, value)
		}
	}
	return result
}

// Compile validates p and returns an immutable compiled representation.
func Compile(p Profile) (CompiledProfile, error) {
	owned := cloneProfile(p)
	if err := owned.Validate(); err != nil {
		return CompiledProfile{}, err
	}
	data, err := canonicalJSON(owned)
	if err != nil {
		return CompiledProfile{}, fmt.Errorf("canonicalize profile: %w", err)
	}
	digest := sha256.Sum256(data)
	return CompiledProfile{profile: owned, json: append([]byte(nil), data...), digest: hex.EncodeToString(digest[:])}, nil
}

func (c CompiledProfile) Profile() Profile      { return cloneProfile(c.profile) }
func (c CompiledProfile) CanonicalJSON() []byte { return append([]byte(nil), c.json...) }
func (c CompiledProfile) Digest() string        { return c.digest }

func (p Profile) CanonicalJSON() ([]byte, error) { return canonicalJSON(p) }

func (p Profile) Digest() (string, error) {
	data, err := p.CanonicalJSON()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func cloneProfile(p Profile) Profile {
	out := p
	out.ModelDefaults = append([]string(nil), p.ModelDefaults...)
	out.Operations = make([]Operation, len(p.Operations))
	for i, op := range p.Operations {
		out.Operations[i] = op
		out.Operations[i].Submit = cloneSubmit(op.Submit)
		out.Operations[i].Response = Response{TaskIDPaths: append([]string(nil), op.Response.TaskIDPaths...), ResultPaths: append([]string(nil), op.Response.ResultPaths...)}
		if op.Poll != nil {
			poll := *op.Poll
			poll.Headers = cloneMap(op.Poll.Headers)
			poll.SuccessValues = append([]string(nil), op.Poll.SuccessValues...)
			poll.FailureValues = append([]string(nil), op.Poll.FailureValues...)
			poll.ResultURLPaths = append([]string(nil), op.Poll.ResultURLPaths...)
			out.Operations[i].Poll = &poll
		}
		if op.Content != nil {
			content := *op.Content
			content.Headers = cloneMap(op.Content.Headers)
			out.Operations[i].Content = &content
		}
	}
	return out
}

func cloneSubmit(s Submit) Submit {
	s.Headers = cloneMap(s.Headers)
	if s.Body != nil {
		s.Body = cloneAnyMap(s.Body)
	}
	return s
}
func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func cloneAnyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = cloneAny(v)
	}
	return out
}
func cloneAny(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneAnyMap(x)
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = cloneAny(item)
		}
		return out
	default:
		return v
	}
}

func canonicalJSON(v any) ([]byte, error) {
	// encoding/json sorts map keys; normalize through an interface to ensure
	// callers receive the same bytes regardless of map insertion order.
	var normalized any
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &normalized); err != nil {
		return nil, err
	}
	return marshalSorted(normalized)
}

func marshalSorted(v any) ([]byte, error) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for key := range x {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var b bytes.Buffer
		b.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			keyJSON, _ := json.Marshal(key)
			b.Write(keyJSON)
			b.WriteByte(':')
			valueJSON, err := marshalSorted(x[key])
			if err != nil {
				return nil, err
			}
			b.Write(valueJSON)
		}
		b.WriteByte('}')
		return b.Bytes(), nil
	case []any:
		var b bytes.Buffer
		b.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			data, err := marshalSorted(item)
			if err != nil {
				return nil, err
			}
			b.Write(data)
		}
		b.WriteByte(']')
		return b.Bytes(), nil
	default:
		return json.Marshal(v)
	}
}
