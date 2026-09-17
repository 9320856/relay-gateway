package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

// profileEngineMultipartDirect proxies a multipart direct operation through
// a published Profile. The wire payload is rebuilt only after the Profile
// policy has normalized scalar fields; uploaded files are copied verbatim.
// This keeps /images/edits on the generic executor without pretending that a
// binary multipart body is JSON.
func profileEngineMultipartDirect(c *gin.Context, operation string, form *multipart.Form, pinned *config.UpstreamChannel) (bool, error) {
	if c == nil || form == nil || !profileEngineEnabledFor("RELAY_ENABLE_PROFILE_DIRECT_ENGINE") {
		return false, nil
	}
	modelName := ""
	if values := form.Value["model"]; len(values) > 0 {
		modelName = strings.TrimSpace(values[0])
	}
	if modelName == "" {
		modelName = "gpt-image-2"
	}
	var candidates []*config.UpstreamChannel
	var err error
	if pinned != nil {
		candidates = []*config.UpstreamChannel{pinned}
	} else {
		candidates, err = service.DefaultDispatcher.ResolveProfileCandidates(modelName, operation)
	}
	if err != nil {
		return false, nil
	}
	if entry := audit.FromContext(c.Request.Context()); entry != nil {
		ids := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate != nil {
				ids = append(ids, candidate.ID)
			}
		}
		entry.RecordCandidates(ids)
	}
	var lastErr error
	bound := false
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		binding, bindErr := db.FindChannelProtocolBindingContext(c.Request.Context(), candidate.ID, operation, modelName)
		if errors.Is(bindErr, gorm.ErrRecordNotFound) {
			continue
		}
		if bindErr != nil {
			return true, bindErr
		}
		_, compiled, op, err := loadPublishedProfileOperation(c.Request.Context(), binding)
		if err != nil {
			return true, err
		}
		if op.ExecutionMode != protocol.ExecutionDirect {
			continue
		}
		bound = true
		targetModel := modelName
		if mapped := strings.TrimSpace(candidate.ModelMap[modelName]); mapped != "" {
			targetModel = mapped
		}
		payload, contentType, buildErr := buildProfileMultipartPayload(form, targetModel, op)
		if buildErr != nil {
			return true, buildErr
		}
		if entry := audit.FromContext(c.Request.Context()); entry != nil {
			entry.RecordDispatch(candidate.ID, candidate.Type, candidate.BaseURL, targetModel)
		}
		_, execErr := newProfileOperationExecutor(op).ExecuteRaw(c.Request.Context(), compiled, binding.Operation, protocol.Request{
			BaseURL: candidate.BaseURL, APIKeys: candidate.GetEffectiveKeys(), Headers: candidate.Headers,
			RawBody: payload, RawContentType: contentType,
		}, c.Writer, false)
		if execErr == nil {
			return true, nil
		}
		lastErr = execErr
		if c.Writer.Written() || !profileDirectFailoverEligible(execErr) {
			return true, execErr
		}
	}
	if bound {
		return true, lastErr
	}
	return false, nil
}

func buildProfileMultipartPayload(form *multipart.Form, targetModel string, op protocol.Operation) ([]byte, string, error) {
	if form == nil {
		return nil, "", errors.New("multipart form is required")
	}
	fields := make(map[string][]string, len(form.Value))
	for key, value := range op.Submit.Body {
		fields[key] = []string{fmt.Sprint(value)}
	}
	for key, values := range form.Value {
		fields[key] = append([]string(nil), values...)
	}
	fields["model"] = []string{targetModel}
	policyBody := make(map[string]any, len(fields))
	for key, values := range fields {
		if len(values) > 0 {
			policyBody[key] = values[0]
		}
	}
	transformed, err := protocol.ApplyModelPolicy(op.Operation, op.Policy, policyBody)
	if err != nil {
		return nil, "", err
	}
	if normalized, ok := transformed.(map[string]any); ok {
		for key, value := range normalized {
			fields[key] = []string{fmt.Sprint(value)}
		}
		for key := range fields {
			if _, keep := normalized[key]; !keep {
				delete(fields, key)
			}
		}
	}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, value := range fields[key] {
			if err := mw.WriteField(key, value); err != nil {
				return nil, "", err
			}
		}
	}
	fileKeys := make([]string, 0, len(form.File))
	for key := range form.File {
		fileKeys = append(fileKeys, key)
	}
	sort.Strings(fileKeys)
	for _, key := range fileKeys {
		for _, header := range form.File[key] {
			file, err := header.Open()
			if err != nil {
				return nil, "", err
			}
			part, err := mw.CreateFormFile(key, header.Filename)
			var written int64
			if err == nil {
				written, err = io.Copy(part, io.LimitReader(file, maxMultipartFileBytes+1))
			}
			closeErr := file.Close()
			if err != nil {
				return nil, "", err
			}
			if closeErr != nil {
				return nil, "", closeErr
			}
			if header.Size > maxMultipartFileBytes || written > maxMultipartFileBytes {
				return nil, "", fmt.Errorf("uploaded file exceeds the %d MiB per-file limit", maxMultipartFileBytes>>20)
			}
		}
	}
	if err := mw.Close(); err != nil {
		return nil, "", err
	}
	return body.Bytes(), mw.FormDataContentType(), nil
}

// profileEngineDirect proxies direct JSON/SSE operations through a published
// Profile revision. It is opt-in so existing Adapter routing remains the
// default while channels are migrated incrementally.
func profileEngineDirect(c *gin.Context, operation, apiKeyHeader string) (bool, error) {
	if c == nil {
		return false, nil
	}
	handled, _, err := profileEngineDirectForChannelResult(c, operation, apiKeyHeader, nil, c.Writer, c.Writer.Written)
	return handled, err
}

// profileEngineFlagEnabled keeps the per-operation rollout switches while
// allowing operators to opt out of Profile operations with explicit rollback
// flags. When no explicit flag is set, channels with configured profile
// bindings execute through the profile engine by default.
func profileEngineFlagEnabled(specific string) bool {
	if val := os.Getenv(specific); val != "" {
		return val == "1"
	}
	if val := os.Getenv("RELAY_ENABLE_PROFILE_ENGINE"); val != "" {
		return val == "1"
	}
	if os.Getenv("RELAY_DISABLE_PROFILE_ENGINE") == "1" {
		return false
	}
	return true
}

// profileEngineEnabledFor keeps explicit rollout flags as the normal opt-in.
func profileEngineEnabledFor(specific string) bool {
	return profileEngineFlagEnabled(specific) || os.Getenv("RELAY_DISABLE_LEGACY") == "1"
}

// profileIdempotencyKey gives every profile-backed mutating submit one stable
// key across candidate failover. A caller-provided key remains authoritative;
// the audit request ID is the deterministic fallback for gateway requests.
func profileIdempotencyKey(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if key := strings.TrimSpace(c.GetHeader("Idempotency-Key")); key != "" && len(key) <= 255 && !strings.ContainsAny(key, "\r\n") {
		return key
	}
	if requestID := audit.RequestID(c.Request.Context()); requestID != "" {
		return "rg-" + requestID
	}
	return fmt.Sprintf("rg-%d", time.Now().UnixNano())
}

// profileEngineDirectForChannelResult is the result-bearing form used by the
// Playground, where the response envelope also needs the selected channel.
func profileEngineDirectForChannelResult(c *gin.Context, operation, apiKeyHeader string, pinned *config.UpstreamChannel, writer http.ResponseWriter, written func() bool) (bool, *config.UpstreamChannel, error) {
	if c == nil || !profileEngineEnabledFor("RELAY_ENABLE_PROFILE_DIRECT_ENGINE") {
		return false, nil, nil
	}
	rawBody, modelName, err := readBodyAndModel(c)
	if err != nil {
		// Let the legacy handler produce the canonical 400 response for malformed
		// requests. readBodyAndModel restores the body after reading it.
		return false, nil, nil
	}
	if strings.TrimSpace(modelName) == "" {
		return false, nil, nil
	}
	bindingOperation := operation
	var candidates []*config.UpstreamChannel
	if pinned != nil {
		candidates = []*config.UpstreamChannel{pinned}
	} else {
		candidates, err = service.DefaultDispatcher.ResolveProfileCandidates(modelName, bindingOperation)
	}
	// The built-in image preset historically used images.create while some
	// custom profiles use the singular image.create spelling. Treat the latter
	// as a compatibility alias during migration, but persist/use the operation
	// name that actually matched its binding.
	if err != nil && operation == "images.create" {
		bindingOperation = "image.create"
		candidates, err = service.DefaultDispatcher.ResolveProfileCandidates(modelName, bindingOperation)
	}
	if err != nil {
		return false, nil, nil
	}
	if entry := audit.FromContext(c.Request.Context()); entry != nil {
		ids := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate != nil {
				ids = append(ids, candidate.ID)
			}
		}
		entry.RecordCandidates(ids)
	}
	var lastErr error
	bound := false
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		binding, bindErr := db.FindChannelProtocolBindingContext(c.Request.Context(), candidate.ID, bindingOperation, modelName)
		if errors.Is(bindErr, gorm.ErrRecordNotFound) {
			continue
		}
		if bindErr != nil {
			return true, candidate, bindErr
		}
		_, compiled, op, err := loadPublishedProfileOperation(c.Request.Context(), binding)
		if err != nil {
			return true, candidate, err
		}
		if op.ExecutionMode != protocol.ExecutionDirect {
			// An async operation is owned by the task/image bridge rather than
			// this direct JSON/SSE handler. Leave the request untouched so the
			// route can use its existing async compatibility path (or the
			// dedicated /images/jobs Profile bridge) during migration.
			continue
		}
		bound = true
		var body map[string]any
		if err := json.Unmarshal(rawBody, &body); err != nil {
			return true, candidate, fmt.Errorf("profile request body must be a JSON object: %w", err)
		}
		if mapped := strings.TrimSpace(candidate.ModelMap[modelName]); mapped != "" {
			body["model"] = mapped
		}
		headers := map[string]string{}
		for _, name := range []string{"anthropic-version", "anthropic-beta"} {
			if value := strings.TrimSpace(c.GetHeader(name)); value != "" {
				headers[name] = value
			}
		}
		if entry := audit.FromContext(c.Request.Context()); entry != nil {
			targetModel := modelName
			if mapped := strings.TrimSpace(candidate.ModelMap[modelName]); mapped != "" {
				targetModel = mapped
			}
			entry.RecordDispatch(candidate.ID, candidate.Type, candidate.BaseURL, targetModel)
		}
		_, err = newProfileOperationExecutor(op).ExecuteRaw(c.Request.Context(), compiled, binding.Operation, protocol.Request{
			BaseURL: candidate.BaseURL, APIKeys: candidate.GetEffectiveKeys(), APIKeyHeader: apiKeyHeader,
			Headers: headers, Body: body,
		}, writer, isStreamRequested(rawBody))
		if err == nil {
			return true, candidate, nil
		}
		lastErr = err
		// A stream or a response whose headers were already written cannot be
		// replayed against another channel. For pre-response capability and
		// transient HTTP failures, try the next bound candidate.
		if written != nil && written() || written == nil && c.Writer.Written() || !profileDirectFailoverEligible(err) {
			return true, candidate, err
		}
	}
	if bound {
		return true, nil, lastErr
	}
	return false, nil, nil
}

func profileDirectFailoverEligible(err error) bool {
	var executorErr *protocol.ExecutorError
	if !errors.As(err, &executorErr) {
		return false
	}
	// A mutating request that may have reached the provider must never be
	// submitted again on another channel. HTTPExecutor marks POST/PUT/PATCH
	// 5xx responses this way; preserve the single-submit guarantee even when
	// the response arrived before any public bytes were written.
	if executorErr.MayHaveSubmitted {
		return false
	}
	status := executorErr.HTTPStatus
	return status == 401 || status == 403 || status == 404 || status == 405 || status == 408 || status == 429
}

func profileDirectError(c *gin.Context, err error, prefix string) {
	if err == nil || c == nil || c.Writer.Written() {
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	writeUpstreamError(c, err, prefix)
}
