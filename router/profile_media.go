package router

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/audit"
	"relay-gateway/db"
	relaymedia "relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/task"
)

// materializeProfileVideoURL provides the first required-media end-to-end
// path. It is intentionally opt-in and synchronous for gateway_wait results;
// client/background modes continue to use their durable task state until a
// queued materialization callback is available.
func materializeProfileVideoURL(c *gin.Context, taskRunID, sourceURL, baseURL string) (string, error) {
	return materializeProfileMediaURL(c, taskRunID, "video", 0, sourceURL, baseURL)
}

// materializeProfileMediaURL synchronously creates one logical asset and
// atomically stores its provider result. It is used only when a required
// result must be stable before the HTTP response is sent; async/client and
// background paths use the durable media worker instead.
func materializeProfileMediaURL(c *gin.Context, taskRunID, kind string, ordinal int, sourceURL, baseURL string) (string, error) {
	if c == nil || strings.TrimSpace(sourceURL) == "" {
		return "", errors.New("profile media source URL is required")
	}
	if strings.TrimSpace(kind) == "" {
		return "", errors.New("profile media kind is required")
	}
	source := relaymedia.NormalizeMediaSource(sourceURL)
	if source.SourceKind == relaymedia.SourceBase64 && !strings.HasPrefix(strings.ToLower(source.ContentType), strings.ToLower(strings.TrimSpace(kind))+"/") {
		return "", fmt.Errorf("inline %s media has incompatible content type %q", kind, source.ContentType)
	}
	publicID, capability, capabilityHash, err := db.NewMediaLinkIdentity()
	if err != nil {
		return "", err
	}
	capabilityCiphertext, err := db.EncryptMediaCapability(capability)
	if err != nil {
		return "", err
	}
	asset := &db.MediaAsset{PublicID: publicID, CapabilityHash: capabilityHash, CapabilityCiphertext: capabilityCiphertext, TaskRunID: strings.TrimSpace(taskRunID), OriginRequestID: audit.RequestID(c.Request.Context()), Kind: strings.TrimSpace(kind), Ordinal: ordinal, Status: db.MediaAssetPending, SourceKind: source.SourceKind, SourceLocator: source.Locator, ContentType: source.ContentType}
	const lease = 5 * time.Minute
	job, err := db.CreateMediaAssetForMaterializationContext(c.Request.Context(), asset, "profile-sync", lease)
	if err != nil {
		return "", err
	}
	fail := func(cause error) (string, error) {
		_ = db.FailMediaMaterializationWithAssetForLeaseContext(context.Background(), job, cause.Error(), time.Now().Add(15*time.Second))
		return "", cause
	}
	store, err := getMediaObjectStore()
	if err != nil {
		return fail(err)
	}
	fetcher := newProfileMediaSourceFetcher(sourceURL, baseURL)
	if source.SourceKind == relaymedia.SourceBase64 {
		fetcher = relaymedia.InlineSourceFetcher{}
	}
	if _, err := task.MaterializeClaimedMedia(c.Request.Context(), job, asset, store, fetcher, lease); err != nil {
		return fail(err)
	}
	return mediaPublicURL(c, publicID, capability), nil
}

// Keep the synchronous path on the same strict HTTP fetcher as background
// materialization, while allowing package-level tests to inject a controlled
// fetch policy for httptest servers.
var profileMediaFetcherFactory = func(sourceURL, baseURL string) relaymedia.SourceFetcher {
	return relaymedia.NewHTTPSourceFetcher(sourceURL, baseURL)
}

func newProfileMediaSourceFetcher(sourceURL, baseURL string) relaymedia.SourceFetcher {
	if profileMediaFetcherFactory == nil {
		return relaymedia.NewHTTPSourceFetcher(sourceURL, baseURL)
	}
	if fetcher := profileMediaFetcherFactory(sourceURL, baseURL); fetcher != nil {
		return fetcher
	}
	return relaymedia.NewHTTPSourceFetcher(sourceURL, baseURL)
}

// materializeProfileImageResponse replaces every provider URL in an image
// response with a gateway capability URL. Raw provider payloads are copied
// and rewritten too, so the response cannot leak a temporary signed URL in an
// auxiliary field while data[].url appears stable.
func materializeProfileImageResponse(c *gin.Context, taskRunID, baseURL string, response map[string]any, sourceURLs []string) error {
	if response == nil || len(sourceURLs) == 0 {
		return nil
	}
	replacements := make(map[string]string, len(sourceURLs))
	managedData := make([]map[string]string, 0, len(sourceURLs))
	for ordinal, sourceURL := range sourceURLs {
		stableURL, err := materializeProfileMediaURL(c, taskRunID, "image", ordinal, sourceURL, baseURL)
		if err != nil {
			return err
		}
		replacements[sourceURL] = stableURL
		managedData = append(managedData, map[string]string{"url": stableURL})
	}
	if data, ok := response["data"].([]map[string]string); ok {
		for _, item := range data {
			if source, exists := item["url"]; exists {
				if stable, found := replacements[source]; found {
					item["url"] = stable
				}
			}
		}
	} else {
		response["data"] = managedData
	}
	for _, key := range []string{"raw", "raw_payload"} {
		if value, exists := response[key]; exists {
			response[key] = rewriteProfileMediaValue(value, replacements)
		}
	}
	return nil
}

func rewriteProfileMediaValue(value any, replacements map[string]string) any {
	switch typed := value.(type) {
	case string:
		if replacement, ok := replacements[typed]; ok {
			return replacement
		}
		return typed
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = rewriteProfileMediaValue(item, replacements)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = rewriteProfileMediaValue(item, replacements)
		}
		return out
	default:
		return value
	}
}

func profileMediaRequired() bool { return os.Getenv("RELAY_PROFILE_MEDIA_REQUIRED") == "1" }

func profileMediaRetentionEnabled(op protocol.Operation) bool {
	return os.Getenv("RELAY_PROFILE_MEDIA_DISABLED") != "1" && op.EffectiveMediaRetention() != protocol.MediaRetentionDisabled
}

func isManagedMediaURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && strings.HasPrefix(u.Path, "/v1/media/")
}

// persistableProfileVideoResponse removes capability-bearing URLs before a
// response is written into TaskRun.ResultBody. The caller's response remains
// untouched so the just-created link can still be returned to the client.
func persistableProfileVideoResponse(response *model.VideoTaskResponse) *model.VideoTaskResponse {
	if response == nil {
		return nil
	}
	copy := *response
	if isManagedMediaURL(copy.VideoURL) || isManagedMediaURL(copy.URL) {
		copy.VideoURL, copy.URL, copy.Data = "", "", nil
	}
	return &copy
}

// persistableProfileImageResponse strips capability URLs from the durable
// TaskRun projection while leaving the response returned to the caller intact.
// This covers nested raw payloads as well as data[].url.
func persistableProfileImageResponse(response map[string]any) map[string]any {
	if response == nil {
		return nil
	}
	clean := stripPersistedProfileMediaValue(response)
	if result, ok := clean.(map[string]any); ok {
		return result
	}
	return map[string]any{}
}

func stripPersistedProfileMediaValue(value any) any {
	switch typed := value.(type) {
	case string:
		if isManagedMediaURL(typed) {
			return ""
		}
		return typed
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = stripPersistedProfileMediaValue(item)
		}
		return out
	case []map[string]string:
		out := make([]map[string]string, len(typed))
		for i, item := range typed {
			copyItem := make(map[string]string, len(item))
			for key, raw := range item {
				if isManagedMediaURL(raw) {
					copyItem[key] = ""
				} else {
					copyItem[key] = raw
				}
			}
			out[i] = copyItem
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = stripPersistedProfileMediaValue(item)
		}
		return out
	default:
		return value
	}
}

func profileMediaErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("media materialization failed: %v", err)
}
