package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
)

// materializeProfileVideoURL provides the first required-media end-to-end
// path. It is intentionally opt-in and synchronous for gateway_wait results;
// client/background modes continue to use their durable task state until a
// queued materialization callback is available.
func materializeProfileVideoURL(c *gin.Context, taskRunID, sourceURL string) (string, error) {
	return materializeProfileMediaURL(c, taskRunID, "video", 0, sourceURL)
}

// materializeProfileMediaURL synchronously creates one logical asset and
// atomically stores its provider result. It is used only when a required
// result must be stable before the HTTP response is sent; async/client and
// background paths use the durable media worker instead.
func materializeProfileMediaURL(c *gin.Context, taskRunID, kind string, ordinal int, sourceURL string) (string, error) {
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
	if err := db.CreateMediaAssetContext(c.Request.Context(), asset); err != nil {
		return "", err
	}
	_ = db.RetryMediaAssetMaterializationContext(c.Request.Context(), asset.ID)
	job := &db.MediaMaterializationJob{AssetID: asset.ID, Status: db.MediaJobRunning}
	if err := db.CreateMediaMaterializationJobContext(c.Request.Context(), job); err != nil {
		_ = db.MarkMediaAssetFailedContext(context.Background(), asset.ID, err.Error())
		return "", err
	}
	store, err := getMediaObjectStore()
	if err != nil {
		_ = db.FailMediaMaterializationJobContext(context.Background(), job.ID, err.Error(), time.Time{})
		_ = db.MarkMediaAssetFailedContext(context.Background(), asset.ID, err.Error())
		return "", err
	}
	fetcher := newProfileMediaSourceFetcher()
	if source.SourceKind == relaymedia.SourceBase64 {
		fetcher = relaymedia.InlineSourceFetcher{}
	}
	worker := &relaymedia.MaterializationWorker{Store: store, Fetcher: fetcher}
	key := relaymedia.ProfileObjectKey(publicID, kind, "")
	source.MaxBytes = defaultMediaMaxBytes
	info, err := worker.Materialize(c.Request.Context(), source, key)
	if err != nil {
		_ = db.FailMediaMaterializationJobContext(context.Background(), job.ID, err.Error(), time.Time{})
		_ = db.MarkMediaAssetFailedContext(context.Background(), asset.ID, err.Error())
		return "", err
	}
	objectID := profileMediaObjectID(info.Key, info.SHA256)
	if err := ensureProfileMediaObject(c.Request.Context(), objectID, info); err != nil {
		_ = db.FailMediaMaterializationJobContext(context.Background(), job.ID, err.Error(), time.Time{})
		_ = db.MarkMediaAssetFailedContext(context.Background(), asset.ID, err.Error())
		return "", err
	}
	if err := db.MarkMediaAssetAvailableContext(c.Request.Context(), asset.ID, objectID, info.ContentType, info.SHA256, info.Size); err != nil {
		_ = db.FailMediaMaterializationJobContext(context.Background(), job.ID, err.Error(), time.Time{})
		return "", err
	}
	_ = db.CompleteTaskRunAfterMediaContext(c.Request.Context(), taskRunID, kind)
	_ = db.CompleteMediaMaterializationJobContext(context.Background(), job.ID)
	return mediaPublicURL(c, publicID, capability), nil
}

// Keep the synchronous path on the same strict HTTP fetcher as background
// materialization, while allowing package-level tests to inject a controlled
// fetch policy for httptest servers.
var profileMediaFetcherFactory = func() relaymedia.SourceFetcher {
	return relaymedia.HTTPSourceFetcher{}
}

func newProfileMediaSourceFetcher() relaymedia.SourceFetcher {
	if profileMediaFetcherFactory == nil {
		return relaymedia.HTTPSourceFetcher{}
	}
	if fetcher := profileMediaFetcherFactory(); fetcher != nil {
		return fetcher
	}
	return relaymedia.HTTPSourceFetcher{}
}

// materializeProfileImageResponse replaces every provider URL in an image
// response with a gateway capability URL. Raw provider payloads are copied
// and rewritten too, so the response cannot leak a temporary signed URL in an
// auxiliary field while data[].url appears stable.
func materializeProfileImageResponse(c *gin.Context, taskRunID string, response map[string]any, sourceURLs []string) error {
	if response == nil || len(sourceURLs) == 0 {
		return nil
	}
	replacements := make(map[string]string, len(sourceURLs))
	managed := make([]string, 0, len(sourceURLs))
	for ordinal, sourceURL := range sourceURLs {
		stableURL, err := materializeProfileMediaURL(c, taskRunID, "image", ordinal, sourceURL)
		if err != nil {
			return err
		}
		replacements[sourceURL] = stableURL
		managed = append(managed, stableURL)
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
		response["data"] = make([]map[string]string, 0, len(managed))
		for _, stable := range managed {
			response["data"] = append(response["data"].([]map[string]string), map[string]string{"url": stable})
		}
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

func profileMediaObjectID(key, checksum string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(key) + "\x00" + strings.TrimSpace(checksum)))
	return "obj_" + hex.EncodeToString(sum[:])[:60]
}

func ensureProfileMediaObject(ctx context.Context, objectID string, info relaymedia.ObjectInfo) error {
	err := db.CreateMediaObjectContext(ctx, &db.MediaObject{
		ID:          objectID,
		Backend:     "local",
		StorageKey:  info.Key,
		SHA256:      info.SHA256,
		ByteSize:    info.Size,
		ContentType: info.ContentType,
		ETag:        info.SHA256,
		State:       db.MediaObjectReady,
	})
	if err == nil {
		return nil
	}
	existing, getErr := db.GetMediaObjectByIDContext(ctx, objectID)
	if getErr == nil && existing.StorageKey == info.Key && existing.SHA256 == info.SHA256 && existing.State == db.MediaObjectReady {
		return nil
	}
	return err
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
