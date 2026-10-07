package router

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/audit"
	"relay-gateway/db"
	relaymedia "relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/task"
)

// Persist the complete result set before any download can complete its task.
// Claim the first pending asset in that same transaction; later assets may be
// completed by background workers and are reloaded before a synchronous claim.
func materializeProfileMediaURLs(c *gin.Context, taskRunID, kind, baseURL string, sourceURLs []string) ([]string, error) {
	if c == nil || len(sourceURLs) == 0 {
		return nil, errors.New("profile media sources are required")
	}
	if strings.TrimSpace(kind) == "" {
		return nil, errors.New("profile media kind is required")
	}
	for _, raw := range sourceURLs {
		if strings.TrimSpace(raw) == "" {
			return nil, errors.New("profile media source URL is required")
		}
		source := relaymedia.NormalizeMediaSource(raw)
		if source.SourceKind == relaymedia.SourceBase64 && !strings.HasPrefix(strings.ToLower(source.ContentType), strings.ToLower(strings.TrimSpace(kind))+"/") {
			return nil, fmt.Errorf("inline %s media has incompatible content type %q", kind, source.ContentType)
		}
	}
	ctx := c.Request.Context()
	database := db.DBForContext(ctx)
	if database == nil {
		return nil, errors.New("profile media database is not initialized")
	}
	const lease = 5 * time.Minute
	var assets []db.MediaAsset
	var firstJob *db.MediaMaterializationJob
	firstIndex := -1
	err := database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txCtx := db.WithTx(ctx, tx)
		var err error
		assets, err = db.EnsureTaskResultMediaContext(txCtx, taskRunID, kind, sourceURLs)
		if err != nil {
			return err
		}
		for index, asset := range assets {
			if asset.Kind != kind {
				return errors.New("profile media asset kind conflicts with its result")
			}
			if asset.OriginRequestID == "" {
				if err := tx.Model(&asset).Update("origin_request_id", audit.RequestID(ctx)).Error; err != nil {
					return err
				}
			}
			if firstIndex < 0 && (asset.Status != db.MediaAssetAvailable || asset.ObjectID == "") {
				firstJob, err = db.ClaimMediaAssetMaterializationContext(txCtx, asset.ID, "profile-sync", lease)
				if err != nil {
					return err
				}
				firstIndex = index
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	pendingFirst := firstJob
	defer func() {
		if pendingFirst != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = db.FailMediaMaterializationWithAssetForLeaseContext(cleanupCtx, pendingFirst, "synchronous media materialization interrupted", time.Now().Add(15*time.Second))
		}
	}()
	managed := make([]string, 0, len(assets))
	for index := range assets {
		asset, err := db.GetMediaAssetByIDContext(ctx, assets[index].ID)
		if err != nil {
			return nil, err
		}
		if asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" {
			job := firstJob
			if index != firstIndex {
				job, err = db.ClaimMediaAssetMaterializationContext(ctx, asset.ID, "profile-sync", lease)
				if err != nil {
					return nil, err
				}
			}
			fail := func(cause error) ([]string, error) {
				if index == firstIndex {
					pendingFirst = nil
				}
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = db.FailMediaMaterializationWithAssetForLeaseContext(cleanupCtx, job, cause.Error(), time.Now().Add(15*time.Second))
				return nil, cause
			}
			store, storeErr := getMediaObjectStore()
			if storeErr != nil {
				return fail(storeErr)
			}
			fetcher := newProfileMediaSourceFetcher(sourceURLs[asset.Ordinal], baseURL)
			if asset.SourceKind == relaymedia.SourceBase64 {
				fetcher = relaymedia.InlineSourceFetcher{}
			}
			if _, err := task.MaterializeClaimedMedia(ctx, job, asset, store, fetcher, lease); err != nil {
				return fail(err)
			}
			if index == firstIndex {
				pendingFirst = nil
			}
		}
		capability, err := db.RecoverMediaAssetCapability(asset)
		if err != nil {
			return nil, err
		}
		managed = append(managed, mediaPublicURL(c, asset.PublicID, capability))
	}
	return managed, nil
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

func profileMediaRetentionEnabled(op protocol.Operation) bool {
	return op.EffectiveMediaRetention() != protocol.MediaRetentionDisabled
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
