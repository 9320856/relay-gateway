package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/internal/httpforward"
	"relay-gateway/media"
	"relay-gateway/protocol"
)

// The operation copy is execution-local. The published definition and its
// digest must remain untouched when the channel changes operational policy.
func applyChannelMediaRetention(op protocol.Operation, channel *config.UpstreamChannel) protocol.Operation {
	op.MediaRetention = media.RetentionDisabled
	if channel != nil {
		op.MediaRetention = channel.MediaRetention
		if op.MediaRetention == "" {
			op.MediaRetention = media.RetentionDisabled
		}
	}
	return op
}

func taskMediaOperation(ctx context.Context, run *db.TaskRun, op protocol.Operation) (protocol.Operation, error) {
	policy, err := db.TaskMediaRetention(ctx, run)
	if err == nil {
		op.MediaRetention = policy
	}
	return op, err
}

func enqueueMediaSources(c *gin.Context, ownerID, kind string, sources []string) error {
	ctx := c.Request.Context()
	conn := db.DBForContext(ctx)
	if conn == nil {
		return errors.New("media database is not initialized")
	}
	return conn.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		assets, err := db.EnsureTaskResultMediaContext(db.WithTx(ctx, tx), ownerID, kind, sources)
		if err != nil {
			return err
		}
		for _, asset := range assets {
			if asset.OriginRequestID == "" {
				if err := tx.Model(&asset).Update("origin_request_id", audit.RequestID(ctx)).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// retainMediaResponse is called after a successful provider submission. A
// local storage error must never be returned to the provider retry loop.
func retainMediaResponse(c *gin.Context, ownerID, kind, baseURL, policy string, payload any, sources []string) error {
	policy, err := media.NormalizeRetention(policy)
	if err != nil || policy == media.RetentionDisabled {
		return err
	}
	if c == nil || c.Request == nil || strings.TrimSpace(ownerID) == "" {
		return errors.New("media response provenance is required")
	}
	if len(sources) == 0 {
		err = errors.New("successful media response contains no downloadable media source")
	} else if policy == media.RetentionBestEffort {
		err = enqueueMediaSources(c, ownerID, kind, sources)
	} else {
		var managed []string
		managed, err = materializeProfileMediaURLs(c, ownerID, kind, baseURL, sources)
		if err == nil {
			replacements := make(map[string]string, len(sources)*2)
			for index, source := range sources {
				replacements[source] = managed[index]
				if strings.HasPrefix(source, "data:") {
					if _, encoded, found := strings.Cut(source, ","); found {
						replacements[encoded] = managed[index]
					}
				}
			}
			if encoded, encodeErr := json.Marshal(payload); encodeErr == nil {
				for _, original := range profileResultURLs(string(encoded), kind) {
					normalized := media.NormalizeMediaSources([]string{original}, baseURL)
					if kind == "image" {
						normalized = normalizeProfileImageSources([]string{original}, baseURL)
					}
					if len(normalized) == 1 {
						if stable, exists := replacements[normalized[0]]; exists {
							replacements[original] = stable
						}
					}
				}
			}
			err = replaceMediaPayload(payload, replacements)
		}
	}
	if err != nil && policy == media.RetentionBestEffort {
		// Keep the successful upstream result, recording only a local error.
		audit.AddEvent(c.Request.Context(), "media_retention", audit.EventData{Message: "media retention enqueue failed"})
		_ = c.Error(err)
		return nil
	}
	return err
}

func replaceMediaPayload(payload any, replacements map[string]string) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(encoded, &value); err != nil {
		return err
	}
	value = rewriteRetainedMedia(value, replacements)
	if target, ok := payload.(map[string]any); ok {
		updated, ok := value.(map[string]any)
		if !ok {
			return errors.New("media response is not an object")
		}
		clear(target)
		for key, item := range updated {
			target[key] = item
		}
		return nil
	}
	encoded, err = json.Marshal(value)
	if err != nil {
		return err
	}
	target := reflect.ValueOf(payload)
	if target.Kind() == reflect.Pointer && !target.IsNil() {
		fresh := reflect.New(target.Elem().Type())
		if err := json.Unmarshal(encoded, fresh.Interface()); err != nil {
			return err
		}
		target.Elem().Set(fresh.Elem())
		return nil
	}
	// Named map types (for example gin.H) still refer to the caller's map.
	if target.Kind() == reflect.Map && target.Type().Key().Kind() == reflect.String {
		fresh := reflect.New(target.Type())
		if err := json.Unmarshal(encoded, fresh.Interface()); err != nil {
			return err
		}
		target.Clear()
		for iter := fresh.Elem().MapRange(); iter.Next(); {
			target.SetMapIndex(iter.Key(), iter.Value())
		}
		return nil
	}
	return errors.New("media response must be an object or pointer")
}

func rewriteRetainedMedia(value any, replacements map[string]string) any {
	switch typed := value.(type) {
	case string:
		if replacement, exists := replacements[typed]; exists {
			return replacement
		}
	case []any:
		for index := range typed {
			typed[index] = rewriteRetainedMedia(typed[index], replacements)
		}
	case map[string]any:
		if inline, ok := typed["b64_json"].(string); ok {
			if replacement, exists := replacements[inline]; exists {
				delete(typed, "b64_json")
				typed["url"] = replacement
			}
		}
		for key, item := range typed {
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "url", "video_url", "image_url", "proxy_url", "thumbnail_url", "b64_json":
				typed[key] = rewriteRetainedMedia(item, replacements)
			default:
				// Recurse only into envelopes; identifiers and metadata must
				// remain untouched even if equal to a source locator.
				switch item.(type) {
				case map[string]any, []any:
					typed[key] = rewriteRetainedMedia(item, replacements)
				}
			}
		}
	}
	return value
}

func collectChannelMediaSources(payload any, kind, baseURL string) []string {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	sources := profileResultURLs(string(encoded), kind)
	if kind == "image" {
		return normalizeProfileImageSources(sources, baseURL)
	}
	return media.NormalizeMediaSources(sources, baseURL)
}

// Buffer JSON control responses only, never video bytes. When retention is
// disabled callers pass the original writer through without buffering.
type boundedMediaResponseWriter struct {
	Status        int
	Body          bytes.Buffer
	Err           error
	headers       http.Header
	limit         int64
	fallback      http.ResponseWriter
	passedThrough bool
}

func newBoundedMediaResponseWriter(maxBytes int64) *boundedMediaResponseWriter {
	return &boundedMediaResponseWriter{headers: make(http.Header), limit: maxBytes}
}

func (w *boundedMediaResponseWriter) Header() http.Header { return w.headers }
func (w *boundedMediaResponseWriter) WriteHeader(status int) {
	if w.Status == 0 {
		w.Status = status
	}
}
func (w *boundedMediaResponseWriter) Write(body []byte) (int, error) {
	if w.passedThrough {
		return w.fallback.Write(body)
	}
	if w.Err != nil {
		return 0, w.Err
	}
	if w.Status == 0 {
		w.Status = http.StatusOK
	}
	if int64(w.Body.Len())+int64(len(body)) > w.limit {
		w.Err = fmt.Errorf("%w: media JSON response", protocol.ErrResponseTooLarge)
		if w.fallback != nil {
			httpforward.CopyHeaders(w.fallback, w.headers)
			w.fallback.WriteHeader(w.Status)
			w.passedThrough = true
			if _, err := w.fallback.Write(w.Body.Bytes()); err != nil {
				return 0, err
			}
			w.Body.Reset()
			return w.fallback.Write(body)
		}
		return 0, w.Err
	}
	return w.Body.Write(body)
}
func (w *boundedMediaResponseWriter) Flush() {}

func writeCapturedMediaResponse(c *gin.Context, captured *boundedMediaResponseWriter, payload any) error {
	if captured.Err != nil {
		return captured.Err
	}
	if payload == nil {
		httpforward.CopyHeaders(c.Writer, captured.Header())
		status := captured.Status
		if status == 0 {
			status = http.StatusOK
		}
		c.Status(status)
		_, err := c.Writer.Write(captured.Body.Bytes())
		return err
	}
	httpforward.CopyTransformedHeaders(c.Writer, captured.Header())
	c.JSON(captured.Status, payload)
	return nil
}

func finishLegacyCapturedMedia(c *gin.Context, captured *boundedMediaResponseWriter, channel *config.UpstreamChannel, kind string) error {
	if captured.passedThrough {
		_ = c.Error(captured.Err)
		return nil
	}
	if captured.Err != nil {
		return captured.Err
	}
	if captured.Status < 200 || captured.Status >= 300 {
		return writeCapturedMediaResponse(c, captured, nil)
	}
	var payload map[string]any
	if err := json.Unmarshal(captured.Body.Bytes(), &payload); err != nil {
		if channel.MediaRetention == media.RetentionBestEffort {
			_ = c.Error(err)
			return writeCapturedMediaResponse(c, captured, nil)
		}
		return err
	}
	if err := retainLegacySynchronousMedia(c, channel, kind, payload); err != nil {
		return err
	}
	if channel.MediaRetention == media.RetentionBestEffort {
		return writeCapturedMediaResponse(c, captured, nil)
	}
	return writeCapturedMediaResponse(c, captured, payload)
}
