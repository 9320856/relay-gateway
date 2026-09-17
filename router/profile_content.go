package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/adapter"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
)

// profileTaskContent serves a Profile-owned video task without consulting the
// legacy Adapter registry. A TaskRun already captured the channel, provider
// task ID, and immutable Profile revision at submit time, so content delivery
// must use that snapshot just like status polling does.
func profileTaskContent(c *gin.Context, taskID string) (bool, error) {
	if c == nil || strings.TrimSpace(taskID) == "" {
		return false, nil
	}
	run, err := db.GetTaskRunByAliasContext(c.Request.Context(), taskID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if run.Engine != "profile" || !strings.EqualFold(strings.TrimSpace(run.TaskKind), asyncTaskKindVideo) {
		return false, nil
	}
	// Load and compile the immutable revision once. The previous implementation
	// resolved the operation through profileOperationForRun and then loaded the
	// same revision again before FetchContent, which doubled database work on
	// every public media request and made it easier for the two reads to drift
	// during error handling.
	compiled, err := profileContentProfile(c.Request.Context(), run)
	if err != nil {
		return true, fmt.Errorf("load profile content revision: %w", err)
	}
	op, ok := profileOperation(compiled, run.Operation)
	if !ok || op.Content == nil {
		return true, fmt.Errorf("profile operation %q has no content definition", run.Operation)
	}

	// Required-retention tasks must be served from a gateway-managed object once
	// materialized. Never fall back to a provider URL while the asset is pending
	// or failed, otherwise a temporary signed URL could escape the retention
	// contract. Best-effort/disabled profiles may use the declared provider
	// content endpoint below.
	if served, serveErr := serveProfileManagedVideo(c, run); served || serveErr != nil {
		return true, serveErr
	}
	if op.EffectiveMediaRetention() == protocol.MediaRetentionRequired {
		// A provider may report a completed video without a URL and expose the
		// bytes only through this authenticated Content operation. Ensure a
		// durable media job exists before returning the normal materializing
		// response so a direct content request can recover tasks created before
		// the background poller observed completion.
		if _, enqueueErr := ensureProfileTaskContentMedia(c.Request.Context(), run.ID, asyncTaskKindVideo); enqueueErr != nil {
			return true, enqueueErr
		}
		return true, &adapter.UpstreamHTTPError{StatusCode: http.StatusNotFound, Body: `{"error":"video media is still materializing"}`}
	}

	channel, err := db.GetChannelModelContext(c.Request.Context(), run.ChannelID)
	if err != nil {
		return true, fmt.Errorf("load profile content channel: %w", err)
	}
	upstream := channel.ToUpstreamChannel()
	headers := make(map[string]string)
	if rangeHeader := strings.TrimSpace(c.GetHeader("Range")); rangeHeader != "" {
		headers["Range"] = rangeHeader
	}
	// Do not follow redirects here. A provider may return a short-lived CDN
	// location, which can be safely relayed to the client only after validation;
	// following it inside the gateway could forward provider credentials to an
	// untrusted host.
	client := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	content, err := protocol.NewHTTPExecutor(client).FetchContent(c.Request.Context(), compiled, run.Operation, protocol.Request{
		BaseURL: upstream.BaseURL, APIKeys: upstream.GetEffectiveKeys(), Headers: headers, TaskID: run.ProviderTaskID,
	})
	if err != nil {
		return true, profileContentExecutorError(err)
	}
	if content.Body == nil {
		return true, errors.New("profile content response body is missing")
	}
	defer content.Body.Close()
	if content.HTTPStatus >= 300 && content.HTTPStatus < 400 {
		location, locationErr := safeProfileContentRedirect(channel.BaseURL, content.Headers.Get("Location"))
		if locationErr != nil {
			return true, locationErr
		}
		c.Header("Location", location)
		c.Header("Cache-Control", "no-store")
		c.Header("X-Accel-Buffering", "no")
		c.Status(content.HTTPStatus)
		return true, nil
	}
	copyProfileContentHeaders(c, content.Headers)
	setPublicMediaSecurityHeaders(c)
	rawContentType := c.Writer.Header().Get("Content-Type")
	if strings.TrimSpace(rawContentType) == "" {
		rawContentType = content.Headers.Get("Content-Type")
	}
	taskName := strings.TrimSpace(run.ProviderTaskID)
	if taskName == "" {
		taskName = "video"
	}
	contentType, disposition := sanitizeVideoContentTypeAndDisposition(rawContentType, taskName+".mp4")
	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", disposition)
	c.Header("Accept-Ranges", "bytes")
	c.Status(content.HTTPStatus)
	if c.Request.Method == http.MethodHead {
		return true, nil
	}
	if _, err := io.Copy(c.Writer, content.Body); err != nil {
		return true, err
	}
	return true, nil
}

func profileContentExecutorError(err error) error {
	var executorErr *protocol.ExecutorError
	if errors.As(err, &executorErr) && executorErr.HTTPStatus >= 400 {
		body := executorErr.Body
		contentType := executorErr.ContentType
		if strings.TrimSpace(body) == "" {
			body = fmt.Sprintf(`{"error":%q}`, executorErr.Error())
			contentType = "application/json; charset=utf-8"
		}
		return &adapter.UpstreamHTTPError{
			StatusCode:  executorErr.HTTPStatus,
			ContentType: contentType,
			Body:        body,
			RetryAfter:  executorErr.RetryAfter,
		}
	}
	return err
}

func copyProfileContentHeaders(c *gin.Context, headers http.Header) {
	if c == nil {
		return
	}
	for key, values := range headers {
		canonical := http.CanonicalHeaderKey(key)
		switch canonical {
		case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailers", "Transfer-Encoding", "Upgrade", "Set-Cookie", "Set-Cookie2", "WWW-Authenticate", "Content-Security-Policy", "Content-Security-Policy-Report-Only":
			continue
		}
		if strings.HasPrefix(canonical, "Access-Control-") {
			continue
		}
		for _, value := range values {
			c.Writer.Header().Add(key, value)
		}
	}
}

func safeProfileContentRedirect(baseURL, rawLocation string) (string, error) {
	rawLocation = strings.TrimSpace(rawLocation)
	if rawLocation == "" {
		return "", errors.New("profile content redirect is missing Location")
	}
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("invalid profile content base URL: %w", err)
	}
	location, err := base.Parse(rawLocation)
	if err != nil || location.User != nil || (location.Scheme != "http" && location.Scheme != "https") {
		return "", errors.New("profile content redirect must be an absolute HTTP(S) URL without userinfo")
	}
	host := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(location.Hostname())), ".")
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") || host == "metadata.google.internal" {
		return "", fmt.Errorf("profile content redirect host %q is not public", host)
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsLinkLocalMulticast()) {
		return "", fmt.Errorf("profile content redirect host %q is not public", host)
	}
	return location.String(), nil
}

func serveProfileManagedVideo(c *gin.Context, run *db.TaskRun) (bool, error) {
	assets, err := db.ListMediaAssetsForTaskRunContext(c.Request.Context(), run.ID, asyncTaskKindVideo)
	if err != nil {
		return false, err
	}
	for _, asset := range assets {
		if asset.Status != db.MediaAssetAvailable || strings.TrimSpace(asset.ObjectID) == "" {
			continue
		}
		object, objectErr := db.GetMediaObjectByIDContext(c.Request.Context(), asset.ObjectID)
		if objectErr != nil || object.State == db.MediaObjectDeleted {
			continue
		}
		store, storeErr := getMediaObjectStore()
		if storeErr != nil {
			return true, storeErr
		}
		var byteRange *media.ByteRange
		status := http.StatusOK
		start, end := int64(0), object.ByteSize-1
		if rawRange := strings.TrimSpace(c.GetHeader("Range")); rawRange != "" {
			var rangeErr error
			byteRange, start, end, rangeErr = parseMediaRange(rawRange, object.ByteSize)
			if rangeErr != nil {
				c.Header("Content-Range", fmt.Sprintf("bytes */%d", object.ByteSize))
				return true, &adapter.UpstreamHTTPError{StatusCode: http.StatusRequestedRangeNotSatisfiable, Body: `{"error":"invalid media range"}`}
			}
			status = http.StatusPartialContent
			c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, object.ByteSize))
		}
		rawContentType := strings.TrimSpace(asset.ContentType)
		if rawContentType == "" {
			rawContentType = strings.TrimSpace(object.ContentType)
		}
		filename := strings.TrimSpace(asset.DisplayName)
		if filename == "" {
			ext := ".mp4"
			taskName := strings.TrimSpace(run.ProviderTaskID)
			if taskName == "" {
				taskName = strings.TrimSpace(asset.PublicID)
			}
			if taskName == "" {
				taskName = "video"
			}
			filename = taskName + ext
		}
		setPublicMediaSecurityHeaders(c)
		contentType, disposition := sanitizeVideoContentTypeAndDisposition(rawContentType, filename)
		c.Header("Content-Type", contentType)
		c.Header("Content-Disposition", disposition)
		c.Header("Accept-Ranges", "bytes")
		c.Header("Cache-Control", "private, no-cache")
		if etag := strings.TrimSpace(object.SHA256); etag != "" {
			c.Header("ETag", `"`+etag+`"`)
			if byteRange == nil && c.GetHeader("If-None-Match") == `"`+etag+`"` {
				c.Status(http.StatusNotModified)
				return true, nil
			}
		}
		length := object.ByteSize
		if byteRange != nil {
			length = byteRange.Length
		}
		c.Header("Content-Length", fmt.Sprintf("%d", length))
		if c.Request.Method == http.MethodHead {
			c.Status(status)
			return true, nil
		}
		reader, _, openErr := store.Open(c.Request.Context(), object.StorageKey, byteRange)
		if openErr != nil {
			return true, openErr
		}
		defer reader.Close()
		c.Status(status)
		_, copyErr := io.Copy(c.Writer, reader)
		return true, copyErr
	}
	return false, nil
}

// profileContentProfile loads the immutable revision captured by the task.
// It is kept separate from the status helper so content never dispatches a
// newly-published revision by accident.
func profileContentProfile(ctx context.Context, run *db.TaskRun) (protocol.CompiledProfile, error) {
	revision, err := db.GetProtocolProfileRevisionContext(ctx, run.ProfileID, run.ProfileRevision)
	if err != nil {
		return protocol.CompiledProfile{}, err
	}
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		return protocol.CompiledProfile{}, err
	}
	return protocol.Compile(source)
}

var safeVideoContentTypes = map[string]struct{}{
	"video/mp4":       {},
	"video/webm":      {},
	"video/ogg":       {},
	"video/quicktime": {},
}

const safeMediaContentSecurityPolicy = "default-src 'none'; sandbox allow-scripts allow-same-origin; media-src 'self' https: http: blob: data: *; img-src 'self' https: http: blob: data: *; style-src 'unsafe-inline'"

func setPublicMediaSecurityHeaders(c *gin.Context) {
	setMediaSecurityHeaders(c, "cross-origin")
	if c != nil {
		c.Writer.Header().Del("X-Frame-Options")
	}
}

func setMediaSecurityHeaders(c *gin.Context, resourcePolicy string) {
	if c == nil {
		return
	}
	c.Header("Content-Security-Policy", safeMediaContentSecurityPolicy)
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Cross-Origin-Resource-Policy", resourcePolicy)
	c.Header("Referrer-Policy", "no-referrer")
}

func sanitizeVideoContentTypeAndDisposition(rawType, filename string) (string, string) {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(rawType))
	if err == nil {
		mediaType = strings.ToLower(mediaType)
		if _, ok := safeVideoContentTypes[mediaType]; ok {
			return mediaType, fmt.Sprintf(`inline; filename=%q`, filename)
		}
	}
	if filename == "" {
		filename = "video.bin"
	}
	return "application/octet-stream", fmt.Sprintf(`attachment; filename=%q`, filename)
}
