package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/web"
)

const defaultMediaMaxBytes = int64(512 << 20)

var (
	mediaStoreMu sync.Mutex
	mediaStore   media.ObjectStore
)

// SetMediaObjectStore injects a store for tests or an alternate deployment
// backend. Passing nil restores lazy local-store initialization.
func SetMediaObjectStore(store media.ObjectStore) {
	mediaStoreMu.Lock()
	defer mediaStoreMu.Unlock()
	mediaStore = store
}

func getMediaObjectStore() (media.ObjectStore, error) {
	mediaStoreMu.Lock()
	defer mediaStoreMu.Unlock()
	if mediaStore != nil {
		return mediaStore, nil
	}
	databasePath := strings.TrimSpace(db.DatabasePath())
	if databasePath == "" {
		databasePath = config.GetDatabasePath()
	}
	root := filepath.Join(filepath.Dir(databasePath), "media")
	store, err := media.NewLocalObjectStore(root, defaultMediaMaxBytes)
	if err != nil {
		return nil, err
	}
	mediaStore = store
	return store, nil
}

func handleMediaPage(c *gin.Context) {
	serveAuthenticatedPage(c, web.MediaHTML)
}

func handleGetMediaAsset(c *gin.Context) {
	setPublicMediaSecurityHeaders(c)
	publicID := strings.TrimSpace(c.Param("public_id"))
	capability := strings.TrimSpace(c.Param("capability"))
	if idx := strings.IndexByte(capability, '.'); idx != -1 {
		capability = capability[:idx]
	}
	asset, err := db.GetMediaAssetByLink(publicID, capability)
	if errors.Is(err, db.ErrMediaAssetGone) {
		c.Status(http.StatusGone)
		return
	}
	if errors.Is(err, db.ErrMediaAssetNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		internalError(c, err, "查询媒体资源失败，请稍后重试")
		return
	}
	if asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" {
		c.Status(http.StatusNotFound)
		return
	}
	serveMediaAssetContent(c, asset)
}

func serveMediaAssetContent(c *gin.Context, asset *db.MediaAsset) {
	if asset == nil || asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" {
		c.Status(http.StatusNotFound)
		return
	}
	object, err := db.GetMediaObjectByID(asset.ObjectID)
	if err != nil || object.State == db.MediaObjectDeleted {
		c.Status(http.StatusNotFound)
		return
	}
	store, err := getMediaObjectStore()
	if err != nil {
		internalError(c, err, "媒体存储暂不可用，请稍后重试")
		return
	}
	rawContentType := strings.TrimSpace(asset.ContentType)
	if rawContentType == "" {
		rawContentType = strings.TrimSpace(object.ContentType)
	}
	contentType, isSafeMedia := sanitizeHostedMediaContentType(rawContentType)
	etag := strings.TrimSpace(asset.SHA256)
	if etag == "" {
		etag = strings.TrimSpace(object.SHA256)
	}
	if etag != "" {
		etag = `"` + etag + `"`
		c.Header("ETag", etag)
		if strings.TrimSpace(c.GetHeader("If-None-Match")) == etag {
			c.Status(http.StatusNotModified)
			return
		}
	}
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Accept-Ranges", "bytes")
	c.Header("Content-Type", contentType)
	if c.Writer.Header().Get("Cache-Control") == "" || strings.Contains(c.Writer.Header().Get("Cache-Control"), "no-store") {
		c.Header("Cache-Control", "private, no-cache")
	}
	filename := sanitizeMediaFilename(asset.DisplayName)
	if filename == "" {
		ext := ".bin"
		if strings.Contains(contentType, "video/mp4") || asset.Kind == "video" {
			ext = ".mp4"
		} else if strings.Contains(contentType, "image/png") {
			ext = ".png"
		} else if strings.Contains(contentType, "image/jpeg") {
			ext = ".jpg"
		} else if strings.Contains(contentType, "image/webp") {
			ext = ".webp"
		}
		filename = "media" + ext
	}
	if isSafeMedia {
		c.Header("Content-Disposition", fmt.Sprintf(`inline; filename=%q`, filename))
	} else {
		c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, filename))
	}
	if etag := strings.TrimSpace(object.SHA256); etag != "" {
		c.Header("ETag", `"`+etag+`"`)
		if rangeHeader := strings.TrimSpace(c.GetHeader("Range")); rangeHeader == "" && c.GetHeader("If-None-Match") == `"`+etag+`"` {
			c.Status(http.StatusNotModified)
			return
		}
	}

	var byteRange *media.ByteRange
	status := http.StatusOK
	start, end := int64(0), object.ByteSize-1
	if rangeHeader := strings.TrimSpace(c.GetHeader("Range")); rangeHeader != "" {
		var rangeErr error
		byteRange, start, end, rangeErr = parseMediaRange(rangeHeader, object.ByteSize)
		if rangeErr != nil {
			c.Header("Content-Range", fmt.Sprintf("bytes */%d", object.ByteSize))
			c.Status(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		status = http.StatusPartialContent
		c.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, object.ByteSize))
	}
	length := object.ByteSize
	if byteRange != nil {
		length = byteRange.Length
	}
	if length < 0 {
		length = 0
	}
	c.Header("Content-Length", strconv.FormatInt(length, 10))
	if c.Request.Method == http.MethodHead {
		c.Status(status)
		return
	}
	// Re-check the lifecycle immediately before opening the backing object.
	// Deletion can be requested while headers and ranges are being prepared;
	// do not start a new stream from that stale snapshot.
	latest, latestErr := db.GetMediaAssetByIDContext(c.Request.Context(), asset.ID)
	if errors.Is(latestErr, db.ErrMediaAssetNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if latestErr != nil {
		internalError(c, latestErr, "查询媒体资源失败，请稍后重试")
		return
	}
	if latest.Status != db.MediaAssetAvailable || latest.ObjectID != asset.ObjectID {
		c.Status(http.StatusNotFound)
		return
	}
	reader, _, err := store.Open(c.Request.Context(), object.StorageKey, byteRange)
	if err != nil {
		if errors.Is(err, media.ErrNotFound) {
			c.Status(http.StatusNotFound)
		} else {
			internalError(c, err, "媒体文件暂不可用，请稍后重试")
		}
		return
	}
	requestCtx := c.Request.Context()
	done := make(chan struct{})
	cleanupDone := make(chan struct{})
	var closeReader sync.Once
	closeOpenReader := func() { closeReader.Do(func() { _ = reader.Close() }) }
	defer func() {
		close(done)
		<-cleanupDone
		closeOpenReader()
	}()
	go func(ctx context.Context) {
		defer close(cleanupDone)
		select {
		case <-ctx.Done():
			closeOpenReader()
		case <-done:
		}
	}(requestCtx)
	c.Status(status)
	_, _ = io.Copy(c.Writer, reader)
}

func parseMediaRange(raw string, size int64) (*media.ByteRange, int64, int64, error) {
	if size < 0 || !strings.HasPrefix(strings.ToLower(raw), "bytes=") {
		return nil, 0, 0, media.ErrInvalidRange
	}
	value := strings.TrimSpace(raw[len("bytes="):])
	if strings.Contains(value, ",") || value == "" {
		return nil, 0, 0, media.ErrInvalidRange
	}
	parts := strings.Split(value, "-")
	if len(parts) != 2 {
		return nil, 0, 0, media.ErrInvalidRange
	}
	var start, end int64
	var err error
	if strings.TrimSpace(parts[0]) == "" {
		suffix, parseErr := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if parseErr != nil || suffix <= 0 {
			return nil, 0, 0, media.ErrInvalidRange
		}
		if suffix > size {
			suffix = size
		}
		start, end = size-suffix, size-1
	} else {
		start, err = strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || start < 0 || start >= size {
			return nil, 0, 0, media.ErrInvalidRange
		}
		if strings.TrimSpace(parts[1]) == "" {
			end = size - 1
		} else {
			end, err = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil || end < start {
				return nil, 0, 0, media.ErrInvalidRange
			}
			if end >= size {
				end = size - 1
			}
		}
	}
	return &media.ByteRange{Start: start, Length: end - start + 1}, start, end, nil
}

func sanitizeMediaFilename(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = filepath.Base(value)
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == '"' || r == '\\' || r == '/' {
			return '_'
		}
		return r
	}, value)
	return strings.TrimSpace(value)
}

func mediaPublicURL(c *gin.Context, publicID, capability string) string {
	scheme := "http"
	if c.Request.TLS != nil || trustedForwardedHTTPS(c) {
		scheme = "https"
	}
	host := trustedForwardedHost(c)
	if host == "" {
		host = c.Request.Host
	}
	return fmt.Sprintf("%s://%s/v1/media/%s/%s", scheme, host, url.PathEscape(publicID), url.PathEscape(capability))
}

// mediaPublicURLForAsset recovers the fixed capability link for an available
// asset. Authenticated UI views use this instead of leaking an admin-only
// content route as though it were shareable.
func mediaPublicURLForAsset(c *gin.Context, asset *db.MediaAsset) (string, error) {
	if asset == nil || asset.Status != db.MediaAssetAvailable || strings.TrimSpace(asset.ObjectID) == "" {
		return "", db.ErrMediaAssetGone
	}
	capability, err := db.RecoverMediaAssetCapability(asset)
	if err != nil {
		return "", err
	}
	return mediaPublicURL(c, asset.PublicID, capability), nil
}

// mediaAssetAdminView is used only for authenticated management responses.
// PublicURL is reconstructed in memory from the encrypted capability and is
// never part of the persisted MediaAsset model.
type mediaAssetAdminView struct {
	db.MediaAsset
	PublicURL      string `json:"public_url,omitempty"`
	PublicURLError string `json:"public_url_error,omitempty"`
}

func newMediaAssetAdminView(c *gin.Context, asset db.MediaAsset) mediaAssetAdminView {
	view := mediaAssetAdminView{MediaAsset: asset}
	if asset.Status != db.MediaAssetAvailable {
		return view
	}
	publicURL, err := mediaPublicURLForAsset(c, &asset)
	if err == nil {
		view.PublicURL = publicURL
	} else {
		view.PublicURLError = "媒体公开链接不可恢复，未存储有效访问令牌"
	}
	return view
}

func handleListMediaAssets(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	offset, _ := strconv.Atoi(c.Query("offset"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	kind, status := strings.TrimSpace(c.Query("kind")), strings.TrimSpace(c.Query("status"))
	search := strings.TrimSpace(c.Query("q"))
	if search == "" {
		search = strings.TrimSpace(c.Query("search"))
	}
	assets, err := db.ListMediaAssetsFilteredSearchContext(c.Request.Context(), limit, offset, kind, status, search)
	if err != nil {
		internalError(c, err, "查询媒体资源列表失败，请稍后重试")
		return
	}
	views := make([]mediaAssetAdminView, 0, len(assets))
	for _, asset := range assets {
		views = append(views, newMediaAssetAdminView(c, asset))
	}
	c.JSON(http.StatusOK, gin.H{"data": views, "limit": limit, "offset": offset, "kind": kind, "status": status, "search": search})
}

func handleGetMediaAssetAdmin(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "媒体资源 ID 无效"})
		return
	}
	asset, err := db.GetMediaAssetByID(uint(id))
	if errors.Is(err, db.ErrMediaAssetNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		internalError(c, err, "查询媒体资源失败，请稍后重试")
		return
	}
	c.JSON(http.StatusOK, newMediaAssetAdminView(c, *asset))
}

// handleGetMediaAssetLink returns the existing public URL without changing its
// capability. Reads must never mutate or invalidate a URL a caller may already have retained.
func handleGetMediaAssetLink(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "媒体资源 ID 无效"})
		return
	}
	asset, err := db.GetMediaAssetByIDContext(c.Request.Context(), uint(id))
	if errors.Is(err, db.ErrMediaAssetNotFound) {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	if err != nil {
		internalError(c, err, "查询媒体资源失败，请稍后重试")
		return
	}
	switch asset.Status {
	case db.MediaAssetDeleted, db.MediaAssetDeleteRequested, db.MediaAssetDeleteFailed, db.MediaAssetExpired:
		c.AbortWithStatus(http.StatusGone)
		return
	}
	publicURL, err := mediaPublicURLForAsset(c, asset)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "媒体公开链接不可恢复，未存储有效访问令牌"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": asset.ID, "public_id": asset.PublicID, "url": publicURL})
}

func handleGetMediaAssetAdminContent(c *gin.Context) {
	setMediaSecurityHeaders(c, "same-origin")
	c.Header("Cache-Control", "private, no-cache")
	idParam := strings.TrimSuffix(strings.TrimSpace(c.Param("id")), ".mp4")
	id, err := strconv.ParseUint(idParam, 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "媒体资源 ID 无效"})
		return
	}
	asset, err := db.GetMediaAssetByIDContext(c.Request.Context(), uint(id))
	if errors.Is(err, db.ErrMediaAssetNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		internalError(c, err, "查询媒体资源失败，请稍后重试")
		return
	}
	serveMediaAssetContent(c, asset)
}

func handleDeleteMediaAsset(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "媒体资源 ID 无效"})
		return
	}
	storageKey, shouldDeleteFile, err := db.HardDeleteMediaAssetByIDContext(c.Request.Context(), uint(id))
	if err != nil {
		if errors.Is(err, db.ErrMediaAssetNotFound) {
			c.JSON(http.StatusAccepted, gin.H{"status": "deleted", "id": id})
			return
		}
		internalError(c, err, "删除媒体资源失败，请稍后重试")
		return
	}
	if shouldDeleteFile && storageKey != "" {
		if store, storeErr := getMediaObjectStore(); storeErr == nil && store != nil {
			_ = store.Delete(c.Request.Context(), storageKey)
		}
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "deleted", "id": id})
}

func handleRetryMediaAsset(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "媒体资源 ID 无效"})
		return
	}
	asset, err := db.GetMediaAssetByID(uint(id))
	if errors.Is(err, db.ErrMediaAssetNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		internalError(c, err, "查询媒体资源失败，请稍后重试")
		return
	}
	status := db.MediaAssetMaterializing
	switch asset.Status {
	case db.MediaAssetDeleteRequested, db.MediaAssetDeleteFailed:
		if err := db.RetryMediaAssetDeletion(uint(id)); err != nil {
			if errors.Is(err, db.ErrMediaAssetNotFound) {
				c.JSON(http.StatusConflict, gin.H{"error": "当前媒体资源不能重试"})
				return
			}
			internalError(c, err, "重试删除媒体资源失败，请稍后重试")
			return
		}
		status = db.MediaAssetDeleteRequested
	case db.MediaAssetFailed, db.MediaAssetPending, db.MediaAssetMaterializing:
		if err := db.RetryMediaAssetMaterialization(uint(id)); err != nil {
			if errors.Is(err, db.ErrMediaAssetNotFound) {
				c.JSON(http.StatusConflict, gin.H{"error": "当前媒体资源不能重试"})
				return
			}
			internalError(c, err, "重试处理媒体资源失败，请稍后重试")
			return
		}
	default:
		c.JSON(http.StatusConflict, gin.H{"error": "当前媒体资源不能重试", "status": asset.Status})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": status, "id": id})
}

var safeHostedMediaContentTypes = map[string]struct{}{
	"image/jpeg":      {},
	"image/png":       {},
	"image/webp":      {},
	"image/gif":       {},
	"image/avif":      {},
	"image/apng":      {},
	"video/mp4":       {},
	"video/webm":      {},
	"video/ogg":       {},
	"video/quicktime": {},
	"audio/mpeg":      {},
	"audio/mp4":       {},
	"audio/ogg":       {},
	"audio/wav":       {},
	"audio/webm":      {},
}

func sanitizeHostedMediaContentType(raw string) (string, bool) {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(raw))
	if err == nil {
		mediaType = strings.ToLower(mediaType)
		if _, ok := safeHostedMediaContentTypes[mediaType]; ok {
			return mediaType, true
		}
	}
	return "application/octet-stream", false
}
