package router

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/adapter"
	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/profilebootstrap"
	"relay-gateway/protocol"
	"relay-gateway/security"
	"relay-gateway/service"
	"relay-gateway/web"
)

func Setup() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// Metrics runs before the bounded request slot so rejected overloads are
	// included in status/error counters as well as normal requests.
	r.Use(gin.Logger(), metricsMiddleware(), requestConcurrencyMiddleware(), requestLimitMiddleware(), securityHeadersMiddleware(), corsMiddleware(), auditMiddleware(), gin.Recovery())

	r.GET("/", handleRootPage)
	r.GET("/setup", handleSetupPage)
	r.GET("/login", handleLoginPage)
	r.GET("/dashboard", handleDashboardPage)
	r.GET("/logs", handleLogsPage)
	r.GET("/media", handleMediaPage)
	r.GET("/profiles", handleProfilesPage)
	r.GET("/assets/app.css", func(c *gin.Context) { c.Data(http.StatusOK, "text/css; charset=utf-8", web.AppCSS) })
	r.GET("/assets/app.js", func(c *gin.Context) { c.Data(http.StatusOK, "application/javascript; charset=utf-8", web.AppJS) })

	r.GET("/health", func(c *gin.Context) {
		status := "ok"
		if db.DB != nil {
			if sqlDB, err := db.DB.DB(); err != nil || sqlDB.Ping() != nil {
				status = "degraded"
			}
		}
		c.JSON(http.StatusOK, gin.H{"status": status, "service": "relay-gateway"})
	})

	authAPI := r.Group("/api/auth")
	{
		authAPI.GET("/status", handleAuthStatus)
		authAPI.POST("/setup", handleSetupAdmin)
		authAPI.POST("/login", handleLogin)
	}

	r.GET("/api/media-proxy", handleMediaProxy)
	r.GET("/api/playground/video-content/:id", handleGetVideoContent)
	r.HEAD("/api/playground/video-content/:id", handleGetVideoContent)

	apiGroup := r.Group("/api", adminSessionMiddleware(), csrfMiddleware())
	{
		apiGroup.GET("/auth/me", handleAuthMe)
		apiGroup.POST("/auth/logout", handleLogout)
		apiGroup.PUT("/account/credentials", handleUpdateCredentials)
		apiGroup.POST("/gateway-token/rotate", handleRotateGatewayToken)
		apiGroup.GET("/adapter-types", handleGetAdapterTypes)
		apiGroup.GET("/channels", handleGetChannels)
		apiGroup.POST("/channels", handleSaveChannel)
		apiGroup.PATCH("/channels/:id/toggle", handleToggleChannel)
		apiGroup.DELETE("/channels/:id", handleDeleteChannel)
		apiGroup.POST("/channels/:id/test", handleTestChannel)
		apiGroup.POST("/channels/test-all", handleTestAllChannels)
		apiGroup.GET("/channels/:id/models", handleGetChannelModels)
		apiGroup.POST("/channels/probe-models", handleProbeModels)
		apiGroup.POST("/playground/run", handlePlaygroundRun)
		apiGroup.POST("/playground/chat", handlePlaygroundChat)
		apiGroup.GET("/playground/image-status", handlePlaygroundImageStatus)
		apiGroup.GET("/playground/video-status", handlePlaygroundVideoStatus)
		apiGroup.GET("/settings", handleGetSettings)
		apiGroup.POST("/settings", handleSaveSettings)
		apiGroup.GET("/logs", handleGetLogs)
		apiGroup.GET("/logs/:id", handleGetLogDetail)
		apiGroup.DELETE("/logs", handleDeleteLogs)
		apiGroup.GET("/media-assets", handleListMediaAssets)
		apiGroup.GET("/media-assets/:id", handleGetMediaAssetAdmin)
		apiGroup.GET("/media-assets/:id/link", handleGetMediaAssetLink)
		apiGroup.GET("/media-assets/:id/content", handleGetMediaAssetAdminContent)
		apiGroup.HEAD("/media-assets/:id/content", handleGetMediaAssetAdminContent)
		apiGroup.GET("/media-assets/:id/content.mp4", handleGetMediaAssetAdminContent)
		apiGroup.HEAD("/media-assets/:id/content.mp4", handleGetMediaAssetAdminContent)
		apiGroup.DELETE("/media-assets/:id", handleDeleteMediaAsset)
		apiGroup.POST("/media-assets/:id/retry", handleRetryMediaAsset)
		apiGroup.GET("/profiles", handleListProtocolProfiles)
		apiGroup.GET("/profiles/:id", handleGetProtocolProfile)
		apiGroup.GET("/profiles/:id/revisions/:revision/references", handleGetProtocolProfileRevisionReferences)
		apiGroup.GET("/profiles/:id/revisions/:revision/diff", handleGetProtocolProfileRevisionDiff)
		apiGroup.POST("/profiles", handleCreateProtocolProfile)
		apiGroup.POST("/profiles/:id/revisions", handleCreateProtocolProfileRevision)
		apiGroup.PUT("/profiles/:id/revisions/:revision", handleUpdateProtocolProfileRevision)
		apiGroup.POST("/profiles/:id/revisions/:revision/publish", handlePublishProtocolProfileRevision)
		apiGroup.POST("/profiles/:id/revisions/:revision/retire", handleRetireProtocolProfileRevision)
		apiGroup.DELETE("/profiles/:id/revisions/:revision", handleDeleteProtocolProfileRevision)
		apiGroup.DELETE("/profiles/:id", handleDeleteProtocolProfile)
		apiGroup.POST("/channels/:id/bind-preset", handleReconcileChannelBindings)
		apiGroup.POST("/channels/:id/bind-profile", handleBindChannelProfile)
		apiGroup.GET("/profile-bindings", handleListChannelProtocolBindings)
		apiGroup.GET("/profile-bindings/:id", handleGetChannelProtocolBinding)
		apiGroup.POST("/profile-bindings", handleCreateChannelProtocolBinding)
		apiGroup.PUT("/profile-bindings/:id", handleUpdateChannelProtocolBinding)
		apiGroup.PATCH("/profile-bindings/:id/toggle", handleToggleChannelProtocolBinding)
		apiGroup.DELETE("/profile-bindings/:id", handleDeleteChannelProtocolBinding)
		apiGroup.GET("/migration/channels", handleChannelsMigrationReport)
		apiGroup.GET("/metrics", handleMetrics)
	}

	// Video content is intentionally a public playback resource so the stable
	// video_url can be used by a browser or a plain <video src>. Task creation,
	// status and every other /v1 API route remain protected by the gateway key.
	v1Public := r.Group("/v1")
	registerPublicVideoContentRoutes(v1Public)
	v1Public.GET("/media/:public_id/:capability", handleGetMediaAsset)
	v1Public.HEAD("/media/:public_id/:capability", handleGetMediaAsset)
	v1Public.GET("/media/:public_id/:capability/*filename", handleGetMediaAsset)
	v1Public.HEAD("/media/:public_id/:capability/*filename", handleGetMediaAsset)
	registerV1Routes(r.Group("/v1", gatewayAuthMiddleware()))

	return r
}

const maxConcurrentRequests = 512

var requestSlots = make(chan struct{}, maxConcurrentRequests)

// requestConcurrencyMiddleware bounds memory, goroutines and upstream work
// under overload. Streaming handlers hold a slot for the lifetime of the
// response, which is intentional: otherwise a large number of slow streams
// could still exhaust file descriptors and connection pools.
func requestConcurrencyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		select {
		case requestSlots <- struct{}{}:
			defer func() { <-requestSlots }()
			c.Next()
		default:
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "服务器繁忙，请稍后重试"})
		}
	}
}

// extractBearerToken 从请求标头中提取 Bearer Token 或 x-api-key（共享鉴权逻辑）
func extractBearerToken(c *gin.Context) string {
	auth := strings.TrimSpace(c.Request.Header.Get("Authorization"))
	token := auth
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		token = strings.TrimSpace(auth[7:])
	}
	if token == "" {
		token = strings.TrimSpace(c.Request.Header.Get("x-api-key"))
	}
	return token
}

func registerV1Routes(group *gin.RouterGroup) {
	group.GET("/models", handleModels)
	group.POST("/models/refresh", handleRefreshModels)
	group.POST("/chat/completions", handleChatCompletions)
	group.POST("/responses", handleResponses)
	group.POST("/messages", handleAnthropicMessages)
	group.POST("/messages/count_tokens", handleAnthropicCountTokens)
	group.POST("/moderations", handleModerations)
	group.POST("/embeddings", handleEmbeddings)
	group.POST("/images/generations", handleImagesGenerations)
	group.POST("/images/edits", handleImagesEdits)
	group.POST("/images/jobs", handleCreateImageJob)
	group.GET("/images/jobs/:id", handleGetImageJob)
	group.POST("/audio/speech", handleAudioSpeech)
	group.POST("/audio/transcriptions", handleAudioTranscriptions)
	group.POST("/audio/translations", handleAudioTranslations)
	group.POST("/videos", handleCreateVideo)
	group.POST("/videos/generations", handleCreateVideo)
	group.POST("/video/generations", handleCreateVideo)
	group.GET("/videos/:id", handleGetVideo)
	group.GET("/videos/generations/:id", handleGetVideo)
	group.GET("/video/generations/:id", handleGetVideo)
}

// registerPublicVideoContentRoutes exposes only the mapped video bytes. The
// content handler still requires a durable task mapping and resolves the
// channel from that mapping; callers cannot select an upstream with a query
// parameter. This is deliberately separate from the authenticated /v1 group
// because browsers cannot attach an Authorization header to a plain media
// element URL.
func registerPublicVideoContentRoutes(group *gin.RouterGroup) {
	group.GET("/videos/:id/content", handleGetVideoContent)
	group.HEAD("/videos/:id/content", handleGetVideoContent)
	group.GET("/videos/:id/content.mp4", handleGetVideoContent)
	group.HEAD("/videos/:id/content.mp4", handleGetVideoContent)
	group.GET("/videos/generations/:id/content", handleGetVideoContent)
	group.HEAD("/videos/generations/:id/content", handleGetVideoContent)
	group.GET("/videos/generations/:id/content.mp4", handleGetVideoContent)
	group.HEAD("/videos/generations/:id/content.mp4", handleGetVideoContent)
	group.GET("/video/generations/:id/content", handleGetVideoContent)
	group.HEAD("/video/generations/:id/content", handleGetVideoContent)
	group.GET("/video/generations/:id/content.mp4", handleGetVideoContent)
	group.HEAD("/video/generations/:id/content.mp4", handleGetVideoContent)
}

// -------------------------------------------------------------
// 管理 API Handlers
// -------------------------------------------------------------

func handleGetAdapterTypes(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"types": availableChannelTypeMetasContext(c.Request.Context()),
	})
}

// isChannelTypePublished returns true if the channel type has a published profile
// or is not bound to a retired built-in profile.
func isChannelTypePublished(ctx context.Context, channelType string) bool {
	channelType = strings.ToLower(strings.TrimSpace(channelType))
	profileID := profilebootstrap.BuiltinProfileID(channelType)
	database := db.DBForContext(ctx)
	if database == nil {
		return true
	}
	var profile db.ProtocolProfile
	if err := database.WithContext(ctx).Where("id = ?", profileID).First(&profile).Error; err != nil {
		return true
	}
	var count int64
	if err := database.WithContext(ctx).Model(&db.ProtocolProfileRevision{}).
		Where("profile_id = ? AND state = ?", profileID, db.ProfileRevisionPublished).
		Count(&count).Error; err != nil {
		return true
	}
	return count > 0
}

// availableChannelTypeMetasContext keeps the public channel picker compatible with
// built-in profiles and suppresses types whose profile revisions have been retired.
func availableChannelTypeMetasContext(ctx context.Context) []adapter.AdapterMeta {
	metas := adapter.ListMetas()
	filtered := make([]adapter.AdapterMeta, 0, len(metas))
	for _, meta := range metas {
		if isChannelTypePublished(ctx, meta.Type) {
			filtered = append(filtered, meta)
		}
	}
	return filtered
}

func availableChannelTypeMetas() []adapter.AdapterMeta {
	return availableChannelTypeMetasContext(context.Background())
}

func handleGetChannels(c *gin.Context) {
	cms, err := db.GetAllChannelModels()
	if err != nil {
		internalError(c, err, "读取渠道列表失败，请稍后重试")
		return
	}
	channels := make([]map[string]interface{}, 0, len(cms))
	for _, cm := range cms {
		channels = append(channels, sanitizedChannelModel(cm))
	}
	c.JSON(http.StatusOK, gin.H{
		"channels": channels,
		"port":     config.GetPort(),
	})
}

// maskSecret keeps enough shape for the dashboard to show that a credential is
// configured without returning a reusable credential to an authenticated
// browser session.  The save handlers recognize this exact prefix and retain
// the stored value unless the caller explicitly supplies a replacement.
func maskSecret(secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return ""
	}
	if len(secret) <= 4 {
		return "********"
	}
	return "********" + secret[len(secret)-4:]
}

func isMaskedSecret(secret string) bool {
	return strings.HasPrefix(strings.TrimSpace(secret), "********")
}

func maskSecretList(raw string) string {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == ',' })
	masked := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := maskSecret(part); value != "" {
			masked = append(masked, value)
		}
	}
	return strings.Join(masked, "\n")
}

func isSensitiveHeaderName(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, marker := range []string{"authorization", "api-key", "apikey", "token", "secret", "password", "cookie", "credential"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

func sanitizeHeadersRaw(raw string) string {
	var headers map[string]string
	if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &headers) != nil || headers == nil {
		return ""
	}
	for name, value := range headers {
		if isSensitiveHeaderName(name) && strings.TrimSpace(value) != "" {
			headers[name] = "********"
		}
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func sanitizedChannelModel(cm db.ChannelModel) map[string]interface{} {
	encoded, _ := json.Marshal(cm)
	var result map[string]interface{}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return map[string]interface{}{"id": cm.ID, "name": cm.Name, "type": cm.Type}
	}
	result["api_key"] = maskSecret(cm.APIKey)
	result["api_keys_raw"] = maskSecretList(cm.APIKeysRaw)
	result["headers_raw"] = sanitizeHeadersRaw(cm.HeadersRaw)
	delete(result, "last_error_message")
	delete(result, "api_keys")
	result["has_api_key"] = strings.TrimSpace(cm.APIKey) != "" || strings.TrimSpace(cm.APIKeysRaw) != ""
	keyCount := len(strings.FieldsFunc(cm.APIKeysRaw, func(r rune) bool { return r == '\n' || r == ',' }))
	if keyCount == 0 && strings.TrimSpace(cm.APIKey) != "" {
		keyCount = 1
	}
	result["api_key_count"] = keyCount
	return result
}

func preserveMaskedChannelSecrets(ctx context.Context, cm *db.ChannelModel) error {
	if db.DB == nil || cm == nil {
		return nil
	}
	existing, err := db.GetChannelModelContext(ctx, cm.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return fmt.Errorf("cannot load existing channel credentials: %w", err)
	}
	if isMaskedSecret(cm.APIKey) {
		cm.APIKey = existing.APIKey
	}
	if strings.Contains(cm.APIKeysRaw, "********") {
		cm.APIKeysRaw = existing.APIKeysRaw
		cm.APIKeys = append([]string(nil), existing.APIKeys...)
	}
	if strings.Contains(cm.HeadersRaw, "********") {
		var incoming, existingHeaders map[string]string
		if json.Unmarshal([]byte(cm.HeadersRaw), &incoming) != nil || json.Unmarshal([]byte(existing.HeadersRaw), &existingHeaders) != nil {
			return errors.New("invalid masked headers")
		}
		for name, value := range incoming {
			if isSensitiveHeaderName(name) && strings.TrimSpace(value) == "********" {
				if old, ok := existingHeaders[name]; ok {
					incoming[name] = old
				} else {
					delete(incoming, name)
				}
			}
		}
		encoded, marshalErr := json.Marshal(incoming)
		if marshalErr != nil {
			return marshalErr
		}
		cm.HeadersRaw = string(encoded)
	}
	return nil
}

func handleSaveChannel(c *gin.Context) {
	var cm db.ChannelModel
	if err := c.ShouldBindJSON(&cm); err != nil {
		requestError(c, err, "渠道配置格式不正确")
		return
	}
	cm.ID = strings.TrimSpace(cm.ID)
	cm.Type = strings.ToLower(strings.TrimSpace(cm.Type))
	cm.BaseURL = normalizeBaseURLScheme(cm.BaseURL)
	if _, err := protocol.BuiltinPreset(cm.Type); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("不支持的渠道协议，可选：%s", registeredAdapterTypes())})
		return
	}
	if cm.BaseURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Base URL 不能为空"})
		return
	}
	if cm.ID == "" {
		generatedID, err := generateUniqueChannelID(c.Request.Context(), cm.Type)
		if err != nil {
			internalError(c, err, "生成渠道 ID 失败，请稍后重试")
			return
		}
		cm.ID = generatedID
	}
	_, lookupErr := db.GetChannelModelContext(c.Request.Context(), cm.ID)
	newChannel := errors.Is(lookupErr, gorm.ErrRecordNotFound)
	if lookupErr != nil && !newChannel {
		internalError(c, lookupErr, "查询渠道失败，请稍后重试")
		return
	}
	parsedURL, err := url.Parse(cm.BaseURL)
	if err != nil || parsedURL.Host == "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Base URL 必须是有效的 HTTP 或 HTTPS 地址"})
		return
	}
	if err := isSafeProbeURL(cm.BaseURL); err != nil {
		requestError(c, err, "Base URL 不允许访问，请检查地址")
		return
	}
	if err := preserveMaskedChannelSecrets(c.Request.Context(), &cm); err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusConflict, gin.H{"error": "无法保留已有渠道凭据，请检查数据库加密密钥配置"})
		return
	}

	previousType := ""
	if !newChannel {
		existing, err := db.GetChannelModelContext(c.Request.Context(), cm.ID)
		if err != nil {
			_ = c.Error(err)
			internalError(c, err, "查询渠道失败，请稍后重试")
			return
		}
		previousType = existing.Type
	}
	if (newChannel || !strings.EqualFold(strings.TrimSpace(previousType), cm.Type)) && !isChannelTypePublished(c.Request.Context(), cm.Type) {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("渠道协议 %q 已停用，请先发布对应的 Profile", cm.Type)})
		return
	}

	if err := saveChannelWithProfileBindings(c.Request.Context(), &cm, previousType, newChannel); err != nil {
		_ = c.Error(err)
		requestError(c, err, "保存渠道失败，请检查配置后重试")
		return
	}
	c.Set("changed_channel_id", cm.ID)

	c.JSON(http.StatusOK, gin.H{"status": "ok", "channel": sanitizedChannelModel(cm)})
}

// saveChannelWithProfileBindings keeps a channel type change and its automatic
// bindings atomic. The admin mutation middleware already supplies a request
// transaction; direct callers and focused tests get an equivalent transaction
// here. TaskRuns and profile revisions are never changed by this operation.
func saveChannelWithProfileBindings(ctx context.Context, channel *db.ChannelModel, previousType string, newChannel bool) error {
	if channel == nil {
		return errors.New("channel is required")
	}
	previousType = strings.ToLower(strings.TrimSpace(previousType))
	channel.Type = strings.ToLower(strings.TrimSpace(channel.Type))
	typeChanged := !newChannel && previousType != channel.Type

	save := func(txCtx context.Context) error {
		if (newChannel || typeChanged) && !isChannelTypePublished(txCtx, channel.Type) {
			return fmt.Errorf("cannot use retired adapter type %q; publish its profile first", channel.Type)
		}
		if err := db.SaveChannelModelContext(txCtx, channel); err != nil {
			return err
		}
		if typeChanged {
			if err := removeDefaultProfileBindingsForType(txCtx, previousType, channel.ID); err != nil {
				return err
			}
		}
		if newChannel || typeChanged {
			if err := ensureDefaultProfileBindings(txCtx, channel.Type, channel.ID); err != nil {
				return err
			}
		}
		return nil
	}

	database := db.DBForContext(ctx)
	if database == nil {
		return errors.New("database is not initialized")
	}
	if database != db.DB {
		return save(ctx)
	}
	return database.Transaction(func(tx *gorm.DB) error {
		return save(db.WithTx(ctx, tx))
	})
}

func registeredAdapterTypes() string {
	metas := availableChannelTypeMetas()
	types := make([]string, 0, len(metas))
	for _, meta := range metas {
		types = append(types, meta.Type)
	}
	return strings.Join(types, ", ")
}

func generateUniqueChannelID(ctx context.Context, adapterType string) (string, error) {
	return generateUniqueChannelIDWithContextAndReader(ctx, adapterType, rand.Reader)
}

func generateUniqueChannelIDWithContextAndReader(ctx context.Context, adapterType string, random io.Reader) (string, error) {
	encoding := base32.StdEncoding.WithPadding(base32.NoPadding)
	conn := db.DBForContext(ctx)
	if conn == nil {
		conn = db.DB
	}
	for attempt := 0; attempt < 8; attempt++ {
		randomBytes := make([]byte, 5)
		if _, err := io.ReadFull(random, randomBytes); err != nil {
			return "", fmt.Errorf("generate channel ID: %w", err)
		}
		id := strings.ToLower(strings.TrimSpace(adapterType)) + "-" + strings.ToLower(encoding.EncodeToString(randomBytes))
		var count int64
		if err := conn.Model(&db.ChannelModel{}).Where("id = ?", id).Count(&count).Error; err != nil {
			return "", fmt.Errorf("check channel ID: %w", err)
		}
		if count == 0 {
			return id, nil
		}
	}
	return "", errors.New("could not allocate a unique channel ID")
}

func handleToggleChannel(c *gin.Context) {
	id := c.Param("id")
	enabled, err := db.ToggleChannelContext(c.Request.Context(), id)
	if err != nil {
		_ = c.Error(err)
		internalError(c, err, "切换渠道状态失败，请稍后重试")
		return
	}
	c.Set("changed_channel_id", id)

	c.JSON(http.StatusOK, gin.H{"status": "ok", "enabled": enabled})
}

func handleDeleteChannel(c *gin.Context) {
	id := c.Param("id")
	if err := db.DeleteChannelContext(c.Request.Context(), id); err != nil {
		_ = c.Error(err)
		internalError(c, err, "删除渠道失败，请稍后重试")
		return
	}
	c.Set("changed_channel_id", id)
	c.Set("deleted_channel_id", id)
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// persistChannelHealth records a probe result and its audit event in one short
// SQLite transaction. The upstream request itself must stay outside this
// transaction: holding the single SQLite writer while waiting on a provider
// would block unrelated admin requests for the whole probe timeout.
func persistChannelHealth(c *gin.Context, channelID, status string, latencyMs int, errMsg string, models []string) error {
	if db.DB == nil {
		return errors.New("database is not initialized")
	}
	return db.DB.Transaction(func(tx *gorm.DB) error {
		if err := db.UpdateChannelHealthContext(db.WithTx(c.Request.Context(), tx), channelID, status, latencyMs, errMsg, models); err != nil {
			return err
		}
		if entry := audit.FromContext(c.Request.Context()); entry != nil {
			return entry.AddEventWithDB(tx, "channel_health_updated", audit.EventData{
				ChannelID:  channelID,
				StatusCode: http.StatusOK,
				Message:    status,
				Data: map[string]any{
					"latency_ms":  latencyMs,
					"status":      status,
					"error":       chineseErrorMessage(errors.New(errMsg), "渠道测试失败，请检查地址和密钥后重试"),
					"model_count": len(models),
				},
			})
		}
		return nil
	})
}

const (
	asyncTaskKindVideo = "video"
	asyncTaskKindImage = "image"
	imageTaskIDPrefix  = db.ImageTaskMappingLookupPrefix
)

// These seams keep asynchronous task-routing recovery paths testable. They
// are deliberately scoped to this package; production always uses the
// concrete persistence and audit implementations below.
var (
	persistAsyncTaskMappingsFn          = persistAsyncTaskMappings
	persistVideoTaskAliasRegistrationFn = persistAsyncTaskMappingRegistration
	persistImageTaskAliasRegistrationFn = persistAsyncTaskMappingRegistration
	recordAsyncTaskCreatedFn            = audit.RecordAsyncTaskCreated
	recordVideoTaskConflictAliasFn      = recordVideoTaskConflictAlias
	enqueueAsyncTaskMappingRecoveryFn   = enqueueAsyncTaskMappingRecovery
)

func enqueueAsyncTaskMappingRecovery(reg asyncTaskMappingRegistration) (bool, error) {
	return asyncTaskMappingRecoveries.Enqueue(reg)
}

func recordVideoTaskConflictAlias(ctx context.Context, lookupTaskID, realProviderID string) error {
	lookupMapping := db.GetTaskMappingForKind(lookupTaskID, asyncTaskKindVideo)
	if lookupMapping == nil {
		return fmt.Errorf("video task [%s] is not registered", lookupTaskID)
	}
	var errs []error
	if lookupMapping.TaskAlias == "" || lookupMapping.TaskAlias == lookupTaskID {
		lookupMapping.TaskAlias = realProviderID
		if err := db.RecordTaskMappingContext(ctx, *lookupMapping); err != nil {
			errs = append(errs, fmt.Errorf("record task mapping alias: %w", err))
		}
	}
	if run, err := db.GetTaskRunByAlias(lookupTaskID); err == nil && run != nil {
		if run.ProviderTaskID == "" || run.ProviderTaskID == lookupTaskID {
			if err := db.UpdateTaskRunProviderTaskIDContext(ctx, run.ID, realProviderID); err != nil {
				errs = append(errs, fmt.Errorf("update task run provider id: %w", err))
			}
		}
	}
	return errors.Join(errs...)
}

// persistAsyncTaskMappings records every lookup alias returned by an async
// create call. All aliases point to the same originating request so subsequent
// successful status polls can be folded back into that request's audit row.
// The upstream request has already completed before this helper runs, so
// SQLite is never held while waiting on a provider. Cache publication is
// deferred until the transaction commits.
func persistAsyncTaskMappings(c *gin.Context, channelID, taskKind, taskAlias, initialStatus string, taskIDs ...string) error {
	if c == nil {
		return nil
	}
	registration := newAsyncTaskMappingRegistration(c.Request.Context(), channelID, taskKind, taskAlias, initialStatus, taskIDs...)
	if registration.empty() {
		return nil
	}
	if err := persistAsyncTaskMappingRegistration(registration); err != nil {
		return err
	}
	// An audit summary is useful observability, but it is not part of task
	// routing.  In particular, an event-log write must never roll back a
	// mapping after the upstream has already accepted a paid create request.
	if registration.OriginRequestID != "" {
		if err := recordAsyncTaskCreatedFn(c.Request.Context(), registration.TaskAlias, registration.TaskKind, registration.InitialStatus); err != nil {
			log.Printf("[ASYNC_TASK] mapping for %s persisted but creation audit summary could not be recorded: %v", registration.TaskAlias, err)
		}
	}
	ensureLegacyTaskRun(registration)
	return nil
}

// registerAsyncTaskMappings is the safe boundary immediately after an
// upstream async-create call succeeds. A local routing write may be retried or
// deferred, but it must never turn that successful upstream creation into a
// false 500 that encourages the caller to submit (and pay for) a duplicate.
// It returns only a cross-channel conflict. Other local persistence failures
// are made recoverable by the durable journal and do not replace the upstream
// success response.
func registerAsyncTaskMappings(c *gin.Context, channelID, taskKind, taskAlias, initialStatus string, taskIDs ...string) error {
	if c == nil {
		return nil
	}
	registration := newAsyncTaskMappingRegistration(c.Request.Context(), channelID, taskKind, taskAlias, initialStatus, taskIDs...)
	if registration.empty() {
		return nil
	}
	if err := persistAsyncTaskMappingsFn(c, channelID, taskKind, taskAlias, initialStatus, taskIDs...); err != nil {
		deferAsyncTaskMappingPersistence(c, registration, err)
		if errors.Is(err, db.ErrTaskMappingChannelConflict) {
			return err
		}
	}
	return nil
}

// persistVideoTaskAliases records IDs that a provider reveals only during a
// later status poll. It deliberately preserves the original task mapping's
// audit provenance instead of treating the poll as a second task creation.
//
// The returned registration is retained on a transient local write failure so
// the caller can journal it without replacing a successful upstream status
// response with a false 500.
func persistVideoTaskAliases(c *gin.Context, channelID, lookupTaskID string, resp *model.VideoTaskResponse) (asyncTaskMappingRegistration, error) {
	var registration asyncTaskMappingRegistration
	if c == nil || resp == nil {
		return registration, nil
	}
	channelID = strings.TrimSpace(channelID)
	lookupTaskID = strings.TrimSpace(lookupTaskID)
	if channelID == "" || lookupTaskID == "" {
		return registration, errors.New("video task alias is missing its channel or lookup ID")
	}
	lookupMapping := db.GetTaskMappingForKind(lookupTaskID, asyncTaskKindVideo)
	if lookupMapping == nil {
		return registration, fmt.Errorf("video task [%s] is not registered", lookupTaskID)
	}
	if lookupMapping.ChannelID != channelID {
		return registration, fmt.Errorf("%w: video task [%s]", db.ErrTaskMappingChannelConflict, lookupTaskID)
	}

	responseAlias, responseIDs := collectVideoTaskIDs(resp)
	if len(responseIDs) == 0 {
		return registration, nil
	}
	taskAlias := lookupMapping.TaskAlias
	if taskAlias == "" {
		taskAlias = responseAlias
	}
	if taskAlias == "" {
		taskAlias = lookupTaskID
	}
	taskKind := lookupMapping.TaskKind
	if taskKind == "" {
		taskKind = asyncTaskKindVideo
	}

	pending := make([]string, 0, len(responseIDs))
	for _, taskID := range responseIDs {
		taskID = strings.TrimSpace(taskID)
		if !db.IsValidTaskID(taskID) {
			continue
		}
		if isTaskIDRegisteredToOtherChannel(taskID, asyncTaskKindVideo, channelID) {
			return registration, fmt.Errorf("%w: video task alias [%s]", db.ErrTaskMappingChannelConflict, taskID)
		}
		if existing := db.GetTaskMappingForKind(taskID, asyncTaskKindVideo); existing != nil {
			if existing.ChannelID != channelID {
				return registration, fmt.Errorf("%w: video task alias [%s]", db.ErrTaskMappingChannelConflict, taskID)
			}
			continue
		}
		pending = append(pending, taskID)
	}
	if len(pending) == 0 {
		return registration, nil
	}

	// Alias recovery needs only routing identity and original provenance. Do
	// not put the current poll status here: statuses evolve over time and would
	// create a different recovery key for each retry of the same alias write.
	registration = newAsyncTaskMappingRegistration(c.Request.Context(), channelID, taskKind, taskAlias, "", pending...)
	registration.OriginRequestID = lookupMapping.OriginRequestID
	if registration.empty() {
		return registration, nil
	}
	return registration, persistVideoTaskAliasRegistrationFn(registration)
}

// persistImageTaskAliases records image IDs that a provider reveals only in a
// later status response. Every alias retains the creating request's audit
// provenance and the internal imgjob_ namespace, so it remains isolated from
// same-named video tasks.
func persistImageTaskAliases(c *gin.Context, channelID, lookupTaskID string, resp interface{}) (asyncTaskMappingRegistration, error) {
	var registration asyncTaskMappingRegistration
	if c == nil || resp == nil {
		return registration, nil
	}
	channelID = strings.TrimSpace(channelID)
	lookupTaskID = strings.TrimSpace(lookupTaskID)
	if channelID == "" || !db.IsValidTaskID(lookupTaskID) {
		return registration, errors.New("image task alias is missing its channel or lookup ID")
	}

	lookupMappingID := imageTaskIDPrefix + lookupTaskID
	lookupMapping := db.GetTaskMappingForKind(lookupMappingID, asyncTaskKindImage)
	if lookupMapping == nil {
		return registration, fmt.Errorf("image task [%s] is not registered", lookupTaskID)
	}
	if lookupMapping.ChannelID != channelID {
		return registration, fmt.Errorf("%w: image task [%s]", db.ErrTaskMappingChannelConflict, lookupTaskID)
	}

	responseAlias, _, responseIDs := collectImageJobTaskDetails(resp)
	if len(responseIDs) == 0 {
		return registration, nil
	}
	taskAlias := lookupMapping.TaskAlias
	if taskAlias == "" {
		taskAlias = responseAlias
	}
	if taskAlias == "" {
		taskAlias = lookupTaskID
	}

	pending := make([]string, 0, len(responseIDs))
	for _, responseID := range responseIDs {
		lookupIDs := imageJobLookupIDs([]string{responseID})
		if len(lookupIDs) == 0 {
			continue
		}
		aliasMappingID := lookupIDs[0]
		if existing := db.GetTaskMappingForKind(aliasMappingID, asyncTaskKindImage); existing != nil {
			if existing.ChannelID != channelID {
				return registration, fmt.Errorf("%w: image task alias [%s]", db.ErrTaskMappingChannelConflict, responseID)
			}
			continue
		}
		pending = append(pending, aliasMappingID)
	}
	if len(pending) == 0 {
		return registration, nil
	}

	// The original creation status is authoritative. A status poll may expose
	// a new alias, but it must not overwrite the creation audit summary.
	registration = newAsyncTaskMappingRegistration(c.Request.Context(), channelID, asyncTaskKindImage, taskAlias, "", pending...)
	registration.OriginRequestID = lookupMapping.OriginRequestID
	if registration.empty() {
		return registration, nil
	}
	return registration, persistImageTaskAliasRegistrationFn(registration)
}

func collectVideoTaskIDs(resp *model.VideoTaskResponse) (string, []string) {
	if resp == nil {
		return "", nil
	}
	primaryID := strings.TrimSpace(resp.ID)
	secondaryID := strings.TrimSpace(resp.TaskID)
	if primaryID == "" {
		primaryID = secondaryID
	}
	if primaryID == "" {
		return "", nil
	}
	ids := []string{primaryID}
	if secondaryID != "" && secondaryID != primaryID {
		ids = append(ids, secondaryID)
	}
	return primaryID, ids
}

// collectImageJobTaskDetails returns the canonical public task ID, its initial
// status, and every equivalent ID that a provider returned.  The public image
// job route historically preferred job.id over a wrapper-level id, so retain
// that priority while mapping every alias for later status requests.
func collectImageJobTaskDetails(resp interface{}) (taskAlias, status string, taskIDs []string) {
	response, ok := resp.(map[string]interface{})
	if !ok {
		return "", "", nil
	}
	seen := make(map[string]struct{}, 3)
	appendID := func(raw interface{}) {
		id, ok := raw.(string)
		id = strings.TrimSpace(id)
		if !ok || id == "" {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		taskIDs = append(taskIDs, id)
	}
	readDetails := func(value map[string]interface{}) {
		for _, key := range []string{"id", "task_id"} {
			appendID(value[key])
		}
		if status == "" {
			if rawStatus, ok := value["status"].(string); ok {
				status = strings.TrimSpace(rawStatus)
			}
		}
	}
	// job is the legacy canonical response envelope. Read it first so the
	// parent log continues to show the same task ID as earlier releases.
	if nested, ok := response["job"].(map[string]interface{}); ok {
		readDetails(nested)
	}
	readDetails(response)
	if nested, ok := response["data"].(map[string]interface{}); ok {
		readDetails(nested)
	}
	if len(taskIDs) > 0 {
		taskAlias = taskIDs[0]
	}
	return taskAlias, status, taskIDs
}

func imageJobDetails(resp interface{}) (taskID, status string) {
	taskID, status, _ = collectImageJobTaskDetails(resp)
	return taskID, status
}

func playgroundImageTaskStatus(resp interface{}, fallback string) string {
	if response, ok := resp.(map[string]interface{}); ok {
		if raw, ok := response["status"].(string); ok && !strings.EqualFold(strings.TrimSpace(raw), "ok") {
			return normalizeProfileImageStatus(raw)
		}
	}
	_, status := imageJobDetails(resp)
	if strings.TrimSpace(status) == "" {
		status = fallback
	}
	return normalizeProfileImageStatus(status)
}

// playgroundImageURLs normalizes the conventional image response shapes used
// by direct generations, image jobs, and Profile-managed media. A top-level
// data field is authoritative so a required-retention response cannot leak a
// temporary provider URL from its raw payload after managed URLs are attached.
func playgroundImageURLs(resp interface{}) []string {
	seen := make(map[string]struct{})
	images := make([]string, 0)
	var visit func(interface{})
	appendImage := func(raw string) {
		value := profileImageDisplaySource(raw)
		if value == "" {
			return
		}
		if _, exists := seen[value]; exists {
			return
		}
		seen[value] = struct{}{}
		images = append(images, value)
	}
	visit = func(value interface{}) {
		switch typed := value.(type) {
		case string:
			appendImage(typed)
		case []interface{}:
			for _, item := range typed {
				visit(item)
			}
		case []map[string]string:
			for _, item := range typed {
				visit(item)
			}
		case map[string]string:
			for _, key := range []string{"url", "image_url", "proxy_url", "b64_json", "thumbnail_url"} {
				if raw := strings.TrimSpace(typed[key]); raw != "" {
					appendImage(raw)
					break
				}
			}
		case map[string]interface{}:
			for _, key := range []string{"url", "image_url", "proxy_url", "b64_json", "thumbnail_url"} {
				if raw, ok := typed[key].(string); ok && strings.TrimSpace(raw) != "" {
					appendImage(raw)
					break
				}
			}
			for key, item := range typed {
				switch strings.ToLower(strings.TrimSpace(key)) {
				case "url", "image_url", "proxy_url", "thumbnail_url", "b64_json", "error":
					continue
				}
				switch item.(type) {
				case map[string]interface{}, map[string]string, []interface{}, []map[string]string:
					visit(item)
				}
			}
		}
	}
	if response, ok := resp.(map[string]interface{}); ok {
		if data, exists := response["data"]; exists {
			visit(data)
			if len(images) > 0 {
				return images
			}
		}
	}
	visit(resp)
	return images
}

// playgroundProfileImageURLs keeps the admin workbench useful while required
// local retention is still materializing. The normal projection deliberately
// hides temporary provider URLs; the workbench may preview the already-audited
// provider result without asking the server to fetch it or weakening SSRF
// checks in the media materializer.
func playgroundProfileImageURLs(ctx context.Context, run *db.TaskRun, resp interface{}) []string {
	if images := playgroundImageURLs(resp); len(images) > 0 {
		return images
	}
	if run == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	latest, err := db.GetTaskRunContext(ctx, run.ID)
	if err != nil || strings.TrimSpace(latest.ResultBody) == "" {
		return nil
	}
	var durableResult interface{}
	if json.Unmarshal([]byte(latest.ResultBody), &durableResult) != nil {
		return nil
	}
	return playgroundImageURLs(durableResult)
}

func playgroundImageTaskError(resp interface{}) string {
	response, ok := resp.(map[string]interface{})
	if !ok {
		return ""
	}
	value, exists := response["error"]
	if !exists {
		if job, ok := response["job"].(map[string]interface{}); ok {
			value = job["error"]
		}
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]interface{}:
		if message, ok := typed["message"].(string); ok {
			return strings.TrimSpace(message)
		}
	}
	return ""
}

func playgroundVideoTaskError(resp interface{}) string {
	if resp == nil {
		return ""
	}
	if taskResp, ok := resp.(*model.VideoTaskResponse); ok && taskResp != nil {
		if taskResp.Error != nil {
			switch e := taskResp.Error.(type) {
			case string:
				return strings.TrimSpace(e)
			case map[string]interface{}:
				if msg, ok := e["message"].(string); ok && strings.TrimSpace(msg) != "" {
					return strings.TrimSpace(msg)
				}
				if b, err := json.Marshal(e); err == nil {
					return string(b)
				}
			default:
				if b, err := json.Marshal(e); err == nil {
					return string(b)
				}
			}
		}
		return ""
	}
	if m, ok := resp.(map[string]interface{}); ok && m != nil {
		if val, exists := m["error"]; exists {
			switch e := val.(type) {
			case string:
				return strings.TrimSpace(e)
			case map[string]interface{}:
				if msg, ok := e["message"].(string); ok && strings.TrimSpace(msg) != "" {
					return strings.TrimSpace(msg)
				}
				if b, err := json.Marshal(e); err == nil {
					return string(b)
				}
			default:
				if b, err := json.Marshal(e); err == nil {
					return string(b)
				}
			}
		}
	}
	return ""
}

func extractUpstreamErrorInfo(err error) (status int, contentType string, retryAfter string, rawBody string) {
	if err == nil {
		return 0, "", "", ""
	}
	var upstreamErr *adapter.UpstreamHTTPError
	if errors.As(err, &upstreamErr) {
		return upstreamErr.StatusCode, upstreamErr.ContentType, upstreamErr.RetryAfter, upstreamErr.Body
	}
	var executorErr *protocol.ExecutorError
	if errors.As(err, &executorErr) {
		return executorErr.HTTPStatus, executorErr.ContentType, executorErr.RetryAfter, executorErr.Body
	}
	return 0, "", "", ""
}

func extractFriendlyMessage(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return ""
	}

	var extractMsgFromMap func(m map[string]interface{}) string
	extractMsgFromMap = func(m map[string]interface{}) string {
		if errObj, ok := m["error"].(map[string]interface{}); ok {
			if msg, ok := errObj["message"].(string); ok && strings.TrimSpace(msg) != "" {
				var nested map[string]interface{}
				if err := json.Unmarshal([]byte(msg), &nested); err == nil {
					if inner := extractMsgFromMap(nested); inner != "" {
						return inner
					}
				}
				return strings.TrimSpace(msg)
			}
		}
		if errStr, ok := m["error"].(string); ok && strings.TrimSpace(errStr) != "" {
			return strings.TrimSpace(errStr)
		}
		if msg, ok := m["message"].(string); ok && strings.TrimSpace(msg) != "" {
			var nested map[string]interface{}
			if err := json.Unmarshal([]byte(msg), &nested); err == nil {
				if inner := extractMsgFromMap(nested); inner != "" {
					return inner
				}
			}
			return strings.TrimSpace(msg)
		}
		for _, key := range []string{"fail_reason", "reason", "detail"} {
			if str, ok := m[key].(string); ok && strings.TrimSpace(str) != "" {
				return strings.TrimSpace(str)
			}
		}
		return ""
	}

	return extractMsgFromMap(obj)
}

func sendPlaygroundError(c *gin.Context, err error, fallback string, latency int, channelID string) {
	if err != nil {
		_ = c.Error(err)
	}
	status, contentType, retryAfter, rawBody := extractUpstreamErrorInfo(err)
	trimmedBody := strings.TrimSpace(rawBody)
	resp := gin.H{
		"status":  "error",
		"latency": latency,
	}
	if channelID != "" {
		resp["channel"] = channelID
	}
	if trimmedBody != "" {
		resp["error"] = rawBody
		resp["upstream_body"] = rawBody
		if friendly := extractFriendlyMessage(trimmedBody); friendly != "" && friendly != trimmedBody {
			resp["provider_message"] = friendly
		}
		if status > 0 {
			resp["upstream_status"] = status
		}
		if contentType != "" {
			resp["upstream_content_type"] = contentType
		}
		if retryAfter != "" {
			resp["upstream_retry_after"] = retryAfter
		}
	} else {
		msg := ""
		if err != nil {
			msg = chineseErrorMessage(err, fallback)
		}
		if msg == "" {
			msg = fallback
		}
		resp["error"] = msg
	}
	c.JSON(http.StatusOK, resp)
}

// discoverChannelModels centralizes the provider-neutral /models contract.
// Built-in Profile defaults cover providers that explicitly report 404/405
// without requiring their Legacy Adapter.
func discoverChannelModels(ctx context.Context, channel *config.UpstreamChannel) ([]string, error) {
	if channel == nil {
		return nil, errors.New("channel is required")
	}
	return protocol.DiscoverModels(ctx, channel.BaseURL, channel.GetEffectiveKeys(), channel.Headers, protocol.ProfileModelDefaults(channel.Type))
}

func imageJobLookupIDs(taskIDs []string) []string {
	lookupIDs := make([]string, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		taskID = strings.TrimSpace(taskID)
		// taskID is externally supplied by the provider and remains subject to
		// the public 128-byte contract. The prefixed result is an internal
		// mapping key and may therefore be seven bytes longer.
		if !db.IsValidTaskID(taskID) {
			continue
		}
		lookupIDs = append(lookupIDs, imageTaskIDPrefix+taskID)
	}
	return lookupIDs
}

// markMappedAsyncTaskPoll deliberately waits until a route has authenticated,
// resolved, and successfully queried an existing task. Everything else keeps
// its own request log so failed, unknown, expired, and media requests remain
// auditable as standalone entries.
func markMappedAsyncTaskPoll(c *gin.Context, lookupTaskID, taskKind, status string, response interface{}) {
	mapping := db.GetTaskMappingForKind(lookupTaskID, taskKind)
	if mapping == nil {
		return
	}
	recordLegacyTaskPoll(lookupTaskID, taskKind, status, response)
	if mapping.OriginRequestID == "" {
		return
	}
	audit.MarkAsyncTaskPoll(c.Request.Context(), lookupTaskID, taskKind, status, response, nil)
}

func handleTestChannel(c *gin.Context) {
	id := c.Param("id")
	cm, err := db.GetChannelModelContext(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "渠道不存在"})
			return
		}
		internalError(c, err, "读取渠道凭据失败，请检查 RELAY_DB_ENCRYPTION_KEY 配置")
		return
	}
	if err := isSafeProbeURL(cm.BaseURL); err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "渠道地址不允许探测"})
		return
	}

	up := cm.ToUpstreamChannel()
	testCtx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	models, err := discoverChannelModels(testCtx, &up)
	latency := int(time.Since(start).Milliseconds())

	if err != nil {
		if persistErr := persistChannelHealth(c, id, "error", latency, err.Error(), nil); persistErr != nil {
			_ = c.Error(persistErr)
			internalError(c, persistErr, "保存渠道测试结果失败，请稍后重试")
			return
		}
		_ = c.Error(err)
		c.JSON(http.StatusOK, gin.H{"status": "error", "message": chineseErrorMessage(err, "渠道测试失败，请检查地址和密钥后重试"), "latency": latency})
		return
	}

	if persistErr := persistChannelHealth(c, id, "healthy", latency, "", models); persistErr != nil {
		_ = c.Error(persistErr)
		internalError(c, persistErr, "保存渠道测试结果失败，请稍后重试")
		return
	}
	service.DefaultDispatcher.UpdateRemoteModels(id, models)
	c.JSON(http.StatusOK, gin.H{"status": "ok", "models": models, "latency": latency})
}

func handleTestAllChannels(c *gin.Context) {
	cms, err := db.GetAllChannelModels()
	if err != nil {
		internalError(c, err, "读取渠道列表失败，请稍后重试")
		return
	}

	type testResult struct {
		ID      string   `json:"id"`
		Status  string   `json:"status"`
		Latency int      `json:"latency"`
		Error   string   `json:"error,omitempty"`
		Models  []string `json:"models,omitempty"`
	}
	results := make([]testResult, len(cms))

	// Bound upstream probes.  A large channel list should not create one
	// simultaneous request per channel (and consequently exhaust sockets,
	// provider rate limits, or SQLite's single writer connection).
	const maxConcurrentChannelTests = 8
	workerCount := len(cms)
	if workerCount > maxConcurrentChannelTests {
		workerCount = maxConcurrentChannelTests
	}
	if workerCount == 0 {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "results": results})
		return
	}

	type testJob struct {
		idx int
		ch  db.ChannelModel
	}
	jobs := make(chan testJob)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				idx, ch := job.idx, job.ch
				if safeErr := isSafeProbeURL(ch.BaseURL); safeErr != nil {
					results[idx] = testResult{ID: ch.ID, Status: "error", Error: "渠道地址不允许探测"}
					continue
				}
				// Derive each probe from the request context so a disconnected
				// dashboard does not leave background requests running.
				testCtx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
				up := ch.ToUpstreamChannel()
				start := time.Now()
				models, err := discoverChannelModels(testCtx, &up)
				cancel()
				latency := int(time.Since(start).Milliseconds())

				if err != nil {
					if persistErr := persistChannelHealth(c, ch.ID, "error", latency, err.Error(), nil); persistErr != nil {
						results[idx] = testResult{ID: ch.ID, Status: "error", Latency: latency, Error: "保存渠道测试结果失败，请稍后重试"}
					} else {
						results[idx] = testResult{ID: ch.ID, Status: "error", Latency: latency, Error: chineseErrorMessage(err, "渠道测试失败，请检查地址和密钥后重试")}
					}
				} else {
					if persistErr := persistChannelHealth(c, ch.ID, "healthy", latency, "", models); persistErr != nil {
						results[idx] = testResult{ID: ch.ID, Status: "error", Latency: latency, Error: "保存渠道测试结果失败，请稍后重试"}
					} else {
						service.DefaultDispatcher.UpdateRemoteModels(ch.ID, models)
						results[idx] = testResult{ID: ch.ID, Status: "healthy", Latency: latency, Models: models}
					}
				}
			}
		}()
	}
sendJobs:
	for i := range cms {
		select {
		case jobs <- testJob{idx: i, ch: cms[i]}:
		case <-c.Request.Context().Done():
			break sendJobs
		}
	}
	close(jobs)
	wg.Wait()

	if err := c.Request.Context().Err(); err != nil {
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "results": results})
}

func handleGetChannelModels(c *gin.Context) {
	id := c.Param("id")
	var cm db.ChannelModel
	if err := db.DB.First(&cm, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "渠道不存在"})
		return
	}

	var models []string
	if cm.ModelsSyncedRaw != "" {
		_ = json.Unmarshal([]byte(cm.ModelsSyncedRaw), &models)
	}

	// 支持 ?refresh=true 强制重新向上游拉取最新模型
	if c.Query("refresh") == "true" || len(models) == 0 {
		up := cm.ToUpstreamChannel()
		if err := isSafeProbeURL(cm.BaseURL); err != nil {
			_ = c.Error(err)
			c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "渠道地址不允许探测"})
			return
		}
		testCtx, cancel := context.WithTimeout(c.Request.Context(), 12*time.Second)
		defer cancel()
		start := time.Now()
		fetched, err := discoverChannelModels(testCtx, &up)
		latency := int(time.Since(start).Milliseconds())
		if err != nil {
			if persistErr := persistChannelHealth(c, id, "error", latency, err.Error(), nil); persistErr != nil {
				_ = c.Error(persistErr)
			}
			_ = c.Error(err)
			c.JSON(http.StatusBadGateway, gin.H{"status": "error", "error": chineseErrorMessage(err, "获取上游模型失败，请检查地址和密钥后重试"), "latency": latency})
			return
		}
		if len(fetched) == 0 {
			message := "上游服务未返回模型列表"
			if persistErr := persistChannelHealth(c, id, "error", latency, message, nil); persistErr != nil {
				_ = c.Error(persistErr)
			}
			c.JSON(http.StatusBadGateway, gin.H{"status": "error", "error": message, "latency": latency})
			return
		}
		models = fetched
		if persistErr := persistChannelHealth(c, id, "healthy", latency, "", models); persistErr != nil {
			_ = c.Error(persistErr)
			internalError(c, persistErr, "保存渠道模型失败，请稍后重试")
			return
		}
		service.DefaultDispatcher.UpdateRemoteModels(id, models)
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "ok",
		"channel_id": cm.ID,
		"models":     models,
	})
}

// handleProbeModels 通过给定的 Type, BaseURL, APIKey(s) 实时探测拉取上游可用模型清单
func handleProbeModels(c *gin.Context) {
	var req struct {
		ChannelID  string `json:"channel_id"`
		Type       string `json:"type"`
		BaseURL    string `json:"base_url"`
		APIKey     string `json:"api_key"`
		APIKeysRaw string `json:"api_keys_raw"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "模型探测请求格式不正确"})
		return
	}

	baseURL := normalizeBaseURLScheme(req.BaseURL)
	if baseURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "Base URL 不能为空"})
		return
	}
	if err := isSafeProbeURL(baseURL); err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "渠道地址不允许探测"})
		return
	}

	key := strings.TrimSpace(req.APIKey)
	if key == "" && req.APIKeysRaw != "" && !strings.Contains(req.APIKeysRaw, "********") {
		lines := strings.Split(req.APIKeysRaw, "\n")
		for _, l := range lines {
			t := strings.TrimSpace(l)
			if t != "" {
				key = t
				break
			}
		}
	}
	if (key == "" || isMaskedSecret(key)) && strings.TrimSpace(req.ChannelID) != "" {
		existing, err := db.GetChannelModel(strings.TrimSpace(req.ChannelID))
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"status": "error", "error": "渠道不存在"})
			return
		}
		upstream := existing.ToUpstreamChannel()
		keys := upstream.GetEffectiveKeys()
		if len(keys) > 0 {
			key = keys[0]
		}
	}
	key = strings.TrimPrefix(key, "Bearer ")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "上游 Key 不能为空"})
		return
	}

	adapterType := strings.TrimSpace(req.Type)
	if adapterType == "" {
		adapterType = "openai"
	}
	if _, err := protocol.BuiltinPreset(adapterType); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "不支持的渠道协议"})
		return
	}

	tempChannel := config.UpstreamChannel{
		ID:      "probe-temp",
		Type:    adapterType,
		BaseURL: baseURL,
		APIKey:  key,
		APIKeys: []string{key},
	}

	testCtx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	start := time.Now()
	models, err := discoverChannelModels(testCtx, &tempChannel)
	latency := int(time.Since(start).Milliseconds())

	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  "error",
			"error":   chineseErrorMessage(err, "模型探测失败，请检查地址和密钥后重试"),
			"latency": latency,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"latency": latency,
		"models":  models,
		"count":   len(models),
	})
}

// isSafeProbeURL 校验探测 URL，防止通过 probe-models 发起 SSRF 攻击局域网或云厂商元数据接口 (SEC-03)
// 允许 localhost/127.0.0.1（本地 Ollama、Sub2API 等合法场景），阻断其他内网地址与云厂商元数据端点。
//
// DNS 解析失败时必须拒绝请求。放行解析失败的名称会让校验形同虚设：HTTP 客户端
// 可能在稍后解析到内网地址，或者遭遇 DNS rebinding，从而绕过这里的 IP 检查。
func isSafeProbeURL(rawURL string) error {
	return isSafeProbeURLWithResolver(rawURL, net.LookupHost)
}

// isSafeProbeURLWithResolver is split out so the security behavior for DNS
// failures can be tested deterministically without depending on the host's
// resolver or network availability.
func isSafeProbeURLWithResolver(rawURL string, lookupHost func(string) ([]string, error)) error {
	if lookupHost == nil {
		return fmt.Errorf("unable to resolve probe host: resolver is unavailable")
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("invalid URL scheme: must be http or https")
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return fmt.Errorf("invalid URL: host is required")
	}

	// 阻断已知云厂商元数据端点
	if host == "169.254.169.254" || strings.HasPrefix(host, "169.254.") || host == "metadata.google.internal" {
		return fmt.Errorf("access to link-local metadata address is blocked")
	}

	// 允许 localhost 和 127.0.0.1（本地开发场景合法）
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return nil
	}
	// RFC 6761 reserves these names for documentation/testing and they are
	// never valid public upstreams. Some sandbox resolvers synthesize an IP
	// for them instead of returning NXDOMAIN, so reject them before lookup.
	for _, suffix := range []string{".invalid", ".example", ".test", ".localhost", ".local", ".internal", ".home.arpa"} {
		if host == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(host, suffix) {
			return fmt.Errorf("probe host uses a reserved non-public domain")
		}
	}

	// DNS 解析后校验 IP，防止 DNS 重绑定绕过域名检查
	ips, err := lookupHost(host)
	if err != nil {
		return fmt.Errorf("unable to resolve probe host: %w", err)
	}
	resolved := false
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		resolved = true
		if ip.IsLoopback() {
			// Only the explicit local development hosts above are allowed to
			// target loopback.  A public-looking DNS name resolving to 127/::1
			// must not be able to reach services on the gateway host.
			return fmt.Errorf("access to loopback address %s is blocked", ipStr)
		}
		if ip.IsUnspecified() || !ip.IsGlobalUnicast() {
			return fmt.Errorf("access to non-global address %s is blocked", ipStr)
		}
		// RFC 6598 shared address space is not public routable space and can
		// terminate inside a provider or the operator's private network.
		if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
			return fmt.Errorf("access to shared/internal address %s is blocked", ipStr)
		}
		if v4 := ip.To4(); v4 != nil && ((v4[0] == 192 && v4[1] == 0 && v4[2] == 0) ||
			(v4[0] == 192 && v4[1] == 0 && v4[2] == 2) ||
			(v4[0] == 198 && v4[1] == 51 && v4[2] == 100) ||
			(v4[0] == 203 && v4[1] == 0 && v4[2] == 113)) {
			return fmt.Errorf("access to special-use address %s is blocked", ipStr)
		}
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() {
			return fmt.Errorf("access to private/internal address %s is blocked", ipStr)
		}
	}
	if !resolved {
		return fmt.Errorf("unable to resolve probe host: no IP address returned")
	}

	return nil
}

// normalizeBaseURLScheme 自动补齐缺失的 http:// 或 https:// 协议头，提升用户输入容错性
func normalizeBaseURLScheme(rawURL string) string {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		return trimmed
	}
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "localhost") || strings.HasPrefix(lower, "127.0.0.1") || strings.HasPrefix(lower, "0.0.0.0") || strings.HasPrefix(lower, "192.168.") || strings.HasPrefix(lower, "10.") {
		return "http://" + trimmed
	}
	return "https://" + trimmed
}

// resolveTargetChannel resolves video task routes. Keep this compatibility
// wrapper for video callers so an image mapping cannot be selected here.
func resolveTargetChannel(channelID, taskID string) (*config.UpstreamChannel, error) {
	return resolveTargetChannelForKind(channelID, taskID, asyncTaskKindVideo)
}

// resolveImageTargetChannel resolves image job routes, including their legacy
// imgjob_ lookup keys, without allowing those mappings into video routing.
func resolveImageTargetChannel(channelID, taskID string) (*config.UpstreamChannel, error) {
	return resolveTargetChannelForKind(channelID, taskID, asyncTaskKindImage)
}

// resolveTargetChannelForKind finds a channel from a type-specific mapping.
// If no task ID is supplied, it retains the existing active-channel fallback
// used by explicitly selected playground calls.
func resolveTargetChannelForKind(channelID, taskID, taskKind string) (*config.UpstreamChannel, error) {
	targetID := channelID
	if targetID == "" && taskID != "" {
		targetID = db.GetTaskChannelForKind(taskID, taskKind)
	}

	active := db.GetActiveUpstreamChannels()
	if len(active) == 0 {
		return nil, fmt.Errorf("no active upstream channels available")
	}

	if targetID != "" {
		for i := range active {
			if active[i].ID == targetID {
				return &active[i], nil
			}
		}
		return nil, fmt.Errorf("mapped channel [%s] is disabled or no longer exists", targetID)
	}

	// 若通过 taskID 反查但未在数据库/缓存中找到该任务，直接返回错误，绝不盲目回退到默认渠道代理由网关 Key 转发 (SEC-04)
	if channelID == "" && taskID != "" && targetID == "" {
		return nil, fmt.Errorf("task [%s] not found in mapping registry", taskID)
	}

	// 智能回退：仅当未指定 taskID 时（例如 playground 未知任务），优先回退到视频渠道
	for i := range active {
		for _, m := range active[i].Models {
			if strings.Contains(strings.ToLower(m), "video") {
				return &active[i], nil
			}
		}
	}

	return &active[0], nil
}

// findActiveChannelByID 根据渠道唯一标识查询已启用的渠道对象
func findActiveChannelByID(channelID string) *config.UpstreamChannel {
	if channelID == "" {
		return nil
	}
	active := db.GetActiveUpstreamChannels()
	for i := range active {
		if active[i].ID == channelID {
			return &active[i]
		}
	}
	return nil
}

// extractModelFast 快速从 JSON 字节切片中提取顶层 "model" 字段值，避免为大请求体做完整的 json.Unmarshal
// 具备深度感知 (depth == 1) 与字符串状态机，杜绝用户提示词中包含 "model": "..." 时的碰撞劫持 (BUG-01)
func extractModelFast(body []byte) string {
	inString := false
	escaped := false
	depth := 0
	n := len(body)

	for i := 0; i < n; i++ {
		c := body[i]
		if inString {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}

		switch c {
		case '"':
			inString = true
			// 仅在处于顶层对象深度 (depth == 1) 时匹配 "model"
			if depth == 1 && i+7 <= n && string(body[i:i+7]) == `"model"` {
				pos := i + 7
				// 跳过冒号前的空白字符
				for pos < n && (body[pos] == ' ' || body[pos] == '\t' || body[pos] == '\r' || body[pos] == '\n') {
					pos++
				}
				if pos < n && body[pos] == ':' {
					pos++
					// 跳过冒号后的空白字符
					for pos < n && (body[pos] == ' ' || body[pos] == '\t' || body[pos] == '\r' || body[pos] == '\n') {
						pos++
					}
					if pos < n && body[pos] == '"' {
						pos++ // 跳过开始引号
						start := pos
						valEscaped := false
						for pos < n {
							if body[pos] == '\\' {
								valEscaped = true
								if pos+1 < n {
									pos += 2
								} else {
									break
								}
								continue
							}
							if body[pos] == '"' {
								val := string(body[start:pos])
								if valEscaped {
									var s string
									if err := json.Unmarshal(body[start-1:pos+1], &s); err == nil {
										return strings.TrimSpace(s)
									}
								}
								return strings.TrimSpace(val)
							}
							pos++
						}
					}
				}
			}
		case '{':
			depth++
		case '}':
			depth--
		}
	}
	return ""
}

// isStreamRequested 判断请求体中是否开启了 stream（使用 json.Unmarshal 精确解析，杜绝用户消息内容碰撞劫持）
func isStreamRequested(body []byte) bool {
	var meta struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(body, &meta)
	return meta.Stream
}

// readBodyAndModel 统一读取并解析 JSON 请求体与 model 字段 (带 32MB 请求体上限防护 SEC-05)
func readBodyAndModel(c *gin.Context) ([]byte, string, error) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 32<<20) // 限制最大 32MB 请求体 (SEC-05)
	rawBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read request body failed or exceeded limit (max 32MB): %w", err)
	}
	// Restore the body before validating so an optional Profile Engine probe
	// cannot consume malformed input that the legacy handler still needs to
	// classify as a client-side 400 error.
	c.Request.Body = io.NopCloser(bytes.NewReader(rawBytes))
	if !json.Valid(rawBytes) {
		return nil, "", errors.New("invalid JSON body")
	}

	modelName := extractModelFast(rawBytes)
	if entry := audit.FromContext(c.Request.Context()); entry != nil {
		entry.SetReqBody(rawBytes, modelName)
	}
	if modelName == "" {
		var meta struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(rawBytes, &meta); err != nil {
			return nil, "", fmt.Errorf("invalid JSON body: %w", err)
		}
		modelName = strings.TrimSpace(meta.Model)
	}

	return rawBytes, modelName, nil
}

const (
	maxMultipartFileBytes  = 16 << 20
	maxMultipartTotalBytes = 32 << 20
	maxMultipartFiles      = 8
)

func validateMultipartFiles(form *multipart.Form) error {
	if form == nil {
		return nil
	}
	count := 0
	var total int64
	for _, headers := range form.File {
		for _, fh := range headers {
			count++
			if count > maxMultipartFiles {
				return fmt.Errorf("too many uploaded files (maximum %d)", maxMultipartFiles)
			}
			if fh == nil || fh.Size < 0 || fh.Size > maxMultipartFileBytes {
				return fmt.Errorf("uploaded file exceeds the %d MiB per-file limit", maxMultipartFileBytes>>20)
			}
			total += fh.Size
			if total > maxMultipartTotalBytes {
				return fmt.Errorf("uploaded files exceed the %d MiB total limit", maxMultipartTotalBytes>>20)
			}
		}
	}
	return nil
}

func readMultipartFile(fh *multipart.FileHeader) ([]byte, error) {
	if fh == nil {
		return nil, errors.New("missing uploaded file")
	}
	f, err := fh.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxMultipartFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxMultipartFileBytes {
		return nil, fmt.Errorf("uploaded file exceeds the %d MiB per-file limit", maxMultipartFileBytes>>20)
	}
	return data, nil
}

func isVideoModel(modelName string) bool {
	m := strings.ToLower(strings.TrimSpace(modelName))
	return strings.Contains(m, "video") ||
		strings.Contains(m, "kling") ||
		strings.Contains(m, "runway") ||
		strings.Contains(m, "sora") ||
		strings.Contains(m, "cogvideo") ||
		strings.Contains(m, "luma") ||
		strings.Contains(m, "hailuo")
}

func isImageModel(modelName string) bool {
	m := strings.ToLower(strings.TrimSpace(modelName))
	return strings.Contains(m, "image") ||
		(strings.Contains(m, "imagine") && !strings.Contains(m, "video")) ||
		strings.Contains(m, "dall-e") ||
		strings.Contains(m, "flux") ||
		strings.Contains(m, "midjourney") ||
		strings.Contains(m, "stable-diffusion") ||
		strings.HasPrefix(m, "sd-") ||
		strings.HasPrefix(m, "sdxl")
}

type playgroundRunRequest struct {
	Kind           string   `json:"kind"`
	ChannelID      string   `json:"channel_id"`
	Model          string   `json:"model"`
	Prompt         string   `json:"prompt"`
	N              int      `json:"n"`
	Size           string   `json:"size"`
	Quality        string   `json:"quality"`
	OutputFormat   string   `json:"output_format"`
	ReferenceURLs  []string `json:"reference_urls"`
	Duration       int      `json:"duration"`
	AspectRatio    string   `json:"aspect_ratio"`
	Resolution     string   `json:"resolution"`
	ReferenceImage string   `json:"reference_image"`
}

func handlePlaygroundRun(c *gin.Context) {
	handlePlayground(c, false)
}

// handlePlaygroundChat is retained for older dashboard clients. When kind is
// omitted it keeps the previous model-name inference behavior.
func handlePlaygroundChat(c *gin.Context) {
	handlePlayground(c, true)
}

func handlePlayground(c *gin.Context, legacy bool) {
	var req playgroundRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		requestError(c, err, "测试请求格式不正确")
		return
	}

	req.Kind = strings.ToLower(strings.TrimSpace(req.Kind))
	if req.Kind == "" && legacy {
		if isVideoModel(req.Model) {
			req.Kind = "video"
		} else if isImageModel(req.Model) {
			req.Kind = "image"
		} else {
			req.Kind = "chat"
		}
	}
	req.Model = strings.TrimSpace(req.Model)
	if req.Model == "" {
		switch req.Kind {
		case "image":
			req.Model = "gpt-image-2"
		case "video":
			req.Model = "grok-imagine-video-1.5"
		default:
			req.Model = "gpt-4o"
		}
	}
	if req.Kind != "chat" && req.Kind != "image" && req.Kind != "video" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "任务类型必须是 chat、image 或 video"})
		return
	}
	if strings.TrimSpace(req.Prompt) == "" {
		if req.Kind == "video" {
			req.Prompt = "ocean waves rolling onto a sandy beach at sunset, cinematic, 4k"
		} else if req.Kind == "image" {
			req.Prompt = "一只可爱的橘猫坐在窗台上晒太阳，午后温暖阳光，胶片质感"
		} else {
			req.Prompt = "Hello, please reply in 1 sentence."
		}
	}

	start := time.Now()
	targetChannel := findActiveChannelByID(req.ChannelID)
	if strings.TrimSpace(req.ChannelID) != "" && targetChannel == nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "指定渠道不存在或已停用"})
		return
	}

	// 1. 图像测试。
	if req.Kind == "image" {
		if req.N <= 0 {
			req.N = 1
		}
		if req.N > 4 {
			c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "图片张数必须在 1 到 4 之间"})
			return
		}
		if strings.TrimSpace(req.Size) == "" {
			req.Size = "1024x1024"
		}
		if strings.TrimSpace(req.Quality) == "" {
			req.Quality = "auto"
		}
		if strings.TrimSpace(req.OutputFormat) == "" {
			req.OutputFormat = "png"
		}
		imgReq := &model.ImageGenerationRequest{
			Model:          req.Model,
			Prompt:         req.Prompt,
			N:              &req.N,
			Size:           req.Size,
			Quality:        req.Quality,
			OutputFormat:   req.OutputFormat,
			ResponseFormat: "url",
			InputImages:    req.ReferenceURLs,
		}

		// Keep the Playground image path aligned with /v1 for channels that
		// have explicitly adopted a Profile binding.
		if profileResp, profileChannel, handled, profileErr := profileEngineImageCreateForChannel(c, imgReq, targetChannel); handled {
			latency := int(time.Since(start).Milliseconds())
			channelID := ""
			if profileChannel != nil {
				channelID = profileChannel.ID
			}
			if profileErr != nil {
				sendPlaygroundError(c, profileErr, "Profile 图片请求失败，请检查配置后重试", latency, channelID)
				return
			}
			images := playgroundImageURLs(profileResp)
			taskID, _ := imageJobDetails(profileResp)
			taskStatus := playgroundImageTaskStatus(profileResp, "")
			if taskStatus == "failed" {
				message := playgroundImageTaskError(profileResp)
				if message == "" {
					message = "图片任务提交失败"
				}
				c.JSON(http.StatusOK, gin.H{"status": "error", "error": message, "latency": latency, "channel": channelID})
				return
			}
			if taskID != "" && (taskStatus != "completed" || len(images) == 0) {
				c.JSON(http.StatusOK, gin.H{
					"status":      "ok",
					"type":        "image_task",
					"latency":     latency,
					"channel":     channelID,
					"task_id":     taskID,
					"task_status": taskStatus,
					"images":      images,
					"response":    "图片任务已提交，正在等待生成结果",
				})
				return
			}
			if len(images) == 0 {
				c.JSON(http.StatusOK, gin.H{"status": "error", "error": "上游未返回图片结果或可轮询的任务 ID", "latency": latency, "channel": channelID})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"status":   "ok",
				"type":     "image",
				"latency":  latency,
				"channel":  channelID,
				"images":   images,
				"response": fmt.Sprintf("生图成功，共生成 %d 张图像", len(images)),
			})
			return
		}

		var imgResp *model.ImageResponse
		var successChannel *config.UpstreamChannel
		var err error

		if targetChannel != nil {
			adp := adapter.Get(targetChannel.Type)
			if adp == nil {
				c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "未找到该渠道协议的处理程序"})
				return
			}
			imgResp, err = adp.ImagesGenerations(c.Request.Context(), targetChannel, imgReq)
			successChannel = targetChannel
		} else {
			err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), req.Model, "images", service.RetryCreateTask, func() bool {
				return imgResp == nil
			}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
				var rErr error
				imgResp, rErr = adp.ImagesGenerations(c.Request.Context(), ch, imgReq)
				if rErr == nil && imgResp != nil {
					successChannel = ch
				}
				return rErr
			})
		}

		latency := int(time.Since(start).Milliseconds())
		if err != nil {
			chanID := ""
			if targetChannel != nil {
				chanID = targetChannel.ID
			} else if successChannel != nil {
				chanID = successChannel.ID
			}
			sendPlaygroundError(c, err, "图片请求失败，请检查渠道配置或稍后重试", latency, chanID)
			return
		}

		var imageURLs []string
		if imgResp != nil {
			for _, item := range imgResp.Data {
				if item.URL != "" {
					imageURLs = append(imageURLs, item.URL)
				} else if item.B64JSON != "" {
					if strings.HasPrefix(item.B64JSON, "/9j/") {
						imageURLs = append(imageURLs, "data:image/jpeg;base64,"+item.B64JSON)
					} else if strings.HasPrefix(item.B64JSON, "UklGR") {
						imageURLs = append(imageURLs, "data:image/webp;base64,"+item.B64JSON)
					} else {
						imageURLs = append(imageURLs, "data:image/png;base64,"+item.B64JSON)
					}
				}
			}
		}
		if len(imageURLs) == 0 {
			c.JSON(http.StatusOK, gin.H{"status": "error", "error": "上游未返回可用的图片结果", "latency": latency})
			return
		}

		chanID := ""
		if successChannel != nil {
			chanID = successChannel.ID
		}

		c.JSON(http.StatusOK, gin.H{
			"status":   "ok",
			"type":     "image",
			"latency":  latency,
			"channel":  chanID,
			"images":   imageURLs,
			"response": fmt.Sprintf("生图成功，共生成 %d 张图像", len(imageURLs)),
		})
		return
	}

	// 2. 视频测试：提交任务，状态由 dashboard 通过 video-status 轮询。
	if req.Kind == "video" {
		if req.Duration <= 0 {
			req.Duration = 6
		}
		if req.Duration < 1 || req.Duration > 15 {
			c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "视频时长必须在 1 到 15 秒之间"})
			return
		}
		if strings.TrimSpace(req.AspectRatio) == "" {
			req.AspectRatio = "16:9"
		}
		if strings.TrimSpace(req.Resolution) == "" {
			req.Resolution = "720p"
		}
		vidReq := &model.VideoGenerationRequest{
			Model:      req.Model,
			Prompt:     req.Prompt,
			Seconds:    strconv.Itoa(req.Duration),
			Size:       req.AspectRatio,
			Quality:    req.Resolution,
			FirstFrame: req.ReferenceImage,
		}

		// A migrated channel must use the same Profile execution path from the
		// Playground as it does from /v1. The optional channel pin preserves the
		// operator's explicit selection while requiring an enabled binding; an
		// unbound channel still falls through to the legacy compatibility path.
		if profileResp, profileChannel, handled, profileErr := profileEngineVideoCreateForChannel(c, vidReq, targetChannel); handled {
			latency := int(time.Since(start).Milliseconds())
			chanID := ""
			if profileChannel != nil {
				chanID = profileChannel.ID
			}
			if profileErr != nil {
				sendPlaygroundError(c, profileErr, "Profile 视频请求失败，请检查配置后重试", latency, chanID)
				return
			}
			if profileResp == nil {
				sendPlaygroundError(c, errors.New("Profile 未返回视频任务结果"), "Profile 未返回视频任务结果", latency, chanID)
				return
			}
			taskAlias := videoContentTaskID(profileResp)
			taskStatus := model.NormalizeVideoStatus(profileResp.Status)
			if taskStatus == "failed" || taskAlias == "" {
				message := playgroundVideoTaskError(profileResp)
				if message == "" {
					message = "视频任务提交失败"
				}
				c.JSON(http.StatusOK, gin.H{
					"status":  "error",
					"error":   message,
					"latency": latency,
					"channel": chanID,
				})
				return
			}
			canExposeContent := profileChannel != nil && canExposeGatewayStableVideoContent(profileResp, profileChannel.ID)
			videoURL := playgroundVideoResultURL(c, profileResp, canExposeContent)
			c.JSON(http.StatusOK, gin.H{
				"status":      "ok",
				"type":        "video_task",
				"latency":     latency,
				"channel":     chanID,
				"task_id":     taskAlias,
				"task_status": taskStatus,
				"video_url":   videoURL,
				"response":    fmt.Sprintf("视频任务已提交 (ID: %s, 初始状态: %s)", taskAlias, taskStatus),
			})
			return
		}

		var vidResp *model.VideoTaskResponse
		var successChannel *config.UpstreamChannel
		var err error

		if targetChannel != nil {
			adp := adapter.Get(targetChannel.Type)
			if adp == nil {
				c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "未找到该渠道协议的处理程序"})
				return
			}
			vidResp, err = adp.CreateVideo(c.Request.Context(), targetChannel, vidReq)
			successChannel = targetChannel
		} else {
			err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), req.Model, "video", service.RetryCreateTask, func() bool {
				return vidResp == nil
			}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
				var rErr error
				vidResp, rErr = adp.CreateVideo(c.Request.Context(), ch, vidReq)
				if rErr == nil && vidResp != nil {
					successChannel = ch
				}
				return rErr
			})
		}

		latency := int(time.Since(start).Milliseconds())
		chanID := ""
		if targetChannel != nil {
			chanID = targetChannel.ID
		} else if successChannel != nil {
			chanID = successChannel.ID
		}
		if err != nil {
			sendPlaygroundError(c, err, "视频请求失败，请检查渠道配置或稍后重试", latency, chanID)
			return
		}
		if vidResp == nil {
			sendPlaygroundError(c, errors.New("上游服务未返回视频任务结果"), "上游服务未返回视频任务结果", latency, chanID)
			return
		}

		taskAlias, taskIDs := collectVideoTaskIDs(vidResp)
		if taskAlias != "" && successChannel != nil {
			if gtID, disambiguated := disambiguateAsyncTaskIDs(successChannel.ID, asyncTaskKindVideo, taskAlias, taskIDs); disambiguated {
				canonicalProviderID := taskAlias
				if canonicalProviderID == "" {
					canonicalProviderID = strings.TrimSpace(vidResp.TaskID)
				}
				if canonicalProviderID == "" {
					canonicalProviderID = strings.TrimSpace(vidResp.ID)
				}
				vidResp.ID = gtID
				vidResp.TaskID = gtID
				taskAlias = gtID
				_ = registerAsyncTaskMappings(c, successChannel.ID, asyncTaskKindVideo, canonicalProviderID, vidResp.Status, gtID)
			} else {
				_ = registerAsyncTaskMappings(c, successChannel.ID, asyncTaskKindVideo, taskAlias, vidResp.Status, taskIDs...)
			}
		}
		if taskAlias == "" {
			taskAlias = strings.TrimSpace(vidResp.TaskID)
		}
		taskStatus := model.NormalizeVideoStatus(vidResp.Status)
		if taskStatus == "failed" || taskAlias == "" {
			message := playgroundVideoTaskError(vidResp)
			if message == "" {
				message = "视频任务提交失败"
			}
			c.JSON(http.StatusOK, gin.H{
				"status":  "error",
				"error":   message,
				"latency": latency,
				"channel": chanID,
			})
			return
		}
		canExposeContent := false
		if successChannel != nil {
			canExposeContent = canExposeGatewayStableVideoContent(vidResp, successChannel.ID)
		}
		videoURL := playgroundVideoResultURL(c, vidResp, canExposeContent)

		c.JSON(http.StatusOK, gin.H{
			"status":      "ok",
			"type":        "video_task",
			"latency":     latency,
			"channel":     chanID,
			"task_id":     taskAlias,
			"task_status": taskStatus,
			"video_url":   videoURL,
			"response":    fmt.Sprintf("视频任务已提交 (ID: %s, 初始状态: %s)", taskAlias, taskStatus),
		})
		return
	}

	// 3. 文本对话 / 推理模型：标准 ChatCompletions 调用
	body := map[string]interface{}{
		"model": req.Model,
		"messages": []map[string]string{
			{"role": "user", "content": req.Prompt},
		},
		"stream": false,
	}
	rawBytes, _ := json.Marshal(body)

	var successChannel *config.UpstreamChannel
	rec := &responseRecorder{header: make(http.Header), body: &strings.Builder{}, statusCode: 200}
	writer := &mockResponseWriter{rec: rec}
	var err error
	// Capture a Profile response into the same recorder used by the legacy
	// Playground adapter, then wrap it in the dashboard's stable JSON envelope.
	// This keeps an explicitly bound channel on its Profile contract.
	c.Request.Body = io.NopCloser(bytes.NewReader(rawBytes))
	if handled, profileChannel, profileErr := profileEngineDirectForChannelResult(c, "chat.completions", "", targetChannel, writer, func() bool { return rec.body.Len() > 0 }); handled {
		latency := int(time.Since(start).Milliseconds())
		channelID := ""
		if profileChannel != nil {
			channelID = profileChannel.ID
		}
		if profileErr != nil {
			sendPlaygroundError(c, profileErr, "Profile 聊天请求失败，请检查配置后重试", latency, channelID)
			return
		}
		if rec.statusCode >= 400 {
			c.JSON(http.StatusOK, gin.H{"status": "error", "type": "chat", "latency": latency, "channel": channelID, "error": fmt.Sprintf("上游服务返回状态码 %d", rec.statusCode), "response": rec.body.String()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok", "type": "chat", "latency": latency, "channel": channelID, "response": rec.body.String()})
		return
	}

	if targetChannel != nil {
		adp := adapter.Get(targetChannel.Type)
		if adp == nil {
			c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "未找到该渠道协议的处理程序"})
			return
		}
		err = adp.ChatCompletions(c.Request.Context(), targetChannel, rawBytes, req.Model, false, writer)
		if err == nil {
			successChannel = targetChannel
		}
	} else {
		// 全网关高可用容灾调度
		err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), req.Model, "chat", service.RetryInference, func() bool {
			return rec.body.Len() == 0
		}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
			rec.body.Reset()
			rErr := adp.ChatCompletions(c.Request.Context(), ch, rawBytes, req.Model, false, writer)
			if rErr == nil {
				successChannel = ch
			}
			return rErr
		})
	}

	latency := int(time.Since(start).Milliseconds())
	if err != nil {
		chanID := ""
		if targetChannel != nil {
			chanID = targetChannel.ID
		} else if successChannel != nil {
			chanID = successChannel.ID
		}
		sendPlaygroundError(c, err, "聊天请求失败，请检查渠道配置或稍后重试", latency, chanID)
		return
	}

	chanID := ""
	if successChannel != nil {
		chanID = successChannel.ID
	}

	if rec.statusCode >= 400 {
		c.JSON(http.StatusOK, gin.H{
			"status":   "error",
			"type":     "chat",
			"latency":  latency,
			"channel":  chanID,
			"error":    fmt.Sprintf("上游服务返回状态码 %d", rec.statusCode),
			"response": rec.body.String(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":   "ok",
		"type":     "chat",
		"latency":  latency,
		"channel":  chanID,
		"response": rec.body.String(),
	})
}

// handlePlaygroundImageStatus lets the dashboard follow both Profile-owned
// and legacy asynchronous image tasks without requiring the gateway API key.
// Persisted mappings always win over the optional channel query parameter.
func handlePlaygroundImageStatus(c *gin.Context) {
	taskID := strings.TrimSpace(c.Query("task_id"))
	chanID := strings.TrimSpace(c.Query("channel_id"))
	if !db.IsValidTaskID(taskID) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "任务 ID 无效或缺失"})
		return
	}
	lookupID := imageTaskIDPrefix + taskID

	if run, runErr := db.GetTaskRunByAliasContext(c.Request.Context(), lookupID); runErr == nil && run.Engine == "profile" && strings.EqualFold(strings.TrimSpace(run.TaskKind), asyncTaskKindImage) {
		profileResp, handled, profileErr := profileTaskStatus(c, lookupID, asyncTaskKindImage)
		if handled {
			if profileErr != nil {
				sendPlaygroundError(c, profileErr, "Profile 图片状态查询失败", 0, run.ChannelID)
				return
			}
			payload, ok := profileResp.(map[string]interface{})
			if !ok || payload == nil {
				c.JSON(http.StatusOK, gin.H{"status": "error", "error": "Profile 状态查询返回了无效的图片结果"})
				return
			}
			taskStatus := playgroundImageTaskStatus(payload, run.TaskStatus)
			result := gin.H{
				"status":      "ok",
				"task_id":     taskID,
				"channel_id":  run.ChannelID,
				"task_status": taskStatus,
				"images":      playgroundProfileImageURLs(c.Request.Context(), run, payload),
			}
			if message := playgroundImageTaskError(payload); message != "" {
				result["error"] = message
			}
			markMappedAsyncTaskPoll(c, lookupID, asyncTaskKindImage, taskStatus, result)
			c.JSON(http.StatusOK, result)
			return
		}
	}

	var targetChannel *config.UpstreamChannel
	var err error
	if mapping := db.GetTaskMappingForKind(lookupID, asyncTaskKindImage); mapping != nil {
		mappedChannelID := strings.TrimSpace(mapping.ChannelID)
		if mappedChannelID == "" {
			err = fmt.Errorf("task [%s] has an invalid channel mapping", taskID)
		} else {
			targetChannel, err = resolveImageTargetChannel(mappedChannelID, "")
		}
	} else if chanID != "" {
		targetChannel, err = resolveImageTargetChannel(chanID, "")
	} else {
		err = fmt.Errorf("task [%s] not found in mapping registry", taskID)
	}
	if err != nil {
		sendPlaygroundError(c, err, "查询图片任务失败，请稍后重试", 0, "")
		return
	}

	adp := adapter.Get(targetChannel.Type)
	if adp == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "error", "error": "该渠道协议已停用"})
		return
	}
	providerTaskID := resolveImageProviderTaskID(taskID)
	resp, err := adp.GetImageJob(c.Request.Context(), targetChannel, providerTaskID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			_ = c.Error(err)
			return
		}
		sendPlaygroundError(c, err, "查询图片任务失败，请稍后重试", 0, targetChannel.ID)
		return
	}
	if taskID != providerTaskID {
		setImageJobResponseIDs(resp, taskID)
	}
	if db.GetTaskMappingForKind(lookupID, asyncTaskKindImage) != nil {
		if registration, persistErr := persistImageTaskAliases(c, targetChannel.ID, taskID, resp); persistErr != nil {
			if errors.Is(persistErr, db.ErrTaskMappingChannelConflict) {
				c.Header("X-Relay-Task-Mapping", "conflict")
				log.Printf("[ASYNC_TASK] successful playground image status for %s revealed a cross-channel alias conflict: %v", taskID, persistErr)
			} else if !registration.empty() {
				deferAsyncTaskMappingPersistence(c, registration, persistErr)
			}
		}
	} else {
		c.Header("X-Relay-Task-Mapping", "missing")
	}
	taskStatus := playgroundImageTaskStatus(resp, "")
	result := gin.H{
		"status":      "ok",
		"task_id":     taskID,
		"channel_id":  targetChannel.ID,
		"task_status": taskStatus,
		"images":      playgroundImageURLs(resp),
		"response":    resp,
	}
	if message := playgroundImageTaskError(resp); message != "" {
		result["error"] = message
	}
	markMappedAsyncTaskPoll(c, lookupID, asyncTaskKindImage, taskStatus, result)
	c.JSON(http.StatusOK, result)
}

// handlePlaygroundVideoStatus 供控制台实时轮询异步视频任务状态
func handlePlaygroundVideoStatus(c *gin.Context) {
	taskID := strings.TrimSpace(c.Query("task_id"))
	chanID := strings.TrimSpace(c.Query("channel_id"))
	if !db.IsValidTaskID(taskID) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "error": "任务 ID 无效或缺失"})
		return
	}

	// Profile-owned tasks are already pinned to their creation channel and
	// immutable revision. Query them through the shared Profile status path so
	// the Playground cannot accidentally re-enter the Legacy Adapter after a
	// channel has been migrated. Unknown/legacy mappings continue below for
	// rollback and old-task compatibility.
	if run, runErr := db.GetTaskRunByAliasContext(c.Request.Context(), taskID); runErr == nil && run.Engine == "profile" && strings.EqualFold(strings.TrimSpace(run.TaskKind), asyncTaskKindVideo) {
		profileResp, handled, profileErr := profileTaskStatus(c, taskID, asyncTaskKindVideo)
		if handled {
			if profileErr != nil {
				sendPlaygroundError(c, profileErr, "Profile 视频状态查询失败", 0, run.ChannelID)
				return
			}
			resp, ok := profileResp.(*model.VideoTaskResponse)
			if !ok || resp == nil {
				c.JSON(http.StatusOK, gin.H{"status": "error", "error": "Profile 状态查询返回了无效的视频结果"})
				return
			}
			taskStatus := model.NormalizeVideoStatus(resp.Status)
			canExposeContent := canExposeGatewayStableVideoContent(resp, run.ChannelID)
			videoURL := playgroundVideoResultURL(c, resp, canExposeContent)
			result := gin.H{
				"status":      "ok",
				"task_id":     taskID,
				"channel_id":  run.ChannelID,
				"task_status": taskStatus,
				"progress":    resp.Progress,
				"video_url":   videoURL,
			}
			if taskStatus == "failed" {
				if msg := playgroundVideoTaskError(resp); msg != "" {
					result["error"] = msg
					result["upstream_body"] = msg
				}
			}
			markMappedAsyncTaskPoll(c, taskID, asyncTaskKindVideo, taskStatus, result)
			c.JSON(http.StatusOK, result)
			return
		}
	}

	// A persisted task mapping is authoritative for channel selection. Disabled
	// or deleted channels are intentionally rejected rather than falling back to
	// another credential set; only a completely missing mapping may use the
	// explicit playground channel while local recovery catches up.
	var targetChannel *config.UpstreamChannel
	var err error
	if mapping := db.GetTaskMappingForKind(taskID, asyncTaskKindVideo); mapping != nil {
		mappedChannelID := strings.TrimSpace(mapping.ChannelID)
		if mappedChannelID == "" {
			err = fmt.Errorf("task [%s] has an invalid channel mapping", taskID)
		} else {
			targetChannel, err = resolveTargetChannel(mappedChannelID, "")
		}
	} else if chanID != "" {
		targetChannel, err = resolveTargetChannel(chanID, "")
	} else {
		err = fmt.Errorf("task [%s] not found in mapping registry", taskID)
	}
	if err != nil {
		sendPlaygroundError(c, err, "查询视频任务失败，请稍后重试", 0, "")
		return
	}

	adp := adapter.Get(targetChannel.Type)
	if adp == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "error", "error": "该渠道协议已停用"})
		return
	}
	providerTaskID := taskID
	if strings.HasPrefix(taskID, "gt_") {
		providerTaskID = resolveCanonicalProviderTaskID(taskID, asyncTaskKindVideo)
	}
	resp, err := adp.GetVideo(c.Request.Context(), targetChannel, providerTaskID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			_ = c.Error(err)
			return
		}
		sendPlaygroundError(c, err, "查询视频任务失败，请稍后重试", 0, targetChannel.ID)
		return
	}
	if resp != nil && taskID != providerTaskID {
		resp.ID = taskID
		resp.TaskID = taskID
	}
	canExposeContent, persistErr := reconcileVideoStatusAliases(c, targetChannel.ID, taskID, resp)
	if persistErr != nil {
		_ = c.Error(persistErr)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "error": "保存视频任务路由失败"})
		return
	}

	taskStatus := model.NormalizeVideoStatus(resp.Status)
	videoURL := playgroundVideoResultURL(c, resp, canExposeContent)

	result := gin.H{
		"status":      "ok",
		"task_id":     taskID,
		"channel_id":  targetChannel.ID,
		"task_status": taskStatus,
		"progress":    resp.Progress,
		"video_url":   videoURL,
	}
	if taskStatus == "failed" {
		if msg := playgroundVideoTaskError(resp); msg != "" {
			result["error"] = msg
			result["upstream_body"] = msg
		}
	}
	markMappedAsyncTaskPoll(c, taskID, asyncTaskKindVideo, taskStatus, result)
	c.JSON(http.StatusOK, result)
}

type responseRecorder struct {
	header     http.Header
	body       *strings.Builder
	statusCode int
}

type mockResponseWriter struct {
	rec *responseRecorder
}

func (m *mockResponseWriter) Header() http.Header { return m.rec.header }
func (m *mockResponseWriter) Write(b []byte) (int, error) {
	m.rec.body.Write(b)
	return len(b), nil
}
func (m *mockResponseWriter) WriteHeader(statusCode int) { m.rec.statusCode = statusCode }

func handleGetSettings(c *gin.Context) {
	retention, _ := strconv.Atoi(db.GetSetting("audit_retention_days", "30"))
	c.JSON(http.StatusOK, gin.H{
		"port":                 config.GetPort(),
		"audit_retention_days": retention,
		"gateway_token":        security.GatewayTokenInfo(),
	})
}

func handleSaveSettings(c *gin.Context) {
	var s struct {
		Port               int `json:"port"`
		AuditRetentionDays int `json:"audit_retention_days"`
	}
	if err := c.ShouldBindJSON(&s); err != nil {
		requestError(c, err, "设置请求格式不正确")
		return
	}
	if s.AuditRetentionDays < 1 || s.AuditRetentionDays > 365 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "审计日志保留天数必须在 1 到 365 之间"})
		return
	}
	if s.Port != 0 && !config.IsValidPort(s.Port) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "端口必须在 1 到 65535 之间"})
		return
	}

	if s.Port > 0 {
		if err := db.SetSettingContext(c.Request.Context(), "port", strconv.Itoa(s.Port)); err != nil {
			_ = c.Error(err)
			internalError(c, err, "保存端口设置失败，请稍后重试")
			return
		}
		c.Set("pending_port", s.Port)
	}
	if err := db.SetSettingContext(c.Request.Context(), "audit_retention_days", strconv.Itoa(s.AuditRetentionDays)); err != nil {
		_ = c.Error(err)
		internalError(c, err, "保存审计日志设置失败，请稍后重试")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// -------------------------------------------------------------
// OpenAI & Anthropic 协议 Handlers (含 Failover 容灾重试)
// -------------------------------------------------------------

func handleModels(c *gin.Context) {
	c.JSON(http.StatusOK, service.DefaultDispatcher.ListModels())
}

func handleRefreshModels(c *gin.Context) {
	service.DefaultDispatcher.SyncRemoteModels(c.Request.Context())
	c.JSON(http.StatusOK, gin.H{
		"status":  "syncing",
		"message": "已触发活动渠道的动态模型同步",
		"models":  service.DefaultDispatcher.ListModels(),
	})
}

type modelStreamingExecutor func(ctx context.Context, ch *config.UpstreamChannel, adp adapter.Adapter, rawBytes []byte, modelName string) error

func dispatchStreamingModelRequest(c *gin.Context, protocol string, exec modelStreamingExecutor) {
	rawBytes, modelName, err := readBodyAndModel(c)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求格式不正确", "invalid_request_error"))
		return
	}
	if modelName == "" {
		c.JSON(http.StatusBadRequest, model.NewError("缺少必填参数：model", "invalid_request_error"))
		return
	}

	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), modelName, protocol, service.RetryInference, func() bool {
		return !c.Writer.Written()
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		return exec(c.Request.Context(), ch, adp, rawBytes, modelName)
	})

	if err != nil && !c.Writer.Written() {
		writeUpstreamError(c, err, "所有上游渠道均请求失败")
	}
}

// writeUpstreamError preserves a provider's meaningful HTTP status (notably
// 4xx/5xx and 200 async business errors) after failover is exhausted. It returns
// the exact raw response body bytes verbatim without restructuring or wrapping
// plain text in model.NewError.
func writeUpstreamError(c *gin.Context, err error, prefix string) {
	if err == nil || c.Writer.Written() || errors.Is(err, context.Canceled) {
		return
	}
	status, contentType, retryAfter, rawBody := extractUpstreamErrorInfo(err)
	if status < 200 || status > 599 {
		status = http.StatusBadGateway
	}
	if retryAfter != "" {
		c.Header("Retry-After", retryAfter)
	}

	body := strings.TrimSpace(rawBody)
	if body != "" {
		if contentType == "" {
			if json.Valid([]byte(body)) {
				contentType = "application/json; charset=utf-8"
			} else {
				contentType = "text/plain; charset=utf-8"
			}
		}
		c.Data(status, contentType, []byte(rawBody))
		return
	}

	message := prefix
	if message == "" {
		message = "上游请求失败"
	}
	c.JSON(status, model.NewError(message, "upstream_error"))
}

func handleChatCompletions(c *gin.Context) {
	if handled, err := profileEngineDirect(c, "chat.completions", ""); handled {
		profileDirectError(c, err, "Profile 聊天上游请求失败")
		return
	}
	dispatchStreamingModelRequest(c, "chat", func(ctx context.Context, ch *config.UpstreamChannel, adp adapter.Adapter, rawBytes []byte, modelName string) error {
		return adp.ChatCompletions(ctx, ch, rawBytes, modelName, isStreamRequested(rawBytes), c.Writer)
	})
}

// handleResponses proxies OpenAI's newer Responses API.  The request model and
// stream flag use the same top-level JSON contract as Chat Completions, while
// the response body remains provider-native (JSON or SSE).
func handleResponses(c *gin.Context) {
	if handled, err := profileEngineDirect(c, "responses.create", ""); handled {
		profileDirectError(c, err, "Profile 响应上游请求失败")
		return
	}
	dispatchStreamingModelRequest(c, "responses", func(ctx context.Context, ch *config.UpstreamChannel, adp adapter.Adapter, rawBytes []byte, modelName string) error {
		return adp.Responses(ctx, ch, rawBytes, modelName, isStreamRequested(rawBytes), c.Writer)
	})
}

func handleAnthropicMessages(c *gin.Context) {
	ctx := c.Request.Context()
	if beta := c.Request.Header.Get("anthropic-beta"); beta != "" {
		ctx = context.WithValue(ctx, adapter.CtxAnthropicBeta, beta)
	}
	if ver := c.Request.Header.Get("anthropic-version"); ver != "" {
		ctx = context.WithValue(ctx, adapter.CtxAnthropicVersion, ver)
	}
	c.Request = c.Request.WithContext(ctx)
	if handled, err := profileEngineDirect(c, "messages.create", "x-api-key"); handled {
		profileDirectError(c, err, "Profile Anthropic 上游请求失败")
		return
	}

	dispatchStreamingModelRequest(c, "anthropic_messages", func(ctx context.Context, ch *config.UpstreamChannel, adp adapter.Adapter, rawBytes []byte, modelName string) error {
		return adp.AnthropicMessages(ctx, ch, rawBytes, isStreamRequested(rawBytes), c.Writer)
	})
}

func handleEmbeddings(c *gin.Context) {
	if handled, err := profileEngineDirect(c, "embeddings.create", ""); handled {
		profileDirectError(c, err, "Profile 向量请求失败")
		return
	}
	dispatchStreamingModelRequest(c, "embeddings", func(ctx context.Context, ch *config.UpstreamChannel, adp adapter.Adapter, rawBytes []byte, modelName string) error {
		return adp.Embeddings(ctx, ch, rawBytes, modelName, c.Writer)
	})
}

func handleImagesGenerations(c *gin.Context) {
	// Image responses use a structured envelope and may require synchronous
	// media materialization. Let the image bridge handle bound direct Profiles
	// before the generic raw JSON path, while leaving unbound/legacy channels
	// untouched for rollback compatibility.
	if profileEngineEnabledFor("RELAY_ENABLE_PROFILE_IMAGE_ENGINE") || profileEngineEnabledFor("RELAY_ENABLE_PROFILE_DIRECT_ENGINE") {
		rawBytes, modelName, readErr := readBodyAndModel(c)
		if readErr == nil {
			var imageReq model.ImageGenerationRequest
			if json.Unmarshal(rawBytes, &imageReq) == nil {
				if strings.TrimSpace(imageReq.Model) == "" {
					imageReq.Model = modelName
				}
				if profileResp, _, handled, profileErr := profileEngineImageCreate(c, &imageReq); handled {
					if profileErr != nil {
						profileDirectError(c, profileErr, "Profile 图片上游请求失败")
						return
					}
					c.JSON(http.StatusOK, profileResp)
					return
				}
			}
		}
	}
	// A direct image generations operation can coexist with an async image
	// job operation. Other profiles continue to use images.create below.
	if handled, err := profileEngineDirect(c, "images.generations", ""); handled {
		profileDirectError(c, err, "Profile 图片上游请求失败")
		return
	}
	if handled, err := profileEngineDirect(c, "images.create", ""); handled {
		profileDirectError(c, err, "Profile 图片上游请求失败")
		return
	}
	rawBytes, modelName, err := readBodyAndModel(c)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求体格式不正确", "invalid_request_error"))
		return
	}
	var req model.ImageGenerationRequest
	if err := json.Unmarshal(rawBytes, &req); err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求体格式不正确", "invalid_request_error"))
		return
	}
	if req.Model == "" && modelName != "" {
		req.Model = modelName
	}
	if strings.TrimSpace(req.Model) == "" {
		c.JSON(http.StatusBadRequest, model.NewError("缺少必填参数：model", "invalid_request_error"))
		return
	}

	var resp *model.ImageResponse
	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), req.Model, "images", service.RetryCreateTask, func() bool {
		return resp == nil
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		reqCopy := req
		var rErr error
		resp, rErr = adp.ImagesGenerations(c.Request.Context(), ch, &reqCopy)
		return rErr
	})

	if err != nil {
		writeUpstreamError(c, err, "上游请求失败")
		return
	}
	c.JSON(http.StatusOK, resp)
}

func handleImagesEdits(c *gin.Context) {
	contentType := c.Request.Header.Get("Content-Type")
	if strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data") {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<20)
		form, err := c.MultipartForm()
		if err != nil {
			_ = c.Error(err)
			c.JSON(http.StatusBadRequest, model.NewError("多部分表单解析失败", "invalid_request_error"))
			return
		}
		if err := validateMultipartFiles(form); err != nil {
			_ = c.Error(err)
			c.JSON(http.StatusRequestEntityTooLarge, model.NewError("上传文件不符合大小限制", "invalid_request_error"))
			return
		}

		modelName := strings.TrimSpace(c.PostForm("model"))
		if modelName == "" {
			modelName = "gpt-image-2"
		}
		prompt := c.PostForm("prompt")
		size := c.PostForm("size")
		quality := c.PostForm("quality")
		nStr := c.PostForm("n")

		normalizedSize := adapter.ResolveNormalizedSize(modelName, size, quality, prompt)

		if entry := audit.FromContext(c.Request.Context()); entry != nil {
			var filesSummary []string
			for k, fileHeaders := range form.File {
				for _, fh := range fileHeaders {
					filesSummary = append(filesSummary, fmt.Sprintf("%s: %s (%d bytes)", k, fh.Filename, fh.Size))
				}
			}
			reqSummary := fmt.Sprintf("{\n  \"model\": \"%s\",\n  \"prompt\": \"%s\",\n  \"size\": \"%s\",\n  \"normalized_size\": \"%s\",\n  \"quality\": \"%s\",\n  \"files\": [%s]\n}",
				modelName, prompt, size, normalizedSize, quality, strings.Join(filesSummary, ", "))
			entry.SetReqBody([]byte(reqSummary), modelName)
		}

		// A bound Profile owns the multipart wire contract as well as JSON
		// edits. The helper rebuilds the form after applying the Profile's
		// narrow model policy and uses the generic executor for credentials,
		// timeouts and response handling. Unbound channels retain the legacy
		// path during the migration window.
		if handled, profileErr := profileEngineMultipartDirect(c, "images.edits", form, nil); handled {
			profileDirectError(c, profileErr, "Profile 图片编辑上游请求失败")
			return
		}

		err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), modelName, "images", service.RetryCreateTask, func() bool {
			return !c.Writer.Written()
		}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
			targetModel := modelName
			if mapped, ok := ch.ModelMap[modelName]; ok && mapped != "" {
				targetModel = mapped
			}

			var bodyBuf bytes.Buffer
			mw := multipart.NewWriter(&bodyBuf)
			if err := mw.WriteField("model", targetModel); err != nil {
				return err
			}
			if err := mw.WriteField("prompt", prompt); err != nil {
				return err
			}
			if normalizedSize != "" {
				if err := mw.WriteField("size", normalizedSize); err != nil {
					return err
				}
			}
			if quality != "" && quality != "auto" {
				if err := mw.WriteField("quality", quality); err != nil {
					return err
				}
			}
			if nStr != "" {
				if err := mw.WriteField("n", nStr); err != nil {
					return err
				}
			}

			for key, fileHeaders := range form.File {
				for _, fh := range fileHeaders {
					f, oErr := fh.Open()
					if oErr != nil {
						return oErr
					}
					part, cErr := mw.CreateFormFile(key, fh.Filename)
					if cErr != nil {
						_ = f.Close()
						return cErr
					}
					written, copyErr := io.Copy(part, io.LimitReader(f, maxMultipartFileBytes+1))
					closeErr := f.Close()
					if copyErr != nil {
						return copyErr
					}
					if written > maxMultipartFileBytes {
						return fmt.Errorf("uploaded file exceeds the %d MiB per-file limit", maxMultipartFileBytes>>20)
					}
					if closeErr != nil {
						return closeErr
					}
				}
			}

			for k, vals := range form.Value {
				if k == "model" || k == "prompt" || k == "size" || k == "quality" || k == "n" {
					continue
				}
				for _, v := range vals {
					if err := mw.WriteField(k, v); err != nil {
						return err
					}
				}
			}
			if err := mw.Close(); err != nil {
				return err
			}

			log.Printf("[IMAGE_EDIT] Dispatching multipart edit to upstream [%s]: model=%s, size=%s", ch.ID, targetModel, normalizedSize)
			return adp.ImagesEdits(c.Request.Context(), ch, bodyBuf.Bytes(), mw.FormDataContentType(), c.Writer)
		})

		if err != nil && !c.Writer.Written() {
			writeUpstreamError(c, err, "上游请求失败")
		}
		return
	}

	// JSON 格式请求
	if handled, err := profileEngineDirect(c, "images.edits", ""); handled {
		profileDirectError(c, err, "Profile 图片编辑上游请求失败")
		return
	}
	rawBytes, modelName, err := readBodyAndModel(c)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求体格式不正确", "invalid_request_error"))
		return
	}
	if modelName == "" {
		modelName = "gpt-image-2"
	}

	var req model.ImageGenerationRequest
	if err := json.Unmarshal(rawBytes, &req); err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求体格式不正确", "invalid_request_error"))
		return
	}
	if req.Model == "" {
		req.Model = modelName
	}

	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), modelName, "images", service.RetryCreateTask, func() bool {
		return !c.Writer.Written()
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		reqCopy := req
		if mapped, ok := ch.ModelMap[reqCopy.Model]; ok && mapped != "" {
			reqCopy.Model = mapped
		}
		normPayload := adapter.NormalizeImageRequest(&reqCopy, ch.Type)
		bodyBytes, mErr := json.Marshal(normPayload)
		if mErr != nil {
			return mErr
		}
		log.Printf("[IMAGE_EDIT] Dispatching JSON edit to upstream [%s]: model=%s, size=%v", ch.ID, reqCopy.Model, normPayload["size"])
		return adp.ImagesEdits(c.Request.Context(), ch, bodyBytes, "application/json", c.Writer)
	})

	if err != nil && !c.Writer.Written() {
		writeUpstreamError(c, err, "上游请求失败")
	}
}

func handleAudioSpeech(c *gin.Context) {
	if handled, err := profileEngineDirect(c, "audio.speech", ""); handled {
		profileDirectError(c, err, "Profile 音频上游请求失败")
		return
	}
	rawBytes, modelName, err := readBodyAndModel(c)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求体格式不正确", "invalid_request_error"))
		return
	}
	var req model.AudioSpeechRequest
	if err := json.Unmarshal(rawBytes, &req); err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求体格式不正确", "invalid_request_error"))
		return
	}
	if req.Model == "" {
		req.Model = modelName
	}
	if strings.TrimSpace(req.Model) == "" {
		c.JSON(http.StatusBadRequest, model.NewError("缺少必填参数：model", "invalid_request_error"))
		return
	}

	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), req.Model, "audio", service.RetryInference, func() bool {
		return !c.Writer.Written()
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		return adp.AudioSpeech(c.Request.Context(), ch, &req, c.Writer)
	})

	if err != nil && !c.Writer.Written() {
		writeUpstreamError(c, err, "上游请求失败")
	}
}

func handleAnthropicCountTokens(c *gin.Context) {
	ctx := c.Request.Context()
	if v := c.Request.Header.Get("anthropic-version"); v != "" {
		ctx = context.WithValue(ctx, adapter.CtxAnthropicVersion, v)
	}
	if b := c.Request.Header.Get("anthropic-beta"); b != "" {
		ctx = context.WithValue(ctx, adapter.CtxAnthropicBeta, b)
	}
	c.Request = c.Request.WithContext(ctx)
	if handled, err := profileEngineDirect(c, "messages.count_tokens", "x-api-key"); handled {
		profileDirectError(c, err, "Profile Anthropic 计数上游请求失败")
		return
	}
	rawBytes, modelName, err := readBodyAndModel(c)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求格式不正确", "invalid_request_error"))
		return
	}
	if modelName == "" {
		modelName = "claude-3-7-sonnet"
	}
	rawBytes = adapter.RewriteJSONModel(rawBytes, modelName)
	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), modelName, "messages", service.RetryInference, func() bool {
		return !c.Writer.Written()
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		return adp.CountTokens(c.Request.Context(), ch, rawBytes, c.Writer)
	})
	if err != nil && !c.Writer.Written() {
		writeUpstreamError(c, err, "所有上游渠道均请求失败")
	}
}

func handleModerations(c *gin.Context) {
	if handled, err := profileEngineDirect(c, "moderations.create", ""); handled {
		profileDirectError(c, err, "Profile 内容审查上游请求失败")
		return
	}
	rawBytes, modelName, err := readBodyAndModel(c)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求格式不正确", "invalid_request_error"))
		return
	}
	if modelName == "" {
		modelName = "omni-moderation-latest"
	}
	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), modelName, "moderations", service.RetryInference, func() bool {
		return !c.Writer.Written()
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		return adp.Moderations(c.Request.Context(), ch, rawBytes, modelName, c.Writer)
	})
	if err != nil && !c.Writer.Written() {
		writeUpstreamError(c, err, "所有上游渠道均请求失败")
	}
}

func handleAudioTranscriptions(c *gin.Context) {
	handleAudioTranscribeOrTranslate(c, "/audio/transcriptions")
}

func handleAudioTranslations(c *gin.Context) {
	handleAudioTranscribeOrTranslate(c, "/audio/translations")
}

func handleAudioTranscribeOrTranslate(c *gin.Context, endpoint string) {
	contentType := c.Request.Header.Get("Content-Type")
	if !strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data") {
		c.JSON(http.StatusBadRequest, model.NewError("必须使用 multipart/form-data 格式上传音频", "invalid_request_error"))
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<20)
	form, err := c.MultipartForm()
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("音频表单解析失败", "invalid_request_error"))
		return
	}
	defer func() {
		if form != nil {
			_ = form.RemoveAll()
		}
	}()

	modelName := strings.TrimSpace(c.PostForm("model"))
	if modelName == "" {
		modelName = "whisper-1"
	}

	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), modelName, "audio", service.RetryCreateTask, func() bool {
		return !c.Writer.Written()
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		targetModel := modelName
		if mapped, ok := ch.ModelMap[modelName]; ok && mapped != "" {
			targetModel = mapped
		}

		var bodyBuf bytes.Buffer
		mw := multipart.NewWriter(&bodyBuf)
		if err := mw.WriteField("model", targetModel); err != nil {
			return err
		}
		for k, vals := range form.Value {
			if k == "model" {
				continue
			}
			for _, v := range vals {
				if err := mw.WriteField(k, v); err != nil {
					return err
				}
			}
		}
		for key, fileHeaders := range form.File {
			for _, fh := range fileHeaders {
				f, oErr := fh.Open()
				if oErr != nil {
					return oErr
				}
				part, cErr := mw.CreateFormFile(key, fh.Filename)
				if cErr != nil {
					_ = f.Close()
					return cErr
				}
				_, copyErr := io.Copy(part, f)
				_ = f.Close()
				if copyErr != nil {
					return copyErr
				}
			}
		}
		if err := mw.Close(); err != nil {
			return err
		}

		return adp.AudioTranscriptions(c.Request.Context(), ch, endpoint, bodyBuf.Bytes(), mw.FormDataContentType(), c.Writer)
	})

	if err != nil && !c.Writer.Written() {
		writeUpstreamError(c, err, "音频处理上游请求失败")
	}
}

// parseVideoGenerationRequest 智能多模态解析视频请求体（完美兼容 application/json, multipart/form-data 及 form-urlencoded）
func parseVideoGenerationRequest(c *gin.Context) (*model.VideoGenerationRequest, error) {
	req := &model.VideoGenerationRequest{}
	contentType := strings.ToLower(c.Request.Header.Get("Content-Type"))

	// 1. 处理 Multipart/Form-Data 或 URL-Encoded 表单请求（如 infinite-canvas 前端 FormData 提交）
	if strings.Contains(contentType, "multipart/form-data") || strings.Contains(contentType, "application/x-www-form-urlencoded") {
		form, err := c.MultipartForm()
		if err != nil && strings.Contains(contentType, "multipart/form-data") {
			_ = c.Request.ParseMultipartForm(32 << 20)
			form = c.Request.MultipartForm
		}
		if strings.Contains(contentType, "multipart/form-data") && err != nil && form == nil {
			return nil, fmt.Errorf("failed to parse multipart form: %w", err)
		}
		if fileErr := validateMultipartFiles(form); fileErr != nil {
			return nil, fileErr
		}

		req.Model = strings.TrimSpace(c.PostForm("model"))
		req.Prompt = strings.TrimSpace(c.PostForm("prompt"))

		// 持续时间：兼容 seconds / duration
		sec := strings.TrimSpace(c.PostForm("seconds"))
		if sec == "" {
			sec = strings.TrimSpace(c.PostForm("duration"))
		}
		req.Seconds = sec

		// 尺寸比例：兼容 size / aspect_ratio
		sz := strings.TrimSpace(c.PostForm("size"))
		if sz == "" {
			sz = strings.TrimSpace(c.PostForm("aspect_ratio"))
		}
		req.Size = sz

		// 画质分辨率：兼容 quality / resolution_name / resolution / mode
		q := strings.TrimSpace(c.PostForm("quality"))
		if q == "" {
			q = strings.TrimSpace(c.PostForm("resolution_name"))
		}
		if q == "" {
			q = strings.TrimSpace(c.PostForm("resolution"))
		}
		if q == "" {
			q = strings.TrimSpace(c.PostForm("mode"))
		}
		req.Quality = q

		// 首尾帧
		ff := strings.TrimSpace(c.PostForm("first_frame_url"))
		if ff == "" {
			ff = strings.TrimSpace(c.PostForm("first_frame"))
		}
		if ff == "" {
			ff = strings.TrimSpace(c.PostForm("image"))
		}
		req.FirstFrame = ff

		lf := strings.TrimSpace(c.PostForm("last_frame_url"))
		if lf == "" {
			lf = strings.TrimSpace(c.PostForm("last_frame"))
		}
		req.LastFrame = lf

		if form != nil {
			// 提取文本参考图
			for _, key := range []string{"input_reference[]", "images[]", "images", "input_reference"} {
				if vals, ok := form.Value[key]; ok {
					for _, v := range vals {
						t := strings.TrimSpace(v)
						if t != "" {
							req.Images = append(req.Images, t)
						}
					}
				}
			}
			// 提取二进制文件参考图（自动转为 Base64 Data URI 适配上游）
			for _, fileKey := range []string{"input_reference[]", "images[]", "image", "first_frame", "first_frame_url"} {
				if fileHeaders, ok := form.File[fileKey]; ok {
					for _, fh := range fileHeaders {
						data, readErr := readMultipartFile(fh)
						if readErr != nil {
							return nil, readErr
						}
						if len(data) > 0 {
							mime := fh.Header.Get("Content-Type")
							if mime == "" {
								mime = "image/png"
							}
							dataURI := fmt.Sprintf("data:%s;base64,%s", mime, base64.StdEncoding.EncodeToString(data))
							if (fileKey == "image" || fileKey == "first_frame" || fileKey == "first_frame_url") && req.FirstFrame == "" {
								req.FirstFrame = dataURI
							} else {
								req.Images = append(req.Images, dataURI)
							}
						}
					}
				}
			}
		}

		genAudio := c.PostForm("video_generate_audio")
		if genAudio == "" {
			genAudio = c.PostForm("with_audio")
		}
		req.GenerateAudio = strings.EqualFold(genAudio, "true") || genAudio == "1"
		if entry := audit.FromContext(c.Request.Context()); entry != nil {
			if summary, marshalErr := json.Marshal(req); marshalErr == nil {
				entry.SetReqBody(summary, req.Model)
			}
		}

		return req, nil
	}

	// 2. 处理 JSON 格式请求（支持宽容反序列化，容忍数值型 seconds/duration，带 64MB 限制保护 SEC-05）
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<20)
	rawBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body failed or exceeded limit (max 64MB): %w", err)
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(rawBytes))
	if entry := audit.FromContext(c.Request.Context()); entry != nil {
		entry.SetReqBody(rawBytes, extractModelFast(rawBytes))
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(rawBytes, &raw); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}

	if v, ok := raw["model"].(string); ok {
		req.Model = strings.TrimSpace(v)
	}
	if v, ok := raw["prompt"].(string); ok {
		req.Prompt = strings.TrimSpace(v)
	}

	if v, ok := raw["seconds"]; ok {
		req.Seconds = strings.TrimSpace(fmt.Sprint(v))
	} else if v, ok := raw["duration"]; ok {
		req.Seconds = strings.TrimSpace(fmt.Sprint(v))
	}

	if v, ok := raw["size"].(string); ok {
		req.Size = strings.TrimSpace(v)
	} else if v, ok := raw["aspect_ratio"].(string); ok {
		req.Size = strings.TrimSpace(v)
	}

	if v, ok := raw["quality"].(string); ok {
		req.Quality = strings.TrimSpace(v)
	} else if v, ok := raw["resolution_name"].(string); ok {
		req.Quality = strings.TrimSpace(v)
	} else if v, ok := raw["resolution"].(string); ok {
		req.Quality = strings.TrimSpace(v)
	} else if v, ok := raw["mode"].(string); ok {
		req.Quality = strings.TrimSpace(v)
	}
	if metadata, ok := raw["metadata"].(map[string]interface{}); ok {
		if v, ok := metadata["aspect_ratio"].(string); ok && strings.TrimSpace(v) != "" {
			req.Size = strings.TrimSpace(v)
		}
		if v, ok := metadata["resolution"].(string); ok && strings.TrimSpace(v) != "" {
			req.Quality = strings.TrimSpace(v)
		}
	}

	if v, ok := raw["first_frame"].(string); ok {
		req.FirstFrame = strings.TrimSpace(v)
	} else if v, ok := raw["first_frame_url"].(string); ok {
		req.FirstFrame = strings.TrimSpace(v)
	} else if v, ok := raw["image"].(string); ok {
		req.FirstFrame = strings.TrimSpace(v)
	}

	if v, ok := raw["last_frame"].(string); ok {
		req.LastFrame = strings.TrimSpace(v)
	} else if v, ok := raw["last_frame_url"].(string); ok {
		req.LastFrame = strings.TrimSpace(v)
	}

	if imgs, ok := raw["images"].([]interface{}); ok {
		for _, img := range imgs {
			switch value := img.(type) {
			case string:
				if strings.TrimSpace(value) != "" {
					req.Images = append(req.Images, strings.TrimSpace(value))
				}
			case map[string]interface{}:
				if imageURL, ok := value["image_url"].(string); ok && strings.TrimSpace(imageURL) != "" {
					req.Images = append(req.Images, strings.TrimSpace(imageURL))
				}
			}
		}
	} else if refs, ok := raw["input_reference"].([]interface{}); ok {
		for _, ref := range refs {
			if s, ok := ref.(string); ok && strings.TrimSpace(s) != "" {
				req.Images = append(req.Images, strings.TrimSpace(s))
			}
		}
	} else if ref, ok := raw["input_reference"].(string); ok && strings.TrimSpace(ref) != "" {
		req.Images = append(req.Images, strings.TrimSpace(ref))
	}

	if v, ok := raw["generate_audio"].(bool); ok {
		req.GenerateAudio = v
	} else if v, ok := raw["with_audio"].(bool); ok {
		req.GenerateAudio = v
	}
	return req, nil
}

func handleCreateVideo(c *gin.Context) {
	req, err := parseVideoGenerationRequest(c)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求体格式不正确", "invalid_request_error"))
		return
	}
	if strings.TrimSpace(req.Model) == "" {
		c.JSON(http.StatusBadRequest, model.NewError("缺少必填参数：model", "invalid_request_error"))
		return
	}
	if profileResp, profileChannel, handled, profileErr := profileEngineVideoCreate(c, req); handled {
		if profileErr != nil {
			writeUpstreamError(c, profileErr, "Profile 上游请求失败")
			return
		}
		if profileResp == nil {
			c.JSON(http.StatusBadGateway, model.NewError("Profile 未返回有效结果", "upstream_error"))
			return
		}
		if profileChannel != nil {
			if !isManagedMediaURL(profileResp.VideoURL) {
				canExpose := canExposeGatewayStableVideoContent(profileResp, profileChannel.ID)
				if canExpose {
					formatVideoTaskResponse(c, profileResp)
				} else {
					suppressGatewayStableVideoContentURLs(c, profileResp)
				}
			}
		}
		c.JSON(http.StatusOK, profileResp)
		return
	}

	var resp *model.VideoTaskResponse
	var successChannel *config.UpstreamChannel
	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), req.Model, "video", service.RetryCreateTask, func() bool {
		return resp == nil
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		var rErr error
		resp, rErr = adp.CreateVideo(c.Request.Context(), ch, req)
		if rErr == nil && resp != nil {
			successChannel = ch
		}
		return rErr
	})

	if err != nil {
		writeUpstreamError(c, err, "上游请求失败")
		return
	}

	if resp != nil {
		if successChannel != nil {
			taskAlias, taskIDs := collectVideoTaskIDs(resp)
			if gtID, disambiguated := disambiguateAsyncTaskIDs(successChannel.ID, asyncTaskKindVideo, taskAlias, taskIDs); disambiguated {
				canonicalProviderID := taskAlias
				if canonicalProviderID == "" {
					canonicalProviderID = strings.TrimSpace(resp.TaskID)
				}
				if canonicalProviderID == "" {
					canonicalProviderID = strings.TrimSpace(resp.ID)
				}
				resp.ID = gtID
				resp.TaskID = gtID
				_ = registerAsyncTaskMappings(c, successChannel.ID, asyncTaskKindVideo, canonicalProviderID, resp.Status, gtID)
			} else {
				_ = registerAsyncTaskMappings(c, successChannel.ID, asyncTaskKindVideo, taskAlias, resp.Status, taskIDs...)
			}
			if canExposeGatewayStableVideoContent(resp, successChannel.ID) {
				formatVideoTaskResponse(c, resp)
			} else {
				// The upstream create already succeeded, so preserve its task result
				// rather than inducing a paid retry. Do not turn any URL into a
				// gateway /content URL, though: an unpinned or conflicting provider
				// ID could otherwise route through another channel's credentials.
				suppressGatewayStableVideoContentURLs(c, resp)
			}
		} else {
			formatVideoTaskResponse(c, resp)
		}
	}

	c.JSON(http.StatusOK, resp)
}

func handleGetVideo(c *gin.Context) {
	taskID := strings.TrimSpace(c.Param("id"))
	if taskID == "" {
		taskID = strings.TrimSpace(c.Query("task_id"))
	}
	if !db.IsValidTaskID(taskID) {
		c.JSON(http.StatusBadRequest, model.NewError("任务 ID 无效", "invalid_request_error"))
		return
	}
	if profileResp, handled, profileErr := profileTaskStatus(c, taskID, asyncTaskKindVideo); handled {
		if profileErr != nil {
			writeUpstreamError(c, profileErr, "Profile 视频状态查询失败")
			return
		}
		c.JSON(http.StatusOK, profileResp)
		return
	}

	targetChannel, err := resolveTargetChannel("", taskID)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusInternalServerError, model.NewError("解析任务所属渠道失败", "channel_error"))
		return
	}

	adp := adapter.Get(targetChannel.Type)
	if adp == nil {
		writeUpstreamError(c, errors.New("该渠道协议已停用"), "上游请求失败")
		return
	}
	providerTaskID := taskID
	if strings.HasPrefix(taskID, "gt_") {
		providerTaskID = resolveCanonicalProviderTaskID(taskID, asyncTaskKindVideo)
	}
	resp, err := adp.GetVideo(c.Request.Context(), targetChannel, providerTaskID)
	if err != nil {
		writeUpstreamError(c, err, "上游请求失败")
		return
	}
	if resp != nil {
		if taskID != providerTaskID {
			resp.ID = taskID
			resp.TaskID = taskID
		}
		canExposeContent, persistErr := reconcileVideoStatusAliases(c, targetChannel.ID, taskID, resp)
		if persistErr != nil {
			_ = c.Error(persistErr)
			c.JSON(http.StatusInternalServerError, model.NewError("保存视频任务路由失败", "task_mapping_error"))
			return
		}
		if db.GetTaskChannelForKind(resp.ID, asyncTaskKindVideo) != targetChannel.ID {
			resp.ID = taskID
		}
		if db.GetTaskChannelForKind(resp.TaskID, asyncTaskKindVideo) != targetChannel.ID {
			resp.TaskID = taskID
		}
		if canExposeContent {
			formatVideoTaskResponse(c, resp)
		} else {
			suppressGatewayStableVideoContentURLs(c, resp)
		}
		markMappedAsyncTaskPoll(c, taskID, asyncTaskKindVideo, resp.Status, resp)
	}
	c.JSON(http.StatusOK, resp)
}

// reconcileVideoStatusAliases applies the routing side effects shared by the
// public and playground status endpoints. It preserves a valid upstream status
// response when a newly revealed alias conflicts, while refusing to publish a
// stable gateway content URL unless that exact media ID is pinned correctly.
func reconcileVideoStatusAliases(c *gin.Context, channelID, lookupTaskID string, resp *model.VideoTaskResponse) (bool, error) {
	registration, persistErr := persistVideoTaskAliases(c, channelID, lookupTaskID, resp)
	if persistErr != nil {
		if errors.Is(persistErr, db.ErrTaskMappingChannelConflict) {
			c.Header("X-Relay-Task-Mapping", "conflict")
			log.Printf("[ASYNC_TASK] successful video status for %s revealed a cross-channel alias conflict: %v", lookupTaskID, persistErr)
			realProviderID := strings.TrimSpace(resp.TaskID)
			if realProviderID == "" || realProviderID == lookupTaskID {
				realProviderID = strings.TrimSpace(resp.ID)
			}
			var conflictPersistErr error
			if realProviderID != "" && realProviderID != lookupTaskID {
				var reqCtx context.Context = context.Background()
				if c != nil && c.Request != nil {
					reqCtx = c.Request.Context()
				}
				conflictPersistErr = recordVideoTaskConflictAliasFn(reqCtx, lookupTaskID, realProviderID)
			}
			if db.GetTaskChannelForKind(resp.ID, asyncTaskKindVideo) != channelID {
				resp.ID = lookupTaskID
			}
			if db.GetTaskChannelForKind(resp.TaskID, asyncTaskKindVideo) != channelID {
				resp.TaskID = lookupTaskID
			}
			if conflictPersistErr != nil {
				var reqCtx context.Context = context.Background()
				if c != nil && c.Request != nil {
					reqCtx = c.Request.Context()
				}
				reg := newAsyncTaskMappingRegistration(reqCtx, channelID, asyncTaskKindVideo, realProviderID, resp.Status, lookupTaskID)
				if lookupMapping := db.GetTaskMappingForKind(lookupTaskID, asyncTaskKindVideo); lookupMapping != nil && lookupMapping.OriginRequestID != "" {
					reg.OriginRequestID = lookupMapping.OriginRequestID
				}
				journaled, journalErr := enqueueAsyncTaskMappingRecoveryFn(reg)
				if journaled && journalErr == nil {
					c.Header("X-Relay-Task-Mapping", "pending")
					log.Printf("[ASYNC_TASK] failed to persist real provider ID %s for video task %s (channel %s): %v; recovery journal is durable", realProviderID, lookupTaskID, channelID, conflictPersistErr)
				} else {
					c.Header("X-Relay-Task-Mapping", "failed")
					log.Printf("[ASYNC_TASK] failed to persist real provider ID %s for video task %s (channel %s): %v; recovery journal enqueue also failed: %v", realProviderID, lookupTaskID, channelID, conflictPersistErr, journalErr)
				}
				return false, nil
			}
			return canExposeGatewayStableVideoContent(resp, channelID), nil
		}
		if registration.empty() {
			// The playground may temporarily route through its explicit channel
			// while a just-created mapping is being recovered. The upstream status
			// remains valid, but no stable gateway URL or parent-log coalescing is
			// allowed until the lookup ID is actually registered.
			c.Header("X-Relay-Task-Mapping", "missing")
			log.Printf("[ASYNC_TASK] preserving successful video status for unregistered task %s without a gateway content URL: %v", lookupTaskID, persistErr)
			return false, nil
		}
		// The provider has already returned a valid task status. Defer only the
		// local alias write; the journal worker restores it without contacting
		// the upstream or turning this poll into a false provider failure.
		deferAsyncTaskMappingPersistence(c, registration, persistErr)
	}
	return canExposeGatewayStableVideoContent(resp, channelID), nil
}

func playgroundVideoResultURL(c *gin.Context, resp *model.VideoTaskResponse, canExposeContent bool) string {
	if resp == nil || model.NormalizeVideoStatus(resp.Status) != model.VideoStatusCompleted {
		return ""
	}
	if canExposeContent {
		if contentTaskID := videoContentTaskID(resp); contentTaskID != "" {
			return resolveAbsoluteURL(c, "/api/playground/video-content/"+contentTaskID)
		}
	}

	candidates := []string{resp.VideoURL, resp.URL}
	for _, item := range resp.Data {
		candidates = append(candidates, item["video_url"], item["url"])
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || isGatewayStableVideoContentURL(c, candidate) {
			continue
		}
		return resolveAbsoluteURL(c, candidate)
	}
	return ""
}

// resolveAbsoluteURL 将相对路径转换为包含 scheme + host 的完整绝对 URL（反向代理感知 X-Forwarded-*）
func resolveAbsoluteURL(c *gin.Context, path string) string {
	if !strings.HasPrefix(path, "/") {
		return path
	}
	scheme := "http"
	if c.Request.TLS != nil || trustedForwardedHTTPS(c) {
		scheme = "https"
	}
	host := trustedForwardedHost(c)
	if host == "" {
		host = c.Request.Host
	}
	return fmt.Sprintf("%s://%s%s", scheme, host, path)
}

// suppressGatewayStableVideoContentURLs removes only URLs that a client would
// resolve back through this gateway's /v1/videos/:id/content route. It is used
// whenever that route cannot be verified for this task, including a
// cross-channel task-ID conflict. Direct provider URLs are preserved so a
// successfully-created task is not misrepresented as a failed request.
func suppressGatewayStableVideoContentURLs(c *gin.Context, resp *model.VideoTaskResponse) {
	if c == nil || resp == nil {
		return
	}
	if isGatewayStableVideoContentURL(c, resp.VideoURL) {
		resp.VideoURL = ""
	}
	if isGatewayStableVideoContentURL(c, resp.URL) {
		resp.URL = ""
	}

	data := make([]map[string]string, 0, len(resp.Data))
	for _, item := range resp.Data {
		if item == nil {
			continue
		}
		for _, key := range []string{"url", "video_url"} {
			if isGatewayStableVideoContentURL(c, item[key]) {
				delete(item, key)
			}
		}
		if len(item) > 0 {
			data = append(data, item)
		}
	}
	if len(data) == 0 {
		resp.Data = nil
		return
	}
	resp.Data = data
}

// canExposeGatewayStableVideoContent confirms that the exact ID which the
// formatter would place in /content is already pinned to this request's
// channel as a video task. It intentionally rejects missing, pending, and
// cross-type mappings instead of guessing from a provider response.
func canExposeGatewayStableVideoContent(resp *model.VideoTaskResponse, channelID string) bool {
	contentTaskID := videoContentTaskID(resp)
	channelID = strings.TrimSpace(channelID)
	if contentTaskID == "" || channelID == "" {
		return false
	}
	mapping := db.GetTaskMappingForKind(contentTaskID, asyncTaskKindVideo)
	return mapping != nil && mapping.ChannelID == channelID
}

func isGatewayStableVideoContentURL(c *gin.Context, rawURL string) bool {
	rawURL = strings.TrimSpace(rawURL)
	if c == nil || rawURL == "" {
		return false
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || !strings.HasPrefix(parsed.Path, "/v1/videos/") || !strings.HasSuffix(parsed.Path, "/content") {
		return false
	}
	if parsed.Host == "" {
		// A relative URL in an API response resolves against the gateway host.
		return strings.HasPrefix(rawURL, "/")
	}
	gatewayHost := trustedForwardedHost(c)
	if gatewayHost == "" {
		gatewayHost = c.Request.Host
	}
	return gatewayHost != "" && strings.EqualFold(parsed.Host, gatewayHost)
}

// formatVideoTaskResponse standardizes video response fields. When a provider
// exposes a playable URL (or declares a task complete), clients receive the
// gateway's stable content URL instead of an upstream CDN URL whose signature
// can expire while a client such as Infinite Canvas keeps it in a document.
func formatVideoTaskResponse(c *gin.Context, resp *model.VideoTaskResponse) {
	if resp == nil {
		return
	}
	if resp.ID == "" && resp.TaskID != "" {
		resp.ID = resp.TaskID
	}
	if resp.TaskID == "" && resp.ID != "" {
		resp.TaskID = resp.ID
	}

	// A provider may expose an API-facing request ID as id and the actual media
	// task ID as task_id. The content handler forwards the path ID upstream, so
	// prefer a valid task_id whenever it is present.
	contentTaskID := videoContentTaskID(resp)
	if contentTaskID != "" && (model.NormalizeVideoStatus(resp.Status) == model.VideoStatusCompleted || videoResponseHasPlayableURL(resp)) {
		stableURL := resolveAbsoluteURL(c, "/v1/videos/"+contentTaskID+"/content")
		resp.VideoURL = stableURL
		resp.URL = stableURL
		if len(resp.Data) == 0 {
			resp.Data = []map[string]string{{"url": stableURL}}
		} else {
			for i := range resp.Data {
				if resp.Data[i] == nil {
					resp.Data[i] = make(map[string]string)
				}
				resp.Data[i]["url"] = stableURL
				if _, hasVideoURL := resp.Data[i]["video_url"]; hasVideoURL {
					resp.Data[i]["video_url"] = stableURL
				}
			}
		}
		return
	}

	resp.VideoURL = resolveAbsoluteURL(c, resp.VideoURL)
	resp.URL = resolveAbsoluteURL(c, resp.URL)

	if resp.URL == "" {
		resp.URL = resp.VideoURL
	}
	if resp.VideoURL == "" && resp.URL != "" {
		resp.VideoURL = resp.URL
	}
	if resp.VideoURL != "" && len(resp.Data) == 0 {
		resp.Data = []map[string]string{{"url": resp.VideoURL}}
	}
}

func videoResponseHasPlayableURL(resp *model.VideoTaskResponse) bool {
	if resp == nil {
		return false
	}
	if strings.TrimSpace(resp.VideoURL) != "" || strings.TrimSpace(resp.URL) != "" {
		return true
	}
	for _, item := range resp.Data {
		if strings.TrimSpace(item["url"]) != "" || strings.TrimSpace(item["video_url"]) != "" {
			return true
		}
	}
	return false
}

func videoContentTaskID(resp *model.VideoTaskResponse) string {
	if resp == nil {
		return ""
	}
	for _, candidate := range []string{strings.TrimSpace(resp.TaskID), strings.TrimSpace(resp.ID)} {
		if db.IsValidTaskID(candidate) {
			return candidate
		}
	}
	return ""
}

func handleGetVideoContent(c *gin.Context) {
	setPublicMediaSecurityHeaders(c)
	taskID := strings.TrimSuffix(strings.TrimSpace(c.Param("id")), ".mp4")
	if !db.IsValidTaskID(taskID) {
		c.JSON(http.StatusBadRequest, model.NewError("任务 ID 无效", "invalid_request_error"))
		return
	}
	if handled, profileErr := profileTaskContent(c, taskID); handled {
		if profileErr != nil && !c.Writer.Written() {
			writeUpstreamError(c, profileErr, "Profile 视频内容获取失败")
		}
		return
	}

	// A registered task is pinned to its creation channel. Never let an
	// untrusted query parameter redirect a public media request to another
	// channel and make the gateway send that channel's credentials upstream.
	// Unlike the legacy playground status endpoint, content delivery has no
	// safe reason to route an unknown ID: accepting channel_id here would let a
	// gateway-key holder turn this endpoint into an arbitrary authenticated
	// upstream probe.  Wait for the durable mapping instead.
	mappedChannelID := db.GetVideoTaskChannel(taskID)
	if mappedChannelID == "" {
		c.JSON(http.StatusNotFound, model.NewError("视频任务未登记", "not_found"))
		return
	}
	var targetChannel *config.UpstreamChannel
	targetChannel, err := resolveTargetChannel(mappedChannelID, "")
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusInternalServerError, model.NewError("解析任务所属渠道失败", "channel_error"))
		return
	}

	adp := adapter.Get(targetChannel.Type)
	if adp == nil {
		writeUpstreamError(c, errors.New("该渠道协议已停用"), "获取视频内容失败")
		return
	}
	providerTaskID := db.GetCanonicalTaskIDForKind(taskID, asyncTaskKindVideo)
	if strings.HasPrefix(taskID, "gt_") {
		providerTaskID = resolveCanonicalProviderTaskID(taskID, asyncTaskKindVideo)
	}
	err = adp.GetVideoContent(c.Request.Context(), targetChannel, providerTaskID, c.Writer, c.Request)
	if err != nil {
		_ = c.Error(err)
		if !c.Writer.Written() {
			writeUpstreamError(c, err, "获取视频内容失败")
		}
	}
}

func handleCreateImageJob(c *gin.Context) {
	rawBytes, modelName, err := readBodyAndModel(c)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, model.NewError("请求格式不正确", "invalid_request_error"))
		return
	}
	if modelName == "" {
		modelName = "gpt-image-2"
	}
	var profileImageReq model.ImageGenerationRequest
	if json.Unmarshal(rawBytes, &profileImageReq) == nil {
		if profileImageReq.Model == "" {
			profileImageReq.Model = modelName
		}
		if profileResp, _, handled, profileErr := profileEngineImageCreate(c, &profileImageReq); handled {
			if profileErr != nil {
				writeUpstreamError(c, profileErr, "Profile 图片上游请求失败")
				return
			}
			c.JSON(http.StatusAccepted, profileResp)
			return
		}
	}

	var resp interface{}
	var successChannel *config.UpstreamChannel
	err = service.DefaultDispatcher.ExecuteWithPolicy(c.Request.Context(), modelName, "images", service.RetryCreateTask, func() bool {
		return resp == nil
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		var rErr error
		resp, rErr = adp.CreateImageJob(c.Request.Context(), ch, rawBytes)
		if rErr == nil && resp != nil {
			successChannel = ch
		}
		return rErr
	})

	if err != nil {
		writeUpstreamError(c, err, "创建图片任务失败")
		return
	}

	if successChannel != nil {
		if taskAlias, status, taskIDs := collectImageJobTaskDetails(resp); taskAlias != "" {
			if gtID, disambiguated := disambiguateAsyncTaskIDs(successChannel.ID, asyncTaskKindImage, taskAlias, taskIDs); disambiguated {
				canonicalProviderID := taskAlias
				setImageJobResponseIDs(resp, gtID)
				registerAsyncTaskMappings(c, successChannel.ID, asyncTaskKindImage, canonicalProviderID, status, imageJobLookupIDs([]string{gtID})...)
			} else {
				registerAsyncTaskMappings(c, successChannel.ID, asyncTaskKindImage, taskAlias, status, imageJobLookupIDs(taskIDs)...)
			}
		}
	}

	c.JSON(http.StatusAccepted, resp)
}

func handleGetImageJob(c *gin.Context) {
	taskID := strings.TrimSpace(c.Param("id"))
	if !db.IsValidTaskID(taskID) {
		c.JSON(http.StatusBadRequest, model.NewError("任务 ID 无效", "invalid_request_error"))
		return
	}
	if profileResp, handled, profileErr := profileTaskStatus(c, imageTaskIDPrefix+taskID, asyncTaskKindImage); handled {
		if profileErr != nil {
			writeUpstreamError(c, profileErr, "Profile 图片状态查询失败")
			return
		}
		c.JSON(http.StatusOK, profileResp)
		return
	}

	targetChannel, err := resolveImageTargetChannel("", imageTaskIDPrefix+taskID)
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusInternalServerError, model.NewError("解析任务所属渠道失败", "channel_error"))
		return
	}

	adp := adapter.Get(targetChannel.Type)
	if adp == nil {
		writeUpstreamError(c, errors.New("该渠道协议已停用"), "获取图片任务失败")
		return
	}
	providerTaskID := resolveImageProviderTaskID(taskID)
	resp, err := adp.GetImageJob(c.Request.Context(), targetChannel, providerTaskID)
	if err != nil {
		writeUpstreamError(c, err, "获取图片任务失败")
		return
	}
	if resp != nil && taskID != providerTaskID {
		setImageJobResponseIDs(resp, taskID)
	}
	registration, persistErr := persistImageTaskAliases(c, targetChannel.ID, taskID, resp)
	if persistErr != nil {
		if errors.Is(persistErr, db.ErrTaskMappingChannelConflict) {
			// The upstream status is still valid. Keep it successful while making
			// the conflicting provider alias visible for operator investigation.
			c.Header("X-Relay-Task-Mapping", "conflict")
			log.Printf("[ASYNC_TASK] successful image status for %s revealed a cross-channel alias conflict: %v", taskID, persistErr)
		} else if registration.empty() {
			_ = c.Error(persistErr)
			c.JSON(http.StatusInternalServerError, model.NewError("保存图片任务路由失败", "task_mapping_error"))
			return
		} else {
			// Do not turn an already successful image status poll into a retryable
			// failure. The durable journal restores only the local alias mapping.
			deferAsyncTaskMappingPersistence(c, registration, persistErr)
		}
	}
	_, status := imageJobDetails(resp)
	markMappedAsyncTaskPoll(c, imageTaskIDPrefix+taskID, asyncTaskKindImage, status, resp)
	c.JSON(http.StatusOK, resp)
}

// -------------------------------------------------------------
// 全链路审计日志组件
// -------------------------------------------------------------

type auditResponseWriter struct {
	gin.ResponseWriter
	bodyBuf    bytes.Buffer
	maxCapture int
	captured   int
}

func (w *auditResponseWriter) Write(b []byte) (int, error) {
	if w.captured < w.maxCapture {
		toWrite := len(b)
		if w.captured+toWrite > w.maxCapture {
			toWrite = w.maxCapture - w.captured
		}
		w.bodyBuf.Write(b[:toWrite])
		w.captured += toWrite
	}
	return w.ResponseWriter.Write(b)
}

func (w *auditResponseWriter) WriteString(s string) (int, error) {
	if w.captured < w.maxCapture {
		toWrite := len(s)
		if w.captured+toWrite > w.maxCapture {
			toWrite = w.maxCapture - w.captured
		}
		w.bodyBuf.WriteString(s[:toWrite])
		w.captured += toWrite
	}
	return w.ResponseWriter.WriteString(s)
}

func (w *auditResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func isMediaDownloadPath(path string) bool {
	if strings.HasPrefix(path, "/v1/media/") {
		return true
	}
	if (strings.HasPrefix(path, "/v1/videos/") || strings.HasPrefix(path, "/v1/video/")) &&
		(strings.HasSuffix(path, "/content") || strings.HasSuffix(path, "/content.mp4")) {
		return true
	}
	if strings.HasPrefix(path, "/api/playground/video-content/") {
		return true
	}
	return false
}

func auditMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		isAPICall := (path == "/v1" || strings.HasPrefix(path, "/v1/")) && !isMediaDownloadPath(path)
		isPlaygroundRun := (path == "/api/playground/run" || path == "/api/playground/chat") && c.Request.Method == http.MethodPost
		isAsyncStatusPoll := c.Request.Method == http.MethodGet && (path == "/api/playground/video-status" || path == "/api/playground/image-status")

		// Only model calls generate audit logs:
		// 1. External /v1 API calls (excluding static media asset downloads)
		// 2. Web console playground model execution (/api/playground/run, /api/playground/chat)
		// 3. Status polls for async playground image/video tasks (coalesced into parent run)
		// Page operations and static media streaming execute directly without polluting model call logs.
		if !isAPICall && !isPlaygroundRun && !isAsyncStatusPoll {
			c.Next()
			if c.Writer.Status() < 400 {
				if pending, ok := c.Get("pending_port"); ok {
					config.SetPort(pending.(int))
				}
				if deleted, ok := c.Get("deleted_channel_id"); ok {
					db.InvalidateTaskCacheForChannel(deleted.(string))
					service.DefaultDispatcher.RemoveChannel(deleted.(string))
				}
				if changed, ok := c.Get("changed_channel_id"); ok {
					db.NotifyChannelsChanged(changed.(string))
					service.DefaultDispatcher.SyncRemoteModels(context.Background())
				}
				if path == "/api/settings" {
					if days, parseErr := strconv.Atoi(db.GetSetting("audit_retention_days", "30")); parseErr == nil {
						_ = audit.Cleanup(days)
					}
				}
			}
			return
		}
		if db.DB == nil {
			c.Next()
			return
		}
		kind := "api_call"
		auditWriter := &auditResponseWriter{
			ResponseWriter: c.Writer,
			maxCapture:     audit.MaxFieldBytes + 1,
		}
		c.Writer = auditWriter
		run := func(entry *audit.AuditEntry, tx *gorm.DB) error {
			c.Header("X-Request-ID", entry.ID)
			ctx := audit.WithAudit(c.Request.Context(), entry)
			if tx != nil {
				ctx = db.WithTx(ctx, tx)
			}
			// Preserve a caller-provided idempotency key for task-creation requests.
			if key := strings.TrimSpace(c.GetHeader("Idempotency-Key")); key != "" && len(key) <= 255 && !strings.ContainsAny(key, "\r\n") {
				ctx = context.WithValue(ctx, adapter.CtxIdempotencyKey, key)
			}
			c.Request = c.Request.WithContext(ctx)
			c.Next()
			var respErr error
			if len(c.Errors) > 0 {
				respErr = c.Errors.Last().Err
			} else if c.Request.Context().Err() != nil {
				respErr = c.Request.Context().Err()
			}
			statusCode := c.Writer.Status()
			respBytes := auditWriter.bodyBuf.Bytes()
			if errors.Is(respErr, context.Canceled) && !c.Writer.Written() {
				// No response reached a disconnected client. Avoid recording Gin's
				// default 200 or a locally constructed cancellation body as if it did.
				statusCode = 0
				respBytes = nil
			}
			entry.RecordResult(statusCode, respBytes, respErr)
			// A handler marks only authenticated, mapped, successful async status
			// polls. Coalescing is fail-safe: if its short transaction fails, the
			// already-recorded child request remains visible as its own audit row.
			if coalesceErr := audit.CoalesceMarkedAsyncTaskPoll(entry); coalesceErr != nil {
				_ = c.Error(coalesceErr)
			}
			if tx != nil {
				if auditErr := entry.WriteError(); auditErr != nil {
					return auditErr
				}
			}
			if len(c.Errors) > 0 {
				return c.Errors.Last()
			}
			return nil
		}
		entry, err := audit.Start(kind, c.Request.RemoteAddr, c.Request.Method, path, c.Request.Header)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "审计日志存储暂不可用，请稍后重试"})
			return
		}
		_ = run(entry, nil)
	}
}

func handleGetLogs(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "50"))
	query := audit.ListQuery{Page: page, PageSize: pageSize, Kind: strings.TrimSpace(c.Query("kind")), ChannelID: strings.TrimSpace(c.Query("channel_id")), Model: strings.TrimSpace(c.Query("model")), Outcome: strings.TrimSpace(c.Query("outcome")), Search: strings.TrimSpace(c.Query("q"))}
	if value := c.Query("from"); value != "" {
		if parsed, err := time.Parse(time.RFC3339, value); err == nil {
			query.From = &parsed
		}
	}
	if value := c.Query("to"); value != "" {
		if parsed, err := time.Parse(time.RFC3339, value); err == nil {
			query.To = &parsed
		}
	}
	rows, total, err := audit.List(query)
	if err != nil {
		internalError(c, err, "读取审计日志失败，请稍后重试")
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": rows, "page": query.Page, "page_size": query.PageSize, "total": total})
}

func handleGetLogDetail(c *gin.Context) {
	detail, err := audit.GetDetail(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "审计日志不存在"})
		return
	}
	for i := range detail.MediaAssets {
		asset := &detail.MediaAssets[i]
		if asset.Status != db.MediaAssetAvailable {
			continue
		}
		publicURL, linkErr := mediaPublicURLForAsset(c, &asset.MediaAsset)
		if linkErr == nil {
			asset.PublicURL = publicURL
		} else {
			// Keep unrecoverable historical assets explicit. Never substitute the
			// authenticated admin content route or a newly generated capability.
			asset.PublicURLError = "媒体公开链接不可恢复，未存储有效访问令牌"
		}
	}
	c.JSON(http.StatusOK, detail)
}

func handleDeleteLogs(c *gin.Context) {
	if c.Query("confirm") != "DELETE" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请填写 DELETE 以确认清空审计日志"})
		return
	}
	preserveID := audit.RequestID(c.Request.Context())
	if err := audit.DeleteAllContext(c.Request.Context(), preserveID); err != nil {
		_ = c.Error(err)
		internalError(c, err, "清空审计日志失败，请稍后重试")
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
