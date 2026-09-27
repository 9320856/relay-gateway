package router

import (
	"bytes"
	"crypto/subtle"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"relay-gateway/audit"
	"relay-gateway/db"
	"relay-gateway/security"
	"relay-gateway/web"
)

const (
	adminUserContextKey    = "admin_user"
	sessionTokenContextKey = "admin_session_token"
)

func requestLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := int64(64 << 20)
		if strings.HasPrefix(c.Request.URL.Path, "/api/auth/") {
			limit = 64 << 10
		} else if strings.HasPrefix(c.Request.URL.Path, "/api/") {
			limit = 1 << 20
		}
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		}
		c.Next()
	}
}

func securityHeadersMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Referrer-Policy", "no-referrer")
		c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data: https: blob:; media-src 'self' https: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		c.Next()
	}
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := strings.TrimSpace(c.GetHeader("Origin"))
		if c.Request.URL.Path == "/v1" || strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			if origin != "" {
				c.Header("Access-Control-Allow-Origin", "*")
			}
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key, anthropic-version, anthropic-beta, Idempotency-Key, Range")
			c.Header("Access-Control-Expose-Headers", "Content-Range, Accept-Ranges, Content-Length, Content-Type")
			c.Header("Access-Control-Allow-Methods", "GET, POST, HEAD, OPTIONS")
		} else if origin != "" {
			parsed, err := url.Parse(origin)
			if err != nil || !strings.EqualFold(parsed.Host, c.Request.Host) {
				if c.Request.Method == http.MethodOptions {
					c.AbortWithStatus(http.StatusForbidden)
					return
				}
			} else {
				c.Header("Access-Control-Allow-Origin", origin)
				c.Header("Access-Control-Allow-Credentials", "true")
				c.Header("Vary", "Origin")
				c.Header("Access-Control-Allow-Headers", "Content-Type, X-CSRF-Token")
				c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			}
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func handleRootPage(c *gin.Context) {
	if !security.HasAdminContext(c.Request.Context()) {
		c.Redirect(http.StatusFound, "/setup")
		return
	}
	if _, err := currentSession(c); err != nil {
		c.Redirect(http.StatusFound, "/login")
		return
	}
	c.Redirect(http.StatusFound, "/dashboard")
}

func handleSetupPage(c *gin.Context) {
	if security.HasAdminContext(c.Request.Context()) {
		c.Redirect(http.StatusFound, "/dashboard")
		return
	}
	servePage(c, web.SetupHTML)
}

func handleLoginPage(c *gin.Context) {
	if !security.HasAdminContext(c.Request.Context()) {
		c.Redirect(http.StatusFound, "/setup")
		return
	}
	if _, err := currentSession(c); err == nil {
		c.Redirect(http.StatusFound, "/dashboard")
		return
	}
	servePage(c, web.LoginHTML)
}

func handleDashboardPage(c *gin.Context) {
	serveAuthenticatedPage(c, web.DashboardHTML)
}

func handleLogsPage(c *gin.Context) {
	serveAuthenticatedPage(c, web.LogsHTML)
}

func handleProfilesPage(c *gin.Context) {
	serveAuthenticatedPage(c, web.ProfilesHTML)
}

func serveAuthenticatedPage(c *gin.Context, content []byte) {
	if !security.HasAdminContext(c.Request.Context()) {
		c.Redirect(http.StatusFound, "/setup")
		return
	}
	if _, err := currentSession(c); err != nil {
		c.Redirect(http.StatusFound, "/login")
		return
	}
	servePage(c, content)
}

func servePage(c *gin.Context, content []byte) {
	c.Header("Cache-Control", "no-store")
	c.Data(http.StatusOK, "text/html; charset=utf-8", content)
}

func currentSession(c *gin.Context) (*security.Session, error) {
	token, err := c.Cookie(security.SessionCookieName)
	if err != nil || strings.TrimSpace(token) == "" {
		return nil, security.ErrInvalidSession
	}
	return security.ValidateSessionContext(c.Request.Context(), token)
}

func setSessionCookie(c *gin.Context, session *security.Session) {
	secure := c.Request.TLS != nil || trustedForwardedHTTPS(c)
	http.SetCookie(c.Writer, &http.Cookie{Name: security.SessionCookieName, Value: session.Token, Path: "/", MaxAge: int(security.SessionLifetime.Seconds()), Expires: session.ExpiresAt, HttpOnly: true, Secure: secure, SameSite: http.SameSiteStrictMode})
}

func clearSessionCookie(c *gin.Context) {
	http.SetCookie(c.Writer, &http.Cookie{Name: security.SessionCookieName, Value: "", Path: "/", MaxAge: -1, Expires: time.Unix(1, 0), HttpOnly: true, Secure: c.Request.TLS != nil || trustedForwardedHTTPS(c), SameSite: http.SameSiteStrictMode})
}

func trustedForwardedHTTPS(c *gin.Context) bool {
	// X-Forwarded-Proto is only meaningful when the application is explicitly
	// configured behind a trusted proxy. Without that opt-in, a client cannot
	// influence the Secure cookie decision by sending a spoofed header.
	if strings.TrimSpace(os.Getenv("RELAY_TRUST_PROXY")) != "1" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(strings.Split(c.GetHeader("X-Forwarded-Proto"), ",")[0]), "https")
}

func adminSessionMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		session, err := currentSession(c)
		if err != nil {
			clearSessionCookie(c)
			audit.AddEvent(c.Request.Context(), "auth_failed", audit.EventData{Message: "administrator session is missing or expired"})
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "需要管理员登录"})
			return
		}
		token, _ := c.Cookie(security.SessionCookieName)
		c.Set(adminUserContextKey, session.User)
		c.Set(sessionTokenContextKey, token)
		audit.AddEvent(c.Request.Context(), "auth_succeeded", audit.EventData{Message: "administrator session verified"})
		c.Next()
	}
}

func csrfMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == http.MethodGet || c.Request.Method == http.MethodHead || c.Request.Method == http.MethodOptions {
			c.Next()
			return
		}
		if origin := strings.TrimSpace(c.GetHeader("Origin")); origin != "" {
			parsed, err := url.Parse(origin)
			if err != nil || !strings.EqualFold(parsed.Host, c.Request.Host) {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "跨域请求被拒绝"})
				return
			}
		}
		token, _ := c.Get(sessionTokenContextKey)
		if err := security.ValidateCSRFContext(c.Request.Context(), token.(string), c.GetHeader("X-CSRF-Token")); err != nil {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "CSRF 校验失败，请刷新页面后重试"})
			return
		}
		if c.Request.Body != nil && strings.Contains(strings.ToLower(c.GetHeader("Content-Type")), "application/json") {
			body, err := io.ReadAll(c.Request.Body)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "请求内容过大"})
				return
			}
			c.Request.Body = io.NopCloser(bytes.NewReader(body))
			if entry := audit.FromContext(c.Request.Context()); entry != nil {
				entry.SetReqBody(body, "")
			}
		}
		c.Next()
	}
}

func gatewayAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := extractBearerToken(c)
		if security.ValidateGatewayTokenContext(c.Request.Context(), token) {
			audit.AddEvent(c.Request.Context(), "auth_succeeded", audit.EventData{Message: "gateway token verified"})
			c.Next()
			return
		}
		audit.AddEvent(c.Request.Context(), "auth_failed", audit.EventData{Message: "gateway token rejected"})
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": gin.H{"message": "API 密钥无效", "type": "invalid_api_key"}})
	}
}

func handleAuthStatus(c *gin.Context) {
	_, err := currentSession(c)
	initialized := security.HasAdminContext(c.Request.Context())
	c.JSON(http.StatusOK, gin.H{
		"initialized":           initialized,
		"authenticated":         err == nil,
		"setup_secret_required": !initialized && strings.TrimSpace(os.Getenv("RELAY_SETUP_SECRET")) != "",
	})
}

func handleSetupAdmin(c *gin.Context) {
	if security.HasAdminContext(c.Request.Context()) {
		c.JSON(http.StatusConflict, gin.H{"error": "管理员已初始化，请直接登录"})
		return
	}
	if origin := strings.TrimSpace(c.GetHeader("Origin")); origin != "" {
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || !strings.EqualFold(parsed.Host, c.Request.Host) {
			c.JSON(http.StatusForbidden, gin.H{"error": "跨域初始化请求被拒绝"})
			return
		}
	}
	if !setupRequestAllowed(c) {
		c.JSON(http.StatusForbidden, gin.H{"error": "初始化口令缺失或不正确，请输入服务器配置的初始化口令"})
		return
	}
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "初始化请求格式不正确"})
		return
	}
	session, gatewayToken, err := security.SetupAdminContext(c.Request.Context(), input.Username, input.Password, c.Request.RemoteAddr, c.GetHeader("User-Agent"))
	if err != nil {
		_ = c.Error(err)
		status := http.StatusBadRequest
		if errors.Is(err, security.ErrAlreadySetup) {
			status = http.StatusConflict
		}
		if errors.Is(err, security.ErrAuthBusy) {
			c.Header("Retry-After", "1")
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "密码处理繁忙，请稍后重试"})
			return
		}
		c.JSON(status, gin.H{"error": chineseErrorMessage(err, "初始化失败，请检查用户名和密码后重试")})
		return
	}
	setSessionCookie(c, session)
	c.JSON(http.StatusCreated, gin.H{"status": "ok", "username": session.User.Username, "csrf_token": session.CSRFToken, "gateway_api_key": gatewayToken})
}

func setupRequestAllowed(c *gin.Context) bool {
	// First-time setup works from any address. An operator may protect it with
	// a preconfigured secret; loopback and proxy requests cannot bypass it.
	configured := strings.TrimSpace(os.Getenv("RELAY_SETUP_SECRET"))
	if configured == "" {
		return true
	}
	provided := strings.TrimSpace(c.GetHeader("X-Relay-Setup-Secret"))
	return provided != "" && subtle.ConstantTimeCompare([]byte(provided), []byte(configured)) == 1
}

func handleLogin(c *gin.Context) {
	if !security.HasAdminContext(c.Request.Context()) {
		c.JSON(http.StatusConflict, gin.H{"error": "管理员尚未初始化"})
		return
	}
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "登录请求格式不正确"})
		return
	}
	attempt, retry := security.BeginLoginAttempt(c.Request.RemoteAddr, input.Username)
	if attempt == nil {
		c.Header("Retry-After", security.RetryAfterSeconds(retry))
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "登录失败次数过多，请稍后再试"})
		return
	}
	defer attempt.Cancel()
	session, err := security.LoginContext(c.Request.Context(), input.Username, input.Password, c.Request.RemoteAddr, c.GetHeader("User-Agent"))
	if errors.Is(err, security.ErrAuthBusy) {
		c.Header("Retry-After", "1")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "密码处理繁忙，请稍后重试"})
		return
	}
	if c.Request.Context().Err() != nil {
		return
	}
	if err == nil || errors.Is(err, security.ErrInvalidLogin) {
		attempt.Finish(err == nil)
	}
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "用户名或密码错误"})
		return
	}
	setSessionCookie(c, session)
	c.JSON(http.StatusOK, gin.H{"status": "ok", "username": session.User.Username, "csrf_token": session.CSRFToken})
}

func handleAuthMe(c *gin.Context) {
	user := c.MustGet(adminUserContextKey).(db.AdminUserModel)
	token := c.MustGet(sessionTokenContextKey).(string)
	csrf, err := security.CSRFTokenContext(c.Request.Context(), token)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "登录会话已过期，请重新登录"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"username": user.Username, "csrf_token": csrf, "gateway_token": security.GatewayTokenInfoContext(c.Request.Context())})
}

func handleLogout(c *gin.Context) {
	token := c.MustGet(sessionTokenContextKey).(string)
	if err := security.LogoutContext(c.Request.Context(), token); err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "退出登录失败，请稍后重试"})
		return
	}
	clearSessionCookie(c)
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func handleUpdateCredentials(c *gin.Context) {
	user := c.MustGet(adminUserContextKey).(db.AdminUserModel)
	var input struct {
		CurrentPassword string `json:"current_password"`
		NewUsername     string `json:"new_username"`
		NewPassword     string `json:"new_password"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "账户信息请求格式不正确"})
		return
	}
	session, err := security.UpdateCredentialsContext(c.Request.Context(), user.ID, input.CurrentPassword, input.NewUsername, input.NewPassword, c.Request.RemoteAddr, c.GetHeader("User-Agent"))
	if err != nil {
		if errors.Is(err, security.ErrCredentialsChanged) {
			c.JSON(http.StatusConflict, gin.H{"error": "账号凭据已被其他请求修改，请重新登录后再试"})
			return
		}
		if errors.Is(err, security.ErrAuthBusy) {
			c.Header("Retry-After", "1")
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "密码处理繁忙，请稍后重试"})
			return
		}
		_ = c.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"error": chineseErrorMessage(err, "账户信息更新失败，请检查输入后重试")})
		return
	}
	setSessionCookie(c, session)
	c.JSON(http.StatusOK, gin.H{"status": "ok", "username": session.User.Username, "csrf_token": session.CSRFToken})
}

func handleRotateGatewayToken(c *gin.Context) {
	token, err := security.RotateGatewayTokenContext(c.Request.Context())
	if err != nil {
		_ = c.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "网关密钥轮换失败，请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "gateway_api_key": token, "message": "请立即复制此密钥；它不会再次显示。"})
}
