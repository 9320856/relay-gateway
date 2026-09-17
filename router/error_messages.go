package router

import (
	"errors"
	"net/http"
	"strings"

	"relay-gateway/adapter"
	"relay-gateway/protocol"

	"github.com/gin-gonic/gin"
)

// requestError returns a stable Chinese message for malformed client input.
// The original parser error can contain implementation details and is retained
// in Gin's error chain for operators instead of being sent to callers.
func requestError(c *gin.Context, err error, message string) {
	if err != nil {
		_ = c.Error(err)
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": message})
}

func internalError(c *gin.Context, err error, message string) {
	if err != nil {
		_ = c.Error(err)
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": message})
}

func chineseErrorMessage(err error, fallback string) string {
	if err == nil {
		return fallback
	}
	var upstreamErr *adapter.UpstreamHTTPError
	if errors.As(err, &upstreamErr) && strings.TrimSpace(upstreamErr.Body) != "" {
		return strings.TrimSpace(upstreamErr.Body)
	}
	var execErr *protocol.ExecutorError
	if errors.As(err, &execErr) && strings.TrimSpace(execErr.Body) != "" {
		return strings.TrimSpace(execErr.Body)
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case errors.Is(err, http.ErrNotSupported), strings.Contains(message, "not found"):
		return "请求的资源不存在或当前不可用"
	case strings.Contains(message, "timeout"), strings.Contains(message, "deadline exceeded"):
		return "请求处理超时，请稍后重试"
	case strings.Contains(message, "invalid"), strings.Contains(message, "malformed"):
		return "请求参数格式不正确"
	case strings.Contains(message, "disabled"):
		return "该渠道当前已停用"
	default:
		return fallback
	}
}
