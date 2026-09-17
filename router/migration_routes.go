package router

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"relay-gateway/migration"
)

// handleChannelsMigrationReport exposes the all-channel retirement gate. It
// is read-only and intentionally does not probe or mutate any provider.
func handleChannelsMigrationReport(c *gin.Context) {
	report, err := migration.AuditChannels(c.Request.Context())
	if err != nil {
		internalError(c, err, "迁移检查报告生成失败，请稍后重试")
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": report})
}
