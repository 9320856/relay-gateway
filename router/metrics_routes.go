package router

import (
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/metrics"
)

// RuntimeMetrics is process-local telemetry used by the rollout gate. It
// contains no request bodies, credentials, URLs, or other user payloads.
var RuntimeMetrics = metrics.New()

func metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		RuntimeMetrics.ObserveStatus(c.Writer.Status(), time.Since(started))
	}
}

func handleMetrics(c *gin.Context) {
	RuntimeMetrics.Handler().ServeHTTP(c.Writer, c.Request)
}
