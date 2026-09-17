package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/metrics"
)

func TestMetricsMiddlewareRecordsCompletedStatus(t *testing.T) {
	previous := RuntimeMetrics
	RuntimeMetrics = metrics.New()
	t.Cleanup(func() { RuntimeMetrics = previous })

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(metricsMiddleware())
	r.GET("/ok", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	r.GET("/fail", func(c *gin.Context) { c.Status(http.StatusBadGateway) })
	for _, path := range []string{"/ok", "/fail"} {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
	s := RuntimeMetrics.Snapshot()
	if s.Requests != 2 || s.Successes != 1 || s.Errors != 1 || s.LatencySamples != 2 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestMetricsHandlerDoesNotExposeSensitiveData(t *testing.T) {
	previous := RuntimeMetrics
	RuntimeMetrics = metrics.New()
	RuntimeMetrics.ObserveRequest(time.Millisecond, true)
	t.Cleanup(func() { RuntimeMetrics = previous })

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	handleMetrics(c)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Content-Type") == "" {
		t.Fatalf("metrics response: code=%d content-type=%q", recorder.Code, recorder.Header().Get("Content-Type"))
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"api_key", "password", "token", "provider_task_id"} {
		if _, found := payload[secret]; found {
			t.Fatalf("metrics response exposes key %q: %s", secret, recorder.Body.String())
		}
	}
}
