package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
)

func TestChannelsMigrationReportEndpoint(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-channels-route.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/migration/channels", nil)
	handleChannelsMigrationReport(c)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"ready":true`) {
		t.Fatalf("generic migration report response: code=%d body=%q", recorder.Code, recorder.Body.String())
	}
}
