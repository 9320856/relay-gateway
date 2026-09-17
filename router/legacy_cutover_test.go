package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/service"
)

func TestGlobalLegacyDisableRejectsUnboundOpenAIImageJob(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/global-cutover-openai.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "")
	t.Setenv("RELAY_ENABLE_PROFILE_IMAGE_ENGINE", "")
	channel := &db.ChannelModel{
		ID: "openai-global-cutover-unbound", Name: "OpenAI", Type: "openai",
		BaseURL: "https://openai.example/v1", APIKey: "test-key", Enabled: true,
		Priority: 1, Weight: 1, ModelsRaw: "gpt-image-1",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	service.DefaultDispatcher.ResetBreaker(channel.ID)
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/jobs", strings.NewReader(`{"model":"gpt-image-1","prompt":"test"}`))
	handleCreateImageJob(c)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("global Legacy disable status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "创建图片任务失败") {
		t.Fatalf("unexpected global Legacy disable response: %q", recorder.Body.String())
	}
}
