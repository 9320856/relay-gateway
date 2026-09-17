package adapter

import (
	"context"
	"net/http"
	"os"
	"sync"

	"relay-gateway/config"
	"relay-gateway/model"
)

type AdapterMeta struct {
	Type        string   `json:"type"`        // 如 "sub2api", "newapi", "openai"
	Name        string   `json:"name"`        // 显示名称
	Description string   `json:"description"` // 描述
	Protocols   []string `json:"protocols"`   // ["chat", "messages", "images", "audio", "video", "embeddings"]
	DefaultURL  string   `json:"default_url"` // 默认 Base URL
}

func (m AdapterMeta) Supports(protocol string) bool {
	for _, candidate := range m.Protocols {
		if candidate == protocol {
			return true
		}
	}
	return false
}

type Adapter interface {
	ChatCompletions(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, isStream bool, w http.ResponseWriter) error
	// Responses proxies the OpenAI Responses API (/v1/responses).  Providers
	// that embed OpenAIAdapter inherit this implementation unchanged.
	Responses(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, isStream bool, w http.ResponseWriter) error
	AnthropicMessages(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, isStream bool, w http.ResponseWriter) error
	Embeddings(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, w http.ResponseWriter) error
	ImagesGenerations(ctx context.Context, channel *config.UpstreamChannel, req *model.ImageGenerationRequest) (*model.ImageResponse, error)
	ImagesEdits(ctx context.Context, channel *config.UpstreamChannel, body []byte, contentType string, w http.ResponseWriter) error
	AudioSpeech(ctx context.Context, channel *config.UpstreamChannel, req *model.AudioSpeechRequest, w http.ResponseWriter) error
	CreateVideo(ctx context.Context, channel *config.UpstreamChannel, req *model.VideoGenerationRequest) (*model.VideoTaskResponse, error)
	GetVideo(ctx context.Context, channel *config.UpstreamChannel, taskID string) (*model.VideoTaskResponse, error)
	GetVideoContent(ctx context.Context, channel *config.UpstreamChannel, taskID string, w http.ResponseWriter, req *http.Request) error
	CreateImageJob(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte) (interface{}, error)
	GetImageJob(ctx context.Context, channel *config.UpstreamChannel, taskID string) (interface{}, error)
	FetchModels(ctx context.Context, channel *config.UpstreamChannel) ([]string, error)
	Moderations(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, w http.ResponseWriter) error
	AudioTranscriptions(ctx context.Context, channel *config.UpstreamChannel, endpoint string, body []byte, contentType string, w http.ResponseWriter) error
	CountTokens(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, w http.ResponseWriter) error
}

// BaseAdapter 通用适配器基类，任何中转适配器均可直接组合继承。
// Deprecated: use OpenAIAdapter or a protocol-specific capability directly.
type BaseAdapter = OpenAIAdapter

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Adapter)
	metas      = make(map[string]AdapterMeta)
)

func Register(meta AdapterMeta, a Adapter) {
	allowed := map[string]bool{"openai": true, "anthropic": true, "newapi": true, "sub2api": true}
	if !allowed[meta.Type] || a == nil {
		return
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[meta.Type] = a
	metas[meta.Type] = meta
}

func Get(channelType string) Adapter {
	// The global retirement switch is intentionally fail-closed for all
	// compatibility adapters. Profile routing does not call this registry, so
	// migrated channels continue to work while unbound channels get an explicit
	// error instead of silently re-entering Legacy.
	if os.Getenv("RELAY_DISABLE_LEGACY") == "1" {
		return nil
	}
	registryMu.RLock()
	defer registryMu.RUnlock()
	if a, ok := registry[channelType]; ok {
		return a
	}
	return nil
}

func GetMeta(channelType string) (AdapterMeta, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	m, ok := metas[channelType]
	return m, ok
}

func ListMetas() []AdapterMeta {
	registryMu.RLock()
	defer registryMu.RUnlock()
	// 稳定有序的推荐展现列表
	preferredOrder := []string{"openai", "anthropic", "newapi", "sub2api"}
	var list []AdapterMeta
	seen := make(map[string]bool)
	for _, t := range preferredOrder {
		if m, ok := metas[t]; ok {
			list = append(list, m)
			seen[t] = true
		}
	}
	for t, m := range metas {
		if !seen[t] {
			list = append(list, m)
		}
	}
	return list
}
