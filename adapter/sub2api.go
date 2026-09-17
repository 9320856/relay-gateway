package adapter

import (
	"context"
	"errors"

	"relay-gateway/config"
)

type Sub2APIAdapter struct {
	*OpenAIAdapter
}

func init() {
	Register(AdapterMeta{
		Type:        "sub2api",
		Name:        "Sub2API (订阅池/思考流)",
		Description: "专用于 Claude Pro/Team、ChatGPT Plus 等订阅账号池，完美支持 Extended Thinking 思考流无损透传",
		Protocols:   []string{"chat", "embeddings", "models"},
		DefaultURL:  "http://localhost:8080/v1",
	}, NewSub2APIAdapter())
}

func NewSub2APIAdapter() *Sub2APIAdapter {
	return &Sub2APIAdapter{
		OpenAIAdapter: NewOpenAIAdapter(),
	}
}

func (a *Sub2APIAdapter) FetchModels(ctx context.Context, channel *config.UpstreamChannel) ([]string, error) {
	models, err := a.OpenAIAdapter.FetchModels(ctx, channel)
	if err == nil && len(models) > 0 {
		return models, nil
	}

	// 仅当上游明确不支持 /models 端点 (404/405) 时返回默认模型列表
	// 网络故障、5xx 等真实错误必须上报以触发调度器熔断，避免流量打到已宕机节点
	if err != nil {
		var upErr *UpstreamHTTPError
		if errors.As(err, &upErr) && (upErr.StatusCode == 404 || upErr.StatusCode == 405) {
			// 上游不支持 /models 端点，返回 Sub2API 常见默认模型
		} else {
			return nil, err // 网络故障 / 5xx / 超时等，直接上报
		}
	}

	// 若远程返回空列表或上游明确不支持 /models，提供常见的 Sub2API 默认订阅模型列表作为备用
	defaultSub2ApiModels := []string{
		"claude-3-7-sonnet",
		"claude-3-7-sonnet-thought",
		"claude-3-5-sonnet-20241022",
		"claude-3-5-sonnet",
		"claude-3-5-haiku",
		"claude-3-opus",
		"gpt-4o",
		"gpt-4o-mini",
		"o1",
		"o1-preview",
		"o1-mini",
		"o3-mini",
	}
	return defaultSub2ApiModels, nil
}
