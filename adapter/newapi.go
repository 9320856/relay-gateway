package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"relay-gateway/config"
	"relay-gateway/model"
)

type NewAPIAdapter struct {
	*OpenAIAdapter
}

func init() {
	adp := NewNewAPIAdapter()
	Register(AdapterMeta{
		Type:        "newapi",
		Name:        "NewAPI 中转",
		Description: "NewAPI 聚合中转协议，支持聊天、图片、语音、向量和视频任务",
		Protocols:   []string{"chat", "responses", "images", "audio", "video", "embeddings", "models"},
		DefaultURL:  "https://api.newapi.com/v1",
	}, adp)
}

func NewNewAPIAdapter() *NewAPIAdapter {
	return &NewAPIAdapter{
		OpenAIAdapter: NewOpenAIAdapter(),
	}
}

// CreateVideo 提交 New-API / One-API 视频任务 POST /v1/videos/generations
func (a *NewAPIAdapter) CreateVideo(ctx context.Context, channel *config.UpstreamChannel, req *model.VideoGenerationRequest) (*model.VideoTaskResponse, error) {
	reqCopy := *req
	if target, ok := channel.ModelMap[reqCopy.Model]; ok && target != "" {
		reqCopy.Model = target
	}

	bodyBytes, err := json.Marshal(&reqCopy)
	if err != nil {
		return nil, err
	}

	targetURL := a.NormalizeURL(channel.BaseURL, "/videos/generations")
	var taskResp model.VideoTaskResponse

	err = a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		respBytes, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(respBytes, &taskResp); err != nil {
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(respBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		if taskResp.ID == "" && taskResp.TaskID == "" {
			if hasLegacyAsyncBusinessError(respBytes, taskResp.Status, taskResp.Error) {
				return &UpstreamHTTPError{
					StatusCode:  resp.StatusCode,
					ContentType: resp.Header.Get("Content-Type"),
					Body:        string(respBytes),
					RetryAfter:  resp.Header.Get("Retry-After"),
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	if taskResp.ID == "" && taskResp.TaskID != "" {
		taskResp.ID = taskResp.TaskID
	}
	taskResp.Status = model.NormalizeVideoStatus(taskResp.Status)
	return &taskResp, nil
}

// GetVideo 查询 New-API / One-API 视频任务 GET /v1/videos/generations/{task_id}
func (a *NewAPIAdapter) GetVideo(ctx context.Context, channel *config.UpstreamChannel, taskID string) (*model.VideoTaskResponse, error) {
	targetURL := a.NormalizeURL(channel.BaseURL, "/videos/generations/"+taskID)
	var taskResp model.VideoTaskResponse

	err := a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(httpReq, channel, key)
		return httpReq, nil
	}, func(resp *http.Response, key string) error {
		defer resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			errBytes, readErr := readUpstreamErrorBody(resp.Body)
			if readErr != nil {
				return readErr
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(errBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		respBytes, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(respBytes, &taskResp); err != nil {
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        string(respBytes),
				RetryAfter:  resp.Header.Get("Retry-After"),
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if taskResp.ID == "" {
		taskResp.ID = taskID
	}
	taskResp.Status = model.NormalizeVideoStatus(taskResp.Status)

	if taskResp.Status == model.VideoStatusCompleted && taskResp.VideoURL == "" {
		taskResp.VideoURL = "/v1/videos/" + taskID + "/content"
	}
	if taskResp.URL == "" {
		taskResp.URL = taskResp.VideoURL
	}
	return &taskResp, nil
}

// GetVideoContent 流式反代 New-API 视频下载 GET /v1/videos/generations/{task_id}/content
func (a *NewAPIAdapter) GetVideoContent(ctx context.Context, channel *config.UpstreamChannel, taskID string, w http.ResponseWriter, req *http.Request) error {
	targetURL := a.NormalizeURL(channel.BaseURL, "/videos/generations/"+taskID+"/content")
	return a.OpenAIAdapter.ForwardVideoContent(ctx, channel, targetURL, req, w)
}
