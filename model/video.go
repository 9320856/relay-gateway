package model

import "strings"

// 标准视频任务状态常量，各适配器从上游获取状态后必须映射为以下标准值
const (
	VideoStatusQueued     = "queued"
	VideoStatusProcessing = "processing"
	VideoStatusCompleted  = "completed"
	VideoStatusFailed     = "failed"
)

// NormalizeVideoStatus 将上游各平台的非标状态映射为网关标准状态
func NormalizeVideoStatus(raw string) string {
	return NormalizeTaskStatus(raw)
}

// NormalizeTaskStatus is the canonical status vocabulary shared by video,
// image and audit task flows. In particular, cancelled/canceled is terminal
// failure everywhere instead of being rendered as a provider-specific state.
func NormalizeTaskStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "succeeded", "success", "complete", "completed", "done":
		return VideoStatusCompleted
	case "running", "in_progress", "in-progress", "generating", "processing":
		return VideoStatusProcessing
	case "queued", "pending", "submitted", "waiting":
		return VideoStatusQueued
	case "failed", "error", "errored", "cancelled", "canceled", "rejected", "expired":
		return VideoStatusFailed
	default:
		return strings.ToLower(strings.TrimSpace(raw)) // 未知状态透传，不丢信息
	}
}

type VideoGenerationRequest struct {
	Model         string   `json:"model"`
	Prompt        string   `json:"prompt"`
	Seconds       string   `json:"seconds,omitempty"`
	Size          string   `json:"size,omitempty"`
	Quality       string   `json:"quality,omitempty"`
	Images        []string `json:"images,omitempty"`
	FirstFrame    string   `json:"first_frame,omitempty"`
	LastFrame     string   `json:"last_frame,omitempty"`
	GenerateAudio bool     `json:"generate_audio,omitempty"`
}

type VideoTaskResponse struct {
	ID        string              `json:"id"`
	TaskID    string              `json:"task_id,omitempty"`
	Status    string              `json:"status"` // "queued", "processing", "completed", "failed"
	Progress  *float64            `json:"progress,omitempty"`
	VideoURL  string              `json:"video_url,omitempty"`
	URL       string              `json:"url,omitempty"`
	Data      []map[string]string `json:"data,omitempty"`
	Error     interface{}         `json:"error,omitempty"`
	CreatedAt int64               `json:"created_at,omitempty"`
}
