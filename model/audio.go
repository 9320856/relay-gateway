package model

type AudioSpeechRequest struct {
	Model          string   `json:"model"`
	Input          string   `json:"input"`
	Voice          string   `json:"voice"`
	ResponseFormat string   `json:"response_format,omitempty"` // "mp3", "opus", "aac", "flac", "wav", "pcm"
	Speed          *float64 `json:"speed,omitempty"`
}
