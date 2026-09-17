package model

import "encoding/json"

type ImageGenerationRequest struct {
	Model             string                 `json:"model"`
	Prompt            string                 `json:"prompt"`
	N                 *int                   `json:"n,omitempty"`
	Quality           string                 `json:"quality,omitempty"`
	ResponseFormat    string                 `json:"response_format,omitempty"` // "url" or "b64_json"
	Size              string                 `json:"size,omitempty"`            // "1024x1024", "16:9", etc.
	Style             string                 `json:"style,omitempty"`
	User              string                 `json:"user,omitempty"`
	AspectRatio       string                 `json:"aspect_ratio,omitempty"`
	Resolution        string                 `json:"resolution,omitempty"` // "1k", "2k", "4k"
	Upscale           string                 `json:"upscale,omitempty"`    // "2k", "4k", "none"
	OutputFormat      string                 `json:"output_format,omitempty"`
	OutputCompression *int                   `json:"output_compression,omitempty"`
	Background        string                 `json:"background,omitempty"`
	Moderation        string                 `json:"moderation,omitempty"`
	NegativePrompt    string                 `json:"negative_prompt,omitempty"`
	Seed              *int64                 `json:"seed,omitempty"`
	Images            []string               `json:"images,omitempty"`
	InputImages       []string               `json:"input_images,omitempty"`
	Stream            *bool                  `json:"stream,omitempty"`
	PartialImages     *int                   `json:"partial_images,omitempty"`
	ExtraParams       map[string]interface{} `json:"-"`
}

func (r *ImageGenerationRequest) UnmarshalJSON(data []byte) error {
	type Alias ImageGenerationRequest
	var aux struct {
		Alias
		AltAspectRatio string `json:"aspectRatio"`
		AltRatio       string `json:"ratio"`
		AltAR          string `json:"ar"`
		ImageSize      string `json:"image_size"`
		AltImageSize   string `json:"imageSize"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*r = ImageGenerationRequest(aux.Alias)
	if r.AspectRatio == "" {
		if aux.AltAspectRatio != "" {
			r.AspectRatio = aux.AltAspectRatio
		} else if aux.AltRatio != "" {
			r.AspectRatio = aux.AltRatio
		} else if aux.AltAR != "" {
			r.AspectRatio = aux.AltAR
		}
	}
	if r.Size == "" {
		if aux.ImageSize != "" {
			r.Size = aux.ImageSize
		} else if aux.AltImageSize != "" {
			r.Size = aux.AltImageSize
		}
	}

	// 捕获未声明的额外参数
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err == nil {
		delete(raw, "model")
		delete(raw, "prompt")
		delete(raw, "n")
		delete(raw, "quality")
		delete(raw, "response_format")
		delete(raw, "size")
		delete(raw, "style")
		delete(raw, "user")
		delete(raw, "aspect_ratio")
		delete(raw, "aspectRatio")
		delete(raw, "ratio")
		delete(raw, "ar")
		delete(raw, "resolution")
		delete(raw, "upscale")
		delete(raw, "output_format")
		delete(raw, "output_compression")
		delete(raw, "background")
		delete(raw, "moderation")
		delete(raw, "negative_prompt")
		delete(raw, "seed")
		delete(raw, "images")
		delete(raw, "input_images")
		delete(raw, "image_size")
		delete(raw, "imageSize")
		delete(raw, "stream")
		delete(raw, "partial_images")
		if len(raw) > 0 {
			r.ExtraParams = raw
		}
	}
	return nil
}

func (r *ImageGenerationRequest) ToMap() map[string]interface{} {
	m := make(map[string]interface{})
	for k, v := range r.ExtraParams {
		m[k] = v
	}
	m["model"] = r.Model
	m["prompt"] = r.Prompt
	if r.N != nil {
		m["n"] = *r.N
	}
	if r.Quality != "" {
		m["quality"] = r.Quality
	}
	if r.ResponseFormat != "" {
		m["response_format"] = r.ResponseFormat
	}
	if r.Size != "" {
		m["size"] = r.Size
	}
	if r.Style != "" {
		m["style"] = r.Style
	}
	if r.User != "" {
		m["user"] = r.User
	}
	if r.AspectRatio != "" {
		m["aspect_ratio"] = r.AspectRatio
	}
	if r.Resolution != "" {
		m["resolution"] = r.Resolution
	}
	if r.Upscale != "" {
		m["upscale"] = r.Upscale
	}
	if r.OutputFormat != "" {
		m["output_format"] = r.OutputFormat
	}
	if r.OutputCompression != nil {
		m["output_compression"] = *r.OutputCompression
	}
	if r.Background != "" {
		m["background"] = r.Background
	}
	if r.Moderation != "" {
		m["moderation"] = r.Moderation
	}
	if r.NegativePrompt != "" {
		m["negative_prompt"] = r.NegativePrompt
	}
	if r.Seed != nil {
		m["seed"] = *r.Seed
	}
	if len(r.Images) > 0 {
		m["images"] = r.Images
	}
	if len(r.InputImages) > 0 {
		m["input_images"] = r.InputImages
	}
	if r.Stream != nil {
		m["stream"] = *r.Stream
	}
	if r.PartialImages != nil {
		m["partial_images"] = *r.PartialImages
	}
	return m
}

type ImageData struct {
	URL           string `json:"url,omitempty"`
	B64JSON       string `json:"b64_json,omitempty"`
	RevisedPrompt string `json:"revised_prompt,omitempty"`
}

type ImageResponse struct {
	Created int64       `json:"created"`
	Data    []ImageData `json:"data"`
}
