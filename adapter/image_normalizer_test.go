package adapter

import (
	"encoding/json"
	"testing"

	"relay-gateway/model"
)

func TestExtractImageIntent(t *testing.T) {
	tests := []struct {
		name       string
		req        model.ImageGenerationRequest
		expectedAR string
		expectedTR string
	}{
		{
			name: "Explicit AspectRatio 16:9",
			req: model.ImageGenerationRequest{
				AspectRatio: "16:9",
			},
			expectedAR: "16:9",
			expectedTR: "1k",
		},
		{
			name: "Composite Size 16:9-2k",
			req: model.ImageGenerationRequest{
				Size: "16:9-2k",
			},
			expectedAR: "16:9",
			expectedTR: "2k",
		},
		{
			name: "Pixel Dimensions 1920x1080",
			req: model.ImageGenerationRequest{
				Size: "1920x1080",
			},
			expectedAR: "16:9",
			expectedTR: "1k",
		},
		{
			name: "Pixel Dimensions 2048x1152 triggers 2K",
			req: model.ImageGenerationRequest{
				Size: "2048x1152",
			},
			expectedAR: "16:9",
			expectedTR: "2k",
		},
		{
			name: "Resolution 2k flag",
			req: model.ImageGenerationRequest{
				AspectRatio: "9:16",
				Resolution:  "2k",
			},
			expectedAR: "9:16",
			expectedTR: "2k",
		},
		{
			name: "Model name suffix -4k",
			req: model.ImageGenerationRequest{
				Model:       "gpt-image-2-4k",
				AspectRatio: "4:3",
			},
			expectedAR: "4:3",
			expectedTR: "4k",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			intent := ExtractImageIntent(&tc.req)
			if intent.AspectRatio != tc.expectedAR {
				t.Errorf("expected AspectRatio %s, got %s", tc.expectedAR, intent.AspectRatio)
			}
			if intent.Tier != tc.expectedTR {
				t.Errorf("expected Tier %s, got %s", tc.expectedTR, intent.Tier)
			}
		})
	}
}

func TestNormalizeGPTImage2(t *testing.T) {
	// Test 1: 16:9 1k -> size 1536x864, no aspect_ratio, no resolution
	req1 := &model.ImageGenerationRequest{
		Model:       "gpt-image-2",
		Prompt:      "a majestic lion",
		AspectRatio: "16:9",
	}
	p1 := NormalizeImageRequest(req1, "newapi")
	if p1["size"] != "1280x720" {
		t.Errorf("expected size 1280x720, got %v", p1["size"])
	}
	if _, ok := p1["aspect_ratio"]; ok {
		t.Errorf("aspect_ratio must not exist for gpt-image-2")
	}
	if _, ok := p1["resolution"]; ok {
		t.Errorf("resolution must not exist for gpt-image-2")
	}

	// Test 2: 16:9 2k on base model -> upscale: "2k", size: "2048x1152"
	req2 := &model.ImageGenerationRequest{
		Model:       "gpt-image-2",
		Prompt:      "cyberpunk city",
		AspectRatio: "16:9",
		Resolution:  "2k",
	}
	p2 := NormalizeImageRequest(req2, "newapi")
	if p2["size"] != "2048x1152" {
		t.Errorf("expected size 2048x1152, got %v", p2["size"])
	}
	if p2["upscale"] != "2k" {
		t.Errorf("expected upscale 2k, got %v", p2["upscale"])
	}

	// Test 3: 16:9 on gpt-image-2-2k -> no upscale to prevent conflict
	req3 := &model.ImageGenerationRequest{
		Model:       "gpt-image-2-2k",
		Prompt:      "mountain sunrise",
		AspectRatio: "16:9",
	}
	p3 := NormalizeImageRequest(req3, "newapi")
	if p3["size"] != "2048x1152" {
		t.Errorf("expected size 2048x1152, got %v", p3["size"])
	}
	if _, ok := p3["upscale"]; ok {
		t.Errorf("upscale must not be present when model is gpt-image-2-2k")
	}

	// Test 4: Transparent background sanitized to opaque
	req4 := &model.ImageGenerationRequest{
		Model:      "gpt-image-2",
		Prompt:     "transparent icon",
		Background: "transparent",
	}
	p4 := NormalizeImageRequest(req4, "newapi")
	if p4["background"] != "opaque" {
		t.Errorf("expected background opaque, got %v", p4["background"])
	}

	// Test 5: Verify all dimensions are multiples of 16
	ratios := []string{"1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3", "21:9"}
	tiers := []string{"1k", "2k", "4k"}
	for _, r := range ratios {
		for _, tier := range tiers {
			req := &model.ImageGenerationRequest{
				Model:       "gpt-image-2",
				AspectRatio: r,
				Resolution:  tier,
			}
			p := NormalizeImageRequest(req, "newapi")
			size, ok := p["size"].(string)
			if !ok {
				t.Fatalf("size missing for %s %s", r, tier)
			}
			var w, h int
			_, err := parseDimensions(size, &w, &h)
			if err != nil {
				t.Fatalf("invalid size string %s: %v", size, err)
			}
			if w%16 != 0 || h%16 != 0 {
				t.Errorf("size %s for ratio %s tier %s is not 16-multiple", size, r, tier)
			}
		}
	}
}

func TestGPTImage2NonStandardRatioUsesSharedSizeDecision(t *testing.T) {
	tests := []struct {
		name     string
		tier     string
		expected string
	}{
		{name: "1k", expected: "1360x720"},
		{name: "2k", tier: "2k", expected: "2720x1440"},
		// The 4k size stays below both the 3,840px edge and 8,294,400px
		// pixel limits while preserving the requested 17:9 ratio.
		{name: "4k", tier: "4k", expected: "3808x2016"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &model.ImageGenerationRequest{
				Model:       "gpt-image-2",
				AspectRatio: "17:9",
				Resolution:  tc.tier,
			}

			jsonPayload := NormalizeImageRequest(req, "openai")
			if got := jsonPayload["size"]; got != tc.expected {
				t.Fatalf("JSON normalization size = %v, want %s", got, tc.expected)
			}

			if got := ResolveNormalizedImageSize(req); got != tc.expected {
				t.Fatalf("multipart request size = %s, want %s", got, tc.expected)
			}

			// Legacy multipart clients commonly put the ratio in size rather
			// than aspect_ratio. It must take the same path too.
			if got := ResolveNormalizedSize("gpt-image-2", "17:9", tc.tier, ""); got != tc.expected {
				t.Fatalf("legacy multipart size = %s, want %s", got, tc.expected)
			}
		})
	}
}

func parseDimensions(s string, w, h *int) (bool, error) {
	var width, height int
	_, err := parseInts(s, &width, &height)
	if err != nil {
		return false, err
	}
	*w = width
	*h = height
	return true, nil
}

func parseInts(s string, w, h *int) (bool, error) {
	for i := 0; i < len(s); i++ {
		if s[i] == 'x' || s[i] == 'X' {
			w64, err1 := json.Number(s[:i]).Int64()
			h64, err2 := json.Number(s[i+1:]).Int64()
			if err1 == nil && err2 == nil {
				*w = int(w64)
				*h = int(h64)
				return true, nil
			}
		}
	}
	return false, nil
}

func TestNormalizeGrokImagine(t *testing.T) {
	// From size 1920x1080 -> aspect_ratio: "16:9", resolution: "1k", size: "1280x720"
	req1 := &model.ImageGenerationRequest{
		Model:  "grok-imagine",
		Prompt: "red sports car",
		Size:   "1920x1080",
	}
	p1 := NormalizeImageRequest(req1, "newapi")
	if p1["aspect_ratio"] != "16:9" {
		t.Errorf("expected aspect_ratio 16:9, got %v", p1["aspect_ratio"])
	}
	if p1["resolution"] != "1k" {
		t.Errorf("expected resolution 1k, got %v", p1["resolution"])
	}
	if p1["size"] != "1280x720" {
		t.Errorf("expected size 1280x720 for grok-imagine, got %v", p1["size"])
	}

	// From 2048x1152 -> aspect_ratio: "16:9", resolution: "2k", size: "2048x1152"
	req2 := &model.ImageGenerationRequest{
		Model:  "grok-imagine-image",
		Prompt: "futuristic city",
		Size:   "2048x1152",
	}
	p2 := NormalizeImageRequest(req2, "newapi")
	if p2["aspect_ratio"] != "16:9" {
		t.Errorf("expected aspect_ratio 16:9, got %v", p2["aspect_ratio"])
	}
	if p2["resolution"] != "2k" {
		t.Errorf("expected resolution 2k, got %v", p2["resolution"])
	}
	if p2["size"] != "2048x1152" {
		t.Errorf("expected size 2048x1152 for grok-imagine 2k, got %v", p2["size"])
	}

	// An aspect-ratio-only request must use the nearest supported ratio and
	// keep the generated size consistent with that ratio.  In particular,
	// 21:9 must not collapse to 1:1 merely because no pixel dimensions were
	// supplied.
	req3 := &model.ImageGenerationRequest{Model: "grok-imagine", AspectRatio: "21:9"}
	p3 := NormalizeImageRequest(req3, "newapi")
	if p3["aspect_ratio"] != "16:9" {
		t.Errorf("expected unsupported 21:9 to map to 16:9, got %v", p3["aspect_ratio"])
	}
	if p3["size"] != "1280x720" {
		t.Errorf("expected mapped 16:9 size 1280x720, got %v", p3["size"])
	}
}

func TestNormalizeDallE3(t *testing.T) {
	// Landscape -> 1792x1024
	req1 := &model.ImageGenerationRequest{
		Model:       "dall-e-3",
		Prompt:      "landscape oil painting",
		AspectRatio: "16:9",
	}
	p1 := NormalizeImageRequest(req1, "openai")
	if p1["size"] != "1792x1024" {
		t.Errorf("expected 1792x1024, got %v", p1["size"])
	}
	if _, ok := p1["aspect_ratio"]; ok {
		t.Errorf("aspect_ratio must not exist for dall-e-3")
	}

	// Portrait -> 1024x1792
	req2 := &model.ImageGenerationRequest{
		Model:       "dall-e-3",
		Prompt:      "portrait photo",
		AspectRatio: "9:16",
	}
	p2 := NormalizeImageRequest(req2, "openai")
	if p2["size"] != "1024x1792" {
		t.Errorf("expected 1024x1792, got %v", p2["size"])
	}

	// Square -> 1024x1024
	req3 := &model.ImageGenerationRequest{
		Model:       "dall-e-3",
		Prompt:      "square avatar",
		AspectRatio: "1:1",
	}
	p3 := NormalizeImageRequest(req3, "openai")
	if p3["size"] != "1024x1024" {
		t.Errorf("expected 1024x1024, got %v", p3["size"])
	}
}

func TestNormalizeGenericModel(t *testing.T) {
	seed := int64(123456)
	req := &model.ImageGenerationRequest{
		Model:          "flux-dev",
		Prompt:         "fantasy tree",
		AspectRatio:    "16:9",
		Seed:           &seed,
		NegativePrompt: "blurry, low quality",
	}
	p := NormalizeImageRequest(req, "newapi")
	if p["aspect_ratio"] != "16:9" {
		t.Errorf("expected aspect_ratio 16:9, got %v", p["aspect_ratio"])
	}
	if p["size"] != "1280x720" {
		t.Errorf("expected calculated size 1280x720, got %v", p["size"])
	}
	if p["seed"] != int64(123456) {
		t.Errorf("expected seed preserved, got %v", p["seed"])
	}
	if p["negative_prompt"] != "blurry, low quality" {
		t.Errorf("expected negative_prompt preserved, got %v", p["negative_prompt"])
	}
}
