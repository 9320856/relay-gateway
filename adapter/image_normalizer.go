package adapter

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"relay-gateway/model"
)

var (
	ratioRegex       = regexp.MustCompile(`^(\d+)\s*[:/：]\s*(\d+)$`)
	dimensionRegex   = regexp.MustCompile(`^(\d+)\s*[xX*×]\s*(\d+)$`)
	tierSizeRegex    = regexp.MustCompile(`^(\d+)\s*[:/：]\s*(\d+)-(2k|4k)$`)
	promptRatioRegex = regexp.MustCompile(`(?i)(?:^|[^\d])(16[:/：]9|9[:/：]16|4[:/：]3|3[:/：]4|3[:/：]2|2[:/：]3|21[:/：]9|1[:/：]1)(?:[^\d]|$)`)
)

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// 16 像素倍数对齐
func align16(dim int) int {
	aligned := (dim / 16) * 16
	if aligned < 256 {
		return 256
	}
	if aligned > 3840 {
		return 3840
	}
	return aligned
}

// 标准 16 倍数尺寸映射矩阵 (100% 兼容 gpt-image-2 官方约束)
var gptImage2StandardSizes = map[string]map[string]string{
	"1:1": {
		"1k": "1024x1024",
		"2k": "2048x2048",
		"4k": "2880x2880",
	},
	"16:9": {
		"1k": "1280x720",
		"2k": "2048x1152",
		"4k": "3840x2160",
	},
	"9:16": {
		"1k": "720x1280",
		"2k": "1152x2048",
		"4k": "2160x3840",
	},
	"4:3": {
		"1k": "1152x864",
		"2k": "2048x1536",
		"4k": "2880x2160",
	},
	"3:4": {
		"1k": "864x1152",
		"2k": "1536x2048",
		"4k": "2160x2880",
	},
	"3:2": {
		"1k": "1248x832",
		"2k": "2048x1360",
		"4k": "3264x2176",
	},
	"2:3": {
		"1k": "832x1248",
		"2k": "1360x2048",
		"4k": "2176x3264",
	},
	"21:9": {
		"1k": "1568x672",
		"2k": "3136x1344",
		"4k": "3840x1648",
	},
}

var grokSupportedAspectRatios = []string{"1:1", "16:9", "9:16", "4:3", "3:4", "2:3", "3:2"}

// findClosestGrokRatio 寻找最接近的 Grok 官方支持比例
func findClosestGrokRatio(w, h int) string {
	if w <= 0 || h <= 0 {
		return "1:1"
	}
	targetVal := float64(w) / float64(h)
	bestRatio := "1:1"
	minDiff := math.MaxFloat64

	for _, r := range grokSupportedAspectRatios {
		parts := strings.Split(r, ":")
		rw, _ := strconv.Atoi(parts[0])
		rh, _ := strconv.Atoi(parts[1])
		rVal := float64(rw) / float64(rh)
		diff := math.Abs(targetVal - rVal)
		if diff < minDiff {
			minDiff = diff
			bestRatio = r
		}
	}
	return bestRatio
}

type ParsedImageIntent struct {
	AspectRatio string // e.g. "16:9"
	Width       int
	Height      int
	Tier        string // "1k", "2k", "4k"
	RawSize     string
}

// ExtractImageIntent 从用户请求参数中解析真实的比例、像素及分辨率档位意图
func ExtractImageIntent(req *model.ImageGenerationRequest) ParsedImageIntent {
	intent := ParsedImageIntent{
		Tier: "1k",
	}

	sizeStr := strings.TrimSpace(req.Size)
	aspectStr := strings.TrimSpace(req.AspectRatio)
	qualityStr := strings.ToLower(strings.TrimSpace(req.Quality))
	resolutionStr := strings.ToLower(strings.TrimSpace(req.Resolution))
	upscaleStr := strings.ToLower(strings.TrimSpace(req.Upscale))

	// 1. 解析清晰度档位 (Tier)
	if resolutionStr == "4k" || upscaleStr == "4k" || qualityStr == "4k" || strings.Contains(strings.ToLower(req.Model), "-4k") {
		intent.Tier = "4k"
	} else if resolutionStr == "2k" || upscaleStr == "2k" || qualityStr == "2k" || qualityStr == "medium" || qualityStr == "hd" || strings.Contains(strings.ToLower(req.Model), "-2k") {
		intent.Tier = "2k"
	}

	// 2. 检查 "16:9-2k" 复合尺寸写法
	if m := tierSizeRegex.FindStringSubmatch(sizeStr); len(m) == 4 {
		intent.AspectRatio = fmt.Sprintf("%s:%s", m[1], m[2])
		intent.Tier = m[3]
		return intent
	}

	// 3. 优先解析显式指定的 aspect_ratio
	if m := ratioRegex.FindStringSubmatch(aspectStr); len(m) == 3 {
		w, _ := strconv.Atoi(m[1])
		h, _ := strconv.Atoi(m[2])
		g := gcd(w, h)
		if g > 0 {
			intent.AspectRatio = fmt.Sprintf("%d:%d", w/g, h/g)
		}
	}

	// 4. 解析 size 字符串（支持纯比率 "16:9" 或具体像素 "1920x1080"）
	if sizeStr != "" && sizeStr != "auto" {
		if m := ratioRegex.FindStringSubmatch(sizeStr); len(m) == 3 {
			w, _ := strconv.Atoi(m[1])
			h, _ := strconv.Atoi(m[2])
			g := gcd(w, h)
			if g > 0 {
				intent.AspectRatio = fmt.Sprintf("%d:%d", w/g, h/g)
			}
		} else if m := dimensionRegex.FindStringSubmatch(sizeStr); len(m) == 3 {
			w, _ := strconv.Atoi(m[1])
			h, _ := strconv.Atoi(m[2])
			intent.Width = w
			intent.Height = h
			intent.RawSize = fmt.Sprintf("%dx%d", w, h)
			g := gcd(w, h)
			if g > 0 {
				intent.AspectRatio = fmt.Sprintf("%d:%d", w/g, h/g)
			}
			// 根据长边自动推断档位
			maxDim := w
			if h > maxDim {
				maxDim = h
			}
			if maxDim >= 3840 && intent.Tier == "1k" {
				intent.Tier = "4k"
			} else if maxDim >= 2048 && intent.Tier == "1k" {
				intent.Tier = "2k"
			}
		}
	}

	// 5. 若未显式提取出特定比例（或请求未指定 size/aspect_ratio），从 prompt 中提取可能声明的比例
	if intent.AspectRatio == "" || (intent.AspectRatio == "1:1" && sizeStr == "" && aspectStr == "") {
		if m := promptRatioRegex.FindStringSubmatch(req.Prompt); len(m) == 2 {
			cleanRatio := strings.ReplaceAll(m[1], "：", ":")
			cleanRatio = strings.ReplaceAll(cleanRatio, "/", ":")
			parts := strings.Split(cleanRatio, ":")
			if len(parts) == 2 {
				w, _ := strconv.Atoi(parts[0])
				h, _ := strconv.Atoi(parts[1])
				g := gcd(w, h)
				if g > 0 {
					intent.AspectRatio = fmt.Sprintf("%d:%d", w/g, h/g)
				}
			}
		} else if strings.Contains(req.Prompt, "横屏") {
			intent.AspectRatio = "16:9"
		} else if strings.Contains(req.Prompt, "竖屏") {
			intent.AspectRatio = "9:16"
		}
	}

	if intent.AspectRatio == "" {
		intent.AspectRatio = "1:1"
	}
	return intent
}

// standardImageSize returns a provider-approved size for the standard aspect
// ratios. Keep this lookup in one place so every normalisation path uses the
// same size matrix.
func standardImageSize(intent ParsedImageIntent) string {
	if table, ok := gptImage2StandardSizes[intent.AspectRatio]; ok {
		return table[intent.Tier]
	}
	return ""
}

// resolveGPTImage2Size turns both standard and non-standard ratios into a
// valid gpt-image-2 pixel size. JSON image requests and multipart image edits
// must use this exact decision so that the transport format cannot change the
// rendered dimensions.
func resolveGPTImage2Size(intent ParsedImageIntent) string {
	if size := standardImageSize(intent); size != "" {
		return size
	}

	parts := strings.Split(intent.AspectRatio, ":")
	if len(parts) == 2 {
		widthRatio, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
		heightRatio, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
		if widthRatio > 0 && heightRatio > 0 {
			// 4k uses the maximum supported pixel area (2880²), rather than a
			// 3840² square. This keeps non-standard ratios below the upstream
			// 8,294,400-pixel limit while the longest edge remains <= 3840.
			basePixels := 1024
			switch intent.Tier {
			case "2k":
				basePixels = 2048
			case "4k":
				basePixels = 2880
			}

			unit := int(math.Round(math.Sqrt(float64(basePixels*basePixels)/(float64(widthRatio)*float64(heightRatio)))/16.0)) * 16
			maxRatioEdge := widthRatio
			if heightRatio > maxRatioEdge {
				maxRatioEdge = heightRatio
			}
			maxUnit := (3840 / maxRatioEdge / 16) * 16
			if maxUnit >= 16 && unit > maxUnit {
				unit = maxUnit
			}
			if unit >= 16 {
				return fmt.Sprintf("%dx%d", widthRatio*unit, heightRatio*unit)
			}
		}
	}

	if intent.Width > 0 && intent.Height > 0 {
		return fmt.Sprintf("%dx%d", align16(intent.Width), align16(intent.Height))
	}
	return "1024x1024"
}

// NormalizeImageRequest 依据目标模型平台与特性，自适应重构合规的生图 Payload
func NormalizeImageRequest(req *model.ImageGenerationRequest, channelType string) map[string]interface{} {
	payload := req.ToMap()
	modelName := strings.ToLower(strings.TrimSpace(req.Model))
	intent := ExtractImageIntent(req)

	isGPTImage2 := strings.Contains(modelName, "gpt-image-2")
	isGrok := strings.Contains(modelName, "grok-imagine")
	isDallE := strings.Contains(modelName, "dall-e")

	// ==========================================
	// 1. gpt-image-2 系列模型 (OpenAI 架构)
	// ==========================================
	if isGPTImage2 {
		// 严禁包含 aspect_ratio 与 resolution（上游硬性校验直接 400 阻断）
		delete(payload, "aspect_ratio")
		delete(payload, "resolution")

		// 尺寸规则必须为 16 的倍数，长宽比 <= 3:1。
		payload["size"] = resolveGPTImage2Size(intent)

		// 安全处理 upscale 与 -2k/-4k 冲突：若模型自带 -2k/-4k，不得再设置 upscale
		if strings.Contains(modelName, "-2k") || strings.Contains(modelName, "-4k") {
			delete(payload, "upscale")
		} else if intent.Tier == "2k" || intent.Tier == "4k" {
			payload["upscale"] = intent.Tier
		}

		// gpt-image-2 不支持 transparent background，只支持 opaque 或 auto
		if bg, ok := payload["background"].(string); ok && strings.ToLower(bg) == "transparent" {
			payload["background"] = "opaque"
		}

		return payload
	}

	// ==========================================
	// 2. grok-imagine 系列模型 (xAI 架构 / 中转聚合兼容)
	// ==========================================
	if isGrok {
		// 清理非标流式参数
		delete(payload, "stream")
		delete(payload, "partial_images")

		closestRatio := intent.AspectRatio
		// aspect_ratio-only requests (for example 21:9) do not populate
		// intent.Width/Height.  Derive dimensions before selecting the nearest
		// Grok-supported ratio; otherwise findClosestGrokRatio(0, 0) silently
		// turns every unsupported ratio into 1:1.
		ratioWidth, ratioHeight := intent.Width, intent.Height
		if (ratioWidth <= 0 || ratioHeight <= 0) && intent.AspectRatio != "" {
			parts := strings.Split(intent.AspectRatio, ":")
			if len(parts) == 2 {
				ratioWidth, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
				ratioHeight, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
			}
		}
		isSupported := false
		for _, r := range grokSupportedAspectRatios {
			if r == intent.AspectRatio {
				isSupported = true
				break
			}
		}
		if !isSupported {
			closestRatio = findClosestGrokRatio(ratioWidth, ratioHeight)
		}
		payload["aspect_ratio"] = closestRatio

		// 分辨率档位
		if intent.Tier == "2k" || intent.Tier == "4k" {
			payload["resolution"] = "2k"
		} else {
			payload["resolution"] = "1k"
		}

		// 尺寸规范化：中转渠道依赖 size 传达实际像素分辨率与比例（若无 size 会回退至默认非标比例）
		finalSize := standardImageSize(ParsedImageIntent{AspectRatio: closestRatio, Tier: intent.Tier})
		if finalSize == "" {
			if ratioWidth > 0 && ratioHeight > 0 {
				finalSize = fmt.Sprintf("%dx%d", align16(ratioWidth), align16(ratioHeight))
			} else {
				finalSize = "1024x1024"
			}
		}
		payload["size"] = finalSize
		return payload
	}

	// ==========================================
	// 3. DALL-E 3 系列模型 (OpenAI 经典接口)
	// ==========================================
	if isDallE {
		delete(payload, "aspect_ratio")
		delete(payload, "resolution")
		delete(payload, "upscale")

		// DALL-E 3 仅支持三种尺寸
		parts := strings.Split(intent.AspectRatio, ":")
		if len(parts) == 2 {
			wR, _ := strconv.Atoi(parts[0])
			hR, _ := strconv.Atoi(parts[1])
			if wR > hR {
				payload["size"] = "1792x1024"
			} else if hR > wR {
				payload["size"] = "1024x1792"
			} else {
				payload["size"] = "1024x1024"
			}
		} else {
			payload["size"] = "1024x1024"
		}
		return payload
	}

	// ==========================================
	// 4. 通用模型 (New-API / FLUX / SD / Midjourney)
	// ==========================================
	// 同时保留合法算出的 size 与 aspect_ratio，最大化兼容各种第三方反向代理
	if intent.Width > 0 && intent.Height > 0 {
		payload["size"] = fmt.Sprintf("%dx%d", intent.Width, intent.Height)
	} else if size := standardImageSize(intent); size != "" {
		payload["size"] = size
	}
	if payload["aspect_ratio"] == nil || payload["aspect_ratio"] == "" {
		payload["aspect_ratio"] = intent.AspectRatio
	}

	return payload
}

// ResolveNormalizedImageSize derives the size that a multipart image edit
// should forward. It shares gpt-image-2 handling with NormalizeImageRequest.
func ResolveNormalizedImageSize(req *model.ImageGenerationRequest) string {
	if req == nil {
		return ""
	}
	intent := ExtractImageIntent(req)
	m := strings.ToLower(strings.TrimSpace(req.Model))
	if strings.Contains(m, "gpt-image-2") {
		return resolveGPTImage2Size(intent)
	}
	if intent.Width > 0 && intent.Height > 0 {
		return fmt.Sprintf("%dx%d", align16(intent.Width), align16(intent.Height))
	}
	return req.Size
}

// ResolveNormalizedSize is retained for callers that only have the legacy
// multipart fields. New callers should pass the full request so aspect_ratio,
// resolution, and upscale participate in the same decision as JSON requests.
func ResolveNormalizedSize(modelName, size, quality, prompt string) string {
	return ResolveNormalizedImageSize(&model.ImageGenerationRequest{
		Model:   modelName,
		Size:    size,
		Quality: quality,
		Prompt:  prompt,
	})
}
