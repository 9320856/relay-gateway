package media

import (
	"net/url"
	"strings"
)

// NormalizeMediaSource classifies a provider media string without fetching it.
// Conventional HTTP(S) and relative URLs stay URL sources, while image/video/
// audio data URLs and common raw image base64 payloads become inline sources.
// Network safety is still enforced later by HTTPSourceFetcher.
func NormalizeMediaSource(raw string) MediaResult {
	value := strings.TrimSpace(raw)
	result := MediaResult{SourceKind: SourceURL, Locator: value}
	if value == "" {
		return result
	}
	if contentType, ok := base64DataURLContentType(value); ok {
		result.SourceKind = SourceBase64
		result.ContentType = contentType
		return result
	}
	if contentType := commonImageBase64ContentType(value); contentType != "" {
		result.SourceKind = SourceBase64
		result.ContentType = contentType
	}
	return result
}

// MediaSourceDataURL turns an inline source into a browser-usable data URL.
// Existing data URLs are retained verbatim; URL sources are returned unchanged.
func MediaSourceDataURL(source MediaResult) string {
	if !strings.EqualFold(strings.TrimSpace(source.SourceKind), SourceBase64) {
		return strings.TrimSpace(source.Locator)
	}
	locator := strings.TrimSpace(source.Locator)
	if strings.HasPrefix(strings.ToLower(locator), "data:") {
		return locator
	}
	contentType := strings.ToLower(strings.TrimSpace(source.ContentType))
	if contentType == "" {
		contentType = "image/png"
	}
	return "data:" + contentType + ";base64," + locator
}

// NormalizeMediaSources produces stable source strings for an operation
// result. Inline payloads become data URLs and relative paths are resolved
// against the immutable upstream base URL. Duplicate sources retain their
// first-seen ordering.
func NormalizeMediaSources(sources []string, baseURL string) []string {
	seen := make(map[string]struct{}, len(sources))
	result := make([]string, 0, len(sources))
	var base *url.URL
	if parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/") + "/"); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		base = parsed
	}
	for _, raw := range sources {
		value := MediaSourceDataURL(NormalizeMediaSource(raw))
		if value == "" {
			continue
		}
		source := NormalizeMediaSource(value)
		if source.SourceKind == SourceURL && base != nil && !strings.ContainsAny(value, " \t\r\n") {
			if reference, err := url.Parse(value); err == nil && !reference.IsAbs() && reference.Host == "" {
				value = base.ResolveReference(reference).String()
			}
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func base64DataURLContentType(raw string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(raw))
	if !strings.HasPrefix(lower, "data:") {
		return "", false
	}
	comma := strings.IndexByte(lower, ',')
	if comma < 0 {
		return "", false
	}
	metadata := strings.Split(lower[len("data:"):comma], ";")
	if len(metadata) < 2 {
		return "", false
	}
	contentType := strings.TrimSpace(metadata[0])
	if !strings.HasPrefix(contentType, "image/") && !strings.HasPrefix(contentType, "video/") && !strings.HasPrefix(contentType, "audio/") {
		return "", false
	}
	for _, parameter := range metadata[1:] {
		if strings.TrimSpace(parameter) == "base64" {
			return contentType, true
		}
	}
	return "", false
}

func commonImageBase64ContentType(raw string) string {
	switch {
	case strings.HasPrefix(raw, "iVBORw0KGgo"):
		return "image/png"
	case strings.HasPrefix(raw, "/9j/"):
		return "image/jpeg"
	case strings.HasPrefix(raw, "UklGR"):
		return "image/webp"
	case strings.HasPrefix(raw, "R0lGOD"):
		return "image/gif"
	default:
		return ""
	}
}
