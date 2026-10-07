package media

import "strings"

// ProfileObjectKey returns the relative storage key for a profile media object,
// ensuring videos are stored with the .mp4 extension and images with appropriate image extensions.
func ProfileObjectKey(publicID, kind, contentType string) string {
	publicID = strings.TrimSpace(publicID)
	ext := ".media"
	k := strings.ToLower(strings.TrimSpace(kind))
	ct := strings.ToLower(strings.TrimSpace(contentType))
	if k == "video" || strings.Contains(ct, "video") || strings.Contains(ct, "mp4") {
		ext = ".mp4"
	} else if k == "image" || strings.Contains(ct, "image") {
		switch {
		case strings.Contains(ct, "jpeg") || strings.Contains(ct, "jpg"):
			ext = ".jpg"
		case strings.Contains(ct, "webp"):
			ext = ".webp"
		case strings.Contains(ct, "gif"):
			ext = ".gif"
		default:
			ext = ".png"
		}
	}
	return "profile/" + publicID + ext
}
