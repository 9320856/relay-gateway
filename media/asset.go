package media

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

const (
	StatusPending       = "pending"
	StatusMaterializing = "materializing"
	StatusAvailable     = "available"
	StatusFailed        = "failed"
	StatusDeleting      = "deleting"
	StatusDeleted       = "deleted"
)

type Asset struct {
	ID             string
	PublicID       string
	CapabilityHash string
	ObjectKey      string
	Status         string
	CreatedAt      time.Time
}

type AssetService struct{ randomBytes int }

func NewAssetService() *AssetService { return &AssetService{randomBytes: 32} }

func (s *AssetService) NewAsset(objectKey string) (Asset, string, error) {
	if s == nil {
		s = NewAssetService()
	}
	publicID, err := randomToken(s.randomBytes)
	if err != nil {
		return Asset{}, "", err
	}
	capability, err := randomToken(s.randomBytes)
	if err != nil {
		return Asset{}, "", err
	}
	hash := HashCapability(capability)
	asset := Asset{ID: publicID, PublicID: publicID, CapabilityHash: hash, ObjectKey: objectKey, Status: StatusPending, CreatedAt: time.Now().UTC()}
	return asset, capability, nil
}

// Create is an alias for NewAsset, useful to callers that model assets as a factory.
func (s *AssetService) Create(objectKey string) (Asset, string, error) { return s.NewAsset(objectKey) }

func randomToken(n int) (string, error) {
	if n < 16 {
		n = 32
	}
	b := make([]byte, n)
	if _, e := rand.Read(b); e != nil {
		return "", e
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func HashCapability(capability string) string {
	sum := sha256.Sum256([]byte(capability))
	return hex.EncodeToString(sum[:])
}

func (s *AssetService) ValidateCapability(asset Asset, capability string) bool {
	return ValidateCapabilityHash(asset.CapabilityHash, capability)
}
func ValidateCapabilityHash(hash, capability string) bool {
	if hash == "" || capability == "" {
		return false
	}
	got := HashCapability(capability)
	if len(hash) != len(got) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(hash), []byte(got)) == 1
}

func (s *AssetService) RotateCapability(asset *Asset) (string, error) {
	if asset == nil {
		return "", errors.New("asset is nil")
	}
	c, e := randomToken(32)
	if e != nil {
		return "", e
	}
	asset.CapabilityHash = HashCapability(c)
	return c, nil
}

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
