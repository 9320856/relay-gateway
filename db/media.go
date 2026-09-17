package db

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

var (
	ErrMediaAssetNotFound  = errors.New("media asset not found")
	ErrMediaAssetGone      = errors.New("media asset is no longer available")
	ErrMediaJobUnavailable = errors.New("media materialization job unavailable")
	ErrMediaJobLeaseOwner  = errors.New("media deletion job lease owner mismatch")
)

const (
	MediaAssetPending         = "pending"
	MediaAssetMaterializing   = "materializing"
	MediaAssetAvailable       = "available"
	MediaAssetDeleteRequested = "delete_requested"
	MediaAssetDeleteFailed    = "delete_failed"
	MediaAssetDeleted         = "deleted"
	MediaAssetExpired         = "expired"
	MediaAssetFailed          = "failed"

	MediaObjectStaging       = "staging"
	MediaObjectReady         = "ready"
	MediaObjectDeletePending = "delete_pending"
	MediaObjectDeleting      = "deleting"
	MediaObjectDeleteFailed  = "delete_failed"
	MediaObjectDeleted       = "deleted"

	MediaJobPending   = "pending"
	MediaJobRunning   = "running"
	MediaJobSucceeded = "succeeded"
	MediaJobFailed    = "failed"
)

type MediaAsset struct {
	ID             uint   `gorm:"primaryKey" json:"id"`
	PublicID       string `gorm:"size:48;not null;uniqueIndex" json:"public_id"`
	CapabilityHash string `gorm:"size:64;not null" json:"-"`
	// CapabilityCiphertext is an encrypted capability retained only when the
	// database encryption key is configured. It is never serialized in API
	// responses or task result bodies.
	CapabilityCiphertext string     `gorm:"size:256" json:"-"`
	TaskRunID            string     `gorm:"size:64;index:idx_media_asset_task_ordinal,priority:1" json:"task_run_id,omitempty"`
	OriginRequestID      string     `gorm:"size:64;index" json:"origin_request_id,omitempty"`
	Kind                 string     `gorm:"size:16;not null;index" json:"kind"`
	Ordinal              int        `gorm:"not null;default:0;uniqueIndex:idx_media_asset_task_ordinal,priority:2" json:"ordinal"`
	Status               string     `gorm:"size:24;not null;index" json:"status"`
	ObjectID             string     `gorm:"size:64;index" json:"object_id,omitempty"`
	SourceKind           string     `gorm:"size:16;not null" json:"source_kind"`
	SourceLocator        string     `gorm:"size:2048" json:"source_locator,omitempty"`
	ContentType          string     `gorm:"size:128" json:"content_type,omitempty"`
	ByteSize             int64      `gorm:"not null;default:0" json:"byte_size,omitempty"`
	SHA256               string     `gorm:"size:64;index" json:"sha256,omitempty"`
	DisplayName          string     `gorm:"size:255" json:"display_name,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	AvailableAt          *time.Time `json:"available_at,omitempty"`
	ExpiresAt            *time.Time `json:"expires_at,omitempty"`
	DeleteRequestedAt    *time.Time `json:"delete_requested_at,omitempty"`
	DeletedAt            *time.Time `json:"deleted_at,omitempty"`
	DeleteReason         string     `gorm:"size:255" json:"delete_reason,omitempty"`
	StateVersion         uint64     `gorm:"not null;default:0" json:"state_version"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

var ErrMediaCapabilityUnavailable = errors.New("media capability is unavailable")

func (MediaAsset) TableName() string { return "media_assets" }

type MediaObject struct {
	ID             string     `gorm:"primaryKey;size:64" json:"id"`
	Backend        string     `gorm:"size:16;not null" json:"backend"`
	StorageKey     string     `gorm:"size:512;not null;uniqueIndex" json:"storage_key"`
	SHA256         string     `gorm:"size:64;not null;index" json:"sha256"`
	ByteSize       int64      `gorm:"not null" json:"byte_size"`
	ContentType    string     `gorm:"size:128;not null" json:"content_type"`
	ETag           string     `gorm:"size:128" json:"etag,omitempty"`
	State          string     `gorm:"size:24;not null;index" json:"state"`
	RefCount       int        `gorm:"not null;default:1" json:"ref_count"`
	DeleteAttempts int        `gorm:"not null;default:0" json:"delete_attempts"`
	NextRetryAt    *time.Time `json:"next_retry_at,omitempty"`
	LastError      string     `gorm:"size:1024" json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (MediaObject) TableName() string { return "media_objects" }

type MediaMaterializationJob struct {
	ID             uint       `gorm:"primaryKey" json:"id"`
	AssetID        uint       `gorm:"not null;uniqueIndex" json:"asset_id"`
	Status         string     `gorm:"size:24;not null;index" json:"status"`
	LeaseOwner     string     `gorm:"size:128;index" json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	Attempts       int        `gorm:"not null;default:0" json:"attempts"`
	NextAttemptAt  *time.Time `gorm:"index" json:"next_attempt_at,omitempty"`
	DeadlineAt     *time.Time `json:"deadline_at,omitempty"`
	LastError      string     `gorm:"size:1024" json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (MediaMaterializationJob) TableName() string { return "media_materialization_jobs" }

// MediaDeletionJob is the durable local cleanup record for a revoked asset.
// It is intentionally separate from materialization jobs so delete retries can
// never resubmit or refetch an upstream task.
type MediaDeletionJob struct {
	ID             uint       `gorm:"primaryKey" json:"id"`
	AssetID        uint       `gorm:"not null;uniqueIndex" json:"asset_id"`
	ObjectID       string     `gorm:"size:64;index" json:"object_id,omitempty"`
	StorageKey     string     `gorm:"size:512" json:"storage_key,omitempty"`
	Status         string     `gorm:"size:24;not null;index" json:"status"`
	LeaseOwner     string     `gorm:"size:128;index" json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	Attempts       int        `gorm:"not null;default:0" json:"attempts"`
	NextAttemptAt  *time.Time `gorm:"index" json:"next_attempt_at,omitempty"`
	LastError      string     `gorm:"size:1024" json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func (MediaDeletionJob) TableName() string { return "media_deletion_jobs" }

func mediaDB(ctx context.Context) (*gorm.DB, error) {
	db := DBForContext(ctx)
	if db == nil {
		return nil, errors.New("database is not initialized")
	}
	return db, nil
}

func newMediaToken(size int) (string, error) {
	if size < 16 || size > 128 {
		return "", errors.New("invalid media token size")
	}
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func hashMediaCapability(capability string) string {
	sum := sha256.Sum256([]byte(capability))
	return fmt.Sprintf("%x", sum[:])
}

// NewMediaLinkIdentity returns non-guessable public and capability values. The
// database stores only the capability hash; callers must keep the capability
// alongside the generated link.
func NewMediaLinkIdentity() (publicID, capability, capabilityHash string, err error) {
	publicID, err = newMediaToken(18)
	if err != nil {
		return "", "", "", err
	}
	capability, err = newMediaToken(32)
	if err != nil {
		return "", "", "", err
	}
	return publicID, capability, hashMediaCapability(capability), nil
}

// EncryptMediaCapability protects a capability for restart recovery. The
// plaintext is intentionally not persisted when RELAY_DB_ENCRYPTION_KEY is
// absent; callers can still use the freshly returned capability in-memory.
func EncryptMediaCapability(capability string) (string, error) {
	capability = strings.TrimSpace(capability)
	if capability == "" {
		return "", ErrMediaCapabilityUnavailable
	}
	if len(channelSecretKey()) == 0 {
		return "", nil
	}
	return encryptChannelSecret(capability)
}

// DecryptMediaCapability returns a capability only in memory. Assets created
// without an encryption key deliberately cannot be reconstructed after a
// restart.
func DecryptMediaCapability(ciphertext string) (string, error) {
	ciphertext = strings.TrimSpace(ciphertext)
	if ciphertext == "" {
		return "", ErrMediaCapabilityUnavailable
	}
	plain, err := decryptChannelSecret(ciphertext)
	if err != nil || strings.TrimSpace(plain) == "" {
		if err != nil {
			return "", err
		}
		return "", ErrMediaCapabilityUnavailable
	}
	return plain, nil
}

// RecoverMediaAssetCapability reveals an encrypted capability without changing
// the asset. Silently replacing it here would invalidate links during a status read.
func RecoverMediaAssetCapability(asset *MediaAsset) (string, error) {
	if asset == nil {
		return "", ErrMediaCapabilityUnavailable
	}
	return DecryptMediaCapability(asset.CapabilityCiphertext)
}

func CreateMediaAssetContext(ctx context.Context, asset *MediaAsset) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if asset == nil || strings.TrimSpace(asset.PublicID) == "" {
		return errors.New("media asset public id is required")
	}
	if asset.Status == "" {
		asset.Status = MediaAssetPending
	}
	if asset.CapabilityHash == "" {
		return errors.New("media asset capability hash is required")
	}
	if asset.Kind == "" {
		asset.Kind = "unknown"
	}
	return db.Create(asset).Error
}

func CreateMediaAsset(asset *MediaAsset) error {
	return CreateMediaAssetContext(context.Background(), asset)
}

func GetMediaAssetByLinkContext(ctx context.Context, publicID, capability string) (*MediaAsset, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	publicID, capability = strings.TrimSpace(publicID), strings.TrimSpace(capability)
	if publicID == "" || capability == "" {
		return nil, ErrMediaAssetNotFound
	}
	var asset MediaAsset
	if err := db.Where("public_id = ?", publicID).First(&asset).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrMediaAssetNotFound
		}
		return nil, err
	}
	got, want := hashMediaCapability(capability), asset.CapabilityHash
	if len(got) != len(want) || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return nil, ErrMediaAssetNotFound
	}
	if asset.Status == MediaAssetDeleted || asset.Status == MediaAssetDeleteRequested || asset.Status == MediaAssetDeleteFailed || asset.Status == MediaAssetExpired {
		return &asset, ErrMediaAssetGone
	}
	return &asset, nil
}

func GetMediaAssetByLink(publicID, capability string) (*MediaAsset, error) {
	return GetMediaAssetByLinkContext(context.Background(), publicID, capability)
}

func RequestMediaAssetDeleteContext(ctx context.Context, publicID, reason string) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	publicID = strings.TrimSpace(publicID)
	if publicID == "" {
		return ErrMediaAssetNotFound
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var asset MediaAsset
		if err := tx.Where("public_id = ?", publicID).First(&asset).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrMediaAssetNotFound
			}
			return err
		}
		if asset.Status == MediaAssetDeleted {
			return nil
		}
		now := time.Now()
		// delete_failed is already a revoked state. Treat retries and repeated
		// requests as idempotent so the shared object's refcount is decremented
		// at most once.
		wasDeleteRequested := asset.Status == MediaAssetDeleteRequested || asset.Status == MediaAssetDeleteFailed
		if !wasDeleteRequested {
			if err := tx.Model(&asset).Updates(map[string]any{
				"status":              MediaAssetDeleteRequested,
				"delete_requested_at": now,
				"delete_reason":       strings.TrimSpace(reason),
				"state_version":       asset.StateVersion + 1,
				"updated_at":          now,
			}).Error; err != nil {
				return err
			}
		}
		job := &MediaDeletionJob{AssetID: asset.ID, ObjectID: asset.ObjectID, Status: MediaJobPending}
		if asset.ObjectID != "" {
			var object MediaObject
			if err := tx.First(&object, "id = ?", asset.ObjectID).Error; err == nil {
				job.StorageKey = object.StorageKey
				if !wasDeleteRequested && object.State != MediaObjectDeleted && object.RefCount > 0 {
					state := MediaObjectDeletePending
					if object.RefCount > 1 {
						// The object remains live while another asset still references it.
						state = MediaObjectReady
					}
					if err := tx.Model(&object).Updates(map[string]any{"ref_count": gorm.Expr("CASE WHEN ref_count > 0 THEN ref_count - 1 ELSE 0 END"), "state": state, "next_retry_at": nil, "last_error": "", "updated_at": now}).Error; err != nil {
						return err
					}
				}
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		return tx.Where("asset_id = ?", asset.ID).FirstOrCreate(job).Error
	})
}

func RequestMediaAssetDelete(publicID, reason string) error {
	return RequestMediaAssetDeleteContext(context.Background(), publicID, reason)
}

func RequestMediaAssetDeleteByIDContext(ctx context.Context, id uint, reason string) error {
	asset, err := GetMediaAssetByIDContext(ctx, id)
	if err != nil {
		return err
	}
	return RequestMediaAssetDeleteContext(ctx, asset.PublicID, reason)
}

func RequestMediaAssetDeleteByID(id uint, reason string) error {
	return RequestMediaAssetDeleteByIDContext(context.Background(), id, reason)
}

// HardDeleteMediaAssetByIDContext physically removes the media asset and its
// associated jobs from the database. If no other asset shares the underlying
// storage object, the storage object is also removed from the database and
// shouldDeleteFile is returned as true with its storage key so the caller can
// delete the physical file from the object store immediately.
func HardDeleteMediaAssetByIDContext(ctx context.Context, id uint) (string, bool, error) {
	dbConn, err := mediaDB(ctx)
	if err != nil {
		return "", false, err
	}
	var storageKey string
	var shouldDeleteFile bool

	err = dbConn.Transaction(func(tx *gorm.DB) error {
		var asset MediaAsset
		if err := tx.First(&asset, "id = ?", id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrMediaAssetNotFound
			}
			return err
		}

		if asset.ObjectID != "" {
			var object MediaObject
			if err := tx.First(&object, "id = ?", asset.ObjectID).Error; err == nil {
				storageKey = strings.TrimSpace(object.StorageKey)
				var refCount int64
				if err := tx.Model(&MediaAsset{}).Where("object_id = ? AND id <> ?", asset.ObjectID, asset.ID).Count(&refCount).Error; err != nil {
					return err
				}
				if refCount <= 0 {
					shouldDeleteFile = true
					if err := tx.Where("id = ?", asset.ObjectID).Delete(&MediaObject{}).Error; err != nil {
						return err
					}
				} else {
					if err := tx.Model(&object).Update("ref_count", refCount).Error; err != nil {
						return err
					}
				}
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}

		_ = tx.Where("asset_id = ?", asset.ID).Delete(&MediaDeletionJob{}).Error
		_ = tx.Where("asset_id = ?", asset.ID).Delete(&MediaMaterializationJob{}).Error

		return tx.Where("id = ?", asset.ID).Delete(&MediaAsset{}).Error
	})

	return storageKey, shouldDeleteFile, err
}

func HardDeleteMediaAssetByID(id uint) (string, bool, error) {
	return HardDeleteMediaAssetByIDContext(context.Background(), id)
}

func GetMediaAssetByIDContext(ctx context.Context, id uint) (*MediaAsset, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	var asset MediaAsset
	if err := db.First(&asset, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrMediaAssetNotFound
		}
		return nil, err
	}
	return &asset, nil
}

func GetMediaAssetByID(id uint) (*MediaAsset, error) {
	return GetMediaAssetByIDContext(context.Background(), id)
}

func GetMediaObjectByIDContext(ctx context.Context, id string) (*MediaObject, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	var object MediaObject
	if err := db.First(&object, "id = ?", strings.TrimSpace(id)).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrMediaAssetNotFound
		}
		return nil, err
	}
	return &object, nil
}

func GetMediaObjectByID(id string) (*MediaObject, error) {
	return GetMediaObjectByIDContext(context.Background(), id)
}

func ListMediaAssetsContext(ctx context.Context, limit, offset int) ([]MediaAsset, error) {
	return ListMediaAssetsFilteredContext(ctx, limit, offset, "", "")
}

// ListMediaAssetsFilteredContext applies filtering before pagination so a
// large media library cannot lose pages after an arbitrary in-memory cap.
func ListMediaAssetsFilteredContext(ctx context.Context, limit, offset int, kind, status string) ([]MediaAsset, error) {
	return ListMediaAssetsFilteredSearchContext(ctx, limit, offset, kind, status, "")
}

// ListMediaAssetsFilteredSearchContext applies filtering and keyword search before pagination.
func ListMediaAssetsFilteredSearchContext(ctx context.Context, limit, offset int, kind, status, search string) ([]MediaAsset, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	query := db.Model(&MediaAsset{})
	if kind = strings.TrimSpace(kind); kind != "" {
		query = query.Where("kind = ?", kind)
	}
	if status = strings.TrimSpace(status); status != "" {
		query = query.Where("status = ?", status)
	} else {
		query = query.Where("status <> ?", MediaAssetDeleted)
	}
	if search = strings.TrimSpace(search); search != "" {
		if idVal, err := strconv.ParseUint(search, 10, 64); err == nil && idVal > 0 {
			query = query.Where("id = ? OR task_run_id LIKE ? OR display_name LIKE ?", idVal, "%"+search+"%", "%"+search+"%")
		} else {
			query = query.Where("task_run_id LIKE ? OR display_name LIKE ?", "%"+search+"%", "%"+search+"%")
		}
	}
	var assets []MediaAsset
	if err := query.Order("created_at DESC, id DESC").Limit(limit).Offset(offset).Find(&assets).Error; err != nil {
		return nil, err
	}
	return assets, nil
}

func ListMediaAssets(limit, offset int) ([]MediaAsset, error) {
	return ListMediaAssetsContext(context.Background(), limit, offset)
}

func ListMediaAssetsForTaskRunContext(ctx context.Context, taskRunID, kind string) ([]MediaAsset, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	query := db.Where("task_run_id = ?", strings.TrimSpace(taskRunID)).Order("ordinal ASC, id ASC")
	if strings.TrimSpace(kind) != "" {
		query = query.Where("kind = ?", strings.TrimSpace(kind))
	}
	var assets []MediaAsset
	if err := query.Find(&assets).Error; err != nil {
		return nil, err
	}
	return assets, nil
}

func ListMediaAssetsForTaskRun(taskRunID, kind string) ([]MediaAsset, error) {
	return ListMediaAssetsForTaskRunContext(context.Background(), taskRunID, kind)
}

// CompleteTaskRunAfterMediaContext promotes a provider-completed task only
// after every required asset is available. Materialization workers call this
// after local storage succeeds; it never contacts or re-submits upstream.
func CompleteTaskRunAfterMediaContext(ctx context.Context, taskRunID, kind string) error {
	dbConn, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	taskRunID, kind = strings.TrimSpace(taskRunID), strings.TrimSpace(kind)
	if taskRunID == "" {
		return errors.New("task run id is required")
	}
	return dbConn.Transaction(func(tx *gorm.DB) error {
		var run TaskRun
		if err := tx.First(&run, "id = ?", taskRunID).Error; err != nil {
			return err
		}
		if isTaskTerminal(run.TaskStatus) && run.TaskStatus != "materializing" {
			return nil
		}
		assets := tx.Model(&MediaAsset{}).Where("task_run_id = ?", taskRunID)
		if kind != "" {
			assets = assets.Where("kind = ?", kind)
		}
		var total, unavailable int64
		if err := assets.Count(&total).Error; err != nil {
			return err
		}
		if total == 0 {
			return nil
		}
		if err := assets.Where("status <> ? OR object_id = ''", MediaAssetAvailable).Count(&unavailable).Error; err != nil {
			return err
		}
		if unavailable > 0 {
			return nil
		}
		now := time.Now()
		updates := map[string]any{"task_status": "completed", "task_outcome": "success", "completed_at": now, "state_version": run.StateVersion + 1, "updated_at": now}
		if err := tx.Model(&run).Updates(updates).Error; err != nil {
			return err
		}
		sequence := run.EventSequence + 1
		if err := tx.Model(&run).Updates(map[string]any{"event_sequence": sequence, "state_version": run.StateVersion + 2, "updated_at": now}).Error; err != nil {
			return err
		}
		return tx.Create(&TaskEvent{TaskRunID: taskRunID, Sequence: sequence, Type: "status_changed", Data: "completed"}).Error
	})
}

func MarkMediaAssetAvailableContext(ctx context.Context, assetID uint, objectID, contentType, sha256Value string, size int64) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if objectID == "" {
		return errors.New("media object id is required")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var asset MediaAsset
		if err := tx.First(&asset, "id = ?", assetID).Error; err != nil {
			return err
		}
		if asset.Status == MediaAssetDeleted || asset.Status == MediaAssetDeleteRequested || asset.Status == MediaAssetDeleteFailed || asset.Status == MediaAssetExpired {
			return ErrMediaAssetGone
		}
		// A materializer may race with a second asset resolving to the same
		// deduplicated object. The object creator starts at refcount one for the
		// first asset; only increment when another live asset already references
		// this object.
		var object MediaObject
		if err := tx.First(&object, "id = ?", strings.TrimSpace(objectID)).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrMediaAssetNotFound
			}
			return err
		}
		if object.State == MediaObjectDeleted {
			return ErrMediaAssetGone
		}
		if strings.TrimSpace(asset.ObjectID) != strings.TrimSpace(objectID) {
			if oldID := strings.TrimSpace(asset.ObjectID); oldID != "" {
				if err := tx.Model(&MediaObject{}).Where("id = ? AND ref_count > 0", oldID).Updates(map[string]any{"ref_count": gorm.Expr("ref_count - 1"), "updated_at": time.Now()}).Error; err != nil {
					return err
				}
				if err := tx.Model(&MediaObject{}).Where("id = ? AND ref_count <= 0 AND state <> ?", oldID, MediaObjectDeleted).Updates(map[string]any{"state": MediaObjectDeletePending, "next_retry_at": nil, "updated_at": time.Now()}).Error; err != nil {
					return err
				}
			}
			var references int64
			if err := tx.Model(&MediaAsset{}).Where("object_id = ? AND status NOT IN ? AND id <> ?", objectID, []string{MediaAssetDeleted, MediaAssetDeleteRequested, MediaAssetDeleteFailed, MediaAssetExpired}, asset.ID).Count(&references).Error; err != nil {
				return err
			}
			updates := map[string]any{"state": MediaObjectReady, "updated_at": time.Now()}
			if references > 0 {
				updates["ref_count"] = gorm.Expr("ref_count + 1")
			}
			if err := tx.Model(&object).Updates(updates).Error; err != nil {
				return err
			}
		}
		now := time.Now()
		result := tx.Model(&MediaAsset{}).Where("id = ? AND status NOT IN ?", assetID, []string{MediaAssetDeleted, MediaAssetDeleteRequested, MediaAssetDeleteFailed, MediaAssetExpired}).Updates(map[string]any{
			"object_id": objectID, "status": MediaAssetAvailable, "content_type": contentType,
			"sha256": sha256Value, "byte_size": size, "available_at": now,
			"state_version": gorm.Expr("state_version + 1"), "updated_at": now,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrMediaAssetGone
		}
		return nil
	})
}

func MarkMediaAssetAvailable(assetID uint, objectID, contentType, sha256Value string, size int64) error {
	return MarkMediaAssetAvailableContext(context.Background(), assetID, objectID, contentType, sha256Value, size)
}

// MarkMediaAssetFailed records a materialization failure without revoking the
// logical asset. The asset can be retried later and no provider task is
// resubmitted as part of this transition.
func MarkMediaAssetFailedContext(ctx context.Context, assetID uint, failure string) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	result := db.Model(&MediaAsset{}).Where("id = ? AND status NOT IN ?", assetID, []string{MediaAssetDeleted, MediaAssetDeleteRequested}).Updates(map[string]any{
		"status": MediaAssetFailed, "delete_reason": strings.TrimSpace(failure), "state_version": gorm.Expr("state_version + 1"), "updated_at": time.Now(),
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaAssetNotFound
	}
	return nil
}

func MarkMediaAssetFailed(assetID uint, failure string) error {
	return MarkMediaAssetFailedContext(context.Background(), assetID, failure)
}

func CompleteMediaAssetDeleteContext(ctx context.Context, assetID uint) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var asset MediaAsset
		if err := tx.First(&asset, "id = ?", assetID).Error; err != nil {
			return err
		}
		if asset.Status == MediaAssetDeleted {
			return nil
		}
		now := time.Now()
		if asset.ObjectID != "" {
			var object MediaObject
			if err := tx.First(&object, "id = ?", asset.ObjectID).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			} else if err == nil {
				updates := map[string]any{"next_retry_at": nil, "last_error": "", "updated_at": now}
				if object.RefCount > 0 {
					// The asset may be revoked while the underlying object remains
					// shared by another asset. Keep the shared object available.
					updates["state"] = MediaObjectReady
				} else {
					updates["state"] = MediaObjectDeleted
					updates["ref_count"] = 0
				}
				if err := tx.Model(&MediaObject{}).Where("id = ?", asset.ObjectID).Updates(updates).Error; err != nil {
					return err
				}
			}
		}
		return tx.Model(&asset).Updates(map[string]any{"status": MediaAssetDeleted, "deleted_at": now, "state_version": asset.StateVersion + 1, "updated_at": now}).Error
	})
}

func CompleteMediaAssetDelete(assetID uint) error {
	return CompleteMediaAssetDeleteContext(context.Background(), assetID)
}

func RetryMediaAssetMaterializationContext(ctx context.Context, assetID uint) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var asset MediaAsset
		if err := tx.Where("id = ? AND status IN ?", assetID, []string{MediaAssetFailed, MediaAssetPending, MediaAssetMaterializing}).First(&asset).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrMediaAssetNotFound
			}
			return err
		}
		var job MediaMaterializationJob
		lookup := tx.Where("asset_id = ?", assetID).First(&job).Error
		if lookup != nil && !errors.Is(lookup, gorm.ErrRecordNotFound) {
			return lookup
		}
		now := time.Now()
		if lookup == nil && job.Status == MediaJobRunning && job.LeaseExpiresAt != nil && job.LeaseExpiresAt.After(now) {
			return nil
		}
		result := tx.Model(&MediaAsset{}).
			Where("id = ? AND status IN ?", assetID, []string{MediaAssetFailed, MediaAssetPending, MediaAssetMaterializing}).
			Updates(map[string]any{"status": MediaAssetMaterializing, "state_version": gorm.Expr("state_version + 1"), "updated_at": time.Now()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrMediaAssetNotFound
		}
		if errors.Is(lookup, gorm.ErrRecordNotFound) {
			return tx.Create(&MediaMaterializationJob{AssetID: assetID, Status: MediaJobPending}).Error
		}
		return tx.Model(&job).Updates(map[string]any{"status": MediaJobPending, "next_attempt_at": nil, "deadline_at": nil, "lease_owner": "", "lease_expires_at": nil, "last_error": "", "updated_at": now}).Error
	})
}

func CreateMediaDeletionJobContext(ctx context.Context, job *MediaDeletionJob) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if job == nil || job.AssetID == 0 {
		return errors.New("deletion job asset is required")
	}
	if job.Status == "" {
		job.Status = MediaJobPending
	}
	return db.Where("asset_id = ?", job.AssetID).FirstOrCreate(job).Error
}

func CreateMediaDeletionJob(job *MediaDeletionJob) error {
	return CreateMediaDeletionJobContext(context.Background(), job)
}

func GetMediaDeletionJobContext(ctx context.Context, assetID uint) (*MediaDeletionJob, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	var job MediaDeletionJob
	if err := db.Where("asset_id = ?", assetID).First(&job).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrMediaJobUnavailable
		}
		return nil, err
	}
	return &job, nil
}

func GetMediaDeletionJob(assetID uint) (*MediaDeletionJob, error) {
	return GetMediaDeletionJobContext(context.Background(), assetID)
}

func ClaimMediaDeletionJobContext(ctx context.Context, owner string, lease time.Duration) (*MediaDeletionJob, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, errors.New("media deletion owner is required")
	}
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	now := time.Now()
	var claimed MediaDeletionJob
	err = db.Transaction(func(tx *gorm.DB) error {
		res := tx.Where("status IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?) AND (lease_expires_at IS NULL OR lease_expires_at <= ?)", []string{MediaJobPending, MediaJobRunning, MediaJobFailed}, now, now).Order("COALESCE(next_attempt_at, created_at) ASC, id ASC").First(&claimed)
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return ErrMediaJobUnavailable
		}
		if res.Error != nil {
			return res.Error
		}
		expires := now.Add(lease)
		return tx.Model(&claimed).Updates(map[string]any{"status": MediaJobRunning, "lease_owner": owner, "lease_expires_at": expires, "attempts": claimed.Attempts + 1, "updated_at": now}).Error
	})
	if err != nil {
		return nil, err
	}
	expires := now.Add(lease)
	claimed.Status = MediaJobRunning
	claimed.LeaseOwner = owner
	claimed.LeaseExpiresAt = &expires
	claimed.Attempts++
	return &claimed, nil
}

func ClaimMediaDeletionJob(owner string, lease time.Duration) (*MediaDeletionJob, error) {
	return ClaimMediaDeletionJobContext(context.Background(), owner, lease)
}

func CompleteMediaDeletionJobContext(ctx context.Context, id uint) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	result := db.Model(&MediaDeletionJob{}).Where("id = ?", id).Updates(map[string]any{"status": MediaJobSucceeded, "lease_owner": "", "lease_expires_at": nil, "last_error": "", "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaJobUnavailable
	}
	return nil
}

// CompleteMediaDeletionJobForLeaseContext only allows the worker currently
// holding a live lease to complete a deletion job. This prevents a slow worker
// from overwriting a job after its lease has expired and another worker has
// claimed it.
func CompleteMediaDeletionJobForLeaseContext(ctx context.Context, id uint, owner string) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	owner = strings.TrimSpace(owner)
	if id == 0 || owner == "" {
		return errors.New("media deletion lease identity is required")
	}
	result := db.Model(&MediaDeletionJob{}).Where("id = ? AND status = ? AND lease_owner = ? AND lease_expires_at > ?", id, MediaJobRunning, owner, time.Now()).Updates(map[string]any{"status": MediaJobSucceeded, "lease_owner": "", "lease_expires_at": nil, "last_error": "", "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaJobLeaseOwner
	}
	return nil
}

func CompleteMediaDeletionJobForLease(id uint, owner string) error {
	return CompleteMediaDeletionJobForLeaseContext(context.Background(), id, owner)
}
func CompleteMediaDeletionJob(id uint) error {
	return CompleteMediaDeletionJobContext(context.Background(), id)
}

func FailMediaDeletionJobContext(ctx context.Context, id uint, failure string, next time.Time) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	status := MediaJobFailed
	var nextAttempt any
	if !next.IsZero() {
		status = MediaJobPending
		nextAttempt = next
	}
	result := db.Model(&MediaDeletionJob{}).Where("id = ?", id).Updates(map[string]any{"status": status, "lease_owner": "", "lease_expires_at": nil, "next_attempt_at": nextAttempt, "last_error": strings.TrimSpace(failure), "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaJobUnavailable
	}
	return nil
}

// FailMediaDeletionJobForLeaseContext is the lease-checked counterpart of
// FailMediaDeletionJobContext. It is safe for retry/error paths in workers.
func FailMediaDeletionJobForLeaseContext(ctx context.Context, id uint, owner, failure string, next time.Time) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	owner = strings.TrimSpace(owner)
	if id == 0 || owner == "" {
		return errors.New("media deletion lease identity is required")
	}
	status := MediaJobFailed
	var nextAttempt any
	if !next.IsZero() {
		status = MediaJobPending
		nextAttempt = next
	}
	result := db.Model(&MediaDeletionJob{}).Where("id = ? AND status = ? AND lease_owner = ? AND lease_expires_at > ?", id, MediaJobRunning, owner, time.Now()).Updates(map[string]any{"status": status, "lease_owner": "", "lease_expires_at": nil, "next_attempt_at": nextAttempt, "last_error": strings.TrimSpace(failure), "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaJobLeaseOwner
	}
	return nil
}

func FailMediaDeletionJobForLease(id uint, owner, failure string, next time.Time) error {
	return FailMediaDeletionJobForLeaseContext(context.Background(), id, owner, failure, next)
}
func FailMediaDeletionJob(id uint, failure string, next time.Time) error {
	return FailMediaDeletionJobContext(context.Background(), id, failure, next)
}

// AcquireMediaObjectReferenceContext attaches another logical asset to an
// existing ready object. The conditional update prevents attaching to an
// object already being deleted.
func AcquireMediaObjectReferenceContext(ctx context.Context, objectID string) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	result := db.Model(&MediaObject{}).Where("id = ? AND state = ?", strings.TrimSpace(objectID), MediaObjectReady).
		Updates(map[string]any{"ref_count": gorm.Expr("ref_count + 1"), "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaAssetGone
	}
	return nil
}

func AcquireMediaObjectReference(objectID string) error {
	return AcquireMediaObjectReferenceContext(context.Background(), objectID)
}

// PrepareMediaObjectDeletionContext atomically claims the physical deletion
// right after a worker checks the reference count.
func PrepareMediaObjectDeletionContext(ctx context.Context, objectID string) (bool, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return false, err
	}
	result := db.Model(&MediaObject{}).Where("id = ? AND state IN ? AND ref_count <= 0", strings.TrimSpace(objectID), []string{MediaObjectDeletePending, MediaObjectDeleteFailed, MediaObjectDeleting}).
		Updates(map[string]any{"state": MediaObjectDeleting, "updated_at": time.Now()})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func RetryMediaAssetDeletionContext(ctx context.Context, assetID uint) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var asset MediaAsset
		if err := tx.First(&asset, "id = ?", assetID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrMediaAssetNotFound
			}
			return err
		}
		if asset.Status != MediaAssetDeleteFailed && asset.Status != MediaAssetDeleteRequested {
			return ErrMediaAssetNotFound
		}
		now := time.Now()
		var activeJob MediaDeletionJob
		activeLookup := tx.Where("asset_id = ?", assetID).First(&activeJob).Error
		if activeLookup != nil && !errors.Is(activeLookup, gorm.ErrRecordNotFound) {
			return activeLookup
		}
		if activeLookup == nil && activeJob.Status == MediaJobRunning && activeJob.LeaseExpiresAt != nil && activeJob.LeaseExpiresAt.After(now) {
			return nil
		}
		if err := tx.Model(&asset).Updates(map[string]any{"status": MediaAssetDeleteRequested, "state_version": gorm.Expr("state_version + 1"), "updated_at": now}).Error; err != nil {
			return err
		}
		if asset.ObjectID != "" {
			if err := tx.Model(&MediaObject{}).Where("id = ? AND ref_count <= 0", asset.ObjectID).Updates(map[string]any{"state": MediaObjectDeletePending, "next_retry_at": nil, "updated_at": now}).Error; err != nil {
				return err
			}
		}
		var job MediaDeletionJob
		if err := tx.Where("asset_id = ?", assetID).First(&job).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return tx.Create(&MediaDeletionJob{AssetID: assetID, ObjectID: asset.ObjectID, Status: MediaJobPending}).Error
		} else if err != nil {
			return err
		}
		return tx.Model(&job).Updates(map[string]any{"status": MediaJobPending, "next_attempt_at": nil, "lease_owner": "", "lease_expires_at": nil, "last_error": "", "updated_at": now}).Error
	})
}
func RetryMediaAssetDeletion(assetID uint) error {
	return RetryMediaAssetDeletionContext(context.Background(), assetID)
}

// MarkMediaAssetDeleteFailed records a physical cleanup failure while keeping
// the public capability revoked. The deletion job is failed separately so the
// durable worker can schedule its next attempt.
func MarkMediaAssetDeleteFailedContext(ctx context.Context, assetID uint, failure string, next time.Time) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var asset MediaAsset
		if err := tx.First(&asset, "id = ?", assetID).Error; err != nil {
			return err
		}
		if asset.Status == MediaAssetDeleted {
			return nil
		}
		now := time.Now()
		if err := tx.Model(&asset).Updates(map[string]any{"status": MediaAssetDeleteFailed, "delete_reason": strings.TrimSpace(failure), "state_version": gorm.Expr("state_version + 1"), "updated_at": now}).Error; err != nil {
			return err
		}
		if asset.ObjectID != "" {
			if err := tx.Model(&MediaObject{}).Where("id = ? AND ref_count <= 0", asset.ObjectID).Updates(map[string]any{"state": MediaObjectDeleteFailed, "delete_attempts": gorm.Expr("delete_attempts + 1"), "next_retry_at": nullableTime(next), "last_error": strings.TrimSpace(failure), "updated_at": now}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func MarkMediaAssetDeleteFailed(assetID uint, failure string, next time.Time) error {
	return MarkMediaAssetDeleteFailedContext(context.Background(), assetID, failure, next)
}

func RetryMediaAssetMaterialization(assetID uint) error {
	return RetryMediaAssetMaterializationContext(context.Background(), assetID)
}

func CreateMediaObjectContext(ctx context.Context, object *MediaObject) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if object == nil || strings.TrimSpace(object.ID) == "" || strings.TrimSpace(object.StorageKey) == "" {
		return errors.New("media object identity is required")
	}
	if object.State == "" {
		object.State = MediaObjectReady
	}
	if object.RefCount <= 0 {
		object.RefCount = 1
	}
	return db.Create(object).Error
}

func CreateMediaObject(object *MediaObject) error {
	return CreateMediaObjectContext(context.Background(), object)
}

func CreateMediaMaterializationJobContext(ctx context.Context, job *MediaMaterializationJob) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	if job == nil || job.AssetID == 0 {
		return errors.New("materialization job asset is required")
	}
	if job.Status == "" {
		job.Status = MediaJobPending
	}
	return db.Where("asset_id = ?", job.AssetID).FirstOrCreate(job).Error
}

func CreateMediaMaterializationJob(job *MediaMaterializationJob) error {
	return CreateMediaMaterializationJobContext(context.Background(), job)
}

func GetMediaMaterializationJobContext(ctx context.Context, assetID uint) (*MediaMaterializationJob, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	var job MediaMaterializationJob
	if err := db.Where("asset_id = ?", assetID).First(&job).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrMediaJobUnavailable
		}
		return nil, err
	}
	return &job, nil
}

func GetMediaMaterializationJob(assetID uint) (*MediaMaterializationJob, error) {
	return GetMediaMaterializationJobContext(context.Background(), assetID)
}

// ClaimMediaMaterializationJob claims one due job for a bounded lease. The
// transaction keeps selection and lease assignment together so two workers do
// not both process the same asset on a single-node SQLite deployment.
func ClaimMediaMaterializationJobContext(ctx context.Context, owner string, lease time.Duration) (*MediaMaterializationJob, error) {
	db, err := mediaDB(ctx)
	if err != nil {
		return nil, err
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, errors.New("media job lease owner is required")
	}
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	now := time.Now()
	var claimed MediaMaterializationJob
	claimedAttempts := 0
	err = db.Transaction(func(tx *gorm.DB) error {
		result := tx.Where("status IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?) AND (lease_expires_at IS NULL OR lease_expires_at <= ?)", []string{MediaJobPending, MediaJobRunning, MediaJobFailed}, now, now).
			Order("COALESCE(next_attempt_at, created_at) ASC, id ASC").First(&claimed)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return ErrMediaJobUnavailable
		}
		if result.Error != nil {
			return result.Error
		}
		claimedAttempts = claimed.Attempts + 1
		expires := now.Add(lease)
		return tx.Model(&claimed).Updates(map[string]any{
			"status":           MediaJobRunning,
			"lease_owner":      owner,
			"lease_expires_at": expires,
			"attempts":         claimed.Attempts + 1,
			"updated_at":       now,
		}).Error
	})
	if err != nil {
		return nil, err
	}
	claimed.Status = MediaJobRunning
	claimed.LeaseOwner = owner
	expires := now.Add(lease)
	claimed.LeaseExpiresAt = &expires
	claimed.Attempts = claimedAttempts
	claimed.UpdatedAt = now
	return &claimed, nil
}

func ClaimMediaMaterializationJob(owner string, lease time.Duration) (*MediaMaterializationJob, error) {
	return ClaimMediaMaterializationJobContext(context.Background(), owner, lease)
}

func CompleteMediaMaterializationJobContext(ctx context.Context, id uint) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	result := db.Model(&MediaMaterializationJob{}).Where("id = ?", id).Updates(map[string]any{
		"status": MediaJobSucceeded, "lease_owner": "", "lease_expires_at": nil, "last_error": "", "updated_at": time.Now(),
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaJobUnavailable
	}
	return nil
}

// CompleteMediaMaterializationJobForLeaseContext only allows the worker that
// currently owns a live lease to complete a materialization job. A stale
// worker must not overwrite a job after another worker has taken it over.
func CompleteMediaMaterializationJobForLeaseContext(ctx context.Context, id uint, owner string) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	owner = strings.TrimSpace(owner)
	if id == 0 || owner == "" {
		return errors.New("media materialization lease identity is required")
	}
	result := db.Model(&MediaMaterializationJob{}).
		Where("id = ? AND status = ? AND lease_owner = ? AND lease_expires_at > ?", id, MediaJobRunning, owner, time.Now()).
		Updates(map[string]any{"status": MediaJobSucceeded, "lease_owner": "", "lease_expires_at": nil, "last_error": "", "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaJobLeaseOwner
	}
	return nil
}

func CompleteMediaMaterializationJob(id uint) error {
	return CompleteMediaMaterializationJobContext(context.Background(), id)
}

func CompleteMediaMaterializationJobForLease(id uint, owner string) error {
	return CompleteMediaMaterializationJobForLeaseContext(context.Background(), id, owner)
}

func FailMediaMaterializationJobContext(ctx context.Context, id uint, failure string, next time.Time) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	status := MediaJobFailed
	var nextAttempt any
	if !next.IsZero() {
		status = MediaJobPending
		nextAttempt = next
	}
	result := db.Model(&MediaMaterializationJob{}).Where("id = ?", id).Updates(map[string]any{
		"status": status, "lease_owner": "", "lease_expires_at": nil, "next_attempt_at": nextAttempt,
		"last_error": strings.TrimSpace(failure), "updated_at": time.Now(),
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaJobUnavailable
	}
	return nil
}

// FailMediaMaterializationJobForLeaseContext is the lease-checked retry/error
// path for materialization workers.
func FailMediaMaterializationJobForLeaseContext(ctx context.Context, id uint, owner, failure string, next time.Time) error {
	db, err := mediaDB(ctx)
	if err != nil {
		return err
	}
	owner = strings.TrimSpace(owner)
	if id == 0 || owner == "" {
		return errors.New("media materialization lease identity is required")
	}
	status := MediaJobFailed
	var nextAttempt any
	if !next.IsZero() {
		status = MediaJobPending
		nextAttempt = next
	}
	result := db.Model(&MediaMaterializationJob{}).
		Where("id = ? AND status = ? AND lease_owner = ? AND lease_expires_at > ?", id, MediaJobRunning, owner, time.Now()).
		Updates(map[string]any{"status": status, "lease_owner": "", "lease_expires_at": nil, "next_attempt_at": nextAttempt, "last_error": strings.TrimSpace(failure), "updated_at": time.Now()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrMediaJobLeaseOwner
	}
	return nil
}

func FailMediaMaterializationJob(id uint, failure string, next time.Time) error {
	return FailMediaMaterializationJobContext(context.Background(), id, failure, next)
}
