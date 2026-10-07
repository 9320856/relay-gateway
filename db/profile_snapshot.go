package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"relay-gateway/protocol"
)

// ErrProfileSnapshotInvalid means that stored metadata cannot identify the
// immutable protocol captured by a Profile task. Callers must not replace it
// with a current channel binding or another revision.
var ErrProfileSnapshotInvalid = errors.New("profile task snapshot is invalid")

// ProtocolProfileSnapshot retains an immutable protocol after its catalog
// revision is deleted. Tasks share one canonical copy through ProfileDigest;
// channel credentials and mutable channel configuration are never copied here.
type ProtocolProfileSnapshot struct {
	ContentDigest string    `gorm:"primaryKey;size:128" json:"content_digest"`
	SchemaVersion int       `gorm:"not null" json:"schema_version"`
	ContentJSON   string    `gorm:"type:text;not null" json:"content_json"`
	CreatedAt     time.Time `json:"created_at"`
}

func (ProtocolProfileSnapshot) TableName() string { return "protocol_profile_snapshots" }

// GetTaskProfileRevisionContext resolves only the protocol captured by run.
// An attached task reads its immutable catalog revision; detached history uses
// its exact digest archive. Missing catalog rows may also use the archive when
// a reader held the TaskRun across a concurrent catalog deletion.
func GetTaskProfileRevisionContext(ctx context.Context, run *TaskRun) (*ProtocolProfileRevision, error) {
	revision, _, err := getTaskProfileSnapshot(ctx, run)
	return revision, err
}

// LoadTaskProfileContext returns the compiled immutable snapshot validated by
// the same loader as GetTaskProfileRevisionContext. Runtime consumers reuse that
// compilation instead of decoding and validating the profile a second time.
func LoadTaskProfileContext(ctx context.Context, run *TaskRun) (protocol.CompiledProfile, error) {
	_, compiled, err := getTaskProfileSnapshot(ctx, run)
	return compiled, err
}

func getTaskProfileSnapshot(ctx context.Context, run *TaskRun) (*ProtocolProfileRevision, protocol.CompiledProfile, error) {
	var empty protocol.CompiledProfile
	if run == nil || !strings.EqualFold(strings.TrimSpace(run.Engine), "profile") {
		return nil, empty, fmt.Errorf("%w: task is not Profile-owned", ErrProfileSnapshotInvalid)
	}
	database, err := profileDB(ctx)
	if err != nil {
		return nil, empty, err
	}
	digest := run.ProfileDigest
	if digest != "" && !validProfileSnapshotDigest(digest) {
		return nil, empty, fmt.Errorf("%w: task digest is not canonical SHA256", ErrProfileSnapshotInvalid)
	}
	if strings.TrimSpace(run.ProfileID) != "" && run.ProfileRevision > 0 {
		var revision ProtocolProfileRevision
		err := database.Where("profile_id = ? AND revision = ?", run.ProfileID, run.ProfileRevision).First(&revision).Error
		if err == nil {
			if revision.State != ProfileRevisionPublished && revision.State != ProfileRevisionRetired {
				return nil, empty, fmt.Errorf("%w: captured revision is not immutable", ErrProfileSnapshotInvalid)
			}
			_, compiled, err := canonicalProfileSnapshot(&revision, digest)
			if err != nil {
				return nil, empty, err
			}
			return &revision, compiled, nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, empty, err
		}
	}
	if digest == "" {
		return nil, empty, fmt.Errorf("%w: task has no retained profile digest", ErrProfileSnapshotInvalid)
	}
	var snapshot ProtocolProfileSnapshot
	if err := database.Where("content_digest = ?", digest).First(&snapshot).Error; err != nil {
		return nil, empty, err
	}
	// The archive is not a catalog revision and cannot authorize a new binding.
	// Retired state lets historical consumers use the immutable definition.
	revision := &ProtocolProfileRevision{
		SchemaVersion: snapshot.SchemaVersion, ContentJSON: snapshot.ContentJSON,
		ContentDigest: snapshot.ContentDigest, State: ProfileRevisionRetired,
		CreatedAt: snapshot.CreatedAt,
	}
	canonical, compiled, err := canonicalProfileSnapshot(revision, digest)
	if err != nil {
		return nil, empty, err
	}
	if canonical.ContentJSON != snapshot.ContentJSON || canonical.SchemaVersion != snapshot.SchemaVersion {
		return nil, empty, fmt.Errorf("%w: archived content is not canonical", ErrProfileSnapshotInvalid)
	}
	return revision, compiled, nil
}

func validProfileSnapshotDigest(digest string) bool {
	if len(digest) != 64 || digest != strings.ToLower(digest) {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func canonicalProfileSnapshot(revision *ProtocolProfileRevision, taskDigest string) (*ProtocolProfileSnapshot, protocol.CompiledProfile, error) {
	var empty protocol.CompiledProfile
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		return nil, empty, fmt.Errorf("%w: decode content: %v", ErrProfileSnapshotInvalid, err)
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		return nil, empty, fmt.Errorf("%w: compile content: %v", ErrProfileSnapshotInvalid, err)
	}
	digest := compiled.Digest()
	if revision.ContentDigest != digest || taskDigest != "" && taskDigest != digest {
		return nil, empty, fmt.Errorf("%w: content digest mismatch", ErrProfileSnapshotInvalid)
	}
	schemaVersion := compiled.Profile().SchemaVersion
	// Older valid Profiles omitted schema_version (zero) while the catalog's
	// GORM default stored one. The canonical JSON remains the snapshot identity.
	if revision.SchemaVersion != schemaVersion && !(schemaVersion == 0 && revision.SchemaVersion == 1) {
		return nil, empty, fmt.Errorf("%w: content schema version mismatch", ErrProfileSnapshotInvalid)
	}
	return &ProtocolProfileSnapshot{ContentDigest: digest, SchemaVersion: schemaVersion, ContentJSON: string(compiled.CanonicalJSON())}, compiled, nil
}

// archiveProfileTaskRevisions runs inside the deletion transaction. It validates
// and archives only revisions used by terminal Profile tasks, then fills digest
// metadata on older tasks before their catalog references are detached.
func archiveProfileTaskRevisions(tx *gorm.DB, profileID string, revisionNumber int) error {
	query := tx.Model(&TaskRun{}).Where("profile_id = ? AND LOWER(TRIM(engine)) = ?", profileID, "profile")
	if revisionNumber > 0 {
		query = query.Where("profile_revision = ?", revisionNumber)
	}
	var revisions []int
	if err := query.Distinct("profile_revision").Pluck("profile_revision", &revisions).Error; err != nil {
		return err
	}
	for _, number := range revisions {
		var revision ProtocolProfileRevision
		if err := tx.Where("profile_id = ? AND revision = ?", profileID, number).First(&revision).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: referenced revision is missing", ErrProfileSnapshotInvalid)
			}
			return err
		}
		if revision.State != ProfileRevisionPublished && revision.State != ProfileRevisionRetired {
			return fmt.Errorf("%w: referenced revision is not immutable", ErrProfileSnapshotInvalid)
		}
		snapshot, _, err := canonicalProfileSnapshot(&revision, "")
		if err != nil {
			return err
		}
		referenced := func() *gorm.DB {
			return tx.Model(&TaskRun{}).Where("profile_id = ? AND profile_revision = ? AND LOWER(TRIM(engine)) = ?", profileID, number, "profile")
		}
		var mismatched int64
		if err := referenced().Where("COALESCE(profile_digest, '') <> '' AND profile_digest <> ?", snapshot.ContentDigest).Count(&mismatched).Error; err != nil {
			return err
		}
		if mismatched > 0 {
			return fmt.Errorf("%w: referenced task digest mismatch", ErrProfileSnapshotInvalid)
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "content_digest"}}, DoNothing: true}).Create(snapshot).Error; err != nil {
			return err
		}
		var stored ProtocolProfileSnapshot
		if err := tx.First(&stored, "content_digest = ?", snapshot.ContentDigest).Error; err != nil {
			return err
		}
		if stored.ContentJSON != snapshot.ContentJSON || stored.SchemaVersion != snapshot.SchemaVersion {
			return fmt.Errorf("%w: immutable archive conflicts with revision", ErrProfileSnapshotInvalid)
		}
		if err := referenced().Where("COALESCE(profile_digest, '') = ''").Update("profile_digest", snapshot.ContentDigest).Error; err != nil {
			return err
		}
	}
	return nil
}
