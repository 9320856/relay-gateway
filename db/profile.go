package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

var (
	ErrPublishedRevisionImmutable = errors.New("published profile revision is immutable")
	ErrRetiredRevisionImmutable   = errors.New("retired profile revision is immutable")
	ErrRevisionHasBindings        = errors.New("profile revision still has channel bindings")
	ErrRevisionHasActiveTasks     = errors.New("profile revision still has active task runs")
	ErrRevisionHasActiveBindings  = errors.New("profile revision still has enabled channel bindings")
	ErrBindingConflict            = errors.New("channel protocol binding conflicts with an equally-preferred model pattern")
	ErrBuiltinProfileImmutable    = errors.New("cannot delete built-in protocol profile")
	ErrProfileHasBindings         = errors.New("profile still has channel bindings")
	ErrProfileHasTaskRuns         = errors.New("profile is referenced by historical tasks")
)

var (
	ErrInvalidProfileSource = errors.New("invalid profile source")
	ErrInvalidRevisionState = errors.New("invalid profile revision state")
)

const (
	ProfileSourceBuiltin = "builtin"
	ProfileSourceCustom  = "custom"

	ProfileRevisionDraft     = "draft"
	ProfileRevisionPublished = "published"
	ProfileRevisionRetired   = "retired"
)

type ProtocolProfile struct {
	ID             string    `gorm:"primaryKey;size:64" json:"id"`
	Name           string    `gorm:"size:128;not null" json:"name"`
	Source         string    `gorm:"size:16;not null;default:'custom'" json:"source"`
	LatestRevision int       `gorm:"not null;default:0" json:"latest_revision"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (ProtocolProfile) TableName() string { return "protocol_profiles" }

type ProtocolProfileRevision struct {
	ID            uint      `gorm:"primaryKey" json:"id"`
	ProfileID     string    `gorm:"size:64;not null;uniqueIndex:idx_profile_revision" json:"profile_id"`
	Revision      int       `gorm:"not null;uniqueIndex:idx_profile_revision" json:"revision"`
	SchemaVersion int       `gorm:"not null;default:1" json:"schema_version"`
	ContentJSON   string    `gorm:"type:text;not null" json:"content_json"`
	ContentDigest string    `gorm:"size:128;not null" json:"content_digest"`
	State         string    `gorm:"size:16;not null;index" json:"state"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (ProtocolProfileRevision) TableName() string { return "protocol_profile_revisions" }

type ChannelProtocolBinding struct {
	ID              uint      `gorm:"primaryKey" json:"id"`
	ChannelID       string    `gorm:"size:64;not null;index:idx_channel_binding_lookup,priority:1" json:"channel_id"`
	Operation       string    `gorm:"size:64;not null;index:idx_channel_binding_lookup,priority:2" json:"operation"`
	ModelPattern    string    `gorm:"size:255;not null;index:idx_channel_binding_lookup,priority:3" json:"model_pattern"`
	ProfileID       string    `gorm:"size:64;not null" json:"profile_id"`
	ProfileRevision int       `gorm:"not null" json:"profile_revision"`
	Precedence      int       `gorm:"not null;default:0;index" json:"precedence"`
	Enabled         bool      `gorm:"not null;index" json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (ChannelProtocolBinding) TableName() string { return "channel_protocol_bindings" }

// ProfileRevisionReferences summarizes durable references that prevent an
// operator from treating a revision as unused. Bindings affect new requests;
// TaskRuns (especially non-terminal ones) keep the immutable revision needed
// to poll already-accepted work.
type ProfileRevisionReferences struct {
	Bindings            int64 `json:"bindings"`
	EnabledBindings     int64 `json:"enabled_bindings"`
	TaskRuns            int64 `json:"task_runs"`
	NonTerminalTaskRuns int64 `json:"non_terminal_task_runs"`
}

var terminalTaskStatuses = []string{"completed", "succeeded", "failed", "cancelled", "canceled", "expired", "timeout", "timed_out"}

func profileDB(ctx context.Context) (*gorm.DB, error) {
	db := DBForContext(ctx)
	if db == nil {
		return nil, errors.New("database is not initialized")
	}
	return db, nil
}

func CreateProtocolProfile(p *ProtocolProfile) error {
	return CreateProtocolProfileContext(context.Background(), p)
}

func CreateProtocolProfileContext(ctx context.Context, p *ProtocolProfile) error {
	db, err := profileDB(ctx)
	if err != nil {
		return err
	}
	if p == nil || strings.TrimSpace(p.ID) == "" {
		return errors.New("profile id is required")
	}
	if p.Source == "" {
		p.Source = ProfileSourceCustom
	}
	if p.Source != ProfileSourceBuiltin && p.Source != ProfileSourceCustom {
		return fmt.Errorf("%w: %q", ErrInvalidProfileSource, p.Source)
	}
	p.ID = strings.TrimSpace(p.ID)
	p.Name = strings.TrimSpace(p.Name)
	return db.Create(p).Error
}

// CreateProtocolProfileWithInitialRevisionContext creates a custom profile and
// its first draft as one durable unit. A profile without a revision cannot be
// configured or bound, so exposing one after a failed second write leaves the
// management API in a misleading state.
func CreateProtocolProfileWithInitialRevisionContext(ctx context.Context, p *ProtocolProfile, r *ProtocolProfileRevision) error {
	database, err := profileDB(ctx)
	if err != nil {
		return err
	}
	if p == nil || r == nil || strings.TrimSpace(p.ID) != strings.TrimSpace(r.ProfileID) {
		return errors.New("profile and initial revision must have the same identity")
	}
	create := func(tx *gorm.DB) error {
		txCtx := WithTx(ctx, tx)
		if err := CreateProtocolProfileContext(txCtx, p); err != nil {
			return err
		}
		return SaveProtocolProfileRevisionContext(txCtx, r)
	}
	if database == DB {
		return database.WithContext(ctx).Transaction(create)
	}
	return create(database)
}

func GetProtocolProfile(id string) (*ProtocolProfile, error) {
	return GetProtocolProfileContext(context.Background(), id)
}

func GetProtocolProfileContext(ctx context.Context, id string) (*ProtocolProfile, error) {
	db, err := profileDB(ctx)
	if err != nil {
		return nil, err
	}
	var p ProtocolProfile
	if err := db.First(&p, "id = ?", strings.TrimSpace(id)).Error; err != nil {
		return nil, err
	}
	return &p, nil
}

func SaveProtocolProfileRevision(r *ProtocolProfileRevision) error {
	return SaveProtocolProfileRevisionContext(context.Background(), r)
}

func SaveProtocolProfileRevisionContext(ctx context.Context, r *ProtocolProfileRevision) error {
	db, err := profileDB(ctx)
	if err != nil {
		return err
	}
	if r == nil || strings.TrimSpace(r.ProfileID) == "" || r.Revision <= 0 {
		return errors.New("profile revision identity is required")
	}
	r.ProfileID = strings.TrimSpace(r.ProfileID)
	if r.State == "" {
		r.State = ProfileRevisionDraft
	}
	if r.State != ProfileRevisionDraft && r.State != ProfileRevisionPublished && r.State != ProfileRevisionRetired {
		return fmt.Errorf("%w: %q", ErrInvalidRevisionState, r.State)
	}
	var profile ProtocolProfile
	if err := db.First(&profile, "id = ?", r.ProfileID).Error; err != nil {
		return fmt.Errorf("load profile %q: %w", r.ProfileID, err)
	}
	var old ProtocolProfileRevision
	if e := db.Where("profile_id = ? AND revision = ?", r.ProfileID, r.Revision).First(&old).Error; e == nil {
		switch old.State {
		case ProfileRevisionPublished:
			return ErrPublishedRevisionImmutable
		case ProfileRevisionRetired:
			return ErrRetiredRevisionImmutable
		}
		r.ID, r.CreatedAt = old.ID, old.CreatedAt
		if err := db.Save(r).Error; err != nil {
			return err
		}
		if r.Revision > profile.LatestRevision {
			return db.Model(&profile).Update("latest_revision", r.Revision).Error
		}
		return nil
	} else if !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	if err := db.Create(r).Error; err != nil {
		return err
	}
	if r.Revision > profile.LatestRevision {
		return db.Model(&profile).Update("latest_revision", r.Revision).Error
	}
	return nil
}

// DeleteProtocolProfileRevisionContext removes an unreferenced custom revision.
// A published or retired revision can be removed once it has no channel
// bindings and no active task runs. Completed task history is detached from the
// catalog entry before deletion so the audit record remains available without
// pointing at a missing immutable revision. The profile's latest revision
// pointer is recalculated after deletion so a later draft can reuse the next
// available number without leaving stale metadata behind.
func DeleteProtocolProfileRevisionContext(ctx context.Context, profileID string, revision int) error {
	database, err := profileDB(ctx)
	if err != nil {
		return err
	}
	profileID = strings.TrimSpace(profileID)
	if profileID == "" || revision <= 0 {
		return errors.New("profile revision identity is required")
	}
	remove := func(tx *gorm.DB) error {
		var profile ProtocolProfile
		if err := tx.Where("id = ?", profileID).First(&profile).Error; err != nil {
			return err
		}
		var current ProtocolProfileRevision
		if err := tx.Where("profile_id = ? AND revision = ?", profileID, revision).First(&current).Error; err != nil {
			return err
		}
		if profile.Source == ProfileSourceBuiltin {
			return ErrBuiltinProfileImmutable
		}
		switch current.State {
		case ProfileRevisionDraft, ProfileRevisionPublished, ProfileRevisionRetired:
		default:
			return fmt.Errorf("%w: %q", ErrInvalidRevisionState, current.State)
		}
		var bindingCount int64
		if err := tx.Model(&ChannelProtocolBinding{}).
			Where("profile_id = ? AND profile_revision = ?", profileID, revision).
			Count(&bindingCount).Error; err != nil {
			return err
		}
		if bindingCount > 0 {
			return fmt.Errorf("%w: %d", ErrRevisionHasBindings, bindingCount)
		}
		var activeTaskCount int64
		if err := tx.Model(&TaskRun{}).
			Where("profile_id = ? AND profile_revision = ? AND task_status NOT IN ?", profileID, revision, terminalTaskStatuses).
			Count(&activeTaskCount).Error; err != nil {
			return err
		}
		if activeTaskCount > 0 {
			return fmt.Errorf("%w: %d", ErrRevisionHasActiveTasks, activeTaskCount)
		}
		if err := tx.Model(&TaskRun{}).
			Where("profile_id = ? AND profile_revision = ?", profileID, revision).
			Updates(map[string]any{"profile_id": "", "profile_revision": 0}).Error; err != nil {
			return err
		}
		if err := tx.Delete(&current).Error; err != nil {
			return err
		}
		var latestResult struct {
			Value int `gorm:"column:value"`
		}
		if err := tx.Model(&ProtocolProfileRevision{}).
			Where("profile_id = ?", profileID).
			Select("COALESCE(MAX(revision), 0) AS value").Scan(&latestResult).Error; err != nil {
			return err
		}
		return tx.Model(&ProtocolProfile{}).Where("id = ?", profileID).Update("latest_revision", latestResult.Value).Error
	}
	if database == DB {
		return database.WithContext(ctx).Transaction(remove)
	}
	return remove(database.WithContext(ctx))
}

func DeleteProtocolProfileRevision(profileID string, revision int) error {
	return DeleteProtocolProfileRevisionContext(context.Background(), profileID, revision)
}

func PublishProtocolProfileRevision(profileID string, revision int) error {
	return PublishProtocolProfileRevisionContext(context.Background(), profileID, revision)
}

func PublishProtocolProfileRevisionContext(ctx context.Context, profileID string, revision int) error {
	db, err := profileDB(ctx)
	if err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var r ProtocolProfileRevision
		if err := tx.Where("profile_id = ? AND revision = ?", profileID, revision).First(&r).Error; err != nil {
			return err
		}
		if r.State == ProfileRevisionPublished {
			return nil
		}
		if r.State != ProfileRevisionDraft && r.State != ProfileRevisionRetired {
			return errors.New("only draft or retired revisions can be published")
		}
		now := time.Now()
		if err := tx.Model(&r).Updates(map[string]any{"state": ProfileRevisionPublished, "updated_at": now}).Error; err != nil {
			return err
		}
		var profile ProtocolProfile
		if err := tx.Where("id = ?", profileID).First(&profile).Error; err != nil {
			return err
		}
		updates := map[string]any{"updated_at": now}
		if revision >= profile.LatestRevision || profile.LatestRevision <= 0 {
			updates["latest_revision"] = revision
		}
		return tx.Model(&ProtocolProfile{}).Where("id = ?", profileID).Updates(updates).Error
	})
}

// RetireProtocolProfileRevision makes a published revision unavailable for
// new bindings while preserving it for tasks that already captured it.
func RetireProtocolProfileRevision(profileID string, revision int) error {
	return RetireProtocolProfileRevisionContext(context.Background(), profileID, revision)
}

func RetireProtocolProfileRevisionContext(ctx context.Context, profileID string, revision int) error {
	db, err := profileDB(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(profileID) == "" || revision <= 0 {
		return errors.New("profile revision identity is required")
	}
	profileID = strings.TrimSpace(profileID)
	return db.Transaction(func(tx *gorm.DB) error {
		var current ProtocolProfileRevision
		if err := tx.Where("profile_id = ? AND revision = ?", profileID, revision).First(&current).Error; err != nil {
			return err
		}
		if current.State == ProfileRevisionRetired {
			return nil
		}
		if current.State != ProfileRevisionPublished {
			return gorm.ErrRecordNotFound
		}
		var activeBindings int64
		if err := tx.Model(&ChannelProtocolBinding{}).Where("profile_id = ? AND profile_revision = ? AND enabled = ?", profileID, revision, true).Count(&activeBindings).Error; err != nil {
			return err
		}
		if activeBindings > 0 {
			return fmt.Errorf("%w: %d", ErrRevisionHasActiveBindings, activeBindings)
		}
		result := tx.Model(&ProtocolProfileRevision{}).
			Where("profile_id = ? AND revision = ? AND state = ?", profileID, revision, ProfileRevisionPublished).
			Updates(map[string]any{"state": ProfileRevisionRetired, "updated_at": time.Now()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
}

// DeleteProtocolProfileContext deletes a custom protocol profile and its
// revisions if and only if it has zero bindings and zero historical task runs.
// Built-in profiles cannot be deleted.
func DeleteProtocolProfileContext(ctx context.Context, profileID string) error {
	db, err := profileDB(ctx)
	if err != nil {
		return err
	}
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return errors.New("profile id is required")
	}
	deleteProfile := func(tx *gorm.DB) error {
		var profile ProtocolProfile
		if err := tx.Where("id = ?", profileID).First(&profile).Error; err != nil {
			return err
		}
		if profile.Source == ProfileSourceBuiltin {
			return ErrBuiltinProfileImmutable
		}
		var bindingCount int64
		if err := tx.Model(&ChannelProtocolBinding{}).Where("profile_id = ?", profileID).Count(&bindingCount).Error; err != nil {
			return err
		}
		if bindingCount > 0 {
			return fmt.Errorf("%w: %d", ErrProfileHasBindings, bindingCount)
		}
		var taskCount int64
		if err := tx.Model(&TaskRun{}).Where("profile_id = ?", profileID).Count(&taskCount).Error; err != nil {
			return err
		}
		if taskCount > 0 {
			var activeCount int64
			if err := tx.Model(&TaskRun{}).Where("profile_id = ? AND task_status NOT IN ?", profileID, terminalTaskStatuses).Count(&activeCount).Error; err != nil {
				return err
			}
			if activeCount > 0 {
				return fmt.Errorf("%w: %d 个未完成任务", ErrProfileHasTaskRuns, activeCount)
			}
			// Completed tasks remain as audit history but no longer depend on the
			// deleted catalog entry. Keep their execution/result data intact.
			if err := tx.Model(&TaskRun{}).Where("profile_id = ?", profileID).Updates(map[string]any{"profile_id": "", "profile_revision": 0}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("profile_id = ?", profileID).Delete(&ProtocolProfileRevision{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", profileID).Delete(&ProtocolProfile{}).Error
	}
	if db == DB {
		return db.WithContext(ctx).Transaction(deleteProfile)
	}
	return deleteProfile(db)
}

func DeleteProtocolProfile(profileID string) error {
	return DeleteProtocolProfileContext(context.Background(), profileID)
}

func GetProtocolProfileRevision(profileID string, revision int) (*ProtocolProfileRevision, error) {
	return GetProtocolProfileRevisionContext(context.Background(), profileID, revision)
}

func GetProtocolProfileRevisionContext(ctx context.Context, profileID string, revision int) (*ProtocolProfileRevision, error) {
	db, err := profileDB(ctx)
	if err != nil {
		return nil, err
	}
	var r ProtocolProfileRevision
	if err := db.Where("profile_id = ? AND revision = ?", profileID, revision).First(&r).Error; err != nil {
		return nil, err
	}
	return &r, nil
}

// GetProtocolProfileRevisionReferencesContext returns binding and task usage
// counts for a revision. Counts are deliberately computed from the source
// tables rather than cached so the management UI cannot show stale safety
// information before a retire/delete decision.
func GetProtocolProfileRevisionReferencesContext(ctx context.Context, profileID string, revision int) (*ProfileRevisionReferences, error) {
	db, err := profileDB(ctx)
	if err != nil {
		return nil, err
	}
	profileID = strings.TrimSpace(profileID)
	if profileID == "" || revision <= 0 {
		return nil, errors.New("profile revision identity is required")
	}
	var result ProfileRevisionReferences
	query := db.Model(&ChannelProtocolBinding{}).Where("profile_id = ? AND profile_revision = ?", profileID, revision)
	if err := query.Count(&result.Bindings).Error; err != nil {
		return nil, err
	}
	if err := query.Where("enabled = ?", true).Count(&result.EnabledBindings).Error; err != nil {
		return nil, err
	}
	taskQuery := db.Model(&TaskRun{}).Where("profile_id = ? AND profile_revision = ?", profileID, revision)
	if err := taskQuery.Count(&result.TaskRuns).Error; err != nil {
		return nil, err
	}
	if err := taskQuery.Where("task_status NOT IN ?", terminalTaskStatuses).Count(&result.NonTerminalTaskRuns).Error; err != nil {
		return nil, err
	}
	return &result, nil
}

func GetProtocolProfileRevisionReferences(profileID string, revision int) (*ProfileRevisionReferences, error) {
	return GetProtocolProfileRevisionReferencesContext(context.Background(), profileID, revision)
}

func SaveChannelProtocolBinding(b *ChannelProtocolBinding) error {
	return SaveChannelProtocolBindingContext(context.Background(), b)
}

func SaveChannelProtocolBindingContext(ctx context.Context, b *ChannelProtocolBinding) error {
	db, err := profileDB(ctx)
	if err != nil {
		return err
	}
	if b == nil || strings.TrimSpace(b.ChannelID) == "" || strings.TrimSpace(b.Operation) == "" || strings.TrimSpace(b.ModelPattern) == "" {
		return errors.New("binding identity is required")
	}
	b.ChannelID = strings.TrimSpace(b.ChannelID)
	b.Operation = strings.TrimSpace(b.Operation)
	b.ModelPattern = strings.TrimSpace(b.ModelPattern)

	query := db.Where("channel_id = ? AND operation = ? AND precedence = ?", b.ChannelID, b.Operation, b.Precedence)
	if b.ID != 0 {
		query = query.Where("id <> ?", b.ID)
	}
	var peers []ChannelProtocolBinding
	if err := query.Find(&peers).Error; err != nil {
		return err
	}
	for _, peer := range peers {
		if bindingPatternsOverlap(b.ModelPattern, peer.ModelPattern) {
			return fmt.Errorf("%w: %q and %q at precedence %d", ErrBindingConflict, b.ModelPattern, peer.ModelPattern, b.Precedence)
		}
	}
	if b.ID != 0 {
		return db.Save(b).Error
	}
	// GORM applies the `default:true` tag to a zero-value bool unless the
	// field is explicitly selected. That would silently turn a requested
	// disabled binding back on, corrupting rollout and reference statistics.
	return db.Select("channel_id", "operation", "model_pattern", "profile_id", "profile_revision", "precedence", "enabled", "created_at", "updated_at").Create(b).Error
}

// SetChannelProtocolBindingEnabledContext changes a binding's rollout state.
// Enabling is conditional on its revision still being published, preventing a
// retired revision from being made routable again through the toggle endpoint.
func SetChannelProtocolBindingEnabledContext(ctx context.Context, id uint, enabled bool) (*ChannelProtocolBinding, error) {
	database, err := profileDB(ctx)
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, errors.New("binding id is required")
	}
	setEnabled := func(tx *gorm.DB) (*ChannelProtocolBinding, error) {
		var binding ChannelProtocolBinding
		if err := tx.First(&binding, id).Error; err != nil {
			return nil, err
		}
		if binding.Enabled == enabled {
			return &binding, nil
		}
		if enabled {
			var revision ProtocolProfileRevision
			if err := tx.Where("profile_id = ? AND revision = ? AND state = ?", binding.ProfileID, binding.ProfileRevision, ProfileRevisionPublished).First(&revision).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, ErrRetiredRevisionImmutable
				}
				return nil, err
			}
		}
		if err := tx.Model(&binding).Update("enabled", enabled).Error; err != nil {
			return nil, err
		}
		binding.Enabled = enabled
		return &binding, nil
	}
	if database == DB {
		var binding *ChannelProtocolBinding
		err := database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			binding, err = setEnabled(tx)
			return err
		})
		return binding, err
	}
	return setEnabled(database)
}

// bindingPatternsOverlap reports whether two supported binding patterns can
// select the same model. Patterns are exact names, a global "*", or a prefix
// followed by "*"; this mirrors FindChannelProtocolBindingContext.
func bindingPatternsOverlap(left, right string) bool {
	left, right = strings.TrimSpace(left), strings.TrimSpace(right)
	if left == "*" || right == "*" {
		return true
	}
	leftPrefix, leftWildcard := strings.CutSuffix(left, "*")
	rightPrefix, rightWildcard := strings.CutSuffix(right, "*")
	switch {
	case !leftWildcard && !rightWildcard:
		return left == right
	case leftWildcard && rightWildcard:
		return strings.HasPrefix(leftPrefix, rightPrefix) || strings.HasPrefix(rightPrefix, leftPrefix)
	case leftWildcard:
		return strings.HasPrefix(right, leftPrefix)
	default:
		return strings.HasPrefix(left, rightPrefix)
	}
}

func FindChannelProtocolBinding(channelID, operation, model string) (*ChannelProtocolBinding, error) {
	return FindChannelProtocolBindingContext(context.Background(), channelID, operation, model)
}

func FindChannelProtocolBindingContext(ctx context.Context, channelID, operation, model string) (*ChannelProtocolBinding, error) {
	db, err := profileDB(ctx)
	if err != nil {
		return nil, err
	}
	var bindings []ChannelProtocolBinding
	if err := db.Where("channel_id = ? AND operation = ? AND enabled = ?", channelID, operation, true).Order("precedence DESC, id ASC").Find(&bindings).Error; err != nil {
		return nil, err
	}
	for i := range bindings {
		if bindings[i].ModelPattern == model || bindings[i].ModelPattern == "*" || (strings.HasSuffix(bindings[i].ModelPattern, "*") && strings.HasPrefix(model, strings.TrimSuffix(bindings[i].ModelPattern, "*"))) {
			return &bindings[i], nil
		}
	}
	return nil, gorm.ErrRecordNotFound
}
