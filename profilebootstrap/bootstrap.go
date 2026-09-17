// Package profilebootstrap materializes the protocol package's catalog
// profiles into the durable profile catalog. It deliberately does not create
// channel bindings: adopting a profile is an explicit migration decision and
// existing channels must remain on the legacy engine by default.
package profilebootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/protocol"
)

const builtinProfilePrefix = "builtin-"

// Result describes the idempotent startup work performed by
// EnsureBuiltinProfiles.
type Result struct {
	Created  []string `json:"created,omitempty"`
	Existing []string `json:"existing,omitempty"`
}

// BuiltinProfileID returns the stable database id for a built-in preset.
func BuiltinProfileID(name string) string {
	return builtinProfilePrefix + strings.ToLower(strings.TrimSpace(name))
}

// EnsureBuiltinProfiles materializes every immutable built-in profile.
func EnsureBuiltinProfiles(ctx context.Context) (Result, error) {
	var result Result
	for _, name := range protocol.BuiltinPresetNames() {
		created, err := ensureBuiltinProfile(ctx, name)
		if err != nil {
			return result, err
		}
		if created {
			result.Created = append(result.Created, name)
		} else {
			result.Existing = append(result.Existing, name)
		}
	}
	return result, nil
}

func ensureBuiltinProfile(ctx context.Context, name string) (bool, error) {
	draft, err := protocol.BuiltinPreset(name)
	if err != nil {
		return false, err
	}
	compiled, err := protocol.Compile(draft)
	if err != nil {
		return false, fmt.Errorf("compile builtin profile %q: %w", name, err)
	}
	profileID := BuiltinProfileID(name)
	profile, err := db.GetProtocolProfileContext(ctx, profileID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		profile = &db.ProtocolProfile{ID: profileID, Name: draft.Name, Source: db.ProfileSourceBuiltin}
		if err := db.CreateProtocolProfileContext(ctx, profile); err != nil {
			return false, fmt.Errorf("create builtin profile %q: %w", name, err)
		}
		revision := &db.ProtocolProfileRevision{
			ProfileID:     profileID,
			Revision:      1,
			SchemaVersion: compiled.Profile().SchemaVersion,
			ContentJSON:   string(compiled.CanonicalJSON()),
			ContentDigest: compiled.Digest(),
			State:         db.ProfileRevisionDraft,
		}
		if err := db.SaveProtocolProfileRevisionContext(ctx, revision); err != nil {
			return false, fmt.Errorf("create builtin profile %q revision: %w", name, err)
		}
		if err := db.PublishProtocolProfileRevisionContext(ctx, profileID, 1); err != nil {
			return false, fmt.Errorf("publish builtin profile %q revision: %w", name, err)
		}
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("load builtin profile %q: %w", name, err)
	}
	if profile.Source != db.ProfileSourceBuiltin {
		return false, fmt.Errorf("builtin profile id %q is already owned by source %q", profileID, profile.Source)
	}
	if profile.LatestRevision <= 0 {
		return false, fmt.Errorf("builtin profile %q has no revision", profileID)
	}
	if _, err := db.GetProtocolProfileRevisionContext(ctx, profileID, profile.LatestRevision); err != nil {
		return false, fmt.Errorf("load builtin profile %q revision %d: %w", name, profile.LatestRevision, err)
	}
	// Existing revisions are operator-managed snapshots, including drafts and
	// retired versions. Catalog changes require an explicit new revision; startup
	// must preserve the versions pinned by channel bindings and accepted tasks.
	return false, nil
}
