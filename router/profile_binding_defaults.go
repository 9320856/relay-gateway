package router

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/profilebootstrap"
	"relay-gateway/protocol"
)

// ensureDefaultProfileBindings adopts the published built-in profile for a
// newly-created channel. Existing channels are intentionally left
// untouched: migration of an existing deployment must remain an explicit,
// auditable binding change. A low-precedence wildcard gives operators room to
// add narrower model bindings without changing the default.
func ensureDefaultProfileBindings(ctx context.Context, channelType, channelID string) error {
	channelType = strings.ToLower(strings.TrimSpace(channelType))
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return nil
	}
	switch channelType {
	case protocol.PresetOpenAI, protocol.PresetAnthropic, protocol.PresetNewAPI, protocol.PresetSub2API:
	default:
		return nil
	}

	profileID := profilebootstrap.BuiltinProfileID(channelType)
	profile, err := db.GetProtocolProfileContext(ctx, profileID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("builtin profile %q is not materialized", profileID)
	}
	if err != nil {
		return fmt.Errorf("load builtin profile %q: %w", profileID, err)
	}
	if profile.Source != db.ProfileSourceBuiltin || profile.LatestRevision <= 0 {
		return fmt.Errorf("builtin profile %q is not published", profileID)
	}
	revision, err := db.GetProtocolProfileRevisionContext(ctx, profileID, profile.LatestRevision)
	if err != nil {
		return fmt.Errorf("load builtin profile %q revision: %w", profileID, err)
	}
	if revision.State != db.ProfileRevisionPublished {
		return fmt.Errorf("builtin profile %q revision %d is not published", profileID, revision.Revision)
	}
	draft, err := protocol.BuiltinPreset(channelType)
	if err != nil {
		return err
	}
	for _, op := range draft.Operations {
		var existing db.ChannelProtocolBinding
		lookup := db.DBForContext(ctx).
			Where("channel_id = ? AND operation = ? AND model_pattern = ? AND precedence = ?", channelID, op.Operation, "*", -100).
			First(&existing)
		if lookup.Error == nil {
			// A concurrent create (or an operator's explicit binding) already
			// owns this exact slot. Preserve it rather than reporting a conflict.
			continue
		}
		if !errors.Is(lookup.Error, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check default profile binding %s: %w", op.Operation, lookup.Error)
		}
		if err := db.SaveChannelProtocolBindingContext(ctx, &db.ChannelProtocolBinding{
			ChannelID: channelID, Operation: op.Operation, ModelPattern: "*",
			ProfileID: profileID, ProfileRevision: revision.Revision,
			Precedence: -100, Enabled: true,
		}); err != nil {
			return fmt.Errorf("create default profile binding %s: %w", op.Operation, err)
		}
	}
	return nil
}

// removeDefaultProfileBindingsForType removes only the wildcard, lowest-
// precedence bindings that the gateway created for a built-in channel type.
// It deliberately leaves operator-managed bindings alone, including custom
// profile IDs and any binding with a higher precedence.
func removeDefaultProfileBindingsForType(ctx context.Context, channelType, channelID string) error {
	channelType = strings.ToLower(strings.TrimSpace(channelType))
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return nil
	}
	switch channelType {
	case protocol.PresetOpenAI, protocol.PresetAnthropic, protocol.PresetNewAPI, protocol.PresetSub2API:
	default:
		return nil
	}

	profileID := profilebootstrap.BuiltinProfileID(channelType)
	profile, err := db.GetProtocolProfileContext(ctx, profileID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load builtin profile %q: %w", profileID, err)
	}
	if profile.Source != db.ProfileSourceBuiltin {
		return nil
	}
	if err := db.DBForContext(ctx).
		Where("channel_id = ? AND profile_id = ? AND model_pattern = ? AND precedence = ?", channelID, profileID, "*", -100).
		Delete(&db.ChannelProtocolBinding{}).Error; err != nil {
		return fmt.Errorf("remove default profile bindings for %q: %w", channelType, err)
	}
	return nil
}

// ReconcileBindingsReport summarizes channels processed by ReconcileAllChannelsProfileBindings.
type ReconcileBindingsReport struct {
	ReconciledChannels []string `json:"reconciled_channels"`
	SkippedChannels    []string `json:"skipped_channels"`
}

// ReconcileChannelProfileBindings reconciles all operations of a channel's
// matching built-in profile to
// default wildcard bindings (precedence -100).
// If pruneMismatchedDefaults is true, any existing default wildcard bindings
// pointing to a different profile are cleaned up.
func ReconcileChannelProfileBindings(ctx context.Context, channelID string, pruneMismatchedDefaults bool) error {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return errors.New("channel_id is required")
	}
	database := db.DBForContext(ctx)
	if database == nil {
		return errors.New("database is not initialized")
	}
	var channel db.ChannelModel
	if err := database.WithContext(ctx).Where("id = ?", channelID).First(&channel).Error; err != nil {
		return fmt.Errorf("lookup channel %q: %w", channelID, err)
	}
	channelType := strings.ToLower(strings.TrimSpace(channel.Type))
	profileID := profilebootstrap.BuiltinProfileID(channelType)
	profile, err := db.GetProtocolProfileContext(ctx, profileID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("builtin profile %q is not materialized", profileID)
	}
	if err != nil {
		return fmt.Errorf("load builtin profile %q: %w", profileID, err)
	}
	if profile.LatestRevision <= 0 {
		return fmt.Errorf("builtin profile %q has no published revision", profileID)
	}
	revision, err := db.GetProtocolProfileRevisionContext(ctx, profileID, profile.LatestRevision)
	if err != nil {
		return fmt.Errorf("load builtin profile %q revision %d: %w", profileID, profile.LatestRevision, err)
	}
	if revision.State != db.ProfileRevisionPublished {
		return fmt.Errorf("builtin profile %q revision %d is not published", profileID, revision.Revision)
	}
	draft, err := protocol.BuiltinPreset(channelType)
	if err != nil {
		return fmt.Errorf("builtin preset %q: %w", channelType, err)
	}

	if pruneMismatchedDefaults {
		// Clean up default wildcard bindings for this channel pointing to any other profile
		if err := database.WithContext(ctx).
			Where("channel_id = ? AND precedence = ? AND model_pattern = ? AND profile_id != ?", channelID, -100, "*", profileID).
			Delete(&db.ChannelProtocolBinding{}).Error; err != nil {
			return fmt.Errorf("prune mismatched default bindings: %w", err)
		}
	}

	for _, op := range draft.Operations {
		var existing db.ChannelProtocolBinding
		lookup := database.WithContext(ctx).
			Where("channel_id = ? AND operation = ? AND model_pattern = ? AND precedence = ?", channelID, op.Operation, "*", -100).
			First(&existing)
		if lookup.Error == nil {
			if existing.ProfileID != profileID || existing.ProfileRevision != revision.Revision || !existing.Enabled {
				existing.ProfileID = profileID
				existing.ProfileRevision = revision.Revision
				existing.Enabled = true
				if err := db.SaveChannelProtocolBindingContext(ctx, &existing); err != nil {
					return fmt.Errorf("update default binding %s: %w", op.Operation, err)
				}
			}
			continue
		}
		if !errors.Is(lookup.Error, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check default profile binding %s: %w", op.Operation, lookup.Error)
		}
		if err := db.SaveChannelProtocolBindingContext(ctx, &db.ChannelProtocolBinding{
			ChannelID: channelID, Operation: op.Operation, ModelPattern: "*",
			ProfileID: profileID, ProfileRevision: revision.Revision,
			Precedence: -100, Enabled: true,
		}); err != nil {
			return fmt.Errorf("create default profile binding %s: %w", op.Operation, err)
		}
	}
	return nil
}

// ReconcileAllChannelsProfileBindings reconciles all enabled channels that
// match a supported built-in profile.
func ReconcileAllChannelsProfileBindings(ctx context.Context, pruneMismatchedDefaults bool) (ReconcileBindingsReport, error) {
	database := db.DBForContext(ctx)
	if database == nil {
		return ReconcileBindingsReport{}, errors.New("database is not initialized")
	}
	var channels []db.ChannelModel
	if err := database.WithContext(ctx).Where("enabled = ?", true).Find(&channels).Error; err != nil {
		return ReconcileBindingsReport{}, fmt.Errorf("query enabled channels: %w", err)
	}
	var report ReconcileBindingsReport
	for _, ch := range channels {
		chType := strings.ToLower(strings.TrimSpace(ch.Type))
		switch chType {
		case protocol.PresetOpenAI, protocol.PresetAnthropic, protocol.PresetNewAPI, protocol.PresetSub2API:
			if err := ReconcileChannelProfileBindings(ctx, ch.ID, pruneMismatchedDefaults); err != nil {
				return report, fmt.Errorf("reconcile channel %q (%s): %w", ch.ID, ch.Type, err)
			}
			report.ReconciledChannels = append(report.ReconciledChannels, ch.ID)
		default:
			report.SkippedChannels = append(report.SkippedChannels, ch.ID)
		}
	}
	return report, nil
}
