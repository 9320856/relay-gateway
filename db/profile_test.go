package db

import (
	"context"
	"errors"
	"testing"

	"gorm.io/gorm"
)

func TestProtocolProfileRevisionLifecycle(t *testing.T) {
	if err := InitDB(t.TempDir() + "/profile.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	ctx := context.Background()
	if err := CreateProtocolProfileContext(ctx, &ProtocolProfile{ID: "p1", Name: "Custom", Source: "custom"}); err != nil {
		t.Fatal(err)
	}
	r := &ProtocolProfileRevision{ProfileID: "p1", Revision: 1, SchemaVersion: 1, ContentJSON: `{"operation":"video.create"}`, ContentDigest: "digest-1", State: "draft"}
	if err := SaveProtocolProfileRevisionContext(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.ContentJSON = `{"operation":"video.status"}`
	if err := SaveProtocolProfileRevisionContext(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := PublishProtocolProfileRevisionContext(ctx, "p1", 1); err != nil {
		t.Fatal(err)
	}
	r.ContentJSON = `{"operation":"forbidden"}`
	if err := SaveProtocolProfileRevisionContext(ctx, r); !errors.Is(err, ErrPublishedRevisionImmutable) {
		t.Fatalf("expected immutable error, got %v", err)
	}
	got, err := GetProtocolProfileRevisionContext(ctx, "p1", 1)
	if err != nil || got.State != "published" || got.ContentJSON != `{"operation":"video.status"}` {
		t.Fatalf("unexpected revision: %#v, %v", got, err)
	}
	profile, err := GetProtocolProfileContext(ctx, "p1")
	if err != nil || profile.LatestRevision != 1 {
		t.Fatalf("latest revision was not advanced: %#v, %v", profile, err)
	}
	if err := RetireProtocolProfileRevisionContext(ctx, "p1", 1); err != nil {
		t.Fatalf("retire revision: %v", err)
	}
	got, err = GetProtocolProfileRevisionContext(ctx, "p1", 1)
	if err != nil || got.State != ProfileRevisionRetired {
		t.Fatalf("unexpected retired revision: %#v, %v", got, err)
	}
	if err := RetireProtocolProfileRevisionContext(ctx, "p1", 1); err != nil {
		t.Fatalf("retire should be idempotent: %v", err)
	}
	got.ContentJSON = `{"operation":"cannot-change"}`
	if err := SaveProtocolProfileRevisionContext(ctx, got); !errors.Is(err, ErrRetiredRevisionImmutable) {
		t.Fatalf("expected retired revision immutability, got %v", err)
	}

	// Re-publishing a retired revision should succeed and transition state to published
	if err := PublishProtocolProfileRevisionContext(ctx, "p1", 1); err != nil {
		t.Fatalf("re-publish retired revision: %v", err)
	}
	got, err = GetProtocolProfileRevisionContext(ctx, "p1", 1)
	if err != nil || got.State != ProfileRevisionPublished {
		t.Fatalf("unexpected state after republish: %#v, %v", got, err)
	}
}

func TestProtocolProfileValidation(t *testing.T) {
	if err := InitDB(t.TempDir() + "/profile-validation.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "bad", Source: "remote"}); !errors.Is(err, ErrInvalidProfileSource) {
		t.Fatalf("invalid source error = %v", err)
	}
	if err := SaveProtocolProfileRevision(&ProtocolProfileRevision{ProfileID: "missing", Revision: 1}); err == nil {
		t.Fatal("missing profile should be rejected")
	}
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "p", Source: ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := SaveProtocolProfileRevision(&ProtocolProfileRevision{ProfileID: "p", Revision: 1, State: "invalid"}); !errors.Is(err, ErrInvalidRevisionState) {
		t.Fatalf("invalid revision state error = %v", err)
	}
}

func TestChannelProtocolBindingLookup(t *testing.T) {
	if err := InitDB(t.TempDir() + "/binding.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	ctx := context.Background()
	items := []*ChannelProtocolBinding{
		{ChannelID: "ch", Operation: "video.create", ModelPattern: "*", ProfileID: "default", ProfileRevision: 1, Precedence: 1, Enabled: true},
		{ChannelID: "ch", Operation: "video.create", ModelPattern: "model-x", ProfileID: "specific", ProfileRevision: 2, Precedence: 10, Enabled: true},
	}
	for _, item := range items {
		if err := SaveChannelProtocolBindingContext(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	got, err := FindChannelProtocolBindingContext(ctx, "ch", "video.create", "model-x")
	if err != nil || got.ProfileID != "specific" {
		t.Fatalf("specific binding not selected: %#v, %v", got, err)
	}
	got, err = FindChannelProtocolBindingContext(ctx, "ch", "video.create", "other")
	if err != nil || got.ProfileID != "default" {
		t.Fatalf("wildcard binding not selected: %#v, %v", got, err)
	}
	items[1].Enabled = false
	if err := SaveChannelProtocolBindingContext(ctx, items[1]); err != nil {
		t.Fatal(err)
	}
	got, err = FindChannelProtocolBindingContext(ctx, "ch", "video.create", "model-x")
	if err != nil || got.ProfileID != "default" {
		t.Fatalf("disabled binding should be skipped: %#v, %v", got, err)
	}
}

func TestChannelProtocolBindingRejectsOverlappingPatternsAtSamePrecedence(t *testing.T) {
	if err := InitDB(t.TempDir() + "/binding-conflicts.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	ctx := context.Background()
	base := &ChannelProtocolBinding{ChannelID: "ch", Operation: "chat.completions", ModelPattern: "model-*", ProfileID: "profile-a", ProfileRevision: 1, Precedence: 5, Enabled: true}
	if err := SaveChannelProtocolBindingContext(ctx, base); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []*ChannelProtocolBinding{
		{ChannelID: "ch", Operation: "chat.completions", ModelPattern: "model-fast", ProfileID: "profile-b", ProfileRevision: 1, Precedence: 5, Enabled: true},
		{ChannelID: "ch", Operation: "chat.completions", ModelPattern: "model-fast-*", ProfileID: "profile-b", ProfileRevision: 1, Precedence: 5, Enabled: true},
		{ChannelID: "ch", Operation: "chat.completions", ModelPattern: "*", ProfileID: "profile-b", ProfileRevision: 1, Precedence: 5, Enabled: true},
	} {
		if err := SaveChannelProtocolBindingContext(ctx, candidate); !errors.Is(err, ErrBindingConflict) {
			t.Fatalf("overlap %q error = %v, want ErrBindingConflict", candidate.ModelPattern, err)
		}
	}
	if err := SaveChannelProtocolBindingContext(ctx, &ChannelProtocolBinding{ChannelID: "ch", Operation: "chat.completions", ModelPattern: "model-fast", ProfileID: "profile-b", ProfileRevision: 1, Precedence: 6, Enabled: true}); err != nil {
		t.Fatalf("different precedence should be accepted: %v", err)
	}
}

func TestChannelProtocolBindingUpdateExcludesItselfFromConflictCheck(t *testing.T) {
	if err := InitDB(t.TempDir() + "/binding-update.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	ctx := context.Background()
	first := &ChannelProtocolBinding{ChannelID: "ch", Operation: "chat.completions", ModelPattern: "model-a*", ProfileID: "profile-a", ProfileRevision: 1, Precedence: 5, Enabled: true}
	second := &ChannelProtocolBinding{ChannelID: "ch", Operation: "chat.completions", ModelPattern: "model-b*", ProfileID: "profile-b", ProfileRevision: 1, Precedence: 5, Enabled: true}
	if err := SaveChannelProtocolBindingContext(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := SaveChannelProtocolBindingContext(ctx, second); err != nil {
		t.Fatal(err)
	}
	first.ProfileRevision = 2
	if err := SaveChannelProtocolBindingContext(ctx, first); err != nil {
		t.Fatalf("updating an unchanged pattern should not conflict with itself: %v", err)
	}
}

func TestChannelProtocolBindingCreatePreservesDisabledState(t *testing.T) {
	if err := InitDB(t.TempDir() + "/binding-disabled.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	binding := &ChannelProtocolBinding{ChannelID: "ch", Operation: "chat.completions", ModelPattern: "*", ProfileID: "profile", ProfileRevision: 1, Enabled: false}
	if err := SaveChannelProtocolBinding(binding); err != nil {
		t.Fatal(err)
	}
	var stored ChannelProtocolBinding
	if err := DB.First(&stored, binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Enabled {
		t.Fatalf("disabled binding was persisted as enabled: %+v", stored)
	}
}

func TestRetireProfileRevisionRejectsEnabledBindings(t *testing.T) {
	if err := InitDB(t.TempDir() + "/revision-retire-binding.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "retire-profile", Name: "Retire", Source: ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := SaveProtocolProfileRevision(&ProtocolProfileRevision{ProfileID: "retire-profile", Revision: 1, SchemaVersion: 1, ContentJSON: `{}`, ContentDigest: "digest", State: ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	binding := &ChannelProtocolBinding{ChannelID: "retire-channel", Operation: "chat.completions", ModelPattern: "*", ProfileID: "retire-profile", ProfileRevision: 1, Enabled: true}
	if err := SaveChannelProtocolBinding(binding); err != nil {
		t.Fatal(err)
	}
	if err := RetireProtocolProfileRevision("retire-profile", 1); !errors.Is(err, ErrRevisionHasActiveBindings) {
		t.Fatalf("retire error = %v, want ErrRevisionHasActiveBindings", err)
	}
	stored, err := GetProtocolProfileRevision("retire-profile", 1)
	if err != nil || stored.State != ProfileRevisionPublished {
		t.Fatalf("blocked retire changed revision: %#v, %v", stored, err)
	}
	binding.Enabled = false
	if err := SaveChannelProtocolBinding(binding); err != nil {
		t.Fatal(err)
	}
	if err := RetireProtocolProfileRevision("retire-profile", 1); err != nil {
		t.Fatalf("retire after disabling binding: %v", err)
	}
}

func TestCreateProtocolProfileWithInitialRevisionIsAtomic(t *testing.T) {
	if err := InitDB(t.TempDir() + "/profile-initial-revision.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	profile := &ProtocolProfile{ID: "atomic-profile", Name: "Atomic", Source: ProfileSourceCustom}
	revision := &ProtocolProfileRevision{ProfileID: profile.ID, Revision: 1, State: "invalid"}
	if err := CreateProtocolProfileWithInitialRevisionContext(context.Background(), profile, revision); !errors.Is(err, ErrInvalidRevisionState) {
		t.Fatalf("invalid initial revision error = %v, want ErrInvalidRevisionState", err)
	}
	if _, err := GetProtocolProfile("atomic-profile"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("failed atomic create left a profile behind: %v", err)
	}
	revision.State = ProfileRevisionDraft
	if err := CreateProtocolProfileWithInitialRevisionContext(context.Background(), profile, revision); err != nil {
		t.Fatalf("create profile with initial revision: %v", err)
	}
	stored, err := GetProtocolProfileRevision(profile.ID, 1)
	if err != nil || stored.State != ProfileRevisionDraft {
		t.Fatalf("initial revision not persisted: %#v, %v", stored, err)
	}
}
