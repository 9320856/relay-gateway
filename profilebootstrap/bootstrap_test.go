package profilebootstrap

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/protocol"
)

func TestEnsureBuiltinProfilesMaterializesPublishedRevisionsIdempotently(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	first, err := EnsureBuiltinProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Created) != len(protocol.BuiltinPresetNames()) || len(first.Existing) != 0 {
		t.Fatalf("first bootstrap result = %+v", first)
	}
	second, err := EnsureBuiltinProfiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Created) != 0 || len(second.Existing) != len(protocol.BuiltinPresetNames()) {
		t.Fatalf("second bootstrap result = %+v", second)
	}
	var bindingCount int64
	if err := db.DB.Model(&db.ChannelProtocolBinding{}).Count(&bindingCount).Error; err != nil {
		t.Fatal(err)
	}
	if bindingCount != 0 {
		t.Fatalf("builtin bootstrap created %d channel bindings; explicit migration is required", bindingCount)
	}

	for _, name := range protocol.BuiltinPresetNames() {
		profile, err := db.GetProtocolProfile(BuiltinProfileID(name))
		if err != nil || profile.Source != db.ProfileSourceBuiltin || profile.LatestRevision != 1 {
			t.Fatalf("builtin profile %q = %#v, err=%v", name, profile, err)
		}
		revision, err := db.GetProtocolProfileRevision(BuiltinProfileID(name), 1)
		if err != nil || revision.State != db.ProfileRevisionPublished || revision.ContentDigest == "" {
			t.Fatalf("builtin profile %q revision = %#v, err=%v", name, revision, err)
		}
	}
}

func TestEnsureBuiltinProfilesRejectsCustomIDCollision(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/collision.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: BuiltinProfileID(protocol.PresetOpenAI), Name: "operator-owned", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	_, err := EnsureBuiltinProfiles(context.Background())
	if err == nil || errors.Is(err, gorm.ErrRecordNotFound) || !strings.Contains(err.Error(), "already owned") {
		t.Fatalf("expected collision error, got %v", err)
	}
}

func TestEnsureBuiltinProfilesPreservesExistingRevisionsAfterRestart(t *testing.T) {
	path := t.TempDir() + "/existing.db"
	if err := db.InitDB(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	profileID := BuiltinProfileID(protocol.PresetOpenAI)
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: profileID, Name: "Existing OpenAI", Source: db.ProfileSourceBuiltin}); err != nil {
		t.Fatal(err)
	}
	snapshots := make([]*db.ProtocolProfileRevision, 0, 3)
	for revision := 1; revision <= 3; revision++ {
		content, err := protocol.BuiltinPreset(protocol.PresetOpenAI)
		if err != nil {
			t.Fatal(err)
		}
		content.Operations[0].Submit.Path = fmt.Sprintf("/revision-%d/chat/completions", revision)
		compiled, err := protocol.Compile(content)
		if err != nil {
			t.Fatal(err)
		}
		rev := &db.ProtocolProfileRevision{ProfileID: profileID, Revision: revision, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionDraft}
		if err := db.SaveProtocolProfileRevision(rev); err != nil {
			t.Fatal(err)
		}
		if revision <= 2 {
			if err := db.PublishProtocolProfileRevision(profileID, revision); err != nil {
				t.Fatal(err)
			}
		}
		if revision == 1 {
			if err := db.RetireProtocolProfileRevision(profileID, revision); err != nil {
				t.Fatal(err)
			}
		}
		snapshot, err := db.GetProtocolProfileRevision(profileID, revision)
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
	}
	profileBefore, err := db.GetProtocolProfile(profileID)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitDB(path); err != nil {
		t.Fatal(err)
	}
	for startup := 0; startup < 2; startup++ {
		if _, err := EnsureBuiltinProfiles(context.Background()); err != nil {
			t.Fatal(err)
		}
		profileAfter, err := db.GetProtocolProfile(profileID)
		if err != nil || !reflect.DeepEqual(profileBefore, profileAfter) {
			t.Fatalf("bootstrap changed existing profile: before=%+v after=%+v err=%v", profileBefore, profileAfter, err)
		}
		for _, snapshot := range snapshots {
			got, err := db.GetProtocolProfileRevision(profileID, snapshot.Revision)
			if err != nil || !reflect.DeepEqual(snapshot, got) {
				t.Fatalf("revision %d was changed or deleted: before=%+v after=%+v err=%v", snapshot.Revision, snapshot, got, err)
			}
		}
	}
}
