package service

import (
	"os"
	"testing"

	"relay-gateway/db"
)

func init() {
	if os.Getenv("RELAY_DB_ENCRYPTION_KEY") == "" {
		_ = os.Setenv("RELAY_DB_ENCRYPTION_KEY", "default-test-encryption-key-for-unit-tests-entropy")
	}
}

// Profile routing selects by an enabled binding and channel capability.
func TestResolveProfileCandidatesUsesBinding(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profile-dispatcher.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	channel := &db.ChannelModel{
		ID: "custom-profile-channel", Name: "Custom Profile", Type: "openai",
		BaseURL: "https://openai.example/v1", APIKey: "test-key", Enabled: true,
		Priority: 1, Weight: 1, ModelsRaw: "grok-imagine",
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "custom-image", Name: "Custom Image", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "custom-image", Revision: 1, State: db.ProfileRevisionPublished, ContentJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "images.create", ModelPattern: "grok-imagine",
		ProfileID: "custom-image", ProfileRevision: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	// The resolver only needs the persisted channel cache and binding.
	candidates, err := DefaultDispatcher.ResolveProfileCandidates("grok-imagine", "images.create")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].ID != channel.ID {
		t.Fatalf("unexpected profile candidates: %+v", candidates)
	}
}
