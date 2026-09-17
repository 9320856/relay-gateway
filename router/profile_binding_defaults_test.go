package router

import (
	"context"
	"testing"

	"relay-gateway/db"
	"relay-gateway/profilebootstrap"
	"relay-gateway/protocol"
)

func bootstrapProfilesForRouterTest(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := profilebootstrap.EnsureBuiltinProfiles(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureDefaultProfileBindingsForNewChannels(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/default-bindings.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bootstrapProfilesForRouterTest(t)
	for _, channelType := range []string{protocol.PresetOpenAI, protocol.PresetAnthropic, protocol.PresetNewAPI, protocol.PresetSub2API} {
		channelID := "new-" + channelType
		if err := db.SaveChannelModel(&db.ChannelModel{ID: channelID, Name: channelID, Type: channelType, BaseURL: "https://example.test/v1", Enabled: true}); err != nil {
			t.Fatal(err)
		}
		if err := ensureDefaultProfileBindings(context.Background(), channelType, channelID); err != nil {
			t.Fatalf("ensure %s defaults: %v", channelType, err)
		}
		profile, err := protocol.BuiltinPreset(channelType)
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range profile.Operations {
			binding, err := db.FindChannelProtocolBinding(channelID, op.Operation, "any-model")
			if err != nil || binding.ProfileID != profilebootstrap.BuiltinProfileID(channelType) || binding.ProfileRevision != 1 || binding.Precedence != -100 {
				t.Fatalf("%s %s binding = %#v, err=%v", channelType, op.Operation, binding, err)
			}
		}
		if err := ensureDefaultProfileBindings(context.Background(), channelType, channelID); err != nil {
			t.Fatalf("repeating %s defaults should be idempotent: %v", channelType, err)
		}
	}
}

func TestSaveChannelSameTypeDoesNotRewriteBindings(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/same-channel-type.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	bootstrapProfilesForRouterTest(t)

	channel := &db.ChannelModel{ID: "same-channel", Name: "Same", Type: protocol.PresetOpenAI, BaseURL: "https://openai.example/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := ensureDefaultProfileBindings(context.Background(), channel.Type, channel.ID); err != nil {
		t.Fatal(err)
	}
	var before []db.ChannelProtocolBinding
	if err := db.DB.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&before).Error; err != nil {
		t.Fatal(err)
	}

	updated := *channel
	updated.Name = "Renamed"
	if err := saveChannelWithProfileBindings(context.Background(), &updated, protocol.PresetOpenAI, false); err != nil {
		t.Fatalf("save same channel type: %v", err)
	}
	var after []db.ChannelProtocolBinding
	if err := db.DB.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("binding count changed from %d to %d", len(before), len(after))
	}
	for index := range before {
		if after[index].ID != before[index].ID || after[index].ProfileID != before[index].ProfileID || after[index].ProfileRevision != before[index].ProfileRevision {
			t.Fatalf("binding %d was rewritten: before=%#v after=%#v", index, before[index], after[index])
		}
	}
}
