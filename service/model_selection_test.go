package service

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"relay-gateway/adapter"
	"relay-gateway/config"
	"relay-gateway/db"
)

func TestSelectedModelsCannotBeBypassedByDiscoveryOrFallback(t *testing.T) {
	dispatcher := &Dispatcher{remoteModels: map[string][]string{
		"restricted": {"unselected", "remote-only"},
		"empty":      {"unselected"},
	}}
	channels := []config.UpstreamChannel{
		{
			ID: "restricted", SelectedModels: []string{"selected"},
			Models:   []string{"*", "unselected", "model-*"},
			ModelMap: map[string]string{"blocked-alias": "unselected", "selected-alias": "selected"},
		},
		{ID: "empty", SelectedModels: []string{}, Models: []string{"*", "unselected"}},
	}
	for _, requested := range []string{"unselected", "remote-only", "model-unknown", "unknown", "blocked-alias"} {
		if candidates, err := dispatcher.orderCandidatePool(channels, requested, "chat"); !errors.Is(err, ErrModelNotSelected) || len(candidates) != 0 {
			t.Errorf("unselected model %q escaped restriction: candidates=%v err=%v", requested, candidates, err)
		}
	}
	for _, requested := range []string{"selected", "SELECTED", "selected-alias"} {
		candidates, err := dispatcher.orderCandidatePool(channels, requested, "chat")
		if err != nil || len(candidates) != 1 || candidates[0].ID != "restricted" {
			t.Errorf("selection must explicitly match without discovery: model=%q candidates=%v err=%v", requested, candidates, err)
		}
	}
}

func TestSelectedModelsRemainIsolatedAcrossChannels(t *testing.T) {
	dispatcher := &Dispatcher{}
	channels := []config.UpstreamChannel{
		{ID: "a", SelectedModels: []string{"model-a"}, Models: []string{"*"}},
		{ID: "b", SelectedModels: []string{"model-b"}, Models: []string{"*"}},
		{ID: "c", SelectedModels: []string{"model-a"}, Priority: 2},
	}
	candidates, err := dispatcher.orderCandidatePool(channels, "model-a", "chat")
	if err != nil || len(candidates) != 2 || candidates[0].ID != "a" || candidates[1].ID != "c" {
		t.Fatalf("model-a candidates should include only saved channels: candidates=%v err=%v", candidates, err)
	}
	channels = append(channels, config.UpstreamChannel{ID: "legacy", Models: []string{"*"}})
	candidates, err = dispatcher.orderCandidatePool(channels, "unknown", "chat")
	if err != nil || len(candidates) != 1 || candidates[0].ID != "legacy" {
		t.Fatalf("legacy fallback should not reintroduce restricted channels: candidates=%v err=%v", candidates, err)
	}
}

func TestListModelsOnlyIncludesSavedModelsAndTheirAliases(t *testing.T) {
	previousConfig := config.Global
	config.Global = &config.Config{Channels: []config.UpstreamChannel{
		{
			ID: "restricted", SelectedModels: []string{"model-a"},
			Models:   []string{"manual-hidden", "*"},
			ModelMap: map[string]string{"public-a": "model-a", "public-hidden": "remote-hidden"},
		},
		{ID: "empty", SelectedModels: []string{}, Models: []string{"empty-hidden"}},
		{ID: "legacy", Models: []string{"legacy-model"}},
	}}
	t.Cleanup(func() { config.Global = previousConfig })
	dispatcher := &Dispatcher{remoteModels: map[string][]string{
		"restricted": {"model-a", "remote-hidden"},
		"empty":      {"empty-remote-hidden"},
		"legacy":     {"legacy-remote"},
	}}
	var got []string
	for _, item := range dispatcher.ListModels().Data {
		got = append(got, item.ID)
	}
	want := []string{"legacy-model", "legacy-remote", "model-a", "public-a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("exposed models = %v, want %v", got, want)
	}
}

func TestFailoverCannotAttemptChannelWithoutSelectedModel(t *testing.T) {
	previousConfig := config.Global
	config.Global = &config.Config{Channels: []config.UpstreamChannel{
		{ID: "selected", Type: "openai", SelectedModels: []string{"model-a"}, Priority: 1},
		{ID: "unselected", Type: "openai", SelectedModels: []string{"model-b"}, Models: []string{"*"}, Priority: 2},
	}}
	t.Cleanup(func() { config.Global = previousConfig })
	dispatcher := &Dispatcher{}
	var attempted []string
	err := dispatcher.ExecuteWithPolicy(context.Background(), "model-a", "chat", RetryInference, nil, func(ch *config.UpstreamChannel, _ adapter.Adapter) error {
		attempted = append(attempted, ch.ID)
		return &adapter.UpstreamHTTPError{StatusCode: http.StatusNotFound, Body: "unsupported model"}
	})
	if err == nil || !reflect.DeepEqual(attempted, []string{"selected"}) {
		t.Fatalf("failover escaped model selection: attempted=%v err=%v", attempted, err)
	}
}

func TestProfileBindingsCannotBypassSavedModels(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/selected-profile.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "selected-image", Name: "Selected Image", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "selected-image", Revision: 1, State: db.ProfileRevisionPublished, ContentJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	channel := &db.ChannelModel{
		ID: "selected-profile", Name: "Selected Profile", Type: "openai", BaseURL: "https://upstream.example/v1",
		Enabled: true, ModelsRaw: "*", SelectedModelsRaw: `["image-a"]`,
		ModelMapRaw: `{"public-image":"image-a","blocked-image":"image-b"}`,
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "images.create", ModelPattern: "*", ProfileID: "selected-image", ProfileRevision: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	dispatcher := &Dispatcher{remoteModels: map[string][]string{channel.ID: {"image-a", "image-b"}}}
	for _, requested := range []string{"image-a", "public-image"} {
		candidates, err := dispatcher.ResolveProfileCandidates(requested, "images.create")
		if err != nil || len(candidates) != 1 || candidates[0].ID != channel.ID {
			t.Errorf("selected profile model %q failed: candidates=%v err=%v", requested, candidates, err)
		}
	}
	for _, requested := range []string{"image-b", "blocked-image", "unknown"} {
		if candidates, err := dispatcher.ResolveProfileCandidates(requested, "images.create"); !errors.Is(err, ErrModelNotSelected) || len(candidates) != 0 {
			t.Errorf("wildcard profile binding bypassed selection for %q: candidates=%v err=%v", requested, candidates, err)
		}
	}
	channel.SelectedModelsRaw = `[]`
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if candidates, err := dispatcher.ResolveProfileCandidates("image-a", "images.create"); !errors.Is(err, ErrModelNotSelected) || len(candidates) != 0 {
		t.Fatalf("cleared selection should disable bound profile model: candidates=%v err=%v", candidates, err)
	}
}
