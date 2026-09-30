package config

import "testing"

func TestAllowsModelUsesSavedSelectionAndMappedTarget(t *testing.T) {
	channel := UpstreamChannel{
		SelectedModels: []string{"provider/GPT-4", "claude-3"},
		ModelMap: map[string]string{
			"public-gpt": "provider/GPT-4",
			"hidden-gpt": "provider/GPT-5",
			"claude-3":   "unselected-target",
		},
	}
	for _, model := range []string{"provider/GPT-4", "provider/gpt-4", " public-gpt "} {
		if !channel.AllowsModel(model) {
			t.Errorf("selected model or alias %q was rejected", model)
		}
	}
	for _, model := range []string{"provider/GPT-5", "hidden-gpt", "PUBLIC-GPT", "claude-3", "unknown", ""} {
		if channel.AllowsModel(model) {
			t.Errorf("unselected upstream target for %q was allowed", model)
		}
	}
}

func TestAllowsModelDistinguishesLegacyAndEmptySelection(t *testing.T) {
	legacy := UpstreamChannel{Models: []string{"*"}}
	if !legacy.AllowsModel("unknown") {
		t.Fatal("legacy channels should preserve existing routing")
	}
	legacy.SelectedModels = []string{}
	if legacy.AllowsModel("unknown") {
		t.Fatal("explicitly clearing the selection must disable model calls")
	}
	legacy.SelectedModels = []string{"*", "gpt-*", "model?"}
	for _, model := range []string{"gpt-4", "model1", "*"} {
		if legacy.AllowsModel(model) {
			t.Errorf("selection patterns must not authorize model %q", model)
		}
	}
}
