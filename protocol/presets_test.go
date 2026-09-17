package protocol

import "testing"

func TestBuiltinPresetsCompileAndAreIndependent(t *testing.T) {
	for _, name := range BuiltinPresetNames() {
		profile, err := BuiltinPreset(name)
		if err != nil {
			t.Fatal(err)
		}
		compiled, err := Compile(profile)
		if err != nil {
			t.Fatalf("preset %s does not compile: %v", name, err)
		}
		if compiled.Digest() == "" || len(compiled.CanonicalJSON()) == 0 {
			t.Fatalf("preset %s has no canonical digest", name)
		}
	}
	first, err := BuiltinPreset(PresetOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	first.Operations[0].Submit.Path = "/mutated"
	second, err := BuiltinPreset(PresetOpenAI)
	if err != nil || second.Operations[0].Submit.Path == "/mutated" {
		t.Fatal("builtin preset returned shared mutable state")
	}
}

func TestBuiltinPresetCoverageMatchesCurrentChannelContracts(t *testing.T) {
	newapi, _ := BuiltinPreset(PresetNewAPI)
	if !hasOperation(newapi, "video.create") {
		t.Fatal("newapi preset missing video operation")
	}
	openai, _ := BuiltinPreset(PresetOpenAI)
	if !hasOperation(openai, "audio.speech") {
		t.Fatal("openai preset missing audio operation")
	}
	sub2api, _ := BuiltinPreset(PresetSub2API)
	if hasOperation(sub2api, "video.create") || hasOperation(sub2api, "images.create") {
		t.Fatal("sub2api preset advertises unsupported media operations")
	}
}

func hasOperation(profile Profile, name string) bool {
	for _, op := range profile.Operations {
		if op.Operation == name {
			return true
		}
	}
	return false
}
