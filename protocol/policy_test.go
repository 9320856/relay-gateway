package protocol

import "testing"

func TestApplyModelPolicyWithoutPolicyReturnsOriginalBody(t *testing.T) {
	body := map[string]any{"prompt": "a cat"}
	got, err := ApplyModelPolicy("images.create", "", body)
	if err != nil {
		t.Fatal(err)
	}
	gotBody, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("body type = %T, want map[string]any", got)
	}
	gotBody["status"] = "queued"
	if body["status"] != "queued" {
		t.Fatalf("body = %#v, want original map", body)
	}
}

func TestResolveModelPolicyUnknown(t *testing.T) {
	if _, err := ResolveModelPolicy("missing-policy"); err == nil {
		t.Fatal("unknown policy resolved without error")
	}
}

func TestCompileRejectsUnknownModelPolicy(t *testing.T) {
	profile := Profile{SchemaVersion: CurrentSchemaVersion, Operations: []Operation{{
		Operation: "images.create", ExecutionMode: ExecutionDirect, PollingMode: PollingOff,
		Policy: "missing-policy", Submit: Submit{Method: "POST", Path: "/images", BodyEncoding: "json"},
	}}}
	if _, err := Compile(profile); err == nil {
		t.Fatal("Compile accepted unknown model policy")
	}
}
