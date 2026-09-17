package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func validAsyncProfile() Profile {
	return Profile{SchemaVersion: 1, Name: "video", Operations: []Operation{{
		Operation: "video.create", ExecutionMode: ExecutionAsync, PollingMode: PollingClient,
		Submit:   Submit{Method: "POST", Path: "/videos/generations", BodyEncoding: "json"},
		Response: Response{TaskIDPaths: []string{"id", "task_id"}},
		Poll:     &Poll{Method: "GET", Path: "/videos/generations/{task_id}", IntervalMS: 3000, MaxAttempts: 200, MaxDurationMS: 600000, StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed", "cancelled"}, ResultURLPaths: []string{"video_url", "url"}},
		Content:  &Content{Method: "GET", Path: "/videos/generations/{task_id}/content"},
	}}}
}

func TestValidAsyncVideoProfileCompiles(t *testing.T) {
	c, err := Compile(validAsyncProfile())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(c.CanonicalJSON()) == 0 || len(c.Digest()) != 64 {
		t.Fatalf("compiled profile missing canonical data or digest")
	}
	var decoded Profile
	if err := json.Unmarshal(c.CanonicalJSON(), &decoded); err != nil {
		t.Fatalf("canonical JSON invalid: %v", err)
	}
	if decoded.Operations[0].Operation != "video.create" {
		t.Fatalf("operation did not round-trip")
	}
}

func TestDirectOffProfile(t *testing.T) {
	p := Profile{Operations: []Operation{{Operation: "chat", ExecutionMode: ExecutionDirect, PollingMode: PollingOff, Submit: Submit{Method: "POST", Path: "/chat/completions"}, Response: Response{ResultPaths: []string{"choices.0.message"}}}}}
	if _, err := Compile(p); err != nil {
		t.Fatalf("direct/off profile rejected: %v", err)
	}
}

func TestAsyncOffMayOmitPollingDefinition(t *testing.T) {
	p := Profile{SchemaVersion: 1, Operations: []Operation{{
		Operation: "video.create", ExecutionMode: ExecutionAsync, PollingMode: PollingOff,
		Submit: Submit{Method: "POST", Path: "/videos"}, Response: Response{TaskIDPaths: []string{"id"}},
	}}}
	if _, err := Compile(p); err != nil {
		t.Fatalf("async off profile should compile without poll: %v", err)
	}
}

func TestAsyncClientRequiresPollingDefinition(t *testing.T) {
	p := Profile{SchemaVersion: 1, Operations: []Operation{{
		Operation: "video.create", ExecutionMode: ExecutionAsync, PollingMode: PollingClient,
		Submit: Submit{Method: "POST", Path: "/videos"}, Response: Response{TaskIDPaths: []string{"id"}},
	}}}
	if _, err := Compile(p); err == nil {
		t.Fatal("async client profile without poll should be rejected")
	}
}

func TestInvalidEnumsAndMissingPoll(t *testing.T) {
	p := validAsyncProfile()
	p.Operations[0].ExecutionMode = "streaming"
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "execution_mode") {
		t.Fatalf("expected execution enum error, got %v", err)
	}
	p = validAsyncProfile()
	p.Operations[0].Poll = nil
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "poll is required") {
		t.Fatalf("expected missing poll error, got %v", err)
	}
	p = validAsyncProfile()
	p.Operations[0].PollingMode = "manual"
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "polling_mode") {
		t.Fatalf("expected polling enum error, got %v", err)
	}
	p = validAsyncProfile()
	p.Operations[0].MediaRetention = "sometimes"
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "media_retention") {
		t.Fatalf("expected media retention enum error, got %v", err)
	}
}

func TestMediaRetentionPoliciesAndLegacyDefault(t *testing.T) {
	for _, policy := range []string{MediaRetentionRequired, MediaRetentionBestEffort, MediaRetentionDisabled} {
		p := validAsyncProfile()
		p.Operations[0].MediaRetention = policy
		if err := p.Validate(); err != nil {
			t.Fatalf("policy %q rejected: %v", policy, err)
		}
		if got := p.Operations[0].EffectiveMediaRetention(); got != policy {
			t.Fatalf("policy %q effective value = %q", policy, got)
		}
	}
	legacy := validAsyncProfile()
	if got := legacy.Operations[0].EffectiveMediaRetention(); got != MediaRetentionDisabled {
		t.Fatalf("legacy empty policy effective value = %q, want %q", got, MediaRetentionDisabled)
	}
	data, err := legacy.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "media_retention") {
		t.Fatalf("legacy canonical JSON unexpectedly added media_retention: %s", data)
	}
}

func TestMediaRetentionChangesDigest(t *testing.T) {
	base := validAsyncProfile()
	baseDigest, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	base.Operations[0].MediaRetention = MediaRetentionRequired
	changedDigest, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if baseDigest == changedDigest {
		t.Fatal("media retention policy did not affect profile digest")
	}
}

func TestDangerousPathAndHeadersRejected(t *testing.T) {
	p := validAsyncProfile()
	p.Operations[0].Submit.Path = "https://evil.example/x"
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "unsafe path") {
		t.Fatalf("expected unsafe path error, got %v", err)
	}
	p = validAsyncProfile()
	p.Operations[0].Submit.Headers = map[string]string{"Authorization": "x"}
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected dangerous header error, got %v", err)
	}
	p = validAsyncProfile()
	p.Operations[0].Poll.Path = "/videos/../admin"
	if err := p.Validate(); err == nil {
		t.Fatalf("expected traversal path error")
	}
}

func TestDigestStableAcrossMapInsertionOrder(t *testing.T) {
	a := validAsyncProfile()
	a.Operations[0].Submit.Headers = map[string]string{"X-Z": "2", "X-A": "1"}
	b := validAsyncProfile()
	b.Operations[0].Submit.Headers = map[string]string{"X-A": "1", "X-Z": "2"}
	ca, err := Compile(a)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := Compile(b)
	if err != nil {
		t.Fatal(err)
	}
	if ca.Digest() != cb.Digest() || !bytes.Equal(ca.CanonicalJSON(), cb.CanonicalJSON()) {
		t.Fatalf("digest/canonical JSON is not stable")
	}
}

func TestCompileDeepCopiesProfile(t *testing.T) {
	p := validAsyncProfile()
	p.Operations[0].Submit.Body = map[string]any{"nested": map[string]any{"value": "before"}}
	c, err := Compile(p)
	if err != nil {
		t.Fatal(err)
	}
	p.Operations[0].Submit.Headers = map[string]string{"X-Mutated": "yes"}
	p.Operations[0].Submit.Body["nested"].(map[string]any)["value"] = "after"
	got := c.Profile()
	if _, ok := got.Operations[0].Submit.Headers["X-Mutated"]; ok {
		t.Fatal("compiled profile aliases source headers")
	}
	if got.Operations[0].Submit.Body["nested"].(map[string]any)["value"] != "before" {
		t.Fatal("compiled profile aliases nested body")
	}
	got.Operations[0].Response.TaskIDPaths[0] = "changed"
	if c.Profile().Operations[0].Response.TaskIDPaths[0] == "changed" {
		t.Fatal("Profile() exposes internal slices")
	}
}

func TestProfileValidationRejectsUnsupportedVersionAndSelectors(t *testing.T) {
	p := validAsyncProfile()
	p.SchemaVersion = CurrentSchemaVersion + 1
	if err := p.Validate(); err == nil {
		t.Fatal("unsupported schema version accepted")
	}
	p = validAsyncProfile()
	p.Operations[0].Response.TaskIDPaths = []string{"payload..id"}
	if err := p.Validate(); err == nil {
		t.Fatal("malformed selector accepted")
	}
	p = validAsyncProfile()
	p.Operations[0].Submit.Path = "/v1/items#fragment"
	if err := p.Validate(); err == nil {
		t.Fatal("fragment path accepted")
	}
}

func TestPollValidationBackoffFields(t *testing.T) {
	base := validAsyncProfile()
	tests := []struct {
		name  string
		apply func(*Poll)
	}{
		{name: "mode", apply: func(p *Poll) { p.BackoffMode = "quadratic" }},
		{name: "negative base", apply: func(p *Poll) { p.BackoffBaseMS = -1 }},
		{name: "base too large", apply: func(p *Poll) { p.BackoffBaseMS = 86400001 }},
		{name: "negative max", apply: func(p *Poll) { p.BackoffMaxMS = -1 }},
		{name: "max too large", apply: func(p *Poll) { p.BackoffMaxMS = 604800001 }},
		{name: "negative jitter", apply: func(p *Poll) { p.JitterMS = -1 }},
		{name: "jitter too large", apply: func(p *Poll) { p.JitterMS = 86400001 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := base
			p.Operations[0].Poll = clonePoll(base.Operations[0].Poll)
			tt.apply(p.Operations[0].Poll)
			if err := p.Validate(); err == nil {
				t.Fatalf("invalid backoff configuration accepted")
			}
		})
	}
}

func clonePoll(p *Poll) *Poll {
	if p == nil {
		return nil
	}
	copy := *p
	copy.Headers = cloneStringMap(p.Headers)
	copy.SuccessValues = append([]string(nil), p.SuccessValues...)
	copy.FailureValues = append([]string(nil), p.FailureValues...)
	copy.ResultURLPaths = append([]string(nil), p.ResultURLPaths...)
	return &copy
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}
