package model

import (
	"encoding/json"
	"testing"
)

func TestImageRequestAliasesDoNotLeakIntoForwardedPayload(t *testing.T) {
	var req ImageGenerationRequest
	if err := json.Unmarshal([]byte(`{"model":"gpt-image-2","prompt":"test","ratio":"16:9","ar":"4:3","imageSize":"1024x1024","vendor_option":true}`), &req); err != nil {
		t.Fatal(err)
	}
	if req.AspectRatio != "16:9" || req.Size != "1024x1024" {
		t.Fatalf("aliases were not normalized: %+v", req)
	}
	payload := req.ToMap()
	for _, key := range []string{"ratio", "ar", "imageSize"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("normalized alias %q leaked into forwarded payload: %#v", key, payload)
		}
	}
	if payload["vendor_option"] != true {
		t.Fatalf("unknown provider option was unexpectedly removed: %#v", payload)
	}
}
