package httpforward

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCopyTransformedHeadersDiscardsLengthWithoutChangingPassthrough(t *testing.T) {
	src := http.Header{
		"Content-Length":     {"123"},
		"X-Request-Id":       {"upstream-request"},
		"X-RateLimit":        {"requests=100", "tokens=10000"},
		"Connection":         {"X-Upstream-Private"},
		"X-Upstream-Private": {"internal"},
	}
	transformed := httptest.NewRecorder()
	transformed.Header().Set("Content-Length", "456")
	CopyTransformedHeaders(transformed, src)
	if got := transformed.Header().Get("Content-Length"); got != "" {
		t.Fatalf("converted response retained upstream length: %q", got)
	}
	if got := transformed.Header().Get("X-Request-ID"); got != "upstream-request" {
		t.Fatalf("request metadata=%q", got)
	}
	if got := transformed.Header().Values("X-RateLimit"); len(got) != 2 || got[0] != "requests=100" || got[1] != "tokens=10000" {
		t.Fatalf("rate limit metadata=%v", got)
	}
	for _, name := range []string{"Connection", "X-Upstream-Private"} {
		if got := transformed.Header().Get(name); got != "" {
			t.Fatalf("connection-specific header %s=%q", name, got)
		}
	}
	passthrough := httptest.NewRecorder()
	CopyHeaders(passthrough, src)
	if got := passthrough.Header().Get("Content-Length"); got != "123" {
		t.Fatalf("unchanged passthrough lost upstream length: %q", got)
	}
	if got := src.Get("Content-Length"); got != "123" {
		t.Fatalf("source response headers mutated: %q", got)
	}
}
