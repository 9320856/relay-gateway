package httpforward

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCopyHeadersFiltersUpstreamPolicyAndConnectionTokens(t *testing.T) {
	dst := httptest.NewRecorder()
	dst.Header().Set("Access-Control-Allow-Origin", "https://gateway.example")
	src := http.Header{"Access-Control-Allow-Origin": {"*"}, "Set-Cookie": {"secret=x"}, "Connection": {"X-Private, keep-alive"}, "X-Private": {"secret"}, "Content-Type": {"text/event-stream"}}
	CopyHeaders(dst, src)
	if got := dst.Header().Values("Access-Control-Allow-Origin"); len(got) != 1 || got[0] != "https://gateway.example" {
		t.Fatalf("CORS = %v", got)
	}
	for _, name := range []string{"Set-Cookie", "Connection", "X-Private"} {
		if dst.Header().Get(name) != "" {
			t.Fatalf("leaked %s", name)
		}
	}
	if dst.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatal("missing content type")
	}
}

func TestStreamFlushesAndHonorsCancellation(t *testing.T) {
	recorder := httptest.NewRecorder()
	n, err := Stream(context.Background(), strings.NewReader("event"), recorder)
	if err != nil || n != 5 || !recorder.Flushed {
		t.Fatalf("stream n=%d err=%v flushed=%v", n, err, recorder.Flushed)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder = httptest.NewRecorder()
	if _, err = Stream(ctx, strings.NewReader("event"), recorder); err != context.Canceled || recorder.Body.Len() != 0 {
		t.Fatalf("cancel err=%v body=%q", err, recorder.Body.String())
	}
}
