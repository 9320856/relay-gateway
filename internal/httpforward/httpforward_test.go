package httpforward

import (
	"context"
	"errors"
	"io"
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

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestCopyPreservesReadAndWriteErrorsWithoutFlushing(t *testing.T) {
	upstreamErr, downstreamErr := errors.New("upstream failed"), errors.New("downstream failed")
	for _, tc := range []struct {
		name string
		src  io.Reader
		dst  io.Writer
		want error
	}{
		{"read", failingReader{upstreamErr}, io.Discard, upstreamErr},
		{"write", strings.NewReader("response"), failingWriter{downstreamErr}, downstreamErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Copy(context.Background(), tc.src, tc.dst)
			var transfer *StreamError
			if !errors.As(err, &transfer) || transfer.Op != tc.name || !errors.Is(err, tc.want) {
				t.Fatalf("copy error=%v, want %s wrapping %v", err, tc.name, tc.want)
			}
		})
	}
	writer := httptest.NewRecorder()
	n, err := Copy(context.Background(), strings.NewReader("response"), writer)
	if err != nil || n != 8 || writer.Flushed || writer.Body.String() != "response" {
		t.Fatalf("copy n=%d err=%v flushed=%v body=%q", n, err, writer.Flushed, writer.Body.String())
	}
}

func TestCopyHeadersFiltersConnectionTokensFromNoncanonicalHeaderMap(t *testing.T) {
	dst := httptest.NewRecorder()
	src := http.Header{
		"connection":   {"X-Private"},
		"X-Private":    {"secret"},
		"Content-Type": {"application/json"},
	}
	CopyHeaders(dst, src)
	if got := dst.Header().Get("X-Private"); got != "" {
		t.Fatalf("connection-scoped header leaked: %q", got)
	}
	if got := dst.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
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
