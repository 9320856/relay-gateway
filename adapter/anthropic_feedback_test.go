package adapter

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"relay-gateway/config"
	"relay-gateway/internal/httpforward"
)

type anthropicErrorBody struct {
	err    error
	closed bool
}

func (b *anthropicErrorBody) Read([]byte) (int, error) { return 0, b.err }
func (b *anthropicErrorBody) Close() error {
	b.closed = true
	return nil
}

type anthropicErrorWriter struct{ http.ResponseWriter }

func (w anthropicErrorWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func assertAnthropicErrorSource(t *testing.T, err error, operation string, aborted bool, cause error) {
	t.Helper()
	var source *httpforward.StreamError
	if !errors.As(err, &source) || source.Op != operation {
		t.Fatalf("source=%#v error=%v, want %q", source, err, operation)
	}
	var streamAborted *ErrStreamAborted
	if errors.As(err, &streamAborted) != aborted {
		t.Fatalf("abort classification changed: error=%v want=%v", err, aborted)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("underlying error lost: error=%v want=%v", err, cause)
	}
}

func TestAnthropicStreamErrorsIdentifyReadAndWriteSources(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      io.ReadCloser
		failWrite bool
		operation string
		cause     error
	}{
		{"emitWrite", io.NopCloser(strings.NewReader("data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg\"}}\n\n")), true, "write", io.ErrClosedPipe},
		{"doneWrite", io.NopCloser(strings.NewReader("")), true, "write", io.ErrClosedPipe},
		{"scannerRead", &anthropicErrorBody{err: io.ErrUnexpectedEOF}, false, "read", io.ErrUnexpectedEOF},
		{"providerError", io.NopCloser(strings.NewReader("data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}\n\n")), false, "read", nil},
		{"canceledRead", &anthropicErrorBody{err: context.Canceled}, false, "read", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.body.Close()
			var writer http.ResponseWriter = httptest.NewRecorder()
			if tc.failWrite {
				writer = anthropicErrorWriter{writer}
			}
			err := streamAnthropicAsOpenAI(context.Background(), &http.Response{Header: make(http.Header), Body: tc.body}, "claude", writer)
			assertAnthropicErrorSource(t, err, tc.operation, true, tc.cause)
		})
	}
}

func TestAnthropicJSONErrorsIdentifyReadAndWriteSources(t *testing.T) {
	for _, source := range []string{"read", "write"} {
		t.Run(source, func(t *testing.T) {
			var body io.ReadCloser = io.NopCloser(strings.NewReader(`{"id":"msg","model":"claude","content":[{"type":"text","text":"hello"}]}`))
			var failedBody *anthropicErrorBody
			var writer http.ResponseWriter = httptest.NewRecorder()
			cause := io.ErrClosedPipe
			if source == "read" {
				failedBody = &anthropicErrorBody{err: io.ErrUnexpectedEOF}
				body, cause = failedBody, io.ErrUnexpectedEOF
			} else {
				writer = anthropicErrorWriter{writer}
			}
			a := NewAnthropicAdapter()
			a.Client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
			})}
			err := a.ChatCompletions(context.Background(), &config.UpstreamChannel{ID: "anthropic-feedback", Type: "anthropic", BaseURL: "http://upstream.example/v1"}, []byte(`{"model":"claude","messages":[{"role":"user","content":"hi"}]}`), "claude", false, writer)
			assertAnthropicErrorSource(t, err, source, source == "write", cause)
			if failedBody != nil && !failedBody.closed {
				t.Fatal("failed upstream body was not closed")
			}
		})
	}
}

func TestAnthropicJSONParseErrorsKeepExistingClassification(t *testing.T) {
	a := NewAnthropicAdapter()
	a.Client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("invalid-json"))}, nil
	})}
	err := a.ChatCompletions(context.Background(), &config.UpstreamChannel{ID: "anthropic-parse", Type: "anthropic", BaseURL: "http://upstream.example/v1"}, []byte(`{"model":"claude","messages":[]}`), "claude", false, httptest.NewRecorder())
	var source *httpforward.StreamError
	var aborted *ErrStreamAborted
	if err == nil || errors.As(err, &source) || errors.As(err, &aborted) {
		t.Fatalf("protocol parsing error changed classification: %v", err)
	}
}
