package router

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"relay-gateway/protocol"
)

type profileTimeoutTransport func(*http.Request) (*http.Response, error)

func (f profileTimeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestProfileOperationRequestDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, operation, mode string
		raw                   bool
		parentTimeout         time.Duration
		want                  time.Duration
	}{
		{name: "image bridge", operation: "images.create", mode: protocol.ExecutionDirect, want: 5 * time.Minute},
		{name: "image compatibility alias", operation: "image.create", mode: protocol.ExecutionDirect, want: 5 * time.Minute},
		{name: "raw image generation", operation: "images.create", mode: protocol.ExecutionDirect, raw: true, want: 5 * time.Minute},
		{name: "image edits", operation: "images.edits", mode: protocol.ExecutionDirect, raw: true, want: 5 * time.Minute},
		{name: "chat", operation: "chat.completions", mode: protocol.ExecutionDirect, raw: true, want: time.Minute},
		{name: "async image submission", operation: "images.create", mode: protocol.ExecutionAsync, want: time.Minute},
		{name: "shorter caller deadline", operation: "images.create", mode: protocol.ExecutionDirect, parentTimeout: 10 * time.Second, want: 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := protocol.Operation{Operation: tc.operation, ExecutionMode: tc.mode, PollingMode: protocol.PollingOff,
				Submit:   protocol.Submit{Method: http.MethodPost, Path: "/images", BodyEncoding: "json"},
				Response: protocol.Response{TaskIDPaths: []string{"id"}, ResultPaths: []string{"data"}},
			}
			compiled, err := protocol.Compile(protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Name: "deadline test", Operations: []protocol.Operation{op}})
			if err != nil {
				t.Fatal(err)
			}
			executor := newProfileOperationExecutor(op)
			calls := 0
			executor.Client.Transport = profileTimeoutTransport(func(req *http.Request) (*http.Response, error) {
				calls++
				deadline, ok := req.Context().Deadline()
				remaining := time.Until(deadline)
				if !ok || remaining > tc.want || remaining < tc.want-time.Second {
					t.Errorf("upstream deadline remaining = %v (set=%v), want about %v", remaining, ok, tc.want)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"task-1","data":[{"url":"https://cdn.example/image.png"}]}`)), Request: req}, nil
			})
			ctx := context.Background()
			if tc.parentTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.parentTimeout)
				defer cancel()
			}
			request := protocol.Request{BaseURL: "https://upstream.example/v1", Body: map[string]any{"prompt": "test"}}
			if tc.raw {
				_, err = executor.ExecuteRaw(ctx, compiled, tc.operation, request, httptest.NewRecorder(), false)
			} else {
				_, err = executor.Submit(ctx, compiled, tc.operation, request)
			}
			if err != nil || calls != 1 {
				t.Fatalf("submit calls=%d err=%v", calls, err)
			}
		})
	}
}
