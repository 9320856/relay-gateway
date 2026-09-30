package protocol

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHTTPExecutorNeverReplaysRedirectedProviderRequests(t *testing.T) {
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		for _, clientMode := range []string{"default", "injected", "literal", "nil literal"} {
			t.Run(http.StatusText(status)+"/"+clientMode, func(t *testing.T) {
				var originCalls, redirectedCalls atomic.Int32
				target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					redirectedCalls.Add(1)
					_, _ = io.WriteString(w, `{"ok":true}`)
				}))
				defer target.Close()
				location := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					originCalls.Add(1)
					w.Header().Set("Location", location)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"id":"redirected-task"}`)
				}))
				defer origin.Close()
				var executor *HTTPExecutor
				switch clientMode {
				case "default":
					executor = NewHTTPExecutor(nil)
				case "injected":
					executor = NewHTTPExecutor(origin.Client())
				case "literal":
					executor = &HTTPExecutor{Client: origin.Client()}
				default:
					executor = &HTTPExecutor{}
				}
				profile := testExecutorProfile()
				compiled, err := Compile(profile)
				if err != nil {
					t.Fatal(err)
				}
				request := Request{BaseURL: origin.URL, APIKeys: []string{"private-key"}, APIKeyHeader: "x-api-key", Body: map[string]any{"prompt": "private-body"}, TaskID: "task"}
				_, err = executor.Submit(context.Background(), compiled, "video.create", request)
				var upstream *ExecutorError
				if !errors.As(err, &upstream) || upstream.HTTPStatus != status {
					t.Fatalf("submit error = %v, want redirect status %d", err, status)
				}
				_, err = executor.PollOnce(context.Background(), compiled, "video.create", request)
				if !errors.As(err, &upstream) || upstream.HTTPStatus != status {
					t.Fatalf("poll error = %v, want redirect status %d", err, status)
				}
				content, err := executor.FetchContent(context.Background(), compiled, "video.create", request)
				if err != nil || content.HTTPStatus != status {
					t.Fatalf("content redirect = %+v, %v", content, err)
				}
				_ = content.Body.Close()
				direct, err := Compile(testDirectProfile())
				if err != nil {
					t.Fatal(err)
				}
				writer := httptest.NewRecorder()
				_, err = executor.ExecuteRaw(context.Background(), direct, "chat.completions", request, writer, false)
				if !errors.As(err, &upstream) || upstream.HTTPStatus != status || writer.Body.Len() != 0 || writer.Header().Get("Location") != "" {
					t.Fatalf("raw redirect error=%v body=%q headers=%v", err, writer.Body.String(), writer.Header())
				}
				if got := redirectedCalls.Load(); got != 0 {
					t.Fatalf("redirected requests = %d; credentials/body may have escaped", got)
				}
				if got := originCalls.Load(); got != 4 {
					t.Fatalf("origin requests = %d, want one per operation", got)
				}
			})
		}
	}
}
