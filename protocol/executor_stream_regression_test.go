package protocol

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func rawRegressionProfile(t *testing.T) CompiledProfile {
	t.Helper()
	p, err := Compile(Profile{SchemaVersion: CurrentSchemaVersion, Operations: []Operation{{Operation: "chat.completions", ExecutionMode: ExecutionDirect, PollingMode: PollingOff, Submit: Submit{Method: http.MethodPost, Path: "/chat", BodyEncoding: "json"}}}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// A real downstream HTTP connection is required: a ResponseRecorder checks
// only final bytes, and cannot reveal whether the first event was buffered.
func TestExecuteRawSSEFlushesBeforeUpstreamCompletes(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	allowFinish := func() { once.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
		io.WriteString(w, "data: last\n\n")
	}))
	defer upstream.Close()
	defer allowFinish()
	p := rawRegressionProfile(t)
	executor := NewHTTPExecutor(upstream.Client())
	execDone := make(chan error, 1)
	downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := executor.ExecuteRaw(r.Context(), p, "chat.completions", Request{BaseURL: upstream.URL, Body: map[string]any{}}, w, true)
		execDone <- err
	}))
	defer downstream.Close()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(downstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("first event=%q error=%v", line, err)
	}
	select {
	case err := <-execDone:
		t.Fatalf("upstream finished before release: %v", err)
	default:
	}
	allowFinish()
	remaining, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(remaining), "data: last") {
		t.Fatalf("remaining=%q error=%v", remaining, err)
	}
	if err = <-execDone; err != nil {
		t.Fatal(err)
	}
}

func TestExecuteRawStreamRejectsOversizedResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "123456789") }))
	defer upstream.Close()
	writer := httptest.NewRecorder()
	_, err := NewHTTPExecutor(upstream.Client()).ExecuteRaw(context.Background(), rawRegressionProfile(t), "chat.completions", Request{BaseURL: upstream.URL, Body: map[string]any{}, MaxResponseBytes: 5}, writer, true)
	var executionErr *ExecutorError
	if !errors.Is(err, ErrResponseTooLarge) || !errors.As(err, &executionErr) || executionErr.Phase != "read" || executionErr.AllowFailover {
		t.Fatalf("error=%#v", err)
	}
	if got := writer.Body.String(); got != "12345" {
		t.Fatalf("forwarded beyond limit: %q", got)
	}
}

func TestFetchContentTimeoutLivesUntilBodyCompletes(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-time.After(30 * time.Millisecond):
			io.WriteString(w, "video-content")
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	p, err := Compile(testExecutorProfile())
	if err != nil {
		t.Fatal(err)
	}
	e := NewHTTPExecutor(upstream.Client())
	e.RequestTimeout = time.Second
	result, err := e.FetchContent(context.Background(), p, "video.create", Request{BaseURL: upstream.URL, TaskID: "task"})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Body.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil || string(body) != "video-content" {
		t.Fatalf("body=%q error=%v", body, err)
	}
}
