package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay-gateway/protocol"
)

func executorRegressionProfile(t *testing.T) protocol.CompiledProfile {
	t.Helper()
	p, err := protocol.Compile(protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{
		{Operation: "chat.completions", ExecutionMode: protocol.ExecutionDirect, PollingMode: protocol.PollingOff, Submit: protocol.Submit{Method: http.MethodPost, Path: "/chat", BodyEncoding: "json"}},
		{Operation: "video.create", ExecutionMode: protocol.ExecutionAsync, PollingMode: protocol.PollingClient, Submit: protocol.Submit{Method: http.MethodPost, Path: "/videos", BodyEncoding: "json"}, Response: protocol.Response{TaskIDPaths: []string{"id"}}, Poll: &protocol.Poll{Method: http.MethodGet, Path: "/videos/{task_id}", StatusPath: "status", SuccessValues: []string{"completed"}, FailureValues: []string{"failed"}, ResultURLPaths: []string{"url"}, IntervalMS: 1, MaxAttempts: 3, MaxDurationMS: 1000}, Content: &protocol.Content{Method: http.MethodGet, Path: "/videos/{task_id}/content"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProfileOperationsShareBreakerWithoutRetryingSubmissions(t *testing.T) {
	p := executorRegressionProfile(t)
	for _, operation := range []string{"submit", "poll", "raw", "content"} {
		t.Run(operation, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(503)
				io.WriteString(w, `{"error":"unavailable"}`)
			}))
			defer upstream.Close()
			d := &Dispatcher{}
			e := d.ProfileExecutor(protocol.NewHTTPExecutor(upstream.Client()), "channel")
			request := protocol.Request{BaseURL: upstream.URL, TaskID: "id", Body: map[string]any{}}
			invoke := func() error {
				switch operation {
				case "submit":
					_, err := e.Submit(nil, p, "video.create", request)
					return err
				case "poll":
					_, err := e.PollOnce(nil, p, "video.create", request)
					return err
				case "raw":
					_, err := e.ExecuteRaw(nil, p, "chat.completions", request, httptest.NewRecorder(), false)
					return err
				default:
					_, err := e.FetchContent(nil, p, "video.create", request)
					return err
				}
			}
			for i := 0; i < 3; i++ {
				if err := invoke(); err == nil {
					t.Fatal("upstream failure accepted")
				}
			}
			if err := invoke(); !errors.Is(err, ErrChannelUnavailable) {
				t.Fatalf("cooldown error=%v", err)
			}
			if calls.Load() != 3 {
				t.Fatalf("paid operations were retried or breaker bypassed: calls=%d", calls.Load())
			}
		})
	}
}

func TestProfileHalfOpenAllowsOneAttemptAndReleasesCancellation(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			close(started)
			<-r.Context().Done()
			return
		}
		io.WriteString(w, `{"id":"accepted"}`)
	}))
	defer upstream.Close()
	d := &Dispatcher{}
	st := d.GetBreaker("channel")
	st.failCount = 3
	st.cooldownUntil = time.Now().Add(-time.Second)
	e := d.ProfileExecutor(protocol.NewHTTPExecutor(upstream.Client()), "channel")
	p := executorRegressionProfile(t)
	req := protocol.Request{BaseURL: upstream.URL, Body: map[string]any{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := e.Submit(ctx, p, "video.create", req); done <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not start")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.Submit(context.Background(), p, "video.create", req)
			if !errors.Is(err, ErrChannelUnavailable) {
				t.Errorf("extra probe error=%v", err)
			}
		}()
	}
	wg.Wait()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	st.mu.Lock()
	count, halfOpen := st.failCount, st.halfOpen
	st.mu.Unlock()
	if count != 3 || halfOpen || calls.Load() != 1 {
		t.Fatalf("cancel changed breaker count=%d reserved=%v calls=%d", count, halfOpen, calls.Load())
	}
	if _, err := e.Submit(context.Background(), p, "video.create", req); err != nil {
		t.Fatalf("replacement probe: %v", err)
	}
	if !d.candidateAvailable("channel") {
		t.Fatal("successful probe did not restore channel")
	}
}

type regressionTransport func(*http.Request) (*http.Response, error)

func (f regressionTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failingReadBody struct{ sent bool }

func (b *failingReadBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		return copy(p, "data"), nil
	}
	return 0, io.ErrUnexpectedEOF
}
func (*failingReadBody) Close() error { return nil }

type failingClientWriter struct{ http.ResponseWriter }

func (f failingClientWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestProfileRawFeedbackAttributesReadAndWriteFailures(t *testing.T) {
	p := executorRegressionProfile(t)
	for _, stream := range []bool{false, true} {
		for _, source := range []string{"read", "write"} {
			t.Run(source+map[bool]string{false: "JSON", true: "SSE"}[stream], func(t *testing.T) {
				d := &Dispatcher{}
				client := &http.Client{Transport: regressionTransport(func(*http.Request) (*http.Response, error) {
					var body io.ReadCloser = io.NopCloser(strings.NewReader("data"))
					if source == "read" {
						body = &failingReadBody{}
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
				})}
				var writer http.ResponseWriter = httptest.NewRecorder()
				if source == "write" {
					writer = failingClientWriter{writer}
				}
				_, err := d.ProfileExecutor(protocol.NewHTTPExecutor(client), "channel").ExecuteRaw(context.Background(), p, "chat.completions", protocol.Request{BaseURL: "http://example.test", Body: map[string]any{}}, writer, stream)
				var executionErr *protocol.ExecutorError
				if !errors.As(err, &executionErr) || executionErr.Phase != source || executionErr.AllowFailover {
					t.Fatalf("error=%#v", err)
				}
				st := d.GetBreaker("channel")
				st.mu.Lock()
				count := st.failCount
				st.mu.Unlock()
				want := 0
				if source == "read" {
					want = 1
				}
				if count != want {
					t.Fatalf("failure count=%d want=%d", count, want)
				}
			})
		}
	}
}
