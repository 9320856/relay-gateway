package adapter

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"strings"
	"testing"
	"time"

	"relay-gateway/config"
	"relay-gateway/model"
)

func TestAnthropicChatJSONConversionReframesBody(t *testing.T) {
	const message = `{"id":"msg_framing","model":"claude","stop_reason":"end_turn","content":[{"type":"text","text":"hello"}],"usage":{"input_tokens":7,"output_tokens":11}}`
	for _, tc := range []struct {
		name       string
		body       string
		wantLonger bool
	}{
		{name: "longer", body: message, wantLonger: true},
		{name: "shorter", body: strings.TrimSuffix(message, "}") + `,"ignored":"` + strings.Repeat("padding", 1024) + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)))
				w.Header().Set("X-Request-ID", "upstream-request")
				w.Header().Add("X-RateLimit-Limit", "requests=100")
				w.Header().Add("X-RateLimit-Limit", "tokens=10000")
				w.Header().Set("Connection", "X-Upstream-Private")
				w.Header().Set("X-Upstream-Private", "internal")
				_, _ = io.WriteString(w, tc.body)
			}))
			defer upstream.Close()

			a := NewAnthropicAdapter()
			a.Client = upstream.Client()
			a.Client.Timeout = 5 * time.Second
			channel := &config.UpstreamChannel{ID: "anthropic-framing", Type: "anthropic", BaseURL: upstream.URL}
			results := make(chan error, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				results <- a.ChatCompletions(r.Context(), channel, []byte(`{"model":"claude","messages":[]}`), "claude", false, w)
			}))
			defer gateway.Close()
			client := gateway.Client()
			client.Timeout = 5 * time.Second

			// A real server and transport enforce Content-Length and also exercise
			// the next response on the same connection after consuming the body.
			for request := 0; request < 2; request++ {
				reused := false
				req, err := http.NewRequest(http.MethodPost, gateway.URL, nil)
				if err != nil {
					t.Fatal(err)
				}
				req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
					GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
				}))
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, readErr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err := <-results; err != nil {
					t.Fatalf("conversion failed with a real ResponseWriter: %v", err)
				}
				if readErr != nil {
					t.Fatalf("reading converted response: %v", readErr)
				}
				if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(body)) {
					t.Fatalf("status=%d Content-Length=%d actual body=%d", resp.StatusCode, resp.ContentLength, len(body))
				}
				if (len(body) > len(tc.body)) != tc.wantLonger || len(body) == len(tc.body) {
					t.Fatalf("fixture does not exercise %s conversion: upstream=%d converted=%d", tc.name, len(tc.body), len(body))
				}
				if request > 0 && !reused {
					t.Fatal("converted response did not preserve reusable connection framing")
				}
				if resp.Header.Get("Content-Type") != "application/json; charset=utf-8" || resp.Header.Get("X-Request-ID") != "upstream-request" {
					t.Fatalf("converted response lost headers: %v", resp.Header)
				}
				if got := resp.Header.Values("X-RateLimit-Limit"); len(got) != 2 || got[0] != "requests=100" || got[1] != "tokens=10000" {
					t.Fatalf("rate limit metadata=%v", got)
				}
				if got := resp.Header.Get("X-Upstream-Private"); got != "" {
					t.Fatalf("connection-specific header leaked: %q", got)
				}
				var converted model.ChatCompletionResponse
				if err := json.Unmarshal(body, &converted); err != nil {
					t.Fatal(err)
				}
				if converted.ID != "msg_framing" || len(converted.Choices) != 1 || converted.Choices[0].Message.Content != "hello" || converted.Usage == nil || converted.Usage.TotalTokens != 18 {
					t.Fatalf("converted response=%+v", converted)
				}
			}
		})
	}
}

func TestAnthropicChatStreamingConversionReframesBody(t *testing.T) {
	const events = "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"model\":\"claude\"}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(events)))
		w.Header().Set("X-Request-ID", "upstream-stream")
		_, _ = io.WriteString(w, events)
	}))
	defer upstream.Close()
	a := NewAnthropicAdapter()
	a.Client = upstream.Client()
	a.Client.Timeout = 5 * time.Second
	channel := &config.UpstreamChannel{ID: "anthropic-stream-framing", Type: "anthropic", BaseURL: upstream.URL}
	results := make(chan error, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		results <- a.ChatCompletions(r.Context(), channel, []byte(`{"model":"claude","messages":[],"stream":true}`), "claude", true, w)
	}))
	defer gateway.Close()
	client := gateway.Client()
	client.Timeout = 5 * time.Second
	resp, err := client.Get(gateway.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err := <-results; err != nil {
		t.Fatalf("stream conversion failed: %v", err)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	if resp.ContentLength != -1 || resp.Header.Get("Content-Length") != "" || len(resp.TransferEncoding) != 1 || resp.TransferEncoding[0] != "chunked" {
		t.Fatalf("stream framing: Content-Length=%d Transfer-Encoding=%v headers=%v", resp.ContentLength, resp.TransferEncoding, resp.Header)
	}
	if resp.Header.Get("X-Request-ID") != "upstream-stream" || resp.Header.Get("Content-Type") != "text/event-stream; charset=utf-8" || resp.Header.Get("Cache-Control") != "no-cache, no-transform" || resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("stream headers=%v", resp.Header)
	}
	for _, fragment := range []string{`"role":"assistant"`, `"content":"hello"`, `"finish_reason":"stop"`, "data: [DONE]\n\n"} {
		if !strings.Contains(string(body), fragment) {
			t.Fatalf("stream missing %q: %s", fragment, body)
		}
	}
}
