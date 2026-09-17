package protocol

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// TestNewAPIPresetVideoAsyncContract ensures removing provider-specific
// presets does not change NewAPI's declarative async submit, poll, or content
// retrieval contract.
func TestNewAPIPresetVideoAsyncContract(t *testing.T) {
	profile, err := BuiltinPreset(PresetNewAPI)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}

	var submit map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/videos/generations":
			if err := json.NewDecoder(r.Body).Decode(&submit); err != nil {
				t.Fatal(err)
			}
			_, _ = io.WriteString(w, `{"id":"video-1","status":"queued"}`)
		case "GET /v1/videos/generations/video-1":
			_, _ = io.WriteString(w, `{"status":"completed","video_url":"https://cdn.example/video.mp4"}`)
		case "GET /v1/videos/generations/video-1/content":
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = io.WriteString(w, "video-bytes")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	executor := NewHTTPExecutor(server.Client())
	accepted, err := executor.Submit(context.Background(), compiled, "video.create", Request{
		BaseURL: server.URL + "/v1",
		Body:    map[string]any{"model": "video-model", "prompt": "a cat"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(submit, map[string]any{"model": "video-model", "prompt": "a cat"}) {
		t.Fatalf("submit body = %#v", submit)
	}
	if accepted.TaskID != "video-1" {
		t.Fatalf("task ID = %q", accepted.TaskID)
	}

	polled, err := executor.PollOnce(context.Background(), compiled, "video.create", Request{BaseURL: server.URL + "/v1", TaskID: accepted.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	if polled.Status != "completed" || !reflect.DeepEqual(polled.ResultURLs, []string{"https://cdn.example/video.mp4"}) {
		t.Fatalf("poll result = %#v", polled)
	}
	content, err := executor.FetchContent(context.Background(), compiled, "video.create", Request{BaseURL: server.URL + "/v1", TaskID: accepted.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	defer content.Body.Close()
	body, _ := io.ReadAll(content.Body)
	if content.HTTPStatus != http.StatusOK || string(body) != "video-bytes" {
		t.Fatalf("content = status %d body %q", content.HTTPStatus, body)
	}
}
