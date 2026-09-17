package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"relay-gateway/config"
	"relay-gateway/model"
)

func TestOpenAIToAnthropicRichRequest(t *testing.T) {
	raw := []byte(`{
  "model":"client-model",
  "reasoning_effort":"high",
  "max_tokens":2048,
  "messages":[
    {"role":"system","content":"You are concise."},
    {"role":"user","content":[{"type":"text","text":"describe"},{"type":"image_url","image_url":{"url":"data:image/png;base64,QUJD"}}]},
    {"role":"assistant","content":"checking","tool_calls":[{"id":"call_1","type":"function","function":{"name":"weather","arguments":"{\"city\":\"Paris\"}"}}]},
    {"role":"tool","tool_call_id":"call_1","content":"sunny"}
  ],
  "tools":[{"type":"function","function":{"name":"weather","description":"lookup","parameters":{"type":"object"}}}],
  "tool_choice":{"type":"function","function":{"name":"weather"}}
}`)

	converted, err := openAIToAnthropic(raw, "claude-target")
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(converted, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["model"] != "claude-target" || payload["max_tokens"].(float64) != 2048 {
		t.Fatalf("model/max_tokens conversion failed: %s", converted)
	}
	thinking := payload["thinking"].(map[string]any)
	if thinking["type"] != "enabled" || thinking["budget_tokens"].(float64) != 4096 {
		t.Fatalf("reasoning conversion failed: %#v", thinking)
	}
	system := payload["system"].([]any)
	if len(system) != 1 || system[0].(map[string]any)["text"] != "You are concise." {
		t.Fatalf("system conversion failed: %#v", system)
	}
	messages := payload["messages"].([]any)
	userContent := messages[0].(map[string]any)["content"].([]any)
	image := userContent[1].(map[string]any)["source"].(map[string]any)
	if image["type"] != "base64" || image["media_type"] != "image/png" || image["data"] != "QUJD" {
		t.Fatalf("image conversion failed: %#v", image)
	}
	assistantContent := messages[1].(map[string]any)["content"].([]any)
	toolUse := assistantContent[1].(map[string]any)
	if toolUse["type"] != "tool_use" || toolUse["name"] != "weather" {
		t.Fatalf("tool call conversion failed: %#v", toolUse)
	}
	toolResult := messages[2].(map[string]any)["content"].([]any)[0].(map[string]any)
	if toolResult["type"] != "tool_result" || toolResult["tool_use_id"] != "call_1" {
		t.Fatalf("tool result conversion failed: %#v", toolResult)
	}
	choice := payload["tool_choice"].(map[string]any)
	if choice["type"] != "tool" || choice["name"] != "weather" {
		t.Fatalf("tool choice conversion failed: %#v", choice)
	}
}

func TestAnthropicToOpenAIResponse(t *testing.T) {
	raw := []byte(`{"id":"msg_1","model":"claude-target","stop_reason":"tool_use","content":[{"type":"thinking","thinking":"plan"},{"type":"text","text":"hello"},{"type":"tool_use","id":"tool_1","name":"weather","input":{"city":"Paris"}}],"usage":{"input_tokens":7,"output_tokens":11}}`)
	converted, err := anthropicToOpenAI(raw)
	if err != nil {
		t.Fatal(err)
	}
	var response model.ChatCompletionResponse
	if err := json.Unmarshal(converted, &response); err != nil {
		t.Fatal(err)
	}
	if response.ID != "msg_1" || response.Model != "claude-target" || response.Usage == nil || response.Usage.TotalTokens != 18 {
		t.Fatalf("metadata conversion failed: %+v", response)
	}
	message := response.Choices[0].Message
	if message.Content != "hello" || message.ReasoningContent != "plan" || len(message.ToolCalls) != 1 {
		t.Fatalf("content conversion failed: %+v", message)
	}
	if response.Choices[0].FinishReason == nil || *response.Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("stop reason conversion failed: %+v", response.Choices[0].FinishReason)
	}
}

func TestAnthropicChatStreamingConversionAndHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("x-api-key") != "anthropic-secret" || r.Header.Get("Authorization") != "" {
			t.Fatalf("unexpected auth headers: %#v", r.Header)
		}
		if r.Header.Get("anthropic-version") != "2023-06-01" {
			t.Fatalf("missing Anthropic version: %#v", r.Header)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"model":"claude-upstream"`) {
			t.Fatalf("model was not mapped before dispatch: %s", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_stream\",\"model\":\"claude-upstream\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"plan\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"weather\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\\\"Paris\\\"}\"}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":9}}\n\n")
	}))
	defer server.Close()

	channel := &config.UpstreamChannel{ID: "anthropic-main", Type: "anthropic", BaseURL: server.URL + "/v1", APIKeys: []string{"anthropic-secret"}}
	recorder := httptest.NewRecorder()
	err := NewAnthropicAdapter().ChatCompletions(context.Background(), channel, []byte(`{"model":"client-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`), "claude-upstream", true, recorder)
	if err != nil {
		t.Fatal(err)
	}
	result := recorder.Body.String()
	for _, fragment := range []string{`"role":"assistant"`, `"reasoning_content":"plan"`, `"content":"hello"`, `"finish_reason":"tool_calls"`, "data: [DONE]"} {
		if !strings.Contains(result, fragment) {
			t.Fatalf("stream missing %q: %s", fragment, result)
		}
	}
}
