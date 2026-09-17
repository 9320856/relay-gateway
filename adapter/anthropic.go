package adapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/model"
)

type AnthropicAdapter struct{ *OpenAIAdapter }

func init() {
	Register(AdapterMeta{
		Type:        "anthropic",
		Name:        "Claude / Anthropic 官方",
		Description: "Anthropic Messages 原生协议，并支持从 OpenAI Chat Completions 自动转换",
		Protocols:   []string{"chat", "messages", "models"},
		DefaultURL:  "https://api.anthropic.com/v1",
	}, NewAnthropicAdapter())
}

func NewAnthropicAdapter() *AnthropicAdapter {
	return &AnthropicAdapter{OpenAIAdapter: NewOpenAIAdapter()}
}

func (a *AnthropicAdapter) AnthropicMessages(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, isStream bool, w http.ResponseWriter) error {
	return a.OpenAIAdapter.AnthropicMessages(ctx, channel, rawBody, isStream, w)
}

func (a *AnthropicAdapter) ChatCompletions(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, isStream bool, w http.ResponseWriter) error {
	if channel != nil {
		if mapped, ok := channel.ModelMap[targetModel]; ok && mapped != "" {
			targetModel = mapped
		}
	}
	body, err := openAIToAnthropic(rawBody, targetModel)
	if err != nil {
		return err
	}
	targetURL := a.NormalizeURL(channel.BaseURL, "/messages")
	return a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(req, channel, key)
		return req, nil
	}, func(resp *http.Response, _ string) error {
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4097))
			return &UpstreamHTTPError{StatusCode: resp.StatusCode, Body: truncateBody(data, 4096), RetryAfter: resp.Header.Get("Retry-After")}
		}
		if isStream {
			audit.AddEvent(ctx, "stream_started", audit.EventData{ChannelID: channel.ID, StatusCode: resp.StatusCode, Message: "streaming response started"})
			return streamAnthropicAsOpenAI(ctx, resp, targetModel, w)
		}
		data, err := readUpstreamJSONBody(resp.Body)
		if err != nil {
			return err
		}
		converted, err := anthropicToOpenAI(data)
		if err != nil {
			return err
		}
		copyHeader(w, resp.Header)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(resp.StatusCode)
		if _, err := w.Write(converted); err != nil {
			return &ErrStreamAborted{Err: err}
		}
		return nil
	})
}

func openAIToAnthropic(raw []byte, targetModel string) ([]byte, error) {
	var source map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&source); err != nil {
		return nil, fmt.Errorf("invalid OpenAI chat request: %w", err)
	}
	if targetModel != "" {
		source["model"] = targetModel
	}
	out := map[string]any{"model": source["model"], "max_tokens": 4096}
	for _, key := range []string{"temperature", "top_p", "stop", "stream", "metadata", "thinking"} {
		if value, ok := source[key]; ok {
			out[key] = value
		}
	}
	if value, ok := source["max_tokens"]; ok {
		out["max_tokens"] = value
	}
	if _, ok := source["thinking"]; !ok {
		if effort, ok := source["reasoning_effort"].(string); ok && effort != "" && effort != "none" {
			budget := 1024
			if effort == "high" {
				budget = 4096
			}
			out["thinking"] = map[string]any{"type": "enabled", "budget_tokens": budget}
		}
	}
	var system []any
	var messages []any
	if incoming, ok := source["messages"].([]any); ok {
		for _, item := range incoming {
			message, ok := item.(map[string]any)
			if !ok {
				continue
			}
			role, _ := message["role"].(string)
			content := convertContent(message["content"])
			if role == "system" || role == "developer" {
				system = append(system, content...)
				continue
			}
			if role == "tool" {
				toolID, _ := message["tool_call_id"].(string)
				messages = append(messages, map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": toolID, "content": content}}})
				continue
			}
			if role != "assistant" {
				role = "user"
			}
			if calls, ok := message["tool_calls"].([]any); ok {
				for _, rawCall := range calls {
					call, _ := rawCall.(map[string]any)
					function, _ := call["function"].(map[string]any)
					input := any(map[string]any{})
					if args, ok := function["arguments"].(string); ok && args != "" {
						_ = json.Unmarshal([]byte(args), &input)
					}
					content = append(content, map[string]any{"type": "tool_use", "id": call["id"], "name": function["name"], "input": input})
				}
			}
			messages = append(messages, map[string]any{"role": role, "content": content})
		}
	}
	if len(system) > 0 {
		out["system"] = system
	}
	out["messages"] = messages
	if tools, ok := source["tools"].([]any); ok {
		converted := make([]any, 0, len(tools))
		for _, rawTool := range tools {
			tool, _ := rawTool.(map[string]any)
			function, _ := tool["function"].(map[string]any)
			converted = append(converted, map[string]any{"name": function["name"], "description": function["description"], "input_schema": function["parameters"]})
		}
		out["tools"] = converted
	}
	if choice, ok := source["tool_choice"]; ok {
		out["tool_choice"] = convertToolChoice(choice)
	}
	return json.Marshal(out)
}

func convertContent(value any) []any {
	switch content := value.(type) {
	case string:
		return []any{map[string]any{"type": "text", "text": content}}
	case []any:
		out := make([]any, 0, len(content))
		for _, rawPart := range content {
			part, ok := rawPart.(map[string]any)
			if !ok {
				continue
			}
			typeName, _ := part["type"].(string)
			switch typeName {
			case "text", "input_text":
				out = append(out, map[string]any{"type": "text", "text": part["text"]})
			case "image_url", "input_image":
				imageURL := ""
				switch image := part["image_url"].(type) {
				case string:
					imageURL = image
				case map[string]any:
					imageURL, _ = image["url"].(string)
				}
				if imageURL == "" {
					imageURL, _ = part["url"].(string)
				}
				if strings.HasPrefix(imageURL, "data:") {
					media, data := splitDataURL(imageURL)
					out = append(out, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": media, "data": data}})
				} else if imageURL != "" {
					out = append(out, map[string]any{"type": "image", "source": map[string]any{"type": "url", "url": imageURL}})
				}
			case "tool_result", "tool_use", "thinking":
				out = append(out, part)
			}
		}
		return out
	default:
		return []any{map[string]any{"type": "text", "text": fmt.Sprint(value)}}
	}
}

func splitDataURL(value string) (string, string) {
	media := "application/octet-stream"
	if semi := strings.Index(value, ";"); semi > len("data:") {
		media = value[len("data:"):semi]
	}
	if comma := strings.Index(value, ","); comma >= 0 {
		return media, value[comma+1:]
	}
	return media, value
}

func convertToolChoice(value any) any {
	if text, ok := value.(string); ok {
		switch text {
		case "required":
			return map[string]any{"type": "any"}
		case "none":
			return nil
		default:
			return map[string]any{"type": "auto"}
		}
	}
	if object, ok := value.(map[string]any); ok {
		if fn, ok := object["function"].(map[string]any); ok {
			return map[string]any{"type": "tool", "name": fn["name"]}
		}
	}
	return map[string]any{"type": "auto"}
}

func anthropicToOpenAI(raw []byte) ([]byte, error) {
	var source struct {
		ID         string           `json:"id"`
		Model      string           `json:"model"`
		StopReason string           `json:"stop_reason"`
		Content    []map[string]any `json:"content"`
		Usage      struct {
			Input  int `json:"input_tokens"`
			Output int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &source); err != nil {
		return nil, fmt.Errorf("invalid Anthropic response: %w", err)
	}
	message := model.ChatMessage{Role: "assistant"}
	var texts, thinking []string
	for _, block := range source.Content {
		switch block["type"] {
		case "text":
			if text, ok := block["text"].(string); ok {
				texts = append(texts, text)
			}
		case "thinking":
			if text, ok := block["thinking"].(string); ok {
				thinking = append(thinking, text)
			}
		case "tool_use":
			args, _ := json.Marshal(block["input"])
			message.ToolCalls = append(message.ToolCalls, model.ToolCall{ID: fmt.Sprint(block["id"]), Type: "function", Function: model.FunctionCall{Name: fmt.Sprint(block["name"]), Arguments: string(args)}})
		}
	}
	message.Content = strings.Join(texts, "")
	message.ReasoningContent = strings.Join(thinking, "")
	finish := mapStopReason(source.StopReason)
	response := model.ChatCompletionResponse{ID: source.ID, Object: "chat.completion", Created: time.Now().Unix(), Model: source.Model, Choices: []model.ChatCompletionChoice{{Index: 0, Message: message, FinishReason: &finish}}, Usage: &model.Usage{PromptTokens: source.Usage.Input, CompletionTokens: source.Usage.Output, TotalTokens: source.Usage.Input + source.Usage.Output}}
	return json.Marshal(response)
}

func mapStopReason(reason string) string {
	switch reason {
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	case "stop_sequence", "end_turn":
		return "stop"
	default:
		return reason
	}
}

func streamAnthropicAsOpenAI(ctx context.Context, resp *http.Response, fallbackModel string, w http.ResponseWriter) error {
	copyHeader(w, resp.Header)
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Del("Content-Length")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 2<<20)
	id, modelName := "", fallbackModel
	toolIndexes := map[int]int{}
	toolCount := 0
	emit := func(delta map[string]any, finish *string, usage any) error {
		chunk := map[string]any{"id": id, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": modelName, "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}}
		if usage != nil {
			chunk["usage"] = usage
		}
		data, _ := json.Marshal(chunk)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return &ErrStreamAborted{Err: ctx.Err()}
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(data), &event) != nil {
			continue
		}
		typeName, _ := event["type"].(string)
		switch typeName {
		case "message_start":
			message, _ := event["message"].(map[string]any)
			id, _ = message["id"].(string)
			if m, ok := message["model"].(string); ok {
				modelName = m
			}
			if err := emit(map[string]any{"role": "assistant", "content": ""}, nil, nil); err != nil {
				return &ErrStreamAborted{Err: err}
			}
		case "content_block_start":
			block, _ := event["content_block"].(map[string]any)
			idx := intNumber(event["index"])
			if block["type"] == "tool_use" {
				toolIndexes[idx] = toolCount
				delta := map[string]any{"tool_calls": []any{map[string]any{"index": toolCount, "id": block["id"], "type": "function", "function": map[string]any{"name": block["name"], "arguments": ""}}}}
				toolCount++
				if err := emit(delta, nil, nil); err != nil {
					return &ErrStreamAborted{Err: err}
				}
			}
		case "content_block_delta":
			delta, _ := event["delta"].(map[string]any)
			idx := intNumber(event["index"])
			switch delta["type"] {
			case "text_delta":
				if err := emit(map[string]any{"content": delta["text"]}, nil, nil); err != nil {
					return &ErrStreamAborted{Err: err}
				}
			case "thinking_delta":
				if err := emit(map[string]any{"reasoning_content": delta["thinking"]}, nil, nil); err != nil {
					return &ErrStreamAborted{Err: err}
				}
			case "input_json_delta":
				if err := emit(map[string]any{"tool_calls": []any{map[string]any{"index": toolIndexes[idx], "function": map[string]any{"arguments": delta["partial_json"]}}}}, nil, nil); err != nil {
					return &ErrStreamAborted{Err: err}
				}
			}
		case "message_delta":
			delta, _ := event["delta"].(map[string]any)
			finish := mapStopReason(fmt.Sprint(delta["stop_reason"]))
			if err := emit(map[string]any{}, &finish, event["usage"]); err != nil {
				return &ErrStreamAborted{Err: err}
			}
		case "error":
			return &ErrStreamAborted{Err: fmt.Errorf("anthropic stream error: %s", data)}
		}
	}
	if err := scanner.Err(); err != nil {
		return &ErrStreamAborted{Err: err}
	}
	if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
		return &ErrStreamAborted{Err: err}
	}
	if flusher != nil {
		flusher.Flush()
	}
	return nil
}

func intNumber(value any) int {
	switch n := value.(type) {
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}
