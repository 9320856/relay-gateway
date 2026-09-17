package protocol

import (
	"errors"
	"sort"
	"strings"
)

var ErrBuiltinPresetNotFound = errors.New("builtin protocol preset not found")

const (
	PresetOpenAI    = "openai"
	PresetAnthropic = "anthropic"
	PresetNewAPI    = "newapi"
	PresetSub2API   = "sub2api"
)

// BuiltinPreset returns a fresh copy of a versioned official protocol profile.
// The caller may edit the returned draft; published revisions must persist the
// compiled canonical JSON rather than mutate this catalog.
func BuiltinPreset(name string) (Profile, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case PresetOpenAI:
		return openAIPreset(), nil
	case PresetAnthropic:
		return anthropicPreset(), nil
	case PresetNewAPI:
		return newAPIPreset(), nil
	case PresetSub2API:
		return sub2APIPreset(), nil
	default:
		return Profile{}, ErrBuiltinPresetNotFound
	}
}

func BuiltinPresetNames() []string {
	names := []string{PresetAnthropic, PresetNewAPI, PresetOpenAI, PresetSub2API}
	sort.Strings(names)
	return names
}

func openAIPreset() Profile {
	return Profile{SchemaVersion: CurrentSchemaVersion, Name: "OpenAI-compatible", Operations: []Operation{
		directJSONOperation("chat.completions", "/chat/completions", []string{"choices"}),
		directJSONOperation("responses.create", "/responses", []string{"output"}),
		directJSONOperation("embeddings.create", "/embeddings", []string{"data"}),
		directJSONOperation("audio.speech", "/audio/speech", nil),
		withMediaRetention(directJSONOperation("images.create", "/images/generations", []string{"data"}), MediaRetentionRequired),
		directJSONOperation("moderations.create", "/moderations", []string{"results"}),
		videoOperation(),
	}}
}

func anthropicPreset() Profile {
	opMsg := directJSONOperation("messages.create", "/messages", []string{"content.0.text"})
	opMsg.Submit.Headers = map[string]string{"anthropic-version": "2023-06-01"}
	opTokens := directJSONOperation("messages.count_tokens", "/messages/count_tokens", []string{"input_tokens"})
	opTokens.Submit.Headers = map[string]string{"anthropic-version": "2023-06-01"}
	return Profile{SchemaVersion: CurrentSchemaVersion, Name: "Anthropic Messages", Operations: []Operation{opMsg, opTokens}}
}

func newAPIPreset() Profile {
	p := openAIPreset()
	p.Name = "NewAPI OpenAI-compatible"
	return p
}

func sub2APIPreset() Profile {
	p := openAIPreset()
	p.Name = "Sub2API OpenAI-compatible"
	p.ModelDefaults = []string{
		"claude-3-7-sonnet", "claude-3-7-sonnet-thought", "claude-3-5-sonnet-20241022",
		"claude-3-5-sonnet", "claude-3-5-haiku", "claude-3-opus", "gpt-4o", "gpt-4o-mini",
		"o1", "o1-preview", "o1-mini", "o3-mini",
	}
	// Sub2API currently advertises chat/embeddings/model discovery only; keep
	// image/video operations out of this preset until the upstream exposes them.
	p.Operations = []Operation{p.Operations[0], p.Operations[2]}
	return p
}

func directJSONOperation(name, path string, resultPaths []string) Operation {
	return Operation{Operation: name, ExecutionMode: ExecutionDirect, PollingMode: PollingOff, Submit: Submit{Method: "POST", Path: path, BodyEncoding: "json"}, Response: Response{ResultPaths: resultPaths}}
}

func videoOperation() Operation {
	return withMediaRetention(asyncJSONOperation("video.create", "/videos/generations", "/videos/generations/{task_id}", "/videos/generations/{task_id}/content", []string{"id", "task_id"}, []string{"completed", "succeeded"}, []string{"failed", "cancelled"}), MediaRetentionRequired)
}

func asyncJSONOperation(name, submitPath, pollPath, contentPath string, taskPaths, success, failure []string) Operation {
	return Operation{Operation: name, ExecutionMode: ExecutionAsync, PollingMode: PollingClient,
		Submit:   Submit{Method: "POST", Path: submitPath, BodyEncoding: "json"},
		Response: Response{TaskIDPaths: taskPaths},
		Poll:     &Poll{Method: "GET", Path: pollPath, IntervalMS: 3000, MaxAttempts: 200, MaxDurationMS: 600000, StatusPath: "status", SuccessValues: success, FailureValues: failure, ResultURLPaths: []string{"video_url", "url"}},
		Content:  &Content{Method: "GET", Path: contentPath},
	}
}

func withMediaRetention(op Operation, policy string) Operation {
	op.MediaRetention = policy
	return op
}
