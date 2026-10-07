package db

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"relay-gateway/media"
	"relay-gateway/protocol"
)

func TestTaskMediaRetentionUsesFrozenChannelPolicy(t *testing.T) {
	for _, policy := range []string{media.RetentionDisabled, media.RetentionBestEffort, media.RetentionRequired} {
		// Frozen policies do not need to consult a mutable channel or a Profile
		// catalog which may already have been deleted.
		run := &TaskRun{Engine: "profile", Operation: "video.create", MediaRetention: policy}
		if got, err := TaskMediaRetention(context.Background(), run); err != nil || got != policy {
			t.Fatalf("frozen policy %q = %q, %v", policy, got, err)
		}
	}
	if got, err := TaskMediaRetention(context.Background(), &TaskRun{Engine: "legacy"}); err != nil || got != media.RetentionDisabled {
		t.Fatalf("historical legacy policy = %q, %v", got, err)
	}
	for _, run := range []*TaskRun{nil, {MediaRetention: "sometimes"}, {MediaRetention: " "}} {
		if got, err := TaskMediaRetention(context.Background(), run); err == nil || got != "" {
			t.Fatalf("invalid frozen policy = %q, %v", got, err)
		}
	}
}

func TestTaskMediaRetentionPreservesHistoricalProfileSnapshot(t *testing.T) {
	newSnapshotTestDB(t)
	source, err := protocol.BuiltinPreset(protocol.PresetOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	for i := range source.Operations {
		if source.Operations[i].Operation == "video.create" {
			source.Operations[i].MediaRetention = media.RetentionBestEffort
		}
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	createSnapshotTestRevision(t, "retention-profile", 1, compiled)
	run := createSnapshotTestRun(t, "historical-retention", "retention-profile", 1, compiled.Digest())
	channel := &ChannelModel{ID: run.ChannelID, Type: "openai", BaseURL: "https://retention.example.invalid/v1", Enabled: true, MediaRetention: media.RetentionDisabled}
	if err := SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	createSnapshotTestRevision(t, "retention-profile", 2, snapshotTestProfile(t, "Current required retention"))
	if got, err := TaskMediaRetention(context.Background(), run); err != nil || got != media.RetentionBestEffort {
		t.Fatalf("historical policy before deletion = %q, %v", got, err)
	}
	if err := DeleteProtocolProfileContext(context.Background(), "retention-profile"); err != nil {
		t.Fatal(err)
	}
	stored, err := GetTaskRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := TaskMediaRetention(context.Background(), stored); err != nil || got != media.RetentionBestEffort {
		t.Fatalf("archived historical policy = %q, %v", got, err)
	}
	if stored.MediaRetention != "" {
		t.Fatalf("old blank policy was changed during deletion: %+v", stored)
	}
	var archive ProtocolProfileSnapshot
	if err := DB.First(&archive, "content_digest = ?", compiled.Digest()).Error; err != nil || archive.ContentJSON != string(compiled.CanonicalJSON()) {
		t.Fatalf("retention resolution changed canonical snapshot JSON: %+v, %v", archive, err)
	}
	stored.Operation = "images.missing"
	if _, err := TaskMediaRetention(context.Background(), stored); !errors.Is(err, ErrProfileSnapshotInvalid) {
		t.Fatalf("missing captured operation = %v", err)
	}
	stored.Operation = "video.create"
	if err := DB.Model(&archive).Update("content_json", `{}`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := TaskMediaRetention(context.Background(), stored); !errors.Is(err, ErrProfileSnapshotInvalid) {
		t.Fatalf("invalid archived snapshot = %v", err)
	}
}

func TestTaskMediaRetentionHistoricalProfileFailuresAreClosed(t *testing.T) {
	newSnapshotTestDB(t)
	for _, run := range []*TaskRun{
		{Engine: "profile", Operation: "video.create"},
		{Engine: "profile", Operation: "video.create", ProfileID: "missing", ProfileRevision: 1},
		{Engine: "profile", Operation: "video.create", ProfileDigest: strings.Repeat("a", 64)},
		{Engine: "profile", Operation: "video.create", ProfileDigest: "invalid"},
	} {
		if got, err := TaskMediaRetention(context.Background(), run); err == nil || got != "" {
			t.Fatalf("invalid historical snapshot resolved retention %q without error: %+v", got, run)
		}
	}
}

func TestProfileTaskProjectionPreservesFrozenAndHistoricalRetention(t *testing.T) {
	newSnapshotTestDB(t)
	for _, policy := range []string{"", media.RetentionDisabled, media.RetentionBestEffort, media.RetentionRequired} {
		t.Run("policy-"+policy, func(t *testing.T) {
			run := &TaskRun{ID: "frozen-" + policy, Engine: "profile", TaskKind: "video", Operation: "video.create", MediaRetention: policy, SubmissionState: "submitting"}
			if err := CreateTaskRun(run); err != nil {
				t.Fatal(err)
			}
			run.MediaRetention = media.RetentionRequired
			run.ProviderTaskID, run.SubmissionState = "accepted-id", "accepted"
			if err := UpdateProfileTaskRunProjectionContext(context.Background(), run); err != nil {
				t.Fatal(err)
			}
			stored, err := GetTaskRun(run.ID)
			if err != nil || stored.MediaRetention != policy || run.MediaRetention != policy || stored.ProviderTaskID != "accepted-id" {
				t.Fatalf("projection changed reservation policy: stored=%+v incoming=%+v err=%v", stored, run, err)
			}
		})
	}
	invalid := &TaskRun{ID: "invalid-policy", MediaRetention: "sometimes"}
	if err := CreateTaskRun(invalid); err == nil {
		t.Fatal("task creation accepted an invalid frozen policy")
	}
	encoded, err := json.Marshal(&TaskRun{MediaRetention: ""})
	if err != nil || strings.Contains(string(encoded), `"media_retention"`) {
		t.Fatalf("historical empty retention is not omitted: %s, %v", encoded, err)
	}
	encoded, err = json.Marshal(&TaskRun{MediaRetention: media.RetentionDisabled})
	if err != nil || !strings.Contains(string(encoded), `"media_retention":"disabled"`) {
		t.Fatalf("frozen disabled retention is not public: %s, %v", encoded, err)
	}
}
