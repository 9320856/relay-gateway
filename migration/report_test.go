package migration

import (
	"context"
	"testing"
	"time"

	"relay-gateway/db"
	"relay-gateway/protocol"
)

func TestAuditChannelsReportsGenericLegacyGate(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-channels.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "")
	channel := &db.ChannelModel{ID: "migration-openai", Name: "OpenAI", Type: "openai", BaseURL: "https://openai.example/v1", Enabled: true, ModelsRaw: "gpt-4o"}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTaskRun(&db.TaskRun{ID: "migration-openai-legacy", TaskKind: "video", Operation: "video.create", ChannelID: channel.ID, Engine: "legacy", TaskStatus: "processing"}); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Create(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "chat.completions", ModelPattern: "*",
		ProfileID: "missing-profile", ProfileRevision: 1, Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTaskRun(&db.TaskRun{
		ID: "migration-openai-profile", TaskKind: "video", Operation: "video.create", ChannelID: channel.ID,
		Engine: "profile", ProfileID: "missing-profile", ProfileRevision: 1,
		ProviderTaskID: "provider-task", TaskStatus: "processing",
	}); err != nil {
		t.Fatal(err)
	}
	report, err := AuditChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || report.EnabledChannels != 1 || report.EnabledChannelsWithoutBinding != 0 || report.NonTerminalLegacyTaskRuns != 1 || report.BindingsMissingRevision != 1 || report.ProfileTasksMissingRevision != 1 || report.ProfileTasksMissingDigest != 1 {
		t.Fatalf("unexpected generic migration report = %+v", report)
	}
	if len(report.Channels) != 1 || report.Channels[0].Type != "openai" {
		t.Fatalf("unexpected channel summaries = %+v", report.Channels)
	}
	if len(report.Blockers) < 3 {
		t.Fatalf("expected generic blocker explanations, got %v", report.Blockers)
	}
}

func TestAuditChannelsBlocksOperationGapsWhenLegacyDisabled(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-channels-operation-gap.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	channel := &db.ChannelModel{ID: "migration-openai-operation-gap", Name: "OpenAI", Type: "openai", BaseURL: "https://openai.example/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	// One operation is covered, but the OpenAI preset also declares Responses,
	// Embeddings, Audio, and Images. A global Legacy shutdown must reject this
	// partial channel migration instead of reporting it ready.
	if err := db.DB.Create(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "chat.completions", ModelPattern: "*",
		ProfileID: "profile-openai-operation-gap", ProfileRevision: 1, Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	preset, err := protocol.BuiltinPreset(protocol.PresetOpenAI)
	if err != nil {
		t.Fatal(err)
	}
	report, err := AuditChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	expectedMissing := int64(len(preset.Operations) - 1)
	if report.Ready || report.OperationsMissingBinding != expectedMissing || len(report.Channels) != 1 || report.Channels[0].OperationsMissingBinding != expectedMissing {
		t.Fatalf("operation gap was not reported as a cutover blocker: %+v", report)
	}
	found := false
	for _, blocker := range report.Blockers {
		if blocker == "enabled channels are missing Profile bindings for one or more operations" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing operation-coverage blocker: %v", report.Blockers)
	}
}

func TestAuditChannelsEmptyDeploymentReadyWhenLegacySwitchOff(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-channels-empty.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	report, err := AuditChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Ready || report.EnabledChannels != 0 || len(report.Blockers) != 0 {
		t.Fatalf("empty generic report = %+v", report)
	}
}

func TestAuditChannelsBlocksPendingMedia(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-channels-media.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	channel := &db.ChannelModel{ID: "migration-openai-media", Name: "OpenAI", Type: "openai", BaseURL: "https://openai.example/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Create(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "chat.completions", ModelPattern: "*",
		ProfileID: "profile-openai-media", ProfileRevision: 1, Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Create(&db.ProtocolProfileRevision{
		ProfileID: "profile-openai-media", Revision: 1, ContentJSON: "{}", ContentDigest: "digest", State: db.ProfileRevisionPublished,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTaskRun(&db.TaskRun{
		ID: "migration-openai-media-task", TaskKind: "image", Operation: "images.generations", ChannelID: channel.ID,
		Engine: "profile", ProfileID: "profile-openai-media", ProfileRevision: 1, ProfileDigest: "digest",
		ProviderTaskID: "provider-openai-media-task", TaskStatus: "completed",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMediaAsset(&db.MediaAsset{
		PublicID: "migration-openai-pending", CapabilityHash: "migration-openai-pending-hash",
		TaskRunID: "migration-openai-media-task", Kind: "image", Ordinal: 0, Status: db.MediaAssetMaterializing,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := AuditChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || report.MediaPending != 1 {
		t.Fatalf("pending generic media was reported ready: %+v", report)
	}
	found := false
	for _, blocker := range report.Blockers {
		if blocker == "media assets are still pending materialization" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing pending-media blocker: %v", report.Blockers)
	}
}

func TestAuditChannelsBlocksIdempotencyConflicts(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-idempotency.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	channel := &db.ChannelModel{ID: "migration-openai-idempotency", Name: "OpenAI", Type: "openai", BaseURL: "https://openai.example/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Create(&db.ChannelProtocolBinding{
		ChannelID: channel.ID, Operation: "video.create", ModelPattern: "*",
		ProfileID: "profile-openai-idempotency", ProfileRevision: 1, Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Create(&db.ProtocolProfileRevision{
		ProfileID: "profile-openai-idempotency", Revision: 1, ContentJSON: "{}", ContentDigest: "digest", State: db.ProfileRevisionPublished,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"migration-idempotency-a", "migration-idempotency-b"} {
		if err := db.CreateTaskRun(&db.TaskRun{
			ID: id, TaskKind: "video", Operation: "video.create", ChannelID: channel.ID,
			Engine: "profile", ProfileID: "profile-openai-idempotency", ProfileRevision: 1, ProfileDigest: "digest",
			IdempotencyKey: "same-client-key", RequestFingerprint: "same-fingerprint",
			ProviderTaskID: "provider-" + id, TaskStatus: "completed",
		}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := AuditChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || report.IdempotencyConflicts != 1 {
		t.Fatalf("idempotency conflict was reported ready: %+v", report)
	}
	found := false
	for _, blocker := range report.Blockers {
		if blocker == "Profile idempotency keys resolve to multiple TaskRuns" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing idempotency blocker: %v", report.Blockers)
	}
}

func TestAuditChannelsBlocksMissingPrimaryLogsAndProviderTaskConflicts(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-reconciliation.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	channel := &db.ChannelModel{ID: "migration-openai-reconciliation", Name: "OpenAI", Type: "openai", BaseURL: "https://openai.example/v1", Enabled: true}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Create(&db.RequestLogModel{ID: "migration-primary-log", Kind: "api_call", StartedAt: time.Now(), Method: "POST", Path: "/v1/videos", Outcome: "success"}).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"migration-reconciliation-a", "migration-reconciliation-b"} {
		originRequestID := ""
		if id == "migration-reconciliation-a" {
			originRequestID = "migration-primary-log"
		}
		if err := db.CreateTaskRun(&db.TaskRun{
			ID: id, OriginRequestID: originRequestID, TaskKind: "video", Operation: "video.create", ChannelID: channel.ID,
			Engine: "profile", ProfileID: "profile-reconciliation", ProfileRevision: 1,
			ProfileDigest: "digest", ProviderTaskID: "same-provider-task", TaskStatus: "completed", TaskOutcome: "success",
		}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := AuditChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || report.ProfileTasksMissingPrimaryLog != 1 || report.ProviderTaskIDConflicts != 1 {
		t.Fatalf("reconciliation defects were not reported: %+v", report)
	}
	if len(report.Channels) != 1 || report.Channels[0].ProfileTasksMissingPrimaryLog != 1 || report.Channels[0].ProviderTaskIDConflicts != 1 {
		t.Fatalf("channel reconciliation summary = %+v", report.Channels)
	}
	for _, want := range []string{
		"Profile tasks are missing their Primary request log",
		"Profile provider task IDs conflict within a channel and task kind",
	} {
		found := false
		for _, blocker := range report.Blockers {
			if blocker == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing reconciliation blocker %q: %v", want, report.Blockers)
		}
	}
}

func TestAuditChannelsReconcilesCreationLogsByChannelAndTaskKind(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-create-reconciliation.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	channels := []*db.ChannelModel{
		{ID: "migration-reconcile-channel-a", Name: "OpenAI A", Type: "openai", BaseURL: "https://openai-a.example/v1", Enabled: true},
		{ID: "migration-reconcile-channel-b", Name: "OpenAI B", Type: "openai", BaseURL: "https://openai-b.example/v1", Enabled: true},
	}
	for _, channel := range channels {
		if err := db.SaveChannelModel(channel); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DB.Create(&db.ProtocolProfileRevision{
		ProfileID: "migration-reconcile-profile", Revision: 1, SchemaVersion: 1,
		ContentJSON: "{}", ContentDigest: "migration-reconcile-digest", State: db.ProfileRevisionPublished,
	}).Error; err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	logs := []db.RequestLogModel{
		{ID: "migration-reconcile-log-a1", Kind: "api_call", StartedAt: started, Method: "POST", Path: "/v1/videos", ChannelID: channels[0].ID, ChannelType: "openai", AsyncTaskKind: "video", AsyncTaskID: "shared-provider-id", Outcome: "success"},
		{ID: "migration-reconcile-log-a2", Kind: "api_call", StartedAt: started.Add(time.Millisecond), Method: "POST", Path: "/v1/videos", ChannelID: channels[0].ID, ChannelType: "openai", AsyncTaskKind: "video", AsyncTaskID: "shared-provider-id", Outcome: "success"},
		// The same provider ID on another channel is valid because providers
		// commonly scope IDs to the channel credential set.
		{ID: "migration-reconcile-log-b1", Kind: "api_call", StartedAt: started.Add(2 * time.Millisecond), Method: "POST", Path: "/v1/videos", ChannelID: channels[1].ID, ChannelType: "openai", AsyncTaskKind: "video", AsyncTaskID: "shared-provider-id", Outcome: "success"},
	}
	for i := range logs {
		if err := db.DB.Create(&logs[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	runs := []*db.TaskRun{
		{ID: "migration-reconcile-run-a1", OriginRequestID: logs[0].ID, TaskKind: "video", Operation: "video.create", ChannelID: channels[0].ID, Engine: "profile", ProfileID: "migration-reconcile-profile", ProfileRevision: 1, ProfileDigest: "migration-reconcile-digest", ProviderTaskID: "shared-provider-id", TaskStatus: "completed", TaskOutcome: "success"},
		// This run deliberately points at the first creation log but has a
		// different provider ID, proving that an existing origin alone is not
		// sufficient for reconciliation.
		{ID: "migration-reconcile-run-a2", OriginRequestID: logs[0].ID, TaskKind: "video", Operation: "video.create", ChannelID: channels[0].ID, Engine: "profile", ProfileID: "migration-reconcile-profile", ProfileRevision: 1, ProfileDigest: "migration-reconcile-digest", ProviderTaskID: "different-provider-id", TaskStatus: "completed", TaskOutcome: "success"},
		{ID: "migration-reconcile-run-b1", OriginRequestID: logs[2].ID, TaskKind: "video", Operation: "video.create", ChannelID: channels[1].ID, Engine: "profile", ProfileID: "migration-reconcile-profile", ProfileRevision: 1, ProfileDigest: "migration-reconcile-digest", ProviderTaskID: "shared-provider-id", TaskStatus: "completed", TaskOutcome: "success"},
	}
	for _, run := range runs {
		if err := db.CreateTaskRun(run); err != nil {
			t.Fatal(err)
		}
	}
	report, err := AuditChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ProfileCreateRequests != 3 || report.ProfileCreateRequestsWithoutTaskRun != 0 {
		t.Fatalf("unexpected create request reconciliation: %+v", report)
	}
	if report.ProfilePrimaryLogConflicts != 1 || report.ProfileTasksCreateLogMismatches != 1 {
		t.Fatalf("unexpected Primary/create mismatch counts: %+v", report)
	}
	if report.ProviderTaskIDConflicts != 0 {
		t.Fatalf("cross-channel provider ID was incorrectly treated as a conflict: %+v", report)
	}
	if report.Ready {
		t.Fatalf("duplicate/mismatched creation logs were not blocking: %+v", report.Blockers)
	}
}

func TestAuditChannelsRejectsTaskRunOriginFromAnotherChannel(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/migration-cross-channel-origin.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Setenv("RELAY_DISABLE_LEGACY", "1")
	for _, channel := range []*db.ChannelModel{
		{ID: "migration-origin-channel-a", Name: "OpenAI A", Type: "openai", BaseURL: "https://openai-a.example/v1", Enabled: true},
		{ID: "migration-origin-channel-b", Name: "OpenAI B", Type: "openai", BaseURL: "https://openai-b.example/v1", Enabled: true},
	} {
		if err := db.SaveChannelModel(channel); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DB.Create(&db.RequestLogModel{
		ID: "migration-cross-channel-origin-log", Kind: "api_call", StartedAt: time.Now(), Method: "POST", Path: "/v1/videos",
		ChannelID: "migration-origin-channel-a", ChannelType: "openai", AsyncTaskKind: "video", AsyncTaskID: "cross-channel-task", Outcome: "success",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTaskRun(&db.TaskRun{
		ID: "migration-cross-channel-origin-run", OriginRequestID: "migration-cross-channel-origin-log", TaskKind: "video", Operation: "video.create",
		ChannelID: "migration-origin-channel-b", Engine: "profile", ProfileID: "migration-cross-channel-profile", ProfileRevision: 1,
		ProfileDigest: "digest", ProviderTaskID: "cross-channel-task", TaskStatus: "completed", TaskOutcome: "success",
	}); err != nil {
		t.Fatal(err)
	}
	report, err := AuditChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.ProfileTasksCreateLogMismatches != 1 || report.Ready {
		t.Fatalf("cross-channel origin was not rejected: %+v", report)
	}
}
