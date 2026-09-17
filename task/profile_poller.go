package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

// NewProfileBackgroundPoller builds the durable worker used for Profile tasks
// created with polling_mode=background. The worker never submits a new task;
// it only polls the provider task captured in TaskRun.
func NewProfileBackgroundPoller(owner string) *BackgroundPoller {
	return &BackgroundPoller{Owner: owner, Workers: 2, RetryAfter: 5 * time.Second, Poll: ProfilePoll}
}

// ensureTaskResultMedia is a narrow seam for the durable enqueue operation.
// Keeping it replaceable makes failure handling testable without introducing
// provider or storage mocks into the Profile poller itself.
var ensureTaskResultMedia = db.EnsureTaskResultMediaContext

// ensureTaskContentMedia is the companion path for providers whose completed
// task response has no downloadable URL and requires the operation's
// authenticated Content endpoint instead (for example OpenAI-style video
// tasks). It remains replaceable so enqueue failures are testable without a
// provider or storage dependency.
var ensureTaskContentMedia = db.EnsureTaskContentMediaContext

func ProfilePoll(ctx context.Context, run *db.TaskRun) (Observation, error) {
	if run == nil || strings.TrimSpace(run.ProfileID) == "" || run.ProfileRevision <= 0 {
		return Observation{}, errors.New("profile task snapshot is incomplete")
	}
	revision, err := db.GetProtocolProfileRevisionContext(ctx, run.ProfileID, run.ProfileRevision)
	if err != nil {
		return Observation{}, fmt.Errorf("load profile revision: %w", err)
	}
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		return Observation{}, fmt.Errorf("decode profile revision: %w", err)
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		return Observation{}, fmt.Errorf("compile profile revision: %w", err)
	}
	op, ok := profileOperation(compiled, run.Operation)
	if !ok || op.Poll == nil {
		return Observation{}, fmt.Errorf("profile operation %q has no poll definition", run.Operation)
	}
	if observation, exceeded := profilePollBudget(run, op); exceeded {
		return observation, nil
	}
	channel, err := db.GetChannelModelContext(ctx, run.ChannelID)
	if err != nil {
		return Observation{}, fmt.Errorf("load channel: %w", err)
	}
	if strings.TrimSpace(run.ProviderTaskID) == "" {
		return Observation{}, errors.New("profile task provider ID is missing")
	}
	upstream := channel.ToUpstreamChannel()
	executor := protocol.NewHTTPExecutor(nil)
	result, err := executor.PollOnce(ctx, compiled, run.Operation, protocol.Request{
		BaseURL: upstream.BaseURL, APIKeys: upstream.GetEffectiveKeys(), Headers: upstream.Headers, TaskID: run.ProviderTaskID,
	})
	if err != nil {
		observation := Observation{}
		var executorErr *protocol.ExecutorError
		if errors.As(err, &executorErr) {
			observation.HTTPStatus = executorErr.HTTPStatus
		}
		observation.NextPollAt = time.Now().Add(op.Poll.NextDelay(run.PollCount+1, result.Headers.Get("Retry-After"), nil))
		return observation, err
	}
	if strings.EqualFold(strings.TrimSpace(run.TaskKind), "image") {
		result.ResultURLs = media.NormalizeMediaSources(result.ResultURLs, upstream.BaseURL)
	}
	// Persist every successful provider response before projecting lifecycle
	// state. This makes restart recovery independent of the in-memory poller.
	if len(result.RawBody) > 0 {
		_ = db.UpdateTaskRunResultContext(ctx, run.ID, string(result.RawBody), false)
	} else if result.JSON != nil {
		if encoded, marshalErr := json.Marshal(result.JSON); marshalErr == nil {
			_ = db.UpdateTaskRunResultContext(ctx, run.ID, string(encoded), false)
		}
	}
	status := model.NormalizeTaskStatus(result.Status)
	observation := Observation{Status: status, HTTPStatus: result.HTTPStatus, Success: true, Outcome: "pending"}
	if containsFold(op.Poll.SuccessValues, result.Status) {
		observation.Status, observation.Outcome = model.VideoStatusCompleted, "success"
		// Required retention is part of the task contract. If the durable
		// enqueue fails, keep the task non-terminal so the poller can retry
		// without ever submitting a second provider task.
		if os.Getenv("RELAY_PROFILE_MEDIA_DISABLED") != "1" && op.EffectiveMediaRetention() != protocol.MediaRetentionDisabled {
			var mediaErr error
			var mediaAssets []db.MediaAsset
			if len(result.ResultURLs) > 0 {
				mediaAssets, mediaErr = ensureTaskResultMedia(ctx, run.ID, run.TaskKind, result.ResultURLs)
			} else if profileContentMediaEligible(run, op) {
				// The provider status is terminal but intentionally URL-free. Keep
				// the result in processing until the media worker has fetched the
				// authenticated Content response into the local store.
				mediaAssets, mediaErr = ensureTaskContentMedia(ctx, run.ID, run.TaskKind)
			}
			if mediaErr != nil && op.EffectiveMediaRetention() == protocol.MediaRetentionRequired {
				observation.Status = model.VideoStatusProcessing
				observation.Outcome = "pending"
				observation.Success = false
				observation.Error = fmt.Sprintf("required media materialization enqueue failed: %v", mediaErr)
				observation.NextPollAt = time.Now().Add(op.Poll.NextDelay(run.PollCount+1, result.Headers.Get("Retry-After"), nil))
				return observation, mediaErr
			}
			if mediaErr == nil && op.EffectiveMediaRetention() == protocol.MediaRetentionRequired && len(mediaAssets) > 0 {
				// The provider is already terminal. Move to a distinct local-media
				// state so the background poller does not query the provider again
				// while the materializer owns the remaining work.
				observation.Status = "materializing"
				observation.Outcome = "pending"
				observation.PausePolling = true
			}
		}
	} else if containsFold(op.Poll.FailureValues, result.Status) {
		observation.Status, observation.Outcome, observation.Success = model.VideoStatusFailed, "failed", false
	}
	if observation.Status != model.VideoStatusCompleted && observation.Status != model.VideoStatusFailed {
		observation.NextPollAt = time.Now().Add(op.Poll.NextDelay(run.PollCount+1, result.Headers.Get("Retry-After"), nil))
	}
	return observation, nil
}

// profileContentMediaEligible reports whether a successful URL-free result
// can still be materialized through the operation's authenticated content
// endpoint. The fallback is deliberately limited to video-like TaskRuns;
// image APIs normally return their assets in the poll response itself.
func profileContentMediaEligible(run *db.TaskRun, op protocol.Operation) bool {
	if run == nil || !strings.EqualFold(strings.TrimSpace(run.TaskKind), "video") || op.Content == nil {
		return false
	}
	return strings.TrimSpace(op.Content.Path) != ""
}

// profilePollBudget stops a durable worker before it makes another provider
// request once the immutable operation budget has been consumed. The returned
// observation is terminal but explicitly skips poll accounting because no
// provider request was made; the caller still persists the state transition so
// an expired task cannot be picked up again.
func profilePollBudget(run *db.TaskRun, op protocol.Operation) (Observation, bool) {
	if run == nil || op.Poll == nil {
		return Observation{}, false
	}
	if run.PollCount >= op.Poll.MaxAttempts {
		return Observation{Status: "expired", Outcome: "failed", Success: false, Error: "profile task polling attempt limit exceeded", SkipPollAccounting: true}, true
	}
	if run.DeadlineAt != nil && !time.Now().Before(*run.DeadlineAt) {
		return Observation{Status: "expired", Outcome: "failed", Success: false, Error: "profile task polling deadline exceeded", SkipPollAccounting: true}, true
	}
	return Observation{}, false
}

func profileOperation(compiled protocol.CompiledProfile, name string) (protocol.Operation, bool) {
	for _, op := range compiled.Profile().Operations {
		if op.Operation == name {
			return op, true
		}
	}
	return protocol.Operation{}, false
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}
