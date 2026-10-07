package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
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
	compiled, err := db.LoadTaskProfileContext(ctx, run)
	if err != nil {
		return Observation{}, fmt.Errorf("load profile task snapshot: %w", err)
	}
	op, ok := profileOperation(compiled, run.Operation)
	if !ok || op.Poll == nil {
		return Observation{}, fmt.Errorf("profile operation %q has no poll definition", run.Operation)
	}
	retention, err := db.TaskMediaRetention(ctx, run)
	if err != nil {
		return Observation{}, fmt.Errorf("load task media retention: %w", err)
	}
	op.MediaRetention = retention
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
	executor := service.DefaultDispatcher.ProfileExecutor(protocol.NewHTTPExecutor(nil), run.ChannelID)
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
	result.ResultURLs = media.NormalizeMediaSources(result.ResultURLs, upstream.BaseURL)
	apply := func(write func(context.Context) error) error {
		if strings.TrimSpace(run.LeaseOwner) != "" {
			return db.WithTaskRunLeaseContext(ctx, run.ID, run.LeaseOwner, write)
		}
		return write(ctx)
	}
	// Commit the successful provider payload separately so an enqueue failure
	// remains recoverable. An enqueue callback must return its actual error;
	// that fence's whole transaction then rolls back partial media writes.
	if err := apply(func(writeCtx context.Context) error { return persistProfilePollPayload(writeCtx, run, result) }); err != nil {
		return Observation{}, err
	}
	var observation Observation
	var projectionErr error
	err = apply(func(writeCtx context.Context) error {
		observation, projectionErr = projectProfilePollResult(writeCtx, run, op, result)
		return projectionErr
	})
	if err != nil && projectionErr == nil {
		return Observation{}, err
	}
	if projectionErr != nil && op.EffectiveMediaRetention() == protocol.MediaRetentionRequired {
		return observation, projectionErr
	}
	return observation, nil
}

func persistProfilePollPayload(ctx context.Context, run *db.TaskRun, result protocol.Result) error {
	// Persist every successful provider response before projecting lifecycle
	// state. This makes restart recovery independent of the in-memory poller.
	if len(result.RawBody) > 0 {
		if err := db.UpdateTaskRunResultContext(ctx, run.ID, string(result.RawBody), false); err != nil {
			return err
		}
	} else if result.JSON != nil {
		encoded, marshalErr := json.Marshal(result.JSON)
		if marshalErr != nil {
			return marshalErr
		}
		if err := db.UpdateTaskRunResultContext(ctx, run.ID, string(encoded), false); err != nil {
			return err
		}
	}
	return nil
}

func projectProfilePollResult(ctx context.Context, run *db.TaskRun, op protocol.Operation, result protocol.Result) (Observation, error) {
	status := model.NormalizeTaskStatus(result.Status)
	observation := Observation{Status: status, HTTPStatus: result.HTTPStatus, Success: true, Outcome: "pending"}
	if containsFold(op.Poll.SuccessValues, result.Status) {
		observation.Status, observation.Outcome = model.VideoStatusCompleted, "success"
		// Required retention is part of the task contract. If the durable
		// enqueue fails, keep the task non-terminal so the poller can retry
		// without ever submitting a second provider task.
		if op.EffectiveMediaRetention() != protocol.MediaRetentionDisabled {
			var mediaErr error
			var mediaAssets []db.MediaAsset
			if len(result.ResultURLs) > 0 {
				mediaAssets, mediaErr = ensureTaskResultMedia(ctx, run.ID, run.TaskKind, result.ResultURLs)
			} else if profileContentMediaEligible(run, op) {
				// The provider status is terminal but intentionally URL-free. Keep
				// the result in processing until the media worker has fetched the
				// authenticated Content response into the local store.
				mediaAssets, mediaErr = ensureTaskContentMedia(ctx, run.ID, run.TaskKind)
			} else if op.EffectiveMediaRetention() == protocol.MediaRetentionRequired {
				mediaErr = errors.New("required media result has no source URL or content endpoint")
			}
			if mediaErr == nil && op.EffectiveMediaRetention() == protocol.MediaRetentionRequired && len(mediaAssets) == 0 {
				mediaErr = errors.New("required media result did not create any media assets")
			}
			if mediaErr != nil && op.EffectiveMediaRetention() == protocol.MediaRetentionRequired {
				observation.Status = model.VideoStatusProcessing
				observation.Outcome = "pending"
				observation.Success = false
				observation.Error = fmt.Sprintf("required media materialization enqueue failed: %v", mediaErr)
				observation.NextPollAt = time.Now().Add(op.Poll.NextDelay(run.PollCount+1, result.Headers.Get("Retry-After"), nil))
				return observation, mediaErr
			}
			if mediaErr != nil {
				return observation, mediaErr
			}
			if mediaErr == nil && op.EffectiveMediaRetention() == protocol.MediaRetentionRequired && len(mediaAssets) > 0 {
				allAvailable := true
				for _, asset := range mediaAssets {
					if asset.Status != db.MediaAssetAvailable || strings.TrimSpace(asset.ObjectID) == "" {
						allAvailable = false
						break
					}
				}
				if allAvailable {
					return observation, nil
				}
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
