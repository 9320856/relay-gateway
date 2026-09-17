package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

func profileTaskStatus(c *gin.Context, lookupID, kind string) (any, bool, error) {
	if c == nil || strings.TrimSpace(lookupID) == "" {
		return nil, false, nil
	}
	run, err := db.GetTaskRunByAliasContext(c.Request.Context(), lookupID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if run.Engine != "profile" {
		return nil, false, nil
	}
	if strings.TrimSpace(kind) != "" && !strings.EqualFold(strings.TrimSpace(run.TaskKind), strings.TrimSpace(kind)) {
		// An alias must never make an image route serve a video TaskRun (or
		// vice versa), even if a legacy database contains a duplicate lookup ID.
		return nil, false, nil
	}
	if run.PollingMode == protocol.PollingBackground {
		if handled, latest := profileRequiredMediaStatus(c, run, kind); handled {
			response := profileDurableStatus(c, latest, kind, lookupID)
			markMappedAsyncTaskPoll(c, lookupID, kind, normalizeProfileStatus(latest.TaskStatus), response)
			return response, true, nil
		}
		response := profileDurableStatus(c, run, kind, lookupID)
		markMappedAsyncTaskPoll(c, lookupID, kind, normalizeProfileStatus(run.TaskStatus), response)
		return response, true, nil
	}
	if isTaskTerminalForProjection(run.TaskStatus) {
		if handled, latest := profileRequiredMediaStatus(c, run, kind); handled {
			response := profileDurableStatus(c, latest, kind, lookupID)
			markMappedAsyncTaskPoll(c, lookupID, kind, normalizeProfileStatus(latest.TaskStatus), response)
			return response, true, nil
		}
		response := profileDurableStatus(c, run, kind, lookupID)
		// A terminal durable read can enrich the primary audit row after local
		// media materialization completes, without dispatching another provider
		// request. This keeps the final managed URL visible in task details.
		markMappedAsyncTaskPoll(c, lookupID, kind, normalizeProfileStatus(run.TaskStatus), response)
		return response, true, nil
	}
	if handled, latest := profileRequiredMediaStatus(c, run, kind); handled {
		response := profileDurableStatus(c, latest, kind, lookupID)
		markMappedAsyncTaskPoll(c, lookupID, kind, normalizeProfileStatus(latest.TaskStatus), response)
		return response, true, nil
	}
	claimed, claimErr := db.ClaimTaskRunPollContext(c.Request.Context(), run.ID, "profile-client", 2*time.Minute)
	if errors.Is(claimErr, db.ErrTaskLeaseUnavailable) {
		latest, latestErr := db.GetTaskRunByAliasContext(c.Request.Context(), lookupID)
		if latestErr != nil {
			return nil, true, latestErr
		}
		response := profileDurableStatus(c, latest, kind, lookupID)
		markMappedAsyncTaskPoll(c, lookupID, kind, normalizeProfileStatus(latest.TaskStatus), response)
		return response, true, nil
	}
	if claimErr != nil {
		return nil, true, claimErr
	}
	run = claimed
	defer func() { _ = db.ReleaseTaskRunLease(run.ID, "profile-client") }()
	if budgetReason, exceeded := profileClientPollBudget(run, profileOperationSnapshot(c.Request.Context(), run)); exceeded {
		_ = db.UpdateTaskRunStatusForLease(run.ID, "profile-client", "expired", "failed")
		run.TaskStatus, run.TaskOutcome, run.TaskError = "expired", "failed", budgetReason
		response := profileDurableStatus(c, run, kind, lookupID)
		markMappedAsyncTaskPoll(c, lookupID, kind, "expired", response)
		return response, true, nil
	}
	pollStarted := time.Now()
	result, op, err := profilePollOnce(c.Request.Context(), run)
	pollFinished := time.Now()
	if err != nil {
		if recordErr := db.RecordTaskPollForLease(run.ID, "profile-client", false, result.HTTPStatus); recordErr == nil {
			appendProfilePollAttempt(c.Request.Context(), run.ID, pollStarted, pollFinished, result.HTTPStatus, "failed", err.Error())
			if latest, loadErr := db.GetTaskRunContext(c.Request.Context(), run.ID); loadErr == nil {
				if reason, exceeded := profileClientPollBudget(latest, op); exceeded {
					if statusErr := db.UpdateTaskRunStatusForLease(run.ID, "profile-client", "expired", "failed"); statusErr == nil {
						_, _ = db.AppendTaskEvent(c.Request.Context(), run.ID, "budget_exhausted", reason)
					}
				}
			}
		}
		return nil, true, err
	}
	if result.TaskID == "" {
		result.TaskID = run.ProviderTaskID
	}
	status := model.NormalizeTaskStatus(result.Status)
	if status == "" {
		status = normalizeProfileStatus(run.TaskStatus)
	}
	outcome := "pending"
	success := true
	if op.Poll != nil && containsProfileFold(op.Poll.SuccessValues, result.Status) {
		status, outcome = model.VideoStatusCompleted, "success"
	} else if op.Poll != nil && containsProfileFold(op.Poll.FailureValues, result.Status) {
		status, outcome, success = model.VideoStatusFailed, "failed", false
	}
	providerSucceeded := outcome == "success"
	mediaExpected := len(result.ResultURLs) > 0 || profileContentMediaEligible(op, run.TaskKind)
	requiredMedia := providerSucceeded && mediaExpected && op.EffectiveMediaRetention() == protocol.MediaRetentionRequired
	var mediaAssets []db.MediaAsset
	var mediaErr error
	if outcome == "success" && profileMediaRetentionEnabled(op) {
		if len(result.ResultURLs) > 0 {
			mediaAssets, mediaErr = ensureProfileTaskResultMedia(c.Request.Context(), run.ID, run.TaskKind, result.ResultURLs)
		} else if profileContentMediaEligible(op, run.TaskKind) {
			mediaAssets, mediaErr = ensureProfileTaskContentMedia(c.Request.Context(), run.ID, run.TaskKind)
		}
	}
	if len(result.RawBody) > 0 {
		_ = db.UpdateTaskRunResultContext(c.Request.Context(), run.ID, string(result.RawBody), false)
	} else if result.JSON != nil {
		if encoded, encodeErr := json.Marshal(result.JSON); encodeErr == nil {
			_ = db.UpdateTaskRunResultContext(c.Request.Context(), run.ID, string(encoded), false)
		}
	}
	// A required-media result is not terminal until every local asset is
	// available. The provider result is already durable above, so later status
	// reads can retry local materialization without submitting or polling
	// upstream again.
	if requiredMedia && (mediaErr != nil || !profileMediaAssetsAvailable(mediaAssets)) {
		status, outcome = model.VideoStatusProcessing, "pending"
		if mediaErr != nil {
			success = false
		}
	}
	if recordErr := db.RecordTaskPollForLease(run.ID, "profile-client", success, result.HTTPStatus); recordErr == nil {
		appendProfilePollAttempt(c.Request.Context(), run.ID, pollStarted, pollFinished, result.HTTPStatus, pollAttemptOutcome(success, outcome), profilePollAttemptError(mediaErr, outcome))
	}
	previousStatus := run.TaskStatus
	if status != "" {
		if statusErr := db.UpdateTaskRunStatusForLease(run.ID, "profile-client", status, outcome); statusErr == nil && !strings.EqualFold(strings.TrimSpace(previousStatus), strings.TrimSpace(status)) {
			_, _ = db.AppendTaskEvent(c.Request.Context(), run.ID, "status_changed", status)
		}
	}
	if kind == asyncTaskKindImage {
		response := profileImageResponse(result, true)
		if lookupID != "" {
			cleanLookupID := strings.TrimPrefix(lookupID, imageTaskIDPrefix)
			setImageJobResponseIDs(response, cleanLookupID)
		}
		managed := attachProfileManagedMedia(c, run, kind, response)
		if requiredMedia && (!managed || mediaErr != nil) {
			clearProfileMediaPayload(response)
			response["status"] = "materializing"
			if requiredMedia && mediaErr != nil {
				response["error"] = profileMediaErrorMessage(mediaErr)
			}
		}
		markMappedAsyncTaskPoll(c, lookupID, kind, status, response)
		return response, true, nil
	}
	response := profileVideoResponse(result, true)
	if lookupID != "" {
		response.ID = lookupID
		response.TaskID = lookupID
	}
	managed := attachProfileManagedMedia(c, run, kind, response)
	if requiredMedia && (!managed || mediaErr != nil) {
		clearProfileMediaPayload(response)
		response.Status = "materializing"
		if requiredMedia && mediaErr != nil {
			response.Error = profileMediaErrorMessage(mediaErr)
		}
	}
	markMappedAsyncTaskPoll(c, lookupID, kind, status, response)
	return response, true, nil
}

// profileRequiredMediaStatus keeps a provider-completed task out of the
// provider Poll loop while required local assets are pending. For every
// retention-enabled operation, status requests may also enqueue the same
// idempotent asset job from the frozen provider result. The media worker owns
// retrying the source download, so this never needs another provider poll.
func profileRequiredMediaStatus(c *gin.Context, run *db.TaskRun, kind string) (bool, *db.TaskRun) {
	if c == nil || run == nil {
		return false, run
	}
	op, ok := profileOperationForRun(c.Request.Context(), run)
	if !ok || !profileMediaRetentionEnabled(op) {
		return false, run
	}
	assets, err := db.ListMediaAssetsForTaskRunContext(c.Request.Context(), run.ID, kind)
	if err == nil && len(assets) > 0 {
		allReady := true
		for _, asset := range assets {
			if asset.Status != db.MediaAssetAvailable || strings.TrimSpace(asset.ObjectID) == "" {
				allReady = false
				break
			}
		}
		if allReady {
			_ = db.CompleteTaskRunAfterMediaContext(c.Request.Context(), run.ID, kind)
			if latest, loadErr := db.GetTaskRunContext(c.Request.Context(), run.ID); loadErr == nil {
				return true, latest
			}
		}
		return true, run
	}
	urls := profileResultURLs(run.ResultBody, kind)
	if len(urls) > 0 {
		_, _ = ensureProfileTaskResultMedia(c.Request.Context(), run.ID, kind, urls)
		return true, run
	}
	if profileContentMediaEligible(op, kind) && (strings.EqualFold(strings.TrimSpace(run.TaskStatus), model.VideoStatusCompleted) || profileResultIndicatesSuccess(run.ResultBody, op)) {
		_, _ = ensureProfileTaskContentMedia(c.Request.Context(), run.ID, kind)
		return true, run
	}
	return false, run
}

func profileResultURLs(resultBody, kind string) []string {
	if strings.TrimSpace(resultBody) == "" {
		return nil
	}
	var payload any
	if json.Unmarshal([]byte(resultBody), &payload) != nil {
		return nil
	}
	keys := map[string]struct{}{"url": {}, "video_url": {}, "proxy_url": {}}
	if kind == asyncTaskKindImage {
		keys = map[string]struct{}{"url": {}, "image_url": {}, "proxy_url": {}, "thumbnail_url": {}, "b64_json": {}}
	}
	return collectProfileResultURLs(payload, keys)
}

func collectProfileResultURLs(value any, keys map[string]struct{}) []string {
	seen := make(map[string]struct{})
	urls := make([]string, 0)
	var visit func(any)
	visit = func(current any) {
		switch typed := current.(type) {
		case []any:
			for _, item := range typed {
				visit(item)
			}
		case map[string]any:
			// Prefer the conventional media keys in a stable order. JSON object
			// map iteration is intentionally random in Go, and the URL order is
			// also the media ordinal order for multi-image results.
			preferredCount := len(urls)
			for _, key := range []string{"video_url", "image_url", "url", "proxy_url", "b64_json"} {
				if _, selected := keys[key]; !selected {
					continue
				}
				if item, exists := typed[key]; exists {
					collectProfileResultStrings(item, &urls, seen)
				}
			}
			// A thumbnail is a usable preview only when the same media object did
			// not expose a full-size source.
			if len(urls) == preferredCount {
				if _, selected := keys["thumbnail_url"]; selected {
					if item, exists := typed["thumbnail_url"]; exists {
						collectProfileResultStrings(item, &urls, seen)
					}
				}
			}
			for key, item := range typed {
				if _, selected := keys[strings.ToLower(strings.TrimSpace(key))]; selected {
					continue
				}
				// Continue through envelopes such as job.assets and data[] so
				// callers can recover a URL after a failed enqueue/restart.
				switch item.(type) {
				case map[string]any, []any:
					visit(item)
				}
			}
		}
	}
	visit(value)
	return urls
}

func collectProfileResultStrings(value any, urls *[]string, seen map[string]struct{}) {
	switch typed := value.(type) {
	case string:
		candidate := strings.TrimSpace(typed)
		if candidate == "" || isManagedMediaURL(candidate) {
			return
		}
		if _, exists := seen[candidate]; !exists {
			seen[candidate] = struct{}{}
			*urls = append(*urls, candidate)
		}
	case []any:
		for _, item := range typed {
			collectProfileResultStrings(item, urls, seen)
		}
	}
}

func profileResultIndicatesSuccess(resultBody string, op protocol.Operation) bool {
	if strings.TrimSpace(resultBody) == "" || op.Poll == nil {
		return false
	}
	var payload any
	if json.Unmarshal([]byte(resultBody), &payload) != nil {
		return false
	}
	status := profileJSONValueString(profileSelectJSON(payload, op.Poll.StatusPath))
	if status == "" {
		status = profileJSONValueString(profileSelectJSON(payload, "status"))
	}
	return containsProfileFold(op.Poll.SuccessValues, status)
}

func profileSelectJSON(value any, selector string) any {
	if value == nil || strings.TrimSpace(selector) == "" {
		return nil
	}
	current := value
	for _, part := range strings.Split(selector, ".") {
		switch typed := current.(type) {
		case map[string]any:
			current = typed[part]
		case []any:
			var index int
			if _, err := fmt.Sscanf(part, "%d", &index); err != nil || index < 0 || index >= len(typed) {
				return nil
			}
			current = typed[index]
		default:
			return nil
		}
	}
	return current
}

func profileJSONValueString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func appendProfilePollAttempt(ctx context.Context, taskRunID string, started, finished time.Time, httpStatus int, outcome, attemptError string) {
	if finished.IsZero() {
		finished = time.Now()
	}
	_ = db.AppendTaskAttemptContext(ctx, &db.TaskAttempt{TaskRunID: taskRunID, AttemptType: "poll", StartedAt: started, FinishedAt: &finished, HTTPStatus: httpStatus, Outcome: outcome, Error: attemptError})
}

func pollAttemptOutcome(success bool, outcome string) string {
	if success && outcome == "success" {
		return "success"
	}
	if success {
		return "pending"
	}
	return "failed"
}

func profilePollAttemptError(mediaErr error, outcome string) string {
	if mediaErr != nil && outcome == "pending" {
		return profileMediaErrorMessage(mediaErr)
	}
	return ""
}

func profileMediaAssetsAvailable(assets []db.MediaAsset) bool {
	if len(assets) == 0 {
		return false
	}
	for _, asset := range assets {
		if asset.Status != db.MediaAssetAvailable || strings.TrimSpace(asset.ObjectID) == "" {
			return false
		}
	}
	return true
}

// ensureProfileTaskResultMedia is replaceable in package tests so the
// client-poll state transition can be verified independently of SQLite.
var ensureProfileTaskResultMedia = db.EnsureTaskResultMediaContext

// ensureProfileTaskContentMedia is the durable enqueue seam for video APIs
// whose completed status is URL-free and whose bytes are exposed by the
// operation's authenticated Content endpoint.
var ensureProfileTaskContentMedia = db.EnsureTaskContentMediaContext

func profileContentMediaEligible(op protocol.Operation, kind string) bool {
	if !strings.EqualFold(strings.TrimSpace(kind), asyncTaskKindVideo) || op.Content == nil {
		return false
	}
	return strings.TrimSpace(op.Content.Path) != ""
}

func profileOperationSnapshot(ctx context.Context, run *db.TaskRun) protocol.Operation {
	op, _ := profileOperationForRun(ctx, run)
	return op
}

func profileClientPollBudget(run *db.TaskRun, op protocol.Operation) (string, bool) {
	if run == nil || op.Poll == nil {
		return "", false
	}
	if run.PollCount >= op.Poll.MaxAttempts {
		return "profile task polling attempt limit exceeded", true
	}
	if run.DeadlineAt != nil && !time.Now().Before(*run.DeadlineAt) {
		return "profile task polling deadline exceeded", true
	}
	return "", false
}

func profilePollOnce(ctx context.Context, run *db.TaskRun) (protocol.Result, protocol.Operation, error) {
	if run == nil {
		return protocol.Result{}, protocol.Operation{}, errors.New("profile task is nil")
	}
	revision, err := db.GetProtocolProfileRevisionContext(ctx, run.ProfileID, run.ProfileRevision)
	if err != nil {
		return protocol.Result{}, protocol.Operation{}, fmt.Errorf("load profile revision: %w", err)
	}
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		return protocol.Result{}, protocol.Operation{}, fmt.Errorf("decode profile revision: %w", err)
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		return protocol.Result{}, protocol.Operation{}, fmt.Errorf("compile profile revision: %w", err)
	}
	op, ok := profileOperation(compiled, run.Operation)
	if !ok || op.Poll == nil {
		return protocol.Result{}, protocol.Operation{}, fmt.Errorf("profile operation %q has no poll definition", run.Operation)
	}
	channel, err := db.GetChannelModelContext(ctx, run.ChannelID)
	if err != nil {
		return protocol.Result{}, protocol.Operation{}, fmt.Errorf("load channel: %w", err)
	}
	upstream := channel.ToUpstreamChannel()
	result, err := protocol.NewHTTPExecutor(nil).PollOnce(ctx, compiled, run.Operation, protocol.Request{BaseURL: upstream.BaseURL, APIKeys: upstream.GetEffectiveKeys(), Headers: upstream.Headers, TaskID: run.ProviderTaskID})
	if err == nil && strings.EqualFold(strings.TrimSpace(run.TaskKind), asyncTaskKindImage) {
		result.ResultURLs = normalizeProfileImageSources(result.ResultURLs, upstream.BaseURL)
	}
	return result, op, err
}

func profileDurableStatus(c *gin.Context, run *db.TaskRun, kind string, requestedID ...string) any {
	reqID := ""
	if len(requestedID) > 0 {
		reqID = strings.TrimPrefix(strings.TrimSpace(requestedID[0]), imageTaskIDPrefix)
	}
	op, _ := profileOperationForRun(c.Request.Context(), run)
	requiredMedia := op.EffectiveMediaRetention() == protocol.MediaRetentionRequired
	if kind == asyncTaskKindImage {
		var payload map[string]any
		if strings.TrimSpace(run.ResultBody) != "" && json.Unmarshal([]byte(run.ResultBody), &payload) == nil {
			if reqID != "" {
				if _, ok := payload["id"]; ok {
					payload["id"] = reqID
				}
				if _, ok := payload["task_id"]; ok {
					payload["task_id"] = reqID
				}
			}
			managed := attachProfileManagedMedia(c, run, kind, payload)
			if requiredMedia && !managed && (profilePayloadHasMedia(payload) || profileMediaAssetsExist(c, run, kind)) {
				clearProfileMediaPayload(payload)
				payload["status"] = "materializing"
			}
			return payload
		}
		fallbackID := run.ProviderTaskID
		if reqID != "" {
			fallbackID = reqID
		}
		payload = map[string]any{"id": fallbackID, "task_id": fallbackID, "status": normalizeProfileStatus(run.TaskStatus)}
		attachProfileManagedMedia(c, run, kind, payload)
		return payload
	}
	var payload model.VideoTaskResponse
	if strings.TrimSpace(run.ResultBody) != "" && json.Unmarshal([]byte(run.ResultBody), &payload) == nil {
		if reqID != "" {
			payload.ID = reqID
			payload.TaskID = reqID
		}
		if payload.Error == nil && (normalizeProfileStatus(run.TaskStatus) == model.VideoStatusFailed || run.TaskOutcome == "failed" || payload.Status == model.VideoStatusFailed) {
			var raw map[string]any
			if json.Unmarshal([]byte(run.ResultBody), &raw) == nil {
				if errVal, exists := raw["error"]; exists && errVal != nil {
					payload.Error = errVal
				} else if msg, exists := raw["message"]; exists && msg != nil {
					payload.Error = msg
				} else if failReason, exists := raw["fail_reason"]; exists && failReason != nil {
					payload.Error = failReason
				} else if failureReason, exists := raw["failure_reason"]; exists && failureReason != nil {
					payload.Error = failureReason
				}
			}
			if payload.Error == nil && run.TaskError != "" {
				payload.Error = run.TaskError
			}
		}
		managed := attachProfileManagedMedia(c, run, kind, &payload)
		if requiredMedia && !managed && (profilePayloadHasMedia(&payload) || profileMediaAssetsExist(c, run, kind)) {
			clearProfileMediaPayload(&payload)
			payload.Status = "materializing"
		}
		return &payload
	}
	fallbackID := run.ProviderTaskID
	if reqID != "" {
		fallbackID = reqID
	}
	payload = model.VideoTaskResponse{ID: fallbackID, TaskID: fallbackID, Status: normalizeProfileStatus(run.TaskStatus)}
	if run.TaskError != "" {
		payload.Error = run.TaskError
	}
	attachProfileManagedMedia(c, run, kind, &payload)
	return &payload
}

// attachProfileManagedMedia exposes only capabilities that can be recovered
// from encrypted-at-rest asset metadata. Pending assets produce no managed
// URL; callers enforcing required retention scrub any provider URL as well.
func attachProfileManagedMedia(c *gin.Context, run *db.TaskRun, kind string, payload any) bool {
	if c == nil || run == nil || payload == nil {
		return false
	}
	assets, err := db.ListMediaAssetsForTaskRunContext(c.Request.Context(), run.ID, kind)
	if err != nil {
		return false
	}
	managed := make([]map[string]string, 0, len(assets))
	for _, asset := range assets {
		if asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" {
			continue
		}
		capability, capErr := db.RecoverMediaAssetCapability(&asset)
		if capErr != nil {
			continue
		}
		managed = append(managed, map[string]string{"url": mediaPublicURL(c, asset.PublicID, capability)})
	}
	if len(managed) == 0 {
		return false
	}
	if video, ok := payload.(*model.VideoTaskResponse); ok {
		video.VideoURL, video.URL, video.Data = managed[0]["url"], managed[0]["url"], managed
		return true
	}
	if image, ok := payload.(map[string]any); ok {
		image["data"] = managed
	}
	return true
}

func profileOperationForRun(ctx context.Context, run *db.TaskRun) (protocol.Operation, bool) {
	if run == nil || strings.TrimSpace(run.ProfileID) == "" || run.ProfileRevision <= 0 {
		return protocol.Operation{}, false
	}
	revision, err := db.GetProtocolProfileRevisionContext(ctx, run.ProfileID, run.ProfileRevision)
	if err != nil {
		return protocol.Operation{}, false
	}
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		return protocol.Operation{}, false
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		return protocol.Operation{}, false
	}
	return profileOperation(compiled, run.Operation)
}

func profilePayloadHasMedia(payload any) bool {
	switch value := payload.(type) {
	case *model.VideoTaskResponse:
		return strings.TrimSpace(value.VideoURL) != "" || strings.TrimSpace(value.URL) != "" || len(value.Data) > 0
	case map[string]any:
		switch data := value["data"].(type) {
		case []any:
			return len(data) > 0
		case []map[string]string:
			return len(data) > 0
		}
		return len(collectProfileResultURLs(value, map[string]struct{}{"url": {}, "image_url": {}, "proxy_url": {}, "thumbnail_url": {}, "b64_json": {}})) > 0
	}
	return false
}

func profileMediaAssetsExist(c *gin.Context, run *db.TaskRun, kind string) bool {
	if c == nil || run == nil {
		return false
	}
	assets, err := db.ListMediaAssetsForTaskRunContext(c.Request.Context(), run.ID, kind)
	return err == nil && len(assets) > 0
}

func clearProfileMediaPayload(payload any) {
	switch value := payload.(type) {
	case *model.VideoTaskResponse:
		value.VideoURL, value.URL, value.Data = "", "", nil
	case map[string]any:
		delete(value, "data")
		value["raw"], value["raw_payload"] = nil, nil
	}
}

func containsProfileFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}
