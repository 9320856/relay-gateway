package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

// profileEngineVideoCreate is an opt-in bridge from the legacy video route to
// the generic Profile executor. Keeping it disabled by default makes rollout
// and rollback a process-level configuration change rather than a code fork.
func profileEngineVideoCreate(c *gin.Context, req *model.VideoGenerationRequest) (*model.VideoTaskResponse, *config.UpstreamChannel, bool, error) {
	return profileEngineVideoCreateForChannel(c, req, nil)
}

// profileEngineVideoCreateForChannel is the same migration bridge as
// profileEngineVideoCreate, with an optional channel pin for callers such as
// the admin Playground. A pinned channel is still required to have an
// explicit Profile binding; the helper never invents a binding or silently
// switches the operator's selected channel.
func profileEngineVideoCreateForChannel(c *gin.Context, req *model.VideoGenerationRequest, pinned *config.UpstreamChannel) (*model.VideoTaskResponse, *config.UpstreamChannel, bool, error) {
	if c == nil || req == nil || !profileEngineEnabledFor("RELAY_ENABLE_PROFILE_ENGINE") {
		return nil, nil, false, nil
	}
	idempotencyKey := profileIdempotencyKey(c)
	callerIdempotencyKey := profileCallerIdempotencyKey(c)
	requestFingerprint := ""
	if callerIdempotencyKey != "" {
		fingerprintBody, fingerprintErr := videoProfileBody(req, nil)
		if fingerprintErr != nil {
			return nil, nil, true, fingerprintErr
		}
		requestFingerprint, fingerprintErr = profileRequestFingerprint("video.create", req.Model, fingerprintBody)
		if fingerprintErr != nil {
			return nil, nil, true, fingerprintErr
		}
		if existing, lookupErr := db.FindProfileTaskRunByIdempotencyKeyContext(c.Request.Context(), callerIdempotencyKey, "video.create"); lookupErr == nil {
			if existing.RequestFingerprint != requestFingerprint {
				return nil, nil, true, db.ErrTaskIdempotencyConflict
			}
			response, responseErr := profileVideoResponseFromTaskRun(existing)
			if responseErr != nil {
				return nil, nil, true, responseErr
			}
			return response, nil, true, nil
		} else if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			return nil, nil, true, lookupErr
		}
	}
	// Profile bindings define capability independently of the legacy adapter
	// metadata. This lets an OpenAI-compatible or otherwise generic channel
	// expose a custom video operation before its legacy adapter is removed.
	var candidates []*config.UpstreamChannel
	if pinned != nil {
		candidates = []*config.UpstreamChannel{pinned}
	} else {
		var err error
		candidates, err = service.DefaultDispatcher.ResolveProfileCandidates(req.Model, "video.create")
		if err != nil {
			return nil, nil, false, nil
		}
	}
	if entry := audit.FromContext(c.Request.Context()); entry != nil {
		ids := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate != nil {
				ids = append(ids, candidate.ID)
			}
		}
		entry.RecordCandidates(ids)
	}
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		binding, bindErr := db.FindChannelProtocolBindingContext(c.Request.Context(), candidate.ID, "video.create", req.Model)
		if errors.Is(bindErr, gorm.ErrRecordNotFound) {
			continue
		}
		if bindErr != nil {
			return nil, candidate, true, bindErr
		}
		revision, compiled, op, err := loadPublishedProfileOperation(c.Request.Context(), binding)
		if err != nil {
			return nil, candidate, true, err
		}
		body, err := videoProfileBody(req, candidate)
		if err != nil {
			return nil, candidate, true, err
		}
		reservationID := ""
		reservationFingerprint := ""
		unlock := func() {}
		if op.ExecutionMode == protocol.ExecutionAsync && callerIdempotencyKey != "" {
			unlock = profileSubmitLocks.acquire(callerIdempotencyKey + "\x00" + binding.Operation)
			existing, lookupErr := db.FindProfileTaskRunByIdempotencyContext(c.Request.Context(), callerIdempotencyKey, binding.Operation, requestFingerprint)
			if lookupErr == nil {
				response, responseErr := profileVideoResponseFromTaskRun(existing)
				unlock()
				if responseErr != nil {
					return nil, candidate, true, responseErr
				}
				return response, candidate, true, nil
			}
			if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
				unlock()
				return nil, candidate, true, lookupErr
			}
			reserved, created, reserveErr := reserveProfileTaskRun(c.Request.Context(), candidate, binding, revision, compiled, op, asyncTaskKindVideo, callerIdempotencyKey, requestFingerprint, profileRequestID(c))
			if reserveErr != nil {
				unlock()
				return nil, candidate, true, reserveErr
			}
			if !created {
				unlock()
				return nil, candidate, true, errors.New("idempotent profile submission is already in progress or unknown")
			}
			reservationID = reserved.ID
			reservationFingerprint = requestFingerprint
		}
		if entry := audit.FromContext(c.Request.Context()); entry != nil {
			targetModel := req.Model
			if mapped := strings.TrimSpace(candidate.ModelMap[req.Model]); mapped != "" {
				targetModel = mapped
			}
			entry.RecordDispatch(candidate.ID, candidate.Type, candidate.BaseURL, targetModel)
		}
		executor := protocol.NewHTTPExecutor(nil)
		request := protocol.Request{BaseURL: candidate.BaseURL, APIKeys: candidate.GetEffectiveKeys(), Headers: candidate.Headers, Body: body, IdempotencyKey: idempotencyKey}
		engine := protocol.NewAsyncEngine(executor, executor)
		run, err := engine.Run(c.Request.Context(), compiled, binding.Operation, request)
		if err != nil {
			if op.ExecutionMode == protocol.ExecutionAsync && strings.TrimSpace(run.Accepted.TaskID) != "" {
				// Submit may have succeeded even when gateway_wait later times
				// out or observes a provider failure. Persist the accepted task
				// before returning the polling error so retries never submit again.
				accepted := profileAsyncProjectionResult(run)
				persistResponse := profileVideoResponse(accepted, true)
				taskRunID := strings.TrimSpace(reservationID)
				if taskRunID == "" {
					taskRunID = profileTaskRunID(candidate.ID, run.Accepted.TaskID, binding.Operation)
				}
				persistedTaskRunID := persistProfileTaskProjection(c, candidate, binding, revision, compiled, op, run.Accepted, persistResponse, run.PollAttempts, reservationID, callerIdempotencyKey, reservationFingerprint)
				if persistedTaskRunID != "" {
					taskRunID = persistedTaskRunID
				}
				if encoded, encodeErr := json.Marshal(persistableProfileVideoResponse(persistResponse)); encodeErr == nil {
					_ = db.UpdateTaskRunResultContext(c.Request.Context(), taskRunID, string(encoded), false)
				}
				if reservationID != "" {
					unlock()
				}
				return nil, candidate, true, err
			}
			if reservationID != "" {
				_ = db.UpdateTaskRunSubmissionStateContext(context.Background(), reservationID, profileSubmissionErrorState(err), err.Error())
				unlock()
			}
			return nil, candidate, true, err
		}
		result := run.Accepted
		if run.Final != nil {
			result = *run.Final
			if result.TaskID == "" {
				result.TaskID = run.Accepted.TaskID
			}
		}
		if op.ExecutionMode == protocol.ExecutionAsync && strings.TrimSpace(run.Accepted.TaskID) == "" {
			if reservationID != "" {
				_ = db.UpdateTaskRunSubmissionStateContext(context.Background(), reservationID, "submission_unknown", "profile async submit returned no task ID")
				unlock()
			}
			return nil, candidate, true, errors.New("profile async submit returned no task ID")
		}
		response := profileVideoResponse(result, op.ExecutionMode == protocol.ExecutionAsync)
		if op.ExecutionMode == protocol.ExecutionAsync && response != nil && response.TaskID != "" {
			// Persist the TaskRun before creating media rows. Media assets have a
			// durable TaskRun owner, and using the deterministic ID before this
			// projection can otherwise fail the foreign-key check and lose the
			// result URL when the error is ignored.
			taskRunID := strings.TrimSpace(reservationID)
			if taskRunID == "" {
				taskRunID = profileTaskRunID(candidate.ID, run.Accepted.TaskID, binding.Operation)
			}
			retention := op.EffectiveMediaRetention()
			persistResponse := response
			// Required gateway-wait media remains processing until the local
			// object is available. This lets a failed materialization retry from
			// the durable provider result without attempting an invalid completed
			// to processing state transition.
			if (profileMediaRequired() || retention == protocol.MediaRetentionRequired) && op.PollingMode == protocol.PollingGatewayWait && strings.TrimSpace(response.VideoURL) != "" {
				persistResponse = profileVideoResponse(result, op.ExecutionMode == protocol.ExecutionAsync)
				persistResponse.Status = model.VideoStatusProcessing
			}
			persistedTaskRunID := persistProfileTaskProjection(c, candidate, binding, revision, compiled, op, run.Accepted, persistResponse, run.PollAttempts, reservationID, callerIdempotencyKey, reservationFingerprint)
			if persistedTaskRunID == "" {
				if reservationID != "" {
					unlock()
				}
				return nil, candidate, true, errors.New("persist profile video TaskRun before media materialization")
			}
			taskRunID = persistedTaskRunID
			if len(result.ResultURLs) > 0 && profileMediaRetentionEnabled(op) && !(retention == protocol.MediaRetentionRequired && op.PollingMode == protocol.PollingGatewayWait) {
				if _, mediaErr := db.EnsureTaskResultMediaContext(c.Request.Context(), taskRunID, asyncTaskKindVideo, result.ResultURLs); mediaErr != nil {
					if reservationID != "" {
						unlock()
					}
					return nil, candidate, true, fmt.Errorf("enqueue profile video result media: %w", mediaErr)
				}
			}
			if (profileMediaRequired() || retention == protocol.MediaRetentionRequired) && op.PollingMode == protocol.PollingGatewayWait && strings.TrimSpace(response.VideoURL) != "" {
				stableURL, materializeErr := materializeProfileVideoURL(c, taskRunID, response.VideoURL)
				if materializeErr != nil {
					response.VideoURL, response.URL, response.Data = "", "", nil
					response.Status = "materializing"
					response.Error = profileMediaErrorMessage(materializeErr)
					// Keep the provider URL in the durable result only. The
					// public response is scrubbed, while a later retry can still
					// recreate a missing local asset/job without re-submitting.
					persistResponse = profileVideoResponse(result, op.ExecutionMode == protocol.ExecutionAsync)
					persistResponse.Status = model.VideoStatusProcessing
					persistResponse.Error = response.Error
					encoded, encodeErr := json.Marshal(persistableProfileVideoResponse(persistResponse))
					if encodeErr != nil {
						if reservationID != "" {
							unlock()
						}
						return nil, candidate, true, fmt.Errorf("encode profile video materialization result: %w", encodeErr)
					}
					if updateErr := db.UpdateTaskRunResultContext(c.Request.Context(), taskRunID, string(encoded), false); updateErr != nil {
						if reservationID != "" {
							unlock()
						}
						return nil, candidate, true, fmt.Errorf("persist profile video materialization result: %w", updateErr)
					}
				} else {
					response.VideoURL, response.URL = stableURL, stableURL
					response.Data = []map[string]string{{"url": stableURL}}
					encoded, encodeErr := json.Marshal(persistableProfileVideoResponse(profileVideoResponse(result, op.ExecutionMode == protocol.ExecutionAsync)))
					if encodeErr != nil {
						if reservationID != "" {
							unlock()
						}
						return nil, candidate, true, fmt.Errorf("encode completed profile video media result: %w", encodeErr)
					}
					if updateErr := db.UpdateTaskRunResultContext(c.Request.Context(), taskRunID, string(encoded), false); updateErr != nil {
						if reservationID != "" {
							unlock()
						}
						return nil, candidate, true, fmt.Errorf("persist completed profile video media result: %w", updateErr)
					}
				}
			}
		}
		if reservationID != "" {
			unlock()
		}
		return response, candidate, true, nil
	}
	return nil, nil, false, nil
}

// profileAsyncProjectionResult keeps the accepted Provider Task ID when a
// gateway_wait poll returns an error. A non-empty final observation wins, but
// a timeout still has a durable accepted result to project and retry later.
func profileAsyncProjectionResult(run protocol.AsyncRunResult) protocol.Result {
	result := run.Accepted
	if run.Final == nil {
		return result
	}
	final := *run.Final
	if strings.TrimSpace(final.TaskID) == "" {
		final.TaskID = result.TaskID
	}
	if strings.TrimSpace(final.Status) != "" || final.JSON != nil || len(final.RawBody) > 0 || len(final.ResultURLs) > 0 || final.HTTPStatus != 0 {
		return final
	}
	return result
}

func profileOperation(compiled protocol.CompiledProfile, name string) (protocol.Operation, bool) {
	for _, op := range compiled.Profile().Operations {
		if op.Operation == name {
			return op, true
		}
	}
	return protocol.Operation{}, false
}

// loadPublishedProfileOperation is shared by the Profile submit and direct
// routes. Task polling uses its captured revision even after it is retired,
// so it intentionally does not call this published-only helper.
func loadPublishedProfileOperation(ctx context.Context, binding *db.ChannelProtocolBinding) (*db.ProtocolProfileRevision, protocol.CompiledProfile, protocol.Operation, error) {
	var empty protocol.CompiledProfile
	revision, err := db.GetProtocolProfileRevisionContext(ctx, binding.ProfileID, binding.ProfileRevision)
	if err != nil {
		return nil, empty, protocol.Operation{}, fmt.Errorf("load profile revision: %w", err)
	}
	if revision.State != db.ProfileRevisionPublished {
		return nil, empty, protocol.Operation{}, fmt.Errorf("profile revision %s/%d is not published", binding.ProfileID, binding.ProfileRevision)
	}
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		return nil, empty, protocol.Operation{}, fmt.Errorf("decode profile revision: %w", err)
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		return nil, empty, protocol.Operation{}, fmt.Errorf("compile profile revision: %w", err)
	}
	op, ok := profileOperation(compiled, binding.Operation)
	if !ok {
		return nil, empty, protocol.Operation{}, fmt.Errorf("profile operation %q is missing", binding.Operation)
	}
	return revision, compiled, op, nil
}

func videoProfileBody(req *model.VideoGenerationRequest, channel *config.UpstreamChannel) (map[string]any, error) {
	encoded, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode video request: %w", err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		return nil, fmt.Errorf("encode video request: %w", err)
	}
	if channel != nil && channel.ModelMap != nil {
		if mapped := strings.TrimSpace(channel.ModelMap[req.Model]); mapped != "" {
			body["model"] = mapped
		}
	}
	return body, nil
}

func profileVideoResponse(result protocol.Result, async bool) *model.VideoTaskResponse {
	response := &model.VideoTaskResponse{ID: strings.TrimSpace(result.TaskID), TaskID: strings.TrimSpace(result.TaskID)}
	if response.ID == "" {
		response.ID = profileJSONString(result.JSON, "id")
		response.TaskID = profileJSONString(result.JSON, "task_id")
		if response.ID == "" {
			response.ID = response.TaskID
		}
		if response.TaskID == "" {
			response.TaskID = response.ID
		}
	}
	response.Status = strings.TrimSpace(result.Status)
	if response.Status == "" {
		response.Status = profileJSONString(result.JSON, "status")
	}
	if response.Status == "" {
		if async {
			response.Status = model.VideoStatusQueued
		} else {
			response.Status = model.VideoStatusCompleted
		}
	} else {
		response.Status = model.NormalizeVideoStatus(response.Status)
	}
	if len(result.ResultURLs) > 0 {
		response.VideoURL, response.URL = result.ResultURLs[0], result.ResultURLs[0]
		response.Data = []map[string]string{{"url": result.ResultURLs[0]}}
	}
	if result.JSON != nil {
		if m, ok := result.JSON.(map[string]any); ok {
			if errVal, exists := m["error"]; exists && errVal != nil {
				response.Error = errVal
			} else if job, ok := m["job"].(map[string]any); ok && job["error"] != nil {
				response.Error = job["error"]
			} else if data, ok := m["data"].(map[string]any); ok && data["error"] != nil {
				response.Error = data["error"]
			} else if failReason, exists := m["fail_reason"]; exists && failReason != nil {
				response.Error = failReason
			} else if failureReason, exists := m["failure_reason"]; exists && failureReason != nil {
				response.Error = failureReason
			} else if msg, exists := m["message"]; exists && msg != nil && (response.Status == model.VideoStatusFailed || strings.EqualFold(response.Status, "failed")) {
				response.Error = msg
			}
		}
	}
	if response.Error == nil && (response.Status == model.VideoStatusFailed || strings.EqualFold(response.Status, "failed")) {
		if len(result.RawBody) > 0 {
			response.Error = string(result.RawBody)
		}
	}
	return response
}

func profileJSONString(value any, key string) string {
	object, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	if text, ok := object[key].(string); ok && strings.TrimSpace(text) != "" {
		return strings.TrimSpace(text)
	}
	if nested, ok := object["job"].(map[string]any); ok {
		if text, ok := nested[key].(string); ok && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	if nested, ok := object["data"].(map[string]any); ok {
		if text, ok := nested[key].(string); ok && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func persistProfileTaskProjection(c *gin.Context, channel *config.UpstreamChannel, binding *db.ChannelProtocolBinding, revision *db.ProtocolProfileRevision, compiled protocol.CompiledProfile, op protocol.Operation, accepted protocol.Result, response *model.VideoTaskResponse, pollAttempts []protocol.PollAttempt, reservedRunID, idempotencyKey, requestFingerprint string) string {
	if c == nil || channel == nil || binding == nil || revision == nil || response == nil || !db.IsValidTaskID(response.TaskID) {
		return ""
	}
	taskAlias := accepted.TaskID
	if taskAlias == "" {
		taskAlias = response.TaskID
	}
	lookupIDs := []string{response.ID, response.TaskID}
	if gtID, disambiguated := disambiguateAsyncTaskIDs(channel.ID, asyncTaskKindVideo, taskAlias, lookupIDs); disambiguated {
		response.ID = gtID
		response.TaskID = gtID
		lookupIDs = []string{gtID}
	}
	registration := newAsyncTaskMappingRegistration(c.Request.Context(), channel.ID, asyncTaskKindVideo, taskAlias, response.Status, lookupIDs...)
	if registration.empty() {
		return ""
	}
	if err := persistAsyncTaskMappingRegistration(registration); err != nil {
		deferAsyncTaskMappingPersistence(c, registration, err)
	}
	now := time.Now()
	run := &db.TaskRun{
		ID:              profileTaskRunID(channel.ID, accepted.TaskID, binding.Operation),
		OriginRequestID: registration.OriginRequestID,
		TaskKind:        asyncTaskKindVideo,
		Operation:       binding.Operation,
		ChannelID:       channel.ID,
		Engine:          "profile",
		ProfileID:       binding.ProfileID,
		ProfileRevision: revision.Revision,
		ProfileDigest:   compiled.Digest(),
		PollingMode:     op.PollingMode,
		ProviderTaskID:  accepted.TaskID,
		SubmissionState: "accepted",
		TaskStatus:      normalizeProfileStatus(response.Status),
		TaskOutcome:     "pending",
		AcceptedAt:      &now,
	}
	if strings.TrimSpace(reservedRunID) != "" {
		run.ID = strings.TrimSpace(reservedRunID)
		run.IdempotencyKey = strings.TrimSpace(idempotencyKey)
		run.RequestFingerprint = strings.TrimSpace(requestFingerprint)
	}
	if encoded, err := json.Marshal(persistableProfileVideoResponse(response)); err == nil {
		run.ResultBody = string(encoded)
	}
	if op.Poll != nil && op.PollingMode != protocol.PollingOff {
		deadline := now.Add(time.Duration(op.Poll.MaxDurationMS) * time.Millisecond)
		run.DeadlineAt = &deadline
		if op.PollingMode == protocol.PollingBackground {
			next := now.Add(time.Duration(op.Poll.IntervalMS) * time.Millisecond)
			run.NextPollAt = &next
		}
	}
	if isTaskTerminalForProjection(run.TaskStatus) {
		run.TaskOutcome = profileTaskOutcome(run.TaskStatus)
		run.CompletedAt = &now
	}
	if strings.TrimSpace(reservedRunID) != "" {
		if err := db.UpdateProfileTaskRunProjectionContext(context.Background(), run); err != nil {
			log.Printf("[PROFILE_TASK] update reserved video TaskRun %s: %v", run.ID, err)
			return ""
		}
	} else if err := db.CreateTaskRunContext(context.Background(), run); err != nil {
		// The run ID is deterministic across retries. If another request won
		// the race, still reconcile aliases against the existing run below.
		if existing, loadErr := db.GetTaskRun(run.ID); loadErr == nil {
			run = existing
		} else {
			log.Printf("[PROFILE_TASK] persist video TaskRun %s: %v", run.ID, err)
			return ""
		}
	}
	if len(pollAttempts) > 0 {
		records := make([]db.TaskPollRecord, 0, len(pollAttempts))
		for _, attempt := range pollAttempts {
			records = append(records, db.TaskPollRecord{StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt, HTTPStatus: attempt.HTTPStatus, Outcome: attempt.Outcome, Error: attempt.Error})
		}
		if err := db.PersistTaskPollBatchContext(context.Background(), run.ID, records); err != nil {
			log.Printf("[PROFILE_TASK] persist poll attempts %s: %v", run.ID, err)
		}
	}
	persistProfileTaskAliases(registration, run.ID)
	if registration.OriginRequestID != "" {
		if err := audit.RecordAsyncTaskCreated(c.Request.Context(), response.TaskID, asyncTaskKindVideo, response.Status); err != nil {
			log.Printf("[PROFILE_TASK] record video creation audit summary %s: %v", response.TaskID, err)
		}
	}
	return run.ID
}

// persistProfileTaskAliases links every public/provider lookup ID emitted by
// an accepted Profile task to its single durable TaskRun. Status handlers use
// these aliases instead of re-dispatching the request, so the captured channel
// and Profile revision remain authoritative across polls and restarts.
func persistProfileTaskAliases(registration asyncTaskMappingRegistration, runID string) {
	runID = strings.TrimSpace(runID)
	if registration.empty() || runID == "" {
		return
	}
	for _, lookupID := range registration.TaskIDs {
		lookupID = strings.TrimSpace(lookupID)
		if lookupID == "" {
			continue
		}
		if err := db.RecordTaskAliasContext(context.Background(), &db.TaskAlias{TaskRunID: runID, LookupID: lookupID, Source: "create"}); err != nil {
			log.Printf("[PROFILE_TASK] persist alias %s -> %s: %v", lookupID, runID, err)
		}
	}
}

func profileTaskRunID(channelID, taskID, operation string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(channelID) + "\x00" + strings.TrimSpace(taskID) + "\x00" + strings.TrimSpace(operation)))
	return "pr_" + hex.EncodeToString(sum[:])[:61]
}

func normalizeProfileStatus(status string) string {
	status = model.NormalizeVideoStatus(status)
	if status == "" {
		return model.VideoStatusQueued
	}
	return status
}

func isTaskTerminalForProjection(status string) bool {
	status = normalizeProfileStatus(status)
	return status == model.VideoStatusCompleted || status == model.VideoStatusFailed
}

func profileTaskOutcome(status string) string {
	if normalizeProfileStatus(status) == model.VideoStatusCompleted {
		return "success"
	}
	return "failed"
}
