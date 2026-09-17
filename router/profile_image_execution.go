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
	"relay-gateway/adapter"
	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/db"
	relaymedia "relay-gateway/media"
	"relay-gateway/model"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

// profileEngineImageCreate is an opt-in bridge for image.create profiles. It
// deliberately has no retry loop: an accepted paid image submission must not
// be submitted a second time by this helper.
func profileEngineImageCreate(c *gin.Context, req *model.ImageGenerationRequest) (map[string]any, *config.UpstreamChannel, bool, error) {
	return profileEngineImageCreateForChannel(c, req, nil)
}

// profileEngineImageCreateForChannel adds an optional channel pin for
// Playground callers. A pin never bypasses binding validation: if the channel
// has not explicitly adopted a published Profile, the caller can retain its
// Legacy fallback behavior.
func profileEngineImageCreateForChannel(c *gin.Context, req *model.ImageGenerationRequest, pinned *config.UpstreamChannel) (map[string]any, *config.UpstreamChannel, bool, error) {
	if c == nil || req == nil || !(profileEngineEnabledFor("RELAY_ENABLE_PROFILE_IMAGE_ENGINE") || profileEngineEnabledFor("RELAY_ENABLE_PROFILE_DIRECT_ENGINE")) {
		return nil, nil, false, nil
	}
	idempotencyKey := profileIdempotencyKey(c)
	callerIdempotencyKey := profileCallerIdempotencyKey(c)
	// Built-in profiles historically used the plural operation name; resolve
	// either spelling without consulting the legacy Adapter registry.
	operation := "image.create"
	if c.Request != nil {
		path := strings.ToLower(strings.TrimSpace(c.Request.URL.Path))
		// The route (and Playground) selects a direct generations operation
		// when one is bound, with images.create as the compatibility fallback.
		if strings.HasSuffix(path, "/images/generations") || strings.Contains(path, "/playground/") {
			operation = "images.generations"
		}
	}
	requestFingerprints := make(map[string]string)
	if callerIdempotencyKey != "" {
		fingerprintBody := req.ToMap()
		seenOperations := make(map[string]struct{})
		lookupOperations := []string{operation}
		if operation == "image.create" {
			lookupOperations = append(lookupOperations, "images.create")
		} else if operation == "images.create" {
			lookupOperations = append(lookupOperations, "image.create")
		}
		for _, lookupOperation := range lookupOperations {
			if _, seen := seenOperations[lookupOperation]; seen {
				continue
			}
			seenOperations[lookupOperation] = struct{}{}
			fingerprint, fingerprintErr := profileRequestFingerprint(lookupOperation, req.Model, fingerprintBody)
			if fingerprintErr != nil {
				return nil, nil, true, fingerprintErr
			}
			requestFingerprints[lookupOperation] = fingerprint
			if existing, lookupErr := db.FindProfileTaskRunByIdempotencyKeyContext(c.Request.Context(), callerIdempotencyKey, lookupOperation); lookupErr == nil {
				if existing.RequestFingerprint != fingerprint {
					return nil, nil, true, db.ErrTaskIdempotencyConflict
				}
				response, responseErr := profileImageResponseFromTaskRun(existing)
				if responseErr != nil {
					return nil, nil, true, responseErr
				}
				return response, nil, true, nil
			} else if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
				return nil, nil, true, lookupErr
			}
		}
	}
	var candidates []*config.UpstreamChannel
	if pinned != nil {
		candidates = []*config.UpstreamChannel{pinned}
	} else {
		var err error
		candidates, err = service.DefaultDispatcher.ResolveProfileCandidates(req.Model, operation)
		if err != nil {
			operation = "images.create"
			candidates, err = service.DefaultDispatcher.ResolveProfileCandidates(req.Model, operation)
		}
		if err != nil {
			operation = "images.generations"
			candidates, err = service.DefaultDispatcher.ResolveProfileCandidates(req.Model, operation)
		}
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
		binding, bindErr := db.FindChannelProtocolBindingContext(c.Request.Context(), candidate.ID, operation, req.Model)
		// Built-in profiles historically used the plural operation name;
		// accept both spellings while custom profiles converge on image.create.
		if errors.Is(bindErr, gorm.ErrRecordNotFound) {
			binding, bindErr = db.FindChannelProtocolBindingContext(c.Request.Context(), candidate.ID, "images.create", req.Model)
		}
		// Keep a direct generations operation distinct from an async image
		// job contract when resolving an explicitly pinned Playground channel.
		if errors.Is(bindErr, gorm.ErrRecordNotFound) {
			binding, bindErr = db.FindChannelProtocolBindingContext(c.Request.Context(), candidate.ID, "images.generations", req.Model)
		}
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
		reqCopy := *req
		if mapped := strings.TrimSpace(candidate.ModelMap[req.Model]); mapped != "" {
			reqCopy.Model = mapped
		}
		// Profile and Legacy transports must apply the same model-specific image
		// normalization. Otherwise the selected execution engine can silently
		// change size/aspect fields for an otherwise identical request.
		body := adapter.NormalizeImageRequest(&reqCopy, candidate.Type)
		if op.ExecutionMode == protocol.ExecutionDirect {
			delete(body, "stream")
			delete(body, "partial_images")
		}
		reservationID := ""
		reservationFingerprint := ""
		unlock := func() {}
		if op.ExecutionMode == protocol.ExecutionAsync && callerIdempotencyKey != "" {
			fingerprint := requestFingerprints[binding.Operation]
			if fingerprint == "" {
				fingerprintBody := req.ToMap()
				var fingerprintErr error
				fingerprint, fingerprintErr = profileRequestFingerprint(binding.Operation, req.Model, fingerprintBody)
				if fingerprintErr != nil {
					return nil, candidate, true, fingerprintErr
				}
			}
			unlock = profileSubmitLocks.acquire(callerIdempotencyKey + "\x00" + binding.Operation)
			existing, lookupErr := db.FindProfileTaskRunByIdempotencyContext(c.Request.Context(), callerIdempotencyKey, binding.Operation, fingerprint)
			if lookupErr == nil {
				response, responseErr := profileImageResponseFromTaskRun(existing)
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
			reserved, created, reserveErr := reserveProfileTaskRun(c.Request.Context(), candidate, binding, revision, compiled, op, asyncTaskKindImage, callerIdempotencyKey, fingerprint, profileRequestID(c))
			if reserveErr != nil {
				unlock()
				return nil, candidate, true, reserveErr
			}
			if !created {
				unlock()
				return nil, candidate, true, errors.New("idempotent profile submission is already in progress or unknown")
			}
			reservationID = reserved.ID
			reservationFingerprint = fingerprint
		}
		if entry := audit.FromContext(c.Request.Context()); entry != nil {
			targetModel := req.Model
			if mapped := strings.TrimSpace(candidate.ModelMap[req.Model]); mapped != "" {
				targetModel = mapped
			}
			entry.RecordDispatch(candidate.ID, candidate.Type, candidate.BaseURL, targetModel)
		}
		executor := newProfileOperationExecutor(op)
		request := protocol.Request{BaseURL: candidate.BaseURL, APIKeys: candidate.GetEffectiveKeys(), Headers: candidate.Headers, Body: body, IdempotencyKey: idempotencyKey}
		run, err := protocol.NewAsyncEngine(executor, executor).Run(c.Request.Context(), compiled, binding.Operation, request)
		if err != nil {
			if op.ExecutionMode == protocol.ExecutionAsync && strings.TrimSpace(run.Accepted.TaskID) != "" {
				// gateway_wait can fail after Submit has been accepted. Keep the
				// provider task durable so a retry with the same key is read-only.
				accepted := profileAsyncProjectionResult(run)
				persistResponse := profileImageResponse(accepted, true)
				taskRunID := strings.TrimSpace(reservationID)
				if taskRunID == "" {
					taskRunID = profileTaskRunID(candidate.ID, run.Accepted.TaskID, binding.Operation)
				}
				persistedTaskRunID := persistProfileImageProjection(c, candidate, binding, revision, compiled, op, run.Accepted, persistResponse, run.PollAttempts, reservationID, callerIdempotencyKey, reservationFingerprint)
				if persistedTaskRunID != "" {
					taskRunID = persistedTaskRunID
				}
				if encoded, encodeErr := json.Marshal(persistableProfileImageResponse(persistResponse)); encodeErr == nil {
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
		result.ResultURLs = normalizeProfileImageSources(result.ResultURLs, candidate.BaseURL)
		if op.ExecutionMode == protocol.ExecutionAsync && strings.TrimSpace(run.Accepted.TaskID) == "" {
			if reservationID != "" {
				_ = db.UpdateTaskRunSubmissionStateContext(context.Background(), reservationID, "submission_unknown", "profile async submit returned no task ID")
				unlock()
			}
			return nil, candidate, true, errors.New("profile async submit returned no task ID")
		}
		response := profileImageResponse(result, op.ExecutionMode == protocol.ExecutionAsync)
		retention := op.EffectiveMediaRetention()
		if op.ExecutionMode == protocol.ExecutionDirect && len(result.ResultURLs) > 0 && profileMediaRetentionEnabled(op) && response != nil {
			taskRunID := profileTaskRunID(candidate.ID, idempotencyKey, binding.Operation)
			if err := materializeProfileImageResponse(c, taskRunID, response, result.ResultURLs); err != nil && retention == protocol.MediaRetentionRequired {
				return nil, candidate, true, fmt.Errorf("profile image media materialization failed: %w", err)
			}
		}
		if op.ExecutionMode == protocol.ExecutionAsync && response != nil && imageResponseTaskID(response) != "" {
			// Media rows require a durable TaskRun owner. Persist the provider
			// projection first so submit responses that already carry result URLs
			// cannot lose their media context to a foreign-key failure.
			taskRunID := strings.TrimSpace(reservationID)
			if taskRunID == "" {
				taskRunID = profileTaskRunID(candidate.ID, run.Accepted.TaskID, binding.Operation)
			}
			persistResponse := response
			// Required gateway-wait media remains processing until the local
			// object is available, so a failed materialization can be retried
			// from the durable result without regressing a completed TaskRun.
			if len(result.ResultURLs) > 0 && profileMediaRetentionEnabled(op) && retention == protocol.MediaRetentionRequired && op.PollingMode == protocol.PollingGatewayWait {
				persistResponse = profileImageResponse(result, op.ExecutionMode == protocol.ExecutionAsync)
				persistResponse["status"] = model.VideoStatusProcessing
			}
			persistedTaskRunID := persistProfileImageProjection(c, candidate, binding, revision, compiled, op, run.Accepted, persistResponse, run.PollAttempts, reservationID, callerIdempotencyKey, reservationFingerprint)
			if persistedTaskRunID == "" {
				if reservationID != "" {
					unlock()
				}
				return nil, candidate, true, errors.New("persist profile image TaskRun before media materialization")
			}
			taskRunID = persistedTaskRunID
			if len(result.ResultURLs) > 0 && profileMediaRetentionEnabled(op) && !(retention == protocol.MediaRetentionRequired && op.PollingMode == protocol.PollingGatewayWait) {
				if _, mediaErr := db.EnsureTaskResultMediaContext(c.Request.Context(), taskRunID, asyncTaskKindImage, result.ResultURLs); mediaErr != nil {
					if reservationID != "" {
						unlock()
					}
					return nil, candidate, true, fmt.Errorf("enqueue profile image result media: %w", mediaErr)
				}
			}
			if len(result.ResultURLs) > 0 && profileMediaRetentionEnabled(op) && retention == protocol.MediaRetentionRequired && op.PollingMode == protocol.PollingGatewayWait {
				if err := materializeProfileImageResponse(c, taskRunID, response, result.ResultURLs); err != nil {
					delete(response, "data")
					delete(response, "raw")
					delete(response, "raw_payload")
					response["status"] = "materializing"
					response["error"] = profileMediaErrorMessage(err)
					// Keep the provider result in TaskRun.ResultBody only; the
					// response sent to the caller remains URL-free while a later
					// retry can recreate the local asset/job.
					persistResponse = profileImageResponse(result, op.ExecutionMode == protocol.ExecutionAsync)
					persistResponse["status"] = model.VideoStatusProcessing
					persistResponse["error"] = response["error"]
					encoded, encodeErr := json.Marshal(persistableProfileImageResponse(persistResponse))
					if encodeErr != nil {
						if reservationID != "" {
							unlock()
						}
						return nil, candidate, true, fmt.Errorf("encode profile image materialization result: %w", encodeErr)
					}
					if updateErr := db.UpdateTaskRunResultContext(c.Request.Context(), taskRunID, string(encoded), false); updateErr != nil {
						if reservationID != "" {
							unlock()
						}
						return nil, candidate, true, fmt.Errorf("persist profile image materialization result: %w", updateErr)
					}
				} else {
					encoded, encodeErr := json.Marshal(persistableProfileImageResponse(profileImageResponse(result, op.ExecutionMode == protocol.ExecutionAsync)))
					if encodeErr != nil {
						if reservationID != "" {
							unlock()
						}
						return nil, candidate, true, fmt.Errorf("encode completed profile image media result: %w", encodeErr)
					}
					if updateErr := db.UpdateTaskRunResultContext(c.Request.Context(), taskRunID, string(encoded), false); updateErr != nil {
						if reservationID != "" {
							unlock()
						}
						return nil, candidate, true, fmt.Errorf("persist completed profile image media result: %w", updateErr)
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

func profileImageResponse(result protocol.Result, async bool) map[string]any {
	id := strings.TrimSpace(result.TaskID)
	if id == "" {
		id = profileJSONString(result.JSON, "id")
		if id == "" {
			id = profileJSONString(result.JSON, "task_id")
		}
	}
	status := strings.TrimSpace(result.Status)
	if status == "" {
		status = profileJSONString(result.JSON, "status")
	}
	if status == "" {
		if async {
			status = "queued"
		} else {
			status = "completed"
		}
	}
	// Keep both names: raw is compact for existing image-job consumers, while
	// raw_payload makes the preservation contract explicit to new callers.
	response := map[string]any{"id": id, "task_id": id, "status": status, "raw": result.JSON, "raw_payload": result.JSON}
	if len(result.ResultURLs) > 0 {
		data := make([]map[string]string, 0, len(result.ResultURLs))
		for _, u := range result.ResultURLs {
			if displayURL := profileImageDisplaySource(u); displayURL != "" {
				data = append(data, map[string]string{"url": displayURL})
			}
		}
		if len(data) > 0 {
			response["data"] = data
		}
	}
	return response
}

func profileImageDisplaySource(raw string) string {
	return relaymedia.MediaSourceDataURL(relaymedia.NormalizeMediaSource(raw))
}

// normalizeProfileImageSources converts inline image results into browser-safe
// data URLs and resolves conventional relative media paths against the frozen
// upstream base URL. HTTPSourceFetcher still performs the final public-network
// validation before any resolved URL is downloaded.
func normalizeProfileImageSources(sources []string, baseURL string) []string {
	return relaymedia.NormalizeMediaSources(sources, baseURL)
}

func imageResponseTaskID(response map[string]any) string {
	if response == nil {
		return ""
	}
	if id, ok := response["task_id"].(string); ok && db.IsValidTaskID(id) {
		return strings.TrimSpace(id)
	}
	if id, ok := response["id"].(string); ok && db.IsValidTaskID(id) {
		return strings.TrimSpace(id)
	}
	return ""
}

func persistProfileImageProjection(c *gin.Context, channel *config.UpstreamChannel, binding *db.ChannelProtocolBinding, revision *db.ProtocolProfileRevision, compiled protocol.CompiledProfile, op protocol.Operation, accepted protocol.Result, response map[string]any, pollAttempts []protocol.PollAttempt, reservedRunID, idempotencyKey, requestFingerprint string) string {
	if c == nil || channel == nil || binding == nil || revision == nil {
		return ""
	}
	taskID := imageResponseTaskID(response)
	if taskID == "" {
		return ""
	}
	canonicalProviderID := accepted.TaskID
	if canonicalProviderID == "" {
		canonicalProviderID = taskID
	}
	lookupIDs := []string{imageTaskIDPrefix + taskID, taskID}
	if gtID, disambiguated := disambiguateAsyncTaskIDs(channel.ID, asyncTaskKindImage, canonicalProviderID, lookupIDs); disambiguated {
		setImageJobResponseIDs(response, gtID)
		taskID = gtID
		lookupIDs = []string{imageTaskIDPrefix + gtID, gtID}
	}
	statusText, _ := response["status"].(string)
	registration := newAsyncTaskMappingRegistration(c.Request.Context(), channel.ID, asyncTaskKindImage, canonicalProviderID, statusText, lookupIDs...)
	if !registration.empty() {
		if err := persistAsyncTaskMappingRegistration(registration); err != nil {
			deferAsyncTaskMappingPersistence(c, registration, err)
		}
	}
	now := time.Now()
	status := normalizeProfileImageStatus(statusText)
	run := &db.TaskRun{ID: profileTaskRunID(channel.ID, accepted.TaskID, binding.Operation), OriginRequestID: registration.OriginRequestID, TaskKind: asyncTaskKindImage, Operation: binding.Operation, ChannelID: channel.ID, Engine: "profile", ProfileID: binding.ProfileID, ProfileRevision: revision.Revision, ProfileDigest: compiled.Digest(), PollingMode: op.PollingMode, ProviderTaskID: accepted.TaskID, SubmissionState: "accepted", TaskStatus: status, TaskOutcome: "pending", AcceptedAt: &now}
	if strings.TrimSpace(reservedRunID) != "" {
		run.ID = strings.TrimSpace(reservedRunID)
		run.IdempotencyKey = strings.TrimSpace(idempotencyKey)
		run.RequestFingerprint = strings.TrimSpace(requestFingerprint)
	}
	if encoded, err := json.Marshal(response); err == nil {
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
	if status == "completed" || status == "failed" {
		run.TaskOutcome = profileTaskOutcome(status)
		run.CompletedAt = &now
	}
	if strings.TrimSpace(reservedRunID) != "" {
		if err := db.UpdateProfileTaskRunProjectionContext(context.Background(), run); err != nil {
			return ""
		}
	} else if err := db.CreateTaskRunContext(context.Background(), run); err != nil {
		// The run ID is deterministic across retries. Reconcile aliases against
		// a concurrently-created projection when possible.
		if existing, loadErr := db.GetTaskRun(run.ID); loadErr == nil {
			run = existing
		} else {
			return ""
		}
	}
	if len(pollAttempts) > 0 {
		records := make([]db.TaskPollRecord, 0, len(pollAttempts))
		for _, attempt := range pollAttempts {
			records = append(records, db.TaskPollRecord{StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt, HTTPStatus: attempt.HTTPStatus, Outcome: attempt.Outcome, Error: attempt.Error})
		}
		_ = db.PersistTaskPollBatchContext(context.Background(), run.ID, records)
	}
	persistProfileTaskAliases(registration, run.ID)
	if registration.OriginRequestID != "" {
		_ = audit.RecordAsyncTaskCreated(c.Request.Context(), taskID, asyncTaskKindImage, statusText)
	}
	return run.ID
}

func normalizeProfileImageStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "completed", "complete", "succeeded", "success", "done":
		return "completed"
	case "failed", "error", "expired", "cancelled", "canceled":
		return "failed"
	case "processing", "running", "in_progress", "in-progress", "materializing":
		return "processing"
	default:
		return "queued"
	}
}
