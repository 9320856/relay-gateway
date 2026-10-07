package router

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

// prepareProfileAsyncMedia runs after the paid provider submission and its
// durable projection. Local-media failures must never re-enter submit failover.
func prepareProfileAsyncMedia(c *gin.Context, runID, kind string, op protocol.Operation, result protocol.Result, response any) error {
	policy := op.EffectiveMediaRetention()
	if policy == protocol.MediaRetentionDisabled {
		return nil
	}
	succeeded := profileResultSucceeded(result, op)
	if !succeeded {
		if policy == protocol.MediaRetentionRequired {
			clearProfileMediaPayload(response)
		}
		return nil
	}
	var assets []db.MediaAsset
	var mediaErr error
	switch {
	case len(result.ResultURLs) > 0:
		assets, mediaErr = ensureProfileTaskResultMedia(c.Request.Context(), runID, kind, result.ResultURLs)
	case succeeded && profileContentMediaEligible(op, kind):
		assets, mediaErr = ensureProfileTaskContentMedia(c.Request.Context(), runID, kind)
	case succeeded && policy == protocol.MediaRetentionRequired:
		mediaErr = errors.New("required media result has no source URL or content endpoint")
	default:
		return nil
	}
	if policy == protocol.MediaRetentionBestEffort {
		if mediaErr != nil {
			log.Printf("[PROFILE_MEDIA] enqueue best-effort task %s: %v", runID, mediaErr)
		}
		return nil
	}
	if mediaErr == nil && len(assets) == 0 {
		mediaErr = errors.New("required media result did not create any media assets")
	}
	providerPayload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if err := db.UpdateTaskRunResultContext(c.Request.Context(), runID, string(providerPayload), false); err != nil {
		return err
	}
	// gateway_wait URL results are materialized synchronously by the caller;
	// retain the original metadata until that attempt has finished.
	if op.PollingMode == protocol.PollingGatewayWait && len(result.ResultURLs) > 0 {
		return nil
	}
	// Enqueue and projection are separate durable writes. A worker may finish
	// between them, so re-read the complete asset batch in the state transaction
	// instead of regressing a completed task using the enqueue's stale snapshot.
	ready := false
	if err := db.SQLDBForContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
		txCtx := db.WithTx(c.Request.Context(), tx)
		if mediaErr == nil {
			var err error
			ready, err = profileTaskMediaReady(txCtx, runID, kind)
			if err != nil {
				return err
			}
			if ready {
				return db.CompleteTaskRunAfterMediaContext(txCtx, runID, kind)
			}
		}
		if err := db.UpdateTaskRunStatusContext(txCtx, runID, "materializing", "pending"); err != nil {
			return err
		}
		if mediaErr != nil {
			return tx.Model(&db.TaskRun{}).Where("id = ?", runID).Update("task_error", strings.TrimSpace(profileMediaErrorMessage(mediaErr))).Error
		}
		return nil
	}); err != nil {
		return err
	}
	if ready {
		run, err := db.GetTaskRunContext(c.Request.Context(), runID)
		if err != nil {
			return err
		}
		if !attachProfileManagedMedia(c, run, kind, response) {
			return errors.New("required media result is unavailable")
		}
		setProfileMediaResponseStatus(response, model.VideoStatusCompleted)
		return nil
	}
	clearProfileMediaPayload(response)
	setProfileMediaResponseStatus(response, "materializing")
	switch payload := response.(type) {
	case map[string]any:
		if mediaErr != nil {
			payload["error"] = profileMediaErrorMessage(mediaErr)
		}
	case *model.VideoTaskResponse:
		if mediaErr != nil {
			payload.Error = profileMediaErrorMessage(mediaErr)
		}
	}
	return nil
}

// Call inside the lifecycle projection transaction so a materializer's
// completed batch cannot be overwritten using an earlier enqueue snapshot.
func profileTaskMediaReady(ctx context.Context, runID, kind string) (bool, error) {
	assets, err := db.ListMediaAssetsForTaskRunContext(ctx, runID, kind)
	return err == nil && profileMediaAssetsAvailable(assets), err
}

func profileResultSucceeded(result protocol.Result, op protocol.Operation) bool {
	status := strings.TrimSpace(result.Status)
	if status == "" && op.Poll != nil {
		status = profileJSONValueString(profileSelectJSON(result.JSON, op.Poll.StatusPath))
	}
	if status == "" {
		status = profileJSONString(result.JSON, "status")
	}
	return model.NormalizeTaskStatus(status) == model.VideoStatusCompleted || op.Poll != nil && containsProfileFold(op.Poll.SuccessValues, status)
}

func setProfileMediaResponseStatus(response any, status string) {
	switch payload := response.(type) {
	case map[string]any:
		payload["status"] = status
	case *model.VideoTaskResponse:
		payload.Status = status
	}
}
