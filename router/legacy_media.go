package router

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"net/http"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/model"
	"strings"
)

type legacyMediaPolicyKey struct{}
type legacyMediaPolicy struct{ ChannelID, Retention string }

type channelMediaResponsePolicyKey struct{}

func setChannelMediaResponsePolicy(c *gin.Context, policy string) {
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), channelMediaResponsePolicyKey{}, policy))
}

func preserveOriginalMediaResponse(c *gin.Context) bool {
	policy, ok := c.Request.Context().Value(channelMediaResponsePolicyKey{}).(string)
	return ok && policy != media.RetentionRequired
}

func freezeLegacyChannelPolicy(c *gin.Context, channel *config.UpstreamChannel) {
	if c == nil || channel == nil {
		return
	}
	policy, err := media.NormalizeRetention(channel.MediaRetention)
	if err != nil {
		return
	}
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), legacyMediaPolicyKey{}, legacyMediaPolicy{channel.ID, policy}))
}

func retainLegacySynchronousMedia(c *gin.Context, channel *config.UpstreamChannel, kind string, payload any) error {
	if channel == nil {
		return nil
	}
	owner := profileTaskRunID(channel.ID, profileRequestID(c), "legacy."+kind)
	return retainMediaResponse(c, owner, kind, channel.BaseURL, channel.MediaRetention, payload, collectChannelMediaSources(payload, kind, channel.BaseURL))
}

func legacyMediaRun(ctx context.Context, lookupID, kind string) (*db.TaskRun, error) {
	run, err := db.GetTaskRunByAliasContext(ctx, lookupID)
	if err == nil && strings.EqualFold(run.TaskKind, kind) {
		return run, nil
	}
	if err == nil {
		err = gorm.ErrRecordNotFound
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	mapping := db.GetTaskMappingForKindContext(ctx, lookupID, kind)
	if mapping == nil {
		return nil, gorm.ErrRecordNotFound
	}
	registration := legacyRegistrationFromMapping(mapping, "")
	ensureLegacyTaskRun(registration)
	return db.GetTaskRunContext(ctx, legacyTaskRunID(registration))
}

// The provider result is persisted before the local media gate changes the
// public projection. Subsequent polls and workers therefore need no Submit.
func retainLegacyTaskMedia(c *gin.Context, channel *config.UpstreamChannel, lookupID, kind, status string, payload any) error {
	run, err := legacyMediaRun(c.Request.Context(), lookupID, kind)
	if err != nil {
		// The provider has already accepted a create. Even when both local
		// persistence and recovery fail, preserve that acceptance without
		// exposing required media or encouraging another paid submission.
		if selected, ok := c.Request.Context().Value(legacyMediaPolicyKey{}).(legacyMediaPolicy); ok && channel != nil && selected.ChannelID == channel.ID {
			setChannelMediaResponsePolicy(c, selected.Retention)
			_ = c.Error(err)
			if selected.Retention == media.RetentionRequired {
				clearProfileMediaPayload(payload)
				setLegacyMediaPending(payload, err)
			}
			return nil
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	if channel == nil || run.ChannelID != channel.ID || run.TaskKind != kind || run.Engine != "legacy" {
		return errors.New("media task does not match its fixed channel and kind")
	}
	policy, err := db.TaskMediaRetention(c.Request.Context(), run)
	setChannelMediaResponsePolicy(c, policy)
	if err != nil || policy == media.RetentionDisabled {
		return err
	}
	if model.NormalizeTaskStatus(status) != model.VideoStatusCompleted && status != "materializing" {
		if policy == media.RetentionRequired {
			clearProfileMediaPayload(payload)
		}
		return nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err = db.UpdateTaskRunResultContext(c.Request.Context(), run.ID, string(encoded), false); err != nil {
		if policy == media.RetentionBestEffort {
			_ = c.Error(err)
			return nil
		}
		return err
	}
	sources := profileResultURLs(string(encoded), kind)
	if kind == "video" {
		public := sources[:0]
		for _, source := range sources {
			if !isGatewayStableVideoContentURL(c, source) {
				public = append(public, source)
			}
		}
		sources = public
	}
	if kind == "image" {
		sources = normalizeProfileImageSources(sources, channel.BaseURL)
	} else {
		sources = media.NormalizeMediaSources(sources, channel.BaseURL)
	}
	var assets []db.MediaAsset
	if len(sources) > 0 {
		assets, err = db.EnsureTaskResultMediaContext(c.Request.Context(), run.ID, kind, sources)
	} else if kind == "video" {
		assets, err = db.EnsureTaskContentMediaContext(c.Request.Context(), run.ID, kind)
	} else {
		err = errors.New("successful media task contains no downloadable media source")
	}
	if err != nil {
		if policy == media.RetentionBestEffort {
			_ = c.Error(err)
			return nil
		}
		clearProfileMediaPayload(payload)
		setLegacyMediaPending(payload, err)
		_ = db.UpdateTaskRunStatusContext(c.Request.Context(), run.ID, "materializing", "pending")
		return nil
	}
	if policy == media.RetentionRequired {
		// The worker can finish immediately after enqueue. Read the whole
		// batch and project its state within one transaction.
		err = db.DBForContext(c.Request.Context()).Transaction(func(tx *gorm.DB) error {
			txCtx := db.WithTx(c.Request.Context(), tx)
			assets, err = db.ListMediaAssetsForTaskRunContext(txCtx, run.ID, kind)
			if err != nil {
				return err
			}
			if profileMediaAssetsAvailable(assets) {
				return db.CompleteTaskRunAfterMediaContext(txCtx, run.ID, kind)
			}
			return db.UpdateTaskRunStatusContext(txCtx, run.ID, "materializing", "pending")
		})
		if err != nil {
			return err
		}
	}
	ready := len(assets) > 0
	replacements := make(map[string]string)
	managed := make([]map[string]string, 0, len(assets))
	for index := range assets {
		asset := &assets[index]
		if asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" {
			ready = false
			continue
		}
		capability, capErr := db.RecoverMediaAssetCapability(asset)
		if capErr != nil {
			ready = false
			continue
		}
		stable := mediaPublicURL(c, asset.PublicID, capability)
		managed = append(managed, map[string]string{"url": stable})
		replacements[asset.SourceLocator] = stable
		if asset.Ordinal >= 0 && asset.Ordinal < len(sources) {
			replacements[sources[asset.Ordinal]] = stable
		}
	}
	if policy == media.RetentionRequired && !ready {
		clearProfileMediaPayload(payload)
		setLegacyMediaPending(payload, nil)
		return nil
	}
	if len(managed) > 0 {
		for _, original := range profileResultURLs(string(encoded), kind) {
			normalized := media.NormalizeMediaSources([]string{original}, channel.BaseURL)
			if kind == "image" {
				normalized = normalizeProfileImageSources([]string{original}, channel.BaseURL)
			}
			if len(normalized) == 1 {
				if stable, exists := replacements[normalized[0]]; exists {
					replacements[original] = stable
				}
			}
		}
		if err := replaceMediaPayload(payload, replacements); err != nil {
			return err
		}
		if video, ok := payload.(*model.VideoTaskResponse); ok {
			video.VideoURL, video.URL, video.Data = managed[0]["url"], managed[0]["url"], managed
			if ready {
				video.Status = model.VideoStatusCompleted
			}
		}
		if image, ok := payload.(map[string]any); ok && ready {
			image["status"] = "completed"
		}
		if policy == media.RetentionRequired {
			_ = db.CompleteTaskRunAfterMediaContext(c.Request.Context(), run.ID, kind)
		}
	}
	return nil
}

func setLegacyMediaPending(payload any, err error) {
	if video, ok := payload.(*model.VideoTaskResponse); ok {
		video.Status = "materializing"
		if err != nil {
			video.Error = "媒体保存失败，可重试下载"
		}
	}
	if image, ok := payload.(map[string]any); ok {
		image["status"] = "materializing"
		if err != nil {
			image["error"] = "媒体保存失败，可重试下载"
		}
	}
}

func legacyManagedVideoContent(c *gin.Context, taskID string) (bool, error) {
	run, err := legacyMediaRun(c.Request.Context(), taskID, "video")
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if !strings.EqualFold(run.Engine, "legacy") {
		return false, nil
	}
	policy, err := db.TaskMediaRetention(c.Request.Context(), run)
	if err != nil {
		return true, err
	}
	if policy == media.RetentionDisabled {
		return false, nil
	}
	if handled, err := serveProfileManagedVideo(c, run); handled || err != nil {
		return handled, err
	}
	if policy != media.RetentionRequired {
		return false, nil
	}
	c.Header("Retry-After", "2")
	c.JSON(http.StatusAccepted, gin.H{"status": "materializing"})
	return true, nil
}

// While local storage is pending or complete, reuse the persisted provider
// result. Polling its expired URL again cannot improve the media result.
func legacyStoredMediaResponse(c *gin.Context, lookupID, kind string) (any, bool, error) {
	run, err := legacyMediaRun(c.Request.Context(), lookupID, kind)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if run.Engine != "legacy" || (run.TaskStatus != "materializing" && run.TaskStatus != "completed") || run.ResultBody == "" {
		return nil, false, nil
	}
	policy, err := db.TaskMediaRetention(c.Request.Context(), run)
	if err != nil || policy == media.RetentionDisabled {
		return nil, false, err
	}
	channel, err := db.GetChannelModelContext(c.Request.Context(), run.ChannelID)
	if err != nil {
		return nil, true, err
	}
	upstream := channel.ToUpstreamChannel()
	var payload any
	if kind == "video" {
		video := &model.VideoTaskResponse{}
		if err := json.Unmarshal([]byte(run.ResultBody), video); err != nil {
			return nil, true, err
		}
		payload = video
	} else {
		var image map[string]any
		if err := json.Unmarshal([]byte(run.ResultBody), &image); err != nil {
			return nil, true, err
		}
		payload = image
	}
	return payload, true, retainLegacyTaskMedia(c, &upstream, lookupID, kind, "completed", payload)
}

func serveStoredLegacyMediaStatus(c *gin.Context, lookupID, kind string, playground bool) bool {
	payload, handled, err := legacyStoredMediaResponse(c, lookupID, kind)
	if !handled && err == nil {
		return false
	}
	if err != nil {
		if playground {
			sendPlaygroundError(c, err, "读取媒体任务失败", 0, "")
		} else {
			writeUpstreamError(c, err, "读取媒体任务失败")
		}
		return true
	}
	if !playground {
		c.JSON(http.StatusOK, payload)
		return true
	}
	run, err := legacyMediaRun(c.Request.Context(), lookupID, kind)
	if err != nil {
		sendPlaygroundError(c, err, "读取媒体任务失败", 0, "")
		return true
	}
	if video, ok := payload.(*model.VideoTaskResponse); ok {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "task_id": lookupID, "channel_id": run.ChannelID, "task_status": video.Status, "video_url": playgroundVideoResultURL(c, video, true), "progress": video.Progress, "response": payload})
	} else {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "task_id": strings.TrimPrefix(lookupID, imageTaskIDPrefix), "channel_id": run.ChannelID, "task_status": playgroundImageTaskStatus(payload, ""), "images": playgroundImageURLs(payload), "response": payload})
	}
	return true
}

func legacyRequiredMediaReady(ctx context.Context, run *db.TaskRun, kind string) bool {
	assets, err := db.ListMediaAssetsForTaskRunContext(ctx, run.ID, kind)
	if err != nil || len(assets) == 0 {
		return false
	}
	for _, asset := range assets {
		if asset.Status != db.MediaAssetAvailable || asset.ObjectID == "" {
			return false
		}
	}
	return true
}
