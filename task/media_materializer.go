package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gorm.io/gorm"
	"relay-gateway/adapter"
	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

const defaultMediaMaterializationMaxBytes = int64(512 << 20)

var mediaURLFetcherFactory = media.NewHTTPSourceFetcher

// MediaMaterializationPoller processes durable result URLs. It never submits
// or polls provider tasks, so a retry can only repeat local storage work.
type MediaMaterializationPoller struct {
	Owner      string
	Store      media.ObjectStore
	Fetcher    media.SourceFetcher
	Workers    int
	Lease      time.Duration
	RetryAfter time.Duration
}

func NewMediaMaterializationPoller(owner string, store media.ObjectStore) *MediaMaterializationPoller {
	return &MediaMaterializationPoller{Owner: owner, Store: store, Workers: 2, RetryAfter: 15 * time.Second}
}

func (p *MediaMaterializationPoller) RunOnce(ctx context.Context) (bool, error) {
	if p == nil || p.Store == nil {
		return false, errors.New("media materialization store is required")
	}
	owner := strings.TrimSpace(p.Owner)
	if owner == "" {
		return false, errors.New("media materialization owner is required")
	}
	job, err := db.ClaimMediaMaterializationJobContext(ctx, owner, p.lease())
	if errors.Is(err, db.ErrMediaJobUnavailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	asset, err := db.GetMediaAssetByIDContext(ctx, job.AssetID)
	if err != nil {
		return p.fail(job, err)
	}
	if asset.Status == db.MediaAssetDeleted || asset.Status == db.MediaAssetDeleteRequested || asset.Status == db.MediaAssetDeleteFailed || asset.Status == db.MediaAssetExpired {
		return p.complete(job)
	}
	// A durable asset may already have been materialized by a previous worker
	// attempt (or by a synchronous fast path). Complete a stale job without
	// fetching the provider URL again.
	if asset.Status == db.MediaAssetAvailable && strings.TrimSpace(asset.ObjectID) != "" {
		return p.complete(job)
	}
	fetcher := p.fetcher()
	if strings.EqualFold(strings.TrimSpace(asset.SourceKind), media.SourceBase64) {
		fetcher = media.InlineSourceFetcher{}
	} else if strings.EqualFold(strings.TrimSpace(asset.SourceKind), media.SourceProviderContent) {
		// Provider content is an authenticated Profile operation rather than a
		// public URL. Resolve the frozen TaskRun snapshot at materialization time
		// so the durable job survives restarts without storing credentials or a
		// mutable Profile definition in the asset row.
		fetcher = media.SourceFetcherFunc(func(fetchCtx context.Context, _ media.MediaResult) (media.FetchedSource, error) {
			return fetchProfileProviderContent(fetchCtx, asset.TaskRunID)
		})
	} else if strings.EqualFold(strings.TrimSpace(asset.SourceKind), media.SourceURL) && p.Fetcher == nil {
		fetcher = defaultURLFetcherForAsset(ctx, asset)
	}
	_, err = MaterializeClaimedMedia(ctx, job, asset, p.Store, fetcher, p.lease())
	if err != nil {
		if errors.Is(err, db.ErrMediaAssetGone) || errors.Is(err, db.ErrMediaJobLeaseOwner) {
			return p.complete(job)
		}
		return p.failWithAsset(job, err)
	}
	return true, nil
}

// fetchProfileProviderContent opens the immutable Profile Content operation
// for a completed task. The returned body is consumed by the media worker and
// is never exposed to the caller, so provider credentials stay on the
// upstream request and the media library receives only a local object.
func fetchProfileProviderContent(ctx context.Context, taskRunID string) (media.FetchedSource, error) {
	taskRunID = strings.TrimSpace(taskRunID)
	if taskRunID == "" {
		return media.FetchedSource{}, errors.New("profile content media task run is required")
	}
	run, err := db.GetTaskRunContext(ctx, taskRunID)
	if err != nil {
		return media.FetchedSource{}, fmt.Errorf("load profile content task: %w", err)
	}
	if strings.EqualFold(strings.TrimSpace(run.Engine), "legacy") {
		return fetchLegacyProviderContent(ctx, run)
	}
	if !strings.EqualFold(strings.TrimSpace(run.Engine), "profile") {
		return media.FetchedSource{}, errors.New("content media task has an unknown engine")
	}
	compiled, err := db.LoadTaskProfileContext(ctx, run)
	if err != nil {
		return media.FetchedSource{}, fmt.Errorf("load profile content snapshot: %w", err)
	}
	var op protocol.Operation
	found := false
	for _, candidate := range compiled.Profile().Operations {
		if candidate.Operation == run.Operation {
			op, found = candidate, true
			break
		}
	}
	if !found || op.Content == nil {
		return media.FetchedSource{}, fmt.Errorf("profile operation %q has no content definition", run.Operation)
	}
	channel, err := db.GetChannelModelContext(ctx, run.ChannelID)
	if err != nil {
		return media.FetchedSource{}, fmt.Errorf("load profile content channel: %w", err)
	}
	// Open the authenticated endpoint once, then follow any CDN location with
	// the public media fetcher so credentials never cross to another host.
	client := &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	upstream := channel.ToUpstreamChannel()
	upstream.Headers = maps.Clone(upstream.Headers)
	removeContentRangeHeaders(upstream.Headers)
	// CompiledProfile.Profile returns an owned copy. Removing a configured
	// Range here does not change the persisted snapshot or its digest.
	contentProfile := compiled.Profile()
	for i := range contentProfile.Operations {
		if contentProfile.Operations[i].Operation == run.Operation && contentProfile.Operations[i].Content != nil {
			removeContentRangeHeaders(contentProfile.Operations[i].Content.Headers)
		}
	}
	compiled, err = protocol.Compile(contentProfile)
	if err != nil {
		return media.FetchedSource{}, err
	}
	content, err := service.DefaultDispatcher.ProfileExecutor(protocol.NewHTTPExecutor(client), run.ChannelID).FetchContent(ctx, compiled, run.Operation, protocol.Request{
		BaseURL: upstream.BaseURL, APIKeys: upstream.GetEffectiveKeys(), Headers: upstream.Headers, TaskID: run.ProviderTaskID,
	})
	if err != nil {
		return media.FetchedSource{}, fmt.Errorf("fetch profile content: %w", err)
	}
	if content.Body == nil {
		return media.FetchedSource{}, errors.New("profile content response body is missing")
	}
	if content.HTTPStatus != http.StatusOK || strings.TrimSpace(content.Headers.Get("Content-Range")) != "" {
		if content.HTTPStatus >= 300 && content.HTTPStatus < 400 {
			location, err := url.Parse(strings.TrimSpace(content.Headers.Get("Location")))
			_ = content.Body.Close()
			if err != nil || location == nil || location.String() == "" {
				return media.FetchedSource{}, errors.New("invalid content redirect")
			}
			base, err := url.Parse(content.RequestURL)
			if err != nil {
				return media.FetchedSource{}, err
			}
			return mediaURLFetcherFactory(base.ResolveReference(location).String(), upstream.BaseURL).Fetch(ctx, media.MediaResult{SourceKind: media.SourceURL, Locator: base.ResolveReference(location).String(), MaxBytes: defaultMediaMaterializationMaxBytes})
		}
		_ = content.Body.Close()
		return media.FetchedSource{}, fmt.Errorf("profile content requires a complete response, received %d", content.HTTPStatus)
	}
	contentType := strings.TrimSpace(content.Headers.Get("Content-Type"))
	// Fanren and a few OpenAI-compatible gateways omit the MIME header on the
	// binary endpoint. The task kind is already pinned to video, so retaining a
	// video MIME type lets the local media route play the object with nosniff
	// enabled instead of exposing it as application/octet-stream.
	if contentType == "" || strings.EqualFold(contentType, "application/octet-stream") {
		contentType = "video/mp4"
	}
	return media.FetchedSource{Body: content.Body, ContentType: contentType}, nil
}

func fetchLegacyProviderContent(ctx context.Context, run *db.TaskRun) (media.FetchedSource, error) {
	channel, err := db.GetChannelModelContext(ctx, run.ChannelID)
	if err != nil {
		return media.FetchedSource{}, err
	}
	upstream := channel.ToUpstreamChannel()
	upstream.Headers = maps.Clone(upstream.Headers)
	removeContentRangeHeaders(upstream.Headers)
	opener, ok := adapter.Get(upstream.Type).(adapter.VideoContentOpener)
	if !ok {
		return media.FetchedSource{}, errors.New("channel does not support streaming video content")
	}
	response, err := opener.OpenVideoContent(ctx, &upstream, run.ProviderTaskID, nil)
	if err != nil {
		return media.FetchedSource{}, err
	}
	if response == nil || response.Body == nil {
		return media.FetchedSource{}, errors.New("provider content body is missing")
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		location, err := response.Location()
		response.Body.Close()
		if err != nil {
			return media.FetchedSource{}, err
		}
		return mediaURLFetcherFactory(location.String(), upstream.BaseURL).Fetch(ctx, media.MediaResult{SourceKind: media.SourceURL, Locator: location.String(), MaxBytes: defaultMediaMaterializationMaxBytes})
	}
	if response.StatusCode != http.StatusOK || strings.TrimSpace(response.Header.Get("Content-Range")) != "" {
		response.Body.Close()
		return media.FetchedSource{}, fmt.Errorf("provider content requires a complete response, received %d", response.StatusCode)
	}
	contentType := strings.TrimSpace(response.Header.Get("Content-Type"))
	if contentType == "" || strings.EqualFold(contentType, "application/octet-stream") {
		contentType = "video/mp4"
	}
	return media.FetchedSource{Body: response.Body, ContentType: contentType}, nil
}

func removeContentRangeHeaders(headers map[string]string) {
	for name := range headers {
		if strings.EqualFold(name, "Range") || strings.EqualFold(name, "If-Range") {
			delete(headers, name)
		}
	}
}

func (p *MediaMaterializationPoller) complete(job *db.MediaMaterializationJob) (bool, error) {
	if job == nil {
		return true, errors.New("media materialization job is required")
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.CompleteMediaMaterializationJobForLeaseContext(cleanupCtx, job.ID, job.LeaseOwner)
	if errors.Is(err, db.ErrMediaJobLeaseOwner) {
		return true, nil
	}
	return true, err
}

func (p *MediaMaterializationPoller) fail(job *db.MediaMaterializationJob, cause error) (bool, error) {
	if cause == nil {
		cause = errors.New("media materialization failed")
	}
	next := time.Now().Add(p.retryAfter())
	if job == nil {
		return true, cause
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.FailMediaMaterializationJobForLeaseContext(cleanupCtx, job.ID, job.LeaseOwner, cause.Error(), next)
	if errors.Is(err, db.ErrMediaJobLeaseOwner) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return true, cause
}

func (p *MediaMaterializationPoller) failWithAsset(job *db.MediaMaterializationJob, cause error) (bool, error) {
	if cause == nil {
		cause = errors.New("media materialization failed")
	}
	next := time.Now().Add(p.retryAfter())
	if job == nil {
		return true, cause
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.FailMediaMaterializationWithAssetForLeaseContext(cleanupCtx, job, cause.Error(), next)
	if errors.Is(err, db.ErrMediaJobLeaseOwner) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return true, cause
}

func (p *MediaMaterializationPoller) Start(ctx context.Context) error {
	if p == nil || p.Store == nil {
		return errors.New("media materialization store is required")
	}
	return runPollWorkers(ctx, p.Workers, 16, p.RunOnce)
}

func (p *MediaMaterializationPoller) fetcher() media.SourceFetcher {
	if p.Fetcher != nil {
		return p.Fetcher
	}
	return media.HTTPSourceFetcher{}
}

func defaultURLFetcherForAsset(ctx context.Context, asset *db.MediaAsset) media.HTTPSourceFetcher {
	if asset == nil {
		return media.HTTPSourceFetcher{}
	}
	fetcher := mediaURLFetcherFactory(asset.SourceLocator, "")
	var run *db.TaskRun
	if strings.TrimSpace(asset.TaskRunID) != "" {
		var err error
		run, err = db.GetTaskRunContext(ctx, asset.TaskRunID)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fetcher
		}
	}
	var channelID string
	if run == nil {
		// Direct responses may use a synthetic task ID without persisting a
		// TaskRun. Their durable originating API-call log records the selected
		// upstream, including for background retries of synchronous failures.
		originID := strings.TrimSpace(asset.OriginRequestID)
		conn := db.SQLDBForContext(ctx)
		if originID == "" || conn == nil {
			return fetcher
		}
		var origin db.RequestLogModel
		if err := conn.Select("channel_id").Where("id = ? AND kind = ?", originID, "api_call").First(&origin).Error; err != nil {
			return fetcher
		}
		channelID = origin.ChannelID
	} else {
		// Both engines persist the selected upstream in TaskRun. A damaged
		// existing task must not fall back to an origin log, mutable task alias,
		// or the currently selected/default upstream channel.
		switch strings.ToLower(strings.TrimSpace(run.Engine)) {
		case "profile", "legacy":
		default:
			return fetcher
		}
		channelID = run.ChannelID
	}
	if strings.TrimSpace(channelID) == "" {
		return fetcher
	}
	channel, err := db.GetChannelModelContext(ctx, channelID)
	if err != nil {
		return fetcher
	}
	return mediaURLFetcherFactory(asset.SourceLocator, channel.BaseURL)
}

func (p *MediaMaterializationPoller) lease() time.Duration {
	if p.Lease > 0 {
		return p.Lease
	}
	return 5 * time.Minute
}

func (p *MediaMaterializationPoller) retryAfter() time.Duration {
	if p.RetryAfter > 0 {
		return p.RetryAfter
	}
	return 15 * time.Second
}

func mediaObjectID(key, checksum string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(key) + "\x00" + strings.TrimSpace(checksum)))
	return "obj_" + hex.EncodeToString(sum[:])[:60]
}

func (p *MediaMaterializationPoller) String() string {
	if p == nil {
		return "MediaMaterializationPoller(<nil>)"
	}
	return fmt.Sprintf("MediaMaterializationPoller(%s)", strings.TrimSpace(p.Owner))
}
