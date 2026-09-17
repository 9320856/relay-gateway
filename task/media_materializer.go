package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/protocol"
)

const defaultMediaMaterializationMaxBytes = int64(512 << 20)

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
	result := media.MediaResult{
		SourceKind:  asset.SourceKind,
		Locator:     asset.SourceLocator,
		ContentType: asset.ContentType,
		MaxBytes:    defaultMediaMaterializationMaxBytes,
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
	info, err := (&media.MaterializationWorker{Store: p.Store, Fetcher: fetcher}).Materialize(ctx, result, mediaObjectKey(asset.PublicID, asset.Kind, result.ContentType))
	if err != nil {
		return p.failWithAsset(job, asset.ID, err)
	}
	objectID := mediaObjectID(info.Key, info.SHA256)
	if err := ensureMediaObject(ctx, objectID, info); err != nil {
		return p.failWithAsset(job, asset.ID, err)
	}
	if err := db.MarkMediaAssetAvailableContext(ctx, asset.ID, objectID, info.ContentType, info.SHA256, info.Size); err != nil {
		if errors.Is(err, db.ErrMediaAssetGone) {
			return p.complete(job)
		}
		return p.fail(job, err)
	}
	_ = db.CompleteTaskRunAfterMediaContext(ctx, asset.TaskRunID, asset.Kind)
	return p.complete(job)
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
	if !strings.EqualFold(strings.TrimSpace(run.Engine), "profile") {
		return media.FetchedSource{}, errors.New("profile content media task is not Profile-owned")
	}
	revision, err := db.GetProtocolProfileRevisionContext(ctx, run.ProfileID, run.ProfileRevision)
	if err != nil {
		return media.FetchedSource{}, fmt.Errorf("load profile content revision: %w", err)
	}
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		return media.FetchedSource{}, fmt.Errorf("decode profile content revision: %w", err)
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		return media.FetchedSource{}, fmt.Errorf("compile profile content revision: %w", err)
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
	// Do not follow a provider redirect here. A redirecting provider response
	// can be retried through its declared Content operation, while following it
	// inside this worker could forward credentials to an unrelated host.
	client := &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	upstream := channel.ToUpstreamChannel()
	content, err := protocol.NewHTTPExecutor(client).FetchContent(ctx, compiled, run.Operation, protocol.Request{
		BaseURL: upstream.BaseURL, APIKeys: upstream.GetEffectiveKeys(), Headers: upstream.Headers, TaskID: run.ProviderTaskID,
	})
	if err != nil {
		return media.FetchedSource{}, fmt.Errorf("fetch profile content: %w", err)
	}
	if content.Body == nil {
		return media.FetchedSource{}, errors.New("profile content response body is missing")
	}
	if content.HTTPStatus < http.StatusOK || content.HTTPStatus >= http.StatusMultipleChoices {
		_ = content.Body.Close()
		return media.FetchedSource{}, fmt.Errorf("profile content returned status %d", content.HTTPStatus)
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

func (p *MediaMaterializationPoller) complete(job *db.MediaMaterializationJob) (bool, error) {
	if job == nil {
		return true, errors.New("media materialization job is required")
	}
	err := db.CompleteMediaMaterializationJobForLeaseContext(context.Background(), job.ID, job.LeaseOwner)
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
	err := db.FailMediaMaterializationJobForLeaseContext(context.Background(), job.ID, job.LeaseOwner, cause.Error(), next)
	if errors.Is(err, db.ErrMediaJobLeaseOwner) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return true, cause
}

func (p *MediaMaterializationPoller) failWithAsset(job *db.MediaMaterializationJob, assetID uint, cause error) (bool, error) {
	if cause == nil {
		cause = errors.New("media materialization failed")
	}
	next := time.Now().Add(p.retryAfter())
	if job == nil {
		return true, cause
	}
	err := db.FailMediaMaterializationJobForLeaseContext(context.Background(), job.ID, job.LeaseOwner, cause.Error(), next)
	if errors.Is(err, db.ErrMediaJobLeaseOwner) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	if markErr := db.MarkMediaAssetFailedContext(context.Background(), assetID, cause.Error()); markErr != nil && !errors.Is(markErr, db.ErrMediaAssetGone) {
		return true, markErr
	}
	return true, cause
}

func (p *MediaMaterializationPoller) Start(ctx context.Context) error {
	if p == nil || p.Store == nil {
		return errors.New("media materialization store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workers := p.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > 16 {
		workers = 16
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				claimed, err := p.RunOnce(ctx)
				if err != nil && !claimed {
					if !waitContext(ctx, time.Second) {
						return
					}
					continue
				}
				if !claimed && !waitContext(ctx, 250*time.Millisecond) {
					return
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (p *MediaMaterializationPoller) fetcher() media.SourceFetcher {
	if p.Fetcher != nil {
		return p.Fetcher
	}
	return media.HTTPSourceFetcher{}
}

func defaultURLFetcherForAsset(ctx context.Context, asset *db.MediaAsset) media.HTTPSourceFetcher {
	fetcher := media.HTTPSourceFetcher{}
	if asset == nil || strings.TrimSpace(asset.TaskRunID) == "" {
		return fetcher
	}
	run, err := db.GetTaskRunContext(ctx, asset.TaskRunID)
	if err != nil || !strings.EqualFold(strings.TrimSpace(run.Engine), "profile") || strings.TrimSpace(run.ChannelID) == "" {
		return fetcher
	}
	channel, err := db.GetChannelModelContext(ctx, run.ChannelID)
	if err != nil {
		return fetcher
	}
	host, ok := media.TrustedFakeIPHost(asset.SourceLocator, channel.BaseURL)
	if !ok {
		return fetcher
	}
	fetcher.TrustedFakeIPHosts = map[string]struct{}{host: {}}
	return fetcher
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

func mediaObjectKey(publicID, kind, contentType string) string {
	return media.ProfileObjectKey(publicID, kind, contentType)
}

func mediaObjectID(key, checksum string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(key) + "\x00" + strings.TrimSpace(checksum)))
	return "obj_" + hex.EncodeToString(sum[:])[:60]
}

func ensureMediaObject(ctx context.Context, objectID string, info media.ObjectInfo) error {
	err := db.CreateMediaObjectContext(ctx, &db.MediaObject{ID: objectID, Backend: "local", StorageKey: info.Key, SHA256: info.SHA256, ByteSize: info.Size, ContentType: info.ContentType, ETag: info.SHA256, State: db.MediaObjectReady})
	if err == nil {
		return nil
	}
	existing, getErr := db.GetMediaObjectByIDContext(ctx, objectID)
	if getErr == nil && existing.StorageKey == info.Key && existing.SHA256 == info.SHA256 && existing.State == db.MediaObjectReady {
		return nil
	}
	return fmt.Errorf("persist media object: %w", err)
}

func (p *MediaMaterializationPoller) String() string {
	if p == nil {
		return "MediaMaterializationPoller(<nil>)"
	}
	return fmt.Sprintf("MediaMaterializationPoller(%s)", strings.TrimSpace(p.Owner))
}
