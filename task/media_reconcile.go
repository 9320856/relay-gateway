package task

import (
	"context"
	"errors"
	"strings"
	"time"

	"relay-gateway/db"
	"relay-gateway/media"
)

// MediaReconcileReport summarizes one local object directory scan.
type MediaReconcileReport struct {
	ScannedFiles       int
	TemporaryFiles     int
	MissingDBObjects   int
	MissingObjectFiles int
	OrphanFiles        int
	RemovedFiles       int
	Changed            int
	Errors             int
}

// MediaReconcilePoller audits the local object directory against media_objects.
// It never submits, polls, or mutates media assets. Cleanup is opt-in.
type MediaReconcilePoller struct {
	Store         *media.LocalObjectStore
	DeleteOrphans bool
	SafetyWindow  time.Duration
	ScanInterval  time.Duration
}

func NewMediaReconcilePoller(store *media.LocalObjectStore) *MediaReconcilePoller {
	return &MediaReconcilePoller{Store: store, SafetyWindow: 15 * time.Minute, ScanInterval: time.Minute}
}

// RunOnce scans the store and returns whether anything was found. A nil DB is
// an error because without durable records no orphan decision is trustworthy.
func (p *MediaReconcilePoller) RunOnce(ctx context.Context) (MediaReconcileReport, error) {
	var report MediaReconcileReport
	if p == nil || p.Store == nil {
		return report, errors.New("media reconcile store is required")
	}
	if db.DB == nil {
		return report, errors.New("database is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var objects []db.MediaObject
	if err := db.DB.WithContext(ctx).Where("state <> ?", db.MediaObjectDeleted).Find(&objects).Error; err != nil {
		return report, err
	}
	known := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		key := strings.TrimSpace(object.StorageKey)
		if key != "" {
			known[key] = struct{}{}
		}
	}
	files, err := p.Store.ListFiles(ctx)
	if err != nil {
		return report, err
	}
	report.ScannedFiles = len(files)
	now := time.Now()
	window := p.safetyWindow()
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		if mediaPart(file.Key) {
			report.TemporaryFiles++
		} else if _, ok := known[file.Key]; !ok {
			report.OrphanFiles++
		}
		if !p.DeleteOrphans || now.Sub(file.ModTime) < window {
			continue
		}
		// The second known check is deliberately explicit: temporary files and
		// unknown regular files are removable only after the safety window.
		if _, ok := known[file.Key]; ok && !mediaPart(file.Key) {
			continue
		}
		if err := p.Store.Delete(ctx, file.Key); err != nil && !errors.Is(err, media.ErrNotFound) {
			report.Errors++
			continue
		}
		report.RemovedFiles++
		report.Changed++
	}
	for _, object := range objects {
		if strings.TrimSpace(object.StorageKey) == "" {
			continue
		}
		if _, err := p.Store.Stat(ctx, object.StorageKey); errors.Is(err, media.ErrNotFound) {
			report.MissingObjectFiles++
		} else if err != nil {
			report.Errors++
		}
	}
	return report, nil
}

// Start periodically runs the same idempotent audit until ctx is cancelled.
func (p *MediaReconcilePoller) Start(ctx context.Context) error {
	if p == nil || p.Store == nil {
		return errors.New("media reconcile store is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	interval := p.scanInterval()
	_, _ = p.RunOnce(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, _ = p.RunOnce(ctx)
		}
	}
}

func (p *MediaReconcilePoller) safetyWindow() time.Duration {
	if p.SafetyWindow > 0 {
		return p.SafetyWindow
	}
	return 15 * time.Minute
}
func (p *MediaReconcilePoller) scanInterval() time.Duration {
	if p.ScanInterval > 0 {
		return p.ScanInterval
	}
	return time.Minute
}
func mediaPart(key string) bool { return strings.HasSuffix(strings.TrimSpace(key), ".part") }
