package task

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"relay-gateway/db"
)

type Observation struct {
	Status             string
	Outcome            string
	HTTPStatus         int
	Success            bool
	Error              string
	NextPollAt         time.Time
	SkipPollAccounting bool
	PausePolling       bool
}

type PollFunc func(context.Context, *db.TaskRun) (Observation, error)

// BackgroundPoller owns only background polling. Claiming and rescheduling
// are durable; PollFunc is invoked outside database transactions and must never
// submit a new provider task.
type BackgroundPoller struct {
	Owner      string
	Lease      time.Duration
	Workers    int
	Poll       PollFunc
	RetryAfter time.Duration
}

func (p *BackgroundPoller) RunOnce(ctx context.Context) (bool, error) {
	if p == nil || p.Poll == nil {
		return false, errors.New("background poll function is required")
	}
	owner := strings.TrimSpace(p.Owner)
	if owner == "" {
		return false, errors.New("background poll owner is required")
	}
	run, err := db.ClaimDueTaskRunContext(ctx, owner, p.lease())
	if errors.Is(err, db.ErrTaskLeaseUnavailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	started := time.Now()
	observation, pollErr := p.Poll(ctx, run)
	finished := time.Now()
	if pollErr != nil {
		observation.Success = false
		if observation.Error == "" {
			observation.Error = pollErr.Error()
		}
	}
	if !observation.SkipPollAccounting {
		if err := db.RecordTaskPollForLease(run.ID, owner, observation.Success, observation.HTTPStatus); err != nil {
			if errors.Is(err, db.ErrTaskLeaseOwner) {
				return true, nil
			}
			return true, err
		}
		_ = db.AppendTaskAttempt(&db.TaskAttempt{TaskRunID: run.ID, AttemptType: "poll", StartedAt: started, FinishedAt: &finished, HTTPStatus: observation.HTTPStatus, Outcome: pollOutcome(observation), Error: observation.Error})
	}
	if strings.TrimSpace(observation.Status) != "" {
		if err := db.UpdateTaskRunStatusForLease(run.ID, owner, observation.Status, observation.Outcome); err == nil {
			_, _ = db.AppendTaskEvent(context.Background(), run.ID, "status_changed", observation.Status)
		} else if errors.Is(err, db.ErrTaskLeaseOwner) {
			return true, nil
		}
	}
	if isTerminal(observation.Status) {
		if err := db.ReleaseTaskRunLease(run.ID, owner); err != nil {
			return true, err
		}
		return true, nil
	}
	if observation.PausePolling {
		if err := db.ReleaseTaskRunLease(run.ID, owner); err != nil {
			return true, err
		}
		return true, pollErr
	}
	next := observation.NextPollAt
	if next.IsZero() {
		next = time.Now().Add(p.retryAfter())
	}
	if err := db.RescheduleTaskRunPoll(run.ID, owner, next); err != nil {
		return true, err
	}
	return true, pollErr
}

func (p *BackgroundPoller) Start(ctx context.Context) error {
	if p == nil || p.Poll == nil {
		return errors.New("background poll function is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	workers := p.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > 32 {
		workers = 32
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
				if !claimed {
					if !waitContext(ctx, 250*time.Millisecond) {
						return
					}
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (p *BackgroundPoller) lease() time.Duration {
	if p.Lease > 0 {
		return p.Lease
	}
	return 2 * time.Minute
}

func (p *BackgroundPoller) retryAfter() time.Duration {
	if p.RetryAfter > 0 {
		return p.RetryAfter
	}
	return 5 * time.Second
}

func pollOutcome(observation Observation) string {
	if observation.Success && observation.Error == "" {
		return "success"
	}
	return "failed"
}

func isTerminal(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "completed", "succeeded", "failed", "cancelled", "canceled", "expired", "timeout", "timed_out":
		return true
	default:
		return false
	}
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
