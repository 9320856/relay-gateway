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
	if ctx == nil {
		ctx = context.Background()
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
	owner = run.LeaseOwner
	workCtx, stopLease := KeepTaskRunLeaseAlive(ctx, run, p.lease())
	defer stopLease()
	started := time.Now()
	observation, pollErr := p.Poll(workCtx, run)
	finished := time.Now()
	if renewalErr := stopLease(); renewalErr != nil && ctx.Err() == nil {
		if errors.Is(renewalErr, db.ErrTaskLeaseOwner) {
			return true, nil
		}
		return true, renewalErr
	}
	if pollErr != nil {
		observation.Success = false
		if observation.Error == "" {
			observation.Error = pollErr.Error()
		}
	}
	// A shutdown still records and releases the in-flight poll, but this
	// durable cleanup has a bound and cannot wait forever for SQLite's pool.
	commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = db.WithTaskRunLeaseContext(commitCtx, run.ID, owner, func(txCtx context.Context) error {
		if !observation.SkipPollAccounting {
			if err := db.RecordTaskPollForLeaseContext(txCtx, run.ID, owner, observation.Success, observation.HTTPStatus); err != nil {
				return err
			}
			if err := db.AppendTaskAttemptContext(txCtx, &db.TaskAttempt{TaskRunID: run.ID, AttemptType: "poll", StartedAt: started, FinishedAt: &finished, HTTPStatus: observation.HTTPStatus, Outcome: pollOutcome(observation), Error: observation.Error}); err != nil {
				return err
			}
		}
		if strings.TrimSpace(observation.Status) != "" {
			if err := db.UpdateTaskRunStatusForLeaseContext(txCtx, run.ID, owner, observation.Status, observation.Outcome); err == nil {
				if _, err := db.AppendTaskEvent(txCtx, run.ID, "status_changed", observation.Status); err != nil {
					return err
				}
			} else if !errors.Is(err, db.ErrTaskStateRegression) && !errors.Is(err, db.ErrTaskAlreadyTerminal) {
				return err
			}
		}
		current, err := db.GetTaskRunContext(txCtx, run.ID)
		if err != nil {
			return err
		}
		if isTerminal(current.TaskStatus) || observation.PausePolling {
			return db.ReleaseTaskRunLeaseContext(txCtx, run.ID, owner)
		}
		next := observation.NextPollAt
		if next.IsZero() {
			next = time.Now().Add(p.retryAfter())
		}
		return db.RescheduleTaskRunPollContext(txCtx, run.ID, owner, next)
	})
	if errors.Is(err, db.ErrTaskLeaseOwner) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return true, pollErr
}

func (p *BackgroundPoller) Start(ctx context.Context) error {
	if p == nil || p.Poll == nil {
		return errors.New("background poll function is required")
	}
	return runPollWorkers(ctx, p.Workers, 32, p.RunOnce)
}

// runPollWorkers bounds worker concurrency and joins in-flight work on shutdown.
func runPollWorkers(ctx context.Context, workers, maxWorkers int, runOnce func(context.Context) (bool, error)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > maxWorkers {
		workers = maxWorkers
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				claimed, err := runOnce(ctx)
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
