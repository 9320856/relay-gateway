package protocol

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	ErrPollFailed  = errors.New("upstream task failed")
	ErrPollTimeout = errors.New("upstream task polling timed out")
)

type AsyncRunResult struct {
	Accepted     Result
	Final        *Result
	PollCount    int
	PollAttempts []PollAttempt
}

type PollAttempt struct {
	StartedAt  time.Time
	FinishedAt time.Time
	HTTPStatus int
	Outcome    string
	Error      string
}

type AsyncEngine struct {
	Submitter OperationExecutor
	Querier   StatusQuerier
	Clock     func() time.Time
	Sleep     func(context.Context, time.Duration) error
	// Jitter optionally supplies a bounded [0,max] delay. Tests and callers
	// that need deterministic scheduling can inject it; nil uses a random delay.
	Jitter func(max time.Duration) time.Duration
}

func NewAsyncEngine(submitter OperationExecutor, querier StatusQuerier) *AsyncEngine {
	return &AsyncEngine{Submitter: submitter, Querier: querier, Clock: time.Now, Sleep: sleepContext}
}

// Run performs exactly one Submit. Only gateway_wait owns polling in this
// method; client and background modes return the accepted task for their
// respective caller/worker to invoke PollOnce later.
func (e *AsyncEngine) Run(ctx context.Context, profile CompiledProfile, operation string, req Request) (AsyncRunResult, error) {
	if e == nil || e.Submitter == nil {
		return AsyncRunResult{}, errors.New("async submitter is required")
	}
	op, err := operationFrom(profile, operation)
	if err != nil {
		return AsyncRunResult{}, err
	}
	accepted, err := e.Submitter.Submit(ctx, profile, operation, req)
	if err != nil {
		return AsyncRunResult{}, err
	}
	run := AsyncRunResult{Accepted: accepted}
	if op.ExecutionMode != ExecutionAsync || op.PollingMode != PollingGatewayWait {
		return run, nil
	}
	if e.Querier == nil {
		return run, errors.New("async status querier is required")
	}
	final, polls, attempts, err := e.wait(ctx, profile, operation, req, accepted)
	run.Final, run.PollCount, run.PollAttempts = &final, polls, attempts
	if err != nil {
		return run, err
	}
	return run, nil
}

func (e *AsyncEngine) PollOnce(ctx context.Context, profile CompiledProfile, operation string, req Request) (Result, error) {
	if e == nil || e.Querier == nil {
		return Result{}, errors.New("async status querier is required")
	}
	return e.Querier.PollOnce(ctx, profile, operation, req)
}

// Wait owns a bounded gateway_wait loop. A provider error is retained while
// the operation still has budget; no second Submit is ever attempted.
func (e *AsyncEngine) Wait(ctx context.Context, profile CompiledProfile, operation string, req Request, accepted Result) (Result, int, error) {
	final, polls, _, err := e.wait(ctx, profile, operation, req, accepted)
	return final, polls, err
}

func (e *AsyncEngine) wait(ctx context.Context, profile CompiledProfile, operation string, req Request, accepted Result) (Result, int, []PollAttempt, error) {
	if e == nil || e.Querier == nil {
		return Result{}, 0, nil, errors.New("async status querier is required")
	}
	op, err := operationFrom(profile, operation)
	if err != nil {
		return Result{}, 0, nil, err
	}
	if op.Poll == nil {
		return Result{}, 0, nil, errors.New("operation has no poll definition")
	}
	if strings.TrimSpace(accepted.TaskID) == "" {
		return Result{}, 0, nil, errors.New("accepted result has no task ID")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Bound each gateway-wait operation at the profile's declared duration,
	// including time spent inside an in-flight provider request.
	boundedCtx, cancel := context.WithTimeout(ctx, time.Duration(op.Poll.MaxDurationMS)*time.Millisecond)
	defer cancel()
	now := e.now()
	deadline := now.Add(time.Duration(op.Poll.MaxDurationMS) * time.Millisecond)
	var lastErr error
	attempts := make([]PollAttempt, 0, op.Poll.MaxAttempts)
	for attempt := 1; attempt <= op.Poll.MaxAttempts; attempt++ {
		if err := boundedCtx.Err(); err != nil {
			return Result{}, attempt - 1, attempts, pollTimeoutError(attempt-1, err)
		}
		if !e.now().Before(deadline) {
			return Result{}, attempt - 1, attempts, pollTimeoutError(attempt-1, ErrPollTimeout)
		}
		pollReq := req
		pollReq.TaskID = accepted.TaskID
		started := e.now()
		result, pollErr := e.Querier.PollOnce(boundedCtx, profile, operation, pollReq)
		finished := e.now()
		observation := PollAttempt{StartedAt: started, FinishedAt: finished, HTTPStatus: result.HTTPStatus, Outcome: "pending"}
		if pollErr != nil {
			lastErr = pollErr
			observation.Outcome, observation.Error = "failed", pollErr.Error()
		} else {
			if containsFold(op.Poll.SuccessValues, result.Status) {
				observation.Outcome = "success"
				attempts = append(attempts, observation)
				return result, attempt, attempts, nil
			}
			if containsFold(op.Poll.FailureValues, result.Status) {
				observation.Outcome = "failed"
				attempts = append(attempts, observation)
				return result, attempt, attempts, &PollError{Attempts: attempt, Status: result.Status, Cause: ErrPollFailed}
			}
		}
		attempts = append(attempts, observation)
		if attempt == op.Poll.MaxAttempts {
			break
		}
		delay := op.Poll.nextDelayAt(attempt, result.Headers.Get("Retry-After"), e.Jitter, e.now())
		if delay > 0 {
			if err := e.sleep(boundedCtx, delay); err != nil {
				return Result{}, attempt, attempts, pollTimeoutError(attempt, err)
			}
		}
	}
	if lastErr != nil {
		return Result{}, op.Poll.MaxAttempts, attempts, &PollError{Attempts: op.Poll.MaxAttempts, Cause: lastErr}
	}
	return Result{}, op.Poll.MaxAttempts, attempts, pollTimeoutError(op.Poll.MaxAttempts, ErrPollTimeout)
}

// NextDelay computes the wait before the next Poll attempt. Retry-After takes
// precedence over the profile's backoff, while optional jitter is applied
// afterward. The attempt number is one-based and represents the poll that
// just completed.
func (p Poll) NextDelay(attempt int, retryAfter string, jitter func(time.Duration) time.Duration) time.Duration {
	return p.nextDelayAt(attempt, retryAfter, jitter, time.Now())
}

func (p Poll) nextDelayAt(attempt int, retryAfter string, jitter func(time.Duration) time.Duration, now time.Time) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	baseMS := p.BackoffBaseMS
	if baseMS == 0 {
		baseMS = p.IntervalMS
	}
	delayMS := baseMS
	switch strings.ToLower(strings.TrimSpace(p.BackoffMode)) {
	case "linear":
		delayMS = saturatingMultiplyMS(baseMS, int64(attempt))
	case "exponential":
		delayMS = saturatingPowerOfTwoMS(baseMS, attempt-1)
	}
	if retry, ok := parseRetryAfter(retryAfter, now); ok {
		delayMS = durationToMS(retry)
	}
	if p.JitterMS > 0 {
		maxJitter := msToDuration(p.JitterMS)
		var extra time.Duration
		if jitter != nil {
			extra = jitter(maxJitter)
		} else {
			extra = time.Duration(rand.Int63n(int64(maxJitter) + 1))
		}
		if extra < 0 {
			extra = 0
		}
		if extra > maxJitter {
			extra = maxJitter
		}
		delay := saturatingAdd(msToDuration(delayMS), extra)
		delayMS = durationToMS(delay)
	}
	if p.BackoffMaxMS > 0 && delayMS > p.BackoffMaxMS {
		delayMS = p.BackoffMaxMS
	}
	if delayMS <= 0 {
		return 0
	}
	return msToDuration(delayMS)
}

const maxDurationMS = int64((time.Duration(1<<63 - 1)) / time.Millisecond)

func msToDuration(milliseconds int64) time.Duration {
	if milliseconds <= 0 {
		return 0
	}
	if milliseconds >= maxDurationMS {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func durationToMS(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return int64(duration / time.Millisecond)
}

func saturatingAdd(a, b time.Duration) time.Duration {
	max := time.Duration(1<<63 - 1)
	if b > 0 && a > max-b {
		return max
	}
	if b < 0 && a < time.Duration(-1<<63)-b {
		return time.Duration(-1 << 63)
	}
	return a + b
}

func saturatingMultiplyMS(value, multiplier int64) int64 {
	if value <= 0 || multiplier <= 0 {
		return 0
	}
	if value > (int64(1<<63-1) / multiplier) {
		return int64(1<<63 - 1)
	}
	return value * multiplier
}

func saturatingPowerOfTwoMS(value int64, exponent int) int64 {
	if value <= 0 {
		return 0
	}
	if exponent <= 0 {
		return value
	}
	max := int64(1<<63 - 1)
	for i := 0; i < exponent; i++ {
		if value > max/2 {
			return max
		}
		value *= 2
	}
	return value
}

func parseRetryAfter(raw string, now time.Time) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil && seconds >= 0 {
		maxSeconds := int64((time.Duration(1<<63 - 1)) / time.Second)
		if seconds >= maxSeconds {
			return time.Duration(1<<63 - 1), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(raw)
	if err != nil {
		return 0, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if !when.After(now) {
		return 0, true
	}
	return when.Sub(now), true
}

type PollError struct {
	Attempts int
	Status   string
	Cause    error
}

func (e *PollError) Error() string {
	if e == nil {
		return ""
	}
	if e.Status != "" {
		return fmt.Sprintf("poll failed after %d attempts with status %q", e.Attempts, e.Status)
	}
	return fmt.Sprintf("poll failed after %d attempts: %v", e.Attempts, e.Cause)
}

func (e *PollError) Unwrap() error { return e.Cause }

func pollTimeoutError(attempts int, cause error) error {
	return &PollError{Attempts: attempts, Cause: fmt.Errorf("%w: %v", ErrPollTimeout, cause)}
}

func (e *AsyncEngine) now() time.Time {
	if e != nil && e.Clock != nil {
		return e.Clock()
	}
	return time.Now()
}

func (e *AsyncEngine) sleep(ctx context.Context, duration time.Duration) error {
	if e != nil && e.Sleep != nil {
		return e.Sleep(ctx, duration)
	}
	return sleepContext(ctx, duration)
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func containsFold(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), target) {
			return true
		}
	}
	return false
}
