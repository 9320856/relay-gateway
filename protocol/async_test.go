package protocol

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type fakeAsyncExecutor struct {
	submits  int
	polls    int
	statuses []string
}

func (f *fakeAsyncExecutor) Submit(context.Context, CompiledProfile, string, Request) (Result, error) {
	f.submits++
	return Result{TaskID: "task-1"}, nil
}

func (f *fakeAsyncExecutor) PollOnce(context.Context, CompiledProfile, string, Request) (Result, error) {
	f.polls++
	status := f.statuses[len(f.statuses)-1]
	if len(f.statuses) >= f.polls {
		status = f.statuses[f.polls-1]
	}
	return Result{TaskID: "task-1", Status: status}, nil
}

func TestAsyncEngineGatewayWaitSubmitsOnceAndPollsUntilSuccess(t *testing.T) {
	profile := testExecutorProfile()
	profile.Operations[0].PollingMode = PollingGatewayWait
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeAsyncExecutor{statuses: []string{"processing", "completed"}}
	engine := NewAsyncEngine(fake, fake)
	engine.Sleep = func(context.Context, time.Duration) error { return nil }
	run, err := engine.Run(context.Background(), compiled, "video.create", Request{})
	if err != nil || run.Final == nil || run.Final.Status != "completed" || run.PollCount != 2 || len(run.PollAttempts) != 2 || run.PollAttempts[0].Outcome != "pending" || run.PollAttempts[1].Outcome != "success" || fake.submits != 1 {
		t.Fatalf("run = %+v err=%v submits=%d polls=%d", run, err, fake.submits, fake.polls)
	}
}

func TestAsyncEngineClientAndBackgroundNeverPollInSubmitPath(t *testing.T) {
	profile := testExecutorProfile()
	profile.Operations[0].PollingMode = PollingClient
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeAsyncExecutor{statuses: []string{"completed"}}
	engine := NewAsyncEngine(fake, fake)
	run, err := engine.Run(context.Background(), compiled, "video.create", Request{})
	if err != nil || run.Final != nil || fake.submits != 1 || fake.polls != 0 {
		t.Fatalf("client run = %+v err=%v submits=%d polls=%d", run, err, fake.submits, fake.polls)
	}
	if _, err := engine.PollOnce(context.Background(), compiled, "video.create", Request{TaskID: run.Accepted.TaskID}); err != nil {
		t.Fatal(err)
	}
	if fake.polls != 1 {
		t.Fatalf("explicit client poll count = %d", fake.polls)
	}
}

func TestAsyncEngineReturnsProviderFailureWithoutResubmit(t *testing.T) {
	profile := testExecutorProfile()
	profile.Operations[0].PollingMode = PollingGatewayWait
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeAsyncExecutor{statuses: []string{"failed"}}
	engine := NewAsyncEngine(fake, fake)
	engine.Sleep = func(context.Context, time.Duration) error { return nil }
	_, err = engine.Run(context.Background(), compiled, "video.create", Request{})
	if !errors.Is(err, ErrPollFailed) || fake.submits != 1 {
		t.Fatalf("failure err=%v submits=%d", err, fake.submits)
	}
}

type blockingQuerier struct{}

func (blockingQuerier) PollOnce(ctx context.Context, _ CompiledProfile, _ string, _ Request) (Result, error) {
	<-ctx.Done()
	return Result{}, ctx.Err()
}

func TestAsyncEngineGatewayWaitBoundsInFlightPoll(t *testing.T) {
	profile := testExecutorProfile()
	profile.Operations[0].PollingMode = PollingGatewayWait
	profile.Operations[0].Poll.MaxAttempts = 3
	profile.Operations[0].Poll.MaxDurationMS = 20
	compiled, err := Compile(profile)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeAsyncExecutor{}
	engine := NewAsyncEngine(fake, blockingQuerier{})
	started := time.Now()
	_, err = engine.Run(context.Background(), compiled, "video.create", Request{})
	if !errors.Is(err, ErrPollTimeout) {
		t.Fatalf("error = %v, want timeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("poll was not bounded, elapsed %s", elapsed)
	}
}

func TestPollNextDelayModesCapsRetryAfterAndJitter(t *testing.T) {
	tests := []struct {
		name       string
		poll       Poll
		attempt    int
		retryAfter string
		jitter     func(time.Duration) time.Duration
		want       time.Duration
	}{
		{name: "default uses interval", poll: Poll{IntervalMS: 100}, attempt: 1, want: 100 * time.Millisecond},
		{name: "fixed uses base", poll: Poll{IntervalMS: 100, BackoffMode: "fixed", BackoffBaseMS: 25}, attempt: 4, want: 25 * time.Millisecond},
		{name: "linear", poll: Poll{BackoffMode: "linear", BackoffBaseMS: 25}, attempt: 3, want: 75 * time.Millisecond},
		{name: "exponential", poll: Poll{BackoffMode: "exponential", BackoffBaseMS: 25}, attempt: 3, want: 100 * time.Millisecond},
		{name: "max cap", poll: Poll{BackoffMode: "linear", BackoffBaseMS: 100, BackoffMaxMS: 150}, attempt: 2, want: 150 * time.Millisecond},
		{name: "retry after takes precedence", poll: Poll{IntervalMS: 100}, attempt: 1, retryAfter: "2", want: 2 * time.Second},
		{name: "jitter is bounded and deterministic", poll: Poll{IntervalMS: 10, JitterMS: 7}, attempt: 1, jitter: func(time.Duration) time.Duration { return 7 * time.Millisecond }, want: 17 * time.Millisecond},
		{name: "negative jitter is clamped", poll: Poll{IntervalMS: 10, JitterMS: 7}, attempt: 1, jitter: func(time.Duration) time.Duration { return -time.Second }, want: 10 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.poll.NextDelay(tt.attempt, tt.retryAfter, tt.jitter); got != tt.want {
				t.Fatalf("NextDelay() = %s, want %s", got, tt.want)
			}
		})
	}

	future := time.Now().Add(2 * time.Second).UTC().Format(httpTimeFormat)
	if got := (Poll{}).NextDelay(1, future, nil); got < time.Second || got > 3*time.Second {
		t.Fatalf("HTTP-date Retry-After = %s, want approximately 2s", got)
	}
}

func TestPollNextDelaySaturatesExtremeValues(t *testing.T) {
	max := time.Duration(1<<63 - 1)
	if got := (Poll{BackoffMode: "exponential", BackoffBaseMS: int64(1<<63 - 1), JitterMS: int64(1<<63 - 1)}).NextDelay(2, "", func(time.Duration) time.Duration { return max }); got != max {
		t.Fatalf("extreme backoff = %s, want saturated max duration", got)
	}
	if got := (Poll{}).NextDelay(1, fmt.Sprintf("%d", int64(1<<63-1)), nil); got != max {
		t.Fatalf("extreme Retry-After = %s, want saturated max duration", got)
	}
}

func TestPollNextDelayAtUsesProvidedClockForHTTPDate(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	retryAfter := now.Add(2 * time.Second).Format(httpTimeFormat)
	if got := (Poll{}).nextDelayAt(1, retryAfter, nil, now); got != 2*time.Second {
		t.Fatalf("HTTP-date delay with injected clock = %s, want 2s", got)
	}
}

const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"
