package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay-gateway/adapter"
	"relay-gateway/config"
	"relay-gateway/internal/httpforward"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

func setRegressionChannels(t *testing.T, channels []config.UpstreamChannel) {
	t.Helper()
	previous := config.Global
	config.Global = &config.Config{Channels: channels}
	t.Cleanup(func() { config.Global = previous })
}

// Seed breaker state for tests without exposing an unowned health-update path
// in production. Real responses always finish the lease acquired on admission.
func (d *Dispatcher) recordFailure(channelID string) {
	st := d.GetBreaker(channelID)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.recordFailureLocked(channelID, false)
}

func TestLegacyUncertainFailuresTripBreakerWithoutReplaying(t *testing.T) {
	for _, policy := range []RetryPolicy{RetryInference, RetryCreateTask} {
		for _, failure := range []struct {
			name string
			err  error
		}{
			{"503", &adapter.UpstreamHTTPError{StatusCode: http.StatusServiceUnavailable}},
			{"network", &url.Error{Op: "Post", URL: "http://primary", Err: io.ErrUnexpectedEOF}},
			{"streamRead", &adapter.ErrStreamAborted{Err: &httpforward.StreamError{Op: "read", Err: io.ErrUnexpectedEOF}}},
		} {
			t.Run(fmt.Sprintf("policy%d/%s", policy, failure.name), func(t *testing.T) {
				setRegressionChannels(t, []config.UpstreamChannel{
					{ID: "primary", Type: "openai", Models: []string{"m"}, Priority: 1},
					{ID: "backup", Type: "openai", Models: []string{"m"}, Priority: 2},
				})
				d := &Dispatcher{}
				for request := 0; request < 4; request++ {
					var attempted []string
					err := d.ExecuteWithPolicy(context.Background(), "m", "chat", policy, func() bool { return true }, func(ch *config.UpstreamChannel, _ adapter.Adapter) error {
						attempted = append(attempted, ch.ID)
						if ch.ID == "primary" {
							return failure.err
						}
						return nil
					})
					if request < 3 {
						if err == nil || len(attempted) != 1 || attempted[0] != "primary" {
							t.Fatalf("uncertain request was replayed: request=%d attempted=%v error=%v", request, attempted, err)
						}
					} else if err != nil || len(attempted) != 1 || attempted[0] != "backup" {
						t.Fatalf("new request did not bypass cooling primary: attempted=%v error=%v", attempted, err)
					}
				}
				st := d.GetBreaker("primary")
				if st.failCount != 3 || st.cooldownUntil.IsZero() {
					t.Fatalf("missing health feedback: count=%d cooldown=%v", st.failCount, st.cooldownUntil)
				}
			})
		}
	}
}

func TestLegacyCreateReadFailuresTripBreakerWithoutReplaying(t *testing.T) {
	for _, channelType := range []string{"openai", "newapi"} {
		for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
			t.Run(fmt.Sprintf("%s/status%d", channelType, status), func(t *testing.T) {
				var primaryCalls, backupCalls atomic.Int32
				primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					primaryCalls.Add(1)
					// Ending the body before the advertised length produces a plain
					// io.ErrUnexpectedEOF, not a net.Error, from the real HTTP client.
					w.Header().Set("Content-Length", "64")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"id":"partial`)
				}))
				defer primary.Close()
				backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					backupCalls.Add(1)
					_, _ = io.WriteString(w, `{"id":"accepted","status":"queued"}`)
				}))
				defer backup.Close()
				setRegressionChannels(t, []config.UpstreamChannel{
					{ID: "primary", Type: channelType, BaseURL: primary.URL + "/v1", APIKeys: []string{"one", "two"}, Models: []string{"m"}, Priority: 1},
					{ID: "backup", Type: channelType, BaseURL: backup.URL + "/v1", Models: []string{"m"}, Priority: 2},
				})
				d := &Dispatcher{}
				for request := 0; request < 4; request++ {
					var response *model.VideoTaskResponse
					err := d.ExecuteWithPolicy(context.Background(), "m", "video", RetryCreateTask, func() bool { return response == nil }, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
						var err error
						response, err = adp.CreateVideo(context.Background(), ch, &model.VideoGenerationRequest{Model: "m", Prompt: "test"})
						return err
					})
					if request < 3 {
						var source *httpforward.StreamError
						if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.As(err, &source) || source.Op != "read" || primaryCalls.Load() != int32(request+1) || backupCalls.Load() != 0 {
							t.Fatalf("uncertain creation lost read feedback or was replayed: request=%d primary=%d backup=%d error=%v", request, primaryCalls.Load(), backupCalls.Load(), err)
						}
					} else if err != nil || response == nil || response.ID != "accepted" || primaryCalls.Load() != 3 || backupCalls.Load() != 1 {
						t.Fatalf("new create did not bypass broken reader: primary=%d backup=%d response=%+v error=%v", primaryCalls.Load(), backupCalls.Load(), response, err)
					}
				}
			})
		}
	}
}

func TestLegacyDownstreamFailuresDoNotTripBreaker(t *testing.T) {
	for _, failure := range []struct {
		name string
		err  error
	}{
		{"write", &adapter.ErrStreamAborted{Err: &httpforward.StreamError{Op: "write", Err: io.ErrClosedPipe}}},
		{"canceled", &adapter.ErrStreamAborted{Err: context.Canceled}},
	} {
		t.Run(failure.name, func(t *testing.T) {
			setRegressionChannels(t, []config.UpstreamChannel{{ID: "channel", Type: "openai", Models: []string{"m"}}})
			d := &Dispatcher{}
			for i := 0; i < 4; i++ {
				if err := d.ExecuteWithPolicy(context.Background(), "m", "chat", RetryInference, nil, func(*config.UpstreamChannel, adapter.Adapter) error { return failure.err }); err == nil {
					t.Fatal("downstream error disappeared")
				}
			}
			if st := d.GetBreaker("channel"); st.failCount != 0 || !st.cooldownUntil.IsZero() {
				t.Fatalf("downstream failure changed upstream health: count=%d cooldown=%v", st.failCount, st.cooldownUntil)
			}
		})
	}
}

func TestInterleavedModelsHaveIndependentSchedulingPools(t *testing.T) {
	for _, weight := range []int{0, 3} {
		t.Run(fmt.Sprintf("weight%d", weight), func(t *testing.T) {
			secondWeight := 0
			if weight > 0 {
				secondWeight = 1
			}
			setRegressionChannels(t, []config.UpstreamChannel{
				{ID: "A1", Type: "openai", Models: []string{"a"}, Weight: weight},
				{ID: "A2", Type: "openai", Models: []string{"a"}, Weight: secondWeight},
				{ID: "B", Type: "openai", Models: []string{"b"}},
			})
			d := &Dispatcher{}
			hits := map[string]int{}
			for i := 0; i < 100; i++ {
				candidates, err := d.ResolveCandidatesForProtocol("a", "chat")
				if err != nil {
					t.Fatal(err)
				}
				hits[candidates[0].ID]++
				if _, err := d.ResolveCandidatesForProtocol("b", "chat"); err != nil {
					t.Fatal(err)
				}
			}
			wantFirst := 50
			if weight == 3 {
				wantFirst = 75
			}
			if hits["A1"] != wantFirst || hits["A2"] != 100-wantFirst {
				t.Fatalf("mixed models changed routing ratio: %v", hits)
			}
		})
	}
}

func TestSchedulingPoolCacheIsBoundedAndUsesCandidateSets(t *testing.T) {
	setRegressionChannels(t, []config.UpstreamChannel{
		{ID: "A", Type: "openai", Models: []string{"*"}},
		{ID: "B", Type: "openai", Models: []string{"*"}},
	})
	d := &Dispatcher{}
	for i := 0; i < 100; i++ {
		if _, err := d.ResolveCandidatesForProtocol(fmt.Sprintf("arbitrary-model-%d", i), "chat"); err != nil {
			t.Fatal(err)
		}
	}
	if len(d.schedulingPools) != 1 {
		t.Fatalf("model names grew scheduling cache: pools=%d", len(d.schedulingPools))
	}
	for i := 0; i < maxSchedulingPools+1; i++ {
		d.nextSchedulingIndex([]*config.UpstreamChannel{{ID: "fixed"}, {ID: fmt.Sprintf("changing-%d", i)}})
	}
	if len(d.schedulingPools) != maxSchedulingPools {
		t.Fatalf("pool cache exceeded bound: pools=%d", len(d.schedulingPools))
	}
	d.InvalidateChannel("fixed")
	if len(d.schedulingPools) != 0 {
		t.Fatal("channel configuration change retained obsolete scheduling pools")
	}
}

func expireRegressionCooldown(d *Dispatcher, id string) {
	st := d.GetBreaker(id)
	st.mu.Lock()
	// Simulate the end of the real 30-second cooldown without a slow test.
	st.cooldownUntil = time.Now().Add(-time.Second)
	st.mu.Unlock()
}

func TestOldProfileAttemptCannotChangeNewHalfOpenProbe(t *testing.T) {
	for _, oldResult := range []struct {
		name string
		err  error
	}{
		{"cancel", context.Canceled},
		{"success", nil},
		{"failure", &protocol.ExecutorError{Phase: "submit", HTTPStatus: 503}},
	} {
		t.Run(oldResult.name, func(t *testing.T) {
			d := &Dispatcher{}
			oldFinish, err := d.beginProfileAttempt(context.Background(), "channel")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				d.recordFailure("channel")
			}
			expireRegressionCooldown(d, "channel")
			probeFinish, err := d.beginProfileAttempt(context.Background(), "channel")
			if err != nil {
				t.Fatal(err)
			}
			oldFinish(oldResult.err)
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					finish, err := d.beginProfileAttempt(context.Background(), "channel")
					if !errors.Is(err, ErrChannelUnavailable) {
						if finish != nil {
							finish(context.Canceled)
						}
						t.Errorf("old request released current probe: error=%v", err)
					}
				}()
			}
			wg.Wait()
			st := d.GetBreaker("channel")
			if st.failCount != 3 || st.cooldownUntil.IsZero() || !st.halfOpen {
				t.Fatalf("old request changed breaker: count=%d cooldown=%v reserved=%v", st.failCount, st.cooldownUntil, st.halfOpen)
			}
			probeFinish(context.Canceled)
			replacementFinish, err := d.beginProfileAttempt(context.Background(), "channel")
			if err != nil {
				t.Fatal(err)
			}
			// Deferred cleanup of an already finished request cannot release its
			// replacement probe, including the model-discovery cleanup path.
			probeFinish(nil)
			if _, err := d.beginProfileAttempt(context.Background(), "channel"); !errors.Is(err, ErrChannelUnavailable) {
				t.Fatalf("duplicate finish released replacement probe: %v", err)
			}
			replacementFinish(nil)
			if !d.candidateAvailable("channel") || st.failCount != 0 {
				t.Fatal("successful owning probe did not restore health")
			}
		})
	}
}

func TestHalfOpenFailureReopensAfterSlidingWindowDecay(t *testing.T) {
	d := &Dispatcher{}
	for i := 0; i < 3; i++ {
		d.recordFailure("channel")
	}
	expireRegressionCooldown(d, "channel")
	st := d.GetBreaker("channel")
	st.lastFailure = time.Now().Add(-2 * time.Minute)
	probe, acquired := d.tryAcquireChannel("channel")
	if !acquired {
		t.Fatal("missing half-open probe")
	}
	probe.finish(attemptFailed)
	if d.candidateAvailable("channel") || !time.Now().Before(st.cooldownUntil) {
		t.Fatal("failed probe did not start another cooldown after failure-count decay")
	}
}

func TestOldLegacySuccessCannotResetNewHalfOpenProbe(t *testing.T) {
	setRegressionChannels(t, []config.UpstreamChannel{{ID: "channel", Type: "openai", Models: []string{"m"}}})
	d := &Dispatcher{}
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- d.ExecuteWithPolicy(context.Background(), "m", "chat", RetryInference, nil, func(*config.UpstreamChannel, adapter.Adapter) error {
			close(started)
			<-release
			return nil
		})
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("old Legacy request did not start: %v", err)
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("old Legacy request did not start")
	}
	for i := 0; i < 3; i++ {
		d.recordFailure("channel")
	}
	expireRegressionCooldown(d, "channel")
	probeFinish, err := d.beginProfileAttempt(context.Background(), "channel")
	if err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if d.candidateAvailable("channel") {
		t.Fatal("old Legacy request reset the new Profile probe")
	}
	probeFinish(nil)
}
