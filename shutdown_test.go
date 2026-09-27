package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownGatewayJoinsRefreshStartedByDrainingRequest(t *testing.T) {
	entered, initialSyncStopped := make(chan struct{}), make(chan struct{})
	var refreshing atomic.Bool
	refreshing.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-initialSyncStopped
		// This models an accepted management request scheduling discovery
		// after the initial stop, while HTTP is still draining.
		refreshing.Store(true)
	}))
	defer server.Close()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		if response, err := server.Client().Get(server.URL); err == nil {
			response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not enter handler")
	}
	var stops atomic.Int32
	err := shutdownGateway(server.Config, func() {}, func() {}, func(context.Context) error {
		refreshing.Store(false)
		if stops.Add(1) == 1 {
			close(initialSyncStopped)
		}
		return nil
	}, time.Second)
	if err != nil || stops.Load() != 2 || refreshing.Load() {
		t.Fatalf("late refresh remained active: err=%v stops=%d refreshing=%v", err, stops.Load(), refreshing.Load())
	}
	<-requestDone
}

func TestShutdownGatewayCancelsActiveRequestsAfterGracePeriod(t *testing.T) {
	requestCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	entered, returned := make(chan struct{}), make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(returned)
	}))
	server.Config.BaseContext = func(net.Listener) context.Context { return requestCtx }
	server.Start()
	defer server.Close()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		if response, err := server.Client().Get(server.URL); err == nil {
			response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not enter handler")
	}
	workerDone := make(chan struct{})
	go func() {
		<-requestCtx.Done()
		// Cleanup finishes after HTTP has spent its whole grace period. It
		// therefore needs the independent background drain budget.
		timer := time.NewTimer(20 * time.Millisecond)
		defer timer.Stop()
		<-timer.C
		close(workerDone)
	}()
	syncStarted := make(chan error, 2)
	err := shutdownGateway(server.Config, cancelRequests, func() {}, func(ctx context.Context) error {
		syncStarted <- ctx.Err()
		return nil
	}, 80*time.Millisecond, workerDone)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected forced shutdown, got %v", err)
	}
	if strings.Contains(err.Error(), "worker") {
		t.Fatalf("background cleanup reused the expired HTTP budget: %v", err)
	}
	select {
	case <-workerDone:
	default:
		t.Fatal("shutdown returned before background cleanup completed")
	}
	if syncErr := <-syncStarted; syncErr != nil {
		t.Fatalf("model discovery received expired HTTP context: %v", syncErr)
	}
	if syncErr := <-syncStarted; syncErr != nil {
		t.Fatalf("final model drain received expired HTTP context: %v", syncErr)
	}
	for _, done := range []<-chan struct{}{returned, requestDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("forced shutdown left an active request")
		}
	}
}

func TestShutdownGatewayBoundsModelSyncWait(t *testing.T) {
	for _, blocks := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "blocked"}[blocks], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			defer server.Close()
			release := make(chan struct{})
			defer close(release)
			started := time.Now()
			err := shutdownGateway(server.Config, func() {}, func() {}, func(context.Context) error {
				if blocks {
					<-release
				}
				return context.DeadlineExceeded
			}, 30*time.Millisecond)
			if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "model discovery") {
				t.Fatalf("model sync failure was not propagated: %v", err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("model sync wait was unbounded")
			}
		})
	}
}

func TestShutdownGatewayBoundsWorkerWaitAndDrainsNormally(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "drained", true: "blocked"}[blocked], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			defer server.Close()
			workerDone := make(chan struct{})
			defer func() {
				if blocked {
					close(workerDone)
				}
			}()
			stopped, requestsCancelled := false, false
			started := time.Now()
			err := shutdownGateway(server.Config, func() { requestsCancelled = true }, func() {
				stopped = true
				if !blocked {
					close(workerDone)
				}
			}, func(context.Context) error { return nil }, 30*time.Millisecond, workerDone)
			if !stopped || !requestsCancelled {
				t.Fatal("shutdown did not cancel background work and request context")
			}
			if blocked && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected bounded worker wait, got %v", err)
			}
			if !blocked && err != nil {
				t.Fatalf("normal drain failed: %v", err)
			}
			if time.Since(started) > time.Second {
				t.Fatal("worker wait was unbounded")
			}
		})
	}
}
