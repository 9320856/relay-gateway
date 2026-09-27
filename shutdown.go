package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// shutdownGateway gives HTTP draining and background work separate budgets.
// Workers and model discovery are cancelled immediately, while accepted HTTP
// requests may finish normally until the server's grace period expires.
func shutdownGateway(server *http.Server, cancelRequests, stopWorkers func(), stopSync func(context.Context) error, budget time.Duration, workers ...<-chan struct{}) error {
	stopWorkers()
	syncDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		syncDone <- stopSync(ctx)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	err := server.Shutdown(ctx)
	cancel()
	cancelRequests()
	if err != nil {
		err = errors.Join(fmt.Errorf("HTTP drain: %w", err), server.Close())
	}

	// Do not reuse the HTTP deadline: it may already have expired. A shared
	// deadline bounds the total wait regardless of the number of workers.
	waitCtx, cancelWait := context.WithTimeout(context.Background(), budget)
	defer cancelWait()
	for i, done := range workers {
		select {
		case <-done:
		case <-waitCtx.Done():
			return errors.Join(err, fmt.Errorf("worker %d: %w", i, waitCtx.Err()))
		}
	}
	select {
	case syncErr := <-syncDone:
		if syncErr != nil {
			err = errors.Join(err, fmt.Errorf("model discovery: %w", syncErr))
		}
	case <-waitCtx.Done():
		return errors.Join(err, fmt.Errorf("model discovery: %w", waitCtx.Err()))
	}
	// A management request admitted before HTTP draining may have queued a
	// new refresh after the first StopSync. Join that refresh too, after no
	// normally drained request can start another one.
	finalSyncDone := make(chan error, 1)
	go func() { finalSyncDone <- stopSync(waitCtx) }()
	select {
	case syncErr := <-finalSyncDone:
		if syncErr != nil {
			err = errors.Join(err, fmt.Errorf("final model discovery drain: %w", syncErr))
		}
	case <-waitCtx.Done():
		err = errors.Join(err, fmt.Errorf("final model discovery drain: %w", waitCtx.Err()))
	}
	return err
}
