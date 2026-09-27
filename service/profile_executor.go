package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"relay-gateway/protocol"
)

var ErrChannelUnavailable = errors.New("upstream channel is cooling down or has a half-open probe")

// beginProfileAttempt shares breaker ownership across every Profile operation.
// It adds no retries: submission and polling policies stay with their callers.
func (d *Dispatcher) beginProfileAttempt(ctx context.Context, id string) (func(error), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.modelsMu.RLock()
	generation := d.modelGeneration[id]
	d.modelsMu.RUnlock()
	if !d.tryAcquireChannel(id) {
		return nil, fmt.Errorf("%w: %s", ErrChannelUnavailable, id)
	}
	var once sync.Once
	return func(err error) {
		once.Do(func() {
			d.modelsMu.RLock()
			currentGeneration := d.modelGeneration[id]
			d.modelsMu.RUnlock()
			if generation != currentGeneration {
				return
			}
			defer d.releaseHalfOpenProbe(id)
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			if err == nil {
				d.recordSuccess(id)
				return
			}
			var upstream *protocol.ExecutorError
			if errors.As(err, &upstream) && upstream.Phase != "write" && (upstream.Phase == "read" || upstream.HTTPStatus == 0 || upstream.HTTPStatus == 408 || upstream.HTTPStatus == 429 || upstream.HTTPStatus >= 500) {
				d.recordFailure(id)
			}
		})
	}, nil
}

// ProfileExecutor decorates the common HTTP executor with dispatcher feedback.
type ProfileExecutor struct {
	*protocol.HTTPExecutor
	Dispatcher *Dispatcher
	ChannelID  string
}

func (d *Dispatcher) ProfileExecutor(executor *protocol.HTTPExecutor, channelID string) *ProfileExecutor {
	return &ProfileExecutor{HTTPExecutor: executor, Dispatcher: d, ChannelID: channelID}
}

func (e *ProfileExecutor) Submit(ctx context.Context, p protocol.CompiledProfile, op string, req protocol.Request) (result protocol.Result, err error) {
	finish, err := e.Dispatcher.beginProfileAttempt(ctx, e.ChannelID)
	if err != nil {
		return result, err
	}
	defer func() { finish(err) }()
	return e.HTTPExecutor.Submit(ctx, p, op, req)
}
func (e *ProfileExecutor) PollOnce(ctx context.Context, p protocol.CompiledProfile, op string, req protocol.Request) (result protocol.Result, err error) {
	finish, err := e.Dispatcher.beginProfileAttempt(ctx, e.ChannelID)
	if err != nil {
		return result, err
	}
	defer func() { finish(err) }()
	return e.HTTPExecutor.PollOnce(ctx, p, op, req)
}
func (e *ProfileExecutor) ExecuteRaw(ctx context.Context, p protocol.CompiledProfile, op string, req protocol.Request, w http.ResponseWriter, stream bool) (result protocol.Result, err error) {
	finish, err := e.Dispatcher.beginProfileAttempt(ctx, e.ChannelID)
	if err != nil {
		return result, err
	}
	defer func() { finish(err) }()
	return e.HTTPExecutor.ExecuteRaw(ctx, p, op, req, w, stream)
}
func (e *ProfileExecutor) FetchContent(ctx context.Context, p protocol.CompiledProfile, op string, req protocol.Request) (protocol.ContentResult, error) {
	finish, err := e.Dispatcher.beginProfileAttempt(ctx, e.ChannelID)
	if err != nil {
		return protocol.ContentResult{}, err
	}
	result, err := e.HTTPExecutor.FetchContent(ctx, p, op, req)
	if err != nil || result.Body == nil {
		finish(err)
		return result, err
	}
	result.Body = &profileContentBody{ReadCloser: result.Body, finish: finish}
	return result, nil
}

type profileContentBody struct {
	io.ReadCloser
	finish func(error)
}

func (b *profileContentBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.finish(nil)
	} else if err != nil {
		b.finish(&protocol.ExecutorError{Phase: "read", Cause: err})
	}
	return n, err
}
func (b *profileContentBody) Close() error {
	err := b.ReadCloser.Close()
	// An early close is commonly a HEAD response or a downstream disconnect.
	// Only a completed read establishes upstream health; release the reservation
	// without resetting failures if the caller abandons the body before EOF.
	b.finish(context.Canceled)
	return err
}
