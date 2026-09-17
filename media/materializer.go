package media

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	SourceURL             = "url"
	SourceBase64          = "base64"
	SourceBinary          = "binary"
	SourceProviderContent = "provider_content"
)

// MediaResult is the normalized output of an executor. The source itself is
// deliberately opaque to storage: URL/provider sources are opened by a
// SourceFetcher, while binary and base64 sources can be handled inline.
type MediaResult struct {
	SourceKind  string
	Locator     string
	ContentType string
	Filename    string
	Body        io.Reader
	MaxBytes    int64
}

type FetchedSource struct {
	Body        io.ReadCloser
	ContentType string
	Filename    string
}

// SourceFetcher resolves a normalized result into a streaming body. Network
// implementations should enforce SSRF, redirect, MIME, and size policy before
// returning the body; the worker itself still enforces the final byte limit.
type SourceFetcher interface {
	Fetch(context.Context, MediaResult) (FetchedSource, error)
}

// SourceFetcherFunc adapts a function to SourceFetcher for executors and tests.
type SourceFetcherFunc func(context.Context, MediaResult) (FetchedSource, error)

func (f SourceFetcherFunc) Fetch(ctx context.Context, result MediaResult) (FetchedSource, error) {
	if f == nil {
		return FetchedSource{}, errors.New("source fetcher is nil")
	}
	return f(ctx, result)
}

// InlineSourceFetcher handles sources already present in memory. It does not
// fetch URLs; those must use an explicitly configured network fetcher.
type InlineSourceFetcher struct{}

func (InlineSourceFetcher) Fetch(_ context.Context, result MediaResult) (FetchedSource, error) {
	switch strings.ToLower(strings.TrimSpace(result.SourceKind)) {
	case "", SourceBinary:
		if result.Body == nil {
			return FetchedSource{}, errors.New("binary media body is required")
		}
		return FetchedSource{Body: io.NopCloser(result.Body), ContentType: result.ContentType, Filename: result.Filename}, nil
	case SourceBase64:
		encoded := strings.TrimSpace(result.Locator)
		if comma := strings.IndexByte(encoded, ','); comma >= 0 && strings.HasPrefix(strings.ToLower(encoded[:comma]), "data:") {
			encoded = encoded[comma+1:]
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			if decoded, err = base64.RawStdEncoding.DecodeString(encoded); err != nil {
				return FetchedSource{}, fmt.Errorf("decode base64 media: %w", err)
			}
		}
		return FetchedSource{Body: io.NopCloser(bytes.NewReader(decoded)), ContentType: result.ContentType, Filename: result.Filename}, nil
	default:
		return FetchedSource{}, fmt.Errorf("inline source does not support kind %q", result.SourceKind)
	}
}

// MaterializationWorker streams one normalized result into an ObjectStore.
// Database state transitions belong to the caller, allowing it to use a short
// transaction after the atomic object write and to retry without re-submitting
// the paid generation task.
type MaterializationWorker struct {
	Store   ObjectStore
	Fetcher SourceFetcher
}

func (w *MaterializationWorker) Materialize(ctx context.Context, result MediaResult, key string) (ObjectInfo, error) {
	if w == nil || w.Store == nil {
		return ObjectInfo{}, errors.New("materialization object store is required")
	}
	if strings.TrimSpace(key) == "" {
		return ObjectInfo{}, errors.New("materialization object key is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if w.Fetcher == nil {
		w.Fetcher = InlineSourceFetcher{}
	}
	fetched, err := w.Fetcher.Fetch(ctx, result)
	if err != nil {
		return ObjectInfo{}, err
	}
	if fetched.Body == nil {
		return ObjectInfo{}, errors.New("source fetcher returned an empty body")
	}
	defer fetched.Body.Close()
	contentType := strings.TrimSpace(result.ContentType)
	if contentType == "" {
		contentType = strings.TrimSpace(fetched.ContentType)
	}
	filename := strings.TrimSpace(result.Filename)
	if filename == "" {
		filename = strings.TrimSpace(fetched.Filename)
	}
	return w.Store.PutAtomic(ctx, fetched.Body, PutMeta{Key: key, ContentType: contentType, Filename: filename, MaxBytes: result.MaxBytes})
}
