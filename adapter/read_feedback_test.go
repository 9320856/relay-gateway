package adapter

import (
	"errors"
	"io"
	"strings"
	"testing"

	"relay-gateway/internal/httpforward"
)

func TestUpstreamBodyReadErrorsPreserveSourceAndCause(t *testing.T) {
	for _, read := range []struct {
		name string
		fn   func(io.Reader) ([]byte, error)
	}{
		{"JSON", readUpstreamJSONBody},
		{"error", readUpstreamErrorBody},
	} {
		t.Run(read.name, func(t *testing.T) {
			_, err := read.fn(&anthropicErrorBody{err: io.ErrUnexpectedEOF})
			var source *httpforward.StreamError
			if !errors.As(err, &source) || source.Op != "read" || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("upstream read source/cause lost: %v", err)
			}
			var nested *httpforward.StreamError
			if errors.As(source.Err, &nested) {
				t.Fatalf("duplicate read classification: %v", err)
			}
		})
	}
}

func TestOversizedUpstreamBodiesIdentifyReadSource(t *testing.T) {
	for _, read := range []struct {
		name string
		fn   func() ([]byte, error)
	}{
		{"JSON", func() ([]byte, error) { return readLimitedResponseBody(strings.NewReader("12345"), 4) }},
		{"error", func() ([]byte, error) {
			return readUpstreamErrorBody(strings.NewReader(strings.Repeat("x", int(maxUpstreamErrorBodyBytes)+1)))
		}},
	} {
		t.Run(read.name, func(t *testing.T) {
			_, err := read.fn()
			var source *httpforward.StreamError
			if !errors.As(err, &source) || source.Op != "read" || !errors.Is(err, ErrResponseTooLarge) {
				t.Fatalf("oversized upstream body lost source/cause: %v", err)
			}
		})
	}
}

func TestBodyLimitConfigurationIsNotAnUpstreamReadFailure(t *testing.T) {
	_, err := readLimitedResponseBody(strings.NewReader("data"), 0)
	var source *httpforward.StreamError
	if err == nil || errors.As(err, &source) {
		t.Fatalf("invalid local limit counted as upstream read failure: %v", err)
	}
}
