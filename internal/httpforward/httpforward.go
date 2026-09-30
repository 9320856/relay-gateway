// Package httpforward contains the HTTP response rules shared by both engines.
package httpforward

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
)

var buffers = sync.Pool{New: func() any { b := make([]byte, 32*1024); return &b }}

// StreamError keeps downstream disconnects separate from upstream read failures.
// Callers use Op for health feedback, while Unwrap preserves the original error.
type StreamError struct {
	Op  string
	Err error
}

func (e *StreamError) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *StreamError) Unwrap() error { return e.Err }

// CopyHeaders excludes connection-specific and upstream browser policy headers.
func CopyHeaders(dst http.ResponseWriter, src http.Header) {
	blocked := map[string]bool{"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true, "Te": true, "Trailer": true, "Trailers": true, "Transfer-Encoding": true, "Upgrade": true, "Set-Cookie": true, "Set-Cookie2": true, "Www-Authenticate": true, "Content-Security-Policy": true, "Content-Security-Policy-Report-Only": true}
	for name, values := range src {
		if !strings.EqualFold(name, "Connection") {
			continue
		}
		for _, value := range values {
			for _, token := range strings.Split(value, ",") {
				blocked[http.CanonicalHeaderKey(strings.TrimSpace(token))] = true
			}
		}
	}
	for name, values := range src {
		canonical := http.CanonicalHeaderKey(name)
		if blocked[canonical] || strings.HasPrefix(canonical, "Access-Control-") {
			continue
		}
		for _, value := range values {
			dst.Header().Add(name, value)
		}
	}
}

// Stream flushes each received block. The caller bounds the reader and owns its
// close; HTTP request cancellation interrupts a blocked upstream Body.Read.
func Stream(ctx context.Context, src io.Reader, dst http.ResponseWriter) (int64, error) {
	b := buffers.Get().(*[]byte)
	defer buffers.Put(b)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, readErr := src.Read(*b)
		if n > 0 {
			if err := ctx.Err(); err != nil {
				return total, err
			}
			written, err := dst.Write((*b)[:n])
			total += int64(written)
			if err != nil {
				return total, &StreamError{Op: "write", Err: err}
			}
			if written != n {
				return total, &StreamError{Op: "write", Err: io.ErrShortWrite}
			}
			if flusher, ok := dst.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return total, nil
			}
			return total, &StreamError{Op: "read", Err: readErr}
		}
	}
}
