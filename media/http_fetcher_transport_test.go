package media

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPSourceFetcherClosesOwnedConnectionAfterBodyClose(t *testing.T) {
	for _, suppliedClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "default-client", true: "client-without-transport"}[suppliedClient], func(t *testing.T) {
			closed := make(chan struct{}, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "image/png")
				_, _ = io.WriteString(w, "image")
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateClosed {
					select {
					case closed <- struct{}{}:
					default:
					}
				}
			}
			server.Start()
			defer server.Close()
			fetcher := HTTPSourceFetcher{URLValidator: func(string) error { return nil }}
			if suppliedClient {
				fetcher.Client = &http.Client{}
			}
			result, err := fetcher.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(result.Body); err != nil {
				t.Fatal(err)
			}
			if err := result.Body.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("private transport kept connection idle after body close")
			}
		})
	}
}

func TestHTTPSourceFetcherPreservesInjectedSharedTransport(t *testing.T) {
	var opened atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = io.WriteString(w, "image")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	fetcher := HTTPSourceFetcher{Client: client, URLValidator: func(string) error { return nil }}
	for i := 0; i < 2; i++ {
		result, err := fetcher.Fetch(context.Background(), MediaResult{SourceKind: SourceURL, Locator: server.URL})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(result.Body); err != nil {
			t.Fatal(err)
		}
		if err := result.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if opened.Load() != 1 {
		t.Fatalf("caller-owned transport was closed instead of reused: connections=%d", opened.Load())
	}
	if client.CheckRedirect != nil {
		t.Fatal("fetcher mutated caller-owned client")
	}
}
