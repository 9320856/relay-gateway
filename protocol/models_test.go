package protocol

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDiscoverModelsUsesSharedEndpointAndProfileDefaults(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/v1/models" {
			t.Fatalf("models path = %q", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"model-a"},{"id":"MODEL-A"},{"id":"model-b"}]}`)
	}))
	t.Cleanup(server.Close)
	models, err := DiscoverModels(context.Background(), server.URL+"/v1", []string{"test-key"}, nil, []string{"fallback"})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer test-key" || len(models) != 2 || models[0] != "model-a" || models[1] != "model-b" {
		t.Fatalf("models=%v auth=%q", models, gotAuth)
	}
}

func TestDiscoverModelsFallsBackOnlyForUnsupportedEndpoint(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	models, err := DiscoverModels(context.Background(), server.URL+"/v1", nil, nil, []string{"fallback", "FALLBACK"})
	if err != nil || len(models) != 1 || models[0] != "fallback" {
		t.Fatalf("fallback models=%v err=%v", models, err)
	}
}

func TestProfileModelDefaultsRejectsUnknownChannel(t *testing.T) {
	if models := ProfileModelDefaults("unknown-provider"); models != nil {
		t.Fatalf("unknown provider defaults = %v, want nil", models)
	}
}

func TestDiscoverModelsRefusesCredentialRedirects(t *testing.T) {
	for _, status := range []int{http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var targetCalls atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				targetCalls.Add(1)
				_, _ = io.WriteString(w, `{"data":[{"id":"redirected"}]}`)
			}))
			t.Cleanup(target.Close)
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", strings.Replace(target.URL, "127.0.0.1", "localhost", 1))
				w.WriteHeader(status)
			}))
			t.Cleanup(origin.Close)
			models, err := DiscoverModels(context.Background(), origin.URL, []string{"private-key"}, map[string]string{"x-api-key": "private-key"}, []string{"fallback"})
			var upstream *ExecutorError
			if !errors.As(err, &upstream) || upstream.HTTPStatus != status || len(models) != 0 {
				t.Fatalf("models=%v err=%v, want redirect rejection", models, err)
			}
			if targetCalls.Load() != 0 {
				t.Fatal("model discovery forwarded credentials to a redirect target")
			}
		})
	}
}
