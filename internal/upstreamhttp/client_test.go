package upstreamhttp

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewClientSharesTransportAndPreservesInjectedSettings(t *testing.T) {
	first, second := NewClient(nil), NewClient(nil)
	if first.Transport != Transport || second.Transport != Transport || first.Timeout != DefaultTimeout {
		t.Fatal("default API clients must share the bounded transport and timeout")
	}
	base := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer base.Close()
	client := base.Client()
	client.Timeout = 3 * time.Second
	var redirectCalled bool
	client.CheckRedirect = func(*http.Request, []*http.Request) error { redirectCalled = true; return nil }
	copy := NewClient(client)
	if copy == client || copy.Transport != client.Transport || copy.Timeout != client.Timeout {
		t.Fatal("injected client settings were lost or original was returned")
	}
	if err := copy.CheckRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) || redirectCalled {
		t.Fatal("provider client must refuse redirects")
	}
	if err := client.CheckRedirect(nil, nil); err != nil || !redirectCalled {
		t.Fatal("injected client was mutated")
	}
}
