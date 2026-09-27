package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay-gateway/config"
	"relay-gateway/db"
)

func TestQueuedModelDiscoveryCannotCommitAfterChannelSave(t *testing.T) {
	if err := db.InitDB(filepath.Join(t.TempDir(), "queued-models.db")); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	d := &Dispatcher{remoteModels: make(map[string][]string)}
	previousHook := db.OnChannelSaved
	db.OnChannelSaved = d.InvalidateChannel
	defer func() { db.OnChannelSaved = previousHook }()
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	var once sync.Once
	finishRequests := func() { once.Do(func() { close(release) }) }
	defer finishRequests()
	var queuedCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/9/models" {
			queuedCalls.Add(1)
		} else {
			started <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, `{"data":[{"id":"old-provider-model"}]}`)
	}))
	defer upstream.Close()
	// Created-at ordering places the ninth channel behind the eight occupied
	// network slots. Every configuration/revision must be captured before that wait.
	var ninth *db.ChannelModel
	for i := 1; i <= 9; i++ {
		row := &db.ChannelModel{ID: fmt.Sprintf("queued-%d", i), Name: fmt.Sprintf("queued-%d", i), Type: "openai", BaseURL: fmt.Sprintf("%s/%d", upstream.URL, i), Enabled: true, FetchModels: true, Priority: i, Weight: 1}
		if err := db.SaveChannelModel(row); err != nil {
			t.Fatal(err)
		}
		if i == 9 {
			ninth = row
		}
	}
	d.SyncRemoteModels(context.Background())
	d.syncMu.Lock()
	done := d.syncDone
	d.syncMu.Unlock()
	for i := 0; i < 8; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			finishRequests()
			t.Fatal("model slots did not fill")
		}
	}
	ninth.BaseURL = "http://new-provider.example/v1"
	ninth.FetchModels = false
	if err := db.SaveChannelModel(ninth); err != nil {
		finishRequests()
		t.Fatal(err)
	}
	finishRequests()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("model discovery did not finish")
	}
	if queuedCalls.Load() != 1 {
		t.Fatalf("queued old snapshot was not exercised: calls=%d", queuedCalls.Load())
	}
	current, err := db.GetChannelModel(ninth.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ModelsSyncedRaw != "" || current.LastStatus != ninth.LastStatus {
		t.Fatalf("old queued result changed new channel: models=%q status=%q", current.ModelsSyncedRaw, current.LastStatus)
	}
	d.modelsMu.RLock()
	models := d.remoteModels[ninth.ID]
	d.modelsMu.RUnlock()
	if len(models) != 0 {
		t.Fatalf("old queued result repopulated cache: %v", models)
	}
}

func TestModelDiscoveryFailuresFeedSharedBreaker(t *testing.T) {
	for _, body := range []string{"unavailable", "malformed-json"} {
		t.Run(body, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if body == "unavailable" {
					w.WriteHeader(503)
				}
				io.WriteString(w, body)
			}))
			defer upstream.Close()
			previous := config.Global
			config.Global = &config.Config{Channels: []config.UpstreamChannel{{ID: "models", Type: "openai", BaseURL: upstream.URL, Enabled: true, FetchModels: true}}}
			defer func() { config.Global = previous }()
			d := &Dispatcher{remoteModels: make(map[string][]string)}
			for i := 0; i < 4; i++ {
				d.syncRemoteModelsOnce(context.Background())
			}
			if calls.Load() != 3 || d.candidateAvailable("models") {
				t.Fatalf("discovery bypassed breaker: calls=%d available=%v", calls.Load(), d.candidateAvailable("models"))
			}
		})
	}
}
