package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"relay-gateway/db"
	"relay-gateway/media"
	"relay-gateway/profilebootstrap"
	"relay-gateway/service"
)

func TestChannelMediaRetentionAPIExplicitCreation(t *testing.T) {
	initAuthTestDB(t)
	if _, err := profilebootstrap.EnsureBuiltinProfiles(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []string{media.RetentionDisabled, media.RetentionBestEffort, media.RetentionRequired} {
		t.Run(policy, func(t *testing.T) {
			id := "media-created-" + policy
			c, recorder := selectionRequestContext(http.MethodPost, "/api/channels", fmt.Sprintf(`{"id":%q,"type":"openai","base_url":"http://127.0.0.1:1/v1","media_retention":%q}`, id, policy))
			handleSaveChannel(c)
			if recorder.Code != http.StatusOK {
				t.Fatalf("create policy %q = %d %s", policy, recorder.Code, recorder.Body.String())
			}
			stored, err := db.GetChannelModel(id)
			if err != nil || stored.MediaRetention != policy {
				t.Fatalf("created policy = %+v, %v; want %q", stored, err, policy)
			}
		})
	}
}

func TestChannelMediaRetentionAPIAndRuntimeCache(t *testing.T) {
	initAuthTestDB(t)
	if _, err := profilebootstrap.EnsureBuiltinProfiles(context.Background()); err != nil {
		t.Fatal(err)
	}
	engine := Setup()
	setup := httptest.NewRecorder()
	engine.ServeHTTP(setup, newAuthRequest(http.MethodPost, "/api/auth/setup", `{"username":"media-administrator","password":"correct horse battery"}`, "127.0.0.1:4321"))
	if setup.Code != http.StatusCreated {
		t.Fatalf("admin setup = %d", setup.Code)
	}
	var session struct {
		CSRF string `json:"csrf_token"`
	}
	if err := json.Unmarshal(setup.Body.Bytes(), &session); err != nil || session.CSRF == "" || len(setup.Result().Cookies()) != 1 {
		t.Fatal("admin setup did not return a usable session")
	}
	cookie := setup.Result().Cookies()[0]
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		http.Error(w, "unexpected upstream request", http.StatusBadGateway)
	}))
	defer upstream.Close()
	const channelID = "channel-media-api"
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channelID) })
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := newAuthRequest(method, path, body, "127.0.0.1:4321")
		req.AddCookie(cookie)
		req.Header.Set("X-CSRF-Token", session.CSRF)
		engine.ServeHTTP(recorder, req)
		return recorder
	}
	save := func(policy any, include bool) *httptest.ResponseRecorder {
		t.Helper()
		payload := map[string]any{
			"id": channelID, "type": "openai", "base_url": upstream.URL + "/v1", "enabled": true,
			"fetch_models": false, "models_raw": "media-test-model", "selected_models": []string{"media-test-model"},
		}
		if include {
			payload["media_retention"] = policy
		}
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return request(http.MethodPost, "/api/channels", string(body))
	}
	assertPolicy := func(response *httptest.ResponseRecorder, want string) {
		t.Helper()
		if response.Code != http.StatusOK {
			t.Fatalf("save policy %q = %d %s", want, response.Code, response.Body.String())
		}
		var payload struct {
			Channel struct {
				MediaRetention string `json:"media_retention"`
			} `json:"channel"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || payload.Channel.MediaRetention != want {
			t.Fatalf("saved API policy = %q, %v; want %q", payload.Channel.MediaRetention, err, want)
		}
		stored, err := db.GetChannelModel(channelID)
		if err != nil || stored.MediaRetention != want {
			t.Fatalf("stored policy = %+v, %v; want %q", stored, err, want)
		}
		cached := db.GetActiveUpstreamChannels()
		if len(cached) != 1 || cached[0].ID != channelID || cached[0].MediaRetention != want {
			t.Fatalf("saved API policy did not reach runtime cache: %+v; want %q", cached, want)
		}
		candidates, err := service.DefaultDispatcher.ResolveProfileCandidates("media-test-model", "video.create")
		if err != nil || len(candidates) != 1 || candidates[0].MediaRetention != want {
			t.Fatalf("runtime candidate policy = %+v, %v; want %q", candidates, err, want)
		}
		listing := request(http.MethodGet, "/api/channels", "")
		var list struct {
			Channels []struct {
				ID             string `json:"id"`
				MediaRetention string `json:"media_retention"`
			} `json:"channels"`
		}
		if listing.Code != http.StatusOK || json.Unmarshal(listing.Body.Bytes(), &list) != nil || len(list.Channels) != 1 || list.Channels[0].MediaRetention != want {
			t.Fatalf("listed API policy = %d %s; want %q", listing.Code, listing.Body.String(), want)
		}
	}
	assertPolicy(save(nil, false), media.RetentionDisabled)
	for _, policy := range []string{media.RetentionDisabled, media.RetentionBestEffort, media.RetentionRequired} {
		t.Run(policy, func(t *testing.T) {
			assertPolicy(save(policy, true), policy)
			assertPolicy(save(nil, false), policy) // Older clients omit the new field.
		})
	}
	for i, invalid := range []any{nil, "sometimes", "REQUIRED", " required", " ", 3, true, []string{"disabled"}, map[string]string{"policy": "disabled"}} {
		t.Run(fmt.Sprintf("invalid-%d", i), func(t *testing.T) {
			response := save(invalid, true)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid policy %#v = %d %s", invalid, response.Code, response.Body.String())
			}
			stored, err := db.GetChannelModel(channelID)
			if err != nil || stored.MediaRetention != media.RetentionRequired {
				t.Fatalf("invalid policy changed the saved setting: %+v, %v", stored, err)
			}
		})
	}
	assertPolicy(save("", true), media.RetentionDisabled)
	if upstreamCalls.Load() != 0 {
		t.Fatalf("channel setting changes made %d upstream requests", upstreamCalls.Load())
	}
}
