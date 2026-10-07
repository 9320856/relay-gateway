package router

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"relay-gateway/db"
	"relay-gateway/protocol"
	"relay-gateway/service"
)

func TestReadBodyAndModelRejectsAmbiguousRouting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{
		`{"model":"allowed","model":"blocked"}`,
		`{"model":"blocked","model":"allowed"}`,
		`{"model":"allowed","model":"allowed"}`,
		`{"model":"allowed","m\u006fdel":"blocked"}`,
		`{"Model":"allowed","model":"blocked"}`,
		`{"model":"allowed","MODEL":null}`,
		`{"model":"allowed","model":12}`,
		`{"model":12}`,
		`{"model":["allowed"]}`,
		`{"model":"allowed"} {"model":"blocked"}`,
	} {
		t.Run(body, func(t *testing.T) {
			c, _ := selectionRequestContext(http.MethodPost, "/v1/chat/completions", body)
			if _, _, err := readBodyAndModel(c); err == nil {
				t.Fatal("ambiguous or invalid model was accepted")
			}
			restored, err := io.ReadAll(c.Request.Body)
			if err != nil || string(restored) != body {
				t.Fatalf("fallback body changed: %q, %v", restored, err)
			}
		})
	}
	for _, body := range []string{
		`{"m\u006fdel":" allowed ","messages":[{"model":"blocked"}]}`,
		`{"MODEL":"allowed","prompt":"\"model\":\"blocked\""}`,
	} {
		c, _ := selectionRequestContext(http.MethodPost, "/v1/chat/completions", body)
		if _, got, err := readBodyAndModel(c); err != nil || got != "allowed" {
			t.Fatalf("unambiguous model = %q, %v", got, err)
		}
	}
}

func TestProfileJSONRoutingRejectsDuplicatesAndForwardsSelectedModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	initAuthTestDB(t)
	t.Setenv("RELAY_ENABLE_PROFILE_ENGINE", "1")
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode upstream request: %v", err)
		}
		if payload["model"] != "allowed" {
			t.Errorf("forwarded unselected model: %#v", payload)
		}
		for key := range payload {
			if key != "model" && strings.EqualFold(key, "model") {
				t.Errorf("forwarded ambiguous model key %q", key)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"selected-profile","choices":[]}`)
	}))
	t.Cleanup(upstream.Close)
	channel := &db.ChannelModel{ID: "duplicate-model-channel", Type: "newapi", BaseURL: upstream.URL + "/v1", APIKey: "key", Enabled: true, ModelsRaw: "allowed,blocked", SelectedModelsRaw: `["allowed"]`}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.DefaultDispatcher.RemoveChannel(channel.ID) })
	compiled, err := protocol.Compile(protocol.Profile{SchemaVersion: protocol.CurrentSchemaVersion, Operations: []protocol.Operation{{
		Operation: "chat.completions", ExecutionMode: protocol.ExecutionDirect, PollingMode: protocol.PollingOff,
		Submit: protocol.Submit{Method: http.MethodPost, Path: "/chat/completions", BodyEncoding: "json"}, Response: protocol.Response{ResultPaths: []string{"choices"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "duplicate-model-profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "duplicate-model-profile", Revision: 1, SchemaVersion: protocol.CurrentSchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: channel.ID, Operation: "chat.completions", ModelPattern: "allowed", ProfileID: "duplicate-model-profile", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	engine := gin.New()
	registerV1Routes(engine.Group("/v1"))
	for _, endpoint := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/embeddings", "/v1/audio/speech", "/v1/images/generations", "/v1/images/jobs", "/v1/moderations"} {
		for _, key := range []string{`"model"`, `"m\u006fdel"`, `"MODEL"`} {
			body := fmt.Sprintf(`{"model":"allowed",%s:"blocked","messages":[],"prompt":"test","input":"test"}`, key)
			request := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			engine.ServeHTTP(rec, request)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s duplicate %s = %d: %s", endpoint, key, rec.Code, rec.Body.String())
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("ambiguous request reached upstream")
	}
	for _, body := range []string{`{"model":" allowed ","messages":[]}`, `{"MODEL":" allowed ","messages":[]}`, `{"m\u006fdel":"allowed","messages":[]}`} {
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, request)
		if rec.Code != http.StatusOK {
			t.Fatalf("selected model = %d: %s", rec.Code, rec.Body.String())
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("upstream calls = %d, want 3", calls.Load())
	}
}

func BenchmarkParseRequestModelParallel(b *testing.B) {
	for _, size := range []int{128, 1 << 20} {
		b.Run(fmt.Sprintf("payload_%d", size), func(b *testing.B) {
			payload := []byte(`{"model":"allowed","messages":[{"content":"` + strings.Repeat("x", size) + `"}]}`)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if got, err := parseRequestModel(payload); err != nil || got != "allowed" {
						b.Fatalf("model = %q, %v", got, err)
					}
				}
			})
		})
	}
}
