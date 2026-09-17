package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"relay-gateway/adapter"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/model"
)

func TestDispatcherRouting(t *testing.T) {
	config.Global = &config.Config{
		Channels: []config.UpstreamChannel{
			{
				ID:      "sub2api-main",
				Type:    "sub2api",
				BaseURL: "http://localhost:8080/v1",
				APIKey:  "sub2api-key",
				Models: []string{
					"claude-*",
					"o1*",
					"gpt-4o*",
				},
			},
			{
				ID:      "newapi-main",
				Type:    "newapi",
				BaseURL: "https://api.newapi.com/v1",
				APIKey:  "newapi-key",
				Models: []string{
					"*",
				},
			},
		},
	}

	tests := []struct {
		model          string
		expectedChanID string
	}{
		{"claude-3-7-sonnet", "sub2api-main"},
		{"claude-3-5-sonnet-20241022", "sub2api-main"},
		{"claude-3-haiku", "sub2api-main"},
		{"o1", "sub2api-main"},
		{"o1-preview", "sub2api-main"},
		{"gpt-4o", "sub2api-main"},
		{"gpt-4o-mini", "sub2api-main"},
		// 以下应命中 newapi-main (*)
		{"dall-e-3", "newapi-main"},
		{"deepseek-chat", "newapi-main"},
		{"deepseek-r1", "newapi-main"},
		{"text-embedding-3-small", "newapi-main"},
		{"flux-dev", "newapi-main"},
		{"unknown-xyz", "newapi-main"},
	}

	for _, tt := range tests {
		ch, adp, err := DefaultDispatcher.Resolve(tt.model)
		if err != nil {
			t.Fatalf("Resolve(%q) unexpected error: %v", tt.model, err)
		}
		if ch.ID != tt.expectedChanID {
			t.Errorf("Resolve(%q) = channel %s, want %s", tt.model, ch.ID, tt.expectedChanID)
		}
		if adp == nil {
			t.Errorf("Resolve(%q) returned nil adapter", tt.model)
		}
	}
}

func TestDispatcherRoundRobin(t *testing.T) {
	// 配置两个相同的 Sub2API 节点
	config.Global = &config.Config{
		Channels: []config.UpstreamChannel{
			{
				ID:      "sub2api-node-1",
				Type:    "sub2api",
				BaseURL: "http://node1:8080/v1",
				Models:  []string{"claude-*"},
			},
			{
				ID:      "sub2api-node-2",
				Type:    "sub2api",
				BaseURL: "http://node2:8080/v1",
				Models:  []string{"claude-*"},
			},
		},
	}

	hitCount := make(map[string]int)
	for i := 0; i < 10; i++ {
		ch, _, err := DefaultDispatcher.Resolve("claude-3-7-sonnet")
		if err != nil {
			t.Fatalf("Resolve failed: %v", err)
		}
		hitCount[ch.ID]++
	}

	if hitCount["sub2api-node-1"] != 5 || hitCount["sub2api-node-2"] != 5 {
		t.Errorf("expected 5 hits each on node-1 and node-2, got: %+v", hitCount)
	}
}

func TestDispatcherFailover(t *testing.T) {
	DefaultDispatcher.RemoveChannel("sub2api-node-broken")
	DefaultDispatcher.RemoveChannel("sub2api-node-healthy")
	DefaultDispatcher.rrCounter.Store(0)
	t.Cleanup(func() {
		DefaultDispatcher.RemoveChannel("sub2api-node-broken")
		DefaultDispatcher.RemoveChannel("sub2api-node-healthy")
	})
	config.Global = &config.Config{
		Channels: []config.UpstreamChannel{
			{
				ID:      "sub2api-node-broken",
				Type:    "sub2api",
				BaseURL: "http://broken-node:8080/v1",
				Models:  []string{"claude-*"},
			},
			{
				ID:      "sub2api-node-healthy",
				Type:    "sub2api",
				BaseURL: "http://healthy-node:8080/v1",
				Models:  []string{"claude-*"},
			},
		},
	}

	failoverTriggered := false
	for i := 0; i < 4; i++ {
		attempted := []string{}
		err := DefaultDispatcher.ExecuteWithFailover(
			context.Background(),
			"claude-3-7-sonnet",
			func() bool { return true },
			func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
				attempted = append(attempted, ch.ID)
				if ch.ID == "sub2api-node-broken" {
					return fmt.Errorf("connection refused: 502")
				}
				return nil
			},
		)
		if err != nil {
			t.Fatalf("ExecuteWithFailover failed: %v", err)
		}
		if len(attempted) > 1 {
			failoverTriggered = true
		}
	}

	if !failoverTriggered {
		t.Errorf("expected failover to be triggered when broken node is encountered")
	}
}

func TestRewriteJSONModel(t *testing.T) {
	inputJSON := `{
		"model": "claude-3-7-sonnet",
		"messages": [{"role": "user", "content": "hello"}],
		"thinking": {"type": "enabled", "budget_tokens": 1024},
		"stream": true,
		"reasoning_effort": "high"
	}`

	rewritten := adapter.RewriteJSONModel([]byte(inputJSON), "claude-3-7-sonnet-remapped")

	var parsed map[string]interface{}
	if err := json.Unmarshal(rewritten, &parsed); err != nil {
		t.Fatalf("Unmarshal rewritten JSON failed: %v", err)
	}

	if parsed["model"] != "claude-3-7-sonnet-remapped" {
		t.Errorf("expected model to be updated, got %v", parsed["model"])
	}

	thinking, ok := parsed["thinking"].(map[string]interface{})
	if !ok || thinking["type"] != "enabled" {
		t.Errorf("expected thinking field preserved, got %v", parsed["thinking"])
	}
	if parsed["reasoning_effort"] != "high" {
		t.Errorf("expected reasoning_effort preserved, got %v", parsed["reasoning_effort"])
	}
	if parsed["stream"] != true {
		t.Errorf("expected stream preserved, got %v", parsed["stream"])
	}
}

func TestDispatcherPriority(t *testing.T) {
	config.Global = &config.Config{
		Channels: []config.UpstreamChannel{
			{
				ID:       "chan-fallback-p2",
				Type:     "sub2api",
				BaseURL:  "http://fallback:8080/v1",
				Models:   []string{"claude-*"},
				Priority: 2,
			},
			{
				ID:       "chan-primary-p1",
				Type:     "sub2api",
				BaseURL:  "http://primary:8080/v1",
				Models:   []string{"claude-*"},
				Priority: 1,
			},
		},
	}

	for i := 0; i < 5; i++ {
		ch, _, err := DefaultDispatcher.Resolve("claude-3-7-sonnet")
		if err != nil {
			t.Fatalf("Resolve failed: %v", err)
		}
		if ch.ID != "chan-primary-p1" {
			t.Errorf("expected P1 channel chan-primary-p1, got %s", ch.ID)
		}
	}
}

func TestMultiKeyRotationAndFailoverOn429(t *testing.T) {
	var receivedKeys []string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		key := strings.TrimPrefix(auth, "Bearer ")
		receivedKeys = append(receivedKeys, key)

		if key == "key-1-exhausted" {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":{"message":"Rate limit exceeded","type":"requests"}}`))
			return
		}

		if key == "key-2-healthy" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"id":"chatcmpl-test","choices":[{"message":{"role":"assistant","content":"hello from key-2"}}]}`))
			return
		}

		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	ch := &config.UpstreamChannel{
		ID:      "multi-key-node",
		Type:    "openai",
		BaseURL: ts.URL,
		APIKeys: []string{"key-1-exhausted", "key-2-healthy"},
	}
	// The key rotation counter is intentionally process-wide so concurrent
	// requests share keys fairly. Reset it for this fixed-ID fixture to keep
	// repeated test runs (go test -count=2) independent of one another.
	adapter.RemoveChannelCounter(ch.ID)
	t.Cleanup(func() { adapter.RemoveChannelCounter(ch.ID) })

	adp := adapter.NewOpenAIAdapter()
	rec := httptest.NewRecorder()
	rawBody := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`)

	err := adp.ChatCompletions(context.Background(), ch, rawBody, "gpt-4o", false, rec)
	if err != nil {
		t.Fatalf("expected explicit 429 to rotate safely, got %v", err)
	}

	if len(receivedKeys) != 2 {
		t.Errorf("expected exactly two attempts after explicit 429, got %d: %v", len(receivedKeys), receivedKeys)
	}
}

func TestFetchModelsByUrlAndKey(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if auth != "Bearer valid-test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"object": "list",
			"data": [
				{"id": "claude-3-7-sonnet", "object": "model"},
				{"id": "gpt-4o", "object": "model"},
				{"id": "deepseek-v3", "object": "model"}
			]
		}`))
	}))
	defer ts.Close()

	ch := &config.UpstreamChannel{
		ID:      "test-probe",
		Type:    "openai",
		BaseURL: ts.URL,
		APIKey:  "valid-test-key",
	}

	adp := adapter.Get("openai")
	models, err := adp.FetchModels(context.Background(), ch)
	if err != nil {
		t.Fatalf("FetchModels failed: %v", err)
	}

	if len(models) != 3 {
		t.Fatalf("expected 3 models, got %d: %v", len(models), models)
	}

	expected := []string{"claude-3-7-sonnet", "gpt-4o", "deepseek-v3"}
	for i, m := range expected {
		if models[i] != m {
			t.Errorf("expected model %s at index %d, got %s", m, i, models[i])
		}
	}
}

func TestDispatcherFailoverOnHTTP502(t *testing.T) {
	DefaultDispatcher.RemoveChannel("broken-node-502")
	DefaultDispatcher.RemoveChannel("healthy-node-200")
	t.Cleanup(func() {
		DefaultDispatcher.RemoveChannel("broken-node-502")
		DefaultDispatcher.RemoveChannel("healthy-node-200")
	})
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(`{"error":"bad gateway on node 1"}`))
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"choices":[{"message":{"content":"success from node 2"}}]}`))
	}))
	defer ts2.Close()

	config.Global = &config.Config{
		Channels: []config.UpstreamChannel{
			{
				ID:       "broken-node-502",
				Type:     "openai",
				BaseURL:  ts1.URL,
				Models:   []string{"gpt-test"},
				Priority: 1,
			},
			{
				ID:       "healthy-node-200",
				Type:     "openai",
				BaseURL:  ts2.URL,
				Models:   []string{"gpt-test"},
				Priority: 2,
			},
		},
	}

	rec := httptest.NewRecorder()
	rawBody := []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"hi"}]}`)

	err := DefaultDispatcher.ExecuteWithFailover(context.Background(), "gpt-test", func() bool {
		return rec.Code == 0 || rec.Code == 200
	}, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		return adp.ChatCompletions(context.Background(), ch, rawBody, "gpt-test", false, rec)
	})

	if err != nil {
		t.Fatalf("ExecuteWithFailover failed: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from healthy node, got %d", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), "success from node 2") {
		t.Errorf("expected response from node 2, got: %s", rec.Body.String())
	}
}

func TestChannelModelFilterAndLoadBalance(t *testing.T) {
	// A, B, C 3个渠道：A 和 C 拥有 gpt-4o，B 仅拥有 claude-3-5-sonnet
	config.Global = &config.Config{
		Channels: []config.UpstreamChannel{
			{
				ID:       "channel-A",
				Type:     "openai",
				BaseURL:  "http://nodeA:8080/v1",
				Models:   []string{"gpt-4o"},
				Priority: 1,
			},
			{
				ID:       "channel-B",
				Type:     "openai",
				BaseURL:  "http://nodeB:8080/v1",
				Models:   []string{"claude-3-5-sonnet"},
				Priority: 1,
			},
			{
				ID:       "channel-C",
				Type:     "openai",
				BaseURL:  "http://nodeC:8080/v1",
				Models:   []string{"gpt-4o"},
				Priority: 1,
			},
		},
	}

	// 请求 gpt-4o 时，只有 A 和 C 参与轮询，B 绝不能被选中
	hitCount := make(map[string]int)
	for i := 0; i < 20; i++ {
		ch, _, err := DefaultDispatcher.Resolve("gpt-4o")
		if err != nil {
			t.Fatalf("Resolve failed: %v", err)
		}
		if ch.ID == "channel-B" {
			t.Fatalf("channel-B does not have gpt-4o, but was selected!")
		}
		hitCount[ch.ID]++
	}

	if hitCount["channel-A"] == 0 || hitCount["channel-C"] == 0 {
		t.Errorf("expected both channel-A and channel-C to be hit in round-robin, got: %v", hitCount)
	}

	// 请求 claude-3-5-sonnet 时，仅 B 被选中
	for i := 0; i < 5; i++ {
		ch, _, err := DefaultDispatcher.Resolve("claude-3-5-sonnet")
		if err != nil {
			t.Fatalf("Resolve claude failed: %v", err)
		}
		if ch.ID != "channel-B" {
			t.Fatalf("expected channel-B for claude, got %s", ch.ID)
		}
	}
}

func TestDispatcherRemoveChannel(t *testing.T) {
	DefaultDispatcher.modelsMu.Lock()
	DefaultDispatcher.remoteModels["temp-chan-1"] = []string{"temp-model-1"}
	DefaultDispatcher.modelsMu.Unlock()
	DefaultDispatcher.breakers.Store("temp-chan-1", &breakerState{failCount: 2})

	DefaultDispatcher.RemoveChannel("temp-chan-1")

	DefaultDispatcher.modelsMu.RLock()
	_, modelExists := DefaultDispatcher.remoteModels["temp-chan-1"]
	DefaultDispatcher.modelsMu.RUnlock()
	_, breakerExists := DefaultDispatcher.breakers.Load("temp-chan-1")

	if modelExists || breakerExists {
		t.Errorf("expected temp-chan-1 to be removed from dispatcher, got modelExists=%v breakerExists=%v", modelExists, breakerExists)
	}
}

func TestUpdateRemoteModelsOwnsItsSlice(t *testing.T) {
	dispatcher := &Dispatcher{}
	models := []string{"original-model"}
	dispatcher.UpdateRemoteModels("channel", models)
	models[0] = "mutated-by-caller"

	dispatcher.modelsMu.RLock()
	cached := dispatcher.remoteModels["channel"]
	dispatcher.modelsMu.RUnlock()
	if len(cached) != 1 || cached[0] != "original-model" {
		t.Fatalf("cached models changed after caller mutation: %v", cached)
	}
}

func TestDispatcherInvalidateChannelDropsStaleModels(t *testing.T) {
	dispatcher := &Dispatcher{remoteModels: map[string][]string{"updated": {"stale-model"}}}
	state := dispatcher.GetBreaker("updated")
	state.mu.Lock()
	state.failCount = 3
	state.lastFailure = time.Now()
	state.cooldownUntil = time.Now().Add(time.Minute)
	state.halfOpen = true
	state.mu.Unlock()

	dispatcher.InvalidateChannel("updated")

	dispatcher.modelsMu.RLock()
	_, found := dispatcher.remoteModels["updated"]
	dispatcher.modelsMu.RUnlock()
	if found {
		t.Fatal("stale remote models were retained after a channel change")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.failCount != 0 || !state.lastFailure.IsZero() || !state.cooldownUntil.IsZero() || state.halfOpen {
		t.Fatalf("breaker was not reset after a channel change: %+v", state)
	}
}

func TestSyncRemoteModelsCoalescesPendingRefresh(t *testing.T) {
	var calls atomic.Int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"refreshed-model"}]}`))
	}))
	defer server.Close()

	previousConfig := config.Global
	config.Global = &config.Config{Channels: []config.UpstreamChannel{{ID: "coalesced", Type: "openai", BaseURL: server.URL, Enabled: true, FetchModels: true}}}
	t.Cleanup(func() { config.Global = previousConfig })
	dispatcher := &Dispatcher{remoteModels: make(map[string][]string)}
	dispatcher.SyncRemoteModels(context.Background())
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first model sync did not start")
	}
	dispatcher.SyncRemoteModels(context.Background())
	close(releaseFirst)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		dispatcher.syncMu.Lock()
		syncing := dispatcher.syncing
		dispatcher.syncMu.Unlock()
		if !syncing && calls.Load() == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 2 {
		t.Fatalf("model sync calls = %d, want exactly 2", calls.Load())
	}
	dispatcher.modelsMu.RLock()
	models := append([]string(nil), dispatcher.remoteModels["coalesced"]...)
	dispatcher.modelsMu.RUnlock()
	if len(models) != 1 || models[0] != "refreshed-model" {
		t.Fatalf("coalesced refresh models = %v", models)
	}
}

func TestStopSyncCancelsInFlightRefresh(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	previousConfig := config.Global
	config.Global = &config.Config{Channels: []config.UpstreamChannel{{ID: "cancellable", Type: "openai", BaseURL: server.URL, Enabled: true, FetchModels: true}}}
	t.Cleanup(func() { config.Global = previousConfig })
	dispatcher := &Dispatcher{remoteModels: make(map[string][]string)}
	dispatcher.SyncRemoteModels(context.Background())
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("model sync did not start")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := dispatcher.StopSync(stopCtx); err != nil {
		t.Fatalf("StopSync = %v", err)
	}
	dispatcher.syncMu.Lock()
	syncing := dispatcher.syncing
	dispatcher.syncMu.Unlock()
	if syncing {
		t.Fatal("model sync remained active after StopSync")
	}
}

func TestDatabaseCloseStopsDefaultModelSync(t *testing.T) {
	if err := db.InitDB(filepath.Join(t.TempDir(), "dispatcher-close.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	started := make(chan struct{})
	stopped := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(stopped)
	}))
	defer server.Close()

	channel := &db.ChannelModel{
		ID:          "close-sync",
		Name:        "close-sync",
		Type:        "openai",
		BaseURL:     server.URL,
		Enabled:     true,
		FetchModels: true,
		Priority:    1,
		Weight:      1,
	}
	if err := db.SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { DefaultDispatcher.RemoveChannel(channel.ID) })

	DefaultDispatcher.SyncRemoteModels(context.Background())
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("model sync did not start")
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close database: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("database close did not wait for model sync cancellation")
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("model sync request remained active after database close")
	}
}

func TestSlashWildcardMatching(t *testing.T) {
	cases := []struct {
		pattern string
		model   string
		expect  bool
	}{
		{"*flux*", "black-forest-labs/FLUX.1-schnell", true},
		{"black-forest-labs/*", "black-forest-labs/FLUX.1-schnell", true},
		{"*r1*", "deepseek-ai/DeepSeek-R1", true},
		{"deepseek-ai/*", "deepseek-ai/DeepSeek-R1", true},
		{"qwen/*", "qwen/qwen-2.5-72b-instruct", true},
		{"*llama-3*", "meta-llama/Llama-3.3-70B-Instruct", true},
		{"claude-*", "claude-3-7-sonnet", true},
		{"gpt-4o*", "gpt-4o-mini", true},
		{"openai/*", "anthropic/claude-3", false},
	}

	for _, c := range cases {
		got := matchPattern(c.pattern, c.model)
		if got != c.expect {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", c.pattern, c.model, got, c.expect)
		}
	}
}

func TestCircuitBreakerSlidingWindowDecay(t *testing.T) {
	chanID := "test-decay-channel"
	DefaultDispatcher.RemoveChannel(chanID)

	// 1. 模拟两次失败
	DefaultDispatcher.recordFailure(chanID)
	DefaultDispatcher.recordFailure(chanID)

	st := DefaultDispatcher.GetBreaker(chanID)
	st.mu.Lock()
	if st.failCount != 2 {
		t.Errorf("expected failCount=2, got %d", st.failCount)
	}
	// 2. 模拟时间跨越 65 秒（超过 1 分钟衰减窗口）
	st.lastFailure = time.Now().Add(-65 * time.Second)
	st.mu.Unlock()

	// 3. 再次失败一次：由于超过 1 分钟，failCount 应先衰减复位为 0，然后自增为 1（不会触发熔断冷却）
	DefaultDispatcher.recordFailure(chanID)

	st.mu.Lock()
	if st.failCount != 1 {
		t.Errorf("expected failCount to decay and become 1, got %d", st.failCount)
	}
	st.mu.Unlock()

	if !DefaultDispatcher.isAvailable(chanID) {
		t.Errorf("channel should still be available, but cooldown was triggered prematurely")
	}

	// 4. 连续快速失败 2 次（达到 3 次），应立即触发熔断
	DefaultDispatcher.recordFailure(chanID)
	DefaultDispatcher.recordFailure(chanID)

	if DefaultDispatcher.isAvailable(chanID) {
		t.Errorf("channel should be in cooldown after 3 rapid consecutive failures")
	}

	// 5. 成功一次，应彻底复位熔断状态
	DefaultDispatcher.recordSuccess(chanID)
	if !DefaultDispatcher.isAvailable(chanID) {
		t.Errorf("channel should be available after recordSuccess")
	}
}

func TestClientContextCancellation(t *testing.T) {
	chA := config.UpstreamChannel{
		ID:      "cancel-test-node-1",
		Type:    "openai",
		BaseURL: "http://localhost:11111",
		Enabled: true,
		Models:  []string{"cancel-model"},
	}
	chB := config.UpstreamChannel{
		ID:      "cancel-test-node-2",
		Type:    "openai",
		BaseURL: "http://localhost:22222",
		Enabled: true,
		Models:  []string{"cancel-model"},
	}
	config.Global.Channels = []config.UpstreamChannel{chA, chB}

	ctx, cancel := context.WithCancel(context.Background())
	// 立即取消客户端上下文
	cancel()

	attempts := 0
	err := DefaultDispatcher.ExecuteWithFailover(ctx, "cancel-model", func() bool { return true }, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		attempts++
		return ctx.Err()
	})

	if err == nil {
		t.Fatalf("expected error on cancelled context, got nil")
	}

	// 客户端取消时，应该在第 1 次尝试后立刻退出，不应该重试第 2 个渠道
	if attempts != 1 {
		t.Errorf("expected exactly 1 attempt on client cancel, but got %d attempts", attempts)
	}

	// 渠道 1 的熔断器状态不能被误判为失败
	st := DefaultDispatcher.GetBreaker("cancel-test-node-1")
	st.mu.Lock()
	if st.failCount != 0 {
		t.Errorf("expected failCount=0 on client cancellation, got %d", st.failCount)
	}
	st.mu.Unlock()
}

func TestMultiTierRoundRobinParity(t *testing.T) {
	// 配置两个 P1 渠道和两个 P2 备用渠道
	chP1A := config.UpstreamChannel{ID: "p1-node-a", Type: "openai", BaseURL: "http://1", Priority: 1, Enabled: true, Models: []string{"test-parity-model"}}
	chP1B := config.UpstreamChannel{ID: "p1-node-b", Type: "openai", BaseURL: "http://2", Priority: 1, Enabled: true, Models: []string{"test-parity-model"}}
	chP2A := config.UpstreamChannel{ID: "p2-node-a", Type: "openai", BaseURL: "http://3", Priority: 2, Enabled: true, Models: []string{"test-parity-model"}}
	chP2B := config.UpstreamChannel{ID: "p2-node-b", Type: "openai", BaseURL: "http://4", Priority: 2, Enabled: true, Models: []string{"test-parity-model"}}

	config.Global.Channels = []config.UpstreamChannel{chP1A, chP1B, chP2A, chP2B}

	hits := make(map[string]int)
	for i := 0; i < 100; i++ {
		cands, err := DefaultDispatcher.ResolveCandidates("test-parity-model")
		if err != nil {
			t.Fatalf("ResolveCandidates failed: %v", err)
		}
		if len(cands) != 4 {
			t.Fatalf("expected 4 candidates, got %d", len(cands))
		}
		// 首选必然是 P1 节点中的一个
		firstID := cands[0].ID
		if firstID != "p1-node-a" && firstID != "p1-node-b" {
			t.Fatalf("expected first candidate to be a P1 node, got %s", firstID)
		}
		hits[firstID]++
	}

	// 100 次调用下，A 和 B 应当严格各被选中 50 次，绝不能出现某个节点一直饥饿（0 次）的情况
	if hits["p1-node-a"] != 50 || hits["p1-node-b"] != 50 {
		t.Errorf("expected strictly 50/50 balance between P1 nodes, got: %v", hits)
	}
}

func TestWeightedScheduling(t *testing.T) {
	chA := config.UpstreamChannel{ID: "node-weight-3", Type: "openai", BaseURL: "http://1", Priority: 1, Weight: 3, Enabled: true, Models: []string{"weighted-model"}}
	chB := config.UpstreamChannel{ID: "node-weight-1", Type: "openai", BaseURL: "http://2", Priority: 1, Weight: 1, Enabled: true, Models: []string{"weighted-model"}}

	config.Global.Channels = []config.UpstreamChannel{chA, chB}

	hits := make(map[string]int)
	totalRuns := 40
	for i := 0; i < totalRuns; i++ {
		cands, err := DefaultDispatcher.ResolveCandidates("weighted-model")
		if err != nil {
			t.Fatalf("ResolveCandidates failed: %v", err)
		}
		if len(cands) != 2 {
			t.Fatalf("expected 2 candidates, got %d", len(cands))
		}
		// 验证没有重复渠道
		if cands[0].ID == cands[1].ID {
			t.Fatalf("duplicate channel in candidate list: %s", cands[0].ID)
		}
		hits[cands[0].ID]++
	}

	// 40 次调用在 3:1 权重下，应精确为 30 次与 10 次
	if hits["node-weight-3"] != 30 || hits["node-weight-1"] != 10 {
		t.Errorf("expected 30:10 ratio, got: %v", hits)
	}
}

func TestResetBreaker(t *testing.T) {
	chanID := "breaker-reset-test"
	st := DefaultDispatcher.GetBreaker(chanID)

	// 触发 3 次失败进入熔断
	DefaultDispatcher.recordFailure(chanID)
	DefaultDispatcher.recordFailure(chanID)
	DefaultDispatcher.recordFailure(chanID)

	if DefaultDispatcher.isAvailable(chanID) {
		t.Fatalf("channel %s should be in cooldown after 3 failures", chanID)
	}

	// 调用 ResetBreaker 应当立刻复位熔断
	DefaultDispatcher.ResetBreaker(chanID)

	if !DefaultDispatcher.isAvailable(chanID) {
		t.Errorf("channel %s should be immediately available after ResetBreaker", chanID)
	}

	st.mu.Lock()
	if st.failCount != 0 {
		t.Errorf("failCount should be 0 after ResetBreaker, got %d", st.failCount)
	}
	st.mu.Unlock()
}

func TestCoolingPrimaryDoesNotDisplaceHealthyFallback(t *testing.T) {
	dispatcher := &Dispatcher{remoteModels: make(map[string][]string)}
	config.Global = &config.Config{Channels: []config.UpstreamChannel{
		{ID: "cooling-primary", Type: "openai", BaseURL: "http://primary", Priority: 1, Enabled: true, Models: []string{"model-a"}},
		{ID: "healthy-fallback", Type: "openai", BaseURL: "http://fallback", Priority: 2, Enabled: true, Models: []string{"model-a"}},
	}}
	for i := 0; i < 3; i++ {
		dispatcher.recordFailure("cooling-primary")
	}
	candidates, err := dispatcher.ResolveCandidatesForProtocol("model-a", "chat")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) == 0 || candidates[0].ID != "healthy-fallback" {
		t.Fatalf("healthy lower-priority channel must win while primary is cooling: %+v", candidates)
	}
}

func TestHalfOpenAllowsOnlyOneProbe(t *testing.T) {
	dispatcher := &Dispatcher{remoteModels: make(map[string][]string)}
	state := dispatcher.GetBreaker("half-open-channel")
	state.mu.Lock()
	state.failCount = 3
	state.cooldownUntil = time.Now().Add(-time.Second)
	state.halfOpen = false
	state.mu.Unlock()
	if !dispatcher.tryAcquireChannel("half-open-channel") {
		t.Fatal("first request should acquire half-open probe")
	}
	if dispatcher.tryAcquireChannel("half-open-channel") {
		t.Fatal("second request must not acquire the same half-open probe")
	}
	dispatcher.recordSuccess("half-open-channel")
	if !dispatcher.tryAcquireChannel("half-open-channel") {
		t.Fatal("channel should be available after successful probe")
	}
}

func TestInferenceDoesNotFailOverOnUncertain502(t *testing.T) {
	dispatcher := &Dispatcher{remoteModels: make(map[string][]string)}
	config.Global = &config.Config{Channels: []config.UpstreamChannel{
		{ID: "first", Type: "openai", BaseURL: "http://first", Priority: 1, Enabled: true, Models: []string{"model-b"}},
		{ID: "second", Type: "openai", BaseURL: "http://second", Priority: 2, Enabled: true, Models: []string{"model-b"}},
	}}
	attempts := 0
	err := dispatcher.ExecuteWithPolicy(context.Background(), "model-b", "chat", RetryInference, func() bool { return true }, func(_ *config.UpstreamChannel, _ adapter.Adapter) error {
		attempts++
		return &adapter.UpstreamHTTPError{StatusCode: http.StatusBadGateway, Body: "uncertain upstream result"}
	})
	if err == nil || attempts != 1 {
		t.Fatalf("inference 502 must not be replayed, attempts=%d err=%v", attempts, err)
	}
}

func TestCreateTaskFailoverKeepsIdempotencyKeyAfterExplicitRejection(t *testing.T) {
	var firstKey, secondKey string
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstKey = r.Header.Get("Idempotency-Key")
		http.Error(w, `{"error":"route not supported"}`, http.StatusNotFound)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondKey = r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"video-stable","status":"queued"}`))
	}))
	defer second.Close()

	config.Global = &config.Config{Channels: []config.UpstreamChannel{
		{ID: "create-rejected", Type: "openai", BaseURL: first.URL + "/v1", APIKey: "first", Enabled: true, Priority: 1, Models: []string{"video-stable-model"}},
		{ID: "create-accepted", Type: "openai", BaseURL: second.URL + "/v1", APIKey: "second", Enabled: true, Priority: 2, Models: []string{"video-stable-model"}},
	}}
	dispatcher := &Dispatcher{remoteModels: make(map[string][]string)}
	ctx := context.WithValue(context.Background(), adapter.CtxIdempotencyKey, "client-stable-key")
	var response *model.VideoTaskResponse
	err := dispatcher.ExecuteWithPolicy(ctx, "video-stable-model", "video", RetryCreateTask, func() bool { return response == nil }, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		var callErr error
		response, callErr = adp.CreateVideo(ctx, ch, &model.VideoGenerationRequest{Model: "video-stable-model", Prompt: "test"})
		return callErr
	})
	if err != nil || response == nil || response.ID != "video-stable" {
		t.Fatalf("explicitly rejected create did not fail over safely: response=%+v err=%v", response, err)
	}
	if firstKey != "client-stable-key" || secondKey != firstKey {
		t.Fatalf("create failover changed idempotency key: first=%q second=%q", firstKey, secondKey)
	}
}

func TestCreateTaskDoesNotFailOverOnUncertainServerError(t *testing.T) {
	firstCalls, secondCalls := 0, 0
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls++
		http.Error(w, `{"error":"unknown create result"}`, http.StatusBadGateway)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls++
		_, _ = w.Write([]byte(`{"id":"duplicate","status":"queued"}`))
	}))
	defer second.Close()

	config.Global = &config.Config{Channels: []config.UpstreamChannel{
		{ID: "uncertain-create", Type: "openai", BaseURL: first.URL + "/v1", APIKey: "first", Enabled: true, Priority: 1, Models: []string{"video-uncertain-model"}},
		{ID: "duplicate-create", Type: "openai", BaseURL: second.URL + "/v1", APIKey: "second", Enabled: true, Priority: 2, Models: []string{"video-uncertain-model"}},
	}}
	dispatcher := &Dispatcher{remoteModels: make(map[string][]string)}
	err := dispatcher.ExecuteWithPolicy(context.Background(), "video-uncertain-model", "video", RetryCreateTask, func() bool { return true }, func(ch *config.UpstreamChannel, adp adapter.Adapter) error {
		_, callErr := adp.CreateVideo(context.Background(), ch, &model.VideoGenerationRequest{Model: "video-uncertain-model", Prompt: "test"})
		return callErr
	})
	if err == nil || firstCalls != 1 || secondCalls != 0 {
		t.Fatalf("uncertain create was replayed: first=%d second=%d err=%v", firstCalls, secondCalls, err)
	}
}
