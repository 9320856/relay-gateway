package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
	"relay-gateway/adapter"
	"relay-gateway/audit"
	"relay-gateway/config"
	"relay-gateway/db"
	"relay-gateway/model"
	"relay-gateway/protocol"
)

type breakerState struct {
	mu            sync.Mutex
	failCount     int
	lastFailure   time.Time
	cooldownUntil time.Time
	halfOpen      bool
}

type RetryPolicy int

const (
	RetrySafeRead RetryPolicy = iota
	RetryInference
	RetryCreateTask
)

type Dispatcher struct {
	modelsMu        sync.RWMutex
	remoteModels    map[string][]string // channelID -> []modelName
	modelGeneration map[string]uint64   // channelID -> configuration generation
	breakers        sync.Map            // channelID (string) -> *breakerState (细粒度独立分段锁，零并发竞争)
	rrCounter       atomic.Uint64
	syncMu          sync.Mutex
	syncing         bool
	syncPending     bool
	syncCancel      context.CancelFunc
	syncDone        chan struct{}
}

// Keep background model discovery bounded.  Without a cap, a configuration
// containing hundreds of channels creates the same number of simultaneous
// /models requests and SQLite health writes.
const maxConcurrentModelSync = 8

var DefaultDispatcher = &Dispatcher{
	remoteModels: make(map[string][]string),
}

func init() {
	db.OnChannelSaved = func(channelID string) {
		DefaultDispatcher.InvalidateChannel(channelID)
	}
	db.BeforeClose = func() {
		_ = DefaultDispatcher.StopSync(nil)
	}
}

func (d *Dispatcher) GetBreaker(channelID string) *breakerState {
	val, ok := d.breakers.Load(channelID)
	if ok {
		return val.(*breakerState)
	}
	st := &breakerState{}
	actual, _ := d.breakers.LoadOrStore(channelID, st)
	return actual.(*breakerState)
}

// ResetBreaker 复位渠道的熔断状态与失败计数（在用户更新渠道配置或切换开启状态时调用）
func (d *Dispatcher) ResetBreaker(channelID string) {
	val, ok := d.breakers.Load(channelID)
	if !ok {
		return
	}
	st := val.(*breakerState)
	st.mu.Lock()
	st.failCount = 0
	st.cooldownUntil = time.Time{}
	st.lastFailure = time.Time{}
	st.halfOpen = false
	st.mu.Unlock()
}

// InvalidateChannel drops dynamic state derived from a channel configuration.
// A saved channel may point at a different provider or credential set, so its
// previously discovered model list must not participate in routing while the
// next refresh is pending.
func (d *Dispatcher) InvalidateChannel(channelID string) {
	d.ResetBreaker(channelID)
	d.modelsMu.Lock()
	delete(d.remoteModels, channelID)
	if d.modelGeneration == nil {
		d.modelGeneration = make(map[string]uint64)
	}
	d.modelGeneration[channelID]++
	d.modelsMu.Unlock()
}

func matchPattern(pattern, name string) bool {
	p := strings.ToLower(strings.TrimSpace(pattern))
	n := strings.ToLower(strings.TrimSpace(name))
	if p == "*" || p == n {
		return true
	}
	// 快速子串匹配优化
	if strings.HasPrefix(p, "*") && strings.HasSuffix(p, "*") && len(p) > 2 {
		return strings.Contains(n, p[1:len(p)-1])
	}
	if strings.HasPrefix(p, "*") && !strings.Contains(p[1:], "*") {
		return strings.HasSuffix(n, p[1:])
	}
	if strings.HasSuffix(p, "*") && !strings.Contains(p[:len(p)-1], "*") {
		return strings.HasPrefix(n, p[:len(p)-1])
	}
	// 解决 path.Match 对带命名空间模型斜杠（如 black-forest-labs/flux）不通配的问题
	pNorm := strings.ReplaceAll(p, "/", "_")
	nNorm := strings.ReplaceAll(n, "/", "_")
	matched, err := path.Match(pNorm, nNorm)
	return err == nil && matched
}

// getActiveChannels 从内存高速缓存中获取已启用的渠道列表 (0 数据库 IO)
func (d *Dispatcher) getActiveChannels() []config.UpstreamChannel {
	return db.GetActiveUpstreamChannels()
}

// isAvailable 检查渠道是否处于熔断冷却中 (无锁高并发安全)
func (d *Dispatcher) isAvailable(channelID string) bool {
	return d.tryAcquireChannel(channelID)
}

// candidateAvailable is a read-only breaker check used while constructing a
// candidate list. A half-open slot is reserved only immediately before an
// upstream attempt, otherwise an unused lower-priority candidate could remain
// stuck in half-open state forever.
func (d *Dispatcher) candidateAvailable(channelID string) bool {
	val, ok := d.breakers.Load(channelID)
	if !ok {
		return true
	}
	st := val.(*breakerState)
	st.mu.Lock()
	defer st.mu.Unlock()
	now := time.Now()
	if st.cooldownUntil.IsZero() {
		return true
	}
	if now.Before(st.cooldownUntil) {
		return false
	}
	return !st.halfOpen
}

func (d *Dispatcher) tryAcquireChannel(channelID string) bool {
	val, ok := d.breakers.Load(channelID)
	if !ok {
		return true
	}
	st := val.(*breakerState)
	st.mu.Lock()
	defer st.mu.Unlock()
	now := time.Now()
	if st.cooldownUntil.IsZero() {
		return true
	}
	if now.Before(st.cooldownUntil) || st.halfOpen {
		return false
	}
	st.halfOpen = true
	return true
}

// releaseHalfOpenProbe is a safety net for every return path after a
// half-open slot has been acquired. Normal success/failure recording also
// clears the slot, but validation errors, cancellation and non-retryable
// provider responses must not leave a channel permanently stuck.
func (d *Dispatcher) releaseHalfOpenProbe(channelID string) {
	val, ok := d.breakers.Load(channelID)
	if !ok {
		return
	}
	st := val.(*breakerState)
	st.mu.Lock()
	if st.halfOpen {
		st.halfOpen = false
	}
	st.mu.Unlock()
}

// recordSuccess 记录渠道调用成功，复位熔断计数 (独立通道锁，不阻塞全局调度)
func (d *Dispatcher) recordSuccess(channelID string) {
	val, ok := d.breakers.Load(channelID)
	if !ok {
		return
	}
	st := val.(*breakerState)
	st.mu.Lock()
	st.failCount = 0
	st.cooldownUntil = time.Time{}
	st.halfOpen = false
	st.mu.Unlock()
}

// recordFailure 记录渠道调用失败，连续 3 次失败触发 30 秒熔断保护（带 1 分钟滑动窗口衰减，独立通道锁）
func (d *Dispatcher) recordFailure(channelID string) {
	st := d.GetBreaker(channelID)
	st.mu.Lock()
	defer st.mu.Unlock()

	now := time.Now()
	// 滑动窗口衰减：若距离上次失败已超过 1 分钟，重置连续失败计数
	if !st.lastFailure.IsZero() && now.Sub(st.lastFailure) > 1*time.Minute {
		st.failCount = 0
	}
	st.lastFailure = now
	st.failCount++
	st.halfOpen = false

	if st.failCount >= 3 {
		st.cooldownUntil = now.Add(30 * time.Second)
		log.Printf("[CircuitBreaker] Channel [%s] entered 30s cooldown due to %d consecutive failures", channelID, st.failCount)
	}
}

// RemoveChannel 从调度器内存中清理已删除渠道的动态模型映射、熔断计数与 Key 轮询器
func (d *Dispatcher) RemoveChannel(channelID string) {
	d.InvalidateChannel(channelID)
	d.breakers.Delete(channelID)
	adapter.RemoveChannelCounter(channelID)
}

// UpdateRemoteModels 线程安全地将指定渠道最新同步的模型写入调度器内存，立即可参与智能路由
func (d *Dispatcher) UpdateRemoteModels(channelID string, models []string) {
	d.modelsMu.Lock()
	defer d.modelsMu.Unlock()
	if d.remoteModels == nil {
		d.remoteModels = make(map[string][]string)
	}
	// Callers commonly reuse their decoded response slice. Keep the cache
	// independent so later caller-side mutations cannot silently affect routing.
	d.remoteModels[channelID] = append([]string(nil), models...)
}

// LoadCachedModelsFromDB 从 SQLite 预热缓存的模型到内存，避免服务冷启动瞬态空窗
func (d *Dispatcher) LoadCachedModelsFromDB() {
	cms, err := db.GetAllChannelModels()
	if err != nil {
		return
	}
	d.modelsMu.Lock()
	defer d.modelsMu.Unlock()
	warmedCount := 0
	for _, cm := range cms {
		if cm.ModelsSyncedRaw != "" {
			var models []string
			if err := json.Unmarshal([]byte(cm.ModelsSyncedRaw), &models); err == nil && len(models) > 0 {
				d.remoteModels[cm.ID] = models
				warmedCount += len(models)
			}
		}
	}
	if warmedCount > 0 {
		log.Printf("[Dispatcher] Warm-loaded %d models from SQLite cache into memory", warmedCount)
	}
}

// SyncRemoteModels traverses active channels and refreshes their models. A
// request that arrives while a refresh is running is coalesced into exactly
// one follow-up pass, so a just-saved channel cannot be left with stale model
// capabilities merely because a previous refresh was in flight.
func (d *Dispatcher) SyncRemoteModels(_ context.Context) {
	d.syncMu.Lock()
	if d.syncing {
		d.syncPending = true
		d.syncMu.Unlock()
		return
	}
	d.syncing = true
	d.syncPending = false
	workerCtx, cancel := context.WithCancel(context.Background())
	d.syncCancel = cancel
	done := make(chan struct{})
	d.syncDone = done
	d.syncMu.Unlock()
	go func() {
		defer func() {
			d.syncMu.Lock()
			d.syncing = false
			d.syncPending = false
			d.syncCancel = nil
			d.syncDone = nil
			close(done)
			d.syncMu.Unlock()
		}()
		for {
			d.syncRemoteModelsOnce(workerCtx)
			d.syncMu.Lock()
			if !d.syncPending {
				d.syncMu.Unlock()
				return
			}
			d.syncPending = false
			d.syncMu.Unlock()
		}
	}()
}

// StopSync cancels an in-flight model refresh and waits for every fetch to
// finish. It is used during process shutdown before SQLite is closed.
func (d *Dispatcher) StopSync(ctx context.Context) error {
	d.syncMu.Lock()
	cancel, done := d.syncCancel, d.syncDone
	d.syncMu.Unlock()
	if cancel == nil || done == nil {
		return nil
	}
	cancel()
	if ctx == nil {
		<-done
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Dispatcher) syncRemoteModelsOnce(ctx context.Context) {
	syncCtx, syncCancel := context.WithTimeout(ctx, 60*time.Second)
	defer syncCancel()
	channels := d.getActiveChannels()
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentModelSync)
	for i := range channels {
		ch := channels[i]
		if !ch.FetchModels {
			continue
		}

		wg.Add(1)
		go func(channel config.UpstreamChannel) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-syncCtx.Done():
				return
			}
			fetchCtx, cancel := context.WithTimeout(syncCtx, 15*time.Second)
			defer cancel()
			d.modelsMu.RLock()
			generation := d.modelGeneration[channel.ID]
			d.modelsMu.RUnlock()

			start := time.Now()
			models, err := protocol.DiscoverModels(fetchCtx, channel.BaseURL, channel.GetEffectiveKeys(), channel.Headers, protocol.ProfileModelDefaults(channel.Type))
			latency := int(time.Since(start).Milliseconds())
			d.modelsMu.RLock()
			currentGeneration := d.modelGeneration[channel.ID]
			d.modelsMu.RUnlock()
			if currentGeneration != generation {
				return
			}

			if err != nil {
				log.Printf("[Dispatcher] Fetch models from %s (%s) failed: %v", channel.ID, channel.Type, err)
				db.UpdateChannelHealth(channel.ID, "error", latency, err.Error(), nil)
				return
			}

			d.modelsMu.Lock()
			// A save/delete may have invalidated this request while the upstream
			// call was in flight. Never let the old response repopulate a newly
			// configured channel's model cache.
			if d.modelGeneration[channel.ID] != generation {
				d.modelsMu.Unlock()
				return
			}
			d.remoteModels[channel.ID] = append([]string(nil), models...)
			d.modelsMu.Unlock()

			db.UpdateChannelHealth(channel.ID, "healthy", latency, "", models)
			log.Printf("[Dispatcher] Successfully synced %d models from upstream [%s] (%s, %dms)", len(models), channel.ID, channel.Type, latency)
		}(ch)
	}
	wg.Wait()
}

// scheduleHealthyChannels 结合平滑加权轮询 (Smooth Weighted Round Robin) 与权重打散生成该组的候选队列
func scheduleHealthyChannels(healthy []*config.UpstreamChannel, rrIndex uint64) []*config.UpstreamChannel {
	k := len(healthy)
	if k == 0 {
		return nil
	}
	if k == 1 {
		return []*config.UpstreamChannel{healthy[0]}
	}

	// 检查是否所有权重均相同
	allSameWeight := true
	firstWeight := healthy[0].Weight
	if firstWeight <= 0 {
		firstWeight = 1
	}
	for i := 1; i < k; i++ {
		w := healthy[i].Weight
		if w <= 0 {
			w = 1
		}
		if w != firstWeight {
			allSameWeight = false
			break
		}
	}

	if allSameWeight {
		start := int(rrIndex % uint64(k))
		ordered := make([]*config.UpstreamChannel, k)
		for i := 0; i < k; i++ {
			ordered[i] = healthy[(start+i)%k]
		}
		return ordered
	}

	// 平滑加权轮询 (Smooth Weighted Round Robin)
	weights := make([]int, k)
	totalWeight := 0
	for i, ch := range healthy {
		w := ch.Weight
		if w <= 0 {
			w = 1
		}
		if w > 100 {
			w = 100
		}
		weights[i] = w
		totalWeight += w
	}

	schedule := make([]int, totalWeight)
	curWeights := make([]int, k)
	for s := 0; s < totalWeight; s++ {
		maxIdx := 0
		maxVal := -1 << 30
		for i := 0; i < k; i++ {
			curWeights[i] += weights[i]
			if curWeights[i] > maxVal {
				maxVal = curWeights[i]
				maxIdx = i
			}
		}
		curWeights[maxIdx] -= totalWeight
		schedule[s] = maxIdx
	}

	primaryIdx := schedule[rrIndex%uint64(totalWeight)]

	ordered := make([]*config.UpstreamChannel, 0, k)
	ordered = append(ordered, healthy[primaryIdx])
	for i := 1; i < k; i++ {
		ordered = append(ordered, healthy[(primaryIdx+i)%k])
	}
	return ordered
}

// ResolveCandidates 结合【优先级分层 + 智能熔断 + 加权轮询】生成有序候选列表
func (d *Dispatcher) ResolveCandidates(modelName string) ([]*config.UpstreamChannel, error) {
	return d.ResolveCandidatesForProtocol(modelName, "")
}

// ResolveProfileCandidates returns channels that have an enabled binding for
// the requested profile operation. Unlike ResolveCandidatesForProtocol, this
// path deliberately does not consult the legacy Adapter registry: a channel
// can be migrated to a published Profile before its old Adapter is removed.
// Priority, weight, model matching, and breaker behavior remain identical to
// the legacy dispatcher so rollout does not change scheduling semantics.
func (d *Dispatcher) ResolveProfileCandidates(modelName, operation string) ([]*config.UpstreamChannel, error) {
	channels := d.getActiveChannels()
	if len(channels) == 0 {
		return nil, fmt.Errorf("no active upstream channels configured or all channels disabled")
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		return nil, fmt.Errorf("profile operation is required")
	}
	modelName = strings.TrimSpace(modelName)
	eligible := make([]config.UpstreamChannel, 0, len(channels))
	for i := range channels {
		ch := channels[i]
		_, err := db.FindChannelProtocolBindingContext(context.Background(), ch.ID, operation, modelName)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("resolve profile binding for channel %s: %w", ch.ID, err)
		}
		eligible = append(eligible, ch)
	}
	if len(eligible) == 0 {
		return nil, fmt.Errorf("no active upstream channel has an enabled profile binding for operation %q and model %q", operation, modelName)
	}
	return d.orderCandidatePool(eligible, modelName, operation)
}

func (d *Dispatcher) ResolveCandidatesForProtocol(modelName, protocol string) ([]*config.UpstreamChannel, error) {
	channels := d.getActiveChannels()
	if len(channels) == 0 {
		return nil, fmt.Errorf("no active upstream channels configured or all channels disabled")
	}

	eligible := make([]config.UpstreamChannel, 0, len(channels))
	for i := range channels {
		ch := channels[i]
		meta, ok := adapter.GetMeta(ch.Type)
		if adapter.Get(ch.Type) == nil || !ok {
			continue
		}
		if protocol == "anthropic_messages" && ch.Type != "anthropic" {
			continue
		}
		if protocol != "" && protocol != "anthropic_messages" && !meta.Supports(protocol) {
			continue
		}
		eligible = append(eligible, ch)
	}
	if len(eligible) == 0 {
		return nil, fmt.Errorf("no active upstream channel supports protocol %q", protocol)
	}
	return d.orderCandidatePool(eligible, modelName, protocol)
}

// orderCandidatePool applies the common model matching, priority, weighted
// round-robin, and circuit-breaker ordering to an already-filtered channel
// pool. Keeping this separate lets Profile routing share the mature dispatcher
// scheduling without depending on legacy adapter metadata.
func (d *Dispatcher) orderCandidatePool(eligible []config.UpstreamChannel, modelName, protocol string) ([]*config.UpstreamChannel, error) {
	trimmed := strings.TrimSpace(modelName)

	// 1. 过滤实际拥有该模型的渠道（检查上游同步的模型库、配置模型列表或 ModelMap 映射）
	d.modelsMu.RLock()
	var matchedWithModel []*config.UpstreamChannel
	for i := range eligible {
		ch := &eligible[i]
		hasModel := false

		// 检查上游拉取/同步的动态模型列表
		if remotes, ok := d.remoteModels[ch.ID]; ok {
			for _, rm := range remotes {
				if strings.EqualFold(rm, trimmed) {
					hasModel = true
					break
				}
			}
		}
		// 检查渠道模型列表 (支持精确名称或通配前缀)
		if !hasModel {
			for _, m := range ch.Models {
				if m != "*" && (strings.EqualFold(m, trimmed) || (strings.ContainsAny(m, "*?") && matchPattern(m, trimmed))) {
					hasModel = true
					break
				}
			}
		}
		// 检查 ModelMap 映射
		if !hasModel {
			if _, ok := ch.ModelMap[trimmed]; ok {
				hasModel = true
			}
		}

		if hasModel {
			matchedWithModel = append(matchedWithModel, ch)
		}
	}
	d.modelsMu.RUnlock()

	var matchedPool []*config.UpstreamChannel
	if len(matchedWithModel) > 0 {
		// 拥有该模型的渠道候选池 (例如 A 和 C 有该模型，B 没有，则仅 A 和 C 参与！)
		matchedPool = matchedWithModel
	} else {
		// 兜底：若没有任何渠道明确上报过该模型，优先匹配通配符 * 渠道；若无则回退到所有已启用渠道
		for i := range eligible {
			ch := &eligible[i]
			for _, m := range ch.Models {
				if m == "*" {
					matchedPool = append(matchedPool, ch)
					break
				}
			}
		}
		if len(matchedPool) == 0 {
			for i := range eligible {
				matchedPool = append(matchedPool, &eligible[i])
			}
		}
	}

	// 优先级分层 (Priority Tiering)：按 Priority 升序分组 (如 P1 主用组, P2 备用组)
	priorityGroups := make(map[int][]*config.UpstreamChannel)
	var priorities []int
	for _, ch := range matchedPool {
		p := ch.Priority
		if p <= 0 {
			p = 1
		}
		if _, exists := priorityGroups[p]; !exists {
			priorities = append(priorities, p)
		}
		priorityGroups[p] = append(priorityGroups[p], ch)
	}
	sort.Ints(priorities)

	var finalOrdered []*config.UpstreamChannel
	// 采样请求级别的轮询基准索引（单次自增采样，杜绝跨优先级循环重复累加导致的偶数步长死锁与节点饥饿）
	rrIndex := d.rrCounter.Add(1) - 1

	// 依次处理各个优先级组
	for _, p := range priorities {
		group := priorityGroups[p]
		if len(group) == 0 {
			continue
		}

		// 过滤出未熔断的健康节点
		var healthy []*config.UpstreamChannel
		for _, ch := range group {
			if d.candidateAvailable(ch.ID) {
				healthy = append(healthy, ch)
			}
		}

		// 如果健康节点存在，优先采用 Smooth Weighted Round Robin 加权轮询
		if len(healthy) > 0 {
			finalOrdered = append(finalOrdered, scheduleHealthyChannels(healthy, rrIndex)...)
		}

	}
	if len(finalOrdered) == 0 {
		return nil, fmt.Errorf("no healthy upstream channel supports model %q and protocol %q", modelName, protocol)
	}
	return finalOrdered, nil
}

// Resolve 快速返回首选目标渠道与适配器
func (d *Dispatcher) Resolve(modelName string) (*config.UpstreamChannel, adapter.Adapter, error) {
	candidates, err := d.ResolveCandidates(modelName)
	if err != nil {
		return nil, nil, err
	}
	ch := candidates[0]
	adp := adapter.Get(ch.Type)
	if adp == nil {
		return nil, nil, fmt.Errorf("adapter not found for channel %s (type: %s)", ch.ID, ch.Type)
	}
	return ch, adp, nil
}

// ExecuteWithFailover 带有故障转移、熔断状态反馈的高可用执行引擎
func (d *Dispatcher) ExecuteWithFailover(
	ctx context.Context,
	modelName string,
	canRetry func() bool,
	fn func(channel *config.UpstreamChannel, adp adapter.Adapter) error,
) error {
	return d.ExecuteWithPolicy(ctx, modelName, "", RetrySafeRead, canRetry, fn)
}

func (d *Dispatcher) ExecuteWithPolicy(
	ctx context.Context,
	modelName string,
	protocol string,
	policy RetryPolicy,
	canRetry func() bool,
	fn func(channel *config.UpstreamChannel, adp adapter.Adapter) error,
) error {
	candidates, err := d.ResolveCandidatesForProtocol(modelName, protocol)
	if err != nil {
		return err
	}

	var lastErr error
	auditEntry := audit.FromContext(ctx)
	if auditEntry != nil {
		ids := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate != nil {
				ids = append(ids, candidate.ID)
			}
		}
		auditEntry.RecordCandidates(ids)
	}

	for i, ch := range candidates {
		if !d.tryAcquireChannel(ch.ID) {
			lastErr = fmt.Errorf("upstream channel %s is cooling down or already has a half-open probe", ch.ID)
			continue
		}
		defer d.releaseHalfOpenProbe(ch.ID)
		adp := adapter.Get(ch.Type)
		if adp == nil {
			lastErr = fmt.Errorf("adapter not found for channel %s (type: %s)", ch.ID, ch.Type)
			continue
		}

		targetModel := modelName
		if ch.ModelMap != nil {
			if mapped, ok := ch.ModelMap[modelName]; ok && mapped != "" {
				targetModel = mapped
			}
		}
		if auditEntry != nil {
			auditEntry.RecordDispatch(ch.ID, ch.Type, ch.BaseURL, targetModel)
		}

		err := fn(ch, adp)
		if err == nil {
			d.recordSuccess(ch.ID)
			return nil
		}

		// 若错误是由于客户端主动取消或客户端超时引起，绝不误判为上游节点故障，且不再重试后续渠道
		if ctx.Err() != nil {
			return err
		}
		// A syntactically valid request rejected by the provider (400/422)
		// should be returned to the client instead of being replayed against
		// every channel.  Capability and availability errors remain eligible
		// for failover (404/405, 408, 429 and 5xx).
		if !isFailoverEligibleForPolicy(err, policy) {
			return err
		}

		lastErr = err
		d.recordFailure(ch.ID)

		if auditEntry != nil && i < len(candidates)-1 {
			auditEntry.RecordFailover(fmt.Sprintf("Channel [%s] (%s) failed: %v, trying next [%s]", ch.ID, ch.Type, err, candidates[i+1].ID))
		}

		// 如果已不可重试（如流式数据已开始推向客户端），立即返回
		if canRetry != nil && !canRetry() {
			return err
		}

		if i < len(candidates)-1 {
			log.Printf("[Failover] Channel [%s] (%s, P%d) failed: %v. Retrying next candidate [%s] (P%d)...",
				ch.ID, ch.Type, ch.Priority, err, candidates[i+1].ID, candidates[i+1].Priority)
		}
	}
	return lastErr
}

func isFailoverEligibleForPolicy(err error, policy RetryPolicy) bool {
	// 流式响应 Header 已下发后中断，ResponseWriter 不可复用，绝不重试
	var streamAborted *adapter.ErrStreamAborted
	if errors.As(err, &streamAborted) {
		return false
	}

	var upstreamErr *adapter.UpstreamHTTPError
	if !errors.As(err, &upstreamErr) {
		return policy == RetrySafeRead
	}
	status := upstreamErr.StatusCode
	switch {
	case status == http.StatusBadRequest, status == http.StatusUnprocessableEntity:
		return false
	case status >= 400 && status < 500:
		return status == http.StatusUnauthorized ||
			status == http.StatusForbidden ||
			status == http.StatusNotFound ||
			status == http.StatusMethodNotAllowed ||
			status == http.StatusTooManyRequests
	default:
		return policy == RetrySafeRead
	}
}

// ListModels 聚合所有活跃渠道的静态模型与动态拉取模型
func (d *Dispatcher) ListModels() *model.ModelListResponse {
	seen := make(map[string]bool)
	items := make([]model.ModelItem, 0)

	addModel := func(id, owner string) {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" || trimmed == "*" || strings.ContainsAny(trimmed, "*?") {
			return
		}
		lower := strings.ToLower(trimmed)
		if seen[lower] {
			return
		}
		seen[lower] = true
		items = append(items, model.ModelItem{
			ID:      trimmed,
			Object:  "model",
			Created: 1700000000,
			OwnedBy: owner,
		})
	}

	channels := d.getActiveChannels()
	for _, ch := range channels {
		for _, m := range ch.Models {
			addModel(m, ch.ID)
		}
		for from := range ch.ModelMap {
			addModel(from, ch.ID)
		}
	}

	d.modelsMu.RLock()
	for _, ch := range channels {
		if remotes, ok := d.remoteModels[ch.ID]; ok {
			for _, m := range remotes {
				addModel(m, ch.ID)
			}
		}
	}
	d.modelsMu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].ID < items[j].ID
	})

	return &model.ModelListResponse{
		Object: "list",
		Data:   items,
	}
}
