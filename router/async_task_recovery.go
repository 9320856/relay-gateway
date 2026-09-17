package router

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/audit"
	"relay-gateway/db"
)

const (
	asyncTaskMappingRecoveryJournalSuffix = ".async-task-recovery.jsonl"
	asyncTaskMappingRecoveryInterval      = 15 * time.Second
	asyncTaskMappingWriteTimeout          = 250 * time.Millisecond
	asyncTaskMappingStatusMaxBytes        = 64
)

// asyncTaskMappingRegistration is deliberately small and contains no request
// body, credential, or provider URL. It is all that is needed to restore local
// routing after an upstream async task has already been accepted.
type asyncTaskMappingRegistration struct {
	ChannelID       string   `json:"channel_id"`
	TaskKind        string   `json:"task_kind"`
	TaskAlias       string   `json:"task_alias"`
	InitialStatus   string   `json:"initial_status,omitempty"`
	OriginRequestID string   `json:"origin_request_id,omitempty"`
	TaskIDs         []string `json:"task_ids"`
}

func newAsyncTaskMappingRegistration(ctx context.Context, channelID, taskKind, taskAlias, initialStatus string, taskIDs ...string) asyncTaskMappingRegistration {
	registration := asyncTaskMappingRegistration{
		ChannelID:       strings.TrimSpace(channelID),
		TaskKind:        strings.ToLower(strings.TrimSpace(taskKind)),
		TaskAlias:       strings.TrimSpace(taskAlias),
		InitialStatus:   truncateAsyncTaskMappingStatus(initialStatus),
		OriginRequestID: audit.RequestID(ctx),
		TaskIDs:         make([]string, 0, len(taskIDs)),
	}
	seen := make(map[string]struct{}, len(taskIDs))
	for _, taskID := range taskIDs {
		taskID = strings.TrimSpace(taskID)
		if !db.IsValidTaskMappingLookupID(taskID, registration.TaskKind) {
			continue
		}
		if _, ok := seen[taskID]; ok {
			continue
		}
		seen[taskID] = struct{}{}
		registration.TaskIDs = append(registration.TaskIDs, taskID)
	}
	if !db.IsValidTaskID(registration.TaskAlias) && len(registration.TaskIDs) > 0 {
		registration.TaskAlias = registration.TaskIDs[0]
	}
	return registration
}

func truncateAsyncTaskMappingStatus(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > asyncTaskMappingStatusMaxBytes {
		return value[:asyncTaskMappingStatusMaxBytes]
	}
	return value
}

func (r asyncTaskMappingRegistration) empty() bool {
	return strings.TrimSpace(r.ChannelID) == "" || strings.TrimSpace(r.TaskKind) == "" || len(r.TaskIDs) == 0
}

func (r asyncTaskMappingRegistration) valid() bool {
	if r.empty() || (r.TaskKind != asyncTaskKindVideo && r.TaskKind != asyncTaskKindImage) || !db.IsValidTaskID(r.TaskAlias) {
		return false
	}
	for _, taskID := range r.TaskIDs {
		if !db.IsValidTaskMappingLookupID(taskID, r.TaskKind) {
			return false
		}
	}
	return true
}

func (r asyncTaskMappingRegistration) mappings() []db.TaskMapping {
	if !r.valid() {
		return nil
	}
	mappings := make([]db.TaskMapping, 0, len(r.TaskIDs))
	for _, taskID := range r.TaskIDs {
		mappings = append(mappings, db.TaskMapping{
			TaskID:          taskID,
			ChannelID:       r.ChannelID,
			OriginRequestID: r.OriginRequestID,
			TaskKind:        r.TaskKind,
			TaskAlias:       r.TaskAlias,
		})
	}
	return mappings
}

func (r asyncTaskMappingRegistration) recoveryKey() string {
	encoded, err := json.Marshal(r)
	if err != nil {
		encoded = []byte(strings.Join(append([]string{r.ChannelID, r.TaskKind, r.TaskAlias, r.OriginRequestID}, r.TaskIDs...), "\x00"))
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// persistAsyncTaskMappingRegistration makes one short, bounded local attempt.
// A busy SQLite database must not delay a successful upstream create long
// enough for the client to retry it; the durable journal handles later retries.
// This function only writes task mappings and never contacts an upstream
// provider, so recovery cannot create a duplicate video or image.
func persistAsyncTaskMappingRegistration(registration asyncTaskMappingRegistration) error {
	if registration.empty() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), asyncTaskMappingWriteTimeout)
	defer cancel()
	return db.EnsureTaskMappingsContext(ctx, registration.mappings()...)
}

func deferAsyncTaskMappingPersistence(c *gin.Context, registration asyncTaskMappingRegistration, persistErr error) {
	if c == nil || registration.empty() {
		return
	}
	if errors.Is(persistErr, db.ErrTaskMappingChannelConflict) {
		// Preserve the original successful create response, but never place a
		// conflicting mapping in the cache where it could override the durable
		// channel pin. An operator can investigate this anomalous provider ID.
		c.Header("X-Relay-Task-Mapping", "conflict")
		log.Printf("[ASYNC_TASK] accepted %s task %s, but refused cross-channel mapping conflict: %v", registration.TaskKind, registration.TaskAlias, persistErr)
		return
	}

	journaled, err := enqueueAsyncTaskMappingRecoveryFn(registration)
	if journaled && err == nil {
		c.Header("X-Relay-Task-Mapping", "pending")
		// The cache also authorizes unauthenticated public video content. Only
		// publish it after the recovery record is fsync'd, so a process crash
		// cannot leave an authorization-only phantom mapping behind.
		for _, mapping := range registration.mappings() {
			db.PublishTaskMappingCache(mapping)
		}
		log.Printf("[ASYNC_TASK] accepted %s task %s, but local routing persistence failed: %v; recovery journal is durable", registration.TaskKind, registration.TaskAlias, persistErr)
	} else {
		c.Header("X-Relay-Task-Mapping", "failed")
		log.Printf("[ASYNC_TASK] accepted %s task %s, but local routing persistence failed: %v; recovery journal enqueue also failed: %v", registration.TaskKind, registration.TaskAlias, persistErr, err)
	}
}

type asyncTaskMappingJournalEntry struct {
	Operation    string                        `json:"op"`
	Key          string                        `json:"key"`
	Registration *asyncTaskMappingRegistration `json:"registration,omitempty"`
}

type asyncTaskMappingRecoveryQueue struct {
	mu          sync.Mutex
	loaded      bool
	journalPath string
	pending     map[string]asyncTaskMappingRecoveryItem
}

type asyncTaskMappingRecoveryItem struct {
	registration asyncTaskMappingRegistration
	journaled    bool
}

var asyncTaskMappingRecoveries asyncTaskMappingRecoveryQueue

func asyncTaskMappingRecoveryJournalPath() string {
	path := strings.TrimSpace(db.DatabasePath())
	if path == "" {
		return ""
	}
	return filepath.Clean(path) + asyncTaskMappingRecoveryJournalSuffix
}

func (q *asyncTaskMappingRecoveryQueue) resetForPathLocked(path string) {
	if q.journalPath != path {
		q.loaded = false
		q.journalPath = path
		q.pending = make(map[string]asyncTaskMappingRecoveryItem)
	}
	if q.pending == nil {
		q.pending = make(map[string]asyncTaskMappingRecoveryItem)
	}
}

func (q *asyncTaskMappingRecoveryQueue) loadLocked() (string, error) {
	path := asyncTaskMappingRecoveryJournalPath()
	q.resetForPathLocked(path)
	if q.loaded {
		return path, nil
	}
	if path == "" {
		return path, errors.New("database path is not initialized")
	}

	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		q.loaded = true
		return path, nil
	}
	if err != nil {
		return path, fmt.Errorf("open async-task recovery journal: %w", err)
	}
	defer file.Close()

	loaded := make(map[string]asyncTaskMappingRecoveryItem)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	for scanner.Scan() {
		var entry asyncTaskMappingJournalEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			// An interrupted final append must not prevent earlier complete
			// entries from recovering. Keep the raw journal for inspection.
			log.Printf("[ASYNC_TASK] ignoring malformed recovery journal entry: %v", err)
			continue
		}
		switch entry.Operation {
		case "upsert":
			if entry.Registration == nil || !entry.Registration.valid() {
				continue
			}
			key := strings.TrimSpace(entry.Key)
			if key == "" {
				key = entry.Registration.recoveryKey()
			}
			loaded[key] = asyncTaskMappingRecoveryItem{registration: *entry.Registration, journaled: true}
		case "ack":
			delete(loaded, strings.TrimSpace(entry.Key))
		}
	}
	if err := scanner.Err(); err != nil {
		return path, fmt.Errorf("read async-task recovery journal: %w", err)
	}
	for key, item := range loaded {
		if pending, exists := q.pending[key]; exists {
			pending.journaled = true
			q.pending[key] = pending
			continue
		}
		q.pending[key] = item
	}
	q.loaded = true
	return path, nil
}

func appendAsyncTaskMappingJournal(path string, entry asyncTaskMappingJournalEntry) error {
	if path == "" {
		return errors.New("database path is not initialized")
	}
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("create async-task recovery journal directory: %w", err)
		}
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode async-task recovery journal entry: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open async-task recovery journal for append: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("append async-task recovery journal: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync async-task recovery journal: %w", err)
	}
	return nil
}

// Enqueue makes a failed mapping write recoverable both in the current process
// and after a restart. Its bool reports whether this exact entry was fsync'd.
// Callers may only cache a route (which authorizes public video content) after
// that bool is true.
func (q *asyncTaskMappingRecoveryQueue) Enqueue(registration asyncTaskMappingRegistration) (bool, error) {
	if !registration.valid() {
		return false, errors.New("invalid async task mapping registration")
	}
	key := registration.recoveryKey()
	q.mu.Lock()
	defer q.mu.Unlock()
	path, loadErr := q.loadLocked()
	item, exists := q.pending[key]
	if !exists {
		item = asyncTaskMappingRecoveryItem{registration: registration}
		q.pending[key] = item
	}
	if item.journaled {
		return true, loadErr
	}
	if appendErr := appendAsyncTaskMappingJournal(path, asyncTaskMappingJournalEntry{Operation: "upsert", Key: key, Registration: &registration}); appendErr != nil {
		if loadErr != nil {
			return false, errors.Join(loadErr, appendErr)
		}
		return false, appendErr
	}
	if loadErr != nil {
		// A newly appended line is not enough when an older unreadable line
		// prevents startup replay from reaching it. Keep retrying the journal
		// and do not authorize the cache until the whole log is readable.
		return false, loadErr
	}
	item.journaled = true
	q.pending[key] = item
	return true, nil
}

func (q *asyncTaskMappingRecoveryQueue) ensureJournaled(key string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	path, loadErr := q.loadLocked()
	item, exists := q.pending[key]
	if !exists {
		return false, loadErr
	}
	if item.journaled {
		return true, loadErr
	}
	if err := appendAsyncTaskMappingJournal(path, asyncTaskMappingJournalEntry{Operation: "upsert", Key: key, Registration: &item.registration}); err != nil {
		if loadErr != nil {
			return false, errors.Join(loadErr, err)
		}
		return false, err
	}
	if loadErr != nil {
		return false, loadErr
	}
	item.journaled = true
	q.pending[key] = item
	return true, nil
}

func (q *asyncTaskMappingRecoveryQueue) acknowledge(key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	path, loadErr := q.loadLocked()
	if loadErr != nil {
		return loadErr
	}
	item, exists := q.pending[key]
	if !exists {
		return nil
	}
	if !item.journaled {
		return errors.New("cannot acknowledge a mapping that is not journaled")
	}
	if err := appendAsyncTaskMappingJournal(path, asyncTaskMappingJournalEntry{Operation: "ack", Key: key}); err != nil {
		return err
	}
	delete(q.pending, key)
	if len(q.pending) == 0 {
		// Every prior record now has an fsync'd acknowledgement, so truncating
		// the append-only journal cannot lose a task that still needs recovery.
		// Keep it bounded across many transient SQLite failures.
		if err := truncateAsyncTaskMappingJournal(path); err != nil {
			log.Printf("[ASYNC_TASK] recovered mapping journal acknowledgement is durable, but journal compaction failed: %v", err)
		}
	}
	return nil
}

func truncateAsyncTaskMappingJournal(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

// ReconcilePendingAsyncTaskMappings replays only local task-mapping writes.
// It intentionally contains no adapter calls, which is the key guarantee that
// recovery cannot create a second upstream task.
func ReconcilePendingAsyncTaskMappings(ctx context.Context) error {
	return asyncTaskMappingRecoveries.reconcile(ctx)
}

func (q *asyncTaskMappingRecoveryQueue) reconcile(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	q.mu.Lock()
	if _, err := q.loadLocked(); err != nil {
		q.mu.Unlock()
		return err
	}
	keys := make([]string, 0, len(q.pending))
	for key := range q.pending {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pending := make(map[string]asyncTaskMappingRecoveryItem, len(keys))
	for _, key := range keys {
		pending[key] = q.pending[key]
	}
	q.mu.Unlock()

	var recoveryErrs []error
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			recoveryErrs = append(recoveryErrs, err)
			break
		}
		item := pending[key]
		if !item.journaled {
			journaled, err := q.ensureJournaled(key)
			if err != nil || !journaled {
				if err == nil {
					err = errors.New("mapping recovery journal is not durable yet")
				}
				recoveryErrs = append(recoveryErrs, fmt.Errorf("journal %s task %s: %w", item.registration.TaskKind, item.registration.TaskAlias, err))
				continue
			}
			item.journaled = true
			for _, mapping := range item.registration.mappings() {
				db.PublishTaskMappingCache(mapping)
			}
		}
		if err := persistAsyncTaskMappingRegistration(item.registration); err != nil {
			recoveryErrs = append(recoveryErrs, fmt.Errorf("recover %s task %s: %w", item.registration.TaskKind, item.registration.TaskAlias, err))
			continue
		}
		if err := q.acknowledge(key); err != nil {
			recoveryErrs = append(recoveryErrs, fmt.Errorf("acknowledge recovered %s task %s: %w", item.registration.TaskKind, item.registration.TaskAlias, err))
		}
	}
	return errors.Join(recoveryErrs...)
}

// StartAsyncTaskMappingRecoveryWorker reconciles any journaled mappings at
// startup and periodically while the process is healthy again. main waits for
// its done channel before closing SQLite during shutdown.
func StartAsyncTaskMappingRecoveryWorker(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		var lastOrphanReconcile time.Time
		run := func() {
			if err := ReconcilePendingAsyncTaskMappings(ctx); err != nil {
				log.Printf("[ASYNC_TASK] pending mapping recovery will retry: %v", err)
			}
			// Periodically reconcile orphaned poll logs that escaped coalescing
			// (e.g. from a previous binary version or a race condition).
			if time.Since(lastOrphanReconcile) >= 5*time.Minute {
				if n, err := ReconcileOrphanedAsyncPollLogs(ctx); err == nil && n > 0 {
					log.Printf("[ASYNC_TASK] reconciled %d orphaned poll logs", n)
				}
				lastOrphanReconcile = time.Now()
			}
		}
		run()
		ticker := time.NewTicker(asyncTaskMappingRecoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
	return done
}

func generateGatewayTaskID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("gt_%x%x", time.Now().UnixNano(), time.Now().Unix())
	}
	return "gt_" + hex.EncodeToString(b)
}

func isTaskIDRegisteredToOtherChannel(taskID, taskKind, channelID string) bool {
	taskID = strings.TrimSpace(taskID)
	channelID = strings.TrimSpace(channelID)
	if taskID == "" || channelID == "" {
		return false
	}
	if mapping := db.GetTaskMappingForKind(taskID, taskKind); mapping != nil {
		if mapping.ChannelID != "" && mapping.ChannelID != channelID {
			return true
		}
	}
	if run, err := db.GetTaskRunByAlias(taskID); err == nil && run != nil {
		if run.ChannelID != "" && run.ChannelID != channelID {
			return true
		}
	}
	return false
}

func disambiguateAsyncTaskIDs(channelID, taskKind string, taskAlias string, taskIDs []string) (string, bool) {
	channelID = strings.TrimSpace(channelID)
	taskAlias = strings.TrimSpace(taskAlias)
	if channelID == "" {
		return "", false
	}
	if taskAlias != "" && isTaskIDRegisteredToOtherChannel(taskAlias, taskKind, channelID) {
		return generateGatewayTaskID(), true
	}
	for _, id := range taskIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed != "" && isTaskIDRegisteredToOtherChannel(trimmed, taskKind, channelID) {
			return generateGatewayTaskID(), true
		}
	}
	return "", false
}

func resolveCanonicalProviderTaskID(taskID, taskKind string) string {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return ""
	}
	cleanID := strings.TrimPrefix(taskID, imageTaskIDPrefix)
	if strings.HasPrefix(cleanID, "gt_") {
		if mapping := db.GetTaskMappingForKind(taskID, taskKind); mapping != nil && strings.TrimSpace(mapping.TaskAlias) != "" {
			return strings.TrimSpace(mapping.TaskAlias)
		}
		if run, err := db.GetTaskRunByAlias(taskID); err == nil && run != nil && strings.TrimSpace(run.ProviderTaskID) != "" {
			return strings.TrimSpace(run.ProviderTaskID)
		}
	}
	return db.GetCanonicalTaskIDForKind(taskID, taskKind)
}

// Image mappings use a namespace that must never be sent to the provider.
// Keep the public API and Playground on the same gateway-ID translation path.
func resolveImageProviderTaskID(taskID string) string {
	if !strings.HasPrefix(taskID, "gt_") {
		return taskID
	}
	providerTaskID := resolveCanonicalProviderTaskID(imageTaskIDPrefix+taskID, asyncTaskKindImage)
	if providerTaskID == imageTaskIDPrefix+taskID || providerTaskID == "" {
		return resolveCanonicalProviderTaskID(taskID, asyncTaskKindImage)
	}
	return providerTaskID
}

func setImageJobResponseIDs(resp interface{}, newID string) {
	if newID == "" || resp == nil {
		return
	}
	response, ok := resp.(map[string]interface{})
	if !ok {
		return
	}
	for _, key := range []string{"id", "task_id"} {
		if _, exists := response[key]; exists {
			response[key] = newID
		}
	}
	if nested, ok := response["job"].(map[string]interface{}); ok {
		for _, key := range []string{"id", "task_id"} {
			if _, exists := nested[key]; exists {
				nested[key] = newID
			}
		}
	}
	if nested, ok := response["data"].(map[string]interface{}); ok {
		for _, key := range []string{"id", "task_id"} {
			if _, exists := nested[key]; exists {
				nested[key] = newID
			}
		}
	}
}
