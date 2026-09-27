package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestContextPersistenceCancelsWhileConnectionIsOccupied(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "context.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	tx := DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	checks := []struct {
		name string
		call func(context.Context) error
	}{
		{"task read", func(ctx context.Context) error { _, err := GetTaskRunContext(ctx, "missing"); return err }},
		{"task claim", func(ctx context.Context) error {
			_, err := ClaimDueTaskRunContext(ctx, "test", time.Minute)
			return err
		}},
		{"profile read", func(ctx context.Context) error { _, err := GetProtocolProfileContext(ctx, "missing"); return err }},
		{"setting write", func(ctx context.Context) error { return SetSettingContext(ctx, "cancelled-setting", "value") }},
		{"channel read", func(ctx context.Context) error { _, err := GetChannelModelContext(ctx, "missing"); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- check.call(ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected deadline while waiting for the connection, got %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("SQL ignored the context deadline")
			}
		})
	}
	if err := SetSettingContext(WithTx(context.Background(), tx), "inside-tx", "value"); err != nil {
		t.Fatal(err)
	}
	if !HasContextTransaction(WithTx(context.Background(), tx)) || HasContextTransaction(context.Background()) {
		t.Fatal("transaction ownership was lost when binding SQL context")
	}
}

func TestContextTransactionDoesNotPublishRolledBackMapping(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "rollback-context.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	tx := DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	ctx := WithTx(context.Background(), tx)
	if err := RecordTaskMappingContext(ctx, TaskMapping{TaskID: "rolled-back", ChannelID: "channel", TaskKind: "video"}); err != nil {
		t.Fatal(err)
	}
	if cached := getCachedTaskMapping("rolled-back"); cached != nil {
		t.Fatalf("mapping became globally visible before transaction commit: %+v", cached)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if mapping := GetTaskMapping("rolled-back"); mapping != nil {
		t.Fatalf("rolled-back task mapping survived in durable routing: %+v", mapping)
	}
}

func TestTaskMappingCacheConcurrentUpdatesPreserveCapacityAndList(t *testing.T) {
	c := newTaskMappingCache(32, time.Hour)
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for attempt := 0; attempt < 64; attempt++ {
				id := fmt.Sprintf("lookup-%d", (worker*64+attempt)%80)
				c.publish(TaskMapping{TaskID: id, ChannelID: fmt.Sprint(worker % 4)}, time.Now())
				if mapping := c.get(id, time.Now()); mapping != nil {
					mapping.ChannelID = "caller-local-change"
					if cached := c.get(id, time.Now()); cached != nil && cached.ChannelID == mapping.ChannelID {
						t.Error("caller mutated a shared cache entry")
					}
				}
				if attempt%8 == 0 {
					c.invalidateChannel(fmt.Sprint((worker + 1) % 4))
				}
			}
		}(worker)
	}
	workers.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) > c.max || c.order.Len() != len(c.entries) {
		t.Fatalf("concurrent cache update violated bounds: entries=%d list=%d max=%d", len(c.entries), c.order.Len(), c.max)
	}
	for element := c.order.Front(); element != nil; element = element.Next() {
		entry := element.Value.(videoTaskCacheEntry)
		if c.entries[entry.mapping.TaskID] != element {
			t.Fatalf("list and lookup index disagree for %q", entry.mapping.TaskID)
		}
	}
}

func TestTaskAliasLookupIndexMigratesExistingSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alias-index.db")
	if err := InitDB(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := DB.Exec("DROP INDEX idx_task_alias_lookup_id").Error; err != nil {
		t.Fatal(err)
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(path); err != nil {
		t.Fatal(err)
	}
	var plan []struct{ Detail string }
	if err := DB.Raw("EXPLAIN QUERY PLAN SELECT * FROM async_task_aliases WHERE lookup_id = ? ORDER BY id LIMIT 1", "lookup").Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	if len(plan) == 0 || !strings.Contains(plan[0].Detail, "idx_task_alias_lookup_id") {
		t.Fatalf("lookup still scans the table: %+v", plan)
	}
}

func TestTaskMappingCacheBoundsAndExpiresUnvisitedEntries(t *testing.T) {
	c := newTaskMappingCache(2, time.Hour)
	now := time.Now()
	c.publish(TaskMapping{TaskID: "a", ChannelID: "first"}, now)
	c.publish(TaskMapping{TaskID: "b", ChannelID: "second"}, now.Add(time.Minute))
	c.publish(TaskMapping{TaskID: "c", ChannelID: "second"}, now.Add(2*time.Minute))
	if len(c.entries) != 2 || c.get("a", now) != nil {
		t.Fatal("cache did not evict the oldest entry at capacity")
	}
	c.purge(now.Add(2 * time.Hour))
	if len(c.entries) != 0 {
		t.Fatal("unvisited entries survived proactive expiration")
	}
	c.publish(TaskMapping{TaskID: "b", ChannelID: "second"}, now)
	c.invalidateChannel("second")
	if len(c.entries) != 0 {
		t.Fatal("channel invalidation retained cached mappings")
	}
}

func TestTaskMappingCacheWorkerExpiresAndStops(t *testing.T) {
	c := newTaskMappingCache(2, time.Millisecond)
	c.publish(TaskMapping{TaskID: "unvisited", ChannelID: "channel"}, time.Now().Add(-time.Hour))
	c.startCleanup(time.Millisecond)
	defer c.stopCleanup()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("cleanup worker did not expire unvisited entries")
		case <-ticker.C:
			c.mu.Lock()
			empty := len(c.entries) == 0
			c.mu.Unlock()
			if empty {
				c.stopCleanup()
				return
			}
		}
	}
}

func TestTaskMappingCacheEvictionReloadsDurableMapping(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "eviction.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := RecordTaskMapping(TaskMapping{TaskID: "durable", ChannelID: "original", TaskKind: "video"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < videoTaskCache.max; i++ {
		PublishTaskMappingCache(TaskMapping{TaskID: fmt.Sprintf("cache-only-%d", i), ChannelID: "other"})
	}
	if getCachedTaskMapping("durable") != nil {
		t.Fatal("oldest mapping was not evicted")
	}
	if mapping := GetTaskMapping("durable"); mapping == nil || mapping.ChannelID != "original" {
		t.Fatalf("durable routing was lost after cache eviction: %+v", mapping)
	}
}
