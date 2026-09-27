package db

import (
	"container/list"
	"sync"
	"time"
)

// taskMappingCache bounds process memory independently of durable task history.
// Entries are ordered by publication time so capacity and TTL eviction are O(1)
// per removed entry. Eviction only causes a subsequent SQLite reload.
type taskMappingCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List
	max     int
	ttl     time.Duration
	stop    chan struct{}
	done    chan struct{}
}

func newTaskMappingCache(max int, ttl time.Duration) *taskMappingCache {
	return &taskMappingCache{entries: make(map[string]*list.Element), order: list.New(), max: max, ttl: ttl}
}

func (c *taskMappingCache) remove(element *list.Element) {
	delete(c.entries, element.Value.(videoTaskCacheEntry).mapping.TaskID)
	c.order.Remove(element)
}

func (c *taskMappingCache) purgeLocked(now time.Time) {
	for element := c.order.Front(); element != nil; element = c.order.Front() {
		entry := element.Value.(videoTaskCacheEntry)
		if now.Sub(entry.cachedAt) < c.ttl {
			return
		}
		c.remove(element)
	}
}

func (c *taskMappingCache) purge(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeLocked(now)
}

func (c *taskMappingCache) publish(mapping TaskMapping, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeLocked(now)
	entry := videoTaskCacheEntry{mapping: mapping, cachedAt: now}
	if element := c.entries[mapping.TaskID]; element != nil {
		element.Value = entry
		c.order.MoveToBack(element)
		return
	}
	if c.max <= 0 {
		return
	}
	for len(c.entries) >= c.max {
		c.remove(c.order.Front())
	}
	c.entries[mapping.TaskID] = c.order.PushBack(entry)
}

func (c *taskMappingCache) get(id string, now time.Time) *TaskMapping {
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.entries[id]
	if element == nil {
		return nil
	}
	entry := element.Value.(videoTaskCacheEntry)
	if now.Sub(entry.cachedAt) >= c.ttl {
		c.remove(element)
		return nil
	}
	mapping := entry.mapping
	return &mapping
}

func (c *taskMappingCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.entries)
	c.order.Init()
}

func (c *taskMappingCache) invalidateChannel(channelID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, element := range c.entries {
		if element.Value.(videoTaskCacheEntry).mapping.ChannelID == channelID {
			c.remove(element)
		}
	}
}

// The cleanup worker owns no database connection and is stopped during Close.
func (c *taskMappingCache) startCleanup(interval time.Duration) {
	c.stopCleanup()
	c.mu.Lock()
	stop, done := make(chan struct{}), make(chan struct{})
	c.stop, c.done = stop, done
	c.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				c.purge(now)
			}
		}
	}()
}

func (c *taskMappingCache) stopCleanup() {
	c.mu.Lock()
	stop, done := c.stop, c.done
	c.stop, c.done = nil, nil
	if stop != nil {
		close(stop)
	}
	c.mu.Unlock()
	if done != nil {
		<-done
	}
}
