package geecache

import (
	"geecache/algo"
	"sync"
	"time"
)

type cache struct {
	mu         sync.Mutex
	store      *algo.Cache
	cacheBytes int64
	onExpire   func()
	newEvictor func() algo.Evictor
}

type cacheEntry struct {
	value     ByteView
	expiresAt time.Time
	negative  bool
}

func (e cacheEntry) Len() int {
	return e.value.Len()
}

func (e cacheEntry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

func (c *cache) add(key string, entry cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store == nil {
		c.store = algo.New(c.cacheBytes, c.evictor(), nil)
	}
	c.store.Add(key, entry)
}

func (c *cache) get(key string) (entry cacheEntry, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store == nil {
		return
	}

	if v, ok := c.store.Get(key); ok {
		entry = v.(cacheEntry)
		if entry.expired(time.Now()) {
			c.store.Remove(key)
			if c.onExpire != nil {
				c.onExpire()
			}
			return cacheEntry{}, false
		}
		return entry, ok
	}

	return
}

func (c *cache) evictor() algo.Evictor {
	if c.newEvictor != nil {
		return c.newEvictor()
	}
	return algo.NewLRU()
}
