package geecache

import (
	"geecache/algo"
	"sync"
	"time"
)

type cache struct {
	mu         sync.RWMutex
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
	c.ensureStore().Add(key, entry)
}

func (c *cache) get(key string) (entry cacheEntry, ok bool) {
	store := c.getStore()
	if store == nil {
		return
	}

	if v, ok := store.Get(key); ok {
		entry = v.(cacheEntry)
		if entry.expired(time.Now()) {
			store.Remove(key)
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

func (c *cache) getStore() *algo.Cache {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store
}

func (c *cache) ensureStore() *algo.Cache {
	if store := c.getStore(); store != nil {
		return store
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store == nil {
		c.store = algo.New(c.cacheBytes, c.evictor(), nil)
	}
	return c.store
}
