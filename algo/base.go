package algo

import "sync"

// Evictor controls which key should be evicted next.
// Cache owns the actual values; the evictor only tracks access patterns.
// Evict returns a victim key but must not mutate internal state.
type Evictor interface {
	OnAdd(key string)
	OnAccess(key string)
	OnRemove(key string)
	OnEvict(key string)
	Evict() string
}

type Cache struct {
	mu        sync.RWMutex
	maxBytes  int64
	nbytes    int64
	cache     map[string]Value
	evictor   Evictor
	onEvicted func(key string, value Value)
}

// Value uses Len to count how many bytes it takes.
type Value interface {
	Len() int
}

func New(maxBytes int64, evictor Evictor, onEvicted func(key string, value Value)) *Cache {
	if evictor == nil {
		evictor = NewLRU()
	}
	return &Cache{
		maxBytes:  maxBytes,
		cache:     make(map[string]Value),
		evictor:   evictor,
		onEvicted: onEvicted,
	}
}

func (c *Cache) Add(key string, value Value) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.cache[key]; ok {
		c.nbytes += int64(value.Len()) - int64(existing.Len())
		c.cache[key] = value
		c.evictor.OnAccess(key)
	} else {
		c.cache[key] = value
		c.nbytes += int64(len(key)) + int64(value.Len())
		c.evictor.OnAdd(key)
	}
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.removeOldest()
	}
}

func (c *Cache) Get(key string) (value Value, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if value, ok = c.cache[key]; ok {
		c.evictor.OnAccess(key)
		return value, true
	}
	return nil, false
}

func (c *Cache) GetOrRemoveIf(key string, predicate func(Value) bool) (value Value, ok bool, removed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	value, ok = c.cache[key]
	if !ok {
		return nil, false, false
	}
	if predicate != nil && predicate(value) {
		c.removeKey(key)
		return nil, false, true
	}
	c.evictor.OnAccess(key)
	return value, true, false
}

func (c *Cache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeKey(key)
}

func (c *Cache) RemoveIf(key string, predicate func(Value) bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	value, ok := c.cache[key]
	if !ok {
		return false
	}
	if predicate != nil && !predicate(value) {
		return false
	}
	c.removeKey(key)
	return true
}

func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

func (c *Cache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.cache))
	for key := range c.cache {
		keys = append(keys, key)
	}
	return keys
}

func (c *Cache) removeOldest() {
	key := c.evictor.Evict()
	if key == "" {
		return
	}
	c.evictKey(key)
}

func (c *Cache) removeKey(key string) {
	value, ok := c.cache[key]
	if !ok {
		return
	}
	delete(c.cache, key)
	c.evictor.OnRemove(key)
	c.nbytes -= int64(len(key)) + int64(value.Len())
	if c.onEvicted != nil {
		c.onEvicted(key, value)
	}
}

func (c *Cache) evictKey(key string) {
	value, ok := c.cache[key]
	if !ok {
		return
	}
	delete(c.cache, key)
	c.evictor.OnEvict(key)
	c.nbytes -= int64(len(key)) + int64(value.Len())
	if c.onEvicted != nil {
		c.onEvicted(key, value)
	}
}
