package algo

// Evictor controls which key should be evicted next.
// Cache owns the actual values; the evictor only tracks access patterns.
type Evictor interface {
	OnAdd(key string)
	OnAccess(key string)
	OnRemove(key string)
	Evict() string
}

type Cache struct {
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
	if value, ok = c.cache[key]; ok {
		c.evictor.OnAccess(key)
		return value, true
	}
	return nil, false
}

func (c *Cache) Remove(key string) {
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

func (c *Cache) Len() int {
	return len(c.cache)
}

func (c *Cache) removeOldest() {
	key := c.evictor.Evict()
	if key == "" {
		return
	}
	value, ok := c.cache[key]
	if !ok {
		return
	}
	delete(c.cache, key)
	c.nbytes -= int64(len(key)) + int64(value.Len())
	if c.onEvicted != nil {
		c.onEvicted(key, value)
	}
}
