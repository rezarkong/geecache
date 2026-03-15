package geecache

import (
	"geecache/algo"
	"hash/fnv"
	"sync"
	"time"
)

const defaultShardCount = 1

type cache struct {
	cacheBytes int64
	shardCount int
	onExpire   func()
	newEvictor func() algo.Evictor

	mu     sync.RWMutex
	shards []cacheShard
}

type cacheShard struct {
	store      *algo.Cache
	cacheBytes int64
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
	shard := c.shardFor(key)
	shard.store.Add(key, entry)
}

func (c *cache) get(key string) (entry cacheEntry, ok bool) {
	shard := c.shardFor(key)
	if v, ok := shard.store.Get(key); ok {
		entry = v.(cacheEntry)
		if entry.expired(time.Now()) {
			shard.store.Remove(key)
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

func (c *cache) shardFor(key string) *cacheShard {
	shards := c.ensureShards()
	return &shards[c.shardIndexWithCount(key, len(shards))]
}

func (c *cache) ensureShards() []cacheShard {
	if shards := c.getShards(); len(shards) > 0 {
		return shards
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.shards) == 0 {
		c.shards = make([]cacheShard, c.effectiveShardCount())
		c.initShards(c.shards)
	}
	return c.shards
}

func (c *cache) getShards() []cacheShard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.shards
}

func (c *cache) effectiveShardCount() int {
	count := c.shardCount
	if count <= 0 {
		count = defaultShardCount
	}
	if count < 1 {
		return 1
	}
	return count
}

func (c *cache) initShards(shards []cacheShard) {
	base := c.cacheBytes / int64(len(shards))
	rem := c.cacheBytes % int64(len(shards))
	for i := range shards {
		shards[i].cacheBytes = base
		if int64(i) < rem {
			shards[i].cacheBytes++
		}
		shards[i].store = algo.New(shards[i].cacheBytes, c.evictor(), nil)
	}
}

func (c *cache) shardIndex(key string) int {
	return c.shardIndexWithCount(key, c.effectiveShardCount())
}

func (c *cache) shardIndexWithCount(key string, count int) int {
	if count <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(count))
}
