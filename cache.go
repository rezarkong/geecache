package geecache

import (
	"geecache/algo"
	"hash/fnv"
	"sync"
	"time"
)

// cache.go 缓存包装层 负责 cache 的业务缓存包装
// algo 是下层可替换的底层淘汰器和通用内存缓存容器

// 默认不切片
const defaultShardCount = 1

// 缓存包装
type cache struct {
	cacheBytes int64               // cache
	shardCount int                 // 分片的实现
	onExpire   func()              // 过期操作器
	newEvictor func() algo.Evictor // 淘汰选取器

	mu     sync.RWMutex
	shards []cacheShard
}

// Cache 分片
type cacheShard struct {
	store      *algo.Cache
	cacheBytes int64
}

// cache 封装
type cacheEntry struct {
	value     ByteView
	expiresAt time.Time
	negative  bool
}

func (e cacheEntry) Len() int {
	return e.value.Len()
}

// expired 判断是否过期
func (e cacheEntry) expired(now time.Time) bool {
	// TTL 非零且当前过了过期时间
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// return getShards
func (c *cache) getShards() []cacheShard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.shards
}

// ensureShards cache 的一个懒初始化入口 保证底层 shard 已经建好，return 给调用方 []shard
func (c *cache) ensureShards() []cacheShard {
	if shards := c.getShards(); len(shards) > 0 {
		return shards
	}
	// 没建分片就新建一下
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shards = make([]cacheShard, c.effectiveShardCount())
	c.initShards(c.shards)
	return c.shards
}

// shardIndexWithCount 返回 key 对应的 shard Index
func (c *cache) shardIndexWithCount(key string, count int) int {
	if count <= 1 {
		return 0
	}
	// key 进行哈希对其取模得到落到哪个 shard
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(count))
}

// shardIndex 返回 key 所属 shard 的 index
func (c *cache) shardIndex(key string) int {
	return c.shardIndexWithCount(key, c.effectiveShardCount())
}

// shardFor 判断 key 属于哪个分片，返回对应的地址
func (c *cache) shardFor(key string) *cacheShard {
	// 先确保 shards 存在，然后线程安全读
	shards := c.ensureShards()
	// 再对 key 找 key 落在对应哪个 shards 分片里面，返回对应的地址
	return &shards[c.shardIndexWithCount(key, len(shards))]
}

// add 存 key value
func (c *cache) add(key string, entry cacheEntry) {
	shard := c.shardFor(key)
	shard.store.Add(key, entry)
}

// get 根据 key 返回封装实例 cacheEntry, bool
func (c *cache) get(key string) (entry cacheEntry, ok bool) {
	shard := c.shardFor(key)
	now := time.Now()
	// 调用 algo 从 shard 的底层拿 key-value 并懒检查是否删除
	if v, ok, removed := shard.store.GetOrRemoveIf(key, func(v algo.Value) bool {
		return v.(cacheEntry).expired(now)
	}); ok {
		return v.(cacheEntry), true
	} else if removed {
		if c.onExpire != nil {
			c.onExpire()
		}
	}

	return
}

func (c *cache) compensateAccess(key string, n int) {
	if n <= 0 {
		return
	}
	shard := c.shardFor(key)
	now := time.Now()
	if _, removed := shard.store.CompensateBurstIf(key, n, func(v algo.Value) bool {
		return v.(cacheEntry).expired(now)
	}); removed && c.onExpire != nil {
		c.onExpire()
	}
}

func (c *cache) delete(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		shard := c.shardFor(key)
		shard.store.Remove(key)
	}
}

func (c *cache) clear() {
	for i := range c.ensureShards() {
		for _, key := range c.shards[i].store.Keys() {
			c.shards[i].store.Remove(key)
		}
	}
}

func (c *cache) cleanupExpired(now time.Time) int {
	expired := 0
	for i := range c.ensureShards() {
		expired += c.cleanupExpiredShard(&c.shards[i], now)
	}
	return expired
}

func (c *cache) evictor() algo.Evictor {
	if c.newEvictor != nil {
		return c.newEvictor()
	}
	return algo.NewLRU()
}

// effectiveShardCount 返回 shardCount
func (c *cache) effectiveShardCount() int {
	count := c.shardCount
	if count <= 0 {
		count = defaultShardCount
	}
	return count
}

// initShards 初始化 shards
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

func (c *cache) cleanupExpiredShard(shard *cacheShard, now time.Time) int {
	expired := 0
	for _, key := range shard.store.Keys() {
		if shard.store.RemoveIf(key, func(v algo.Value) bool {
			return v.(cacheEntry).expired(now)
		}) {
			expired++
		}
	}
	return expired
}
