package geecache

import "testing"

func TestCacheShardsRouteAndSplitCapacity(t *testing.T) {
	c := cache{
		cacheBytes: 256,
		shardCount: 4,
	}

	shards := c.ensureShards()
	if got := len(shards); got != 4 {
		t.Fatalf("expected 4 shards, got %d", got)
	}

	var total int64
	for _, shard := range shards {
		total += shard.cacheBytes
	}
	if total != 256 {
		t.Fatalf("expected total shard capacity 256, got %d", total)
	}

	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, key := range keys {
		c.add(key, cacheEntry{value: ByteView{b: []byte(key)}})
	}

	used := 0
	for i := range shards {
		if shards[i].store != nil && shards[i].store.Len() > 0 {
			used++
		}
	}
	if used < 2 {
		t.Fatalf("expected keys to spread across shards, used=%d", used)
	}

	for _, key := range keys {
		entry, ok := c.get(key)
		if !ok {
			t.Fatalf("expected key %q in shard cache", key)
		}
		if got := entry.value.String(); got != key {
			t.Fatalf("expected value %q, got %q", key, got)
		}
	}
}

func TestCacheShardCountDefaultsToSingleShard(t *testing.T) {
	c := cache{
		cacheBytes: 2,
	}

	if got := c.effectiveShardCount(); got != 1 {
		t.Fatalf("expected default shard count 1, got %d", got)
	}
}
