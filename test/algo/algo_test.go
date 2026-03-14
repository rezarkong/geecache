package algo_test

import (
	"geecache/algo"
	"reflect"
	"sync"
	"testing"
	"time"
)

type String string

func (d String) Len() int {
	return len(d)
}

func TestCacheGet(t *testing.T) {
	cache := algo.New(int64(0), nil, nil)
	cache.Add("key1", String("1234"))
	if v, ok := cache.Get("key1"); !ok || string(v.(String)) != "1234" {
		t.Fatalf("cache hit key1=1234 failed")
	}
	if _, ok := cache.Get("key2"); ok {
		t.Fatalf("cache miss key2 failed")
	}
}

func TestLRURemoveOldest(t *testing.T) {
	k1, k2, k3 := "key1", "key2", "k3"
	v1, v2, v3 := "value1", "value2", "v3"
	cap := len(k1 + k2 + v1 + v2)
	cache := algo.New(int64(cap), algo.NewLRU(), nil)
	cache.Add(k1, String(v1))
	cache.Add(k2, String(v2))
	cache.Add(k3, String(v3))

	if _, ok := cache.Get("key1"); ok || cache.Len() != 2 {
		t.Fatalf("LRU eviction failed")
	}
}

func TestOnEvicted(t *testing.T) {
	keys := make([]string, 0)
	callback := func(key string, value algo.Value) {
		keys = append(keys, key)
	}
	cache := algo.New(int64(10), algo.NewLRU(), callback)
	cache.Add("key1", String("123456"))
	cache.Add("k2", String("k2"))
	cache.Add("k3", String("k3"))
	cache.Add("k4", String("k4"))

	expect := []string{"key1", "k2"}
	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("OnEvicted mismatch, expect %v got %v", expect, keys)
	}
}

func TestAddUpdatesExistingValue(t *testing.T) {
	cache := algo.New(int64(0), nil, nil)
	cache.Add("key", String("1"))
	cache.Add("key", String("111"))

	if v, ok := cache.Get("key"); !ok || string(v.(String)) != "111" {
		t.Fatal("expected updated value to be visible")
	}
}

func TestRemove(t *testing.T) {
	cache := algo.New(int64(0), nil, nil)
	cache.Add("key", String("value"))
	cache.Remove("key")
	if _, ok := cache.Get("key"); ok {
		t.Fatal("expected key to be removed")
	}
}

func TestLFUEvictsLeastFrequentlyUsed(t *testing.T) {
	cache := algo.New(int64(len("a1")+len("b22")), algo.NewLFU(), nil)
	cache.Add("a", String("1"))
	cache.Add("b", String("22"))
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected a to exist")
	}
	cache.Add("c", String("3"))

	if _, ok := cache.Get("b"); ok {
		t.Fatal("expected LFU to evict key b")
	}
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected key a to remain")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Fatal("expected key c to remain")
	}
}

func TestLFUTieBreaksByLeastRecentlyUsedWithinSameFreq(t *testing.T) {
	evictor := algo.NewLFU()
	evictor.OnAdd("a")
	evictor.OnAdd("b")
	evictor.OnAccess("a")
	evictor.OnAccess("b")

	if victim := evictor.Evict(); victim != "a" {
		t.Fatalf("expected key a to be evicted first among same frequency keys, got %q", victim)
	}
}

func TestLFURemoveMaintainsMinFreq(t *testing.T) {
	cache := algo.New(int64(0), algo.NewLFU(), nil)
	cache.Add("a", String("1"))
	cache.Add("b", String("1"))
	cache.Get("a")
	cache.Remove("b")
	cache.Add("c", String("1"))

	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected key a to remain after remove")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Fatal("expected key c to be accessible after remove")
	}
}

func TestLRUKEvictsHistoryBeforePromotedEntries(t *testing.T) {
	cache := algo.New(int64(len("a1")+len("b1")), algo.NewLRUK(2), nil)
	cache.Add("a", String("1"))
	cache.Get("a") // promote a into the main cache queue
	cache.Add("b", String("1"))
	cache.Add("c", String("1"))

	if _, ok := cache.Get("b"); ok {
		t.Fatal("expected history entry b to be evicted before promoted key a")
	}
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected promoted key a to remain")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Fatal("expected newest history key c to remain")
	}
}

func TestLRUKBehavesLikeLRUWhenKIsOne(t *testing.T) {
	cache := algo.New(int64(len("a1")+len("b1")), algo.NewLRUK(1), nil)
	cache.Add("a", String("1"))
	cache.Add("b", String("1"))
	cache.Get("a")
	cache.Add("c", String("1"))

	if _, ok := cache.Get("b"); ok {
		t.Fatal("expected key b to be evicted as least recently used")
	}
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected key a to remain after recent access")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Fatal("expected key c to remain")
	}
}

func TestLRUKRemoveKeepsQueuesConsistent(t *testing.T) {
	cache := algo.New(int64(0), algo.NewLRUK(2), nil)
	cache.Add("a", String("1"))
	cache.Get("a") // promote
	cache.Add("b", String("1"))
	cache.Remove("a")
	cache.Add("c", String("1"))

	if _, ok := cache.Get("a"); ok {
		t.Fatal("expected removed key a to stay removed")
	}
	if _, ok := cache.Get("b"); !ok {
		t.Fatal("expected history key b to remain")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Fatal("expected key c to remain")
	}
}

func TestARCEvictsRecentColdEntryBeforeFrequentEntry(t *testing.T) {
	cache := algo.New(int64(len("a1")+len("b1")), algo.NewARC(), nil)
	cache.Add("a", String("1"))
	cache.Add("b", String("1"))
	cache.Get("a") // promote a into T2
	cache.Add("c", String("1"))

	if _, ok := cache.Get("b"); ok {
		t.Fatal("expected ARC to evict cold entry b")
	}
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("expected frequently accessed key a to remain")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Fatal("expected newest key c to remain")
	}
}

func TestARCGhostHitPromotesReintroducedKey(t *testing.T) {
	cache := algo.New(int64(len("a1")+len("b1")), algo.NewARC(), nil)
	cache.Add("a", String("1"))
	cache.Add("b", String("1"))
	cache.Get("a")
	cache.Add("c", String("1")) // evict b into B1

	cache.Add("b", String("1")) // ghost hit should promote b and evict c

	if _, ok := cache.Get("b"); !ok {
		t.Fatal("expected ghost-hit key b to be restored")
	}
	if cache.Len() != 2 {
		t.Fatalf("expected cache size to remain bounded, got %d", cache.Len())
	}
	remaining := 0
	if _, ok := cache.Get("a"); ok {
		remaining++
	}
	if _, ok := cache.Get("c"); ok {
		remaining++
	}
	if remaining != 1 {
		t.Fatalf("expected exactly one of a or c to remain after ghost-hit adaptation, got %d", remaining)
	}
}

func TestCacheEvictionDoesNotDeadlock(t *testing.T) {
	cache := algo.New(int64(len("a1")+len("b1")), algo.NewLRU(), nil)

	done := make(chan struct{})
	go func() {
		cache.Add("a", String("1"))
		cache.Add("b", String("1"))
		cache.Add("c", String("1"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cache eviction appears to deadlock")
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	cache := algo.New(int64(256), algo.NewLFU(), nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := string(rune('a' + (j % 8)))
				cache.Add(key, String("v"))
				cache.Get(key)
				if j%10 == 0 {
					cache.Remove(key)
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent cache access did not complete")
	}
}
