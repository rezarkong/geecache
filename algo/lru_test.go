package algo

import (
	"reflect"
	"testing"
)

type String string

func (d String) Len() int {
	return len(d)
}

func TestCacheGet(t *testing.T) {
	cache := New(int64(0), nil, nil)
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
	cache := New(int64(cap), NewLRU(), nil)
	cache.Add(k1, String(v1))
	cache.Add(k2, String(v2))
	cache.Add(k3, String(v3))

	if _, ok := cache.Get("key1"); ok || cache.Len() != 2 {
		t.Fatalf("LRU eviction failed")
	}
}

func TestOnEvicted(t *testing.T) {
	keys := make([]string, 0)
	callback := func(key string, value Value) {
		keys = append(keys, key)
	}
	cache := New(int64(10), NewLRU(), callback)
	cache.Add("key1", String("123456"))
	cache.Add("k2", String("k2"))
	cache.Add("k3", String("k3"))
	cache.Add("k4", String("k4"))

	expect := []string{"key1", "k2"}
	if !reflect.DeepEqual(expect, keys) {
		t.Fatalf("OnEvicted mismatch, expect %v got %v", expect, keys)
	}
}

func TestAddUpdatesBytes(t *testing.T) {
	cache := New(int64(0), nil, nil)
	cache.Add("key", String("1"))
	cache.Add("key", String("111"))

	if cache.nbytes != int64(len("key")+len("111")) {
		t.Fatal("expected 6 but got", cache.nbytes)
	}
}

func TestRemove(t *testing.T) {
	cache := New(int64(0), nil, nil)
	cache.Add("key", String("value"))
	cache.Remove("key")
	if _, ok := cache.Get("key"); ok {
		t.Fatal("expected key to be removed")
	}
}

func TestLFUEvictsLeastFrequentlyUsed(t *testing.T) {
	cache := New(int64(len("a1")+len("b22")), NewLFU(), nil)
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
	evictor := NewLFU()
	evictor.OnAdd("a")
	evictor.OnAdd("b")
	evictor.OnAccess("a")
	evictor.OnAccess("b")

	if victim := evictor.Evict(); victim != "a" {
		t.Fatalf("expected key a to be evicted first among same frequency keys, got %q", victim)
	}
}

func TestLFURemoveMaintainsMinFreq(t *testing.T) {
	cache := New(int64(0), NewLFU(), nil)
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
