package geecache_test

import (
	"context"
	"sync/atomic"
	"testing"

	"geecache"
)

func TestBloomFilterObserveOnlyLearnsSuccessfulLoads(t *testing.T) {
	filter, err := geecache.NewBloomFilter(128, 0.01)
	if err != nil {
		t.Fatalf("NewBloomFilter: %v", err)
	}

	var loads int32
	group := geecache.NewGroupWithOptions(
		"bloom-observe-only",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte("value-" + key), nil
		}),
		geecache.WithBloomFilter(filter),
	)
	defer group.Close()

	view, err := group.Get("Tom")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := view.String(); got != "value-Tom" {
		t.Fatalf("unexpected value %q", got)
	}
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected one local load, got %d", got)
	}
	if !filter.MightContain("Tom") {
		t.Fatal("expected successful load to update bloom filter")
	}
	if got := group.Stats().BloomRejects; got != 0 {
		t.Fatalf("expected no bloom rejects, got %d", got)
	}
}

func TestBloomFilterRejectOnMissSkipsLocalLoad(t *testing.T) {
	filter, err := geecache.NewBloomFilter(128, 0.01)
	if err != nil {
		t.Fatalf("NewBloomFilter: %v", err)
	}
	filter.Add("Tom")

	var loads int32
	group := geecache.NewGroupWithOptions(
		"bloom-reject-miss",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return nil, geecache.ErrNotFound
		}),
		geecache.WithBloomFilter(filter),
		geecache.WithBloomRejectOnMiss(),
	)
	defer group.Close()

	if _, err := group.Get("missing"); err != geecache.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 0 {
		t.Fatalf("expected bloom reject to skip local load, got %d", got)
	}
	if got := group.Stats().BloomRejects; got != 1 {
		t.Fatalf("expected one bloom reject, got %d", got)
	}
}

func TestBloomFilterRejectOnMissAllowsPreloadedKey(t *testing.T) {
	filter, err := geecache.NewBloomFilter(128, 0.01)
	if err != nil {
		t.Fatalf("NewBloomFilter: %v", err)
	}

	var loads int32
	group := geecache.NewGroupWithOptions(
		"bloom-preload",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte("value-" + key), nil
		}),
		geecache.WithBloomFilter(filter),
		geecache.WithBloomRejectOnMiss(),
	)
	defer group.Close()

	group.AddBloomKeys("Tom")

	view, err := group.Get("Tom")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := view.String(); got != "value-Tom" {
		t.Fatalf("unexpected value %q", got)
	}
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected one local load, got %d", got)
	}
}
