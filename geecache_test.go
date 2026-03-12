package geecache

import (
	"context"
	"fmt"
	"geecache/algo"
	pb "geecache/geecachepb"
	"io"
	"log"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

type failingPeerPicker struct{}

func (failingPeerPicker) PickPeer(string) (PeerGetter, bool) {
	return failingPeerGetter{}, true
}

type failingPeerGetter struct{}

func (failingPeerGetter) Get(context.Context, *pb.Request, *pb.Response) error {
	return fmt.Errorf("peer unavailable")
}

type flakyPeerPicker struct {
	getter PeerGetter
}

func (p flakyPeerPicker) PickPeer(string) (PeerGetter, bool) {
	return p.getter, true
}

type flakyPeerGetter struct {
	failuresLeft int32
	calls        int32
}

func (g *flakyPeerGetter) Get(_ context.Context, in *pb.Request, out *pb.Response) error {
	atomic.AddInt32(&g.calls, 1)
	if atomic.AddInt32(&g.failuresLeft, -1) >= 0 {
		return fmt.Errorf("temporary peer error")
	}
	out.Value = []byte("peer-" + in.Key)
	return nil
}

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

type alwaysFailPeerGetter struct {
	calls int32
	err   error
}

func (g *alwaysFailPeerGetter) Get(context.Context, *pb.Request, *pb.Response) error {
	atomic.AddInt32(&g.calls, 1)
	return g.err
}

type notFoundPeerGetter struct {
	calls int32
}

func (g *notFoundPeerGetter) Get(context.Context, *pb.Request, *pb.Response) error {
	atomic.AddInt32(&g.calls, 1)
	return ErrNotFound
}

func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("key")
	if v, _ := f.Get(context.Background(), "key"); !reflect.DeepEqual(v, expect) {
		t.Fatal("callback failed")
	}
}

func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))
	gee := NewGroup("scores", 2<<10, GetterFunc(
		func(_ context.Context, key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				if _, ok := loadCounts[key]; !ok {
					loadCounts[key] = 0
				}
				loadCounts[key]++
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	for k, v := range db {
		if view, err := gee.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value of Tom")
		}
		if _, err := gee.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}

	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknow should be empty, but %s got", view)
	}
}

func TestGetGroup(t *testing.T) {
	groupName := "scores"
	NewGroup(groupName, 2<<10, GetterFunc(
		func(_ context.Context, _ string) (bytes []byte, err error) { return }))
	if group := GetGroup(groupName); group == nil || group.name != groupName {
		t.Fatalf("group %s not exist", groupName)
	}

	if group := GetGroup(groupName + "111"); group != nil {
		t.Fatalf("expect nil, but %s got", group.name)
	}
}

func TestGetFallsBackOnPeerFailure(t *testing.T) {
	var loads int32
	gee := NewGroup("peer-fallback", 2<<10, GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&loads, 1)
		return []byte("local-" + key), nil
	}))
	gee.RegisterPeers(failingPeerPicker{})

	view, err := gee.Get("Tom")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := view.String(); got != "local-Tom" {
		t.Fatalf("unexpected value %q", got)
	}
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected local getter to run once, got %d", got)
	}
}

func TestGetSingleflight(t *testing.T) {
	const goroutines = 20

	var loads int32
	gee := NewGroup("singleflight", 2<<10, GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&loads, 1)
		time.Sleep(10 * time.Millisecond)
		return []byte("value-" + key), nil
	}))

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			view, err := gee.Get("Tom")
			if err != nil {
				errCh <- err
				return
			}
			if got := view.String(); got != "value-Tom" {
				errCh <- fmt.Errorf("unexpected value %q", got)
			}
		}()
	}
	close(start)

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected loader to run once, got %d", got)
	}
}

func TestGetCachesNotFoundWhenEnabled(t *testing.T) {
	var loads int32
	gee := NewGroupWithOptions(
		"empty-cache",
		2<<10,
		GetterFunc(func(_ context.Context, _ string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return nil, ErrNotFound
		}),
		WithEmptyCache(30*time.Millisecond),
	)

	for i := 0; i < 2; i++ {
		if _, err := gee.Get("missing"); err != ErrNotFound {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	}
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected not-found loader to run once, got %d", got)
	}

	stats := gee.Stats()
	if stats.EmptyHits != 1 {
		t.Fatalf("expected one empty-cache hit, got %+v", stats)
	}

	time.Sleep(40 * time.Millisecond)
	if _, err := gee.Get("missing"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after ttl, got %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 2 {
		t.Fatalf("expected not-found loader to run again after ttl, got %d", got)
	}
}

func TestGetRetriesPeerBeforeLocalFallback(t *testing.T) {
	peer := &flakyPeerGetter{failuresLeft: 1}
	var localLoads int32
	gee := NewGroupWithOptions(
		"peer-retry",
		2<<10,
		GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&localLoads, 1)
			return []byte("local-" + key), nil
		}),
		WithPeerRetries(2),
	)
	gee.RegisterPeers(flakyPeerPicker{getter: peer})

	view, err := gee.Get("Tom")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := view.String(); got != "peer-Tom" {
		t.Fatalf("unexpected value %q", got)
	}
	if got := atomic.LoadInt32(&peer.calls); got != 2 {
		t.Fatalf("expected peer to be retried once, got %d calls", got)
	}
	if got := atomic.LoadInt32(&localLoads); got != 0 {
		t.Fatalf("expected no local fallback, got %d loads", got)
	}
	if stats := gee.Stats(); stats.PeerLoads != 1 {
		t.Fatalf("expected peer load stats to record success, got %+v", stats)
	}
}

func TestPeerNotFoundDoesNotFallbackLocally(t *testing.T) {
	peer := &notFoundPeerGetter{}
	var localLoads int32
	gee := NewGroupWithOptions(
		"peer-not-found",
		2<<10,
		GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&localLoads, 1)
			return []byte("local-" + key), nil
		}),
		WithEmptyCache(30*time.Millisecond),
	)
	gee.RegisterPeers(flakyPeerPicker{getter: peer})

	if _, err := gee.Get("Tom"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if got := atomic.LoadInt32(&localLoads); got != 0 {
		t.Fatalf("expected no local fallback on peer not found, got %d loads", got)
	}
	stats := gee.Stats()
	if stats.PeerNotFounds != 1 || stats.PeerFallbacks != 0 {
		t.Fatalf("unexpected stats after peer not found: %+v", stats)
	}

	if _, err := gee.Get("Tom"); err != ErrNotFound {
		t.Fatalf("expected cached ErrNotFound, got %v", err)
	}
	if got := atomic.LoadInt32(&peer.calls); got != 1 {
		t.Fatalf("expected peer to be called once due to empty cache, got %d", got)
	}
}

func TestGetUsesPositiveCacheTTL(t *testing.T) {
	var loads int32
	gee := NewGroupWithOptions(
		"positive-ttl",
		2<<10,
		GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte("value-" + key), nil
		}),
		WithCacheTTL(25*time.Millisecond, 0),
	)

	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected initial cache hit to suppress reload, got %d", got)
	}

	time.Sleep(35 * time.Millisecond)
	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("get after ttl: %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 2 {
		t.Fatalf("expected reload after ttl, got %d", got)
	}
	if stats := gee.Stats(); stats.CacheExpirations == 0 {
		t.Fatalf("expected cache expiration stats, got %+v", stats)
	}
}

func TestPeerCircuitBreakerOpens(t *testing.T) {
	peer := &alwaysFailPeerGetter{err: timeoutNetError{}}
	var localLoads int32
	gee := NewGroupWithOptions(
		"peer-circuit",
		2<<10,
		GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&localLoads, 1)
			return []byte("local-" + key), nil
		}),
		WithPeerRetries(0),
		WithPeerCircuitBreaker(2, 50*time.Millisecond),
	)
	gee.RegisterPeers(flakyPeerPicker{getter: peer})

	for i := 0; i < 3; i++ {
		if _, err := gee.Get(fmt.Sprintf("Tom-%d", i)); err != nil {
			t.Fatalf("get: %v", err)
		}
	}

	stats := gee.Stats()
	if stats.CircuitOpenEvents == 0 {
		t.Fatalf("expected circuit to open, got %+v", stats)
	}
	if stats.CircuitRejects == 0 {
		t.Fatalf("expected circuit rejects, got %+v", stats)
	}
	if atomic.LoadInt32(&localLoads) != 3 {
		t.Fatalf("expected local fallback for each request, got %d", atomic.LoadInt32(&localLoads))
	}
}

func TestStatsTrackDurations(t *testing.T) {
	gee := NewGroupWithOptions(
		"stats-duration",
		2<<10,
		GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			time.Sleep(5 * time.Millisecond)
			return []byte("value-" + key), nil
		}),
	)

	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("get: %v", err)
	}
	stats := gee.Stats()
	if stats.LocalLoadTotalNanos <= 0 || stats.AvgLocalLoadNanos <= 0 {
		t.Fatalf("expected local load duration stats, got %+v", stats)
	}
	if stats.PeerAttemptTotalNanos != 0 || stats.AvgPeerAttemptNanos != 0 {
		t.Fatalf("expected no peer attempt duration stats, got %+v", stats)
	}
	if stats.Requests != 1 || stats.CacheMisses != 1 {
		t.Fatalf("unexpected counters: %+v", stats)
	}
}

func TestWithEvictorUsesConfiguredAlgorithm(t *testing.T) {
	var loads int32
	gee := NewGroupWithOptions(
		"lfu-evictor",
		int64(len("a")+len("1")+len("b")+len("2")),
		GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte(key), nil
		}),
		WithEvictor(func() algo.Evictor { return algo.NewLFU() }),
	)

	if _, err := gee.Get("a"); err != nil {
		t.Fatalf("get a: %v", err)
	}
	if _, err := gee.Get("b"); err != nil {
		t.Fatalf("get b: %v", err)
	}
	if _, err := gee.Get("a"); err != nil {
		t.Fatalf("get a again: %v", err)
	}
	if _, err := gee.Get("c"); err != nil {
		t.Fatalf("get c: %v", err)
	}

	if _, err := gee.Get("b"); err != nil {
		t.Fatalf("reload b: %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 4 {
		t.Fatalf("expected LFU to evict b and trigger 4 total loads, got %d", got)
	}
}

func silenceLogs() func() {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	return func() {
		log.SetOutput(prev)
	}
}

func benchmarkGroup(b *testing.B, name string) *Group {
	b.Helper()

	return NewGroupWithOptions(name, 2<<10, GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	}), benchmarkEvictorOption(b))
}

func BenchmarkGroupGetCacheHit(b *testing.B) {
	restore := silenceLogs()
	defer restore()

	gee := benchmarkGroup(b, "benchmark-hit")
	if _, err := gee.Get("Tom"); err != nil {
		b.Fatalf("warm cache: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := gee.Get("Tom"); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkGroupGetParallelSameKey(b *testing.B) {
	restore := silenceLogs()
	defer restore()

	gee := benchmarkGroup(b, "benchmark-parallel")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := gee.Get("Tom"); err != nil {
				b.Fatalf("get: %v", err)
			}
		}
	})
}

func BenchmarkGroupGetMixedWorkload(b *testing.B) {
	restore := silenceLogs()
	defer restore()

	const (
		hotKeys      = 3
		coldKeys     = 128
		blockSize    = 10
		coldPerBlock = 8
		cacheEntries = 5
		valueLen     = 1
	)

	cacheBytes := int64(cacheEntries * (len("c000") + valueLen))
	var loads int64
	gee := NewGroupWithOptions(
		"benchmark-mixed",
		cacheBytes,
		GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt64(&loads, 1)
			return []byte("v"), nil
		}),
		benchmarkEvictorOption(b),
	)

	// Warm up hot keys so LFU can accumulate frequency before cold scans start.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("h%02d", i%hotKeys)
		if _, err := gee.Get(key); err != nil {
			b.Fatalf("warm hot key: %v", err)
		}
	}

	startLoads := atomic.LoadInt64(&loads)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var key string
		if i%blockSize < coldPerBlock {
			key = fmt.Sprintf("c%03d", i%coldKeys)
		} else {
			key = fmt.Sprintf("h%02d", i%hotKeys)
		}
		if _, err := gee.Get(key); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
	b.StopTimer()

	misses := atomic.LoadInt64(&loads) - startLoads
	hitRatio := float64(int64(b.N)-misses) / float64(b.N)
	missRatio := float64(misses) / float64(b.N)
	b.ReportMetric(hitRatio, "hit_ratio")
	b.ReportMetric(missRatio, "miss_ratio")
}

func benchmarkEvictorOption(b *testing.B) Option {
	b.Helper()

	switch evictor := os.Getenv("BENCH_EVICTOR"); evictor {
	case "", "lru":
		return WithEvictor(func() algo.Evictor { return algo.NewLRU() })
	case "lfu":
		return WithEvictor(func() algo.Evictor { return algo.NewLFU() })
	default:
		b.Fatalf("unsupported BENCH_EVICTOR %q", evictor)
		return nil
	}
}
