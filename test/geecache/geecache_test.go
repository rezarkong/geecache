package geecache_test

import (
	"context"
	"fmt"
	"geecache"
	"geecache/algo"
	pb "geecache/geecachepb"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
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

func (failingPeerPicker) PickPeer(string) (geecache.PeerGetter, bool, bool) {
	return failingPeerGetter{}, true, false
}

func (failingPeerPicker) Close() error {
	return nil
}

type failingPeerGetter struct{}

func (failingPeerGetter) Get(context.Context, *pb.Request, *pb.Response) error {
	return fmt.Errorf("peer unavailable")
}

type flakyPeerPicker struct {
	getter geecache.PeerGetter
}

func (p flakyPeerPicker) PickPeer(string) (geecache.PeerGetter, bool, bool) {
	return p.getter, true, false
}

func (p flakyPeerPicker) Close() error {
	return nil
}

type switchablePeerPicker struct {
	mu     sync.RWMutex
	getter geecache.PeerGetter
	ok     bool
	self   bool
}

func (p *switchablePeerPicker) PickPeer(string) (geecache.PeerGetter, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.getter, p.ok, p.self
}

func (p *switchablePeerPicker) Close() error {
	return nil
}

func (p *switchablePeerPicker) Set(getter geecache.PeerGetter, ok bool, self bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.getter = getter
	p.ok = ok
	p.self = self
}

type staticValuePeerGetter struct {
	calls int32
	value string
	err   error
}

func (g *staticValuePeerGetter) Get(_ context.Context, _ *pb.Request, out *pb.Response) error {
	atomic.AddInt32(&g.calls, 1)
	if g.err != nil {
		return g.err
	}
	out.Value = []byte(g.value)
	return nil
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
	return geecache.ErrNotFound
}

func TestGetter(t *testing.T) {
	var f geecache.Getter = geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("key")
	if v, _ := f.Get(context.Background(), "key"); !reflect.DeepEqual(v, expect) {
		t.Fatal("callback failed")
	}
}

func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))
	gee := geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(
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
	geecache.NewGroup(groupName, 2<<10, geecache.GetterFunc(
		func(_ context.Context, _ string) (bytes []byte, err error) { return }))
	if group := geecache.GetGroup(groupName); group == nil {
		t.Fatalf("group %s not exist", groupName)
	}

	if group := geecache.GetGroup(groupName + "111"); group != nil {
		t.Fatalf("expect nil, but got non-nil group")
	}
}

func TestGetFallsBackOnPeerFailure(t *testing.T) {
	var loads int32
	gee := geecache.NewGroup("peer-fallback", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
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

func TestGroupSwitchesToLocalOwnerAfterPeerChange(t *testing.T) {
	var localLoads int32
	group := geecache.NewGroup("dynamic-owner-switch", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&localLoads, 1)
		return []byte("local-" + key), nil
	}))
	defer group.Close()

	remote := &staticValuePeerGetter{value: "peer-Tom"}
	picker := &switchablePeerPicker{getter: remote, ok: true}
	group.RegisterPeers(picker)

	view, err := group.Get("Tom")
	if err != nil {
		t.Fatalf("Get from remote owner: %v", err)
	}
	if got := view.String(); got != "peer-Tom" {
		t.Fatalf("expected remote owner value, got %q", got)
	}
	if got := atomic.LoadInt32(&localLoads); got != 0 {
		t.Fatalf("expected no local load while peer owns key, got %d", got)
	}

	picker.Set(nil, true, true)

	view, err = group.Get("Tom")
	if err != nil {
		t.Fatalf("Get after ownership moves local: %v", err)
	}
	if got := view.String(); got != "local-Tom" {
		t.Fatalf("expected local owner value after peer change, got %q", got)
	}
	if got := atomic.LoadInt32(&localLoads); got != 1 {
		t.Fatalf("expected one local reload after peer change, got %d", got)
	}
	if got := atomic.LoadInt32(&remote.calls); got != 1 {
		t.Fatalf("expected one remote fetch before peer change, got %d", got)
	}

	if _, err := group.Get("Tom"); err != nil {
		t.Fatalf("Get cached local value: %v", err)
	}
	if got := atomic.LoadInt32(&localLoads); got != 1 {
		t.Fatalf("expected cached local owner value after first reload, got %d loads", got)
	}

	stats := group.Stats()
	if stats.PeerLoads != 1 || stats.LocalLoads != 1 {
		t.Fatalf("unexpected load counters after peer change: %+v", stats)
	}
}

func TestGetSingleflight(t *testing.T) {
	const goroutines = 20

	var loads int32
	gee := geecache.NewGroup("singleflight", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
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

func TestGetSingleflightCompensatesLFUBurstHotness(t *testing.T) {
	const goroutines = 20

	var loads int32
	gee := geecache.NewGroupWithOptions(
		"singleflight-lfu-burst",
		4,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			time.Sleep(10 * time.Millisecond)
			return []byte("1"), nil
		}),
		geecache.WithEvictor(func() algo.Evictor { return algo.NewLFU() }),
	)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := gee.Get("a"); err != nil {
				t.Errorf("get a: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if _, err := gee.Get("b"); err != nil {
		t.Fatalf("get b: %v", err)
	}
	if _, err := gee.Get("c"); err != nil {
		t.Fatalf("get c: %v", err)
	}
	if _, err := gee.Get("a"); err != nil {
		t.Fatalf("get cached a: %v", err)
	}

	if got := atomic.LoadInt32(&loads); got != 3 {
		t.Fatalf("expected hot key a to stay cached after burst compensation, got %d loads", got)
	}
}

func TestGetSingleflightCompensatesLRUKBurstHotness(t *testing.T) {
	const goroutines = 20

	var loads int32
	gee := geecache.NewGroupWithOptions(
		"singleflight-lruk-burst",
		4,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			time.Sleep(10 * time.Millisecond)
			return []byte("1"), nil
		}),
		geecache.WithEvictor(func() algo.Evictor { return algo.NewLRUK(2) }),
	)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := gee.Get("a"); err != nil {
				t.Errorf("get a: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if _, err := gee.Get("b"); err != nil {
		t.Fatalf("get b: %v", err)
	}
	if _, err := gee.Get("c"); err != nil {
		t.Fatalf("get c: %v", err)
	}
	if _, err := gee.Get("a"); err != nil {
		t.Fatalf("get cached a: %v", err)
	}

	if got := atomic.LoadInt32(&loads); got != 3 {
		t.Fatalf("expected promoted key a to stay cached after burst compensation, got %d loads", got)
	}
}

func TestGetContextSingleflightDoesNotShareCallerCancellation(t *testing.T) {
	var loads int32
	gee := geecache.NewGroup("singleflight-context", 2<<10, geecache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&loads, 1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(30 * time.Millisecond):
			return []byte("value-" + key), nil
		}
	}))

	firstCtx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	type result struct {
		view geecache.ByteView
		err  error
	}

	firstResult := make(chan result, 1)
	secondResult := make(chan result, 1)

	go func() {
		view, err := gee.GetContext(firstCtx, "Tom")
		firstResult <- result{view: view, err: err}
	}()

	time.Sleep(10 * time.Millisecond)

	go func() {
		view, err := gee.GetContext(context.Background(), "Tom")
		secondResult <- result{view: view, err: err}
	}()

	first := <-firstResult
	if first.err != context.DeadlineExceeded {
		t.Fatalf("expected first caller to fail with deadline exceeded, got value=%q err=%v", first.view.String(), first.err)
	}

	second := <-secondResult
	if second.err != nil {
		t.Fatalf("expected second caller to succeed, got %v", second.err)
	}
	if got := second.view.String(); got != "value-Tom" {
		t.Fatalf("unexpected value %q", got)
	}
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected one shared load, got %d", got)
	}
}

func TestGetCachesNotFoundWhenEnabled(t *testing.T) {
	var loads int32
	gee := geecache.NewGroupWithOptions(
		"empty-cache",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, _ string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return nil, geecache.ErrNotFound
		}),
		geecache.WithEmptyCache(30*time.Millisecond),
	)

	for i := 0; i < 2; i++ {
		if _, err := gee.Get("missing"); err != geecache.ErrNotFound {
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
	if _, err := gee.Get("missing"); err != geecache.ErrNotFound {
		t.Fatalf("expected ErrNotFound after ttl, got %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 2 {
		t.Fatalf("expected not-found loader to run again after ttl, got %d", got)
	}
}

func TestGetRetriesPeerBeforeLocalFallback(t *testing.T) {
	peer := &flakyPeerGetter{failuresLeft: 1}
	var localLoads int32
	gee := geecache.NewGroupWithOptions(
		"peer-retry",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&localLoads, 1)
			return []byte("local-" + key), nil
		}),
		geecache.WithPeerRetries(2),
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

func TestGetContextPeerRetryBackoffStopsOnCancellation(t *testing.T) {
	peer := &alwaysFailPeerGetter{err: timeoutNetError{}}
	gee := geecache.NewGroupWithOptions(
		"peer-retry-cancel",
		2<<10,
		geecache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return []byte("local-" + key), nil
			}
		}),
		geecache.WithPeerRetries(3),
		geecache.WithPeerRetryBackoff(200*time.Millisecond),
	)
	gee.RegisterPeers(flakyPeerPicker{getter: peer})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := gee.GetContext(ctx, "Tom")
	elapsed := time.Since(start)

	if err != context.DeadlineExceeded {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("expected cancellation to stop retry backoff quickly, took %s", elapsed)
	}
	if got := atomic.LoadInt32(&peer.calls); got != 1 {
		t.Fatalf("expected retry loop to stop during first backoff, got %d peer calls", got)
	}
}

func TestPeerNotFoundDoesNotFallbackLocally(t *testing.T) {
	peer := &notFoundPeerGetter{}
	var localLoads int32
	gee := geecache.NewGroupWithOptions(
		"peer-not-found",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&localLoads, 1)
			return []byte("local-" + key), nil
		}),
		geecache.WithEmptyCache(30*time.Millisecond),
	)
	gee.RegisterPeers(flakyPeerPicker{getter: peer})

	if _, err := gee.Get("Tom"); err != geecache.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if got := atomic.LoadInt32(&localLoads); got != 0 {
		t.Fatalf("expected no local fallback on peer not found, got %d loads", got)
	}

	if _, err := gee.Get("Tom"); err != geecache.ErrNotFound {
		t.Fatalf("expected cached ErrNotFound, got %v", err)
	}
	if got := atomic.LoadInt32(&peer.calls); got != 1 {
		t.Fatalf("expected peer to be called once due to empty cache, got %d", got)
	}
}

func TestGetUsesPositiveCacheTTL(t *testing.T) {
	var loads int32
	gee := geecache.NewGroupWithOptions(
		"positive-ttl",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte("value-" + key), nil
		}),
		geecache.WithCacheTTL(25*time.Millisecond, 0),
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

func TestDeleteForcesReload(t *testing.T) {
	var loads int32
	gee := geecache.NewGroup(
		"delete-key",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte("value-" + key), nil
		}),
	)

	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	if _, err := gee.Get("Jack"); err != nil {
		t.Fatalf("warm second key: %v", err)
	}
	gee.Delete("Tom", "Jack")
	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("reload after delete: %v", err)
	}
	if _, err := gee.Get("Jack"); err != nil {
		t.Fatalf("reload second key after delete: %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 4 {
		t.Fatalf("expected delete to force reload for both keys, got %d loads", got)
	}
}

func TestGroupWorksWithConfiguredShards(t *testing.T) {
	var loads int32
	gee := geecache.NewGroupWithOptions(
		"sharded-group",
		256,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte("value-" + key), nil
		}),
		geecache.WithShards(4),
	)

	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, key := range keys {
		view, err := gee.Get(key)
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		if got := view.String(); got != "value-"+key {
			t.Fatalf("unexpected value for %q: %q", key, got)
		}
	}

	for _, key := range keys {
		if _, err := gee.Get(key); err != nil {
			t.Fatalf("get cached %q: %v", key, err)
		}
	}

	if got := atomic.LoadInt32(&loads); got != int32(len(keys)) {
		t.Fatalf("expected one load per key with shards enabled, got %d", got)
	}
}

func TestClearForcesReload(t *testing.T) {
	var loads int32
	gee := geecache.NewGroup(
		"clear-cache",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte("value-" + key), nil
		}),
	)

	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	if _, err := gee.Get("Jack"); err != nil {
		t.Fatalf("warm second key: %v", err)
	}
	gee.Clear()
	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("reload after clear: %v", err)
	}
	if _, err := gee.Get("Jack"); err != nil {
		t.Fatalf("reload second key after clear: %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 4 {
		t.Fatalf("expected clear to force reload for all keys, got %d loads", got)
	}
}

func TestBackgroundCleanupRemovesExpiredEntries(t *testing.T) {
	var loads int32
	gee := geecache.NewGroupWithOptions(
		"cleanup-expired",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte("value-" + key), nil
		}),
		geecache.WithCacheTTL(15*time.Millisecond, 0),
		geecache.WithCleanupInterval(5*time.Millisecond),
	)

	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}

	time.Sleep(40 * time.Millisecond)

	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("reload after cleanup: %v", err)
	}
	if got := atomic.LoadInt32(&loads); got != 2 {
		t.Fatalf("expected cleanup to remove expired entry and trigger reload, got %d loads", got)
	}
	if stats := gee.Stats(); stats.CacheExpirations == 0 {
		t.Fatalf("expected cleanup to record expirations, got %+v", stats)
	}
}

func TestReplacingGroupStopsOldCleanup(t *testing.T) {
	oldGroup := geecache.NewGroupWithOptions(
		"cleanup-replaced",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			return []byte("old-" + key), nil
		}),
		geecache.WithCacheTTL(40*time.Millisecond, 0),
		geecache.WithCleanupInterval(10*time.Millisecond),
	)

	if _, err := oldGroup.Get("Tom"); err != nil {
		t.Fatalf("warm old group: %v", err)
	}

	newGroup := geecache.NewGroupWithOptions(
		"cleanup-replaced",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			return []byte("new-" + key), nil
		}),
	)

	time.Sleep(90 * time.Millisecond)

	if got := oldGroup.Stats().CacheExpirations; got != 0 {
		t.Fatalf("expected old cleanup goroutine to stop after replacement, got expirations=%d", got)
	}
	if view, err := newGroup.Get("Tom"); err != nil || view.String() != "new-Tom" {
		t.Fatalf("expected replacement group to be active, got value=%q err=%v", view.String(), err)
	}
}

func TestDeleteGroupStopsCleanupAndRemovesRegistryEntry(t *testing.T) {
	group := geecache.NewGroupWithOptions(
		"delete-group",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			return []byte("value-" + key), nil
		}),
		geecache.WithCacheTTL(40*time.Millisecond, 0),
		geecache.WithCleanupInterval(10*time.Millisecond),
	)

	if _, err := group.Get("Tom"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	if ok := geecache.DeleteGroup("delete-group"); !ok {
		t.Fatal("expected DeleteGroup to remove existing group")
	}
	if got := geecache.GetGroup("delete-group"); got != nil {
		t.Fatalf("expected deleted group lookup to be nil, got %v", got)
	}

	time.Sleep(90 * time.Millisecond)
	if got := group.Stats().CacheExpirations; got != 0 {
		t.Fatalf("expected deleted group cleanup to stop, got expirations=%d", got)
	}
}

func TestGroupCloseStopsCleanupAndRemovesRegistryEntry(t *testing.T) {
	group := geecache.NewGroupWithOptions(
		"close-group",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			return []byte("value-" + key), nil
		}),
		geecache.WithCacheTTL(40*time.Millisecond, 0),
		geecache.WithCleanupInterval(10*time.Millisecond),
	)

	if _, err := group.Get("Tom"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	group.Close()
	if got := geecache.GetGroup("close-group"); got != nil {
		t.Fatalf("expected closed group lookup to be nil, got %v", got)
	}

	time.Sleep(90 * time.Millisecond)
	if got := group.Stats().CacheExpirations; got != 0 {
		t.Fatalf("expected closed group cleanup to stop, got expirations=%d", got)
	}
}

func TestGroupCloseIsIdempotentAndRejectsReads(t *testing.T) {
	group := geecache.NewGroup(
		"closed-group-read",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			return []byte("value-" + key), nil
		}),
	)

	if err := group.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("second close should be a no-op, got %v", err)
	}
	if _, err := group.Get("Tom"); err != geecache.ErrGroupClosed {
		t.Fatalf("expected ErrGroupClosed after close, got %v", err)
	}
}

func TestPeerCircuitBreakerOpens(t *testing.T) {
	peer := &alwaysFailPeerGetter{err: timeoutNetError{}}
	var localLoads int32
	gee := geecache.NewGroupWithOptions(
		"peer-circuit",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&localLoads, 1)
			return []byte("local-" + key), nil
		}),
		geecache.WithPeerRetries(0),
		geecache.WithPeerCircuitBreaker(2, 50*time.Millisecond),
	)
	gee.RegisterPeers(flakyPeerPicker{getter: peer})

	for i := 0; i < 3; i++ {
		if _, err := gee.Get(fmt.Sprintf("Tom-%d", i)); err != nil {
			t.Fatalf("get: %v", err)
		}
	}

	if got := atomic.LoadInt32(&peer.calls); got != 2 {
		t.Fatalf("expected circuit to stop the third peer request, got %d calls", got)
	}
	if atomic.LoadInt32(&localLoads) != 3 {
		t.Fatalf("expected local fallback for each request, got %d", atomic.LoadInt32(&localLoads))
	}
}

func TestStatsTrackCoreCounters(t *testing.T) {
	gee := geecache.NewGroupWithOptions(
		"stats-counters",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			return []byte("value-" + key), nil
		}),
	)

	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := gee.Get("Tom"); err != nil {
		t.Fatalf("get cache hit: %v", err)
	}
	stats := gee.Stats()
	if stats.Requests != 2 || stats.CacheMisses != 1 || stats.CacheHits != 1 {
		t.Fatalf("unexpected counters: %+v", stats)
	}
	if stats.LocalLoads != 1 || stats.PeerLoads != 0 || stats.PeerFailures != 0 {
		t.Fatalf("unexpected counters: %+v", stats)
	}
	if stats.HitRate <= 0 || stats.HitRate >= 1 {
		t.Fatalf("expected hit rate between 0 and 1, got %+v", stats)
	}
}

func TestWithEvictorUsesConfiguredAlgorithm(t *testing.T) {
	var loads int32
	gee := geecache.NewGroupWithOptions(
		"lfu-evictor",
		int64(len("a")+len("1")+len("b")+len("2")),
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt32(&loads, 1)
			return []byte(key), nil
		}),
		geecache.WithEvictor(func() algo.Evictor { return algo.NewLFU() }),
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

func benchmarkGroup(b *testing.B, name string) *geecache.Group {
	b.Helper()

	return geecache.NewGroupWithOptions(name, 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
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

	hotKeys := benchmarkEnvInt("BENCH_HOT_KEYS", 3)
	coldKeys := benchmarkEnvInt("BENCH_COLD_KEYS", 128)
	blockSize := benchmarkEnvInt("BENCH_BLOCK_SIZE", 10)
	coldPerBlock := benchmarkEnvInt("BENCH_COLD_PER_BLOCK", 8)
	cacheEntries := benchmarkEnvInt("BENCH_CACHE_ENTRIES", 5)
	warmHits := benchmarkEnvInt("BENCH_WARM_HITS", 200)
	cacheBytes := int64(cacheEntries * (len("c000000") + 1))
	var loads int64
	gee := geecache.NewGroupWithOptions(
		"benchmark-mixed",
		cacheBytes,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt64(&loads, 1)
			return []byte("v"), nil
		}),
		benchmarkEvictorOption(b),
	)

	// Warm up hot keys so LFU can accumulate frequency before cold scans start.
	for i := 0; i < warmHits; i++ {
		key := fmt.Sprintf("h%06d", i%hotKeys)
		if _, err := gee.Get(key); err != nil {
			b.Fatalf("warm hot key: %v", err)
		}
	}

	startLoads := atomic.LoadInt64(&loads)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var key string
		if i%blockSize < coldPerBlock {
			key = fmt.Sprintf("c%06d", i%coldKeys)
		} else {
			key = fmt.Sprintf("h%06d", i%hotKeys)
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

func BenchmarkGroupGetWideKeyspace(b *testing.B) {
	restore := silenceLogs()
	defer restore()

	uniqueKeys := benchmarkEnvInt("BENCH_UNIQUE_KEYS", 65536)
	cacheEntries := benchmarkEnvInt("BENCH_CACHE_ENTRIES", 5)
	cacheBytes := int64(cacheEntries * (len("u000000") + 1))
	var loads int64
	gee := geecache.NewGroupWithOptions(
		"benchmark-wide-keyspace",
		cacheBytes,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt64(&loads, 1)
			return []byte("v"), nil
		}),
		benchmarkEvictorOption(b),
	)

	startLoads := atomic.LoadInt64(&loads)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("u%06d", i%uniqueKeys)
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

func benchmarkEvictorOption(b *testing.B) geecache.Option {
	b.Helper()

	switch evictor := os.Getenv("BENCH_EVICTOR"); evictor {
	case "", "lru":
		return geecache.WithEvictor(func() algo.Evictor { return algo.NewLRU() })
	case "lfu":
		return geecache.WithEvictor(func() algo.Evictor { return algo.NewLFU() })
	case "lru-k", "lruk":
		return geecache.WithEvictor(func() algo.Evictor { return algo.NewLRUK(2) })
	case "arc":
		return geecache.WithEvictor(func() algo.Evictor { return algo.NewARC() })
	default:
		b.Fatalf("unsupported BENCH_EVICTOR %q", evictor)
		return nil
	}
}

func benchmarkEnvInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}
