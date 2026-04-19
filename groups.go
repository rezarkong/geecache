package geecache

import (
	"context"
	"errors"
	"fmt"
	pb "geecache/geecachepb"
	"geecache/singleflight"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Getter 加载键值的回调函数入口
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc 函数类型实现 Getter 接口
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get 实现 Getter 接口
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

type peerCircuitState struct {
	consecutiveFailures int       // 连续失败的次数
	openUntil           time.Time // 熔断后直到 openUntil 再开启
}

// Group 是一个缓存命名空间
type Group struct {
	name             string
	getter           Getter // 回调函数
	mainCache        *cache
	peersMu          sync.RWMutex
	peers            PeerPicker
	loader           *singleflight.Group // loader singleflight 机制防止缓存击穿
	localLoader      *singleflight.Group //  localLoader 把“本地加载”这条链路上的同 key 并发请求合并掉
	peerRetries      int                 // peerRetries 设置 peer 抓取失败后重试的次数，失败后调用 localLoader
	peerRetryBackoff time.Duration       // peerRetryBackoff 两次重试调用之间间隔时间
	// emptyTTL caches ErrNotFound values for a short period.
	emptyTTL        time.Duration // emptyTTL 空值缓存 TTL 的时间，用于防缓存击穿
	cacheTTL        time.Duration // cacheTTL 缓存值活多久
	cacheTTLJitter  time.Duration // cacheTTLJitter 缓存值过期时间加的随机值，用于防缓存雪崩
	cleanupInterval time.Duration
	// circuit breaker state for peer fetches.
	peerFailureThreshold int                          // peerFailureThreshold peer 连续失败多少次后触发熔断
	peerCircuitOpen      time.Duration                // peerCircuitOpen 熔断打开后，保持多久不再访问这个 peer
	circuitMu            sync.Mutex                   // circuitMu 保护 peer 熔断状态的互斥锁
	peerCircuits         map[string]*peerCircuitState // peerCircuits 每个 peer 的熔断状态表
	cleanupStop          chan struct{}
	cleanupDone          chan struct{}
	cleanupMu            sync.Mutex
	stats                *Stats
	closed               atomic.Bool
}

type peerPickerCloser interface {
	Close() error
}

var (
	mu     sync.RWMutex              // 对应 group 的读写锁
	groups = make(map[string]*Group) // Group
)

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.closed.Load() {
		if peers != nil {
			_ = peers.Close()
		}
		return
	}
	g.peersMu.Lock()
	defer g.peersMu.Unlock()
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// NewGroup 新建 Group
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	return NewGroupWithOptions(name, cacheBytes, getter)
}

// NewGroupWithOptions create a new instance of Group with extra behaviors.
func NewGroupWithOptions(name string, cacheBytes int64, getter Getter, opts ...Option) *Group {
	if name == "" {
		panic("group name must not be empty")
	}
	if getter == nil {
		panic("nil Getter")
	}
	g := &Group{
		name:                 name,
		getter:               getter,
		mainCache:            &cache{cacheBytes: cacheBytes},
		loader:               &singleflight.Group{},
		localLoader:          &singleflight.Group{},
		peerRetryBackoff:     50 * time.Millisecond,
		peerFailureThreshold: 3,
		peerCircuitOpen:      2 * time.Second,
		peerCircuits:         make(map[string]*peerCircuitState),
		stats:                &Stats{},
	}
	g.mainCache.onExpire = func() { g.stats.addCacheExpirations(1) }
	for _, opt := range opts {
		opt(g)
	}
	mu.Lock()
	existing := groups[name]
	if existing != nil {
		existing.stopCleanup()
	}
	groups[name] = g
	g.startCleanup()
	mu.Unlock()
	return g
}

// Stats returns a snapshot of group metrics.
func (g *Group) Stats() StatsSnapshot {
	return g.stats.Snapshot()
}

// Delete cache 删除某个或某些 key
func (g *Group) Delete(keys ...string) {
	if g.closed.Load() {
		return
	}
	g.mainCache.delete(keys...)
}

// Clear 清空当前 group 的本地缓存
func (g *Group) Clear() {
	if g.closed.Load() {
		return
	}
	g.mainCache.clear()
}

// Close 关闭 group
func (g *Group) Close() error {
	if !g.closed.CompareAndSwap(false, true) {
		return nil
	}

	mu.Lock()
	if current := groups[g.name]; current == g {
		delete(groups, g.name)
	}
	mu.Unlock()
	g.stopCleanup()
	g.mainCache.clear()

	g.peersMu.Lock()
	peers := g.peers
	g.peers = nil
	g.peersMu.Unlock()

	if peers != nil {
		if closer, ok := peers.(peerPickerCloser); ok {
			return closer.Close()
		}
	}
	return nil
}

// GetGroup 返回 name 对应的 Group，没有就返回 nil
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// DeleteGroup 停止并移除对应的 Group
func DeleteGroup(name string) bool {
	mu.Lock()
	g := groups[name]
	if g != nil {
		delete(groups, name)
	}
	mu.Unlock()
	if g == nil {
		return false
	}
	_ = g.Close()
	return true
}

// 本地缓存未命中 进 load 里面找
func (g *Group) load(ctx context.Context, key string, loader *singleflight.Group,
	loadFn func(context.Context, string) (ByteView, error)) (value ByteView, err error) {
	if g.closed.Load() {
		return ByteView{}, ErrGroupClosed
	}
	// 当前调用者自己的 ctx 可以取消，但是共享的那次真实加载不要因为某一个调用者取消就被杀掉
	// 这样可以避免热点 key 并发场景下，第一个请求超时后把整次共享加载都中断掉
	loadCtx := context.WithoutCancel(ctx)
	// 利用 singleflight, 同一个 key，同一时刻只有一次 loadFn(loadCtx, key) 会真正执行
	// 其余并发请求不会重复加载
	// 它们都共享这次加载的结果，记录 key 的访问次数，并发后进行热度补偿
	resultCh := loader.DoChan(key, func() (interface{}, error) {
		return loadFn(loadCtx, key)
	})
	select {
	case <-ctx.Done():
		return ByteView{}, ctx.Err()
	case result := <-resultCh:
		if result.Err == nil {
			result.Shared.Do(func(dups int) {
				g.mainCache.compensateAccess(key, dups)
			})
			return result.Val.(ByteView), nil
		}
		return ByteView{}, result.Err
	}
}

func (g *Group) get(ctx context.Context, key string, loader *singleflight.Group, loadFn func(context.Context, string) (ByteView, error)) (ByteView, error) {
	if g.closed.Load() {
		return ByteView{}, ErrGroupClosed
	}
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	g.stats.addRequests(1)
	// 如果在本地缓存中命中
	if entry, ok := g.mainCache.get(key); ok {
		log.Println("[GCache] hit")
		g.stats.addCacheHits(1)
		// 如果命中的是空缓存
		if entry.negative {
			g.stats.addEmptyHits(1)
			return ByteView{}, ErrNotFound
		}
		return entry.value, nil
	}
	// 本地 Cache 未命中
	g.stats.addCacheMisses(1)
	// 否则从其他节点获取或加载
	return g.load(ctx, key, loader, loadFn)
}

// 不想控制上下文时用 Get 设置超时时用 GetContext
// Get 从 cache 中获取 Key
func (g *Group) Get(key string) (ByteView, error) {
	return g.GetContext(context.Background(), key)
}

// 判断 Peer 现在还能不能继续请求
func (g *Group) allowPeer(peerID string) bool {
	g.circuitMu.Lock()
	defer g.circuitMu.Unlock()
	// 检查这个 peer 是否熔断
	state := g.peerCircuits[peerID]
	if state == nil {
		return true
	}
	if state.openUntil.IsZero() || time.Now().After(state.openUntil) {
		state.openUntil = time.Time{}
		return true
	}
	return false
}

func identifyPeer(peer PeerGetter) string {
	type peerIDer interface {
		ID() string
	}
	if named, ok := peer.(peerIDer); ok {
		return named.ID()
	}
	return fmt.Sprintf("%T", peer)
}

// 找 key 在哪个 peer 上
func (g *Group) pickPeer(key string) (PeerGetter, string, bool, bool) {
	g.peersMu.RLock()
	peers := g.peers
	g.peersMu.RUnlock()
	if peers == nil {
		return nil, "", false, false
	}
	peer, ok, self := peers.PickPeer(key)
	if !ok {
		return nil, "", false, false
	}
	if self {
		return nil, "", true, true
	}
	return peer, identifyPeer(peer), true, false
}

// onPeerSuccess 某个 peer 请求成功后，重置这个 peer 的失败/熔断状态
func (g *Group) onPeerSuccess(peerID string) {
	g.circuitMu.Lock()
	defer g.circuitMu.Unlock()
	if state := g.peerCircuits[peerID]; state != nil {
		state.consecutiveFailures = 0
		state.openUntil = time.Time{}
	}
}

// onPeerFailure 当某个 peer 请求失败时，记录这次失败；如果连续失败达到阈值，就打开熔断
func (g *Group) onPeerFailure(peerID string) {
	g.circuitMu.Lock()
	defer g.circuitMu.Unlock()
	state := g.peerCircuits[peerID]
	if state == nil {
		state = &peerCircuitState{}
		g.peerCircuits[peerID] = state
	}
	state.consecutiveFailures++
	if g.peerFailureThreshold > 0 && state.consecutiveFailures >= g.peerFailureThreshold {
		state.openUntil = time.Now().Add(g.peerCircuitOpen)
		state.consecutiveFailures = 0
	}
}

// randomJitter 增加一小段随机抖动时间，为了缓解缓存雪崩
func (g *Group) randomJitter() time.Duration {
	if g.cacheTTLJitter <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(g.cacheTTLJitter)))
}

// populateCacheEntry 本地缓存加入这个 cacheEntry
func (g *Group) populateCacheEntry(key string, entry cacheEntry) {
	g.mainCache.add(key, entry)
}

// populateCache 封装 cacheEntry 放到 mainCache 里
func (g *Group) populateCache(key string, value ByteView) {
	entry := cacheEntry{value: value}
	if g.cacheTTL > 0 {
		entry.expiresAt = time.Now().Add(g.cacheTTL + g.randomJitter())
	}
	g.populateCacheEntry(key, entry)
}

// getFromPeer 从 peer 获取缓存
func (g *Group) getFromPeer(ctx context.Context, peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	var lastErr error
	for attempt := 0; attempt <= g.peerRetries; attempt++ {
		res := &pb.Response{}
		err := peer.Get(ctx, req, res)
		if err == nil {
			return ByteView{b: cloneBytes(res.Value)}, nil
		}
		lastErr = err
		if attempt < g.peerRetries && isRetryableError(err) {
			if g.peerRetryBackoff > 0 {
				timer := time.NewTimer(time.Duration(attempt+1) * g.peerRetryBackoff)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return ByteView{}, ctx.Err()
				case <-timer.C:
				}
			}
			continue
		}
		break
	}
	return ByteView{}, lastErr
}

// loadFromPeer 在真正回本地数据源之前，先看看这个 key 是不是应该去别的 peer 节点取
func (g *Group) loadFromPeer(ctx context.Context, key string) (ByteView, error, bool) {
	// 先通过 key 判断从哪个节点来获取对应的 k-v
	peer, peerID, ok, self := g.pickPeer(key)

	if !ok {
		return ByteView{}, nil, false
	}
	if self {
		return ByteView{}, nil, false
	}
	// 如果熔断了就报错
	if !g.allowPeer(peerID) {
		log.Println("[GCache] Skip peer because circuit is open", peerID)
		return ByteView{}, nil, false
	}
	// 没有熔断就试图从 peer 节点取 key-val
	value, err := g.getFromPeer(ctx, peer, key)
	if err == nil {
		// 从 peer 节点加载出了 key
		g.stats.addPeerLoads(1)
		// 重置熔断状态
		g.onPeerSuccess(peerID)
		return value, nil, true
	}
	// 如果是 空值缓存 同样相当于缓存调用到了
	if errors.Is(err, ErrNotFound) {
		g.onPeerSuccess(peerID)
		// 如果 空值缓存时间 > 0 就要把空值也写进缓存
		if g.emptyTTL > 0 {
			// 把 key 设置 negative, 在 Now() + emptyTTL 后过期
			g.populateCacheEntry(key, cacheEntry{
				negative:  true,
				expiresAt: time.Now().Add(g.emptyTTL),
			})
		}
		return ByteView{}, ErrNotFound, true
	}
	// Group 直接找不到
	if errors.Is(err, ErrGroupNotFound) {
		g.stats.addPeerFailures(1)
		g.onPeerFailure(peerID)
		log.Println("[GCache] Peer is missing group", err)
		return ByteView{}, err, true
	}
	if errors.Is(err, ErrPeerViewMismatch) || errors.Is(err, ErrGroupClosed) {
		g.stats.addPeerFailures(1)
		g.onPeerFailure(peerID)
		log.Println("[GCache] Peer rejected request", err)
		return ByteView{}, nil, false
	}

	g.stats.addPeerFailures(1)
	g.onPeerFailure(peerID)
	log.Println("[GCache] Failed to get from peer", err)
	return ByteView{}, nil, false
}

// getLocally 真正执行本地回源并回填缓存的函数
func (g *Group) getLocally(ctx context.Context, key string) (ByteView, error) {
	if g.closed.Load() {
		return ByteView{}, ErrGroupClosed
	}
	g.stats.addLocalLoads(1)
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) && g.emptyTTL > 0 {
			g.populateCacheEntry(key, cacheEntry{
				negative:  true,
				expiresAt: time.Now().Add(g.emptyTTL),
			})
		}
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

// loadOnce 普通缓存读取在 miss 之后的一次完整取数策略
func (g *Group) loadOnce(ctx context.Context, key string) (ByteView, error) {
	if value, err, handled := g.loadFromPeer(ctx, key); handled {
		return value, err
	}
	return g.getLocally(ctx, key)
}

// GetContext 用 提供的 context 来获取 value 可以用于超时
func (g *Group) GetContext(ctx context.Context, key string) (ByteView, error) {
	return g.get(ctx, key, g.loader, g.loadOnce)
}

// getLocallyContext 从本地获取缓存，携带 ctx
func (g *Group) getLocallyContext(ctx context.Context, key string) (ByteView, error) {
	return g.get(ctx, key, g.localLoader, g.getLocally)
}
func (g *Group) startCleanup() {
	g.cleanupMu.Lock()
	defer g.cleanupMu.Unlock()
	if g.cleanupInterval <= 0 {
		return
	}
	if g.cleanupStop != nil {
		return
	}

	g.cleanupStop = make(chan struct{})
	g.cleanupDone = make(chan struct{})
	go func() {
		defer close(g.cleanupDone)
		ticker := time.NewTicker(g.cleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				expired := g.mainCache.cleanupExpired(time.Now())
				if expired > 0 {
					g.stats.addCacheExpirations(int64(expired))
				}
			case <-g.cleanupStop:
				return
			}
		}
	}()
}

func (g *Group) stopCleanup() {
	g.cleanupMu.Lock()
	defer g.cleanupMu.Unlock()
	if g.cleanupStop == nil {
		return
	}
	close(g.cleanupStop)
	<-g.cleanupDone
	g.cleanupStop = nil
	g.cleanupDone = nil
}
