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
	"time"
)

// A Group is a cache namespace and associated data loaded spread over
type Group struct {
	name      string
	getter    Getter
	mainCache cache
	peers     PeerPicker
	// use singleflight.Group to make sure that
	// each key is only fetched once
	loader *singleflight.Group
	// peerRetries controls how many times a peer fetch is retried
	// before falling back to the local getter.
	peerRetries int
	// peerRetryBackoff sleeps between retry attempts.
	peerRetryBackoff time.Duration
	// emptyTTL caches ErrNotFound values for a short period.
	emptyTTL time.Duration
	// cacheTTL sets TTL for normal cache entries; cacheTTLJitter spreads expirations.
	cacheTTL       time.Duration
	cacheTTLJitter time.Duration
	// circuit breaker state for peer fetches.
	peerFailureThreshold int
	peerCircuitOpen      time.Duration
	circuitMu            sync.Mutex
	peerCircuits         map[string]*peerCircuitState
	stats                *Stats
}

type peerCircuitState struct {
	consecutiveFailures int
	openUntil           time.Time
}

// A Getter loads data for a key.
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get implements Getter interface function
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

// NewGroup create a new instance of Group
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	return NewGroupWithOptions(name, cacheBytes, getter)
}

// NewGroupWithOptions create a new instance of Group with extra behaviors.
func NewGroupWithOptions(name string, cacheBytes int64, getter Getter, opts ...Option) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:                 name,
		getter:               getter,
		mainCache:            cache{cacheBytes: cacheBytes},
		loader:               &singleflight.Group{},
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
	groups[name] = g
	return g
}

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get value for a key from cache
func (g *Group) Get(key string) (ByteView, error) {
	return g.GetContext(context.Background(), key)
}

// GetContext returns the value for a key using the provided context.
func (g *Group) GetContext(ctx context.Context, key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	g.stats.addRequests(1)

	if entry, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		g.stats.addCacheHits(1)
		if entry.negative {
			g.stats.addEmptyHits(1)
			return ByteView{}, ErrNotFound
		}
		return entry.value, nil
	}
	g.stats.addCacheMisses(1)

	return g.load(ctx, key)
}

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// Stats returns a snapshot of group metrics.
func (g *Group) Stats() StatsSnapshot {
	return g.stats.Snapshot()
}

func (g *Group) load(ctx context.Context, key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				peerID := identifyPeer(peer)
				if !g.allowPeer(peerID) {
					g.stats.addCircuitRejects(1)
					g.stats.addPeerFallbacks(1)
					log.Println("[GeeCache] Skip peer because circuit is open", peerID)
					return g.getLocally(ctx, key)
				}
				if value, err = g.getFromPeer(ctx, peer, key); err == nil {
					g.stats.addPeerLoads(1)
					g.onPeerSuccess(peerID)
					return value, nil
				}
				if errors.Is(err, ErrNotFound) {
					g.stats.addPeerNotFounds(1)
					g.onPeerSuccess(peerID)
					if g.emptyTTL > 0 {
						g.populateCacheEntry(key, cacheEntry{
							negative:  true,
							expiresAt: time.Now().Add(g.emptyTTL),
						})
					}
					return ByteView{}, ErrNotFound
				}
				g.stats.addPeerLoadFailures(1)
				g.stats.addPeerFallbacks(1)
				g.onPeerFailure(peerID)
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}

		return g.getLocally(ctx, key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

func (g *Group) populateCache(key string, value ByteView) {
	entry := cacheEntry{value: value}
	if g.cacheTTL > 0 {
		entry.expiresAt = time.Now().Add(g.cacheTTL + g.randomJitter())
	}
	g.populateCacheEntry(key, entry)
}

func (g *Group) populateCacheEntry(key string, entry cacheEntry) {
	g.mainCache.add(key, entry)
}

func (g *Group) getLocally(ctx context.Context, key string) (ByteView, error) {
	g.stats.addLocalLoads(1)
	start := time.Now()
	bytes, err := g.getter.Get(ctx, key)
	g.stats.addLocalLoadNanos(time.Since(start).Nanoseconds())
	if err != nil {
		g.stats.addLocalLoadErrors(1)
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

func (g *Group) getFromPeer(ctx context.Context, peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	var lastErr error
	for attempt := 0; attempt <= g.peerRetries; attempt++ {
		start := time.Now()
		res := &pb.Response{}
		err := peer.Get(ctx, req, res)
		elapsed := time.Since(start).Nanoseconds()
		g.stats.addPeerAttemptNanos(elapsed)
		g.stats.addPeerAttempts(1)
		if err == nil {
			g.stats.addPeerLoadNanos(elapsed)
			return ByteView{b: cloneBytes(res.Value)}, nil
		}
		g.stats.addPeerFailureNanos(elapsed)
		lastErr = err
		if attempt < g.peerRetries && isRetryableError(err) {
			g.stats.addPeerRetries(1)
			if g.peerRetryBackoff > 0 {
				time.Sleep(time.Duration(attempt+1) * g.peerRetryBackoff)
			}
			continue
		}
		break
	}
	return ByteView{}, lastErr
}

func (g *Group) randomJitter() time.Duration {
	if g.cacheTTLJitter <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(g.cacheTTLJitter)))
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

func (g *Group) allowPeer(peerID string) bool {
	g.circuitMu.Lock()
	defer g.circuitMu.Unlock()
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

func (g *Group) onPeerSuccess(peerID string) {
	g.circuitMu.Lock()
	defer g.circuitMu.Unlock()
	if state := g.peerCircuits[peerID]; state != nil {
		state.consecutiveFailures = 0
		state.openUntil = time.Time{}
	}
}

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
		g.stats.addCircuitOpenEvents(1)
	}
}
