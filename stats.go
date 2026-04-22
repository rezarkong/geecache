package geecache

import "sync/atomic"

// StatsSnapshot is a point-in-time copy of group metrics.
type StatsSnapshot struct {
	Requests         int64   `json:"requests"`
	CacheHits        int64   `json:"cache_hits"`
	CacheMisses      int64   `json:"cache_misses"`
	PeerLoads        int64   `json:"peer_loads"`
	PeerFailures     int64   `json:"peer_failures"`
	LocalLoads       int64   `json:"local_loads"`
	EmptyHits        int64   `json:"empty_hits"`
	BloomRejects     int64   `json:"bloom_rejects"`
	CacheExpirations int64   `json:"cache_expirations"`
	HitRate          float64 `json:"hit_rate"`
}

type Stats struct {
	requests         int64
	cacheHits        int64
	cacheMisses      int64
	peerLoads        int64
	peerFailures     int64
	localLoads       int64
	emptyHits        int64
	bloomRejects     int64
	cacheExpirations int64
}

// 计算命中率
func (s *Stats) Snapshot() StatsSnapshot {
	requests := atomic.LoadInt64(&s.requests)
	cacheHits := atomic.LoadInt64(&s.cacheHits)

	var hitRate float64
	if requests > 0 {
		hitRate = float64(cacheHits) / float64(requests)
	}

	return StatsSnapshot{
		Requests:         requests,
		CacheHits:        cacheHits,
		CacheMisses:      atomic.LoadInt64(&s.cacheMisses),
		PeerLoads:        atomic.LoadInt64(&s.peerLoads),
		PeerFailures:     atomic.LoadInt64(&s.peerFailures),
		LocalLoads:       atomic.LoadInt64(&s.localLoads),
		EmptyHits:        atomic.LoadInt64(&s.emptyHits),
		BloomRejects:     atomic.LoadInt64(&s.bloomRejects),
		CacheExpirations: atomic.LoadInt64(&s.cacheExpirations),
		HitRate:          hitRate,
	}
}

func (s *Stats) addRequests(delta int64) {
	atomic.AddInt64(&s.requests, delta)
}

func (s *Stats) addCacheHits(delta int64) {
	atomic.AddInt64(&s.cacheHits, delta)
}

func (s *Stats) addCacheMisses(delta int64) {
	atomic.AddInt64(&s.cacheMisses, delta)
}

func (s *Stats) addPeerLoads(delta int64) {
	atomic.AddInt64(&s.peerLoads, delta)
}

func (s *Stats) addPeerFailures(delta int64) {
	atomic.AddInt64(&s.peerFailures, delta)
}

func (s *Stats) addLocalLoads(delta int64) {
	atomic.AddInt64(&s.localLoads, delta)
}

func (s *Stats) addEmptyHits(delta int64) {
	atomic.AddInt64(&s.emptyHits, delta)
}

func (s *Stats) addBloomRejects(delta int64) {
	atomic.AddInt64(&s.bloomRejects, delta)
}

func (s *Stats) addCacheExpirations(delta int64) {
	atomic.AddInt64(&s.cacheExpirations, delta)
}
