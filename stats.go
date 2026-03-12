package geecache

import "sync/atomic"

// StatsSnapshot is a point-in-time copy of group metrics.
type StatsSnapshot struct {
	Requests              int64   `json:"requests"`
	CacheHits             int64   `json:"cache_hits"`
	CacheMisses           int64   `json:"cache_misses"`
	PeerLoads             int64   `json:"peer_loads"`
	PeerNotFounds         int64   `json:"peer_not_founds"`
	PeerAttempts          int64   `json:"peer_attempts"`
	PeerLoadFailures      int64   `json:"peer_load_failures"`
	PeerRetries           int64   `json:"peer_retries"`
	PeerFallbacks         int64   `json:"peer_fallbacks"`
	CircuitOpenEvents     int64   `json:"circuit_open_events"`
	CircuitRejects        int64   `json:"circuit_rejects"`
	LocalLoads            int64   `json:"local_loads"`
	LocalLoadErrors       int64   `json:"local_load_errors"`
	EmptyHits             int64   `json:"empty_hits"`
	CacheExpirations      int64   `json:"cache_expirations"`
	PeerAttemptTotalNanos int64   `json:"peer_attempt_total_nanos"`
	PeerLoadTotalNanos    int64   `json:"peer_load_total_nanos"`
	PeerFailureTotalNanos int64   `json:"peer_failure_total_nanos"`
	LocalLoadTotalNanos   int64   `json:"local_load_total_nanos"`
	HitRate               float64 `json:"hit_rate"`
	AvgPeerAttemptNanos   int64   `json:"avg_peer_attempt_nanos"`
	AvgPeerLoadNanos      int64   `json:"avg_peer_load_nanos"`
	AvgPeerFailureNanos   int64   `json:"avg_peer_failure_nanos"`
	AvgLocalLoadNanos     int64   `json:"avg_local_load_nanos"`
}

type Stats struct {
	requests              int64
	cacheHits             int64
	cacheMisses           int64
	peerLoads             int64
	peerNotFounds         int64
	peerAttempts          int64
	peerLoadFailures      int64
	peerRetries           int64
	peerFallbacks         int64
	circuitOpenEvents     int64
	circuitRejects        int64
	localLoads            int64
	localLoadErrors       int64
	emptyHits             int64
	cacheExpirations      int64
	peerAttemptTotalNanos int64
	peerLoadTotalNanos    int64
	peerFailureTotalNanos int64
	localLoadTotalNanos   int64
}

func (s *Stats) Snapshot() StatsSnapshot {
	requests := atomic.LoadInt64(&s.requests)
	cacheHits := atomic.LoadInt64(&s.cacheHits)
	peerAttempts := atomic.LoadInt64(&s.peerAttempts)
	peerLoads := atomic.LoadInt64(&s.peerLoads)
	peerNotFounds := atomic.LoadInt64(&s.peerNotFounds)
	peerLoadFailures := atomic.LoadInt64(&s.peerLoadFailures)
	localLoads := atomic.LoadInt64(&s.localLoads)
	peerAttemptTotal := atomic.LoadInt64(&s.peerAttemptTotalNanos)
	peerLoadTotal := atomic.LoadInt64(&s.peerLoadTotalNanos)
	peerFailureTotal := atomic.LoadInt64(&s.peerFailureTotalNanos)
	localLoadTotal := atomic.LoadInt64(&s.localLoadTotalNanos)

	var hitRate float64
	if requests > 0 {
		hitRate = float64(cacheHits) / float64(requests)
	}

	var avgPeerAttempt int64
	if peerAttempts > 0 {
		avgPeerAttempt = peerAttemptTotal / peerAttempts
	}

	var avgPeerLoad int64
	if peerLoads > 0 {
		avgPeerLoad = peerLoadTotal / peerLoads
	}

	var avgPeerFailure int64
	if peerLoadFailures > 0 {
		avgPeerFailure = peerFailureTotal / peerLoadFailures
	}

	var avgLocalLoad int64
	if localLoads > 0 {
		avgLocalLoad = localLoadTotal / localLoads
	}

	return StatsSnapshot{
		Requests:              requests,
		CacheHits:             cacheHits,
		CacheMisses:           atomic.LoadInt64(&s.cacheMisses),
		PeerLoads:             peerLoads,
		PeerNotFounds:         peerNotFounds,
		PeerAttempts:          peerAttempts,
		PeerLoadFailures:      peerLoadFailures,
		PeerRetries:           atomic.LoadInt64(&s.peerRetries),
		PeerFallbacks:         atomic.LoadInt64(&s.peerFallbacks),
		CircuitOpenEvents:     atomic.LoadInt64(&s.circuitOpenEvents),
		CircuitRejects:        atomic.LoadInt64(&s.circuitRejects),
		LocalLoads:            localLoads,
		LocalLoadErrors:       atomic.LoadInt64(&s.localLoadErrors),
		EmptyHits:             atomic.LoadInt64(&s.emptyHits),
		CacheExpirations:      atomic.LoadInt64(&s.cacheExpirations),
		PeerAttemptTotalNanos: peerAttemptTotal,
		PeerLoadTotalNanos:    peerLoadTotal,
		PeerFailureTotalNanos: peerFailureTotal,
		LocalLoadTotalNanos:   localLoadTotal,
		HitRate:               hitRate,
		AvgPeerAttemptNanos:   avgPeerAttempt,
		AvgPeerLoadNanos:      avgPeerLoad,
		AvgPeerFailureNanos:   avgPeerFailure,
		AvgLocalLoadNanos:     avgLocalLoad,
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

func (s *Stats) addPeerNotFounds(delta int64) {
	atomic.AddInt64(&s.peerNotFounds, delta)
}

func (s *Stats) addPeerAttempts(delta int64) {
	atomic.AddInt64(&s.peerAttempts, delta)
}

func (s *Stats) addPeerLoadFailures(delta int64) {
	atomic.AddInt64(&s.peerLoadFailures, delta)
}

func (s *Stats) addPeerRetries(delta int64) {
	atomic.AddInt64(&s.peerRetries, delta)
}

func (s *Stats) addPeerFallbacks(delta int64) {
	atomic.AddInt64(&s.peerFallbacks, delta)
}

func (s *Stats) addCircuitOpenEvents(delta int64) {
	atomic.AddInt64(&s.circuitOpenEvents, delta)
}

func (s *Stats) addCircuitRejects(delta int64) {
	atomic.AddInt64(&s.circuitRejects, delta)
}

func (s *Stats) addLocalLoads(delta int64) {
	atomic.AddInt64(&s.localLoads, delta)
}

func (s *Stats) addLocalLoadErrors(delta int64) {
	atomic.AddInt64(&s.localLoadErrors, delta)
}

func (s *Stats) addEmptyHits(delta int64) {
	atomic.AddInt64(&s.emptyHits, delta)
}

func (s *Stats) addCacheExpirations(delta int64) {
	atomic.AddInt64(&s.cacheExpirations, delta)
}

func (s *Stats) addPeerAttemptNanos(delta int64) {
	atomic.AddInt64(&s.peerAttemptTotalNanos, delta)
}

func (s *Stats) addPeerLoadNanos(delta int64) {
	atomic.AddInt64(&s.peerLoadTotalNanos, delta)
}

func (s *Stats) addPeerFailureNanos(delta int64) {
	atomic.AddInt64(&s.peerFailureTotalNanos, delta)
}

func (s *Stats) addLocalLoadNanos(delta int64) {
	atomic.AddInt64(&s.localLoadTotalNanos, delta)
}
