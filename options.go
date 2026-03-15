package geecache

import (
	"geecache/algo"
	"time"
)

type Option func(*Group)

// WithEvictor selects the eviction algorithm for the main cache.
func WithEvictor(factory func() algo.Evictor) Option {
	return func(g *Group) {
		if factory != nil {
			g.mainCache.newEvictor = factory
		}
	}
}

// WithShards configures how many shards the main cache should use.
func WithShards(count int) Option {
	return func(g *Group) {
		if count > 0 {
			g.mainCache.shardCount = count
		}
	}
}

// WithCacheTTL sets TTL for normal cache entries and optional positive jitter.
func WithCacheTTL(ttl, jitter time.Duration) Option {
	return func(g *Group) {
		if ttl > 0 {
			g.cacheTTL = ttl
		}
		if jitter > 0 {
			g.cacheTTLJitter = jitter
		}
	}
}

// WithPeerRetries retries peer fetches before falling back locally.
func WithPeerRetries(retries int) Option {
	return func(g *Group) {
		if retries >= 0 {
			g.peerRetries = retries
		}
	}
}

// WithPeerRetryBackoff sets the base retry backoff duration between peer fetch attempts.
func WithPeerRetryBackoff(backoff time.Duration) Option {
	return func(g *Group) {
		if backoff >= 0 {
			g.peerRetryBackoff = backoff
		}
	}
}

// WithEmptyCache caches ErrNotFound responses for a short TTL to reduce penetration.
func WithEmptyCache(ttl time.Duration) Option {
	return func(g *Group) {
		if ttl > 0 {
			g.emptyTTL = ttl
		}
	}
}

// WithPeerCircuitBreaker configures consecutive failures and open duration.
func WithPeerCircuitBreaker(threshold int, open time.Duration) Option {
	return func(g *Group) {
		if threshold > 0 {
			g.peerFailureThreshold = threshold
		}
		if open > 0 {
			g.peerCircuitOpen = open
		}
	}
}
