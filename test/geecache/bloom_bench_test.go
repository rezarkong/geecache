package geecache_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"geecache"
)

const splitMix64Gamma = 0x9e3779b97f4a7c15

type splitMix64 struct {
	state uint64
}

func newSplitMix64(seed uint64) splitMix64 {
	return splitMix64{state: seed}
}

func (r *splitMix64) Next() uint64 {
	r.state += splitMix64Gamma
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func BenchmarkBloomFilterMightContainRandomMisses(b *testing.B) {
	items := benchmarkEnvInt("BENCH_BLOOM_ITEMS", 100_000)
	fpRate := benchmarkEnvFloat("BENCH_BLOOM_FP_RATE", 0.01)
	queryKeys := benchmarkBloomQueryKeys(items)

	filter, err := geecache.NewBloomFilter(items, fpRate)
	if err != nil {
		b.Fatalf("NewBloomFilter: %v", err)
	}

	for _, key := range benchmarkBloomKeys("exist-", items, 1) {
		filter.Add(key)
	}

	missKeys := benchmarkBloomKeys("miss-", queryKeys, 2)
	falsePositives := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := missKeys[i%len(missKeys)]
		if filter.MightContain(key) {
			falsePositives++
		}
	}
	b.StopTimer()

	falsePositiveRate := float64(falsePositives) / float64(b.N)
	rejectRate := 1 - falsePositiveRate
	b.ReportMetric(falsePositiveRate, "false_positive_rate")
	b.ReportMetric(rejectRate, "reject_rate")
}

func BenchmarkGroupGetBloomRejectRandomMisses(b *testing.B) {
	restore := silenceLogs()
	defer restore()

	items := benchmarkEnvInt("BENCH_BLOOM_ITEMS", 100_000)
	fpRate := benchmarkEnvFloat("BENCH_BLOOM_FP_RATE", 0.01)
	queryKeys := benchmarkBloomQueryKeys(items)

	filter, err := geecache.NewBloomFilter(items, fpRate)
	if err != nil {
		b.Fatalf("NewBloomFilter: %v", err)
	}

	for _, key := range benchmarkBloomKeys("exist-", items, 3) {
		filter.Add(key)
	}

	var localLoads int64
	group := geecache.NewGroupWithOptions(
		"benchmark-bloom-reject-random-miss",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, _ string) ([]byte, error) {
			atomic.AddInt64(&localLoads, 1)
			return nil, geecache.ErrNotFound
		}),
		geecache.WithBloomFilter(filter),
		geecache.WithBloomRejectOnMiss(),
	)
	defer group.Close()

	missKeys := benchmarkBloomKeys("miss-", queryKeys, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := missKeys[i%len(missKeys)]
		if _, err := group.Get(key); err != geecache.ErrNotFound {
			b.Fatalf("Get(%q): %v", key, err)
		}
	}
	b.StopTimer()

	stats := group.Stats()
	if stats.Requests == 0 {
		b.Fatal("expected benchmark requests to be recorded")
	}

	falsePositiveRate := float64(atomic.LoadInt64(&localLoads)) / float64(stats.Requests)
	rejectRate := float64(stats.BloomRejects) / float64(stats.Requests)
	b.ReportMetric(falsePositiveRate, "false_positive_rate")
	b.ReportMetric(rejectRate, "reject_rate")
}

func benchmarkEnvFloat(name string, fallback float64) float64 {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 || v >= 1 {
		return fallback
	}
	return v
}

func benchmarkBloomKeys(prefix string, count int, seed uint64) []string {
	keys := make([]string, count)
	rng := newSplitMix64(seed)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%016x", prefix, rng.Next())
	}
	return keys
}

func benchmarkBloomQueryKeys(items int) int {
	count := benchmarkEnvInt("BENCH_BLOOM_QUERY_KEYS", 1_000_000)
	if count < items {
		return items
	}
	return count
}
