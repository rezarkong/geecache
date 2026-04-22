package geecache

import (
	"fmt"
	"hash/fnv"
	"math"
	"sync/atomic"
)

// BloomFilter reports whether a key may exist and records observed keys.
type BloomFilter interface {
	Add(key string)
	MightContain(key string) bool
}

// StandardBloomFilter is a lock-free bloom filter implementation backed by a bitset.
type StandardBloomFilter struct {
	words []uint64
	m     uint64
	k     uint64
}

// NewBloomFilter builds a bloom filter from the expected item count and target false-positive rate.
func NewBloomFilter(expectedItems int, falsePositiveRate float64) (*StandardBloomFilter, error) {
	if expectedItems <= 0 {
		return nil, fmt.Errorf("expected items must be > 0")
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return nil, fmt.Errorf("false positive rate must be between 0 and 1")
	}

	m := uint64(math.Ceil(-(float64(expectedItems) * math.Log(falsePositiveRate)) / (math.Ln2 * math.Ln2)))
	if m == 0 {
		return nil, fmt.Errorf("bloom filter bit size must be > 0")
	}

	k := uint64(math.Ceil((float64(m) / float64(expectedItems)) * math.Ln2))
	if k == 0 {
		k = 1
	}

	return &StandardBloomFilter{
		words: make([]uint64, (m+63)/64),
		m:     m,
		k:     k,
	}, nil
}

// MustNewBloomFilter panics when bloom-filter parameters are invalid.
func MustNewBloomFilter(expectedItems int, falsePositiveRate float64) *StandardBloomFilter {
	filter, err := NewBloomFilter(expectedItems, falsePositiveRate)
	if err != nil {
		panic(err)
	}
	return filter
}

// Add records one key in the bloom filter.
func (f *StandardBloomFilter) Add(key string) {
	if f == nil || key == "" {
		return
	}
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < f.k; i++ {
		idx := (h1 + i*h2) % f.m
		word := &f.words[idx/64]
		mask := uint64(1) << (idx % 64)
		for {
			current := atomic.LoadUint64(word)
			if current&mask != 0 {
				break
			}
			if atomic.CompareAndSwapUint64(word, current, current|mask) {
				break
			}
		}
	}
}

// MightContain returns false only when the key is definitely absent.
func (f *StandardBloomFilter) MightContain(key string) bool {
	if f == nil || key == "" {
		return false
	}
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < f.k; i++ {
		idx := (h1 + i*h2) % f.m
		mask := uint64(1) << (idx % 64)
		if atomic.LoadUint64(&f.words[idx/64])&mask == 0 {
			return false
		}
	}
	return true
}

func bloomHashes(key string) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(key))
	sum1 := h1.Sum64()

	h2 := fnv.New64()
	_, _ = h2.Write([]byte(key))
	sum2 := h2.Sum64()
	if sum2 == 0 {
		sum2 = 0x9e3779b97f4a7c15
	}
	return sum1, sum2
}
