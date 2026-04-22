package geecache

import "geecache/algo"

// Store abstracts the local cache backend used by Group.
type Store interface {
	Add(key string, value algo.Value)
	GetOrRemoveIf(key string, predicate func(algo.Value) bool) (value algo.Value, ok bool, removed bool)
	CompensateBurstIf(key string, n int, predicate func(algo.Value) bool) (ok bool, removed bool)
	Remove(key string)
	RemoveIf(key string, predicate func(algo.Value) bool) bool
	Keys() []string
	Len() int
}

// StoreFactory creates one shard-local store instance.
type StoreFactory func(maxBytes int64, evictor algo.Evictor, onEvicted func(key string, value algo.Value)) Store

type algoStore struct {
	cache *algo.Cache
}

func newAlgoStore(maxBytes int64, evictor algo.Evictor, onEvicted func(key string, value algo.Value)) Store {
	return &algoStore{cache: algo.New(maxBytes, evictor, onEvicted)}
}

func (s *algoStore) Add(key string, value algo.Value) {
	s.cache.Add(key, value)
}

func (s *algoStore) GetOrRemoveIf(key string, predicate func(algo.Value) bool) (value algo.Value, ok bool, removed bool) {
	return s.cache.GetOrRemoveIf(key, predicate)
}

func (s *algoStore) CompensateBurstIf(key string, n int, predicate func(algo.Value) bool) (ok bool, removed bool) {
	return s.cache.CompensateBurstIf(key, n, predicate)
}

func (s *algoStore) Remove(key string) {
	s.cache.Remove(key)
}

func (s *algoStore) RemoveIf(key string, predicate func(algo.Value) bool) bool {
	return s.cache.RemoveIf(key, predicate)
}

func (s *algoStore) Keys() []string {
	return s.cache.Keys()
}

func (s *algoStore) Len() int {
	return s.cache.Len()
}
