package algo

import "container/list"

type lrukNode struct {
	key      string
	accesses int
	inCache  bool
	element  *list.Element
}

// LRUK is a two-queue approximation of LRU-K.
// Keys stay in history until they have been observed k times,
// then they are promoted into the main Cache queue and managed by LRU.
type LRUK struct {
	k       int
	nodes   map[string]*lrukNode
	cache   *list.List
	history *list.List
}

func NewLRUK(k int) *LRUK {
	if k <= 1 {
		k = 1
	}
	return &LRUK{
		k:       k,
		nodes:   make(map[string]*lrukNode),
		cache:   list.New(),
		history: list.New(),
	}
}

func (lru *LRUK) OnAdd(key string) {
	if _, ok := lru.nodes[key]; ok {
		lru.OnAccess(key)
		return
	}

	node := &lrukNode{key: key, accesses: 1}
	if lru.k == 1 {
		node.inCache = true
		node.element = lru.cache.PushFront(node)
	} else {
		node.element = lru.history.PushFront(node)
	}
	lru.nodes[key] = node
}

func (lru *LRUK) OnAccess(key string) {
	node, ok := lru.nodes[key]
	if !ok {
		return
	}

	node.accesses++
	if node.inCache {
		lru.cache.MoveToFront(node.element)
		return
	}

	if node.accesses >= lru.k {
		lru.history.Remove(node.element)
		node.inCache = true
		node.element = lru.cache.PushFront(node)
		return
	}

	lru.history.MoveToFront(node.element)
}

func (lru *LRUK) OnBurst(key string, n int) {
	node, ok := lru.nodes[key]
	if !ok || n <= 0 {
		return
	}
	if node.inCache {
		lru.cache.MoveToFront(node.element)
		return
	}

	needed := lru.k - node.accesses
	if needed <= 0 {
		lru.history.MoveToFront(node.element)
		return
	}
	if n < needed {
		node.accesses += n
		lru.history.MoveToFront(node.element)
		return
	}

	node.accesses += needed
	lru.history.Remove(node.element)
	node.inCache = true
	node.element = lru.cache.PushFront(node)
}

func (lru *LRUK) OnRemove(key string) {
	node, ok := lru.nodes[key]
	if !ok {
		return
	}
	if node.inCache {
		lru.cache.Remove(node.element)
	} else {
		lru.history.Remove(node.element)
	}
	delete(lru.nodes, key)
}

func (lru *LRUK) OnEvict(key string) {
	lru.OnRemove(key)
}

func (lru *LRUK) Evict() string {
	if lru.history.Len() > 0 {
		return lru.victimFrom(lru.history)
	}
	return lru.victimFrom(lru.cache)
}

func (lru *LRUK) victimFrom(q *list.List) string {
	ele := q.Back()
	if ele == nil {
		return ""
	}
	node := ele.Value.(*lrukNode)
	return node.key
}
