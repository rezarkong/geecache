package algo

import "container/list"

type LRU struct {
	ll    *list.List
	nodes map[string]*list.Element
}

func NewLRU() *LRU {
	return &LRU{
		ll:    list.New(),
		nodes: make(map[string]*list.Element),
	}
}

func (lru *LRU) OnAdd(key string) {
	if ele, ok := lru.nodes[key]; ok {
		lru.ll.MoveToFront(ele)
		return
	}
	lru.nodes[key] = lru.ll.PushFront(key)
}

func (lru *LRU) OnAccess(key string) {
	if ele, ok := lru.nodes[key]; ok {
		lru.ll.MoveToFront(ele)
	}
}

func (lru *LRU) OnRemove(key string) {
	if ele, ok := lru.nodes[key]; ok {
		lru.ll.Remove(ele)
		delete(lru.nodes, key)
	}
}

func (lru *LRU) Evict() string {
	ele := lru.ll.Back()
	if ele == nil {
		return ""
	}
	key := ele.Value.(string)
	lru.ll.Remove(ele)
	delete(lru.nodes, key)
	return key
}
