package algo

import "container/list"

type LFU struct {
	nodes map[string]*lfuNode
	freqs map[int]*list.Element
	order *list.List
}

type lfuNode struct {
	key     string
	freq    int
	element *list.Element
	bucket  *lfuBucket
}

type lfuBucket struct {
	freq    int
	entries *list.List
	element *list.Element
}

func NewLFU() *LFU {
	return &LFU{
		nodes: make(map[string]*lfuNode),
		freqs: make(map[int]*list.Element),
		order: list.New(),
	}
}

func (lfu *LFU) OnAdd(key string) {
	if node, ok := lfu.nodes[key]; ok {
		lfu.bump(node)
		return
	}

	bucket := lfu.ensureBucket(1, lfu.order.Front())
	node := &lfuNode{key: key, freq: 1, bucket: bucket}
	node.element = bucket.entries.PushFront(node)
	lfu.nodes[key] = node
}

func (lfu *LFU) OnAccess(key string) {
	node, ok := lfu.nodes[key]
	if !ok {
		return
	}
	lfu.bump(node)
}

func (lfu *LFU) OnBurst(key string, n int) {
	node, ok := lfu.nodes[key]
	if !ok || n <= 0 {
		return
	}
	for i := 0; i < n; i++ {
		lfu.bump(node)
	}
}

func (lfu *LFU) OnRemove(key string) {
	node, ok := lfu.nodes[key]
	if !ok {
		return
	}
	lfu.removeFromBucket(node)
	delete(lfu.nodes, key)
}

func (lfu *LFU) OnEvict(key string) {
	lfu.OnRemove(key)
}

func (lfu *LFU) Evict() string {
	front := lfu.order.Front()
	if front == nil {
		return ""
	}

	bucket := front.Value.(*lfuBucket)
	ele := bucket.entries.Back()
	if ele == nil {
		return ""
	}
	return ele.Value.(*lfuNode).key
}

func (lfu *LFU) bump(node *lfuNode) {
	current := node.bucket
	nextElem := current.element.Next()

	lfu.removeFromBucket(node)

	node.freq++
	nextBucket := lfu.ensureBucket(node.freq, nextElem)
	node.bucket = nextBucket
	node.element = nextBucket.entries.PushFront(node)
}

func (lfu *LFU) ensureBucket(freq int, before *list.Element) *lfuBucket {
	if elem, ok := lfu.freqs[freq]; ok {
		return elem.Value.(*lfuBucket)
	}

	bucket := &lfuBucket{
		freq:    freq,
		entries: list.New(),
	}
	if before != nil {
		bucket.element = lfu.order.InsertBefore(bucket, before)
	} else {
		bucket.element = lfu.order.PushBack(bucket)
	}
	lfu.freqs[freq] = bucket.element
	return bucket
}

func (lfu *LFU) removeFromBucket(node *lfuNode) {
	bucket := node.bucket
	if bucket == nil {
		return
	}

	bucket.entries.Remove(node.element)
	if bucket.entries.Len() == 0 {
		lfu.order.Remove(bucket.element)
		delete(lfu.freqs, bucket.freq)
	}

	node.bucket = nil
	node.element = nil
}
