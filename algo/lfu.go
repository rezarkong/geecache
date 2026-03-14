package algo

import "container/list"

type LFU struct {
	nodes   map[string]*lfuNode
	freqs   map[int]*list.List
	minFreq int
}

type lfuNode struct {
	key     string
	freq    int
	element *list.Element
}

func NewLFU() *LFU {
	return &LFU{
		nodes: make(map[string]*lfuNode),
		freqs: make(map[int]*list.List),
	}
}

func (lfu *LFU) OnAdd(key string) {
	if node, ok := lfu.nodes[key]; ok {
		lfu.bump(node) // 往后面挪
		return
	}
	bucket := lfu.bucket(1)
	node := &lfuNode{key: key, freq: 1}
	node.element = bucket.PushFront(node)
	lfu.nodes[key] = node
	lfu.minFreq = 1
}

func (lfu *LFU) OnAccess(key string) {
	node, ok := lfu.nodes[key]
	if !ok {
		return
	}
	lfu.bump(node)
}

func (lfu *LFU) OnRemove(key string) {
	node, ok := lfu.nodes[key]
	if !ok {
		return
	}
	lfu.removeNode(node)
}

func (lfu *LFU) OnEvict(key string) {
	lfu.OnRemove(key)
}

func (lfu *LFU) Evict() string {
	if lfu.minFreq == 0 {
		return ""
	}
	bucket := lfu.freqs[lfu.minFreq]
	if bucket == nil {
		return ""
	}
	ele := bucket.Back()
	if ele == nil {
		return ""
	}
	return ele.Value.(*lfuNode).key
}

func (lfu *LFU) bump(node *lfuNode) {
	oldFreq := node.freq
	oldBucket := lfu.freqs[oldFreq]
	oldBucket.Remove(node.element)
	if oldBucket.Len() == 0 {
		delete(lfu.freqs, oldFreq)
		if lfu.minFreq == oldFreq {
			lfu.minFreq++
		}
	}

	node.freq++
	newBucket := lfu.bucket(node.freq)
	node.element = newBucket.PushFront(node)
}

func (lfu *LFU) removeNode(node *lfuNode) {
	bucket := lfu.freqs[node.freq]
	if bucket != nil {
		bucket.Remove(node.element)
		if bucket.Len() == 0 {
			delete(lfu.freqs, node.freq)
			if lfu.minFreq == node.freq {
				lfu.recomputeMinFreq()
			}
		}
	}
	delete(lfu.nodes, node.key)
}

func (lfu *LFU) recomputeMinFreq() {
	lfu.minFreq = 0
	for freq := range lfu.freqs {
		if lfu.minFreq == 0 || freq < lfu.minFreq {
			lfu.minFreq = freq
		}
	}
}

func (lfu *LFU) bucket(freq int) *list.List {
	bucket, ok := lfu.freqs[freq]
	if !ok {
		bucket = list.New()
		lfu.freqs[freq] = bucket
	}
	return bucket
}
