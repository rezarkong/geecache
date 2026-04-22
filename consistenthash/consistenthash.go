package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash 把 []byte 映射为 int32
type Hash func(data []byte) uint32

// Map 包含所有的哈希 keys
type Map struct {
	hash     Hash
	replicas int
	keys     []int // Sorted
	hashMap  map[int]string
}

// Member carries one physical node and its relative routing weight.
type Member struct {
	Node   string
	Weight int
}

// New creates a Map instance
// New 对每个新节点放 replicas 个虚拟节点, 放到哈希环上
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 先对业务key做哈希，然后在排好序的环上二分找到第一个大于它的位置，对应的那个节点就是负责这个 key 的节点
func (m *Map) Add(keys ...string) {
	members := make([]Member, 0, len(keys))
	for _, key := range keys {
		members = append(members, Member{Node: key, Weight: 1})
	}
	m.AddMembers(members...)
}

// AddMembers adds one or more weighted members to the hash ring.
func (m *Map) AddMembers(members ...Member) {
	for _, member := range members {
		if member.Node == "" {
			continue
		}
		weight := member.Weight
		if weight <= 0 {
			weight = 1
		}
		replicas := m.replicas * weight
		for i := 0; i < replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + member.Node)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = member.Node
		}
	}
	sort.Ints(m.keys)
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key)))
	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]]
}
