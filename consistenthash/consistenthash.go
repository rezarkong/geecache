package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash 把 []byte 映射为 int32
type Hash func(data []byte) uint32

// RingPosition describes one virtual node placement on the hash ring.
type RingPosition struct {
	Hash        int    `json:"hash"`
	Node        string `json:"node"`
	Replica     int    `json:"replica"`
	VirtualNode string `json:"virtual_node"`
}

// LookupResult describes where a key lands on the hash ring.
type LookupResult struct {
	Key              string `json:"key"`
	Hash             int    `json:"hash"`
	Owner            string `json:"owner"`
	OwnerHash        int    `json:"owner_hash"`
	OwnerReplica     int    `json:"owner_replica"`
	OwnerVirtualNode string `json:"owner_virtual_node"`
	Wrapped          bool   `json:"wrapped"`
}

// Map 包含所有的哈希 keys
type Map struct {
	hash        Hash
	replicas    int
	keys        []int // Sorted
	hashMap     map[int]string
	positionMap map[int]RingPosition
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
		replicas:    replicas,
		hash:        fn,
		hashMap:     make(map[int]string),
		positionMap: make(map[int]RingPosition),
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
			virtualNode := strconv.Itoa(i) + member.Node
			hash := int(m.hash([]byte(virtualNode)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = member.Node
			m.positionMap[hash] = RingPosition{
				Hash:        hash,
				Node:        member.Node,
				Replica:     i,
				VirtualNode: virtualNode,
			}
		}
	}
	sort.Ints(m.keys)
}

// HashKey hashes one business key into the same space as the ring.
func (m *Map) HashKey(key string) int {
	if m == nil || m.hash == nil {
		return 0
	}
	return int(m.hash([]byte(key)))
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}

	hash := m.HashKey(key)
	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]]
}

// Positions returns the current virtual node placements in ring order.
func (m *Map) Positions() []RingPosition {
	if m == nil || len(m.keys) == 0 {
		return nil
	}
	positions := make([]RingPosition, 0, len(m.keys))
	for _, hash := range m.keys {
		position, ok := m.positionMap[hash]
		if !ok {
			position = RingPosition{
				Hash: hash,
				Node: m.hashMap[hash],
			}
		}
		positions = append(positions, position)
	}
	return positions
}

// Locate returns the hash location for a key together with the selected owner.
func (m *Map) Locate(key string) LookupResult {
	result := LookupResult{
		Key:  key,
		Hash: m.HashKey(key),
	}
	if m == nil || len(m.keys) == 0 {
		return result
	}

	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= result.Hash
	})
	result.Wrapped = idx == len(m.keys)

	ownerHash := m.keys[idx%len(m.keys)]
	position, ok := m.positionMap[ownerHash]
	if !ok {
		position = RingPosition{
			Hash: ownerHash,
			Node: m.hashMap[ownerHash],
		}
	}

	result.Owner = position.Node
	result.OwnerHash = position.Hash
	result.OwnerReplica = position.Replica
	result.OwnerVirtualNode = position.VirtualNode
	return result
}
