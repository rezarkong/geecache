package discovery

import (
	"strings"

	"geecache/cluster"
	"geecache/consistenthash"
	"geecache/etcd/registry"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (p *Picker) handleWatchEvents(events []*clientv3.Event) {
	memberSet := make(map[string]cluster.Member, len(p.members)+1)
	p.mu.RLock()
	for addr, member := range p.members {
		memberSet[addr] = member
	}
	p.mu.RUnlock()
	memberSet[p.self] = cluster.Member{Addr: p.self, Weight: p.selfWeight}

	for _, event := range events {
		member, _ := parseRegistryMember(p.serviceName, string(event.Kv.Key), string(event.Kv.Value))
		if member.Addr == "" || member.Addr == p.self {
			continue
		}
		switch event.Type {
		case clientv3.EventTypePut:
			memberSet[member.Addr] = member
		case clientv3.EventTypeDelete:
			delete(memberSet, member.Addr)
		}
	}

	members := make([]cluster.Member, 0, len(memberSet))
	for _, member := range memberSet {
		members = append(members, member)
	}
	p.applyMembers(members)
}

func (p *Picker) applyMembers(members []cluster.Member) {
	unique := make(map[string]cluster.Member, len(members)+1)
	unique[p.self] = cluster.Member{Addr: p.self, Weight: p.selfWeight}
	for _, member := range members {
		member.Addr = strings.TrimSpace(member.Addr)
		member.Weight = cluster.MaxWeight(member.Weight)
		if member.Addr == "" {
			continue
		}
		unique[member.Addr] = member
	}

	allMembers := make([]cluster.Member, 0, len(unique))
	for _, member := range unique {
		allMembers = append(allMembers, member)
	}

	ring := consistenthash.New(defaultReplicas, nil)
	ring.AddMembers(cluster.ToHashMembers(allMembers)...)

	p.mu.Lock()
	oldGetters := p.getters
	nextGetters := make(map[string]cluster.ManagedPeer, len(unique))
	for _, member := range allMembers {
		if member.Addr == p.self {
			continue
		}
		if getter, ok := oldGetters[member.Addr]; ok {
			nextGetters[member.Addr] = getter
			continue
		}
		nextGetters[member.Addr] = p.newPeer(member.Addr, p.CurrentPeerView)
	}

	removed := make([]cluster.ManagedPeer, 0, len(oldGetters))
	for member, getter := range oldGetters {
		if _, ok := nextGetters[member]; !ok {
			removed = append(removed, getter)
		}
	}

	p.members = unique
	p.peers = ring
	p.getters = nextGetters
	p.peerView = cluster.NormalizePeerView(allMembers)
	p.mu.Unlock()

	for _, getter := range removed {
		_ = getter.Close()
	}
}

func parseRegistryMember(service, key, raw string) (cluster.Member, bool) {
	record, ok := registry.ParseMember(service, key, raw)
	if !ok {
		return cluster.Member{}, false
	}
	return cluster.Member{Addr: record.Addr, Weight: record.Weight}, true
}
