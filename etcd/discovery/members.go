package discovery

import (
	"strings"

	"geecache/cluster"
	"geecache/consistenthash"
	"geecache/etcd/registry"
	"geecache/internal/logx"

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
		logx.Event("etcd.discovery", "watch_event", map[string]interface{}{
			"member":  member.Addr,
			"node":    p.self,
			"service": p.serviceName,
			"type":    event.Type.String(),
			"weight":  member.Weight,
		})
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
	oldMembers := make(map[string]cluster.Member, len(p.members))
	for addr, member := range p.members {
		oldMembers[addr] = member
	}
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
	peerView := p.peerView
	p.mu.Unlock()

	for _, getter := range removed {
		_ = getter.Close()
	}

	for addr, member := range unique {
		if addr == p.self {
			continue
		}
		old, existed := oldMembers[addr]
		switch {
		case !existed:
			logx.Event("cluster", "peer_joined", map[string]interface{}{
				"member":    member.Addr,
				"node":      p.self,
				"peer_view": peerView,
				"service":   p.serviceName,
				"weight":    member.Weight,
			})
		case old.Weight != member.Weight:
			logx.Event("cluster", "peer_updated", map[string]interface{}{
				"member":     member.Addr,
				"node":       p.self,
				"old_weight": old.Weight,
				"peer_view":  peerView,
				"service":    p.serviceName,
				"weight":     member.Weight,
			})
		}
	}
	for addr, member := range oldMembers {
		if addr == p.self {
			continue
		}
		if _, ok := unique[addr]; !ok {
			logx.Event("cluster", "peer_left", map[string]interface{}{
				"member":    member.Addr,
				"node":      p.self,
				"peer_view": peerView,
				"service":   p.serviceName,
				"weight":    member.Weight,
			})
		}
	}
	logx.Event("cluster", "ring_rebuilt", map[string]interface{}{
		"member_count": len(unique),
		"node":         p.self,
		"peer_view":    peerView,
		"service":      p.serviceName,
	})
}

func parseRegistryMember(service, key, raw string) (cluster.Member, bool) {
	record, ok := registry.ParseMember(service, key, raw)
	if !ok {
		return cluster.Member{}, false
	}
	return cluster.Member{Addr: record.Addr, Weight: record.Weight}, true
}
