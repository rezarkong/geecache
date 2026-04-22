package geecache

import (
	"fmt"
	"geecache/consistenthash"
	"sort"
	"strconv"
	"strings"
)

type weightedPeer struct {
	Addr   string
	Weight int
}

func parsePeerSpec(raw string) weightedPeer {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return weightedPeer{}
	}

	addr := raw
	weight := 1
	if at := strings.LastIndex(raw, "@"); at > 0 && at < len(raw)-1 {
		if parsed, err := strconv.Atoi(strings.TrimSpace(raw[at+1:])); err == nil {
			addr = strings.TrimSpace(raw[:at])
			if parsed > 0 {
				weight = parsed
			}
		}
	}
	return weightedPeer{Addr: addr, Weight: weight}
}

func uniqueWeightedPeers(specs []string) []weightedPeer {
	byAddr := make(map[string]weightedPeer, len(specs))
	for _, spec := range specs {
		peer := parsePeerSpec(spec)
		if peer.Addr == "" {
			continue
		}
		byAddr[peer.Addr] = peer
	}
	peers := make([]weightedPeer, 0, len(byAddr))
	for _, peer := range byAddr {
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Addr < peers[j].Addr
	})
	return peers
}

func formatPeerSpec(peer weightedPeer) string {
	if peer.Weight <= 1 {
		return peer.Addr
	}
	return fmt.Sprintf("%s@%d", peer.Addr, peer.Weight)
}

func normalizePeerView(peers []weightedPeer) string {
	if len(peers) == 0 {
		return ""
	}
	items := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer.Addr == "" {
			continue
		}
		items = append(items, formatPeerSpec(peer))
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}

func toHashMembers(peers []weightedPeer) []consistenthash.Member {
	members := make([]consistenthash.Member, 0, len(peers))
	for _, peer := range peers {
		if peer.Addr == "" {
			continue
		}
		members = append(members, consistenthash.Member{
			Node:   peer.Addr,
			Weight: peer.Weight,
		})
	}
	return members
}
