package cluster

import (
	"context"
	"fmt"
	"geecache/consistenthash"
	pb "geecache/geecachepb"
	"sort"
	"strconv"
	"strings"
)

// PeerPicker locates the peer that owns a specific key.
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool, self bool)
	Close() error
}

// PeerGetter loads one value from a remote peer.
type PeerGetter interface {
	Get(ctx context.Context, in *pb.Request, out *pb.Response) error
}

// MutationRequest is the payload sent to cluster peers for cache mutations.
type MutationRequest struct {
	Group string `json:"group,omitempty"`
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

// MutationPeer supports remote cache mutations.
type MutationPeer interface {
	Set(ctx context.Context, req MutationRequest) error
	Delete(ctx context.Context, req MutationRequest) error
	Invalidate(ctx context.Context, req MutationRequest) error
}

// ManagedPeer is a concrete peer connection used by dynamic discovery modules.
type ManagedPeer interface {
	PeerGetter
	MutationPeer
	ID() string
	Close() error
}

// Member represents one logical peer plus its weight on the hash ring.
type Member struct {
	Addr   string
	Weight int
}

// NormalizeMember trims the address and ensures weight is always positive.
func NormalizeMember(member Member) Member {
	member.Addr = strings.TrimSpace(member.Addr)
	member.Weight = MaxWeight(member.Weight)
	return member
}

// MaxWeight normalizes invalid weights to the default weight of one.
func MaxWeight(weight int) int {
	if weight <= 0 {
		return 1
	}
	return weight
}

// ParseMemberSpec parses "addr@weight" peer specs used by static peer config.
func ParseMemberSpec(raw string) Member {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Member{}
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
	return NormalizeMember(Member{Addr: addr, Weight: weight})
}

// UniqueMembers de-duplicates members by address and returns them sorted.
func UniqueMembers(specs []string) []Member {
	byAddr := make(map[string]Member, len(specs))
	for _, spec := range specs {
		member := ParseMemberSpec(spec)
		if member.Addr == "" {
			continue
		}
		byAddr[member.Addr] = member
	}

	members := make([]Member, 0, len(byAddr))
	for _, member := range byAddr {
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].Addr < members[j].Addr
	})
	return members
}

// FormatMemberSpec renders one member as "addr" or "addr@weight".
func FormatMemberSpec(member Member) string {
	member = NormalizeMember(member)
	if member.Weight <= 1 {
		return member.Addr
	}
	return fmt.Sprintf("%s@%d", member.Addr, member.Weight)
}

// NormalizePeerView produces the canonical cluster membership string.
func NormalizePeerView(members []Member) string {
	if len(members) == 0 {
		return ""
	}
	items := make([]string, 0, len(members))
	for _, member := range members {
		member = NormalizeMember(member)
		if member.Addr == "" {
			continue
		}
		items = append(items, FormatMemberSpec(member))
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}

// ToHashMembers converts logical members into consistent-hash members.
func ToHashMembers(members []Member) []consistenthash.Member {
	out := make([]consistenthash.Member, 0, len(members))
	for _, member := range members {
		member = NormalizeMember(member)
		if member.Addr == "" {
			continue
		}
		out = append(out, consistenthash.Member{
			Node:   member.Addr,
			Weight: member.Weight,
		})
	}
	return out
}
