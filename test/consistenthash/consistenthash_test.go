package consistenthash_test

import (
	"geecache/consistenthash"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {
	hash := consistenthash.New(3, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.Add("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

}

func TestWeightedMembersBiasOwnership(t *testing.T) {
	hash := consistenthash.New(1, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})

	hash.AddMembers(
		consistenthash.Member{Node: "2", Weight: 1},
		consistenthash.Member{Node: "4", Weight: 2},
	)

	testCases := map[string]string{
		"2":  "2",
		"3":  "4",
		"11": "4",
		"15": "2",
	}

	for k, v := range testCases {
		if got := hash.Get(k); got != v {
			t.Fatalf("weighted hash ask %s, got %s want %s", k, got, v)
		}
	}
}

func TestPositionsAndLocateExposeRingState(t *testing.T) {
	hash := consistenthash.New(1, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})

	hash.Add("6", "4", "2")

	positions := hash.Positions()
	if len(positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(positions))
	}

	wantPositions := []struct {
		hash        int
		node        string
		replica     int
		virtualNode string
	}{
		{2, "2", 0, "02"},
		{4, "4", 0, "04"},
		{6, "6", 0, "06"},
	}
	for i, want := range wantPositions {
		got := positions[i]
		if got.Hash != want.hash || got.Node != want.node || got.Replica != want.replica || got.VirtualNode != want.virtualNode {
			t.Fatalf("position[%d]=%+v, want hash=%d node=%s replica=%d virtual=%s", i, got, want.hash, want.node, want.replica, want.virtualNode)
		}
	}

	lookup := hash.Locate("5")
	if lookup.Hash != 5 {
		t.Fatalf("expected key hash 5, got %d", lookup.Hash)
	}
	if lookup.Owner != "6" || lookup.OwnerHash != 6 || lookup.OwnerReplica != 0 || lookup.OwnerVirtualNode != "06" || lookup.Wrapped {
		t.Fatalf("unexpected lookup result: %+v", lookup)
	}

	wrapped := hash.Locate("7")
	if wrapped.Hash != 7 || wrapped.Owner != "2" || wrapped.OwnerHash != 2 || !wrapped.Wrapped {
		t.Fatalf("expected wrapped lookup to land on node 2, got %+v", wrapped)
	}
}
