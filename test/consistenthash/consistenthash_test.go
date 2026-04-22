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
