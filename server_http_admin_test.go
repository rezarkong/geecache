package geecache

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminHashRingReportsVirtualNodePositions(t *testing.T) {
	group := NewGroup("admin-hash-ring", 2<<10, GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	}))
	defer group.Close()

	pool := NewGRPCPool("self")
	pool.Set("node-a", "node-b")
	defer pool.Close()

	server := httptest.NewServer(newServerMux(group, pool))
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/admin/hash/ring")
	if err != nil {
		t.Fatalf("GET /admin/hash/ring: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload hashRingPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode ring payload: %v", err)
	}

	if len(payload.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(payload.Peers))
	}
	if len(payload.Positions) != defaultReplicas*2 {
		t.Fatalf("expected %d positions, got %d", defaultReplicas*2, len(payload.Positions))
	}
	for i := 1; i < len(payload.Positions); i++ {
		if payload.Positions[i-1].Hash > payload.Positions[i].Hash {
			t.Fatalf("positions are not sorted at %d: %d > %d", i, payload.Positions[i-1].Hash, payload.Positions[i].Hash)
		}
	}
}

func TestAdminHashKeyReportsKeyLocation(t *testing.T) {
	group := NewGroup("admin-hash-key", 2<<10, GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	}))
	defer group.Close()

	pool := NewGRPCPool("self")
	pool.Set("node-a", "node-b")
	defer pool.Close()

	server := httptest.NewServer(newServerMux(group, pool))
	defer server.Close()

	expected := pool.LocateKey("Tom")
	resp, err := server.Client().Get(server.URL + "/admin/hash/key?key=Tom")
	if err != nil {
		t.Fatalf("GET /admin/hash/key: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload hashLookupPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode lookup payload: %v", err)
	}

	if payload.Lookup != expected {
		t.Fatalf("lookup=%+v, want %+v", payload.Lookup, expected)
	}
}

func TestAdminHashKeyRequiresKey(t *testing.T) {
	group := NewGroup("admin-hash-key-required", 2<<10, GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	}))
	defer group.Close()

	pool := NewGRPCPool("self")
	pool.Set("node-a")
	defer pool.Close()

	server := httptest.NewServer(newServerMux(group, pool))
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/admin/hash/key")
	if err != nil {
		t.Fatalf("GET /admin/hash/key without key: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestAdminPeersPrettyPrintsOnePeerPerLine(t *testing.T) {
	group := NewGroup("admin-peers-lines", 2<<10, GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	}))
	defer group.Close()

	pool := NewGRPCPool("self")
	pool.Set("node-a", "node-b")
	defer pool.Close()

	server := httptest.NewServer(newServerMux(group, pool))
	defer server.Close()

	resp, err := server.Client().Get(server.URL + "/admin/peers")
	if err != nil {
		t.Fatalf("GET /admin/peers: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /admin/peers body: %v", err)
	}

	got := string(body)
	for _, want := range []string{"\n    \"node-a\"", "\n    \"node-b\""} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected pretty-printed peer line %q in body:\n%s", want, got)
		}
	}
}
