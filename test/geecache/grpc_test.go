package geecache_test

import (
	"context"
	"encoding/json"
	"fmt"
	"geecache"
	pb "geecache/geecachepb"
	"net"
	"reflect"
	"sort"
	"testing"

	"google.golang.org/grpc"
)

func startTestGRPCServer(t *testing.T, groupName string, getter geecache.Getter) (string, func()) {
	t.Helper()

	geecache.NewGroup(groupName, 2<<10, getter)
	pool := geecache.NewGRPCPool("self")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	pool.Register(server)
	go func() {
		_ = server.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		server.Stop()
		_ = lis.Close()
	}
}

func TestGRPCGetterFetchesFromPeer(t *testing.T) {
	groupName := "grpc-fetch"
	addr, cleanup := startTestGRPCServer(t, groupName, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("remote-" + key), nil
	}))
	defer cleanup()

	pool := geecache.NewGRPCPool("self")
	pool.Set(addr)
	getter, ok := pool.PickPeer("Tom")
	if !ok {
		t.Fatal("expected peer getter")
	}
	var out pb.Response
	if err := getter.Get(context.Background(), &pb.Request{Group: groupName, Key: "Tom"}, &out); err != nil {
		t.Fatalf("getter.Get: %v", err)
	}
	if got := string(out.Value); got != "remote-Tom" {
		t.Fatalf("unexpected value %q", got)
	}
}

func TestGRPCPoolPickPeerBeforeSet(t *testing.T) {
	pool := geecache.NewGRPCPool("self")
	if peer, ok := pool.PickPeer("Tom"); ok || peer != nil {
		t.Fatalf("expected no peer before Set")
	}
}

func TestGRPCPoolPeersReflectLatestSet(t *testing.T) {
	pool := geecache.NewGRPCPool("self")
	pool.Set("node1", "node2")
	peers := pool.Peers()
	sort.Strings(peers)

	expect := []string{"node1", "node2"}
	if !reflect.DeepEqual(expect, peers) {
		t.Fatalf("expected peers %v, got %v", expect, peers)
	}

	pool.Set("node3")
	peers = pool.Peers()
	if len(peers) != 1 || peers[0] != "node3" {
		t.Fatalf("expected updated peers, got %v", peers)
	}
}

func TestGRPCGetterPropagatesPeerError(t *testing.T) {
	groupName := "grpc-fetch-miss"
	addr, cleanup := startTestGRPCServer(t, groupName, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return nil, fmt.Errorf("missing %s", key)
	}))
	defer cleanup()

	pool := geecache.NewGRPCPool("self")
	pool.Set(addr)
	getter, ok := pool.PickPeer("Tom")
	if !ok {
		t.Fatal("expected peer getter")
	}
	err := getter.Get(context.Background(), &pb.Request{Group: groupName, Key: "Tom"}, &pb.Response{})
	if err == nil {
		t.Fatal("expected peer error")
	}
}

func TestGRPCGetterReturnsNotFound(t *testing.T) {
	groupName := "grpc-not-found"
	addr, cleanup := startTestGRPCServer(t, groupName, geecache.GetterFunc(func(_ context.Context, _ string) ([]byte, error) {
		return nil, geecache.ErrNotFound
	}))
	defer cleanup()

	pool := geecache.NewGRPCPool("self")
	pool.Set(addr)
	getter, ok := pool.PickPeer("Tom")
	if !ok {
		t.Fatal("expected peer getter")
	}
	err := getter.Get(context.Background(), &pb.Request{Group: groupName, Key: "Tom"}, &pb.Response{})
	if err != geecache.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestStatsJSONShape(t *testing.T) {
	group := geecache.NewGroup("json-stats", 2<<10, geecache.GetterFunc(func(_ context.Context, _ string) ([]byte, error) {
		return []byte("v"), nil
	}))
	if _, err := group.Get("Tom"); err != nil {
		t.Fatalf("get: %v", err)
	}

	body, err := json.Marshal(group.Stats())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("expected non-empty stats json")
	}
}
