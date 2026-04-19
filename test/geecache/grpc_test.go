package geecache_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"geecache"
	pb "geecache/geecachepb"
	"net"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
)

func startTestGRPCServer(t *testing.T, groupName string, getter geecache.Getter) (string, func()) {
	t.Helper()

	geecache.NewGroup(groupName, 2<<10, getter)
	return startRegisteredGRPCServerWithPeers(t)
}

func startRegisteredGRPCServerWithPeers(t *testing.T, peers ...string) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	self := lis.Addr().String()
	pool := geecache.NewGRPCPool(self)
	if len(peers) > 0 {
		pool.Set(peers...)
	}
	server := grpc.NewServer()
	pool.Register(server)
	go func() {
		_ = server.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		server.Stop()
		_ = lis.Close()
		_ = pool.Close()
	}
}

func startBareTestGRPCServer(t *testing.T) (string, func()) {
	t.Helper()
	return startRegisteredGRPCServerWithPeers(t)
}

type countingPeerPicker struct {
	calls  int32
	getter geecache.PeerGetter
}

func (p *countingPeerPicker) PickPeer(string) (geecache.PeerGetter, bool, bool) {
	atomic.AddInt32(&p.calls, 1)
	return p.getter, true, false
}

func (p *countingPeerPicker) Close() error {
	return nil
}

type staticPeerGetter struct {
	err error
}

func (g staticPeerGetter) Get(context.Context, *pb.Request, *pb.Response) error {
	return g.err
}

func TestGRPCGetterFetchesFromPeer(t *testing.T) {
	groupName := "grpc-fetch"
	addr, cleanup := startTestGRPCServer(t, groupName, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("remote-" + key), nil
	}))
	defer cleanup()

	pool := geecache.NewGRPCPool("self")
	pool.Set(addr)
	getter, ok, self := pool.PickPeer("Tom")
	if !ok || self {
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
	if peer, ok, self := pool.PickPeer("Tom"); ok || self || peer != nil {
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
	getter, ok, self := pool.PickPeer("Tom")
	if !ok || self {
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
	getter, ok, self := pool.PickPeer("Tom")
	if !ok || self {
		t.Fatal("expected peer getter")
	}
	err := getter.Get(context.Background(), &pb.Request{Group: groupName, Key: "Tom"}, &pb.Response{})
	if err != geecache.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGRPCServerGetDoesNotForwardToOtherPeers(t *testing.T) {
	groupName := "grpc-local-only"
	group := geecache.NewGroup(groupName, 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("local-" + key), nil
	}))
	defer group.Close()

	picker := &countingPeerPicker{getter: staticPeerGetter{err: fmt.Errorf("unexpected peer forward")}}
	group.RegisterPeers(picker)

	addr, cleanup := startRegisteredGRPCServerWithPeers(t)
	defer cleanup()

	pool := geecache.NewGRPCPool("self")
	pool.Set(addr)
	getter, ok, self := pool.PickPeer("Tom")
	if !ok || self {
		t.Fatal("expected peer getter")
	}

	var out pb.Response
	if err := getter.Get(context.Background(), &pb.Request{Group: groupName, Key: "Tom"}, &out); err != nil {
		t.Fatalf("getter.Get: %v", err)
	}
	if got := string(out.Value); got != "local-Tom" {
		t.Fatalf("unexpected value %q", got)
	}
	if got := atomic.LoadInt32(&picker.calls); got != 0 {
		t.Fatalf("expected grpc handler to stay local, got %d peer picks", got)
	}
}

func TestGRPCGetterReturnsPeerViewMismatch(t *testing.T) {
	groupName := "grpc-peer-view-mismatch"
	geecache.NewGroup(groupName, 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("remote-" + key), nil
	}))

	addr, cleanup := startRegisteredGRPCServerWithPeers(t, "peer-a", "peer-b")
	defer cleanup()

	pool := geecache.NewGRPCPool("self")
	pool.Set(addr)
	getter, ok, self := pool.PickPeer("Tom")
	if !ok || self {
		t.Fatal("expected remote peer getter")
	}

	err := getter.Get(context.Background(), &pb.Request{Group: groupName, Key: "Tom"}, &pb.Response{})
	if !errors.Is(err, geecache.ErrPeerViewMismatch) {
		t.Fatalf("expected ErrPeerViewMismatch, got %v", err)
	}
}

func TestGroupFallsBackLocallyOnPeerViewMismatch(t *testing.T) {
	var localLoads int32
	group := geecache.NewGroup("peer-view-fallback", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&localLoads, 1)
		return []byte("local-" + key), nil
	}))
	defer group.Close()

	group.RegisterPeers(&countingPeerPicker{getter: staticPeerGetter{err: geecache.ErrPeerViewMismatch}})

	view, err := group.Get("Tom")
	if err != nil {
		t.Fatalf("expected local fallback, got %v", err)
	}
	if got := view.String(); got != "local-Tom" {
		t.Fatalf("unexpected value %q", got)
	}
	if got := atomic.LoadInt32(&localLoads); got != 1 {
		t.Fatalf("expected one local fallback load, got %d", got)
	}
}

func TestGRPCGetterReturnsGroupNotFound(t *testing.T) {
	addr, cleanup := startBareTestGRPCServer(t)
	defer cleanup()

	pool := geecache.NewGRPCPool("self")
	pool.Set(addr)
	getter, ok, self := pool.PickPeer("Tom")
	if !ok || self {
		t.Fatal("expected peer getter")
	}

	err := getter.Get(context.Background(), &pb.Request{Group: "missing-group", Key: "Tom"}, &pb.Response{})
	if !errors.Is(err, geecache.ErrGroupNotFound) {
		t.Fatalf("expected ErrGroupNotFound, got %v", err)
	}
	if errors.Is(err, geecache.ErrNotFound) {
		t.Fatalf("group lookup error must not be collapsed into ErrNotFound: %v", err)
	}
}

func TestGroupGetSurfacesPeerGroupNotFound(t *testing.T) {
	var localLoads int32
	group := geecache.NewGroup("peer-group-missing", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&localLoads, 1)
		return []byte("local-" + key), nil
	}))
	defer group.Close()

	group.RegisterPeers(&countingPeerPicker{getter: staticPeerGetter{err: geecache.ErrGroupNotFound}})

	_, err := group.Get("Tom")
	if !errors.Is(err, geecache.ErrGroupNotFound) {
		t.Fatalf("expected ErrGroupNotFound, got %v", err)
	}
	if got := atomic.LoadInt32(&localLoads); got != 0 {
		t.Fatalf("expected no local fallback on peer group mismatch, got %d loads", got)
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
