package geecache_test

import (
	"context"
	"errors"
	"geecache"
	"geecache/algo"
	geecachepb "geecache/geecachepb"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerHealthReportsServing(t *testing.T) {
	group := geecache.NewGroup("server-health", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("v-" + key), nil
	}))
	defer group.Close()

	groupMgr, err := geecache.NewGroupManager(group)
	if err != nil {
		t.Fatalf("NewGroupManager: %v", err)
	}

	server, err := geecache.NewServer(geecache.ServerOptions{
		Addr:        "127.0.0.1:0",
		ServiceName: "server-health",
		StaticPeers: []string{"127.0.0.1:0"},
		Groups:      groupMgr,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Run(ctx)
	}()

	addr := waitForGRPCAddr(t, server)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	resp, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "server-health"})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if got := resp.GetStatus(); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING, got %s", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("server.Run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for server shutdown")
	}
}

func TestGroupSetRoutesToOwnerAndClearsLocalReplica(t *testing.T) {
	localLoads := 0
	localGroup := geecache.NewGroup("mutation-set", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		localLoads++
		return []byte("old-" + key), nil
	}))
	defer localGroup.Close()

	oldView, err := localGroup.Get("Tom")
	if err != nil {
		t.Fatalf("initial local Get: %v", err)
	}
	if got := oldView.String(); got != "old-Tom" {
		t.Fatalf("expected stale local value, got %q", got)
	}

	peer := &fakeMutationPeer{}
	localGroup.RegisterPeers(staticMutationPicker{peer: peer})

	if err := localGroup.Set("Tom", []byte("new-Tom")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	view, err := localGroup.Get("Tom")
	if err != nil {
		t.Fatalf("Get after Set: %v", err)
	}
	if got := view.String(); got != "new-Tom" {
		t.Fatalf("expected owner value after Set, got %q", got)
	}
	if localLoads != 1 {
		t.Fatalf("expected stale local value to be invalidated, local loads=%d", localLoads)
	}
	if got := peer.value; got != "new-Tom" {
		t.Fatalf("expected owner peer to receive new value, got %q", got)
	}
}

func TestGroupDeleteFromClusterRemovesOwnerValue(t *testing.T) {
	localGroup := geecache.NewGroup("mutation-delete", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return nil, geecache.ErrNotFound
	}))
	defer localGroup.Close()

	peer := &fakeMutationPeer{value: "new-Tom"}
	localGroup.RegisterPeers(staticMutationPicker{peer: peer})

	if err := localGroup.Set("Tom", []byte("new-Tom")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := localGroup.DeleteFromCluster("Tom"); err != nil {
		t.Fatalf("DeleteFromCluster: %v", err)
	}
	if _, err := localGroup.Get("Tom"); !errors.Is(err, geecache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if peer.value != "" {
		t.Fatalf("expected owner peer value to be cleared, got %q", peer.value)
	}
}

func TestGRPCMutationServiceAppliesRemoteSet(t *testing.T) {
	group := geecache.NewGroup("grpc-mutation-set", 2<<10, geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return nil, geecache.ErrNotFound
	}))
	defer group.Close()

	addr, cleanup := startRegisteredGRPCServerWithPeers(t)
	defer cleanup()

	pool := geecache.NewGRPCPool("self")
	pool.Set(addr)
	defer pool.Close()

	getter, ok, self := pool.PickPeer("Tom")
	if !ok || self {
		t.Fatal("expected remote getter")
	}
	mutator, ok := getter.(interface {
		Set(context.Context, geecache.MutationRequest) error
	})
	if !ok {
		t.Fatal("expected mutation-capable peer getter")
	}
	if err := mutator.Set(context.Background(), geecache.MutationRequest{
		Group: "grpc-mutation-set",
		Key:   "Tom",
		Value: []byte("remote-Tom"),
	}); err != nil {
		t.Fatalf("mutator.Set: %v", err)
	}

	view, err := group.Get("Tom")
	if err != nil {
		t.Fatalf("group.Get after remote set: %v", err)
	}
	if got := view.String(); got != "remote-Tom" {
		t.Fatalf("expected remote mutation value, got %q", got)
	}
}

func TestStoreFactoryIsUsedForShardInitialization(t *testing.T) {
	var created int
	group := geecache.NewGroupWithOptions(
		"store-factory",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			return []byte("value-" + key), nil
		}),
		geecache.WithStoreFactory(func(maxBytes int64, evictor algo.Evictor, onEvicted func(string, algo.Value)) geecache.Store {
			created++
			return newMapStore()
		}),
	)
	defer group.Close()

	if _, err := group.Get("Tom"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if created == 0 {
		t.Fatal("expected custom store factory to be used")
	}
}

func waitForGRPCAddr(t *testing.T, server *geecache.Server) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		addr := server.GRPCAddr()
		if addr != "" {
			_, port, err := net.SplitHostPort(addr)
			if err == nil && port != "0" {
				return addr
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timeout waiting for grpc addr")
	return ""
}

type mapStore struct {
	mu    sync.Mutex
	items map[string]algo.Value
}

type staticMutationPicker struct {
	peer *fakeMutationPeer
}

func (p staticMutationPicker) PickPeer(string) (geecache.PeerGetter, bool, bool) {
	return p.peer, true, false
}

func (p staticMutationPicker) Close() error {
	return nil
}

type fakeMutationPeer struct {
	value string
}

func (p *fakeMutationPeer) Get(_ context.Context, _ *geecachepb.Request, out *geecachepb.Response) error {
	if p.value == "" {
		return geecache.ErrNotFound
	}
	out.Value = []byte(p.value)
	return nil
}

func (p *fakeMutationPeer) Set(_ context.Context, req geecache.MutationRequest) error {
	p.value = string(req.Value)
	return nil
}

func (p *fakeMutationPeer) Delete(_ context.Context, _ geecache.MutationRequest) error {
	p.value = ""
	return nil
}

func (p *fakeMutationPeer) Invalidate(_ context.Context, _ geecache.MutationRequest) error {
	p.value = ""
	return nil
}

func newMapStore() *mapStore {
	return &mapStore{items: make(map[string]algo.Value)}
}

func (s *mapStore) Add(key string, value algo.Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = value
}

func (s *mapStore) GetOrRemoveIf(key string, predicate func(algo.Value) bool) (value algo.Value, ok bool, removed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok = s.items[key]
	if !ok {
		return nil, false, false
	}
	if predicate != nil && predicate(value) {
		delete(s.items, key)
		return nil, false, true
	}
	return value, true, false
}

func (s *mapStore) CompensateBurstIf(key string, n int, predicate func(algo.Value) bool) (ok bool, removed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.items[key]
	if !ok {
		return false, false
	}
	if predicate != nil && predicate(value) {
		delete(s.items, key)
		return false, true
	}
	return true, false
}

func (s *mapStore) Remove(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}

func (s *mapStore) RemoveIf(key string, predicate func(algo.Value) bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.items[key]
	if !ok {
		return false
	}
	if predicate != nil && !predicate(value) {
		return false
	}
	delete(s.items, key)
	return true
}

func (s *mapStore) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.items))
	for key := range s.items {
		keys = append(keys, key)
	}
	return keys
}

func (s *mapStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}
