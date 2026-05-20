package discovery

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"geecache/cluster"
	"geecache/etcd/registry"

	pb "geecache/geecachepb"
	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func newTestPicker(t *testing.T, cli etcdDiscoveryClient, retryBackoff, resyncInterval time.Duration) *Picker {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	picker := &Picker{
		self:                    "self",
		selfWeight:              1,
		serviceName:             "svc",
		newPeer:                 newTestManagedPeer,
		members:                 map[string]cluster.Member{"self": {Addr: "self", Weight: 1}},
		getters:                 make(map[string]cluster.ManagedPeer),
		etcdCli:                 cli,
		ctx:                     ctx,
		cancel:                  cancel,
		discoveryRetryBackoff:   retryBackoff,
		discoveryResyncInterval: resyncInterval,
	}
	picker.applyMembers([]cluster.Member{{Addr: "self", Weight: 1}})

	rev, err := picker.fetchAllServices()
	if err != nil {
		t.Fatalf("fetchAllServices: %v", err)
	}
	go picker.watchServiceChanges(rev + 1)
	return picker
}

func waitForMembers(t *testing.T, picker *Picker, expect []string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := picker.Peers()
		if sameStrings(got, expect) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for peers %v, got %v", expect, picker.Peers())
}

func pickedPeerID(t *testing.T, picker *Picker, key string) string {
	t.Helper()

	getter, ok, self := picker.PickPeer(key)
	if !ok {
		t.Fatalf("expected key %q to be assigned", key)
	}
	if self {
		return "self"
	}
	named, ok := getter.(interface{ ID() string })
	if !ok {
		t.Fatalf("expected peer getter for %q to expose ID()", key)
	}
	return named.ID()
}

func findKeysForPeer(t *testing.T, picker *Picker, want string, limit int) []string {
	t.Helper()

	keys := make([]string, 0, limit)
	for i := 0; i < 20000 && len(keys) < limit; i++ {
		key := fmt.Sprintf("route-%d", i)
		if got := pickedPeerID(t, picker, key); got == want {
			keys = append(keys, key)
		}
	}
	return keys
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type fakeGetResult struct {
	resp *clientv3.GetResponse
	err  error
}

type fakeEtcdDiscoveryClient struct {
	mu        sync.Mutex
	gets      []fakeGetResult
	watches   []clientv3.WatchChan
	watchRevs []int64
}

func (f *fakeEtcdDiscoveryClient) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.gets) == 0 {
		return nil, fmt.Errorf("unexpected Get call")
	}
	result := f.gets[0]
	f.gets = f.gets[1:]
	return result.resp, result.err
}

func (f *fakeEtcdDiscoveryClient) Watch(_ context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	f.mu.Lock()
	defer f.mu.Unlock()

	op := clientv3.OpGet(key, opts...)
	f.watchRevs = append(f.watchRevs, op.Rev())

	if len(f.watches) == 0 {
		ch := make(chan clientv3.WatchResponse)
		return ch
	}
	ch := f.watches[0]
	f.watches = f.watches[1:]
	return ch
}

func (f *fakeEtcdDiscoveryClient) Close() error {
	return nil
}

func testGetResponse(t *testing.T, rev int64, service string, peers ...cluster.Member) *clientv3.GetResponse {
	t.Helper()

	kvs := make([]*mvccpb.KeyValue, 0, len(peers))
	for _, peer := range peers {
		value, err := registry.EncodeRecord(peer.Addr, peer.Weight)
		if err != nil {
			t.Fatalf("EncodeRecord(%s): %v", peer.Addr, err)
		}
		kvs = append(kvs, &mvccpb.KeyValue{
			Key:   []byte(registry.ServiceKey(service, peer.Addr)),
			Value: []byte(value),
		})
	}

	return (*clientv3.GetResponse)(&etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: rev},
		Kvs:    kvs,
	})
}

type testManagedPeer struct {
	addr string
}

func newTestManagedPeer(addr string, _ func() string) cluster.ManagedPeer {
	return &testManagedPeer{addr: addr}
}

func (p *testManagedPeer) ID() string {
	return p.addr
}

func (p *testManagedPeer) Get(context.Context, *pb.Request, *pb.Response) error {
	return nil
}

func (p *testManagedPeer) Set(context.Context, cluster.MutationRequest) error {
	return nil
}

func (p *testManagedPeer) Delete(context.Context, cluster.MutationRequest) error {
	return nil
}

func (p *testManagedPeer) Invalidate(context.Context, cluster.MutationRequest) error {
	return nil
}

func (p *testManagedPeer) Close() error {
	return nil
}
