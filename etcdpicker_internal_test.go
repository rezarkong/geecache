package geecache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"geecache/registry"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdPickerRelistsAfterWatchChannelCloses(t *testing.T) {
	firstWatch := make(chan clientv3.WatchResponse)
	secondWatch := make(chan clientv3.WatchResponse)

	fake := &fakeEtcdDiscoveryClient{
		gets: []fakeGetResult{
			{resp: testGetResponse(t, 5, "svc", weightedPeer{Addr: "self", Weight: 1}, weightedPeer{Addr: "node-a", Weight: 1})},
			{resp: testGetResponse(t, 8, "svc", weightedPeer{Addr: "self", Weight: 1})},
		},
		watches: []clientv3.WatchChan{firstWatch, secondWatch},
	}

	picker := newTestEtcdPicker(t, fake, 10*time.Millisecond, 0)
	defer picker.Close()

	waitForMembers(t, picker, []string{"node-a", "self"})

	close(firstWatch)

	waitForMembers(t, picker, []string{"self"})

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.watchRevs) < 2 {
		t.Fatalf("expected at least two watch attempts, got %d", len(fake.watchRevs))
	}
	if fake.watchRevs[0] != 6 {
		t.Fatalf("expected first watch rev 6, got %d", fake.watchRevs[0])
	}
	if fake.watchRevs[1] != 9 {
		t.Fatalf("expected second watch rev 9, got %d", fake.watchRevs[1])
	}
}

func TestEtcdPickerPeriodicResyncRefreshesMembers(t *testing.T) {
	firstWatch := make(chan clientv3.WatchResponse)
	secondWatch := make(chan clientv3.WatchResponse)

	fake := &fakeEtcdDiscoveryClient{
		gets: []fakeGetResult{
			{resp: testGetResponse(t, 3, "svc", weightedPeer{Addr: "self", Weight: 1}, weightedPeer{Addr: "node-a", Weight: 1})},
			{resp: testGetResponse(t, 4, "svc", weightedPeer{Addr: "self", Weight: 1})},
		},
		watches: []clientv3.WatchChan{firstWatch, secondWatch},
	}

	picker := newTestEtcdPicker(t, fake, 10*time.Millisecond, 25*time.Millisecond)
	defer picker.Close()

	waitForMembers(t, picker, []string{"node-a", "self"})
	waitForMembers(t, picker, []string{"self"})

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.watchRevs) < 2 {
		t.Fatalf("expected resync to start a second watch, got %d attempts", len(fake.watchRevs))
	}
	if fake.watchRevs[0] != 4 {
		t.Fatalf("expected initial watch rev 4, got %d", fake.watchRevs[0])
	}
	if fake.watchRevs[1] != 5 {
		t.Fatalf("expected post-resync watch rev 5, got %d", fake.watchRevs[1])
	}
}

func newTestEtcdPicker(t *testing.T, cli etcdDiscoveryClient, retryBackoff, resyncInterval time.Duration) *EtcdPicker {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	picker := &EtcdPicker{
		self:                    "self",
		selfWeight:              1,
		serviceName:             "svc",
		members:                 map[string]weightedPeer{"self": {Addr: "self", Weight: 1}},
		grpcGetters:             make(map[string]*grpcGetter),
		etcdCli:                 cli,
		ctx:                     ctx,
		cancel:                  cancel,
		discoveryRetryBackoff:   retryBackoff,
		discoveryResyncInterval: resyncInterval,
	}
	picker.applyMembers([]weightedPeer{{Addr: "self", Weight: 1}})

	rev, err := picker.fetchAllServices()
	if err != nil {
		t.Fatalf("fetchAllServices: %v", err)
	}
	go picker.watchServiceChanges(rev + 1)
	return picker
}

func waitForMembers(t *testing.T, picker *EtcdPicker, expect []string) {
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

func testGetResponse(t *testing.T, rev int64, service string, peers ...weightedPeer) *clientv3.GetResponse {
	t.Helper()

	kvs := make([]*mvccpb.KeyValue, 0, len(peers))
	for _, peer := range peers {
		value, err := registry.EncodeRecord(peer.Addr, peer.Weight)
		if err != nil {
			t.Fatalf("EncodeRecord(%s): %v", peer.Addr, err)
		}
		kvs = append(kvs, &mvccpb.KeyValue{
			Key:   []byte(etcdServicePrefix(service) + peer.Addr),
			Value: []byte(value),
		})
	}

	return (*clientv3.GetResponse)(&etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: rev},
		Kvs:    kvs,
	})
}
