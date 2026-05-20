package discovery

import (
	"testing"
	"time"

	"geecache/cluster"
	"geecache/etcd/registry"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestPickerRelistsAfterWatchChannelCloses(t *testing.T) {
	firstWatch := make(chan clientv3.WatchResponse)
	secondWatch := make(chan clientv3.WatchResponse)

	fake := &fakeEtcdDiscoveryClient{
		gets: []fakeGetResult{
			{resp: testGetResponse(t, 5, "svc", cluster.Member{Addr: "self", Weight: 1}, cluster.Member{Addr: "node-a", Weight: 1})},
			{resp: testGetResponse(t, 8, "svc", cluster.Member{Addr: "self", Weight: 1})},
		},
		watches: []clientv3.WatchChan{firstWatch, secondWatch},
	}

	picker := newTestPicker(t, fake, 10*time.Millisecond, 0)
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

func TestPickerPeriodicResyncRefreshesMembers(t *testing.T) {
	firstWatch := make(chan clientv3.WatchResponse)
	secondWatch := make(chan clientv3.WatchResponse)

	fake := &fakeEtcdDiscoveryClient{
		gets: []fakeGetResult{
			{resp: testGetResponse(t, 3, "svc", cluster.Member{Addr: "self", Weight: 1}, cluster.Member{Addr: "node-a", Weight: 1})},
			{resp: testGetResponse(t, 4, "svc", cluster.Member{Addr: "self", Weight: 1})},
		},
		watches: []clientv3.WatchChan{firstWatch, secondWatch},
	}

	picker := newTestPicker(t, fake, 10*time.Millisecond, 25*time.Millisecond)
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

func TestPickerPickPeerReflectsWatchUpdates(t *testing.T) {
	firstWatch := make(chan clientv3.WatchResponse, 1)

	fake := &fakeEtcdDiscoveryClient{
		gets: []fakeGetResult{
			{resp: testGetResponse(t, 7, "svc", cluster.Member{Addr: "self", Weight: 1}, cluster.Member{Addr: "node-a", Weight: 1})},
		},
		watches: []clientv3.WatchChan{firstWatch},
	}

	picker := newTestPicker(t, fake, 10*time.Millisecond, 0)
	defer picker.Close()

	waitForMembers(t, picker, []string{"node-a", "self"})
	keys := findKeysForPeer(t, picker, "node-a", 32)
	if len(keys) == 0 {
		t.Fatal("expected at least one key owned by node-a before watch update")
	}

	record, err := registry.EncodeRecord("node-b", 1)
	if err != nil {
		t.Fatalf("EncodeRecord(node-b): %v", err)
	}
	firstWatch <- clientv3.WatchResponse{
		Header: etcdserverpb.ResponseHeader{Revision: 8},
		Events: []*clientv3.Event{
			{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Key:   []byte(registry.ServiceKey("svc", "node-b")),
					Value: []byte(record),
				},
			},
			{
				Type: clientv3.EventTypeDelete,
				Kv: &mvccpb.KeyValue{
					Key: []byte(registry.ServiceKey("svc", "node-a")),
				},
			},
		},
	}

	waitForMembers(t, picker, []string{"node-b", "self"})

	switched := false
	for _, key := range keys {
		if got := pickedPeerID(t, picker, key); got == "node-b" {
			switched = true
			break
		}
	}
	if !switched {
		t.Fatalf("expected at least one node-a key to move to node-b after watch update")
	}
}
