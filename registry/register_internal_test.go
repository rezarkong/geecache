package registry

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRegistrationReregistersAfterKeepAliveStops(t *testing.T) {
	firstKeepAlive := make(chan *clientv3.LeaseKeepAliveResponse)
	secondKeepAlive := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	secondKeepAlive <- &clientv3.LeaseKeepAliveResponse{ID: 2, TTL: 10}

	fake := &fakeRegistrationClient{
		grantIDs:     []clientv3.LeaseID{1, 2},
		keepAliveChs: []<-chan *clientv3.LeaseKeepAliveResponse{firstKeepAlive, secondKeepAlive},
	}

	registration, err := registerWithClient(Config{
		ServiceName:  "svc",
		LeaseTTL:     10,
		RetryBackoff: 10 * time.Millisecond,
	}, "127.0.0.1:8080", fake)
	if err != nil {
		t.Fatalf("registerWithClient: %v", err)
	}
	defer registration.Close()

	waitForRegistrationState(t, fake, 1, 1, 1)

	close(firstKeepAlive)

	waitForRegistrationState(t, fake, 2, 2, 2)
	if got := registration.currentLeaseID(); got != 2 {
		t.Fatalf("expected latest lease ID 2, got %d", got)
	}
}

func TestRegistrationCloseRevokesLatestLease(t *testing.T) {
	firstKeepAlive := make(chan *clientv3.LeaseKeepAliveResponse)
	secondKeepAlive := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	secondKeepAlive <- &clientv3.LeaseKeepAliveResponse{ID: 2, TTL: 10}

	fake := &fakeRegistrationClient{
		grantIDs:     []clientv3.LeaseID{1, 2},
		keepAliveChs: []<-chan *clientv3.LeaseKeepAliveResponse{firstKeepAlive, secondKeepAlive},
	}

	registration, err := registerWithClient(Config{
		ServiceName:  "svc",
		LeaseTTL:     10,
		RetryBackoff: 10 * time.Millisecond,
	}, "127.0.0.1:8080", fake)
	if err != nil {
		t.Fatalf("registerWithClient: %v", err)
	}

	close(firstKeepAlive)
	waitForRegistrationState(t, fake, 2, 2, 2)

	if err := registration.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := fake.lastRevokedLeaseID(); got != 2 {
		t.Fatalf("expected latest lease 2 to be revoked, got %d", got)
	}
	if !fake.isClosed() {
		t.Fatal("expected client to be closed")
	}
}

func waitForRegistrationState(t *testing.T, fake *fakeRegistrationClient, grants, puts, keepAlives int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		gotGrants, gotPuts, gotKeepAlives := fake.counts()
		if gotGrants >= grants && gotPuts >= puts && gotKeepAlives >= keepAlives {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	gotGrants, gotPuts, gotKeepAlives := fake.counts()
	t.Fatalf("timed out waiting for registration state grants=%d puts=%d keepAlives=%d, got %d/%d/%d",
		grants, puts, keepAlives, gotGrants, gotPuts, gotKeepAlives)
}

type fakeRegistrationClient struct {
	mu sync.Mutex

	grantIDs     []clientv3.LeaseID
	keepAliveChs []<-chan *clientv3.LeaseKeepAliveResponse

	grantCalls     int
	putCalls       int
	keepAliveCalls int
	revoked        []clientv3.LeaseID
	closed         bool
}

func (f *fakeRegistrationClient) Grant(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.grantCalls++
	if len(f.grantIDs) == 0 {
		return nil, fmt.Errorf("unexpected Grant call")
	}
	id := f.grantIDs[0]
	f.grantIDs = f.grantIDs[1:]
	return &clientv3.LeaseGrantResponse{ID: id, TTL: 10}, nil
}

func (f *fakeRegistrationClient) Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls++
	return &clientv3.PutResponse{}, nil
}

func (f *fakeRegistrationClient) KeepAlive(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keepAliveCalls++
	if len(f.keepAliveChs) == 0 {
		ch := make(chan *clientv3.LeaseKeepAliveResponse)
		return ch, nil
	}
	ch := f.keepAliveChs[0]
	f.keepAliveChs = f.keepAliveChs[1:]
	return ch, nil
}

func (f *fakeRegistrationClient) Revoke(_ context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revoked = append(f.revoked, id)
	return &clientv3.LeaseRevokeResponse{}, nil
}

func (f *fakeRegistrationClient) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeRegistrationClient) counts() (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.grantCalls, f.putCalls, f.keepAliveCalls
}

func (f *fakeRegistrationClient) lastRevokedLeaseID() clientv3.LeaseID {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.revoked) == 0 {
		return 0
	}
	return f.revoked[len(f.revoked)-1]
}

func (f *fakeRegistrationClient) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}
