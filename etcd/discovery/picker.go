package discovery

import (
	"context"
	"fmt"
	"geecache/cluster"
	"geecache/consistenthash"
	"geecache/etcd/registry"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultReplicas       = 50
	defaultRetryBackoff   = time.Second
	defaultResyncInterval = 30 * time.Second
)

type etcdDiscoveryClient interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Close() error
}

// PeerFactory creates one peer client for a discovered member.
type PeerFactory func(addr string, currentPeerView func() string) cluster.ManagedPeer

// Picker keeps the peer list in sync with etcd service discovery.
type Picker struct {
	self        string
	selfWeight  int
	serviceName string
	newPeer     PeerFactory

	mu       sync.RWMutex
	members  map[string]cluster.Member
	peers    *consistenthash.Map
	getters  map[string]cluster.ManagedPeer
	peerView string

	etcdCli                 etcdDiscoveryClient
	ctx                     context.Context
	cancel                  context.CancelFunc
	discoveryRetryBackoff   time.Duration
	discoveryResyncInterval time.Duration
}

// NewPicker creates a picker backed by etcd service discovery.
func NewPicker(self string, cfg registry.Config, newPeer PeerFactory) (*Picker, error) {
	if newPeer == nil {
		return nil, fmt.Errorf("peer factory is required")
	}

	cfg = registry.NormalizeConfig(cfg)
	advertiseAddr, err := registry.NormalizeAdvertiseAddr(self)
	if err != nil {
		return nil, fmt.Errorf("normalize self address: %w", err)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Picker{
		self:                    advertiseAddr,
		selfWeight:              cluster.MaxWeight(cfg.Weight),
		serviceName:             cfg.ServiceName,
		newPeer:                 newPeer,
		members:                 map[string]cluster.Member{advertiseAddr: {Addr: advertiseAddr, Weight: cluster.MaxWeight(cfg.Weight)}},
		getters:                 make(map[string]cluster.ManagedPeer),
		etcdCli:                 cli,
		ctx:                     ctx,
		cancel:                  cancel,
		discoveryRetryBackoff:   defaultRetryBackoff,
		discoveryResyncInterval: defaultResyncInterval,
	}
	p.applyMembers([]cluster.Member{{Addr: advertiseAddr, Weight: p.selfWeight}})
	rev, err := p.fetchAllServices()
	if err != nil {
		cancel()
		_ = cli.Close()
		return nil, err
	}
	go p.watchServiceChanges(rev + 1)
	return p, nil
}

// Peers returns the current canonical peer list.
func (p *Picker) Peers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peers := make([]string, 0, len(p.members))
	for _, member := range p.members {
		peers = append(peers, cluster.FormatMemberSpec(member))
	}
	sort.Strings(peers)
	return peers
}

// PickPeer picks a peer according to key.
func (p *Picker) PickPeer(key string) (cluster.PeerGetter, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.peers == nil {
		return nil, false, false
	}
	peer := p.peers.Get(key)
	if peer == "" {
		return nil, false, false
	}
	if peer == p.self {
		return nil, true, true
	}
	getter, ok := p.getters[peer]
	if !ok {
		return nil, false, false
	}
	return getter, true, false
}

// PeerByAddr returns one discovered mutation-capable peer.
func (p *Picker) PeerByAddr(addr string) (cluster.MutationPeer, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	getter, ok := p.getters[addr]
	return getter, ok
}

// Close releases all etcd and peer client resources.
func (p *Picker) Close() error {
	p.cancel()

	p.mu.Lock()
	getters := p.getters
	p.getters = nil
	p.peers = nil
	p.members = nil
	p.peerView = ""
	p.mu.Unlock()

	var firstErr error
	for peer, getter := range getters {
		if err := getter.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close peer %s: %w", peer, err)
		}
	}
	if err := p.etcdCli.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Self returns the normalized advertise address for this picker.
func (p *Picker) Self() string {
	return p.self
}

// CurrentPeerView returns the canonical peer view string attached to RPC metadata.
func (p *Picker) CurrentPeerView() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.peerView
}

// HashRingPositions exposes the current consistent-hash ring.
func (p *Picker) HashRingPositions() []consistenthash.RingPosition {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.peers == nil {
		return nil
	}
	return p.peers.Positions()
}

// LocateKey returns the lookup result for one key on the current ring.
func (p *Picker) LocateKey(key string) consistenthash.LookupResult {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.peers == nil {
		return consistenthash.LookupResult{Key: key}
	}
	return p.peers.Locate(key)
}
