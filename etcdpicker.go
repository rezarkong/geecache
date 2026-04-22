package geecache

import (
	"context"
	"errors"
	"fmt"
	"geecache/consistenthash"
	"geecache/registry"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultDiscoveryRetryBackoff   = time.Second
	defaultDiscoveryResyncInterval = 30 * time.Second
)

var errEtcdDiscoveryResync = errors.New("etcd discovery resync requested")

type etcdDiscoveryClient interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Close() error
}

// EtcdPicker keeps the peer list in sync with etcd service discovery.
type EtcdPicker struct {
	self        string
	selfWeight  int
	serviceName string
	dialOptions []grpc.DialOption

	mu          sync.RWMutex
	members     map[string]weightedPeer
	peers       *consistenthash.Map
	grpcGetters map[string]*grpcGetter
	peerView    string

	etcdCli                 etcdDiscoveryClient
	ctx                     context.Context
	cancel                  context.CancelFunc
	discoveryRetryBackoff   time.Duration
	discoveryResyncInterval time.Duration
}

// NewEtcdPicker creates a picker backed by etcd service discovery.
func NewEtcdPicker(self string, endpoints []string, serviceName string, selfWeight int) (*EtcdPicker, error) {
	return NewEtcdPickerWithOptions(self, endpoints, serviceName, selfWeight, nil)
}

// NewEtcdPickerWithOptions creates a picker backed by etcd service discovery with custom dial options.
func NewEtcdPickerWithOptions(self string, endpoints []string, serviceName string, selfWeight int, dialOptions []grpc.DialOption) (*EtcdPicker, error) {
	if len(dialOptions) == 0 {
		dialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	cfg := registry.DefaultConfig
	if len(endpoints) > 0 {
		cfg.Endpoints = append([]string(nil), endpoints...)
	}
	if serviceName != "" {
		cfg.ServiceName = serviceName
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &EtcdPicker{
		self:                    self,
		selfWeight:              maxPeerWeight(selfWeight),
		serviceName:             cfg.ServiceName,
		dialOptions:             append([]grpc.DialOption(nil), dialOptions...),
		members:                 map[string]weightedPeer{self: {Addr: self, Weight: maxPeerWeight(selfWeight)}},
		grpcGetters:             make(map[string]*grpcGetter),
		etcdCli:                 cli,
		ctx:                     ctx,
		cancel:                  cancel,
		discoveryRetryBackoff:   defaultDiscoveryRetryBackoff,
		discoveryResyncInterval: defaultDiscoveryResyncInterval,
	}
	p.applyMembers([]weightedPeer{{Addr: self, Weight: p.selfWeight}})
	rev, err := p.fetchAllServices()
	if err != nil {
		cancel()
		_ = cli.Close()
		return nil, err
	}
	go p.watchServiceChanges(rev + 1)
	return p, nil
}

func (p *EtcdPicker) Register(server *grpc.Server) {
	registerGroupCacheServer(server, p)
}

func (p *EtcdPicker) Peers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	peers := make([]string, 0, len(p.members))
	for _, member := range p.members {
		peers = append(peers, formatPeerSpec(member))
	}
	sort.Strings(peers)
	return peers
}

func (p *EtcdPicker) PickPeer(key string) (PeerGetter, bool, bool) {
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
	getter, ok := p.grpcGetters[peer]
	if !ok {
		return nil, false, false
	}
	return getter, true, false
}

func (p *EtcdPicker) Close() error {
	p.cancel()

	p.mu.Lock()
	getters := p.grpcGetters
	p.grpcGetters = nil
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

func (p *EtcdPicker) Self() string {
	return p.self
}

func (p *EtcdPicker) PeerByAddr(addr string) (mutationPeer, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	getter, ok := p.grpcGetters[addr]
	return getter, ok
}

func (p *EtcdPicker) currentPeerView() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.peerView
}

func (p *EtcdPicker) fetchAllServices() (int64, error) {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	resp, err := p.etcdCli.Get(ctx, etcdServicePrefix(p.serviceName), clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("fetch services from etcd: %w", err)
	}

	members := []weightedPeer{{Addr: p.self, Weight: p.selfWeight}}
	for _, kv := range resp.Kvs {
		member, ok := parseRegistryMember(string(kv.Value))
		if !ok {
			addr := parseEtcdAddrFromKey(string(kv.Key), p.serviceName)
			if addr == "" {
				continue
			}
			member = weightedPeer{Addr: addr, Weight: 1}
		}
		if member.Addr == "" {
			continue
		}
		members = append(members, member)
	}
	p.applyMembers(members)
	if resp.Header == nil {
		return 0, nil
	}
	return resp.Header.Revision, nil
}

func (p *EtcdPicker) watchServiceChanges(nextRev int64) {
	for {
		if p.ctx.Err() != nil {
			return
		}
		if nextRev <= 0 {
			rev, err := p.fetchAllServices()
			if err != nil {
				log.Printf("[EtcdPicker %s] discovery relist failed: %v", p.self, err)
				if !p.sleepWithContext(p.discoveryRetryBackoff) {
					return
				}
				continue
			}
			nextRev = rev + 1
		}

		err := p.watchFromRevision(nextRev)
		if p.ctx.Err() != nil {
			return
		}
		switch {
		case err == nil:
			return
		case errors.Is(err, errEtcdDiscoveryResync):
			nextRev = 0
		default:
			log.Printf("[EtcdPicker %s] discovery watch failed: %v", p.self, err)
			nextRev = 0
			if !p.sleepWithContext(p.discoveryRetryBackoff) {
				return
			}
		}
	}
}

func (p *EtcdPicker) watchFromRevision(startRev int64) error {
	watchCh := p.etcdCli.Watch(p.ctx, etcdServicePrefix(p.serviceName), clientv3.WithPrefix(), clientv3.WithRev(startRev))

	var ticker *time.Ticker
	if p.discoveryResyncInterval > 0 {
		ticker = time.NewTicker(p.discoveryResyncInterval)
		defer ticker.Stop()
	}

	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-tickerChan(ticker):
			return errEtcdDiscoveryResync
		case resp, ok := <-watchCh:
			if !ok {
				return fmt.Errorf("etcd watch channel closed")
			}
			if err := resp.Err(); err != nil {
				return fmt.Errorf("etcd watch error at rev %d: %w", startRev, err)
			}
			if resp.Canceled {
				return fmt.Errorf("etcd watch canceled at rev %d", startRev)
			}
			if resp.IsProgressNotify() || len(resp.Events) == 0 {
				continue
			}
			p.handleWatchEvents(resp.Events)
			startRev = resp.Header.Revision + 1
		}
	}
}

func (p *EtcdPicker) sleepWithContext(delay time.Duration) bool {
	if delay <= 0 {
		return p.ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-p.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *EtcdPicker) handleWatchEvents(events []*clientv3.Event) {
	memberSet := make(map[string]weightedPeer, len(p.members)+1)
	p.mu.RLock()
	for addr, peer := range p.members {
		memberSet[addr] = peer
	}
	p.mu.RUnlock()
	memberSet[p.self] = weightedPeer{Addr: p.self, Weight: p.selfWeight}

	for _, event := range events {
		member, ok := parseRegistryMember(string(event.Kv.Value))
		if !ok {
			addr := parseEtcdAddrFromKey(string(event.Kv.Key), p.serviceName)
			if addr == "" {
				continue
			}
			member = weightedPeer{Addr: addr, Weight: 1}
		}
		if member.Addr == "" || member.Addr == p.self {
			continue
		}
		switch event.Type {
		case clientv3.EventTypePut:
			memberSet[member.Addr] = member
		case clientv3.EventTypeDelete:
			delete(memberSet, member.Addr)
		}
	}

	members := make([]weightedPeer, 0, len(memberSet))
	for _, member := range memberSet {
		members = append(members, member)
	}
	p.applyMembers(members)
}

func (p *EtcdPicker) applyMembers(members []weightedPeer) {
	unique := make(map[string]weightedPeer, len(members)+1)
	unique[p.self] = weightedPeer{Addr: p.self, Weight: p.selfWeight}
	for _, member := range members {
		member.Addr = strings.TrimSpace(member.Addr)
		member.Weight = maxPeerWeight(member.Weight)
		if member.Addr == "" {
			continue
		}
		unique[member.Addr] = member
	}

	allMembers := make([]weightedPeer, 0, len(unique))
	for _, member := range unique {
		allMembers = append(allMembers, member)
	}

	ring := consistenthash.New(defaultReplicas, nil)
	ring.AddMembers(toHashMembers(allMembers)...)

	p.mu.Lock()
	oldGetters := p.grpcGetters
	nextGetters := make(map[string]*grpcGetter, len(unique))
	for _, member := range allMembers {
		if member.Addr == p.self {
			continue
		}
		if getter, ok := oldGetters[member.Addr]; ok {
			nextGetters[member.Addr] = getter
			continue
		}
		nextGetters[member.Addr] = &grpcGetter{
			addr:        member.Addr,
			peerView:    p.currentPeerView,
			dialOptions: append([]grpc.DialOption(nil), p.dialOptions...),
		}
	}

	removed := make([]*grpcGetter, 0, len(oldGetters))
	for member, getter := range oldGetters {
		if _, ok := nextGetters[member]; !ok {
			removed = append(removed, getter)
		}
	}

	p.members = unique
	p.peers = ring
	p.grpcGetters = nextGetters
	p.peerView = normalizePeerView(allMembers)
	p.mu.Unlock()

	for _, getter := range removed {
		_ = getter.Close()
	}
}

func etcdServicePrefix(service string) string {
	return fmt.Sprintf("/services/%s/", service)
}

func parseEtcdAddrFromKey(key, service string) string {
	prefix := etcdServicePrefix(service)
	if strings.HasPrefix(key, prefix) {
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}

func parseRegistryMember(raw string) (weightedPeer, bool) {
	record, ok := registry.DecodeRecord(raw)
	if !ok {
		return weightedPeer{}, false
	}
	return weightedPeer{Addr: record.Addr, Weight: record.Weight}, true
}

func maxPeerWeight(weight int) int {
	if weight <= 0 {
		return 1
	}
	return weight
}

func tickerChan(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}
