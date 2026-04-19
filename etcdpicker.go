package geecache

import (
	"context"
	"fmt"
	"geecache/consistenthash"
	"geecache/registry"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// EtcdPicker keeps the peer list in sync with etcd service discovery.
type EtcdPicker struct {
	self        string
	serviceName string

	mu          sync.RWMutex
	members     map[string]struct{}
	peers       *consistenthash.Map
	grpcGetters map[string]*grpcGetter
	peerView    string

	etcdCli *clientv3.Client
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewEtcdPicker creates a picker backed by etcd service discovery.
func NewEtcdPicker(self string, endpoints []string, serviceName string) (*EtcdPicker, error) {
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
		self:        self,
		serviceName: cfg.ServiceName,
		members:     map[string]struct{}{self: {}},
		grpcGetters: make(map[string]*grpcGetter),
		etcdCli:     cli,
		ctx:         ctx,
		cancel:      cancel,
	}
	p.applyMembers([]string{self})
	if err := p.fetchAllServices(); err != nil {
		cancel()
		_ = cli.Close()
		return nil, err
	}
	go p.watchServiceChanges()
	return p, nil
}

func (p *EtcdPicker) Register(server *grpc.Server) {
	registerGroupCacheServer(server, p)
}

func (p *EtcdPicker) Peers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	peers := make([]string, 0, len(p.members))
	for member := range p.members {
		peers = append(peers, member)
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

func (p *EtcdPicker) currentPeerView() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.peerView
}

func (p *EtcdPicker) fetchAllServices() error {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	resp, err := p.etcdCli.Get(ctx, etcdServicePrefix(p.serviceName), clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("fetch services from etcd: %w", err)
	}

	members := []string{p.self}
	for _, kv := range resp.Kvs {
		addr := strings.TrimSpace(string(kv.Value))
		if addr == "" {
			addr = parseEtcdAddrFromKey(string(kv.Key), p.serviceName)
		}
		if addr == "" {
			continue
		}
		members = append(members, addr)
	}
	p.applyMembers(members)
	return nil
}

func (p *EtcdPicker) watchServiceChanges() {
	watcher := clientv3.NewWatcher(p.etcdCli)
	defer watcher.Close()

	watchCh := watcher.Watch(p.ctx, etcdServicePrefix(p.serviceName), clientv3.WithPrefix())
	for {
		select {
		case <-p.ctx.Done():
			return
		case resp, ok := <-watchCh:
			if !ok || resp.Canceled {
				return
			}
			p.handleWatchEvents(resp.Events)
		}
	}
}

func (p *EtcdPicker) handleWatchEvents(events []*clientv3.Event) {
	memberSet := make(map[string]struct{}, len(p.Peers()))
	for _, peer := range p.Peers() {
		memberSet[peer] = struct{}{}
	}
	memberSet[p.self] = struct{}{}

	for _, event := range events {
		addr := strings.TrimSpace(string(event.Kv.Value))
		if addr == "" {
			addr = parseEtcdAddrFromKey(string(event.Kv.Key), p.serviceName)
		}
		if addr == "" || addr == p.self {
			continue
		}
		switch event.Type {
		case clientv3.EventTypePut:
			memberSet[addr] = struct{}{}
		case clientv3.EventTypeDelete:
			delete(memberSet, addr)
		}
	}

	members := make([]string, 0, len(memberSet))
	for member := range memberSet {
		members = append(members, member)
	}
	p.applyMembers(members)
}

func (p *EtcdPicker) applyMembers(members []string) {
	unique := make(map[string]struct{}, len(members)+1)
	unique[p.self] = struct{}{}
	for _, member := range members {
		member = strings.TrimSpace(member)
		if member == "" {
			continue
		}
		unique[member] = struct{}{}
	}

	allMembers := make([]string, 0, len(unique))
	for member := range unique {
		allMembers = append(allMembers, member)
	}

	ring := consistenthash.New(defaultReplicas, nil)
	ring.Add(allMembers...)

	p.mu.Lock()
	oldGetters := p.grpcGetters
	nextGetters := make(map[string]*grpcGetter, len(unique))
	for _, member := range allMembers {
		if member == p.self {
			continue
		}
		if getter, ok := oldGetters[member]; ok {
			nextGetters[member] = getter
			continue
		}
		nextGetters[member] = &grpcGetter{addr: member, peerView: p.currentPeerView}
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
