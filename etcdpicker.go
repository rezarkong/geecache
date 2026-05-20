package geecache

import (
	"fmt"
	"geecache/cluster"
	"geecache/consistenthash"
	"geecache/etcd/discovery"
	"geecache/etcd/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EtcdPicker adapts the etcd discovery picker to the root geecache server wiring.
type EtcdPicker struct {
	picker *discovery.Picker
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
	cfg.Weight = selfWeight
	cfg = registry.NormalizeConfig(cfg)

	picker, err := discovery.NewPicker(self, cfg, func(addr string, currentPeerView func() string) cluster.ManagedPeer {
		return &grpcGetter{
			addr:        addr,
			peerView:    currentPeerView,
			dialOptions: append([]grpc.DialOption(nil), dialOptions...),
		}
	})
	if err != nil {
		return nil, fmt.Errorf("create discovery picker: %w", err)
	}
	return &EtcdPicker{picker: picker}, nil
}

func (p *EtcdPicker) Register(server *grpc.Server) {
	registerGroupCacheServer(server, p)
}

func (p *EtcdPicker) Peers() []string {
	if p == nil || p.picker == nil {
		return nil
	}
	return p.picker.Peers()
}

func (p *EtcdPicker) PickPeer(key string) (PeerGetter, bool, bool) {
	if p == nil || p.picker == nil {
		return nil, false, false
	}
	return p.picker.PickPeer(key)
}

func (p *EtcdPicker) Close() error {
	if p == nil || p.picker == nil {
		return nil
	}
	return p.picker.Close()
}

func (p *EtcdPicker) Self() string {
	if p == nil || p.picker == nil {
		return ""
	}
	return p.picker.Self()
}

func (p *EtcdPicker) PeerByAddr(addr string) (mutationPeer, bool) {
	if p == nil || p.picker == nil {
		return nil, false
	}
	return p.picker.PeerByAddr(addr)
}

func (p *EtcdPicker) currentPeerView() string {
	if p == nil || p.picker == nil {
		return ""
	}
	return p.picker.CurrentPeerView()
}

func (p *EtcdPicker) HashRingPositions() []consistenthash.RingPosition {
	if p == nil || p.picker == nil {
		return nil
	}
	return p.picker.HashRingPositions()
}

func (p *EtcdPicker) LocateKey(key string) consistenthash.LookupResult {
	if p == nil || p.picker == nil {
		return consistenthash.LookupResult{Key: key}
	}
	return p.picker.LocateKey(key)
}
