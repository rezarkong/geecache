package geecache

import (
	"context"
	"errors"
	"fmt"
	"geecache/consistenthash"
	pb "geecache/geecachepb"
	"log"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const defaultReplicas = 50

const peerViewHeader = "x-geecache-peer-view"

// GRPCPool implements PeerPicker for a pool of gRPC peers.
type GRPCPool struct {
	self        string
	mu          sync.RWMutex
	peers       *consistenthash.Map
	grpcGetters map[string]*grpcGetter
	peerView    string
}

// NewGRPCPool 初始化 Peers 的 Pool
func NewGRPCPool(self string) *GRPCPool {
	return &GRPCPool{self: self}
}

// Log server 的日志
func (p *GRPCPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// Set 更新 grpc pools 的 peer
func (p *GRPCPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	next := make(map[string]*grpcGetter, len(peers))
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	for _, peer := range peers {
		if p.grpcGetters != nil {
			if getter, ok := p.grpcGetters[peer]; ok {
				next[peer] = getter
				continue
			}
		}
		next[peer] = &grpcGetter{addr: peer, peerView: p.currentPeerView}
	}
	for peer, getter := range p.grpcGetters {
		if _, ok := next[peer]; !ok {
			_ = getter.Close()
		}
	}
	p.grpcGetters = next
	p.peerView = normalizePeerView(peers)
}

// Peers returns a copy of the current peer list.
func (p *GRPCPool) Peers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	peers := make([]string, 0, len(p.grpcGetters))
	for peer := range p.grpcGetters {
		peers = append(peers, peer)
	}
	return peers
}

// PickPeer picks a peer according to key.
func (p *GRPCPool) PickPeer(key string) (PeerGetter, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.peers == nil {
		return nil, false, false
	}
	if peer := p.peers.Get(key); peer != "" {
		if peer == p.self {
			return nil, true, true
		}
		p.Log("Pick peer %s", peer)
		return p.grpcGetters[peer], true, false
	}
	return nil, false, false
}

// Register registers the cache service on a grpc.Server.
func (p *GRPCPool) Register(server *grpc.Server) {
	pb.RegisterGroupCacheServer(server, &groupCacheServer{pool: p})
}

func (p *GRPCPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for peer, getter := range p.grpcGetters {
		if err := getter.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close peer %s: %w", peer, err)
		}
	}
	p.grpcGetters = nil
	p.peers = nil
	p.peerView = ""
	return firstErr
}

func (p *GRPCPool) currentPeerView() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.peerView
}

var _ PeerPicker = (*GRPCPool)(nil)

type groupCacheServer struct {
	pool *GRPCPool
	pb.UnimplementedGroupCacheServer
}

func (s *groupCacheServer) Get(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	group := GetGroup(req.GetGroup())
	if group == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "group %q is not registered", req.GetGroup())
	}
	if s.pool != nil {
		localView := s.pool.currentPeerView()
		if localView != "" {
			if remoteView := peerViewFromIncomingContext(ctx); remoteView != "" && remoteView != localView {
				return nil, status.Error(codes.Aborted, ErrPeerViewMismatch.Error())
			}
		}
	}

	view, err := group.getLocallyContext(ctx, req.GetKey())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if errors.Is(err, ErrGroupClosed) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Response{Value: view.ByteSlice()}, nil
}

type grpcGetter struct {
	addr string

	peerView func() string

	mu     sync.Mutex       // 懒连接，所以后面 conn 和 client 要保护
	conn   *grpc.ClientConn // 连接生命周期管理的核心对象
	client pb.GroupCacheClient
}

func (h *grpcGetter) ID() string {
	return h.addr
}

func (h *grpcGetter) Get(ctx context.Context, in *pb.Request, out *pb.Response) error {
	client, err := h.clientFor(ctx)
	if err != nil {
		return err
	}
	if h.peerView != nil {
		if view := h.peerView(); view != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, peerViewHeader, view)
		}
	}
	resp, err := client.Get(ctx, in)
	if err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			return ErrNotFound
		case codes.FailedPrecondition:
			return fmt.Errorf("%w: %s", ErrGroupNotFound, status.Convert(err).Message())
		case codes.Aborted:
			return ErrPeerViewMismatch
		case codes.Unavailable:
			return ErrGroupClosed
		}
		return err
	}
	*out = *resp
	return nil
}

func (h *grpcGetter) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn == nil {
		return nil
	}
	err := h.conn.Close()
	h.conn = nil
	h.client = nil
	return err
}

func (h *grpcGetter) clientFor(ctx context.Context) (pb.GroupCacheClient, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.client != nil {
		return h.client, nil
	}

	conn, err := grpc.NewClient(
		h.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	h.conn = conn
	h.client = pb.NewGroupCacheClient(conn)
	return h.client, nil
}

var _ PeerGetter = (*grpcGetter)(nil)

func normalizePeerView(peers []string) string {
	if len(peers) == 0 {
		return ""
	}
	seen := make(map[string]struct{}, len(peers))
	items := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer == "" {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		items = append(items, peer)
	}
	if len(items) == 0 {
		return ""
	}
	sort.Strings(items)
	return strings.Join(items, ",")
}

func peerViewFromIncomingContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(peerViewHeader)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
