package geecache

import (
	"context"
	"errors"
	"fmt"
	"geecache/consistenthash"
	pb "geecache/geecachepb"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	defaultReplicas = 50
	defaultTimeout  = 3 * time.Second
)

// GRPCPool implements PeerPicker for a pool of gRPC peers.
type GRPCPool struct {
	self string

	mu          sync.Mutex
	peers       *consistenthash.Map
	grpcGetters map[string]*grpcGetter
}

// NewGRPCPool initializes a gRPC pool of peers.
func NewGRPCPool(self string) *GRPCPool {
	return &GRPCPool{self: self}
}

// Log info with server name.
func (p *GRPCPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// Set updates the pool's list of peers.
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
		next[peer] = &grpcGetter{addr: peer}
	}
	for peer, getter := range p.grpcGetters {
		if _, ok := next[peer]; !ok {
			_ = getter.Close()
		}
	}
	p.grpcGetters = next
}

// Peers returns a copy of the current peer list.
func (p *GRPCPool) Peers() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	peers := make([]string, 0, len(p.grpcGetters))
	for peer := range p.grpcGetters {
		peers = append(peers, peer)
	}
	return peers
}

// PickPeer picks a peer according to key.
func (p *GRPCPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers == nil {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.grpcGetters[peer], true
	}
	return nil, false
}

// Register registers the cache service on a grpc.Server.
func (p *GRPCPool) Register(server *grpc.Server) {
	pb.RegisterGroupCacheServer(server, &groupCacheServer{self: p.self})
}

var _ PeerPicker = (*GRPCPool)(nil)

type groupCacheServer struct {
	pb.UnimplementedGroupCacheServer
	self string
}

func (s *groupCacheServer) Get(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	group := GetGroup(req.GetGroup())
	if group == nil {
		return nil, status.Errorf(codes.NotFound, "no such group: %s", req.GetGroup())
	}

	view, err := group.GetContext(ctx, req.GetKey())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Response{Value: view.ByteSlice()}, nil
}

type grpcGetter struct {
	addr string

	mu     sync.Mutex
	conn   *grpc.ClientConn
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
	resp, err := client.Get(ctx, in)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return ErrNotFound
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

	dialCtx := ctx
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		dialCtx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}

	conn, err := grpc.DialContext(
		dialCtx,
		h.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	h.conn = conn
	h.client = pb.NewGroupCacheClient(conn)
	return h.client, nil
}

var _ PeerGetter = (*grpcGetter)(nil)
