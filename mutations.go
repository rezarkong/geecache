package geecache

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	mutationServiceName              = "geecache.internal.Mutation"
	mutationSetFullMethodName        = "/" + mutationServiceName + "/Set"
	mutationDeleteFullMethodName     = "/" + mutationServiceName + "/Delete"
	mutationInvalidateFullMethodName = "/" + mutationServiceName + "/Invalidate"
)

type mutationRequest struct {
	Group string `json:"group,omitempty"`
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

// MutationRequest exposes the cluster-mutation payload type for tests and callers
// that want to integrate custom peer implementations.
type MutationRequest = mutationRequest

type mutationResponse struct{}

type mutationPeer interface {
	Set(ctx context.Context, req mutationRequest) error
	Delete(ctx context.Context, req mutationRequest) error
	Invalidate(ctx context.Context, req mutationRequest) error
}

type mutationService interface {
	Set(context.Context, *mutationRequest) (*mutationResponse, error)
	Delete(context.Context, *mutationRequest) (*mutationResponse, error)
	Invalidate(context.Context, *mutationRequest) (*mutationResponse, error)
}

type mutationBroadcaster interface {
	Peers() []string
	Self() string
	PeerByAddr(addr string) (mutationPeer, bool)
}

func (s *groupCacheServer) Set(ctx context.Context, req *mutationRequest) (*mutationResponse, error) {
	group, err := s.groupFromRequest(ctx, req.Group)
	if err != nil {
		return nil, err
	}
	if err := group.applySet(req.Key, req.Value); err != nil {
		return nil, statusForMutationError(err)
	}
	return &mutationResponse{}, nil
}

func (s *groupCacheServer) Delete(ctx context.Context, req *mutationRequest) (*mutationResponse, error) {
	group, err := s.groupFromRequest(ctx, req.Group)
	if err != nil {
		return nil, err
	}
	if err := group.applyDelete(req.Key); err != nil {
		return nil, statusForMutationError(err)
	}
	return &mutationResponse{}, nil
}

func (s *groupCacheServer) Invalidate(ctx context.Context, req *mutationRequest) (*mutationResponse, error) {
	group, err := s.groupFromRequest(ctx, req.Group)
	if err != nil {
		return nil, err
	}
	if err := group.applyDelete(req.Key); err != nil {
		return nil, statusForMutationError(err)
	}
	return &mutationResponse{}, nil
}

func (s *groupCacheServer) groupFromRequest(ctx context.Context, groupName string) (*Group, error) {
	group := GetGroup(groupName)
	if group == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "group %q is not registered", groupName)
	}
	if s.pool != nil {
		localView := s.pool.currentPeerView()
		if localView != "" {
			if remoteView := peerViewFromIncomingContext(ctx); remoteView != "" && remoteView != localView {
				return nil, status.Error(codes.Aborted, ErrPeerViewMismatch.Error())
			}
		}
	}
	return group, nil
}

func statusForMutationError(err error) error {
	switch {
	case errors.Is(err, ErrGroupClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func registerMutationService(server *grpc.Server, srv *groupCacheServer) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: mutationServiceName,
		HandlerType: (*mutationService)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Set", Handler: mutationSetHandler},
			{MethodName: "Delete", Handler: mutationDeleteHandler},
			{MethodName: "Invalidate", Handler: mutationInvalidateHandler},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "geecache-mutations",
	}, srv)
}

func mutationSetHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(mutationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*groupCacheServer).Set(ctx, req.(*mutationRequest))
	}
	if interceptor == nil {
		return handler(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: mutationSetFullMethodName,
	}, handler)
}

func mutationDeleteHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(mutationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*groupCacheServer).Delete(ctx, req.(*mutationRequest))
	}
	if interceptor == nil {
		return handler(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: mutationDeleteFullMethodName,
	}, handler)
}

func mutationInvalidateHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(mutationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*groupCacheServer).Invalidate(ctx, req.(*mutationRequest))
	}
	if interceptor == nil {
		return handler(ctx, in)
	}
	return interceptor(ctx, in, &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: mutationInvalidateFullMethodName,
	}, handler)
}

func withPeerView(ctx context.Context, view string) context.Context {
	if view == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, peerViewHeader, view)
}

func parsePeerAddr(spec string) string {
	peer := parsePeerSpec(spec)
	return peer.Addr
}

func (g *Group) applySet(key string, value []byte) error {
	if g.closed.Load() {
		return ErrGroupClosed
	}
	if key == "" {
		return fmt.Errorf("key is required")
	}
	if len(value) == 0 {
		return fmt.Errorf("value is required")
	}
	g.populateCache(key, ByteView{b: cloneBytes(value)})
	g.bloomAdd(key)
	return nil
}

func (g *Group) applyDelete(key string) error {
	if g.closed.Load() {
		return ErrGroupClosed
	}
	if key == "" {
		return fmt.Errorf("key is required")
	}
	g.mainCache.delete(key)
	return nil
}
