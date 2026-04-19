package geecache

import (
	"context"

	pb "geecache/geecachepb"
)

// PeerPicker is the interface that must be implemented to locate
// the peer that owns a specific key.
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool, self bool)
	Close() error
}

// PeerGetter is the interface that must be implemented by a peer.
type PeerGetter interface {
	Get(ctx context.Context, in *pb.Request, out *pb.Response) error
}
