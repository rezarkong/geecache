package geecache

import "geecache/cluster"

// PeerPicker locates the peer that owns a specific key.
type PeerPicker = cluster.PeerPicker

// PeerGetter loads one value from a remote peer.
type PeerGetter = cluster.PeerGetter
