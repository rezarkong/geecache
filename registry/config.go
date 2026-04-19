package registry

import "time"

// Config defines the etcd client and service registration behavior.
type Config struct {
	Endpoints   []string
	DialTimeout time.Duration
	ServiceName string
	LeaseTTL    int64
}

const DefaultServiceName = "geecache"

// DefaultConfig keeps local development simple while allowing callers to override it.
var DefaultConfig = Config{
	Endpoints:   []string{"localhost:2379"},
	DialTimeout: 5 * time.Second,
	ServiceName: DefaultServiceName,
	LeaseTTL:    10,
}
