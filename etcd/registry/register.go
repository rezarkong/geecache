package registry

import (
	"context"
	"fmt"
	"geecache/internal/logx"
	"net"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdRegistrationClient interface {
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	Close() error
}

// Registration keeps one service lease alive in etcd until Close is called.
type Registration struct {
	cli          etcdRegistrationClient
	key          string
	value        string
	leaseTTL     int64
	retryBackoff time.Duration

	mu      sync.RWMutex
	leaseID clientv3.LeaseID

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	done   chan struct{}
}

// Register registers addr under /services/<service>/<addr> and keeps the lease alive.
func Register(cfg Config, addr string) (*Registration, error) {
	cfg = NormalizeConfig(cfg)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	return registerWithClient(cfg, addr, cli)
}

func registerWithClient(cfg Config, addr string, cli etcdRegistrationClient) (*Registration, error) {
	cfg = NormalizeConfig(cfg)

	advertiseAddr, err := NormalizeAdvertiseAddr(addr)
	if err != nil {
		_ = cli.Close()
		return nil, err
	}

	key := ServiceKey(cfg.ServiceName, advertiseAddr)
	value, err := EncodeRecord(advertiseAddr, cfg.Weight)
	if err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("encode service record: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &Registration{
		cli:          cli,
		key:          key,
		value:        value,
		leaseTTL:     cfg.LeaseTTL,
		retryBackoff: cfg.RetryBackoff,
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
	}

	keepAliveCh, leaseID, err := r.registerOnce()
	if err != nil {
		cancel()
		_ = cli.Close()
		return nil, err
	}
	r.setLeaseID(leaseID)
	logx.Event("etcd.registry", "registered", map[string]interface{}{
		"addr":     advertiseAddr,
		"key":      key,
		"lease_id": leaseID,
		"service":  cfg.ServiceName,
		"ttl":      cfg.LeaseTTL,
		"weight":   cfg.Weight,
	})

	go r.keepRegistered(keepAliveCh)
	return r, nil
}

// Close revokes the lease and closes the etcd client.
func (r *Registration) Close() error {
	if r == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		r.cancel()
		<-r.done

		if leaseID := r.currentLeaseID(); leaseID != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = r.cli.Revoke(ctx, leaseID)
			cancel()
			logx.Event("etcd.registry", "deregistered", map[string]interface{}{
				"key":      r.key,
				"lease_id": leaseID,
			})
		}
		closeErr := r.cli.Close()
		if err == nil {
			err = closeErr
		}
	})
	return err
}

func (r *Registration) registerOnce() (<-chan *clientv3.LeaseKeepAliveResponse, clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
	defer cancel()

	lease, err := r.cli.Grant(ctx, r.leaseTTL)
	if err != nil {
		return nil, 0, fmt.Errorf("grant lease: %w", err)
	}
	if _, err := r.cli.Put(ctx, r.key, r.value, clientv3.WithLease(lease.ID)); err != nil {
		return nil, 0, fmt.Errorf("put service key: %w", err)
	}
	keepAliveCh, err := r.cli.KeepAlive(r.ctx, lease.ID)
	if err != nil {
		return nil, 0, fmt.Errorf("keepalive lease: %w", err)
	}
	return keepAliveCh, lease.ID, nil
}

func (r *Registration) keepRegistered(keepAliveCh <-chan *clientv3.LeaseKeepAliveResponse) {
	defer close(r.done)

	current := keepAliveCh
	for {
		if current == nil {
			ch, leaseID, err := r.retryRegister()
			if err != nil {
				return
			}
			r.setLeaseID(leaseID)
			current = ch
			continue
		}

		select {
		case <-r.ctx.Done():
			return
		case resp, ok := <-current:
			if !ok || resp == nil || resp.TTL <= 0 {
				logx.Event("etcd.registry", "lease_lost", map[string]interface{}{
					"key":      r.key,
					"lease_id": r.currentLeaseID(),
				})
				r.setLeaseID(0)
				current = nil
			}
		}
	}
}

func (r *Registration) retryRegister() (<-chan *clientv3.LeaseKeepAliveResponse, clientv3.LeaseID, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return nil, 0, err
		}

		keepAliveCh, leaseID, err := r.registerOnce()
		if err == nil {
			logx.Event("etcd.registry", "re_registered", map[string]interface{}{
				"key":      r.key,
				"lease_id": leaseID,
			})
			return keepAliveCh, leaseID, nil
		}

		logx.Event("etcd.registry", "re_register_failed", map[string]interface{}{
			"error": err,
			"key":   r.key,
		})
		if !r.sleepWithContext(r.retryBackoff) {
			return nil, 0, r.ctx.Err()
		}
	}
}

func (r *Registration) sleepWithContext(delay time.Duration) bool {
	if delay <= 0 {
		return r.ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *Registration) setLeaseID(leaseID clientv3.LeaseID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaseID = leaseID
}

func (r *Registration) currentLeaseID() clientv3.LeaseID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaseID
}

func NormalizeAdvertiseAddr(addr string) (string, error) {
	if addr == "" {
		return "", fmt.Errorf("empty address")
	}
	if strings.HasPrefix(addr, ":") {
		localIP, err := getLocalIP()
		if err != nil {
			return "", fmt.Errorf("resolve local IP: %w", err)
		}
		return localIP + addr, nil
	}
	return addr, nil
}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}
		if ip := ipNet.IP.To4(); ip != nil {
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("no non-loopback IPv4 address found")
}
