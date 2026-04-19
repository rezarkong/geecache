package registry

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Registration keeps one service lease alive in etcd until Close is called.
type Registration struct {
	cli     *clientv3.Client
	leaseID clientv3.LeaseID
	once    sync.Once
	done    chan struct{}
}

// Register registers addr under /services/<service>/<addr> and keeps the lease alive.
func Register(cfg Config, addr string) (*Registration, error) {
	cfg = normalizeConfig(cfg)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create etcd client: %w", err)
	}

	advertiseAddr, err := normalizeAdvertiseAddr(addr)
	if err != nil {
		_ = cli.Close()
		return nil, err
	}

	lease, err := cli.Grant(context.Background(), cfg.LeaseTTL)
	if err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("grant lease: %w", err)
	}

	key := serviceKey(cfg.ServiceName, advertiseAddr)
	if _, err := cli.Put(context.Background(), key, advertiseAddr, clientv3.WithLease(lease.ID)); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("put service key: %w", err)
	}

	keepAliveCh, err := cli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("keepalive lease: %w", err)
	}

	r := &Registration{
		cli:     cli,
		leaseID: lease.ID,
		done:    make(chan struct{}),
	}
	go func() {
		defer close(r.done)
		for range keepAliveCh {
		}
	}()
	return r, nil
}

// Close revokes the lease and closes the etcd client.
func (r *Registration) Close() error {
	if r == nil {
		return nil
	}
	var err error
	r.once.Do(func() {
		if r.leaseID != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = r.cli.Revoke(ctx, r.leaseID)
			cancel()
		}
		closeErr := r.cli.Close()
		if err == nil {
			err = closeErr
		}
		<-r.done
	})
	return err
}

func normalizeConfig(cfg Config) Config {
	if len(cfg.Endpoints) == 0 {
		cfg.Endpoints = append([]string(nil), DefaultConfig.Endpoints...)
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultConfig.DialTimeout
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = DefaultConfig.ServiceName
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultConfig.LeaseTTL
	}
	return cfg
}

func servicePrefix(service string) string {
	return fmt.Sprintf("/services/%s/", service)
}

func serviceKey(service, addr string) string {
	return servicePrefix(service) + addr
}

func normalizeAdvertiseAddr(addr string) (string, error) {
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
