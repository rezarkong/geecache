package discovery

import (
	"context"
	"errors"
	"fmt"
	"geecache/cluster"
	"geecache/etcd/registry"
	"geecache/internal/logx"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var errResync = errors.New("etcd discovery resync requested")

func (p *Picker) fetchAllServices() (int64, error) {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	resp, err := p.etcdCli.Get(ctx, registry.ServicePrefix(p.serviceName), clientv3.WithPrefix())
	if err != nil {
		return 0, fmt.Errorf("fetch services from etcd: %w", err)
	}

	members := []cluster.Member{{Addr: p.self, Weight: p.selfWeight}}
	for _, kv := range resp.Kvs {
		member, _ := parseRegistryMember(p.serviceName, string(kv.Key), string(kv.Value))
		if member.Addr == "" {
			continue
		}
		members = append(members, member)
	}
	p.applyMembers(members)
	logx.Event("etcd.discovery", "snapshot_loaded", map[string]interface{}{
		"member_count": len(members),
		"node":         p.self,
		"revision":     revisionOf(resp),
		"service":      p.serviceName,
	})
	if resp.Header == nil {
		return 0, nil
	}
	return resp.Header.Revision, nil
}

func (p *Picker) watchServiceChanges(nextRev int64) {
	for {
		if p.ctx.Err() != nil {
			return
		}
		if nextRev <= 0 {
			rev, err := p.fetchAllServices()
			if err != nil {
				logx.Event("etcd.discovery", "relist_failed", map[string]interface{}{
					"error":   err,
					"node":    p.self,
					"service": p.serviceName,
				})
				if !p.sleepWithContext(p.discoveryRetryBackoff) {
					return
				}
				continue
			}
			nextRev = rev + 1
		}

		err := p.watchFromRevision(nextRev)
		if p.ctx.Err() != nil {
			return
		}
		switch {
		case err == nil:
			return
		case errors.Is(err, errResync):
			logx.Event("etcd.discovery", "resync_requested", map[string]interface{}{
				"node":    p.self,
				"service": p.serviceName,
			})
			nextRev = 0
		default:
			logx.Event("etcd.discovery", "watch_failed", map[string]interface{}{
				"error":   err,
				"node":    p.self,
				"service": p.serviceName,
			})
			nextRev = 0
			if !p.sleepWithContext(p.discoveryRetryBackoff) {
				return
			}
		}
	}
}

func (p *Picker) watchFromRevision(startRev int64) error {
	logx.Event("etcd.discovery", "watch_started", map[string]interface{}{
		"node":      p.self,
		"revision":  startRev,
		"service":   p.serviceName,
		"with_peer": len(p.Peers()),
	})
	watchCh := p.etcdCli.Watch(p.ctx, registry.ServicePrefix(p.serviceName), clientv3.WithPrefix(), clientv3.WithRev(startRev))

	var ticker *time.Ticker
	if p.discoveryResyncInterval > 0 {
		ticker = time.NewTicker(p.discoveryResyncInterval)
		defer ticker.Stop()
	}

	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-tickerChan(ticker):
			return errResync
		case resp, ok := <-watchCh:
			if !ok {
				return fmt.Errorf("etcd watch channel closed")
			}
			if err := resp.Err(); err != nil {
				return fmt.Errorf("etcd watch error at rev %d: %w", startRev, err)
			}
			if resp.Canceled {
				return fmt.Errorf("etcd watch canceled at rev %d", startRev)
			}
			if resp.IsProgressNotify() || len(resp.Events) == 0 {
				continue
			}
			p.handleWatchEvents(resp.Events)
			startRev = resp.Header.Revision + 1
		}
	}
}

func revisionOf(resp *clientv3.GetResponse) int64 {
	if resp == nil || resp.Header == nil {
		return 0
	}
	return resp.Header.Revision
}

func (p *Picker) sleepWithContext(delay time.Duration) bool {
	if delay <= 0 {
		return p.ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-p.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func tickerChan(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}
