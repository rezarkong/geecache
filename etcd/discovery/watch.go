package discovery

import (
	"context"
	"errors"
	"fmt"
	"geecache/cluster"
	"geecache/etcd/registry"
	"log"
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
				log.Printf("[EtcdPicker %s] discovery relist failed: %v", p.self, err)
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
			nextRev = 0
		default:
			log.Printf("[EtcdPicker %s] discovery watch failed: %v", p.self, err)
			nextRev = 0
			if !p.sleepWithContext(p.discoveryRetryBackoff) {
				return
			}
		}
	}
}

func (p *Picker) watchFromRevision(startRev int64) error {
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
