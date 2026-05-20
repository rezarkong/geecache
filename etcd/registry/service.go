package registry

import (
	"fmt"
	"strings"
)

// NormalizeConfig applies the shared etcd defaults used by both registration and discovery.
func NormalizeConfig(cfg Config) Config {
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
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = DefaultConfig.RetryBackoff
	}
	return cfg
}

// ServicePrefix returns the etcd key prefix used for one logical service.
func ServicePrefix(service string) string {
	return fmt.Sprintf("/services/%s/", service)
}

// ServiceKey returns the full etcd key for one registered node.
func ServiceKey(service, addr string) string {
	return ServicePrefix(service) + addr
}

// AddrFromServiceKey extracts one node address from a service key.
func AddrFromServiceKey(key, service string) string {
	prefix := ServicePrefix(service)
	if strings.HasPrefix(key, prefix) {
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}

// ParseMember resolves one service record from an etcd key/value pair.
// It accepts both the JSON payload form and the legacy raw-address form.
func ParseMember(service, key, raw string) (ServiceRecord, bool) {
	if record, ok := DecodeRecord(raw); ok {
		return record, true
	}
	addr := AddrFromServiceKey(key, service)
	if addr == "" {
		return ServiceRecord{}, false
	}
	return ServiceRecord{Addr: addr, Weight: 1}, true
}
