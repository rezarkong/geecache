package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"geecache"
	"geecache/algo"
	"geecache/registry"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func evictorFactory(name string) func() algo.Evictor {
	switch strings.ToLower(name) {
	case "lfu":
		return func() algo.Evictor { return algo.NewLFU() }
	case "lru-k", "lruk":
		return func() algo.Evictor { return algo.NewLRUK(2) }
	case "arc":
		return func() algo.Evictor { return algo.NewARC() }
	default:
		return func() algo.Evictor { return algo.NewLRU() }
	}
}

func createGroup(emptyTTL time.Duration, peerRetries int, evictor string, bloomItems int, bloomFP float64, bloomRejectOnMiss bool) *geecache.Group {
	opts := []geecache.Option{
		geecache.WithCacheTTL(2*time.Minute, 15*time.Second),
		geecache.WithEmptyCache(emptyTTL),
		geecache.WithPeerRetries(peerRetries),
		geecache.WithPeerRetryBackoff(100 * time.Millisecond),
		geecache.WithPeerCircuitBreaker(3, 3*time.Second),
		geecache.WithEvictor(evictorFactory(evictor)),
	}
	if bloomItems > 0 {
		filter, err := geecache.NewBloomFilter(bloomItems, bloomFP)
		if err != nil {
			log.Fatalf("create bloom filter: %v", err)
		}
		opts = append(opts, geecache.WithBloomFilter(filter))
		if bloomRejectOnMiss {
			opts = append(opts, geecache.WithBloomRejectOnMiss())
		}
	}

	group := geecache.NewGroupWithOptions(
		"scores",
		2<<10,
		geecache.GetterFunc(func(_ context.Context, key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, geecache.ErrNotFound
		}),
		opts...,
	)
	if bloomItems > 0 {
		keys := make([]string, 0, len(db))
		for key := range db {
			keys = append(keys, key)
		}
		group.AddBloomKeys(keys...)
	}
	return group
}

func splitList(raw string) []string {
	parts := strings.Split(raw, ",")
	peers := make([]string, 0, len(parts))
	for _, peer := range parts {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func main() {
	var (
		addr                  string
		peers                 string
		useEtcd               bool
		etcd                  string
		serviceName           string
		weight                int
		api                   bool
		apiAddr               string
		emptyTTL              time.Duration
		peerRetries           int
		evictor               string
		bloomItems            int
		bloomFalsePositive    float64
		bloomRejectOnMiss     bool
		tlsCert               string
		tlsKey                string
		tlsCA                 string
		tlsServerName         string
		tlsInsecureSkipVerify bool
	)

	flag.StringVar(&addr, "addr", "localhost:8001", "Current cache node address")
	flag.StringVar(&peers, "peers", "localhost:8001,localhost:8002,localhost:8003", "Comma-separated cache peers")
	flag.BoolVar(&useEtcd, "use-etcd", false, "Use etcd service discovery instead of the static peer list")
	flag.StringVar(&etcd, "etcd-endpoints", "localhost:2379", "Comma-separated etcd endpoints")
	flag.StringVar(&serviceName, "service-name", registry.DefaultServiceName, "Service name used for etcd registration/discovery")
	flag.IntVar(&weight, "weight", 1, "Node routing weight used by etcd discovery; static peers can also use addr@weight syntax")
	flag.BoolVar(&api, "api", false, "Start an API server")
	flag.StringVar(&apiAddr, "api-addr", "localhost:9999", "API server listen address")
	flag.DurationVar(&emptyTTL, "empty-ttl", 30*time.Second, "TTL for not-found cache entries")
	flag.IntVar(&peerRetries, "peer-retries", 1, "Number of peer retries before local fallback")
	flag.StringVar(&evictor, "evictor", "lru", "Cache eviction algorithm: lru, lfu, lru-k, or arc")
	flag.IntVar(&bloomItems, "bloom-items", 0, "Expected item count for the optional bloom filter; 0 disables it")
	flag.Float64Var(&bloomFalsePositive, "bloom-fp-rate", 0.01, "Target false-positive rate for the optional bloom filter")
	flag.BoolVar(&bloomRejectOnMiss, "bloom-reject-on-miss", false, "Reject keys that are definitely absent from the bloom filter")
	flag.StringVar(&tlsCert, "tls-cert", "", "TLS certificate file for gRPC server and peer client auth")
	flag.StringVar(&tlsKey, "tls-key", "", "TLS private key file for gRPC server and peer client auth")
	flag.StringVar(&tlsCA, "tls-ca", "", "Optional CA file used to verify peer certificates")
	flag.StringVar(&tlsServerName, "tls-server-name", "", "Optional TLS server name override for peer dialing")
	flag.BoolVar(&tlsInsecureSkipVerify, "tls-insecure-skip-verify", false, "Skip peer certificate verification")
	flag.Parse()

	group := createGroup(emptyTTL, peerRetries, evictor, bloomItems, bloomFalsePositive, bloomRejectOnMiss)
	groupMgr, err := geecache.NewGroupManager(group)
	if err != nil {
		log.Fatalf("create group manager: %v", err)
	}

	var tlsOpts *geecache.TLSOptions
	if tlsCert != "" || tlsKey != "" || tlsCA != "" || tlsServerName != "" || tlsInsecureSkipVerify {
		tlsOpts = &geecache.TLSOptions{
			CertFile:           tlsCert,
			KeyFile:            tlsKey,
			CAFile:             tlsCA,
			ServerName:         tlsServerName,
			InsecureSkipVerify: tlsInsecureSkipVerify,
		}
	}

	server, err := geecache.NewServer(geecache.ServerOptions{
		Addr:        addr,
		EnableAPI:   api,
		APIAddr:     apiAddr,
		UseEtcd:     useEtcd,
		StaticPeers: splitList(peers),
		ServiceName: serviceName,
		Registry: registry.Config{
			Endpoints:   splitList(etcd),
			ServiceName: serviceName,
			Weight:      weight,
		},
		Groups: groupMgr,
		TLS:    tlsOpts,
	})
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if useEtcd {
		fmt.Printf("node=%s discovery=etcd etcd=%v service=%s weight=%d emptyTTL=%s peerRetries=%d evictor=%s bloomItems=%d bloomRejectOnMiss=%t\n",
			addr, splitList(etcd), serviceName, weight, emptyTTL, peerRetries, evictor, bloomItems, bloomRejectOnMiss)
	} else {
		fmt.Printf("node=%s peers=%v emptyTTL=%s peerRetries=%d evictor=%s bloomItems=%d bloomRejectOnMiss=%t\n",
			addr, splitList(peers), emptyTTL, peerRetries, evictor, bloomItems, bloomRejectOnMiss)
	}
	if err := server.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("server stopped: %v", err)
	}
}
