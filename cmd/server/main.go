package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"geecache"
	"geecache/algo"
	"geecache/registry"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
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

func createGroup(emptyTTL time.Duration, peerRetries int, evictor string) *geecache.Group {
	opts := []geecache.Option{
		geecache.WithCacheTTL(2*time.Minute, 15*time.Second),
		geecache.WithEmptyCache(emptyTTL),
		geecache.WithPeerRetries(peerRetries),
		geecache.WithPeerRetryBackoff(100 * time.Millisecond),
		geecache.WithPeerCircuitBreaker(3, 3*time.Second),
		geecache.WithEvictor(evictorFactory(evictor)),
	}

	return geecache.NewGroupWithOptions(
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
}

type peersPayload struct {
	Peers []string `json:"peers"`
}

type peerManager interface {
	geecache.PeerPicker
	Register(*grpc.Server)
	Peers() []string
}

type peerConfigurator interface {
	Set(peers ...string)
}

func startAPIServer(apiAddr string, group *geecache.Group, peers peerManager) error {
	mux := http.NewServeMux()
	mux.Handle("/api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		view, err := group.Get(key)
		if err != nil {
			status := http.StatusInternalServerError
			if err == geecache.ErrNotFound {
				status = http.StatusNotFound
			} else if err == geecache.ErrGroupClosed {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(view.ByteSlice())
	}))
	mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(group.Stats())
	}))
	mux.Handle("/admin/peers", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(peersPayload{Peers: peers.Peers()})
		case http.MethodPost:
			manager, ok := peers.(peerConfigurator)
			if !ok {
				http.Error(w, "peer list is managed dynamically", http.StatusConflict)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var payload peersPayload
			if err := json.Unmarshal(body, &payload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if len(payload.Peers) == 0 {
				http.Error(w, "peers is required", http.StatusBadRequest)
				return
			}
			manager.Set(payload.Peers...)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(peersPayload{Peers: peers.Peers()})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	log.Println("frontend server is running at", apiAddr)
	return http.ListenAndServe(apiAddr, mux)
}

func startCacheServer(addr string, peers peerManager) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	peers.Register(server)

	log.Println("geecache grpc server is running at", addr)
	return server.Serve(lis)
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
		addr        string
		peers       string
		useEtcd     bool
		etcd        string
		serviceName string
		api         bool
		apiAddr     string
		emptyTTL    time.Duration
		peerRetries int
		evictor     string
	)

	flag.StringVar(&addr, "addr", "localhost:8001", "Current cache node address")
	flag.StringVar(&peers, "peers", "localhost:8001,localhost:8002,localhost:8003", "Comma-separated cache peers")
	flag.BoolVar(&useEtcd, "use-etcd", false, "Use etcd service discovery instead of the static peer list")
	flag.StringVar(&etcd, "etcd-endpoints", "localhost:2379", "Comma-separated etcd endpoints")
	flag.StringVar(&serviceName, "service-name", registry.DefaultServiceName, "Service name used for etcd registration/discovery")
	flag.BoolVar(&api, "api", false, "Start an API server")
	flag.StringVar(&apiAddr, "api-addr", "localhost:9999", "API server listen address")
	flag.DurationVar(&emptyTTL, "empty-ttl", 30*time.Second, "TTL for not-found cache entries")
	flag.IntVar(&peerRetries, "peer-retries", 1, "Number of peer retries before local fallback")
	flag.StringVar(&evictor, "evictor", "lru", "Cache eviction algorithm: lru, lfu, lru-k, or arc")
	flag.Parse()

	group := createGroup(emptyTTL, peerRetries, evictor)
	var (
		manager      peerManager
		registration *registry.Registration
	)
	if useEtcd {
		endpoints := splitList(etcd)
		picker, err := geecache.NewEtcdPicker(addr, endpoints, serviceName)
		if err != nil {
			log.Fatalf("create etcd picker: %v", err)
		}
		manager = picker
		registration, err = registry.Register(registry.Config{
			Endpoints:   endpoints,
			ServiceName: serviceName,
		}, addr)
		if err != nil {
			_ = manager.Close()
			log.Fatalf("register service in etcd: %v", err)
		}
		defer registration.Close()
	} else {
		peerList := splitList(peers)
		if len(peerList) == 0 {
			log.Fatal("at least one peer address is required")
		}
		pool := geecache.NewGRPCPool(addr)
		pool.Set(peerList...)
		manager = pool
	}
	defer manager.Close()

	group.RegisterPeers(manager)
	if api {
		go func() {
			if err := startAPIServer(apiAddr, group, manager); err != nil {
				log.Printf("api server stopped: %v", err)
			}
		}()
	}
	if useEtcd {
		fmt.Printf("node=%s discovery=etcd etcd=%v service=%s emptyTTL=%s peerRetries=%d evictor=%s\n",
			addr, splitList(etcd), serviceName, emptyTTL, peerRetries, evictor)
	} else {
		fmt.Printf("node=%s peers=%v emptyTTL=%s peerRetries=%d evictor=%s\n",
			addr, splitList(peers), emptyTTL, peerRetries, evictor)
	}
	if err := startCacheServer(addr, manager); err != nil {
		log.Printf("cache server stopped: %v", err)
	}
}
