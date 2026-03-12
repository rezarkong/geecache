package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"geecache"
	"geecache/algo"
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

func createGroup(emptyTTL time.Duration, peerRetries int, evictor string) *geecache.Group {
	opts := []geecache.Option{
		geecache.WithCacheTTL(2*time.Minute, 15*time.Second),
		geecache.WithEmptyCache(emptyTTL),
		geecache.WithPeerRetries(peerRetries),
		geecache.WithPeerRetryBackoff(100*time.Millisecond),
		geecache.WithPeerCircuitBreaker(3, 3*time.Second),
	}
	switch evictor {
	case "lfu":
		opts = append(opts, geecache.WithEvictor(func() algo.Evictor { return algo.NewLFU() }))
	default:
		opts = append(opts, geecache.WithEvictor(func() algo.Evictor { return algo.NewLRU() }))
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

func startAPIServer(apiAddr string, group *geecache.Group, pool *geecache.GRPCPool) {
	mux := http.NewServeMux()
	mux.Handle("/api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		view, err := group.Get(key)
		if err != nil {
			status := http.StatusInternalServerError
			if err == geecache.ErrNotFound {
				status = http.StatusNotFound
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
			_ = json.NewEncoder(w).Encode(peersPayload{Peers: pool.Peers()})
		case http.MethodPost:
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
			pool.Set(payload.Peers...)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(peersPayload{Peers: pool.Peers()})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	log.Println("frontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr, mux))
}

func startCacheServer(addr string, pool *geecache.GRPCPool) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	pool.Register(server)

	log.Println("geecache grpc server is running at", addr)
	log.Fatal(server.Serve(lis))
}

func splitPeers(raw string) []string {
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
		api         bool
		apiAddr     string
		emptyTTL    time.Duration
		peerRetries int
		evictor     string
	)

	flag.StringVar(&addr, "addr", "localhost:8001", "Current cache node address")
	flag.StringVar(&peers, "peers", "localhost:8001,localhost:8002,localhost:8003", "Comma-separated cache peers")
	flag.BoolVar(&api, "api", false, "Start an API server")
	flag.StringVar(&apiAddr, "api-addr", "localhost:9999", "API server listen address")
	flag.DurationVar(&emptyTTL, "empty-ttl", 30*time.Second, "TTL for not-found cache entries")
	flag.IntVar(&peerRetries, "peer-retries", 1, "Number of peer retries before local fallback")
	flag.StringVar(&evictor, "evictor", "lru", "Cache eviction algorithm: lru or lfu")
	flag.Parse()

	peerList := splitPeers(peers)
	if len(peerList) == 0 {
		log.Fatal("at least one peer address is required")
	}

	group := createGroup(emptyTTL, peerRetries, evictor)
	pool := geecache.NewGRPCPool(addr)
	pool.Set(peerList...)
	group.RegisterPeers(pool)
	if api {
		go startAPIServer(apiAddr, group, pool)
	}
	fmt.Printf("node=%s peers=%v emptyTTL=%s peerRetries=%d evictor=%s\n", addr, peerList, emptyTTL, peerRetries, evictor)
	startCacheServer(addr, pool)
}
