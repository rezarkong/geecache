package geecache

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
)

const httpBenchmarkMissPool = 100000

type noopPeerManager struct{}

func (noopPeerManager) PickPeer(string) (PeerGetter, bool, bool) { return nil, false, false }
func (noopPeerManager) Close() error                             { return nil }
func (noopPeerManager) Register(*grpc.Server)                    {}
func (noopPeerManager) Peers() []string                          { return nil }

func BenchmarkHTTPAPIHot(b *testing.B) {
	restore := silenceHTTPBenchmarkLogs()
	defer restore()

	group := NewGroup("benchmark-http-hot", 2<<10, GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		return []byte("value-" + key), nil
	}))
	defer group.Close()

	server := httptest.NewServer(newServerMux(group, noopPeerManager{}))
	defer server.Close()

	client := server.Client()
	url := server.URL + "/api?key=Tom"

	resp, err := client.Get(url)
	if err != nil {
		b.Fatalf("warm request: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("warm request status=%d", resp.StatusCode)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(url)
			if err != nil {
				b.Fatalf("GET %s: %v", url, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				b.Fatalf("GET %s status=%d", url, resp.StatusCode)
			}
		}
	})
}

func BenchmarkHTTPAPIRandomMisses(b *testing.B) {
	restore := silenceHTTPBenchmarkLogs()
	defer restore()

	group := NewGroup("benchmark-http-miss", 2<<10, GetterFunc(func(_ context.Context, _ string) ([]byte, error) {
		return nil, ErrNotFound
	}))
	defer group.Close()

	server := httptest.NewServer(newServerMux(group, noopPeerManager{}))
	defer server.Close()

	client := server.Client()
	keys := make([]string, httpBenchmarkMissPool)
	for i := range keys {
		keys[i] = fmt.Sprintf("missing-%d", i)
	}

	var next uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := atomic.AddUint64(&next, 1) - 1
			url := server.URL + "/api?key=" + keys[int(idx)%len(keys)]
			resp, err := client.Get(url)
			if err != nil {
				b.Fatalf("GET %s: %v", url, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				b.Fatalf("GET %s status=%d", url, resp.StatusCode)
			}
		}
	})
}

func silenceHTTPBenchmarkLogs() func() {
	prev := log.Writer()
	log.SetOutput(io.Discard)
	return func() {
		log.SetOutput(prev)
	}
}
