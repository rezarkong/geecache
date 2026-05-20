package geecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"geecache/consistenthash"
	"geecache/etcd/registry"
	"geecache/internal/logx"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type serverPeerManager interface {
	PeerPicker
	Register(*grpc.Server)
	Peers() []string
}

type peerConfigurator interface {
	Set(peers ...string)
}

type hashInspector interface {
	HashRingPositions() []consistenthash.RingPosition
	LocateKey(key string) consistenthash.LookupResult
}

type peersPayload struct {
	Peers []string `json:"peers"`
}

type hashRingPayload struct {
	Peers     []string                      `json:"peers"`
	Positions []consistenthash.RingPosition `json:"positions"`
}

type hashLookupPayload struct {
	Peers  []string                    `json:"peers"`
	Lookup consistenthash.LookupResult `json:"lookup"`
}

func writeJSON(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(payload)
}

// GroupManager keeps the groups served by one process together so the server
// can wire peer picking and shutdown consistently.
type GroupManager struct {
	primary *Group
	groups  []*Group
}

// NewGroupManager creates a group manager with one primary group plus optional
// additional groups served by the same process.
func NewGroupManager(primary *Group, others ...*Group) (*GroupManager, error) {
	if primary == nil {
		return nil, fmt.Errorf("primary group is required")
	}

	groups := []*Group{primary}
	seen := map[string]struct{}{primary.name: {}}
	for _, group := range others {
		if group == nil {
			return nil, fmt.Errorf("group must not be nil")
		}
		if _, ok := seen[group.name]; ok {
			return nil, fmt.Errorf("duplicate group %q", group.name)
		}
		seen[group.name] = struct{}{}
		groups = append(groups, group)
	}

	return &GroupManager{
		primary: primary,
		groups:  groups,
	}, nil
}

// Primary returns the primary group used by the HTTP demo endpoints.
func (m *GroupManager) Primary() *Group {
	if m == nil {
		return nil
	}
	return m.primary
}

// Groups returns a copy of the managed groups.
func (m *GroupManager) Groups() []*Group {
	if m == nil {
		return nil
	}
	groups := make([]*Group, len(m.groups))
	copy(groups, m.groups)
	return groups
}

// RegisterPeers wires the same peer picker into all managed groups.
func (m *GroupManager) RegisterPeers(peers PeerPicker) {
	if m == nil {
		return
	}
	for _, group := range m.groups {
		group.RegisterPeers(peers)
	}
}

// Close closes all managed groups.
func (m *GroupManager) Close() error {
	if m == nil {
		return nil
	}
	errs := make([]error, 0, len(m.groups))
	for _, group := range m.groups {
		if err := group.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ServerOptions defines the process-level runtime wiring.
type ServerOptions struct {
	Addr              string
	ServiceName       string
	EnableAPI         bool
	APIAddr           string
	UseEtcd           bool
	StaticPeers       []string
	Registry          registry.Config
	Groups            *GroupManager
	TLS               *TLSOptions
	GRPCServerOptions []grpc.ServerOption
	GRPCDialOptions   []grpc.DialOption
	UnaryInterceptors []grpc.UnaryServerInterceptor
}

// Server owns the runtime modules for one cache process.
type Server struct {
	groupMgr       *GroupManager
	peerMgr        serverPeerManager
	registration   *registry.Registration
	grpcServer     *grpc.Server
	healthServer   *health.Server
	apiServer      *http.Server
	grpcListener   net.Listener
	apiListener    net.Listener
	addr           string
	serviceName    string
	useEtcd        bool
	registryConfig registry.Config

	closeOnce sync.Once
	closeErr  error
}

// NewServer builds the runtime modules and wires groups to peer discovery.
func NewServer(opts ServerOptions) (*Server, error) {
	if opts.Addr == "" {
		return nil, fmt.Errorf("server address is required")
	}
	if opts.Groups == nil || opts.Groups.Primary() == nil {
		return nil, fmt.Errorf("group manager is required")
	}
	if opts.EnableAPI && opts.APIAddr == "" {
		return nil, fmt.Errorf("api address is required when api is enabled")
	}
	if opts.ServiceName == "" {
		if opts.Registry.ServiceName != "" {
			opts.ServiceName = opts.Registry.ServiceName
		} else {
			opts.ServiceName = registry.DefaultServiceName
		}
	}
	if opts.Registry.ServiceName == "" {
		opts.Registry.ServiceName = opts.ServiceName
	}

	serverOpts, err := buildGRPCServerOptions(opts)
	if err != nil {
		return nil, err
	}
	dialOptions, err := buildPeerDialOptions(opts.TLS, opts.GRPCDialOptions)
	if err != nil {
		return nil, err
	}
	manager, err := newServerPeerManager(opts, dialOptions)
	if err != nil {
		return nil, err
	}

	healthServer := health.NewServer()

	server := &Server{
		groupMgr:       opts.Groups,
		peerMgr:        manager,
		grpcServer:     grpc.NewServer(serverOpts...),
		healthServer:   healthServer,
		addr:           opts.Addr,
		serviceName:    opts.ServiceName,
		useEtcd:        opts.UseEtcd,
		registryConfig: opts.Registry,
	}
	if opts.EnableAPI {
		server.apiServer = &http.Server{
			Addr:    opts.APIAddr,
			Handler: newServerMux(opts.Groups.Primary(), manager),
		}
	}

	manager.Register(server.grpcServer)
	healthpb.RegisterHealthServer(server.grpcServer, healthServer)
	server.setHealthStatus(healthpb.HealthCheckResponse_NOT_SERVING)
	opts.Groups.RegisterPeers(manager)

	return server, nil
}

// Run starts the configured listeners and blocks until the server stops.
func (s *Server) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("nil server")
	}

	grpcListener, err := net.Listen("tcp", s.addr)
	if err != nil {
		_ = s.Close()
		return fmt.Errorf("listen grpc on %s: %w", s.addr, err)
	}
	s.grpcListener = grpcListener
	logx.Event("server", "grpc_listening", map[string]interface{}{
		"addr":    grpcListener.Addr().String(),
		"service": s.serviceName,
	})

	if s.apiServer != nil {
		apiListener, err := net.Listen("tcp", s.apiServer.Addr)
		if err != nil {
			_ = grpcListener.Close()
			_ = s.Close()
			return fmt.Errorf("listen api on %s: %w", s.apiServer.Addr, err)
		}
		s.apiListener = apiListener
		logx.Event("server", "api_listening", map[string]interface{}{
			"addr":    apiListener.Addr().String(),
			"service": s.serviceName,
		})
	}

	if ctx != nil {
		go func() {
			<-ctx.Done()
			_ = s.Close()
		}()
	}

	errCh := make(chan error, 2)
	running := 0

	if s.apiServer != nil {
		running++
		go func() {
			err := s.apiServer.Serve(s.apiListener)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			errCh <- err
		}()
	}

	running++
	go func() {
		errCh <- s.grpcServer.Serve(s.grpcListener)
	}()

	s.setHealthStatus(healthpb.HealthCheckResponse_SERVING)
	if s.useEtcd {
		registration, err := registry.Register(s.registryConfig, s.GRPCAddr())
		if err != nil {
			_ = s.Close()
			return fmt.Errorf("register service in etcd: %w", err)
		}
		s.registration = registration
	}
	logx.Event("server", "node_online", map[string]interface{}{
		"api_addr":   listenerAddr(s.apiListener),
		"grpc_addr":  s.GRPCAddr(),
		"service":    s.serviceName,
		"use_etcd":   s.useEtcd,
		"peer_count": len(s.peerMgr.Peers()),
	})

	var firstErr error
	for running > 0 {
		err := <-errCh
		running--
		if err != nil && firstErr == nil {
			firstErr = err
			_ = s.Close()
		}
	}

	if closeErr := s.Close(); closeErr != nil {
		if firstErr != nil {
			return errors.Join(firstErr, closeErr)
		}
		return closeErr
	}
	return firstErr
}

// Close shuts down registration, listeners, peer management, and groups.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}

	s.closeOnce.Do(func() {
		logx.Event("server", "node_stopping", map[string]interface{}{
			"api_addr":  listenerAddr(s.apiListener),
			"grpc_addr": listenerAddr(s.grpcListener),
			"service":   s.serviceName,
		})
		var errs []error

		if s.registration != nil {
			if err := s.registration.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		s.setHealthStatus(healthpb.HealthCheckResponse_NOT_SERVING)

		if s.apiServer != nil && s.apiListener != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := s.apiServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs = append(errs, err)
			}
			cancel()
		}

		if s.grpcServer != nil {
			if s.grpcListener == nil {
				s.grpcServer.Stop()
			} else {
				done := make(chan struct{})
				go func() {
					s.grpcServer.GracefulStop()
					close(done)
				}()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					s.grpcServer.Stop()
					<-done
				}
			}
		}

		if s.apiListener != nil {
			if err := s.apiListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		if s.grpcListener != nil {
			if err := s.grpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}

		if s.peerMgr != nil {
			if err := s.peerMgr.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if s.groupMgr != nil {
			if err := s.groupMgr.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		s.closeErr = errors.Join(errs...)
		logx.Event("server", "node_offline", map[string]interface{}{
			"error":     s.closeErr,
			"grpc_addr": s.addr,
			"service":   s.serviceName,
		})
	})

	return s.closeErr
}

func listenerAddr(listener net.Listener) string {
	if listener == nil {
		return ""
	}
	return listener.Addr().String()
}

// GRPCAddr returns the actual listening address after Run starts.
func (s *Server) GRPCAddr() string {
	if s == nil {
		return ""
	}
	if s.grpcListener != nil {
		return s.grpcListener.Addr().String()
	}
	return s.addr
}

func buildGRPCServerOptions(opts ServerOptions) ([]grpc.ServerOption, error) {
	serverOpts := append([]grpc.ServerOption(nil), opts.GRPCServerOptions...)
	interceptors := []grpc.UnaryServerInterceptor{recoveryUnaryInterceptor, loggingUnaryInterceptor}
	if len(opts.UnaryInterceptors) > 0 {
		interceptors = append(interceptors, opts.UnaryInterceptors...)
	}
	serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(interceptors...))

	if opts.TLS != nil {
		creds, err := loadServerTransportCredentials(opts.TLS)
		if err != nil {
			return nil, err
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	return serverOpts, nil
}

func newServerPeerManager(opts ServerOptions, dialOptions []grpc.DialOption) (serverPeerManager, error) {
	if opts.UseEtcd {
		picker, err := NewEtcdPickerWithOptions(opts.Addr, opts.Registry.Endpoints, opts.Registry.ServiceName, opts.Registry.Weight, dialOptions)
		if err != nil {
			return nil, fmt.Errorf("create etcd picker: %w", err)
		}
		return picker, nil
	}

	if len(opts.StaticPeers) == 0 {
		return nil, fmt.Errorf("at least one peer address is required")
	}

	pool := NewGRPCPoolWithOptions(opts.Addr, dialOptions)
	pool.Set(opts.StaticPeers...)
	return pool, nil
}

func newServerMux(group *Group, peers serverPeerManager) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		view, err := group.Get(key)
		if err != nil {
			status := http.StatusInternalServerError
			if err == ErrNotFound {
				status = http.StatusNotFound
			} else if err == ErrGroupClosed {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(view.ByteSlice())
	}))
	mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, group.Stats())
	}))
	mux.Handle("/admin/peers", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, peersPayload{Peers: peers.Peers()})
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
			writeJSON(w, peersPayload{Peers: peers.Peers()})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	mux.Handle("/admin/hash/ring", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		inspector, ok := peers.(hashInspector)
		if !ok {
			http.Error(w, "hash inspection is unavailable", http.StatusConflict)
			return
		}
		writeJSON(w, hashRingPayload{
			Peers:     peers.Peers(),
			Positions: inspector.HashRingPositions(),
		})
	}))
	mux.Handle("/admin/hash/key", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}
		inspector, ok := peers.(hashInspector)
		if !ok {
			http.Error(w, "hash inspection is unavailable", http.StatusConflict)
			return
		}
		lookup := inspector.LocateKey(key)
		if lookup.Owner == "" {
			http.Error(w, "hash ring is empty", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, hashLookupPayload{
			Peers:  peers.Peers(),
			Lookup: lookup,
		})
	}))
	return mux
}

func (s *Server) setHealthStatus(status healthpb.HealthCheckResponse_ServingStatus) {
	if s == nil || s.healthServer == nil {
		return
	}
	s.healthServer.SetServingStatus("", status)
	if s.serviceName != "" {
		s.healthServer.SetServingStatus(s.serviceName, status)
	}
}
