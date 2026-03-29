package broker

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/jamesainslie/gomq/auth"
	"github.com/jamesainslie/gomq/config"
)

// Server is the top-level AMQP broker. It accepts TCP connections,
// manages virtual hosts, and coordinates graceful shutdown.
type Server struct {
	mu     sync.Mutex
	cfg    *config.Config
	vhosts map[string]*VHost
	users  *auth.UserStore
	conns  map[*Connection]struct{}
	closed bool
}

// NewServer creates a broker server, initialising the data directory,
// default "/" vhost, and user store.
func NewServer(cfg *config.Config) (*Server, error) {
	if err := os.MkdirAll(cfg.DataDir, dirPermissions); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", cfg.DataDir, err)
	}

	vh, err := NewVHost("/", cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create default vhost: %w", err)
	}

	users, err := auth.NewUserStore(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create user store: %w", err)
	}

	return &Server{
		cfg:    cfg,
		vhosts: map[string]*VHost{"/": vh},
		users:  users,
		conns:  make(map[*Connection]struct{}),
	}, nil
}

// Serve accepts connections on the given listener until the context is
// cancelled. It spawns a goroutine per connection for handshake and
// frame processing. Returns nil on graceful shutdown.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	// Close the listener when the context is cancelled so Accept returns.
	go func() {
		<-ctx.Done()
		_ = listener.Close() // safe: causes Accept to return error
	}()

	for {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			// If the context was cancelled, the listener was closed
			// intentionally, so return without error.
			if ctx.Err() != nil {
				return nil //nolint:nilerr // graceful shutdown, accept error is expected
			}
			return fmt.Errorf("accept: %w", acceptErr)
		}

		go s.handleConn(ctx, conn)
	}
}

// ListenAndServe binds to the configured address and serves connections.
func (s *Server) ListenAndServe(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", s.cfg.AMQPBind, s.cfg.AMQPPort)

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	return s.Serve(ctx, listener)
}

// Close shuts down all connections and vhosts. Safe to call multiple times.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	// Close all connections.
	for conn := range s.conns {
		conn.Close()
	}
	clear(s.conns)

	// Close all vhosts.
	var firstErr error
	for name, vh := range s.vhosts {
		if err := vh.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close vhost %q: %w", name, err)
		}
	}

	return firstErr
}

// handleConn processes a single client connection: handshake then read loop.
func (s *Server) handleConn(_ context.Context, netConn net.Conn) {
	s.configureTCP(netConn)

	conn := NewConnection(netConn, s.cfg, s.vhosts, s.users)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = netConn.Close()
		return
	}
	s.conns[conn] = struct{}{}
	s.mu.Unlock()

	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	if err := conn.Handshake(); err != nil {
		// Handshake failed; connection is closed by defer.
		return
	}

	// Run the read loop until the connection closes or errors.
	// ReadLoop returns nil on graceful close and error on protocol faults;
	// in both cases the deferred cleanup closes the connection.
	_ = conn.ReadLoop() //nolint:errcheck,contextcheck // handled by deferred close; consumers manage own contexts
}

// configureTCP sets TCP socket options on the connection.
func (s *Server) configureTCP(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	if s.cfg.TCPNodelay {
		_ = tcpConn.SetNoDelay(true) //nolint:errcheck // best-effort socket tuning
	}

	if s.cfg.TCPKeepalive {
		_ = tcpConn.SetKeepAlive(true)                         //nolint:errcheck // best-effort socket tuning
		_ = tcpConn.SetKeepAlivePeriod(s.cfg.TCPKeepaliveIdle) //nolint:errcheck // best-effort socket tuning
	}
}
