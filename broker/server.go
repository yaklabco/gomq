package broker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaklabco/gomq/amqp"
	"github.com/yaklabco/gomq/auth"
	"github.com/yaklabco/gomq/config"
)

// diskCheckInterval is how often the server checks available disk space.
const diskCheckInterval = 10 * time.Second

// Server is the top-level AMQP broker. It accepts TCP connections,
// manages virtual hosts, and coordinates graceful shutdown.
type Server struct {
	mu      sync.Mutex
	cfg     *config.Config
	vhosts  map[string]*VHost
	users   *auth.UserStore
	conns   map[*Connection]struct{}
	closed  bool
	blocked atomic.Bool // true when broker is resource-constrained
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
	s.startDiskChecker(ctx)

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

// TLSConfig builds a *tls.Config from the server's certificate and key paths.
// Returns an error if TLS is not configured or the certificate cannot be loaded.
func (s *Server) TLSConfig() (*tls.Config, error) {
	if s.cfg.TLSCertFile == "" || s.cfg.TLSKeyFile == "" {
		return nil, errors.New("TLS certificate and key paths are required")
	}

	cert, err := tls.LoadX509KeyPair(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS key pair: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ServeTLS accepts TLS connections on the given listener. The listener
// receives raw TCP connections; TLS handshaking is handled per-connection.
// Both TLS and plain connections use the same handleConn path.
func (s *Server) ServeTLS(ctx context.Context, listener net.Listener) error {
	tlsCfg, err := s.TLSConfig()
	if err != nil {
		return fmt.Errorf("create TLS config: %w", err)
	}

	tlsListener := tls.NewListener(listener, tlsCfg)

	return s.Serve(ctx, tlsListener)
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

	// Check vhost connection limit after handshake (when vhost is known).
	if conn.vhost != nil {
		vhostConnCount := s.countVHostConnections(conn.vhost.Name())
		if err := conn.vhost.CheckConnectionLimit(vhostConnCount); err != nil {
			// Reject: too many connections for this vhost.
			_ = conn.sendFrame(0, &amqp.ConnectionClose{ //nolint:errcheck // best-effort rejection
				ReplyCode: amqp.NotAllowed,
				ReplyText: "max connections limit reached for vhost",
			})
			return
		}
	}

	// Run the read loop until the connection closes or errors.
	// ReadLoop returns nil on graceful close and error on protocol faults;
	// in both cases the deferred cleanup closes the connection.
	_ = conn.ReadLoop() //nolint:errcheck,contextcheck // handled by deferred close; consumers manage own contexts
}

// countVHostConnections counts the number of connections using the named vhost.
func (s *Server) countVHostConnections(vhostName string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for conn := range s.conns {
		if conn.VHostName() == vhostName {
			count++
		}
	}

	return count
}

// startDiskChecker starts a goroutine that periodically checks available
// disk space and sends Connection.Blocked/Unblocked to all connections
// when the state changes. It stops when the context is cancelled.
func (s *Server) startDiskChecker(ctx context.Context) {
	if s.cfg.FreeDiskMin <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(diskCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				free, err := freeDiskBytes(s.cfg.DataDir)
				if err != nil {
					continue
				}

				wasBlocked := s.blocked.Load()
				isLow := free < s.cfg.FreeDiskMin

				if isLow && !wasBlocked {
					s.blocked.Store(true)
					s.notifyAllConnections("low disk space", true)
				} else if !isLow && wasBlocked {
					s.blocked.Store(false)
					s.notifyAllConnections("", false)
				}
			}
		}
	}()
}

// notifyAllConnections sends Connection.Blocked or Connection.Unblocked
// to every active connection.
func (s *Server) notifyAllConnections(reason string, blocked bool) {
	s.mu.Lock()
	conns := make([]*Connection, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	for _, conn := range conns {
		if blocked {
			_ = conn.SendBlocked(reason) //nolint:errcheck // best-effort notification
		} else {
			_ = conn.SendUnblocked() //nolint:errcheck // best-effort notification
		}
	}
}

// Blocked reports whether the server is in a resource-constrained state.
func (s *Server) Blocked() bool {
	return s.blocked.Load()
}

// Connections returns a snapshot of all active connections.
func (s *Server) Connections() []*Connection {
	s.mu.Lock()
	defer s.mu.Unlock()

	conns := make([]*Connection, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}

	return conns
}

// CloseConnection finds and closes the connection with the given name.
// Returns true if the connection was found and closed.
func (s *Server) CloseConnection(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for conn := range s.conns {
		if conn.Name() == name {
			conn.Close()
			delete(s.conns, conn)
			return true
		}
	}

	return false
}

// VHosts returns a snapshot of all virtual hosts.
func (s *Server) VHosts() map[string]*VHost {
	s.mu.Lock()
	defer s.mu.Unlock()

	vhosts := make(map[string]*VHost, len(s.vhosts))
	for name, vh := range s.vhosts {
		vhosts[name] = vh
	}

	return vhosts
}

// GetVHost returns the named virtual host and a boolean indicating
// whether it was found.
func (s *Server) GetVHost(name string) (*VHost, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	vh, ok := s.vhosts[name]
	return vh, ok
}

// CreateVHost creates a new virtual host with the given name.
// Returns an error if a vhost with that name already exists.
func (s *Server) CreateVHost(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.vhosts[name]; ok {
		return fmt.Errorf("vhost %q already exists", name)
	}

	vh, err := NewVHost(name, s.cfg.DataDir)
	if err != nil {
		return fmt.Errorf("create vhost %q: %w", name, err)
	}

	s.vhosts[name] = vh
	return nil
}

// DeleteVHost removes a virtual host. The default "/" vhost cannot be deleted.
func (s *Server) DeleteVHost(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if name == "/" {
		return errors.New("cannot delete default vhost")
	}

	vh, ok := s.vhosts[name]
	if !ok {
		return fmt.Errorf("vhost %q: %w", name, ErrVHostNotFound)
	}

	if err := vh.Close(); err != nil {
		return fmt.Errorf("close vhost %q: %w", name, err)
	}

	delete(s.vhosts, name)
	return nil
}

// Users returns the user store.
func (s *Server) Users() *auth.UserStore {
	return s.users
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
