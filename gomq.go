// Package gomq provides an embeddable AMQP 0-9-1 message broker.
//
// Use [New] to create a broker with functional options, then call
// [Broker.ListenAndServe] or [Broker.Serve] to accept connections.
package gomq

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/yaklabco/gomq/broker"
	"github.com/yaklabco/gomq/cluster"
	"github.com/yaklabco/gomq/config"
	mgmt "github.com/yaklabco/gomq/http"
	mqttpkg "github.com/yaklabco/gomq/mqtt"
	"github.com/yaklabco/gomq/shovel"
)

// httpReadHeaderTimeout is the maximum duration for reading HTTP
// request headers on the management API.
const httpReadHeaderTimeout = 10 * time.Second

// Broker wraps a broker.Server with a clean embeddable API.
type Broker struct {
	server  *broker.Server
	cfg     *config.Config
	api     *mgmt.API
	mqtt    *mqttpkg.Broker
	shovels *shovel.Store

	replServer *cluster.ReplicationServer
	follower   *cluster.Follower

	mu        sync.Mutex
	addr      net.Addr
	httpAddr  net.Addr
	amqpsAddr net.Addr
	mqttAddr  net.Addr
	adCh      chan struct{}
}

// Option configures a Broker.
type Option func(*config.Config)

// WithDataDir sets the data directory for persistent storage.
func WithDataDir(dir string) Option {
	return func(c *config.Config) {
		c.DataDir = dir
	}
}

// WithAMQPPort sets the AMQP listener port. Use 0 for a random port.
func WithAMQPPort(port int) Option {
	return func(c *config.Config) {
		c.AMQPPort = port
	}
}

// WithBind sets the AMQP bind address.
func WithBind(addr string) Option {
	return func(c *config.Config) {
		c.AMQPBind = addr
	}
}

// WithHTTPPort sets the HTTP management API port. Use 0 for a random port.
// Use -1 to disable the HTTP API.
func WithHTTPPort(port int) Option {
	return func(c *config.Config) {
		c.HTTPPort = port
	}
}

// WithHTTPBind sets the HTTP management API bind address.
func WithHTTPBind(addr string) Option {
	return func(c *config.Config) {
		c.HTTPBind = addr
	}
}

// WithTLS sets the TLS certificate and key file paths for AMQPS.
func WithTLS(certFile, keyFile string) Option {
	return func(c *config.Config) {
		c.TLSCertFile = certFile
		c.TLSKeyFile = keyFile
	}
}

// WithAMQPSPort sets the AMQPS (TLS) listener port. Use -1 to disable.
func WithAMQPSPort(port int) Option {
	return func(c *config.Config) {
		c.AMQPSPort = port
	}
}

// WithMQTTPort sets the MQTT listener port. Use 0 for a random port.
// Use -1 to disable the MQTT listener.
func WithMQTTPort(port int) Option {
	return func(c *config.Config) {
		c.MQTTPort = port
	}
}

// WithMQTTBind sets the MQTT bind address.
func WithMQTTBind(addr string) Option {
	return func(c *config.Config) {
		c.MQTTBind = addr
	}
}

// WithCluster enables clustering.
func WithCluster(enabled bool) Option {
	return func(c *config.Config) {
		c.ClusterEnabled = enabled
	}
}

// WithClusterBind sets the clustering replication bind address.
func WithClusterBind(addr string) Option {
	return func(c *config.Config) {
		c.ClusterBind = addr
	}
}

// WithClusterPort sets the clustering replication port.
func WithClusterPort(port int) Option {
	return func(c *config.Config) {
		c.ClusterPort = port
	}
}

// WithClusterPassword sets the shared secret for replication auth.
func WithClusterPassword(pw string) Option {
	return func(c *config.Config) {
		c.ClusterPassword = pw
	}
}

// WithClusterLeaderURI makes this node a follower of the given leader.
func WithClusterLeaderURI(uri string) Option {
	return func(c *config.Config) {
		c.ClusterLeaderURI = uri
	}
}

// New creates a broker with the given options applied to default config.
func New(opts ...Option) (*Broker, error) {
	cfg := config.Default()
	for _, opt := range opts {
		opt(cfg)
	}

	srv, err := broker.NewServer(cfg)
	if err != nil {
		return nil, fmt.Errorf("create broker server: %w", err)
	}

	shovelStore, err := shovel.NewStore(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create shovel store: %w", err)
	}

	api := mgmt.NewAPI(srv, srv.Users(), shovelStore)

	// Get the default vhost for the MQTT broker bridge.
	defaultVHost, _ := srv.GetVHost("/")
	mqttBrk := mqttpkg.NewBroker(defaultVHost, srv.Users())

	return &Broker{
		server:  srv,
		cfg:     cfg,
		api:     api,
		mqtt:    mqttBrk,
		shovels: shovelStore,
		adCh:    make(chan struct{}),
	}, nil
}

// ListenAndServe binds to the configured address and port, then serves
// connections until the context is cancelled. Also starts the HTTP
// management API if configured.
func (b *Broker) ListenAndServe(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", b.cfg.AMQPBind, b.cfg.AMQPPort)

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	return b.Serve(ctx, ln)
}

// Serve accepts connections on the given listener until the context is
// cancelled. The listener address is available via [Broker.Addr] after
// this method is called. Also starts the HTTP management API and AMQPS
// listener if configured.
func (b *Broker) Serve(ctx context.Context, ln net.Listener) error {
	// Start HTTP management API if port is not -1.
	if b.cfg.HTTPPort >= 0 {
		if err := b.startHTTP(ctx); err != nil {
			return fmt.Errorf("start http api: %w", err)
		}
	}

	// Start AMQPS listener if TLS is configured and port is not -1.
	if b.cfg.TLSCertFile != "" && b.cfg.TLSKeyFile != "" && b.cfg.AMQPSPort >= 0 {
		if err := b.startAMQPS(ctx); err != nil {
			return fmt.Errorf("start amqps: %w", err)
		}
	}

	// Start MQTT listener if port is not -1.
	if b.cfg.MQTTPort >= 0 {
		if err := b.startMQTT(ctx); err != nil {
			return fmt.Errorf("start mqtt: %w", err)
		}
	}

	// Start clustering if enabled.
	if b.cfg.ClusterEnabled {
		if err := b.startCluster(ctx); err != nil {
			return fmt.Errorf("start cluster: %w", err)
		}
	}

	// Start shovels and federation links.
	if err := b.shovels.StartAll(ctx); err != nil {
		log.Printf("start shovels: %v", err)
	}

	// Signal that the broker is ready. This must happen after all
	// auxiliary listeners (HTTP, AMQPS, MQTT) are started so that
	// callers waiting on WaitForAddr can safely read MQTTAddr, etc.
	b.setAddr(ln.Addr())

	return b.server.Serve(ctx, ln)
}

// startAMQPS starts the AMQPS (TLS) listener.
func (b *Broker) startAMQPS(ctx context.Context) error {
	tlsCfg, err := b.server.TLSConfig()
	if err != nil {
		return fmt.Errorf("create TLS config: %w", err)
	}

	addr := fmt.Sprintf("%s:%d", b.cfg.AMQPBind, b.cfg.AMQPSPort)

	lc := net.ListenConfig{}
	tcpLn, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen amqps on %s: %w", addr, err)
	}

	tlsLn := tls.NewListener(tcpLn, tlsCfg)

	b.mu.Lock()
	b.amqpsAddr = tlsLn.Addr()
	b.mu.Unlock()

	go func() {
		// Serve blocks until the context is cancelled; ignore the error
		// since the plain AMQP Serve loop is the primary.
		_ = b.server.Serve(ctx, tlsLn) //nolint:errcheck // secondary TLS listener
	}()

	return nil
}

// AMQPSAddr returns the AMQPS listener address, or nil if TLS is not running.
func (b *Broker) AMQPSAddr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.amqpsAddr
}

// startMQTT starts the MQTT listener.
func (b *Broker) startMQTT(ctx context.Context) error {
	mqttAddr := fmt.Sprintf("%s:%d", b.cfg.MQTTBind, b.cfg.MQTTPort)

	addr, err := b.mqtt.ListenAndServe(ctx, mqttAddr)
	if err != nil {
		return fmt.Errorf("mqtt listen on %s: %w", mqttAddr, err)
	}

	b.mu.Lock()
	b.mqttAddr = addr
	b.mu.Unlock()

	return nil
}

// MQTTAddr returns the MQTT listener address, or nil if MQTT is not running.
func (b *Broker) MQTTAddr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mqttAddr
}

// startHTTP starts the HTTP management API listener.
func (b *Broker) startHTTP(ctx context.Context) error {
	httpAddr := fmt.Sprintf("%s:%d", b.cfg.HTTPBind, b.cfg.HTTPPort)

	lc := net.ListenConfig{}
	httpLn, err := lc.Listen(ctx, "tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("listen http on %s: %w", httpAddr, err)
	}

	b.mu.Lock()
	b.httpAddr = httpLn.Addr()
	b.mu.Unlock()

	httpServer := &http.Server{
		Handler:           b.api.Handler(),
		ReadHeaderTimeout: httpReadHeaderTimeout,
	}

	go func() {
		<-ctx.Done()
		_ = httpServer.Close() //nolint:errcheck // shutting down
	}()

	go func() {
		if serveErr := httpServer.Serve(httpLn); serveErr != nil && serveErr != http.ErrServerClosed {
			// Best-effort HTTP server; AMQP serve loop is the primary.
			_ = serveErr //nolint:errcheck // best-effort HTTP server
		}
	}()

	return nil
}

// Addr returns the AMQP listener address, or nil if the broker is not yet
// listening.
func (b *Broker) Addr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.addr
}

// HTTPAddr returns the HTTP management API listener address, or nil if
// the API is not running.
func (b *Broker) HTTPAddr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.httpAddr
}

// WaitForAddr blocks until the broker starts listening or the context
// is cancelled. Returns nil if the context is cancelled first.
func (b *Broker) WaitForAddr(ctx context.Context) net.Addr {
	select {
	case <-b.adCh:
		return b.Addr()
	case <-ctx.Done():
		return nil
	}
}

// Close shuts down the broker, closing all connections and vhosts.
func (b *Broker) Close() error {
	if b.shovels != nil {
		if err := b.shovels.StopAll(); err != nil {
			log.Printf("stop shovels: %v", err)
		}
	}
	if b.replServer != nil {
		if err := b.replServer.Close(); err != nil {
			log.Printf("close replication server: %v", err)
		}
	}
	return b.server.Close()
}

// startCluster starts clustering as either leader or follower based
// on configuration. If ClusterLeaderURI is empty, this node starts as
// the leader (replication server). Otherwise it connects as a follower.
func (b *Broker) startCluster(ctx context.Context) error {
	if b.cfg.ClusterLeaderURI == "" {
		return b.startAsLeader(ctx)
	}
	return b.startAsFollower(ctx)
}

// startAsLeader starts the replication server.
func (b *Broker) startAsLeader(ctx context.Context) error {
	rs, err := cluster.NewReplicationServer(
		ctx,
		b.cfg.ClusterBind,
		b.cfg.ClusterPort,
		b.cfg.DataDir,
		b.cfg.ClusterPassword,
	)
	if err != nil {
		return fmt.Errorf("create replication server: %w", err)
	}

	b.replServer = rs

	go func() {
		if startErr := rs.Start(ctx); startErr != nil {
			log.Printf("replication server: %v", startErr)
		}
	}()

	return nil
}

// startAsFollower connects to the leader and starts streaming actions.
func (b *Broker) startAsFollower(ctx context.Context) error {
	follower := cluster.NewFollower(
		b.cfg.ClusterLeaderURI,
		b.cfg.DataDir,
		b.cfg.ClusterPassword,
	)

	conn, err := follower.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect to leader %s: %w", b.cfg.ClusterLeaderURI, err)
	}

	b.follower = follower

	go func() {
		if streamErr := follower.StreamActions(ctx, conn); streamErr != nil {
			log.Printf("follower stream: %v", streamErr)
		}
	}()

	return nil
}

func (b *Broker) setAddr(addr net.Addr) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.addr = addr
	select {
	case <-b.adCh:
		// already closed
	default:
		close(b.adCh)
	}
}
