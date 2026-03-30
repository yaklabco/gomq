// Package gomq provides an embeddable AMQP 0-9-1 message broker.
//
// Use [New] to create a broker with functional options, then call
// [Broker.ListenAndServe] or [Broker.Serve] to accept connections.
package gomq

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jamesainslie/gomq/broker"
	"github.com/jamesainslie/gomq/config"
	mgmt "github.com/jamesainslie/gomq/http"
	mqttpkg "github.com/jamesainslie/gomq/mqtt"
)

// httpReadHeaderTimeout is the maximum duration for reading HTTP
// request headers on the management API.
const httpReadHeaderTimeout = 10 * time.Second

// Broker wraps a broker.Server with a clean embeddable API.
type Broker struct {
	server *broker.Server
	cfg    *config.Config
	api    *mgmt.API
	mqtt   *mqttpkg.Broker

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

	api := mgmt.NewAPI(srv, srv.Users())

	// Get the default vhost for the MQTT broker bridge.
	defaultVHost, _ := srv.GetVHost("/")
	mqttBrk := mqttpkg.NewBroker(defaultVHost, srv.Users())

	return &Broker{
		server: srv,
		cfg:    cfg,
		api:    api,
		mqtt:   mqttBrk,
		adCh:   make(chan struct{}),
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
	return b.server.Close()
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
