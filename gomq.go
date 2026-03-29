// Package gomq provides an embeddable AMQP 0-9-1 message broker.
//
// Use [New] to create a broker with functional options, then call
// [Broker.ListenAndServe] or [Broker.Serve] to accept connections.
package gomq

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/jamesainslie/gomq/broker"
	"github.com/jamesainslie/gomq/config"
)

// Broker wraps a broker.Server with a clean embeddable API.
type Broker struct {
	server *broker.Server
	cfg    *config.Config

	mu   sync.Mutex
	addr net.Addr
	adCh chan struct{}
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

// New creates a broker with the given options applied to default config.
func New(opts ...Option) (*Broker, error) {
	cfg := config.Default()
	for _, o := range opts {
		o(cfg)
	}

	srv, err := broker.NewServer(cfg)
	if err != nil {
		return nil, fmt.Errorf("create broker server: %w", err)
	}

	return &Broker{
		server: srv,
		cfg:    cfg,
		adCh:   make(chan struct{}),
	}, nil
}

// ListenAndServe binds to the configured address and port, then serves
// connections until the context is cancelled.
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
// this method is called.
func (b *Broker) Serve(ctx context.Context, ln net.Listener) error {
	b.setAddr(ln.Addr())
	return b.server.Serve(ctx, ln)
}

// Addr returns the listener address, or nil if the broker is not yet
// listening.
func (b *Broker) Addr() net.Addr {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.addr
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
