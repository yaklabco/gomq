package shovel

import (
	"context"
	"errors"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Federation header keys.
const (
	// HeaderFederationHops tracks the number of times a message has been
	// forwarded by federation links. Messages are dropped when this value
	// reaches the configured MaxHops.
	HeaderFederationHops = "x-federation-hops"

	// DefaultMaxHops prevents infinite message loops between federated
	// brokers.
	DefaultMaxHops = 1
)

// FederationUpstream describes a remote broker that this broker
// federates with. Messages are consumed from the remote exchange and
// published to the local exchange.
type FederationUpstream struct {
	Name     string
	URI      string
	Exchange string // remote exchange to bind to
	MaxHops  int
	Prefetch int
}

// NewFederationLink creates a shovel that acts as a federation link.
// It consumes from the remote exchange via a temporary queue and
// publishes to the local broker. Messages that have reached MaxHops
// are silently dropped.
func NewFederationLink(upstream *FederationUpstream, localURI, localExchange string) *Shovel {
	if upstream.MaxHops <= 0 {
		upstream.MaxHops = DefaultMaxHops
	}
	if upstream.Prefetch <= 0 {
		upstream.Prefetch = DefaultPrefetch
	}

	src := NewAMQPSource(upstream.URI, "federation."+upstream.Name, upstream.Prefetch)
	dst := &federationDestination{
		inner:    NewAMQPDestination(localURI, localExchange, ""),
		maxHops:  upstream.MaxHops,
		exchange: localExchange,
	}

	return NewShovel("federation-"+upstream.Name, src, dst, AckOnPublish, upstream.Prefetch)
}

// federationDestination wraps an AMQP destination to add hop tracking
// and loop prevention.
type federationDestination struct {
	inner    *AMQPDestination
	maxHops  int
	exchange string
	mu       sync.Mutex
	dropped  int
}

// Connect delegates to the inner AMQP destination.
func (d *federationDestination) Connect(ctx context.Context) error {
	return d.inner.Connect(ctx)
}

// Publish inspects the x-federation-hops header and drops messages
// that have reached MaxHops. Otherwise it increments the counter and
// publishes.
func (d *federationDestination) Publish(msg amqp.Publishing) error {
	hops := extractHops(msg.Headers)

	if hops >= d.maxHops {
		d.mu.Lock()
		d.dropped++
		d.mu.Unlock()
		return nil // silently drop to prevent loop
	}

	if msg.Headers == nil {
		msg.Headers = make(amqp.Table)
	}
	msg.Headers[HeaderFederationHops] = int32(hops + 1) //nolint:gosec // hops bounded by maxHops, safe conversion

	return d.inner.Publish(msg)
}

// Close delegates to the inner AMQP destination.
func (d *federationDestination) Close() error {
	return d.inner.Close()
}

// DroppedCount returns the number of messages dropped due to hop limits.
func (d *federationDestination) DroppedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dropped
}

// extractHops reads the x-federation-hops header value from an AMQP
// table. Returns 0 if the header is absent or not a numeric type.
func extractHops(headers amqp.Table) int {
	if headers == nil {
		return 0
	}

	val, ok := headers[HeaderFederationHops]
	if !ok {
		return 0
	}

	switch hops := val.(type) {
	case int32:
		return int(hops)
	case int64:
		return int(hops)
	case int:
		return hops
	default:
		return 0
	}
}

// FederationConfig holds the configuration for a federation upstream
// as submitted via the HTTP API.
type FederationConfig struct {
	Name          string `json:"name"`
	URI           string `json:"uri"`
	Exchange      string `json:"exchange"`
	LocalExchange string `json:"local_exchange"`
	MaxHops       int    `json:"max_hops"`
	Prefetch      int    `json:"prefetch"`
	VHost         string `json:"vhost"`
}

// Validate checks that the federation config has required fields.
func (c *FederationConfig) Validate() error {
	if c.Name == "" {
		return errors.New("federation link name is required")
	}
	if c.URI == "" {
		return errors.New("federation upstream URI is required")
	}
	return nil
}

// ToUpstream converts a FederationConfig to a FederationUpstream.
func (c *FederationConfig) ToUpstream() *FederationUpstream {
	return &FederationUpstream{
		Name:     c.Name,
		URI:      c.URI,
		Exchange: c.Exchange,
		MaxHops:  c.MaxHops,
		Prefetch: c.Prefetch,
	}
}

// FederationLinkStatus represents the status of a federation link for
// the HTTP API response.
type FederationLinkStatus struct {
	Name     string `json:"name"`
	VHost    string `json:"vhost"`
	Exchange string `json:"exchange"`
	URI      string `json:"uri"`
	MaxHops  int    `json:"max_hops"`
	Status   string `json:"status"`
}

// ScrubURI removes credentials from an AMQP URI for safe display.
func ScrubURI(uri string) string {
	idx := strings.IndexByte(uri, '@')
	if idx < 0 {
		return uri
	}
	prefix := "amqp://***@"
	if strings.HasPrefix(uri, "amqps") {
		prefix = "amqps://***@"
	}
	return prefix + uri[idx+1:]
}

// FederationStatus returns the status representation for a federation
// link shovel.
func FederationStatus(cfg *FederationConfig, shvl *Shovel) *FederationLinkStatus {
	status := StatusStopped
	if shvl != nil {
		status = shvl.Status()
	}
	return &FederationLinkStatus{
		Name:     cfg.Name,
		VHost:    cfg.VHost,
		Exchange: cfg.Exchange,
		URI:      ScrubURI(cfg.URI),
		MaxHops:  cfg.MaxHops,
		Status:   status,
	}
}

// NewFederationLinkFromConfig creates a federation link from a config.
func NewFederationLinkFromConfig(cfg *FederationConfig, localURI string) *Shovel {
	upstream := cfg.ToUpstream()
	localExchange := cfg.LocalExchange
	if localExchange == "" {
		localExchange = cfg.Exchange
	}
	return NewFederationLink(upstream, localURI, localExchange)
}
