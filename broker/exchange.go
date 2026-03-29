package broker

// Exchange type constants used by Type() methods and DeclareExchange.
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

// Destination represents a routable target such as a queue or another exchange.
type Destination interface {
	Name() string
}

// Exchange defines the interface for AMQP exchange types.
// Implementations route messages to bound destinations based on
// exchange-specific matching rules.
type Exchange interface {
	Name() string
	Type() string
	IsDurable() bool
	IsAutoDelete() bool
	Bind(dest Destination, routingKey string, args map[string]interface{}) error
	Unbind(dest Destination, routingKey string, args map[string]interface{}) error
	Route(msg *Message, results map[Destination]struct{})
}

// exchangeBase holds the common fields shared by all exchange implementations.
type exchangeBase struct {
	name       string
	durable    bool
	autoDelete bool
}

// Name returns the exchange name.
func (e *exchangeBase) Name() string { return e.name }

// IsDurable reports whether the exchange survives broker restart.
func (e *exchangeBase) IsDurable() bool { return e.durable }

// IsAutoDelete reports whether the exchange is deleted when all bindings are removed.
func (e *exchangeBase) IsAutoDelete() bool { return e.autoDelete }
