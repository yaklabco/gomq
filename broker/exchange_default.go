package broker

import "errors"

// ErrDefaultExchangeBind is returned when attempting to bind or unbind
// the default exchange, which routes implicitly by queue name.
var ErrDefaultExchangeBind = errors.New("cannot bind or unbind the default exchange")

// DefaultExchange is the nameless ("") exchange that routes messages
// directly to the queue whose name matches the routing key. It cannot
// be explicitly bound or unbound.
type DefaultExchange struct {
	exchangeBase
	queues map[string]*Queue
}

// NewDefaultExchange creates the default exchange backed by the given
// queue map. The map is not copied; the exchange reads it at routing
// time so newly declared queues are immediately routable.
func NewDefaultExchange(queues map[string]*Queue) *DefaultExchange {
	return &DefaultExchange{
		exchangeBase: exchangeBase{
			name:       "",
			durable:    true,
			autoDelete: false,
		},
		queues: queues,
	}
}

// Type returns "direct" since the default exchange behaves as a direct
// exchange with implicit bindings.
func (e *DefaultExchange) Type() string { return ExchangeDirect }

// Bind always returns an error; the default exchange does not support
// explicit bindings.
func (e *DefaultExchange) Bind(_ Destination, _ string, _ map[string]interface{}) error {
	return ErrDefaultExchangeBind
}

// Unbind always returns an error; the default exchange does not support
// explicit bindings.
func (e *DefaultExchange) Unbind(_ Destination, _ string, _ map[string]interface{}) error {
	return ErrDefaultExchangeBind
}

// Bindings returns an empty list; the default exchange has implicit bindings only.
func (e *DefaultExchange) Bindings() []Binding {
	return nil
}

// Route looks up the queue whose name matches msg.RoutingKey and adds
// it to results if found.
func (e *DefaultExchange) Route(msg *Message, results map[Destination]struct{}) {
	if q, ok := e.queues[msg.RoutingKey]; ok {
		results[q] = struct{}{}
	}
}
