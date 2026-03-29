package broker

import "sync"

// DirectExchange routes messages to destinations whose binding key
// exactly matches the message routing key.
type DirectExchange struct {
	exchangeBase
	mu       sync.RWMutex
	bindings map[string]map[Destination]struct{}
}

// NewDirectExchange creates a direct exchange with the given properties.
func NewDirectExchange(name string, durable, autoDelete bool) *DirectExchange {
	return &DirectExchange{
		exchangeBase: exchangeBase{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
		},
		bindings: make(map[string]map[Destination]struct{}),
	}
}

// Type returns "direct".
func (e *DirectExchange) Type() string { return ExchangeDirect }

// Bind adds a destination for the given routing key.
func (e *DirectExchange) Bind(dest Destination, routingKey string, _ map[string]interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	dests, ok := e.bindings[routingKey]
	if !ok {
		dests = make(map[Destination]struct{})
		e.bindings[routingKey] = dests
	}

	dests[dest] = struct{}{}

	return nil
}

// Unbind removes a destination for the given routing key.
func (e *DirectExchange) Unbind(dest Destination, routingKey string, _ map[string]interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	dests, ok := e.bindings[routingKey]
	if !ok {
		return nil
	}

	delete(dests, dest)

	if len(dests) == 0 {
		delete(e.bindings, routingKey)
	}

	return nil
}

// Route adds all destinations bound to the message's routing key into results.
func (e *DirectExchange) Route(msg *Message, results map[Destination]struct{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	dests, ok := e.bindings[msg.RoutingKey]
	if !ok {
		return
	}

	for d := range dests {
		results[d] = struct{}{}
	}
}
