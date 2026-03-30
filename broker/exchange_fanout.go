package broker

import "sync"

// FanoutExchange routes messages to all bound destinations,
// ignoring the routing key entirely.
type FanoutExchange struct {
	exchangeBase
	mu       sync.RWMutex
	bindings map[Destination]struct{}
}

// NewFanoutExchange creates a fanout exchange with the given properties.
func NewFanoutExchange(name string, durable, autoDelete bool) *FanoutExchange {
	return &FanoutExchange{
		exchangeBase: exchangeBase{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
		},
		bindings: make(map[Destination]struct{}),
	}
}

// Type returns "fanout".
func (e *FanoutExchange) Type() string { return ExchangeFanout }

// Bind adds a destination. The routing key and args are ignored.
func (e *FanoutExchange) Bind(dest Destination, _ string, _ map[string]interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.bindings[dest] = struct{}{}

	return nil
}

// Unbind removes a destination. The routing key and args are ignored.
func (e *FanoutExchange) Unbind(dest Destination, _ string, _ map[string]interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.bindings, dest)

	return nil
}

// Bindings returns a snapshot of all bindings on this exchange.
func (e *FanoutExchange) Bindings() []Binding {
	e.mu.RLock()
	defer e.mu.RUnlock()

	bindings := make([]Binding, 0, len(e.bindings))
	for d := range e.bindings {
		bindings = append(bindings, Binding{
			Source:      e.name,
			Destination: d.Name(),
		})
	}

	return bindings
}

// Route adds all bound destinations into results regardless of the message's routing key.
func (e *FanoutExchange) Route(_ *Message, results map[Destination]struct{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for d := range e.bindings {
		results[d] = struct{}{}
	}
}
