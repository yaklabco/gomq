package broker

import "sync"

// headersBinding stores the binding arguments and destinations for a
// headers exchange binding.
type headersBinding struct {
	args  map[string]interface{}
	dests map[Destination]struct{}
}

// HeadersExchange routes messages based on matching message headers against
// binding arguments. The x-match argument controls whether all or any
// binding arguments must match.
type HeadersExchange struct {
	exchangeBase
	mu       sync.RWMutex
	bindings []*headersBinding
}

// NewHeadersExchange creates a headers exchange with the given properties.
func NewHeadersExchange(name string, durable, autoDelete bool) *HeadersExchange {
	return &HeadersExchange{
		exchangeBase: exchangeBase{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
		},
	}
}

// Type returns "headers".
func (e *HeadersExchange) Type() string { return ExchangeHeaders }

// Bind adds a destination with the given argument map. The routing key is ignored.
func (e *HeadersExchange) Bind(dest Destination, _ string, args map[string]interface{}) error {
	if args == nil {
		args = make(map[string]interface{})
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Look for an existing binding with identical args.
	for _, binding := range e.bindings {
		if argsEqual(binding.args, args) {
			binding.dests[dest] = struct{}{}
			return nil
		}
	}

	e.bindings = append(e.bindings, &headersBinding{
		args:  args,
		dests: map[Destination]struct{}{dest: {}},
	})

	return nil
}

// Unbind removes a destination for the given argument map.
func (e *HeadersExchange) Unbind(dest Destination, _ string, args map[string]interface{}) error {
	if args == nil {
		args = make(map[string]interface{})
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for idx, binding := range e.bindings {
		if !argsEqual(binding.args, args) {
			continue
		}

		delete(binding.dests, dest)

		if len(binding.dests) == 0 {
			last := len(e.bindings) - 1
			e.bindings[idx] = e.bindings[last]
			e.bindings[last] = nil
			e.bindings = e.bindings[:last]
		}

		return nil
	}

	return nil
}

// Route adds destinations whose binding arguments match the message headers.
func (e *HeadersExchange) Route(msg *Message, results map[Destination]struct{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, binding := range e.bindings {
		if !headersMatch(binding.args, msg.Headers) {
			continue
		}

		for dest := range binding.dests {
			results[dest] = struct{}{}
		}
	}
}

// Bindings returns a snapshot of all bindings on this exchange.
func (e *HeadersExchange) Bindings() []Binding {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var bindings []Binding
	for _, hb := range e.bindings {
		for dest := range hb.dests {
			args := make(map[string]interface{}, len(hb.args))
			for k, v := range hb.args {
				args[k] = v
			}
			bindings = append(bindings, Binding{
				Source:      e.name,
				Destination: dest.Name(),
				Arguments:   args,
			})
		}
	}

	return bindings
}

// headersMatch checks whether the message headers satisfy the binding arguments
// according to the x-match mode ("all" or "any").
func headersMatch(bindArgs, msgHeaders map[string]interface{}) bool {
	xMatch := "all"
	if val, ok := bindArgs["x-match"].(string); ok && val != "" {
		xMatch = val
	}

	switch xMatch {
	case "any":
		for key, bindVal := range bindArgs {
			if key == "x-match" {
				continue
			}

			if msgVal, ok := msgHeaders[key]; ok && msgVal == bindVal {
				return true
			}
		}

		return false
	default: // "all"
		for key, bindVal := range bindArgs {
			if key == "x-match" {
				continue
			}

			msgVal, ok := msgHeaders[key]
			if !ok || msgVal != bindVal {
				return false
			}
		}

		return true
	}
}

// argsEqual checks whether two argument maps have identical key-value pairs.
func argsEqual(left, right map[string]interface{}) bool {
	if len(left) != len(right) {
		return false
	}

	for key, leftVal := range left {
		rightVal, ok := right[key]
		if !ok || leftVal != rightVal {
			return false
		}
	}

	return true
}
