package broker

import (
	"strings"
	"sync"
)

// segment represents one element in a compiled topic binding pattern.
// The matching algorithm is a recursive chain: each segment holds a
// pointer to the next segment, forming a linked list compiled from
// the dot-separated binding key.
type segment interface {
	match(words []string, pos int) bool
}

// hashSegment matches zero or more words (the '#' wildcard).
type hashSegment struct {
	next segment
}

func (s *hashSegment) match(words []string, pos int) bool {
	if s.next == nil {
		// '#' at end matches everything remaining
		return true
	}

	// Try matching the next segment starting from every remaining position,
	// including the current one (zero words consumed).
	for i := pos; i <= len(words); i++ {
		if s.next.match(words, i) {
			return true
		}
	}

	return false
}

// starSegment matches exactly one word (the '*' wildcard).
type starSegment struct {
	next segment
}

func (s *starSegment) match(words []string, pos int) bool {
	// Must have at least one word to consume.
	if pos >= len(words) {
		return false
	}

	if s.next == nil {
		// '*' at end: matches if this is the last word.
		return pos == len(words)-1
	}

	return s.next.match(words, pos+1)
}

// literalSegment matches a specific word exactly.
type literalSegment struct {
	word string
	next segment
}

func (s *literalSegment) match(words []string, pos int) bool {
	if pos >= len(words) {
		return false
	}

	if words[pos] != s.word {
		return false
	}

	if s.next == nil {
		return pos == len(words)-1
	}

	return s.next.match(words, pos+1)
}

// topicPattern is a compiled binding key pattern for topic exchange matching.
type topicPattern struct {
	head segment
	raw  string
}

// compilePattern builds a segment chain from a dot-separated binding key.
// The segments are built in reverse order (right to left) so each segment
// holds a pointer to the next segment in the chain.
func compilePattern(bindingKey string) *topicPattern {
	parts := strings.Split(bindingKey, ".")

	var next segment

	for idx := len(parts) - 1; idx >= 0; idx-- {
		switch parts[idx] {
		case "#":
			next = &hashSegment{next: next}
		case "*":
			next = &starSegment{next: next}
		default:
			next = &literalSegment{word: parts[idx], next: next}
		}
	}

	return &topicPattern{head: next, raw: bindingKey}
}

// matches reports whether the routing key matches this compiled pattern.
func (p *topicPattern) matches(routingKey string) bool {
	if p.head == nil {
		return routingKey == ""
	}

	words := strings.Split(routingKey, ".")

	return p.head.match(words, 0)
}

// topicBinding pairs a compiled pattern with its bound destinations.
type topicBinding struct {
	pattern *topicPattern
	dests   map[Destination]struct{}
}

// TopicExchange routes messages using wildcard pattern matching on the
// routing key. Binding keys may contain '*' (exactly one word) and
// '#' (zero or more words) wildcards.
type TopicExchange struct {
	exchangeBase
	mu       sync.RWMutex
	bindings []*topicBinding
}

// NewTopicExchange creates a topic exchange with the given properties.
func NewTopicExchange(name string, durable, autoDelete bool) *TopicExchange {
	return &TopicExchange{
		exchangeBase: exchangeBase{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
		},
	}
}

// Type returns "topic".
func (e *TopicExchange) Type() string { return ExchangeTopic }

// Bind adds a destination for the given binding key pattern.
func (e *TopicExchange) Bind(dest Destination, routingKey string, _ map[string]interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Look for an existing binding with the same pattern.
	for _, b := range e.bindings {
		if b.pattern.raw == routingKey {
			b.dests[dest] = struct{}{}
			return nil
		}
	}

	// New pattern: compile and add.
	e.bindings = append(e.bindings, &topicBinding{
		pattern: compilePattern(routingKey),
		dests:   map[Destination]struct{}{dest: {}},
	})

	return nil
}

// Unbind removes a destination for the given binding key pattern.
func (e *TopicExchange) Unbind(dest Destination, routingKey string, _ map[string]interface{}) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for idx, binding := range e.bindings {
		if binding.pattern.raw != routingKey {
			continue
		}

		delete(binding.dests, dest)

		if len(binding.dests) == 0 {
			// Remove the binding by swapping with last element.
			last := len(e.bindings) - 1
			e.bindings[idx] = e.bindings[last]
			e.bindings[last] = nil // allow GC
			e.bindings = e.bindings[:last]
		}

		return nil
	}

	return nil
}

// Bindings returns a snapshot of all bindings on this exchange.
func (e *TopicExchange) Bindings() []Binding {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var bindings []Binding
	for _, b := range e.bindings {
		for d := range b.dests {
			bindings = append(bindings, Binding{
				Source:      e.name,
				Destination: d.Name(),
				RoutingKey:  b.pattern.raw,
			})
		}
	}

	return bindings
}

// Route adds all destinations whose binding pattern matches the message's
// routing key into results. The map deduplicates destinations that match
// multiple patterns.
func (e *TopicExchange) Route(msg *Message, results map[Destination]struct{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, b := range e.bindings {
		if !b.pattern.matches(msg.RoutingKey) {
			continue
		}

		for d := range b.dests {
			results[d] = struct{}{}
		}
	}
}
