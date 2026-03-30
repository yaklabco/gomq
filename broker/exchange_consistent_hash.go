package broker

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// ConsistentHashExchange distributes messages across bound destinations
// using consistent hashing of the routing key. The binding routing key is
// interpreted as a weight (integer), determining how many virtual nodes
// the destination occupies on the hash ring.
type ConsistentHashExchange struct {
	exchangeBase
	mu   sync.RWMutex
	ring hashRing

	// bindings tracks all (destination, weight) pairs for Bindings() and Unbind().
	bindings []consistentHashBinding
}

type consistentHashBinding struct {
	dest   Destination
	weight uint32
}

// NewConsistentHashExchange creates a consistent-hash exchange.
func NewConsistentHashExchange(name string, durable, autoDelete bool) *ConsistentHashExchange {
	return &ConsistentHashExchange{
		exchangeBase: exchangeBase{
			name:       name,
			durable:    durable,
			autoDelete: autoDelete,
		},
	}
}

// Type returns "x-consistent-hash".
func (e *ConsistentHashExchange) Type() string { return ExchangeConsistentHash }

// Bind adds a destination to the hash ring. The routingKey is the weight
// (must be a positive integer). Returns an error if the weight is invalid.
func (e *ConsistentHashExchange) Bind(dest Destination, routingKey string, _ map[string]interface{}) error {
	weight, err := strconv.ParseUint(routingKey, 10, 32)
	if err != nil || weight == 0 {
		return fmt.Errorf("consistent-hash binding weight must be a positive integer, got %q", routingKey)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	w := uint32(weight)
	e.ring.add(dest.Name(), w, dest)
	e.bindings = append(e.bindings, consistentHashBinding{dest: dest, weight: w})

	return nil
}

// Unbind removes a destination from the hash ring using its name and weight.
func (e *ConsistentHashExchange) Unbind(dest Destination, routingKey string, _ map[string]interface{}) error {
	weight, err := strconv.ParseUint(routingKey, 10, 32)
	if err != nil {
		return fmt.Errorf("consistent-hash unbind weight must be a positive integer, got %q", routingKey)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	w := uint32(weight)
	e.ring.remove(dest.Name(), w)

	// Remove from bindings list.
	for idx := range e.bindings {
		if e.bindings[idx].dest == dest && e.bindings[idx].weight == w {
			e.bindings = append(e.bindings[:idx], e.bindings[idx+1:]...)
			break
		}
	}

	return nil
}

// Route hashes the message's routing key and routes to the closest destination
// on the hash ring.
func (e *ConsistentHashExchange) Route(msg *Message, results map[Destination]struct{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	dest := e.ring.get(msg.RoutingKey)
	if dest != nil {
		results[dest] = struct{}{}
	}
}

// Bindings returns a snapshot of all bindings on this exchange.
func (e *ConsistentHashExchange) Bindings() []Binding {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]Binding, 0, len(e.bindings))
	for _, binding := range e.bindings {
		out = append(out, Binding{
			Source:      e.name,
			Destination: binding.dest.Name(),
			RoutingKey:  strconv.FormatUint(uint64(binding.weight), 10),
		})
	}
	return out
}

// replicasPerWeight is the number of virtual nodes added per unit of weight.
// Higher values improve distribution uniformity at the cost of more ring entries.
const replicasPerWeight = 100

// hashRing implements a consistent hash ring with virtual nodes.
// Each real destination is added with N virtual nodes (where N = weight * replicasPerWeight).
// Messages are routed to the nearest node clockwise on the ring.
type hashRing struct {
	nodes []ringNode // sorted by hash
}

type ringNode struct {
	hash uint32
	dest Destination
}

// add inserts virtual nodes for the given destination onto the ring.
// The number of virtual nodes is weight * replicasPerWeight.
func (hr *hashRing) add(key string, weight uint32, dest Destination) {
	totalNodes := weight * replicasPerWeight
	for vnode := range totalNodes {
		vnodeKey := fmt.Sprintf("%s.%d", key, vnode)
		hash := crc32.ChecksumIEEE([]byte(vnodeKey))
		hr.nodes = append(hr.nodes, ringNode{hash: hash, dest: dest})
	}
	sort.Slice(hr.nodes, func(i, j int) bool { return hr.nodes[i].hash < hr.nodes[j].hash })
}

// remove deletes all virtual nodes for the given key and weight from the ring.
func (hr *hashRing) remove(key string, weight uint32) {
	totalNodes := weight * replicasPerWeight
	toRemove := make(map[uint32]struct{}, totalNodes)
	for vnode := range totalNodes {
		vnodeKey := fmt.Sprintf("%s.%d", key, vnode)
		hash := crc32.ChecksumIEEE([]byte(vnodeKey))
		toRemove[hash] = struct{}{}
	}

	filtered := hr.nodes[:0]
	for _, node := range hr.nodes {
		if _, skip := toRemove[node.hash]; !skip {
			filtered = append(filtered, node)
		}
	}
	hr.nodes = filtered
}

// get returns the destination for the given key by finding the nearest
// clockwise node on the ring. Returns nil if the ring is empty.
func (hr *hashRing) get(key string) Destination {
	if len(hr.nodes) == 0 {
		return nil
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Binary search for the first node with hash >= key hash.
	idx := sort.Search(len(hr.nodes), func(i int) bool {
		return hr.nodes[i].hash >= hash
	})

	// Wrap around the ring if no larger hash was found.
	if idx >= len(hr.nodes) {
		idx = 0
	}

	return hr.nodes[idx].dest
}
