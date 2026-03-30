package broker

import (
	"crypto/sha1" //nolint:gosec // SHA1 used only for directory naming, not security
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/jamesainslie/gomq/storage"
)

// dirPermissions is the file mode used for VHost data directories.
const dirPermissions = 0o750

// Sentinel errors for VHost operations.
var (
	ErrExchangeNotFound    = errors.New("exchange not found")
	ErrQueueNotFound       = errors.New("queue not found")
	ErrPreconditionFailed  = errors.New("precondition failed")
	ErrDefaultExchangeProt = errors.New("cannot modify default exchanges")
	ErrUnknownExchangeType = errors.New("unknown exchange type")
	ErrQueueHasConsumers   = errors.New("queue has consumers")
	ErrQueueNotEmpty       = errors.New("queue is not empty")
	ErrVHostClosed         = errors.New("vhost is closed")
)

// isDefaultExchange reports whether the given name belongs to a
// built-in exchange that cannot be deleted or redeclared.
func isDefaultExchange(name string) bool {
	switch name {
	case "", "amq.direct", "amq.fanout", "amq.topic", "amq.headers":
		return true
	default:
		return false
	}
}

// VHost is the namespace that holds exchanges, queues, and manages
// message routing. It corresponds to a single AMQP virtual host.
type VHost struct {
	mu        sync.RWMutex
	name      string
	exchanges map[string]Exchange
	queues    map[string]*Queue
	dataDir   string
	closed    bool
}

// NewVHost creates a virtual host, initialising its data directory and
// default exchanges. The data directory is created at
// dataDir/vhosts/<sha1(name)>/.
func NewVHost(name string, dataDir string) (*VHost, error) {
	dirHash := fmt.Sprintf("%x", sha1.Sum([]byte(name))) //nolint:gosec // SHA1 for directory naming only
	vhostDir := filepath.Join(dataDir, "vhosts", dirHash)

	if err := os.MkdirAll(filepath.Join(vhostDir, "queues"), dirPermissions); err != nil {
		return nil, fmt.Errorf("create vhost data directory: %w", err)
	}

	vh := &VHost{
		name:      name,
		exchanges: make(map[string]Exchange),
		queues:    make(map[string]*Queue),
		dataDir:   vhostDir,
	}

	vh.initDefaultExchanges()

	return vh, nil
}

// Name returns the virtual host name.
func (v *VHost) Name() string { return v.name }

// DataDir returns the on-disk path for this virtual host's data.
func (v *VHost) DataDir() string { return v.dataDir }

// initDefaultExchanges creates the standard AMQP default exchanges.
func (v *VHost) initDefaultExchanges() {
	v.exchanges[""] = NewDefaultExchange(v.queues)
	v.exchanges["amq.direct"] = NewDirectExchange("amq.direct", true, false)
	v.exchanges["amq.fanout"] = NewFanoutExchange("amq.fanout", true, false)
	v.exchanges["amq.topic"] = NewTopicExchange("amq.topic", true, false)
	v.exchanges["amq.headers"] = NewHeadersExchange("amq.headers", true, false)
}

// --- Exchange operations ---

// DeclareExchange creates a new exchange or returns an existing one if
// it was declared with identical settings. Returns ErrPreconditionFailed
// if the existing exchange has different settings.
func (v *VHost) DeclareExchange(name, typ string, durable, autoDelete bool, _ map[string]interface{}) (Exchange, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.closed {
		return nil, ErrVHostClosed
	}

	if existing, ok := v.exchanges[name]; ok {
		if existing.Type() != typ || existing.IsDurable() != durable || existing.IsAutoDelete() != autoDelete {
			return nil, fmt.Errorf("exchange %q: %w", name, ErrPreconditionFailed)
		}

		return existing, nil
	}

	ex, err := newExchange(name, typ, durable, autoDelete)
	if err != nil {
		return nil, err
	}

	v.exchanges[name] = ex

	return ex, nil
}

// DeleteExchange removes an exchange by name. Default exchanges cannot
// be deleted.
func (v *VHost) DeleteExchange(name string, _ bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.closed {
		return ErrVHostClosed
	}

	if isDefaultExchange(name) {
		return fmt.Errorf("exchange %q: %w", name, ErrDefaultExchangeProt)
	}

	if _, ok := v.exchanges[name]; !ok {
		return fmt.Errorf("exchange %q: %w", name, ErrExchangeNotFound)
	}

	delete(v.exchanges, name)

	return nil
}

// GetExchange returns the exchange with the given name and a boolean
// indicating whether it was found.
func (v *VHost) GetExchange(name string) (Exchange, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	ex, ok := v.exchanges[name]

	return ex, ok
}

// --- Queue operations ---

// DeclareQueue creates a new queue or returns an existing one if declared
// with identical settings. Returns ErrPreconditionFailed on mismatch.
func (v *VHost) DeclareQueue(name string, durable, exclusive, autoDelete bool, args map[string]interface{}) (*Queue, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.closed {
		return nil, ErrVHostClosed
	}

	if existing, ok := v.queues[name]; ok {
		if existing.IsDurable() != durable || existing.IsExclusive() != exclusive || existing.IsAutoDelete() != autoDelete {
			return nil, fmt.Errorf("queue %q: %w", name, ErrPreconditionFailed)
		}

		return existing, nil
	}

	queue, err := NewQueue(name, durable, exclusive, autoDelete, args, v.dataDir)
	if err != nil {
		return nil, fmt.Errorf("declare queue %q: %w", name, err)
	}

	queue.vhost = v
	v.queues[name] = queue

	return queue, nil
}

// DeleteQueue removes a queue and returns the number of messages that
// were in it. Honours ifUnused (error if consumers) and ifEmpty (error
// if messages present).
func (v *VHost) DeleteQueue(name string, ifUnused, ifEmpty bool) (uint32, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.closed {
		return 0, ErrVHostClosed
	}

	queue, ok := v.queues[name]
	if !ok {
		return 0, fmt.Errorf("queue %q: %w", name, ErrQueueNotFound)
	}

	if ifUnused && queue.ConsumerCount() > 0 {
		return 0, fmt.Errorf("queue %q: %w", name, ErrQueueHasConsumers)
	}

	// Drain the async inbox so Len() is accurate for the ifEmpty check.
	queue.Drain()
	msgCount := queue.Len()

	if ifEmpty && msgCount > 0 {
		return 0, fmt.Errorf("queue %q has %d messages: %w", name, msgCount, ErrQueueNotEmpty)
	}

	delete(v.queues, name)

	if err := queue.Delete(); err != nil {
		return 0, fmt.Errorf("delete queue %q data: %w", name, err)
	}

	return msgCount, nil
}

// GetQueue returns the queue with the given name and a boolean
// indicating whether it was found.
func (v *VHost) GetQueue(name string) (*Queue, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	queue, ok := v.queues[name]

	return queue, ok
}

// PurgeQueue removes all messages from the named queue and returns the
// number purged.
func (v *VHost) PurgeQueue(name string) (uint32, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return 0, ErrVHostClosed
	}

	queue, ok := v.queues[name]
	if !ok {
		return 0, fmt.Errorf("queue %q: %w", name, ErrQueueNotFound)
	}

	count := queue.Purge(math.MaxInt)

	return uint32(count), nil //nolint:gosec // purge count bounded by queue length
}

// --- Binding operations ---

// BindQueue binds a queue to an exchange with the given routing key
// and arguments.
func (v *VHost) BindQueue(queueName, exchangeName, routingKey string, args map[string]interface{}) error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return ErrVHostClosed
	}

	exchange, ok := v.exchanges[exchangeName]
	if !ok {
		return fmt.Errorf("exchange %q: %w", exchangeName, ErrExchangeNotFound)
	}

	queue, ok := v.queues[queueName]
	if !ok {
		return fmt.Errorf("queue %q: %w", queueName, ErrQueueNotFound)
	}

	if err := exchange.Bind(queue, routingKey, args); err != nil {
		return fmt.Errorf("bind queue %q to exchange %q: %w", queueName, exchangeName, err)
	}

	return nil
}

// UnbindQueue removes a binding between a queue and an exchange.
func (v *VHost) UnbindQueue(queueName, exchangeName, routingKey string, args map[string]interface{}) error {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return ErrVHostClosed
	}

	exchange, ok := v.exchanges[exchangeName]
	if !ok {
		return fmt.Errorf("exchange %q: %w", exchangeName, ErrExchangeNotFound)
	}

	queue, ok := v.queues[queueName]
	if !ok {
		return fmt.Errorf("queue %q: %w", queueName, ErrQueueNotFound)
	}

	if err := exchange.Unbind(queue, routingKey, args); err != nil {
		return fmt.Errorf("unbind queue %q from exchange %q: %w", queueName, exchangeName, err)
	}

	return nil
}

// --- Publishing ---

// defaultRouteResultSize is the initial capacity for route result maps.
const defaultRouteResultSize = 4

// routeResultPool amortises map allocation on the publish hot path.
//
//nolint:gochecknoglobals // sync.Pool is intentionally global for cross-goroutine reuse
var routeResultPool = sync.Pool{
	New: func() any { return make(map[Destination]struct{}, defaultRouteResultSize) },
}

// Publish routes a message through the named exchange. When syncPersist is
// true, the message is written synchronously to the store before returning
// (required for durable queues with publisher confirms). When false, the
// message is enqueued asynchronously for better throughput.
func (v *VHost) Publish(exchangeName, routingKey string, syncPersist bool, msg *storage.Message) error {
	// Snapshot the exchange pointer under the read lock, then release
	// immediately. The exchange has its own internal synchronisation.
	v.mu.RLock()
	if v.closed {
		v.mu.RUnlock()
		return ErrVHostClosed
	}
	exchange, ok := v.exchanges[exchangeName]
	v.mu.RUnlock()

	if !ok {
		return fmt.Errorf("exchange %q: %w", exchangeName, ErrExchangeNotFound)
	}

	// Build the broker message used for routing decisions.
	brokerMsg := &Message{
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Headers:      msg.Properties.Headers,
	}

	poolVal := routeResultPool.Get()
	results, poolOK := poolVal.(map[Destination]struct{})
	if !poolOK {
		results = make(map[Destination]struct{}, defaultRouteResultSize)
	}
	exchange.Route(brokerMsg, results)

	for dest := range results {
		queue, qOk := dest.(*Queue)
		if !qOk {
			continue
		}

		var err error
		if syncPersist && queue.IsDurable() {
			_, err = queue.PublishSync(msg)
		} else {
			_, err = queue.Publish(msg)
		}
		if err != nil {
			// Clear and return map to pool before returning.
			clear(results)
			routeResultPool.Put(results)
			return fmt.Errorf("publish to queue %q: %w", queue.Name(), err)
		}
	}

	clear(results)
	routeResultPool.Put(results)

	return nil
}

// PublishMandatory routes a message through the named exchange and reports
// whether at least one queue received the message. When the first return
// value is false and the caller set the mandatory flag, a Basic.Return
// should be sent to the publishing channel.
func (v *VHost) PublishMandatory(exchangeName, routingKey string, syncPersist bool, msg *storage.Message) (bool, error) {
	v.mu.RLock()
	if v.closed {
		v.mu.RUnlock()
		return false, ErrVHostClosed
	}
	exchange, ok := v.exchanges[exchangeName]
	v.mu.RUnlock()

	if !ok {
		return false, fmt.Errorf("exchange %q: %w", exchangeName, ErrExchangeNotFound)
	}

	brokerMsg := &Message{
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Headers:      msg.Properties.Headers,
	}

	poolVal := routeResultPool.Get()
	results, poolOK := poolVal.(map[Destination]struct{})
	if !poolOK {
		results = make(map[Destination]struct{}, defaultRouteResultSize)
	}
	exchange.Route(brokerMsg, results)

	matched := len(results) > 0

	for dest := range results {
		queue, qOk := dest.(*Queue)
		if !qOk {
			continue
		}

		var pubErr error
		if syncPersist && queue.IsDurable() {
			_, pubErr = queue.PublishSync(msg)
		} else {
			_, pubErr = queue.Publish(msg)
		}
		if pubErr != nil {
			clear(results)
			routeResultPool.Put(results)
			return matched, fmt.Errorf("publish to queue %q: %w", queue.Name(), pubErr)
		}
	}

	clear(results)
	routeResultPool.Put(results)
	return matched, nil
}

// --- Lifecycle ---

// Close closes all queues and marks the vhost as closed. It is safe
// to call multiple times.
func (v *VHost) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.closed {
		return nil
	}

	v.closed = true

	var firstErr error

	for name, queue := range v.queues {
		if err := queue.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close queue %q: %w", name, err)
		}
	}

	// Clear maps so GC can reclaim exchange/queue objects.
	clear(v.exchanges)
	clear(v.queues)

	return firstErr
}

// newExchange creates an exchange of the given type.
func newExchange(name, typ string, durable, autoDelete bool) (Exchange, error) {
	switch typ {
	case ExchangeDirect:
		return NewDirectExchange(name, durable, autoDelete), nil
	case ExchangeFanout:
		return NewFanoutExchange(name, durable, autoDelete), nil
	case ExchangeTopic:
		return NewTopicExchange(name, durable, autoDelete), nil
	case ExchangeHeaders:
		return NewHeadersExchange(name, durable, autoDelete), nil
	default:
		return nil, fmt.Errorf("type %q: %w", typ, ErrUnknownExchangeType)
	}
}
