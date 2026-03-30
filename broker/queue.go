package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaklabco/gomq/storage"
)

const defaultSegmentSize = 8 * 1024 * 1024 // 8 MiB

// maxPriorityLimit is the maximum value for x-max-priority.
const maxPriorityLimit = 255

// Overflow policy values for x-overflow argument.
const (
	overflowDropHead = "drop-head"
	overflowReject   = "reject-publish"
)

// inboxCapacity is the buffered channel size for the async publish inbox.
// Messages beyond this capacity cause back pressure on publishers.
const inboxCapacity = 4096

// writeBatchSize is the maximum number of messages drained from the inbox
// per lock acquisition in writeLoop.
const writeBatchSize = 64

// Queue wraps a storage.MessageStore and adds consumer signaling, limits,
// and AMQP queue semantics such as max-length overflow and exclusive consumers.
type Queue struct {
	mu         sync.RWMutex
	name       string
	durable    bool
	exclusive  bool
	autoDelete bool
	arguments  map[string]interface{}
	store      *storage.MessageStore
	dataDir    string
	consumers  []*consumerStub

	// Signaling: non-blocking send after each successful publish.
	msgNotify chan struct{}
	closed    atomic.Bool

	// Channel-based inbox for lock-free publish path.
	inbox   chan *storage.Message
	inboxWg sync.WaitGroup
	inboxMu sync.Mutex // protects sends to inbox against concurrent close
	fenceMu sync.Mutex
	fences  []chan struct{} // per-Drain fence channels, signaled by writeLoop on nil sentinel

	// Requeued messages waiting to be re-delivered (LIFO order, served first).
	requeued []storage.SegmentPosition

	// Dead letter exchange configuration parsed from arguments.
	deadLetterExchange   string // x-dead-letter-exchange
	deadLetterRoutingKey string // x-dead-letter-routing-key (optional override)
	vhost                *VHost // back-reference for DLX republishing

	// Single active consumer: only one consumer receives messages at a time.
	singleActiveConsumer bool   // x-single-active-consumer
	activeConsumerTag    string // tag of the currently active consumer

	// Delivery limit: messages requeued more than this many times are dead-lettered.
	// deliveryMu is a separate lock to avoid deadlocks with q.mu (which is held
	// during GetFunc callbacks that may trigger Reject via the channel).
	deliveryMu       sync.Mutex
	deliveryLimit    int64            // x-delivery-limit, 0 = unlimited
	deliveryCounters map[string]int64 // maps message segment position string to count

	// Stream queue: when streamMode is true, the queue behaves as an
	// append-only log. Ack is a no-op, consumers track their own offsets.
	streamMode bool         // x-queue-type=stream
	stream     *streamState // non-nil when streamMode is true

	// Priority queue: when maxPriority > 0, messages are stored in per-priority
	// stores and delivered highest-priority first.
	maxPriority    uint8                   // 0 = normal queue, 1-255 = priority queue
	priorityStores []*storage.MessageStore // index = priority level (0..maxPriority)
	ackStores      map[string]*storage.MessageStore

	// Limits parsed from arguments.
	maxLength      int64  // 0 = unlimited (x-max-length)
	maxLengthBytes int64  // 0 = unlimited (x-max-length-bytes)
	messageTTL     int64  // 0 = no TTL (x-message-ttl), milliseconds
	overflow       string // "drop-head" (default) or "reject-publish"
	expiresMs      int64  // 0 = no queue expiry (x-expires), milliseconds
	expireTimer    *time.Timer

	// Policy-applied arguments (separate from user-declared arguments).
	effectiveArgs map[string]interface{}

	// Stats.
	publishCount atomic.Uint64
	deliverCount atomic.Uint64
	ackCount     atomic.Uint64

	// Auto-delete tracking.
	hadConsumers    bool
	markedForDelete atomic.Bool
}

// consumerStub is a minimal consumer record tracked by the queue.
type consumerStub struct {
	Tag       string
	Queue     *Queue
	NoAck     bool
	Exclusive bool
	Priority  int
	consumer  *Consumer     // back-reference for activation and capacity checks
	notify    chan struct{} // capacity-ready signal
}

// hasCapacity reports whether the consumer backing this stub can accept
// another delivery. Returns true if the consumer has no prefetch limit
// or has room below the limit.
func (cs *consumerStub) hasCapacity() bool {
	if cs.consumer == nil {
		return false
	}
	return cs.consumer.HasCapacity()
}

// Sentinel errors for Queue operations.
var (
	// ErrExclusiveQueue is returned when a second consumer is added to a queue
	// that already has an exclusive consumer.
	ErrExclusiveQueue = errors.New("queue has an exclusive consumer")

	// ErrQueueClosed is returned when publishing to a closed queue.
	ErrQueueClosed = errors.New("publish to closed queue")
)

// NewQueue creates a queue backed by a MessageStore in dataDir/queues/<name>/.
// Arguments are parsed for x-max-length, x-max-length-bytes, x-message-ttl,
// x-overflow, and x-max-priority.
func NewQueue(name string, durable, exclusive, autoDelete bool, args map[string]interface{}, dataDir string) (*Queue, error) {
	queueDir := filepath.Join(dataDir, "queues", name)

	queue := &Queue{
		name:       name,
		durable:    durable,
		exclusive:  exclusive,
		autoDelete: autoDelete,
		arguments:  args,
		dataDir:    queueDir,
		msgNotify:  make(chan struct{}, 1),
		inbox:      make(chan *storage.Message, inboxCapacity),
		overflow:   overflowDropHead,
	}

	queue.parseArguments(args)

	if queue.maxPriority > 0 {
		// Priority queue: create one store per priority level (0..maxPriority).
		queue.ackStores = make(map[string]*storage.MessageStore)
		queue.priorityStores = make([]*storage.MessageStore, queue.maxPriority+1)
		for level := range queue.priorityStores {
			pDir := filepath.Join(queueDir, fmt.Sprintf("priority-%d", level))
			ps, err := storage.OpenMessageStore(pDir, defaultSegmentSize)
			if err != nil {
				// Close any stores already opened.
				for prev := range level {
					_ = queue.priorityStores[prev].Close()
				}
				return nil, fmt.Errorf("open priority store %d for queue %q: %w", level, name, err)
			}
			queue.priorityStores[level] = ps
		}
	} else {
		store, err := storage.OpenMessageStore(queueDir, defaultSegmentSize)
		if err != nil {
			return nil, fmt.Errorf("open message store for queue %q: %w", name, err)
		}
		queue.store = store
	}

	if queue.streamMode {
		queue.stream = &streamState{}
	}

	queue.inboxWg.Add(1)
	go queue.writeLoop()

	queue.startExpireTimer()

	return queue, nil
}

// Name returns the queue name, implementing the Destination interface.
func (q *Queue) Name() string { return q.name }

// IsDurable reports whether the queue survives broker restart.
func (q *Queue) IsDurable() bool { return q.durable }

// IsExclusive reports whether the queue is exclusive to one connection.
func (q *Queue) IsExclusive() bool { return q.exclusive }

// IsAutoDelete reports whether the queue is deleted when all consumers disconnect.
func (q *Queue) IsAutoDelete() bool { return q.autoDelete }

// Arguments returns the queue's argument map.
func (q *Queue) Arguments() map[string]interface{} { return q.arguments }

// EffectiveArgs returns the arguments applied by policies. These are
// separate from the user-declared arguments and take effect alongside them.
func (q *Queue) EffectiveArgs() map[string]interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.effectiveArgs == nil {
		return nil
	}

	// Return a copy to prevent mutation.
	result := make(map[string]interface{}, len(q.effectiveArgs))
	for k, v := range q.effectiveArgs {
		result[k] = v
	}
	return result
}

// SetEffectiveArgs updates the policy-applied arguments on the queue.
// A nil map clears the effective arguments.
func (q *Queue) SetEffectiveArgs(args map[string]interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.effectiveArgs = args
}

// MarkedForDelete reports whether the queue has been marked for deletion
// due to auto-delete semantics.
func (q *Queue) MarkedForDelete() bool { return q.markedForDelete.Load() }

// Publish enqueues a message asynchronously via the inbox channel.
// If the inbox is full, it falls back to a synchronous write to avoid
// blocking the caller's goroutine (which may be the connection read loop —
// blocking it would prevent heartbeat processing and cause disconnects).
// Use PublishSync for durable queues or when persistence must be confirmed
// before returning (e.g. publisher confirms).
func (q *Queue) Publish(msg *storage.Message) (bool, error) {
	if q.closed.Load() {
		return false, ErrQueueClosed
	}

	if q.overflow == overflowReject && q.maxLength > 0 && int64(q.totalLen()) >= q.maxLength {
		return false, nil
	}

	owned := *msg
	if len(msg.Body) > 0 {
		owned.Body = make([]byte, len(msg.Body))
		copy(owned.Body, msg.Body)
	}

	q.inboxMu.Lock()
	if q.closed.Load() {
		q.inboxMu.Unlock()
		return false, ErrQueueClosed
	}
	// Non-blocking send: if the inbox is full, fall back to synchronous
	// write. This prevents the connection read loop from blocking on a
	// full inbox, which would stall heartbeat processing and cause the
	// heartbeat goroutine to close the connection.
	select {
	case q.inbox <- &owned:
		q.inboxMu.Unlock()
		q.publishCount.Add(1)
		return true, nil
	default:
		q.inboxMu.Unlock()
	}

	// Inbox full — write synchronously under the queue lock.
	return q.PublishSync(&owned)
}

// PublishSync writes a message directly to the store under the queue lock.
// This guarantees the message is persisted to the mmap segment before
// returning — required for durable queues and publisher confirms.
func (q *Queue) PublishSync(msg *storage.Message) (bool, error) {
	if q.closed.Load() {
		return false, ErrQueueClosed
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.maxLength > 0 && int64(q.totalLen()) >= q.maxLength {
		if q.overflow == overflowReject {
			return false, nil
		}
		_ = q.dropHead() //nolint:errcheck // best-effort overflow eviction
	}

	if q.maxLengthBytes > 0 && int64(q.totalByteSize()) >= q.maxLengthBytes { //nolint:gosec // ByteSize practically bounded well below MaxInt64
		if q.overflow == overflowReject {
			return false, nil
		}
		_ = q.dropHead() //nolint:errcheck // best-effort overflow eviction
	}

	store := q.priorityStore(msg.Properties.Priority)
	sp, err := store.Push(msg)
	if err != nil {
		return false, fmt.Errorf("write message to store: %w", err)
	}

	if q.stream != nil {
		q.stream.recordPosition(sp)
	}

	q.publishCount.Add(1)
	q.signalConsumers()
	return true, nil
}

// writeLoop is the dedicated goroutine that drains the inbox channel and
// writes messages to the store in batches. Batching amortises the cost of
// mutex acquisition across multiple messages.
//
// A nil message acts as a fence sentinel: the writeLoop flushes the current
// batch, then signals the fence channel so that Drain() callers unblock.
func (q *Queue) writeLoop() {
	defer q.inboxWg.Done()

	batch := make([]*storage.Message, 0, writeBatchSize)

	for msg := range q.inbox {
		// nil sentinel means "drain fence" -- signal the waiting Drain caller.
		if msg == nil {
			q.signalAllFences()
			continue
		}

		batch = append(batch[:0], msg)
		sawFence := false

		// Drain additional messages without blocking.
	drain:
		for len(batch) < cap(batch) {
			select {
			case next, ok := <-q.inbox:
				if !ok {
					break drain
				}
				if next == nil {
					sawFence = true
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}

		q.mu.Lock()
		for _, queued := range batch {
			q.publishToStore(queued)
		}
		q.mu.Unlock()

		q.signalConsumers()

		if sawFence {
			q.signalAllFences()
		}
	}
}

// priorityStore returns the MessageStore for the given message priority.
// If this is not a priority queue, it returns the single store.
// Must be called with q.mu held or during construction.
func (q *Queue) priorityStore(priority uint8) *storage.MessageStore {
	if q.maxPriority == 0 {
		return q.store
	}
	p := priority
	if p > q.maxPriority {
		p = q.maxPriority
	}
	return q.priorityStores[p]
}

// totalLen returns the total message count across all stores. Must be called
// with q.mu held.
func (q *Queue) totalLen() uint32 {
	if q.maxPriority == 0 {
		return q.store.Len()
	}
	var total uint32
	for _, ps := range q.priorityStores {
		total += ps.Len()
	}
	return total
}

// totalByteSize returns the total byte size across all stores. Must be called
// with q.mu held.
func (q *Queue) totalByteSize() uint64 {
	if q.maxPriority == 0 {
		return q.store.ByteSize()
	}
	var total uint64
	for _, ps := range q.priorityStores {
		total += ps.ByteSize()
	}
	return total
}

// publishToStore writes a single message to the store, enforcing overflow
// limits. Must be called with q.mu held.
func (q *Queue) publishToStore(msg *storage.Message) {
	// Enforce max-length limit.
	if q.maxLength > 0 && int64(q.totalLen()) >= q.maxLength {
		if q.overflow == overflowReject {
			return
		}
		_ = q.dropHead() //nolint:errcheck // non-fatal; segment GC handles cleanup
	}

	// Enforce max-length-bytes limit.
	if q.maxLengthBytes > 0 && int64(q.totalByteSize()) >= q.maxLengthBytes { //nolint:gosec // ByteSize practically bounded well below MaxInt64
		if q.overflow == overflowReject {
			return
		}
		_ = q.dropHead() //nolint:errcheck // non-fatal; segment GC handles cleanup
	}

	store := q.priorityStore(msg.Properties.Priority)
	sp, _ := store.Push(msg) //nolint:errcheck // non-fatal for individual batch messages

	if q.stream != nil {
		q.stream.recordPosition(sp)
	}
}

// Get shifts one message from the front of the queue (basic.get semantics).
// Requeued messages are served first with the Redelivered flag set.
// For priority queues, messages are shifted from the highest-priority store first.
// Expired messages are dead-lettered or deleted and silently skipped.
// If noAck is true the message is considered immediately consumed. Returns
// false when the queue is empty.
func (q *Queue) Get(noAck bool) (*storage.Envelope, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		// Serve requeued messages first.
		if env, ok := q.shiftRequeued(); ok {
			if q.isExpired(env.Message) {
				_ = q.deadLetterOrDelete(env.SegmentPosition, "expired") //nolint:errcheck // best-effort expiry cleanup
				continue
			}
			q.deliverCount.Add(1)
			if noAck {
				store := q.storeForSP(env.SegmentPosition)
				if delErr := store.Delete(env.SegmentPosition); delErr == nil {
					q.ackCount.Add(1)
				}
			}
			return env, true
		}

		env, ok := q.shiftPriority()
		if !ok {
			return nil, false
		}

		if q.isExpired(env.Message) {
			_ = q.deadLetterOrDelete(env.SegmentPosition, "expired") //nolint:errcheck // best-effort expiry cleanup
			continue
		}

		q.deliverCount.Add(1)

		if noAck {
			store := q.storeForSP(env.SegmentPosition)
			if delErr := store.Delete(env.SegmentPosition); delErr == nil {
				q.ackCount.Add(1)
			}
		}

		return env, true
	}
}

// shiftPriority shifts a message from the highest-priority non-empty store.
// For non-priority queues, this is equivalent to q.store.Shift().
// Must be called with q.mu held.
func (q *Queue) shiftPriority() (*storage.Envelope, bool) {
	if q.maxPriority == 0 {
		return q.store.Shift()
	}
	// Iterate from highest to lowest priority.
	for i := int(q.maxPriority); i >= 0; i-- {
		ps := q.priorityStores[i]
		if env, ok := ps.Shift(); ok {
			q.ackStores[env.SegmentPosition.String()] = ps
			return env, true
		}
	}
	return nil, false
}

// storeForSP returns the store that owns the given segment position.
// For non-priority queues this is the single store. For priority queues,
// the owning store is looked up from the ackStores tracking map.
// Must be called with q.mu held.
func (q *Queue) storeForSP(sp storage.SegmentPosition) *storage.MessageStore {
	if q.maxPriority == 0 {
		return q.store
	}
	if ps, ok := q.ackStores[sp.String()]; ok {
		return ps
	}
	// Fallback: try each store (for requeued messages that may have lost tracking).
	for _, ps := range q.priorityStores {
		if _, err := ps.GetMessage(sp); err == nil {
			return ps
		}
	}
	return q.priorityStores[0]
}

// GetFunc shifts one message from the front of the queue and calls fn with the
// envelope. Requeued messages are served first with the Redelivered flag set.
// Expired messages are dead-lettered or deleted and silently skipped.
// The envelope's Body may alias the mmap region and is only valid during fn.
// If fn returns an error, the message is not consumed and remains available.
// Returns false when the queue is empty.
func (q *Queue) GetFunc(noAck bool, fn func(env *storage.Envelope) error) (bool, error) {
	// Shift a message under the lock, then deliver OUTSIDE the lock.
	// Holding q.mu during socket writes causes deadlocks when the client
	// read loop needs q.mu for nack/reject processing.
	env, ok := q.shiftNext()
	if !ok {
		return false, nil
	}

	if err := fn(env); err != nil {
		// Delivery failed — put message back.
		q.mu.Lock()
		q.requeued = append(q.requeued, env.SegmentPosition)
		q.mu.Unlock()
		return false, err
	}

	q.deliverCount.Add(1)
	if noAck {
		q.mu.Lock()
		store := q.storeForSP(env.SegmentPosition)
		q.mu.Unlock()
		if delErr := store.Delete(env.SegmentPosition); delErr == nil {
			q.ackCount.Add(1)
		}
	}
	return true, nil
}

// shiftNext pops the next available message (requeued first, then store),
// skipping expired messages. The caller receives a fully materialized
// envelope with a copied body — safe to use after the lock is released.
func (q *Queue) shiftNext() (*storage.Envelope, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if env, ok := q.shiftRequeued(); ok {
			if q.isExpired(env.Message) {
				_ = q.deadLetterOrDelete(env.SegmentPosition, "expired") //nolint:errcheck // best-effort
				continue
			}
			return env, true
		}

		if q.maxPriority > 0 {
			env, ok := q.shiftPriority()
			if !ok {
				return nil, false
			}
			if q.isExpired(env.Message) {
				_ = q.deadLetterOrDelete(env.SegmentPosition, "expired") //nolint:errcheck // best-effort
				continue
			}
			return env, true
		}

		env, ok := q.store.Shift()
		if !ok {
			return nil, false
		}
		if q.isExpired(env.Message) {
			_ = q.deadLetterOrDelete(env.SegmentPosition, "expired") //nolint:errcheck // best-effort
			continue
		}
		return env, true
	}
}

// Ack acknowledges a message, deleting it from the store.
// For stream queues, ack is a no-op — messages are retained.
func (q *Queue) Ack(sp storage.SegmentPosition) error {
	if q.streamMode {
		q.ackCount.Add(1)
		return nil
	}

	q.mu.Lock()
	store := q.storeForSP(sp)
	delete(q.ackStores, sp.String())
	q.mu.Unlock()

	if err := store.Delete(sp); err != nil {
		return fmt.Errorf("ack message at %s: %w", sp, err)
	}
	q.ackCount.Add(1)
	return nil
}

// Reject rejects a message. If requeue is true the message is placed back at
// the front of the queue for redelivery with the Redelivered flag set.
// If the delivery limit is configured and the requeue count exceeds it,
// the message is dead-lettered instead of requeued.
// If requeue is false the message is dead-lettered (if a DLX is configured)
// or deleted.
func (q *Queue) Reject(sp storage.SegmentPosition, requeue bool) error {
	if requeue {
		// Check delivery limit before requeuing.
		if q.deliveryLimit > 0 {
			key := sp.String()
			q.deliveryMu.Lock()
			q.deliveryCounters[key]++
			count := q.deliveryCounters[key]
			q.deliveryMu.Unlock()

			if count >= q.deliveryLimit {
				// Delivery limit reached — dead-letter instead of requeue.
				q.deliveryMu.Lock()
				delete(q.deliveryCounters, key)
				q.deliveryMu.Unlock()
				if err := q.deadLetterOrDelete(sp, "delivery-limit"); err != nil {
					return fmt.Errorf("dead-letter message at %s (delivery limit): %w", sp, err)
				}
				return nil
			}
		}

		q.mu.Lock()
		q.requeued = append(q.requeued, sp)
		q.mu.Unlock()
		q.signalConsumers()
		return nil
	}

	// Clean up delivery counter on final rejection.
	if q.deliveryLimit > 0 {
		q.deliveryMu.Lock()
		delete(q.deliveryCounters, sp.String())
		q.deliveryMu.Unlock()
	}

	if err := q.deadLetterOrDelete(sp, "rejected"); err != nil {
		return fmt.Errorf("reject message at %s: %w", sp, err)
	}
	return nil
}

// Purge drains the inbox and then deletes up to limit ready messages from
// the front of the queue, returning the number actually purged.
func (q *Queue) Purge(limit int) int {
	q.Drain()
	if q.maxPriority == 0 {
		return q.store.Purge(limit)
	}
	total := 0
	for i := int(q.maxPriority); i >= 0 && total < limit; i-- {
		total += q.priorityStores[i].Purge(limit - total)
	}
	return total
}

// Len returns the number of messages in the queue, including those
// buffered in the async inbox that have not yet been written to the store.
func (q *Queue) Len() uint32 {
	q.mu.RLock()
	nRequeued := uint32(len(q.requeued)) //nolint:gosec // requeued slice is small
	q.mu.RUnlock()
	return q.totalLen() + uint32(len(q.inbox)) + nRequeued //nolint:gosec // inbox capacity is 4096
}

// PublishCount returns the total number of messages published to this queue.
func (q *Queue) PublishCount() uint64 { return q.publishCount.Load() }

// DeliverCount returns the total number of messages delivered from this queue.
func (q *Queue) DeliverCount() uint64 { return q.deliverCount.Load() }

// AckCount returns the total number of messages acknowledged on this queue.
func (q *Queue) AckCount() uint64 { return q.ackCount.Load() }

// AddConsumer registers a consumer with the queue. It returns an error if
// an exclusive consumer already exists or the new consumer requests exclusivity
// when other consumers are present.
func (q *Queue) AddConsumer(consumer *consumerStub) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, existing := range q.consumers {
		if existing.Exclusive {
			return ErrExclusiveQueue
		}
	}

	if consumer.Exclusive && len(q.consumers) > 0 {
		return ErrExclusiveQueue
	}

	q.consumers = append(q.consumers, consumer)
	q.hadConsumers = true
	q.stopExpireTimer()

	// For single-active-consumer queues, activate the first consumer.
	if q.singleActiveConsumer && q.activeConsumerTag == "" {
		q.activeConsumerTag = consumer.Tag
		if consumer.consumer != nil {
			consumer.consumer.active.Store(true)
		}
	}

	return nil
}

// RemoveConsumer removes the consumer with the given tag. If the queue is
// auto-delete and had consumers but now has none, it is marked for deletion.
// For single-active-consumer queues, the next consumer in line is activated.
func (q *Queue) RemoveConsumer(tag string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, c := range q.consumers {
		if c.Tag == tag {
			q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
			break
		}
	}

	// If the removed consumer was the active one, activate the next.
	if q.singleActiveConsumer && q.activeConsumerTag == tag {
		q.activeConsumerTag = ""
		if len(q.consumers) > 0 {
			next := q.consumers[0]
			q.activeConsumerTag = next.Tag
			if next.consumer != nil {
				next.consumer.active.Store(true)
			}
			// Signal so the newly active consumer wakes up.
			q.signalConsumers()
		}
	}

	if q.autoDelete && q.hadConsumers && len(q.consumers) == 0 {
		q.markedForDelete.Store(true)
	}

	if len(q.consumers) == 0 {
		q.resetExpireTimer()
	}
}

// ConsumerCount returns the number of active consumers.
func (q *Queue) ConsumerCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.consumers)
}

// WaitForMessage blocks until a message is available or the context is cancelled.
// Returns true if a message signal was received, false if the context was cancelled.
func (q *Queue) WaitForMessage(ctx context.Context) bool {
	select {
	case <-q.msgNotify:
		return true
	case <-ctx.Done():
		return false
	}
}

// Close shuts down the writer goroutine, drains remaining messages, and
// closes the underlying message store.
func (q *Queue) Close() error {
	if q.closed.Swap(true) {
		return nil
	}
	q.stopExpireTimer()
	q.inboxMu.Lock()
	close(q.inbox)
	q.inboxMu.Unlock()
	q.inboxWg.Wait()

	if q.maxPriority > 0 {
		var firstErr error
		for _, ps := range q.priorityStores {
			if err := ps.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	return q.store.Close()
}

// Delete closes the queue and removes all data from disk.
func (q *Queue) Delete() error {
	if err := q.Close(); err != nil {
		return fmt.Errorf("close queue %q: %w", q.name, err)
	}
	if err := os.RemoveAll(q.dataDir); err != nil {
		return fmt.Errorf("remove queue data dir %q: %w", q.dataDir, err)
	}
	return nil
}

// Drain blocks until all messages currently in the inbox have been written
// to the store. Each Drain call gets its own fence so concurrent callers
// do not interfere.
func (q *Queue) Drain() {
	if q.closed.Load() {
		return
	}

	fence := make(chan struct{})

	q.fenceMu.Lock()
	q.fences = append(q.fences, fence)
	q.fenceMu.Unlock()

	q.inboxMu.Lock()
	if q.closed.Load() {
		q.inboxMu.Unlock()
		q.signalAllFences()
		return
	}
	q.inbox <- nil // sentinel
	q.inboxMu.Unlock()

	<-fence
}

// signalAllFences unblocks all pending Drain callers.
func (q *Queue) signalAllFences() {
	q.fenceMu.Lock()
	fences := q.fences
	q.fences = nil
	q.fenceMu.Unlock()
	for _, ch := range fences {
		close(ch)
	}
}

// signalConsumers performs a non-blocking send on the message notify channel.
func (q *Queue) signalConsumers() {
	select {
	case q.msgNotify <- struct{}{}:
	default:
	}
}

// hasPriorityConsumers reports whether any consumer on this queue has a
// non-zero priority. Called by consumers to decide whether priority
// yielding logic is needed.
func (q *Queue) hasPriorityConsumers() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, c := range q.consumers {
		if c.Priority != 0 {
			return true
		}
	}
	return false
}

// startExpireTimer starts the queue expiry timer if x-expires is configured
// and no consumers are connected. When the timer fires, the queue is marked
// for deletion.
func (q *Queue) startExpireTimer() {
	if q.expiresMs <= 0 {
		return
	}
	q.expireTimer = time.AfterFunc(time.Duration(q.expiresMs)*time.Millisecond, func() {
		q.markedForDelete.Store(true)
	})
}

// stopExpireTimer cancels the queue expiry timer (e.g., when a consumer connects).
func (q *Queue) stopExpireTimer() {
	if q.expireTimer != nil {
		q.expireTimer.Stop()
		q.expireTimer = nil
	}
}

// resetExpireTimer restarts the queue expiry timer (e.g., when the last consumer
// disconnects). Must be called with q.mu held.
func (q *Queue) resetExpireTimer() {
	q.stopExpireTimer()
	q.startExpireTimer()
}

// isExpired reports whether a message has exceeded the queue's message TTL or
// the per-message expiration. Must be called with q.mu held (or at a point
// where the message is already shifted and owned by the caller).
func (q *Queue) isExpired(msg *storage.BytesMessage) bool {
	// Fast path: no TTL configured and no per-message expiration.
	if q.messageTTL <= 0 && msg.Properties.Expiration == "" {
		return false
	}

	now := time.Now().UnixMilli()
	age := now - msg.Timestamp

	// Queue-level TTL.
	if q.messageTTL > 0 && age > q.messageTTL {
		return true
	}

	// Per-message expiration (AMQP spec: expiration is a string in milliseconds).
	if msg.Properties.Expiration != "" {
		if ttl, err := strconv.ParseInt(msg.Properties.Expiration, 10, 64); err == nil && ttl >= 0 {
			if age > ttl {
				return true
			}
		}
	}

	return false
}

// shiftRequeued pops the last requeued position, reads the message from the
// store, and returns it as a redelivered envelope. Must be called with q.mu held.
func (q *Queue) shiftRequeued() (*storage.Envelope, bool) {
	for len(q.requeued) > 0 {
		// Pop from end (LIFO — most recently requeued served first).
		sp := q.requeued[len(q.requeued)-1]
		q.requeued = q.requeued[:len(q.requeued)-1]

		store := q.storeForSP(sp)
		msg, err := store.GetMessage(sp)
		if err != nil {
			// Message was already deleted or segment cleaned up; skip.
			continue
		}

		return &storage.Envelope{
			SegmentPosition: sp,
			Message:         msg,
			Redelivered:     true,
		}, true
	}
	return nil, false
}

// dropHead shifts the oldest message from the lowest-priority non-empty store
// and either dead-letters or deletes it depending on queue configuration.
func (q *Queue) dropHead() error {
	if q.maxPriority == 0 {
		env, ok := q.store.Shift()
		if !ok {
			return nil
		}
		return q.deadLetterOrDelete(env.SegmentPosition, "maxlen")
	}
	// For priority queues, drop from the lowest-priority non-empty store.
	for i := range q.priorityStores {
		env, ok := q.priorityStores[i].Shift()
		if ok {
			return q.deadLetterOrDeleteStore(q.priorityStores[i], env.SegmentPosition, "maxlen")
		}
	}
	return nil
}

// deadLetterOrDelete republishes the message to the dead-letter exchange if
// one is configured, then deletes it from the store. If no DLX is set, the
// message is simply deleted. The reason is recorded in the x-death header.
func (q *Queue) deadLetterOrDelete(sp storage.SegmentPosition, reason string) error {
	store := q.storeForSP(sp)
	return q.deadLetterOrDeleteStore(store, sp, reason)
}

// deadLetterOrDeleteStore performs dead-letter or delete on a specific store.
func (q *Queue) deadLetterOrDeleteStore(store *storage.MessageStore, sp storage.SegmentPosition, reason string) error {
	if q.deadLetterExchange == "" || q.vhost == nil {
		return store.Delete(sp)
	}

	msg, err := store.GetMessage(sp)
	if err != nil {
		// Cannot read the message; just delete.
		return store.Delete(sp)
	}

	routingKey := q.deadLetterRoutingKey
	if routingKey == "" {
		routingKey = msg.RoutingKey
	}

	// Build x-death entry per AMQP dead-lettering spec.
	xDeath := map[string]interface{}{
		"queue":        q.name,
		"reason":       reason,
		"exchange":     msg.ExchangeName,
		"routing-keys": []interface{}{msg.RoutingKey},
		"count":        int64(1),
		"time":         time.Now().Unix(),
	}

	headers := make(map[string]interface{})
	for k, v := range msg.Properties.Headers {
		headers[k] = v
	}
	headers["x-death"] = []interface{}{xDeath}

	dlMsg := &storage.Message{
		Timestamp:    time.Now().UnixMilli(),
		ExchangeName: q.deadLetterExchange,
		RoutingKey:   routingKey,
		Properties: storage.Properties{
			Headers:    headers,
			Expiration: msg.Properties.Expiration,
		},
		BodySize: msg.BodySize,
		Body:     msg.Body,
	}

	// Publish to the DLX; ignore routing errors (exchange may not exist).
	_ = q.vhost.Publish(q.deadLetterExchange, routingKey, false, dlMsg) //nolint:errcheck // best-effort DLX delivery

	return store.Delete(sp)
}

// parseArguments extracts queue limits from the argument map.
func (q *Queue) parseArguments(args map[string]interface{}) {
	if args == nil {
		return
	}

	if v, ok := args["x-max-length"]; ok {
		q.maxLength = toInt64(v)
	}

	if v, ok := args["x-max-length-bytes"]; ok {
		q.maxLengthBytes = toInt64(v)
	}

	if v, ok := args["x-message-ttl"]; ok {
		q.messageTTL = toInt64(v)
	}

	if v, ok := args["x-overflow"]; ok {
		if s, ok := v.(string); ok {
			q.overflow = s
		}
	}

	q.deadLetterExchange = stringArg(args, "x-dead-letter-exchange")
	q.deadLetterRoutingKey = stringArg(args, "x-dead-letter-routing-key")

	if v, ok := args["x-expires"]; ok {
		q.expiresMs = toInt64(v)
	}

	if v, ok := args["x-single-active-consumer"]; ok {
		if b, bOK := v.(bool); bOK {
			q.singleActiveConsumer = b
		}
	}

	if v, ok := args["x-delivery-limit"]; ok {
		q.deliveryLimit = toInt64(v)
		if q.deliveryLimit > 0 {
			q.deliveryCounters = make(map[string]int64)
		}
	}

	if v, ok := args["x-queue-type"]; ok {
		if s, sOK := v.(string); sOK && s == "stream" {
			q.streamMode = true
		}
	}

	if v, ok := args["x-max-priority"]; ok {
		p := toInt64(v)
		if p > maxPriorityLimit {
			p = maxPriorityLimit
		}
		if p > 0 {
			q.maxPriority = uint8(p) //nolint:gosec // capped at 255 above
		}
	}
}

// toInt64 coerces an interface{} value to int64, handling the various
// integer types that arrive from AMQP table decoding. Different client
// libraries use different wire type tags for the same logical value
// (e.g. amqp091-go uses 'l' for int64 which GoMQ decodes as uint64).
// stringArg extracts a string value from an argument map, returning ""
// if the key is absent or not a string.
func stringArg(args map[string]interface{}, key string) string {
	v, ok := args[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case uint64:
		return int64(val) //nolint:gosec // AMQP wire type reinterpretation
	case int32:
		return int64(val)
	case uint32:
		return int64(val)
	case int16:
		return int64(val)
	case uint16:
		return int64(val)
	case int8:
		return int64(val)
	case uint8:
		return int64(val)
	default:
		return 0
	}
}
