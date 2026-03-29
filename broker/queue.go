package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/jamesainslie/gomq/storage"
)

const defaultSegmentSize = 8 * 1024 * 1024 // 8 MiB

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
	fenceCh chan struct{} // signaled by writeLoop when it processes a nil sentinel

	// Limits parsed from arguments.
	maxLength      int64  // 0 = unlimited (x-max-length)
	maxLengthBytes int64  // 0 = unlimited (x-max-length-bytes)
	messageTTL     int64  // 0 = no TTL (x-message-ttl), milliseconds
	overflow       string // "drop-head" (default) or "reject-publish"

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
	notify    chan struct{} // capacity-ready signal
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
// and x-overflow.
func NewQueue(name string, durable, exclusive, autoDelete bool, args map[string]interface{}, dataDir string) (*Queue, error) {
	queueDir := filepath.Join(dataDir, "queues", name)

	store, err := storage.OpenMessageStore(queueDir, defaultSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("open message store for queue %q: %w", name, err)
	}

	queue := &Queue{
		name:       name,
		durable:    durable,
		exclusive:  exclusive,
		autoDelete: autoDelete,
		arguments:  args,
		store:      store,
		dataDir:    queueDir,
		msgNotify:  make(chan struct{}, 1),
		inbox:      make(chan *storage.Message, inboxCapacity),
		overflow:   overflowDropHead,
	}

	queue.parseArguments(args)

	queue.inboxWg.Add(1)
	go queue.writeLoop()

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

// MarkedForDelete reports whether the queue has been marked for deletion
// due to auto-delete semantics.
func (q *Queue) MarkedForDelete() bool { return q.markedForDelete.Load() }

// Publish enqueues a message to the queue's inbox channel. The dedicated
// writeLoop goroutine drains the inbox and writes to the store under the
// queue lock, amortising lock acquisitions across batches.
//
// The message body is copied to decouple the caller's buffer lifetime from
// the async write. It returns false without error when the queue is at
// capacity and overflow is "reject-publish".
func (q *Queue) Publish(msg *storage.Message) (bool, error) {
	if q.closed.Load() {
		return false, ErrQueueClosed
	}

	// Reject-publish overflow: approximate check without lock. The writeLoop
	// enforces the limit precisely under the lock; this is an optimistic
	// fast-path rejection to avoid filling the inbox with messages that will
	// be discarded.
	if q.overflow == overflowReject && q.maxLength > 0 && int64(q.store.Len()) >= q.maxLength {
		return false, nil
	}

	// Copy the message body so the caller can safely reuse its buffer
	// (e.g. the channel body pool) while the writeLoop processes the message
	// asynchronously.
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
	q.inbox <- &owned
	q.inboxMu.Unlock()

	q.publishCount.Add(1)
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
			q.signalFence()
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
			q.signalFence()
		}
	}
}

// publishToStore writes a single message to the store, enforcing overflow
// limits. Must be called with q.mu held.
func (q *Queue) publishToStore(msg *storage.Message) {
	// Enforce max-length limit.
	if q.maxLength > 0 && int64(q.store.Len()) >= q.maxLength {
		if q.overflow == overflowReject {
			return
		}
		_ = q.dropHead() //nolint:errcheck // non-fatal; segment GC handles cleanup
	}

	// Enforce max-length-bytes limit.
	if q.maxLengthBytes > 0 && int64(q.store.ByteSize()) >= q.maxLengthBytes {
		if q.overflow == overflowReject {
			return
		}
		_ = q.dropHead() //nolint:errcheck // non-fatal; segment GC handles cleanup
	}

	_, _ = q.store.Push(msg) //nolint:errcheck // non-fatal for individual batch messages
}

// Get shifts one message from the front of the queue (basic.get semantics).
// If noAck is true the message is considered immediately consumed. Returns
// false when the queue is empty.
func (q *Queue) Get(noAck bool) (*storage.Envelope, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	env, ok := q.store.Shift()
	if !ok {
		return nil, false
	}

	q.deliverCount.Add(1)

	if noAck {
		// Auto-ack: delete from store immediately. A delete failure here is
		// non-fatal for the caller (the message was already shifted), so we
		// skip the error. The message will be cleaned up on segment GC.
		if delErr := q.store.Delete(env.SegmentPosition); delErr == nil {
			q.ackCount.Add(1)
		}
	}

	return env, true
}

// Ack acknowledges a message, deleting it from the store.
func (q *Queue) Ack(sp storage.SegmentPosition) error {
	if err := q.store.Delete(sp); err != nil {
		return fmt.Errorf("ack message at %s: %w", sp, err)
	}
	q.ackCount.Add(1)
	return nil
}

// Reject rejects a message. If requeue is true the message would be re-queued,
// but for now it is deleted in both cases.
func (q *Queue) Reject(sp storage.SegmentPosition, _ bool) error {
	if err := q.store.Delete(sp); err != nil {
		return fmt.Errorf("reject message at %s: %w", sp, err)
	}
	return nil
}

// Purge drains the inbox and then deletes up to limit ready messages from
// the front of the queue, returning the number actually purged.
func (q *Queue) Purge(limit int) int {
	q.Drain()
	return q.store.Purge(limit)
}

// Len returns the number of messages in the queue.
func (q *Queue) Len() uint32 {
	return q.store.Len()
}

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
	return nil
}

// RemoveConsumer removes the consumer with the given tag. If the queue is
// auto-delete and had consumers but now has none, it is marked for deletion.
func (q *Queue) RemoveConsumer(tag string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, c := range q.consumers {
		if c.Tag == tag {
			q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
			break
		}
	}

	if q.autoDelete && q.hadConsumers && len(q.consumers) == 0 {
		q.markedForDelete.Store(true)
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
	q.inboxMu.Lock()
	close(q.inbox)
	q.inboxMu.Unlock()
	q.inboxWg.Wait()
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
// to the store. It sends a nil sentinel through the inbox channel and waits
// for the writeLoop to process it, guaranteeing all prior messages have
// been persisted.
//
// It is safe to call concurrently with Close; if the queue is closed (or
// closes while draining), Drain returns immediately.
func (q *Queue) Drain() {
	if q.closed.Load() {
		return
	}

	fence := make(chan struct{})
	q.fenceMu.Lock()
	q.fenceCh = fence
	q.fenceMu.Unlock()

	q.inboxMu.Lock()
	if q.closed.Load() {
		q.inboxMu.Unlock()
		// Queue closed between our first check and the lock acquisition.
		// Signal the fence ourselves so callers don't hang.
		q.signalFence()
		return
	}
	q.inbox <- nil
	q.inboxMu.Unlock()

	<-fence
}

// signalFence signals the fence channel, unblocking a Drain() caller.
func (q *Queue) signalFence() {
	q.fenceMu.Lock()
	ch := q.fenceCh
	q.fenceCh = nil
	q.fenceMu.Unlock()
	if ch != nil {
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

// dropHead shifts and deletes the oldest message from the store.
func (q *Queue) dropHead() error {
	env, ok := q.store.Shift()
	if !ok {
		return nil
	}
	return q.store.Delete(env.SegmentPosition)
}

// parseArguments extracts queue limits from the argument map.
func (q *Queue) parseArguments(args map[string]interface{}) {
	if args == nil {
		return
	}

	if v, ok := args["x-max-length"]; ok {
		if n, ok := v.(int64); ok {
			q.maxLength = n
		}
	}

	if v, ok := args["x-max-length-bytes"]; ok {
		if n, ok := v.(int64); ok {
			q.maxLengthBytes = n
		}
	}

	if v, ok := args["x-message-ttl"]; ok {
		if n, ok := v.(int64); ok {
			q.messageTTL = n
		}
	}

	if v, ok := args["x-overflow"]; ok {
		if s, ok := v.(string); ok {
			q.overflow = s
		}
	}
}
