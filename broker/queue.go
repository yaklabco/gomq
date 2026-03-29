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

// ErrExclusiveQueue is returned when a second consumer is added to a queue
// that already has an exclusive consumer.
var ErrExclusiveQueue = errors.New("queue has an exclusive consumer")

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
		overflow:   "drop-head",
	}

	queue.parseArguments(args)

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

// Publish writes a message to the store, enforces overflow limits, and
// signals waiting consumers. It returns false without error when the queue
// is at capacity and overflow is "reject-publish".
func (q *Queue) Publish(msg *storage.Message) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed.Load() {
		return false, errors.New("publish to closed queue")
	}

	// Enforce max-length limit.
	if q.maxLength > 0 && int64(q.store.Len()) >= q.maxLength {
		if q.overflow == "reject-publish" {
			return false, nil
		}
		// drop-head: remove oldest message.
		if err := q.dropHead(); err != nil {
			return false, fmt.Errorf("drop head message: %w", err)
		}
	}

	// Enforce max-length-bytes limit.
	if q.maxLengthBytes > 0 && int64(q.store.ByteSize()) >= q.maxLengthBytes {
		if q.overflow == "reject-publish" {
			return false, nil
		}
		if err := q.dropHead(); err != nil {
			return false, fmt.Errorf("drop head message (bytes limit): %w", err)
		}
	}

	if _, err := q.store.Push(msg); err != nil {
		return false, fmt.Errorf("push message to store: %w", err)
	}

	q.publishCount.Add(1)
	q.signalConsumers()

	return true, nil
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

// Purge deletes up to limit ready messages from the front of the queue
// and returns the number actually purged.
func (q *Queue) Purge(limit int) int {
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

// Close closes the underlying message store.
func (q *Queue) Close() error {
	if q.closed.Swap(true) {
		return nil
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
