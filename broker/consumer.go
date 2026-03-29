package broker

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jamesainslie/gomq/storage"
)

// Consumer drives message delivery from a queue to a client channel.
// It runs a goroutine that respects prefetch limits, waits for messages,
// and invokes a delivery callback for each message shifted from the queue.
type Consumer struct {
	Tag        string
	queue      *Queue
	noAck      bool
	exclusive  bool
	prefetch   uint16
	priority   int
	active     atomic.Bool
	unacked    atomic.Int32
	capacityCh chan struct{}
	deliverFn  func(env *storage.Envelope, deliveryTag uint64) error
	cancel     context.CancelFunc
	done       chan struct{}
}

// NewConsumer creates a consumer bound to the given queue. The deliverFn
// callback is invoked for each message delivered to the consumer. If
// prefetch is 0, no limit is enforced.
func NewConsumer(
	tag string,
	queue *Queue,
	noAck bool,
	exclusive bool,
	prefetch uint16,
	deliverFn func(*storage.Envelope, uint64) error,
) *Consumer {
	return &Consumer{
		Tag:        tag,
		queue:      queue,
		noAck:      noAck,
		exclusive:  exclusive,
		prefetch:   prefetch,
		capacityCh: make(chan struct{}, 1),
		deliverFn:  deliverFn,
		done:       make(chan struct{}),
	}
}

// SetPriority sets the consumer's priority level. Higher values receive
// messages before lower values when multiple consumers compete on the
// same queue. Must be called before Start.
func (c *Consumer) SetPriority(p int) { c.priority = p }

// Priority returns the consumer's priority level.
func (c *Consumer) Priority() int { return c.priority }

// stub returns the queue-level Consumer stub for registration with the queue.
func (c *Consumer) stub() *consumerStub {
	return &consumerStub{
		Tag:       c.Tag,
		Queue:     c.queue,
		NoAck:     c.noAck,
		Exclusive: c.exclusive,
		Priority:  c.priority,
		consumer:  c,
	}
}

// Start launches the delivery goroutine. It blocks until Stop is called
// or the parent context is cancelled.
func (c *Consumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.run(ctx)
}

// Stop cancels the delivery goroutine and waits for it to finish.
func (c *Consumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	<-c.done
}

// Ack decrements the unacked counter and signals capacity to the delivery loop.
func (c *Consumer) Ack() {
	c.unacked.Add(-1)
	select {
	case c.capacityCh <- struct{}{}:
	default:
	}
}

// HasCapacity reports whether the consumer can accept another delivery.
// Returns true when prefetch is 0 (unlimited) or unacked < prefetch.
func (c *Consumer) HasCapacity() bool {
	if c.prefetch == 0 {
		return true
	}
	return c.unacked.Load() < int32(c.prefetch)
}

// run is the delivery goroutine loop.
func (c *Consumer) run(ctx context.Context) {
	defer close(c.done)

	// For single-active-consumer queues, wait until this consumer is activated.
	if !c.waitForActive(ctx) {
		return
	}

	var deliveryTag uint64

	for {
		// Check for cancellation.
		if ctx.Err() != nil {
			return
		}

		// Wait for prefetch capacity.
		if !c.waitForCapacity(ctx) {
			return
		}

		// Yield to higher-priority consumers that have capacity.
		if !c.waitForPriority(ctx) {
			return
		}

		deliveryTag++
		tag := deliveryTag

		// Try to shift a message using zero-copy path; only block if the queue is empty.
		ok, err := c.queue.GetFunc(c.noAck, func(env *storage.Envelope) error {
			return c.deliverFn(env, tag)
		})
		if err != nil {
			// Delivery failed; in a real broker this would nack or
			// close the channel. For now we stop the consumer.
			return
		}
		if !ok {
			// Queue was empty — roll back the tag since nothing was delivered.
			deliveryTag--
			if !c.queue.WaitForMessage(ctx) {
				return
			}
			continue
		}

		if !c.noAck {
			c.unacked.Add(1)
		}
	}
}

// waitForActive blocks until this consumer is the active consumer for
// single-active-consumer queues. For normal queues, it returns immediately.
func (c *Consumer) waitForActive(ctx context.Context) bool {
	if !c.queue.singleActiveConsumer {
		c.active.Store(true)
		return true
	}

	// Poll for activation. We use a short polling interval rather than
	// consuming from msgNotify to avoid stealing signals from the delivery loop.
	const activePollInterval = 50 * time.Millisecond
	for {
		if c.active.Load() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(activePollInterval):
		}
	}
}

// waitForPriority yields if higher-priority consumers on the same queue
// have available capacity. This implements the LavinMQ-style priority
// consumer behavior where lower-priority consumers defer to higher ones.
func (c *Consumer) waitForPriority(ctx context.Context) bool {
	if !c.queue.hasPriorityConsumers() {
		return true
	}

	c.queue.mu.RLock()
	consumers := make([]*consumerStub, len(c.queue.consumers))
	copy(consumers, c.queue.consumers)
	c.queue.mu.RUnlock()

	for _, other := range consumers {
		if other.Priority > c.priority && other.hasCapacity() {
			// Higher-priority consumer has capacity — yield briefly
			// so it can take the message instead.
			const priorityYieldDuration = time.Millisecond
			select {
			case <-ctx.Done():
				return false
			default:
				// Small yield to let higher-priority goroutine proceed.
				time.Sleep(priorityYieldDuration)
				return true // re-enter the loop and re-check
			}
		}
	}
	return true
}

// waitForCapacity blocks until the consumer has prefetch capacity or the
// context is cancelled. Returns true if capacity is available.
func (c *Consumer) waitForCapacity(ctx context.Context) bool {
	if c.prefetch == 0 || c.unacked.Load() < int32(c.prefetch) {
		return true
	}
	for {
		select {
		case <-ctx.Done():
			return false
		case <-c.capacityCh:
			if c.unacked.Load() < int32(c.prefetch) {
				return true
			}
		}
	}
}
