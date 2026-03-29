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
	Tag       string
	queue     *Queue
	noAck     bool
	exclusive bool
	prefetch  uint16
	unacked   atomic.Int32
	deliverFn func(env *storage.Envelope, deliveryTag uint64) error
	cancel    context.CancelFunc
	done      chan struct{}
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
		Tag:       tag,
		queue:     queue,
		noAck:     noAck,
		exclusive: exclusive,
		prefetch:  prefetch,
		deliverFn: deliverFn,
		done:      make(chan struct{}),
	}
}

// stub returns the queue-level Consumer stub for registration with the queue.
func (c *Consumer) stub() *consumerStub {
	return &consumerStub{
		Tag:       c.Tag,
		Queue:     c.queue,
		NoAck:     c.noAck,
		Exclusive: c.exclusive,
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

		// Try to shift a message immediately; only block if the queue is empty.
		env, ok := c.queue.Get(c.noAck)
		if !ok {
			if !c.queue.WaitForMessage(ctx) {
				return
			}
			env, ok = c.queue.Get(c.noAck)
			if !ok {
				continue
			}
		}

		deliveryTag++

		if err := c.deliverFn(env, deliveryTag); err != nil {
			// Delivery failed; in a real broker this would nack or
			// close the channel. For now we stop the consumer.
			return
		}

		if !c.noAck {
			c.unacked.Add(1)
		}
	}
}

// capacityPollInterval is the interval between prefetch capacity checks.
const capacityPollInterval = 5 * time.Millisecond

// waitForCapacity blocks until the consumer has prefetch capacity or the
// context is cancelled. Returns true if capacity is available.
func (c *Consumer) waitForCapacity(ctx context.Context) bool {
	if c.prefetch == 0 {
		return true
	}

	for c.unacked.Load() >= int32(c.prefetch) {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(capacityPollInterval):
		}
	}
	return true
}
