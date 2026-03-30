package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yaklabco/gomq/storage"
)

func TestConsumer_ReceivesPublishedMessages(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t, "consumer-recv", nil)

	var received []*storage.Envelope
	var mu sync.Mutex

	consumer := NewConsumer("ctag-1", queue, true, false, 0, func(env *storage.Envelope, _ uint64) error {
		mu.Lock()
		received = append(received, env)
		mu.Unlock()
		return nil
	})

	if err := queue.AddConsumer(consumer.stub()); err != nil {
		t.Fatalf("AddConsumer() error: %v", err)
	}
	consumer.Start()
	t.Cleanup(func() { consumer.Stop() })

	if _, err := queue.Publish(makeStorageMsg("", "test", "hello")); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for consumer to receive message")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("received %d messages, want 1", len(received))
	}
	if string(received[0].Message.Body) != "hello" {
		t.Errorf("body = %q, want %q", received[0].Message.Body, "hello")
	}
}

func TestConsumer_PrefetchBlocksDelivery(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t, "consumer-prefetch", nil)

	var deliveryCount atomic.Int32

	consumer := NewConsumer("ctag-2", queue, false, false, 1, func(_ *storage.Envelope, _ uint64) error {
		deliveryCount.Add(1)
		return nil
	})

	if err := queue.AddConsumer(consumer.stub()); err != nil {
		t.Fatalf("AddConsumer() error: %v", err)
	}
	consumer.Start()
	t.Cleanup(func() { consumer.Stop() })

	// Publish 3 messages.
	for i := range 3 {
		body := []byte{byte('a' + i)}
		if _, err := queue.Publish(makeStorageMsg("", "key", string(body))); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// Wait for the first delivery.
	deadline := time.After(2 * time.Second)
	for deliveryCount.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for first delivery")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Give a bit of time to ensure the consumer doesn't deliver more.
	time.Sleep(100 * time.Millisecond)
	if got := deliveryCount.Load(); got != 1 {
		t.Errorf("delivery count = %d after prefetch=1, want 1", got)
	}

	// Ack to release capacity, expect the next message.
	consumer.Ack()

	deadline = time.After(2 * time.Second)
	for deliveryCount.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for second delivery after ack")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestConsumer_StopHaltsDelivery(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t, "consumer-stop", nil)

	var count atomic.Int32
	consumer := NewConsumer("ctag-3", queue, true, false, 0, func(_ *storage.Envelope, _ uint64) error {
		count.Add(1)
		return nil
	})

	if err := queue.AddConsumer(consumer.stub()); err != nil {
		t.Fatalf("AddConsumer() error: %v", err)
	}
	consumer.Start()

	// Publish one message, wait for delivery.
	if _, err := queue.Publish(makeStorageMsg("", "key", "msg1")); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	deadline := time.After(2 * time.Second)
	for count.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for delivery")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Stop consumer.
	consumer.Stop()

	before := count.Load()

	// Publish another message.
	if _, err := queue.Publish(makeStorageMsg("", "key", "msg2")); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if count.Load() != before {
		t.Errorf("consumer delivered after Stop(): count changed from %d to %d", before, count.Load())
	}
}

func TestConsumer_HasCapacity(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t, "consumer-cap", nil)

	consumer := NewConsumer("ctag-4", queue, false, false, 2, func(_ *storage.Envelope, _ uint64) error {
		return nil
	})

	if !consumer.HasCapacity() {
		t.Error("HasCapacity() = false with 0 unacked, want true")
	}

	consumer.unacked.Add(2)
	if consumer.HasCapacity() {
		t.Error("HasCapacity() = true with 2 unacked and prefetch=2, want false")
	}

	consumer.Ack()
	if !consumer.HasCapacity() {
		t.Error("HasCapacity() = false after Ack(), want true")
	}
}
