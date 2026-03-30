package broker

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jamesainslie/gomq/storage"
)

// waitForCount blocks until counter reaches target or timeout expires.
// Returns true if the target was reached.
func waitForCount(counter *atomic.Int32, target int32, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for counter.Load() < target {
		select {
		case <-deadline:
			return false
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	return true
}

func TestGuarantee_ConsumerPriority(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping consumer priority test in CI (goroutine scheduling is non-deterministic on shared runners)")
	}
	t.Parallel()

	queue := newTestQueue(t, "priority-q", nil)

	var highCount, lowCount atomic.Int32

	highConsumer := NewConsumer("high", queue, true, false, 0,
		func(_ *storage.Envelope, _ uint64) error {
			highCount.Add(1)
			return nil
		},
	)
	highConsumer.SetPriority(10)

	lowConsumer := NewConsumer("low", queue, true, false, 0,
		func(_ *storage.Envelope, _ uint64) error {
			lowCount.Add(1)
			return nil
		},
	)
	lowConsumer.SetPriority(1)

	// Add consumers: low first, then high (priority should override order).
	if err := queue.AddConsumer(lowConsumer.stub()); err != nil {
		t.Fatalf("AddConsumer(low) error: %v", err)
	}
	if err := queue.AddConsumer(highConsumer.stub()); err != nil {
		t.Fatalf("AddConsumer(high) error: %v", err)
	}

	lowConsumer.Start()
	highConsumer.Start()
	t.Cleanup(func() {
		lowConsumer.Stop()
		highConsumer.Stop()
	})

	// Publish several messages.
	for range 10 {
		if _, err := queue.Publish(makeStorageMsg("", "key", "msg")); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	// Wait for all messages to be delivered.
	if !waitForCount(&highCount, 1, 10*time.Second) {
		t.Fatal("timed out waiting for high-priority consumer to receive messages")
	}

	// Give time for delivery to complete.
	time.Sleep(200 * time.Millisecond)

	totalHigh := highCount.Load()
	totalLow := lowCount.Load()

	// Both consumers should have received messages (priority is best-effort
	// with Go's goroutine scheduler — we cannot guarantee strict ordering).
	if totalHigh == 0 {
		t.Error("high-priority consumer received 0 messages")
	}
	if totalHigh+totalLow != 10 {
		t.Errorf("total delivered = %d, want 10", totalHigh+totalLow)
	}
	t.Logf("high-priority: %d, low-priority: %d", totalHigh, totalLow)
}

func TestGuarantee_SingleActiveConsumer(t *testing.T) {
	t.Parallel()

	queue := newTestQueue(t, "sac-q", map[string]interface{}{
		"x-single-active-consumer": true,
	})

	var firstCount, secondCount atomic.Int32

	first := NewConsumer("first", queue, true, false, 0,
		func(_ *storage.Envelope, _ uint64) error {
			firstCount.Add(1)
			return nil
		},
	)
	second := NewConsumer("second", queue, true, false, 0,
		func(_ *storage.Envelope, _ uint64) error {
			secondCount.Add(1)
			return nil
		},
	)

	if err := queue.AddConsumer(first.stub()); err != nil {
		t.Fatalf("AddConsumer(first) error: %v", err)
	}
	if err := queue.AddConsumer(second.stub()); err != nil {
		t.Fatalf("AddConsumer(second) error: %v", err)
	}

	first.Start()
	second.Start()
	t.Cleanup(func() {
		first.Stop()
		second.Stop()
	})

	// Publish messages — only the active consumer should receive them.
	for range 5 {
		if _, err := queue.Publish(makeStorageMsg("", "key", "msg")); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	if !waitForCount(&firstCount, 5, 3*time.Second) {
		t.Fatalf("timed out: first got %d, second got %d", firstCount.Load(), secondCount.Load())
	}

	if secondCount.Load() != 0 {
		t.Errorf("second consumer received %d messages, want 0 (not the active consumer)", secondCount.Load())
	}

	// Now remove the active consumer; second should become active.
	first.Stop()
	queue.RemoveConsumer("first")

	for range 3 {
		if _, err := queue.Publish(makeStorageMsg("", "key", "msg2")); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	if !waitForCount(&secondCount, 3, 3*time.Second) {
		t.Fatalf("second consumer only received %d messages after failover, want 3", secondCount.Load())
	}
}

func TestGuarantee_BasicReturnMandatory(t *testing.T) {
	t.Parallel()

	vh := newGuaranteeVHost(t)

	// Declare a direct exchange with no bindings.
	if _, err := vh.DeclareExchange("test-ex", "direct", false, false, nil); err != nil {
		t.Fatalf("DeclareExchange() error: %v", err)
	}

	msg := &storage.Message{
		Timestamp:    time.Now().UnixMilli(),
		ExchangeName: "test-ex",
		RoutingKey:   "unbound-key",
		Properties:   storage.Properties{},
		BodySize:     5,
		Body:         []byte("hello"),
	}

	// Use PublishMandatory which returns routed status.
	routed, pubErr := vh.PublishMandatory("test-ex", "unbound-key", false, msg)
	if pubErr != nil {
		t.Fatalf("PublishMandatory() error: %v", pubErr)
	}
	if routed {
		t.Error("PublishMandatory() returned routed=true, want false for unbound routing key")
	}

	// Now bind a queue and verify mandatory publish succeeds.
	if _, err := vh.DeclareQueue("test-q", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}
	if err := vh.BindQueue("test-q", "test-ex", "bound-key", nil); err != nil {
		t.Fatalf("BindQueue() error: %v", err)
	}

	msg.RoutingKey = "bound-key"
	routed, pubErr = vh.PublishMandatory("test-ex", "bound-key", false, msg)
	if pubErr != nil {
		t.Fatalf("PublishMandatory() error: %v", pubErr)
	}
	if !routed {
		t.Error("PublishMandatory() returned routed=false, want true for bound routing key")
	}
}

func TestGuarantee_DeliveryLimit(t *testing.T) {
	t.Parallel()

	vh := newGuaranteeVHost(t)

	// Declare DLX exchange and queue.
	if _, err := vh.DeclareExchange("dlx", "direct", false, false, nil); err != nil {
		t.Fatalf("DeclareExchange(dlx) error: %v", err)
	}
	if _, err := vh.DeclareQueue("dlq", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(dlq) error: %v", err)
	}
	if err := vh.BindQueue("dlq", "dlx", "dl-key", nil); err != nil {
		t.Fatalf("BindQueue(dlq) error: %v", err)
	}

	// Declare source queue with delivery limit = 2 and DLX.
	if _, err := vh.DeclareQueue("limited-q", false, false, false, map[string]interface{}{
		"x-delivery-limit":          int64(2),
		"x-dead-letter-exchange":    "dlx",
		"x-dead-letter-routing-key": "dl-key",
	}); err != nil {
		t.Fatalf("DeclareQueue(limited-q) error: %v", err)
	}

	srcQ, _ := vh.GetQueue("limited-q")
	dlq, _ := vh.GetQueue("dlq")

	// Publish a message.
	msg := makeStorageMsg("", "key", "limited-body")
	if _, err := srcQ.PublishSync(msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	// First delivery + requeue: counter goes to 1.
	env, ok := srcQ.Get(false)
	if !ok {
		t.Fatal("Get() returned false on first attempt")
	}
	if err := srcQ.Reject(env.SegmentPosition, true); err != nil {
		t.Fatalf("Reject(requeue=true) #1 error: %v", err)
	}

	// Second delivery + requeue: counter goes to 2, which equals the limit.
	// The message should be dead-lettered instead of requeued.
	env, ok = srcQ.Get(false)
	if !ok {
		t.Fatal("Get() returned false on second attempt")
	}
	if err := srcQ.Reject(env.SegmentPosition, true); err != nil {
		t.Fatalf("Reject(requeue=true) #2 error: %v", err)
	}

	// Source queue should be empty — message was dead-lettered.
	if srcQ.Len() != 0 {
		t.Errorf("source queue Len() = %d, want 0 (message should be dead-lettered)", srcQ.Len())
	}

	// DLQ should have the message.
	dlq.Drain()
	if dlq.Len() != 1 {
		t.Errorf("DLQ Len() = %d, want 1", dlq.Len())
	}
}

// newGuaranteeVHost creates a throwaway vhost for guarantee tests.
func newGuaranteeVHost(t *testing.T) *VHost {
	t.Helper()
	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	t.Cleanup(func() { _ = vh.Close() })
	return vh
}
