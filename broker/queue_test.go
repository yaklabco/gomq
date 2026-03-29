package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jamesainslie/gomq/storage"
)

func makeStorageMsg(exchange, routingKey, body string) *storage.Message {
	return &storage.Message{
		Timestamp:    1711929600000,
		ExchangeName: exchange,
		RoutingKey:   routingKey,
		Properties:   storage.Properties{},
		BodySize:     uint64(len(body)),
		Body:         []byte(body),
	}
}

func newTestQueue(t *testing.T, name string, args map[string]interface{}) *Queue {
	t.Helper()
	dir := t.TempDir()
	queue, err := NewQueue(name, false, false, false, args, dir)
	if err != nil {
		t.Fatalf("NewQueue() error: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	return queue
}

func TestQueue_PublishAndGetRoundtrip(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "test-q", nil)

	msg := makeStorageMsg("amq.direct", "test.key", "hello world")
	ok, err := queue.Publish(msg)
	if err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	if !ok {
		t.Fatal("Publish() returned false, want true")
	}

	queue.Drain()

	if queue.Len() != 1 {
		t.Errorf("Len() = %d, want 1", queue.Len())
	}

	env, got := queue.Get(true)
	if !got {
		t.Fatal("Get() returned false, want true")
	}
	if string(env.Message.Body) != "hello world" {
		t.Errorf("Get() body = %q, want %q", env.Message.Body, "hello world")
	}

	if queue.Len() != 0 {
		t.Errorf("Len() after Get = %d, want 0", queue.Len())
	}
}

func TestQueue_FIFOOrdering(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "fifo-q", nil)

	bodies := []string{"first", "second", "third"}
	for _, body := range bodies {
		if _, err := queue.Publish(makeStorageMsg("", "", body)); err != nil {
			t.Fatalf("Publish(%q) error: %v", body, err)
		}
	}

	queue.Drain()

	for _, want := range bodies {
		env, ok := queue.Get(true)
		if !ok {
			t.Fatalf("Get() returned false, want message %q", want)
		}
		if string(env.Message.Body) != want {
			t.Errorf("Get() body = %q, want %q", env.Message.Body, want)
		}
	}
}

func TestQueue_EmptyGetReturnsFalse(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "empty-q", nil)

	_, ok := queue.Get(true)
	if ok {
		t.Error("Get() on empty queue returned true, want false")
	}
}

func TestQueue_Purge(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "purge-q", nil)

	for range 5 {
		if _, err := queue.Publish(makeStorageMsg("", "", "msg")); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	queue.Drain()

	purged := queue.Purge(3)
	if purged != 3 {
		t.Errorf("Purge(3) = %d, want 3", purged)
	}
	if queue.Len() != 2 {
		t.Errorf("Len() after Purge = %d, want 2", queue.Len())
	}
}

func TestQueue_MaxLengthDropHead(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "maxlen-q", map[string]interface{}{
		"x-max-length": int64(3),
	})

	for range 5 {
		ok, err := queue.Publish(makeStorageMsg("", "", "msg"))
		if err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
		if !ok {
			t.Fatal("Publish() returned false with drop-head overflow")
		}
	}

	queue.Drain()

	if queue.Len() != 3 {
		t.Errorf("Len() = %d, want 3 (max-length)", queue.Len())
	}
}

func TestQueue_MaxLengthRejectPublish(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "reject-q", map[string]interface{}{
		"x-max-length": int64(2),
		"x-overflow":   "reject-publish",
	})

	// Fill to capacity.
	for range 2 {
		ok, err := queue.Publish(makeStorageMsg("", "", "msg"))
		if err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
		if !ok {
			t.Fatal("Publish() returned false before limit reached")
		}
	}

	// Drain so the store reflects the published messages before we test rejection.
	queue.Drain()

	// Third publish should be rejected.
	ok, err := queue.Publish(makeStorageMsg("", "", "rejected"))
	if err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	if ok {
		t.Error("Publish() returned true, want false (reject-publish at limit)")
	}

	if queue.Len() != 2 {
		t.Errorf("Len() = %d, want 2", queue.Len())
	}
}

func TestQueue_ExclusiveConsumerRejectsSecond(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "exclusive-q", nil)

	c1 := &consumerStub{
		Tag:       "consumer-1",
		Queue:     queue,
		Exclusive: true,
		notify:    make(chan struct{}, 1),
	}
	if err := queue.AddConsumer(c1); err != nil {
		t.Fatalf("AddConsumer(c1) error: %v", err)
	}

	c2 := &consumerStub{
		Tag:       "consumer-2",
		Queue:     queue,
		Exclusive: false,
		notify:    make(chan struct{}, 1),
	}
	if err := queue.AddConsumer(c2); err == nil {
		t.Error("AddConsumer(c2) succeeded, want error (exclusive queue)")
	}
}

func TestQueue_ConsumerAddRemove(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "consumer-q", nil)

	c1 := &consumerStub{Tag: "c1", Queue: queue, notify: make(chan struct{}, 1)}
	c2 := &consumerStub{Tag: "c2", Queue: queue, notify: make(chan struct{}, 1)}

	if err := queue.AddConsumer(c1); err != nil {
		t.Fatalf("AddConsumer(c1) error: %v", err)
	}
	if err := queue.AddConsumer(c2); err != nil {
		t.Fatalf("AddConsumer(c2) error: %v", err)
	}

	if queue.ConsumerCount() != 2 {
		t.Errorf("ConsumerCount() = %d, want 2", queue.ConsumerCount())
	}

	queue.RemoveConsumer("c1")

	if queue.ConsumerCount() != 1 {
		t.Errorf("ConsumerCount() after remove = %d, want 1", queue.ConsumerCount())
	}
}

func TestQueue_AutoDeleteMarked(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	queue, err := NewQueue("auto-del-q", false, false, true, nil, dir)
	if err != nil {
		t.Fatalf("NewQueue() error: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })

	consumer := &consumerStub{Tag: "c1", Queue: queue, notify: make(chan struct{}, 1)}
	if err := queue.AddConsumer(consumer); err != nil {
		t.Fatalf("AddConsumer() error: %v", err)
	}

	queue.RemoveConsumer("c1")

	if !queue.MarkedForDelete() {
		t.Error("MarkedForDelete() = false, want true after last consumer removed from auto-delete queue")
	}
}

func TestQueue_Delete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	queue, err := NewQueue("delete-q", false, false, false, nil, dir)
	if err != nil {
		t.Fatalf("NewQueue() error: %v", err)
	}

	// Publish a message so there's data on disk.
	if _, err := queue.Publish(makeStorageMsg("", "", "data")); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	queue.Drain()

	if err := queue.Delete(); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	// The queue directory should be removed.
	queueDir := filepath.Join(dir, "queues", "delete-q")
	if _, statErr := os.Stat(queueDir); !os.IsNotExist(statErr) {
		t.Errorf("queue directory %s still exists after Delete()", queueDir)
	}
}

func TestQueue_Name(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "named-q", nil)

	if queue.Name() != "named-q" {
		t.Errorf("Name() = %q, want %q", queue.Name(), "named-q")
	}
}

func TestQueue_PropertyAccessors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	queue, err := NewQueue("prop-q", true, true, true, nil, dir)
	if err != nil {
		t.Fatalf("NewQueue() error: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })

	if !queue.IsDurable() {
		t.Error("IsDurable() = false, want true")
	}
	if !queue.IsExclusive() {
		t.Error("IsExclusive() = false, want true")
	}
	if !queue.IsAutoDelete() {
		t.Error("IsAutoDelete() = false, want true")
	}
}

func TestQueue_AckDeletesMessage(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "ack-q", nil)

	msg := makeStorageMsg("", "", "ack-me")
	if _, err := queue.Publish(msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	queue.Drain()

	env, ok := queue.Get(false)
	if !ok {
		t.Fatal("Get() returned false")
	}

	if err := queue.Ack(env.SegmentPosition); err != nil {
		t.Fatalf("Ack() error: %v", err)
	}
}

func TestQueue_RejectDeletesMessage(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "reject-msg-q", nil)

	if _, err := queue.Publish(makeStorageMsg("", "", "reject-me")); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	queue.Drain()

	env, ok := queue.Get(false)
	if !ok {
		t.Fatal("Get() returned false")
	}

	if err := queue.Reject(env.SegmentPosition, false); err != nil {
		t.Fatalf("Reject() error: %v", err)
	}
}

func TestQueue_WaitForMessageUnblocksOnPublish(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "signal-q", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan bool, 1)
	go func() {
		done <- queue.WaitForMessage(ctx)
	}()

	// Give the goroutine time to block.
	time.Sleep(10 * time.Millisecond)

	if _, err := queue.Publish(makeStorageMsg("", "", "wake up")); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	select {
	case got := <-done:
		if !got {
			t.Error("WaitForMessage() returned false, want true")
		}
	case <-ctx.Done():
		t.Fatal("WaitForMessage() did not unblock after Publish()")
	}
}

func TestQueue_WaitForMessageCancelledContext(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "cancel-q", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Already cancelled.

	if queue.WaitForMessage(ctx) {
		t.Error("WaitForMessage() returned true on cancelled context, want false")
	}
}

func TestQueue_GetFunc_Roundtrip(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "getfunc-q", nil)

	msg := makeStorageMsg("amq.direct", "test.key", "hello getfunc")
	ok, err := queue.PublishSync(msg)
	if err != nil {
		t.Fatalf("PublishSync() error: %v", err)
	}
	if !ok {
		t.Fatal("PublishSync() returned false")
	}

	var sawExchange string
	var sawBody string
	ok, err = queue.GetFunc(true, func(env *storage.Envelope) error {
		sawExchange = env.Message.ExchangeName
		sawBody = string(env.Message.Body)
		return nil
	})
	if err != nil {
		t.Fatalf("GetFunc() error: %v", err)
	}
	if !ok {
		t.Fatal("GetFunc() returned false, want true")
	}
	if sawExchange != "amq.direct" {
		t.Errorf("ExchangeName = %q, want %q", sawExchange, "amq.direct")
	}
	if sawBody != "hello getfunc" {
		t.Errorf("Body = %q, want %q", sawBody, "hello getfunc")
	}
}

func TestQueue_GetFunc_Empty(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "getfunc-empty-q", nil)

	ok, err := queue.GetFunc(true, func(_ *storage.Envelope) error {
		t.Fatal("fn should not be called on empty queue")
		return nil
	})
	if err != nil {
		t.Fatalf("GetFunc() error: %v", err)
	}
	if ok {
		t.Error("GetFunc() returned true on empty queue, want false")
	}
}

func TestQueue_GetFunc_FnError(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t, "getfunc-err-q", nil)

	msg := makeStorageMsg("ex", "rk", "body")
	if _, err := queue.PublishSync(msg); err != nil {
		t.Fatalf("PublishSync() error: %v", err)
	}

	errSent := errors.New("callback failed")
	ok, err := queue.GetFunc(true, func(_ *storage.Envelope) error {
		return errSent
	})
	if !errors.Is(err, errSent) {
		t.Errorf("GetFunc() error = %v, want %v", err, errSent)
	}
	if ok {
		t.Error("GetFunc() returned true on fn error, want false")
	}

	// Message should still be available since fn failed.
	if queue.Len() != 1 {
		t.Errorf("Len() after failed GetFunc = %d, want 1", queue.Len())
	}
}
