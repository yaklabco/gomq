package broker

import (
	"os"
	"testing"

	"github.com/yaklabco/gomq/storage"
)

func newTestVHost(t *testing.T) *VHost {
	t.Helper()

	dataDir := t.TempDir()

	vh, err := NewVHost("test", dataDir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}

	t.Cleanup(func() {
		if closeErr := vh.Close(); closeErr != nil {
			t.Errorf("VHost.Close() error: %v", closeErr)
		}
	})

	return vh
}

// --- Constructor Tests ---

func TestNewVHost_CreatesDataDirectory(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	vh, err := NewVHost("myhost", dataDir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	// VHost data dir should exist.
	if _, statErr := os.Stat(vh.DataDir()); os.IsNotExist(statErr) {
		t.Errorf("expected data directory %q to exist", vh.DataDir())
	}
}

func TestNewVHost_DefaultExchanges(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	tests := []struct {
		name     string
		wantType string
	}{
		{name: "", wantType: ExchangeDirect},
		{name: "amq.direct", wantType: ExchangeDirect},
		{name: "amq.fanout", wantType: ExchangeFanout},
		{name: "amq.topic", wantType: ExchangeTopic},
		{name: "amq.headers", wantType: ExchangeHeaders},
	}

	for _, tt := range tests {
		ex, ok := vh.GetExchange(tt.name)
		if !ok {
			t.Errorf("default exchange %q not found", tt.name)
			continue
		}

		if got := ex.Type(); got != tt.wantType {
			t.Errorf("exchange %q Type() = %q, want %q", tt.name, got, tt.wantType)
		}

		if !ex.IsDurable() {
			t.Errorf("default exchange %q should be durable", tt.name)
		}

		if ex.IsAutoDelete() {
			t.Errorf("default exchange %q should not be auto-delete", tt.name)
		}
	}
}

// --- DeclareExchange Tests ---

func TestVHost_DeclareExchange_CreatesNew(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	ex, err := vh.DeclareExchange("my.exchange", ExchangeDirect, true, false, nil)
	if err != nil {
		t.Fatalf("DeclareExchange() error: %v", err)
	}

	if ex.Name() != "my.exchange" {
		t.Errorf("Name() = %q, want %q", ex.Name(), "my.exchange")
	}

	if ex.Type() != ExchangeDirect {
		t.Errorf("Type() = %q, want %q", ex.Type(), ExchangeDirect)
	}
}

func TestVHost_DeclareExchange_RedeclareIdentical(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	ex1, err := vh.DeclareExchange("my.exchange", ExchangeFanout, true, false, nil)
	if err != nil {
		t.Fatalf("DeclareExchange() first: %v", err)
	}

	ex2, err := vh.DeclareExchange("my.exchange", ExchangeFanout, true, false, nil)
	if err != nil {
		t.Fatalf("DeclareExchange() second: %v", err)
	}

	if ex1 != ex2 {
		t.Error("expected same exchange instance on identical redeclare")
	}
}

func TestVHost_DeclareExchange_TypeMismatch(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareExchange("my.exchange", ExchangeDirect, true, false, nil)
	if err != nil {
		t.Fatalf("DeclareExchange() first: %v", err)
	}

	_, err = vh.DeclareExchange("my.exchange", ExchangeFanout, true, false, nil)
	if err == nil {
		t.Error("expected error on type mismatch redeclare, got nil")
	}
}

func TestVHost_DeclareExchange_DurableMismatch(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareExchange("my.exchange", ExchangeDirect, true, false, nil)
	if err != nil {
		t.Fatalf("DeclareExchange() first: %v", err)
	}

	_, err = vh.DeclareExchange("my.exchange", ExchangeDirect, false, false, nil)
	if err == nil {
		t.Error("expected error on durable mismatch redeclare, got nil")
	}
}

func TestVHost_DeclareExchange_InvalidType(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareExchange("my.exchange", "invalid", true, false, nil)
	if err == nil {
		t.Error("expected error for invalid exchange type, got nil")
	}
}

func TestVHost_DeclareExchange_AllTypes(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	types := []string{ExchangeDirect, ExchangeFanout, ExchangeTopic, ExchangeHeaders}
	for _, typ := range types {
		ex, err := vh.DeclareExchange("ex."+typ, typ, false, false, nil)
		if err != nil {
			t.Errorf("DeclareExchange(%q) error: %v", typ, err)
			continue
		}

		if ex.Type() != typ {
			t.Errorf("DeclareExchange(%q) Type() = %q", typ, ex.Type())
		}
	}
}

// --- DeleteExchange Tests ---

func TestVHost_DeleteExchange_Removes(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareExchange("my.exchange", ExchangeDirect, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareExchange() error: %v", err)
	}

	if err := vh.DeleteExchange("my.exchange", false); err != nil {
		t.Fatalf("DeleteExchange() error: %v", err)
	}

	if _, ok := vh.GetExchange("my.exchange"); ok {
		t.Error("expected exchange to be removed after delete")
	}
}

func TestVHost_DeleteExchange_DefaultExchangesForbidden(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	defaults := []string{"", "amq.direct", "amq.fanout", "amq.topic", "amq.headers"}
	for _, name := range defaults {
		if err := vh.DeleteExchange(name, false); err == nil {
			t.Errorf("expected error deleting default exchange %q, got nil", name)
		}
	}
}

func TestVHost_DeleteExchange_NotFound(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	err := vh.DeleteExchange("nonexistent", false)
	if err == nil {
		t.Error("expected error deleting nonexistent exchange, got nil")
	}
}

// --- Queue Tests ---

func TestVHost_DeclareQueue_CreatesNew(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	queue, err := vh.DeclareQueue("orders", true, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	if queue.Name() != "orders" {
		t.Errorf("Name() = %q, want %q", queue.Name(), "orders")
	}
}

func TestVHost_DeclareQueue_RedeclareReturnsExisting(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	q1, err := vh.DeclareQueue("orders", true, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() first: %v", err)
	}

	q2, err := vh.DeclareQueue("orders", true, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() second: %v", err)
	}

	if q1 != q2 {
		t.Error("expected same queue on identical redeclare")
	}
}

func TestVHost_DeclareQueue_DurableMismatch(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("orders", true, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() first: %v", err)
	}

	_, err = vh.DeclareQueue("orders", false, false, false, nil)
	if err == nil {
		t.Error("expected error on durable mismatch, got nil")
	}
}

func TestVHost_GetQueue(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, ok := vh.GetQueue("nonexistent")
	if ok {
		t.Error("expected false for nonexistent queue")
	}

	_, err := vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	queue, ok := vh.GetQueue("orders")
	if !ok {
		t.Fatal("expected queue to be found")
	}

	if queue.Name() != "orders" {
		t.Errorf("Name() = %q, want %q", queue.Name(), "orders")
	}
}

func TestVHost_DeleteQueue_Removes(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	count, err := vh.DeleteQueue("orders", false, false)
	if err != nil {
		t.Fatalf("DeleteQueue() error: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 messages purged, got %d", count)
	}

	if _, ok := vh.GetQueue("orders"); ok {
		t.Error("expected queue to be removed after delete")
	}
}

func TestVHost_DeleteQueue_NotFound(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeleteQueue("nonexistent", false, false)
	if err == nil {
		t.Error("expected error deleting nonexistent queue, got nil")
	}
}

func TestVHost_DeleteQueue_IfUnused(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	queue, err := vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	// Add a consumer to make the queue "used".
	consumer := &consumerStub{Tag: "c1", Queue: queue, notify: make(chan struct{}, 1)}
	if addErr := queue.AddConsumer(consumer); addErr != nil {
		t.Fatalf("AddConsumer() error: %v", addErr)
	}

	_, err = vh.DeleteQueue("orders", true, false)
	if err == nil {
		t.Error("expected error deleting queue with consumers when ifUnused=true")
	}
}

func TestVHost_DeleteQueue_IfEmpty(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	// Publish a message to make the queue non-empty.
	msg := &storage.Message{
		Timestamp:    1,
		ExchangeName: "",
		RoutingKey:   "orders",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	if pubErr := vh.Publish("", "orders", false, msg); pubErr != nil {
		t.Fatalf("Publish() error: %v", pubErr)
	}

	queue, _ := vh.GetQueue("orders")
	queue.Drain()

	_, err = vh.DeleteQueue("orders", false, true)
	if err == nil {
		t.Error("expected error deleting non-empty queue when ifEmpty=true")
	}
}

func TestVHost_PurgeQueue(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	// Publish two messages.
	for i := range 2 {
		msg := &storage.Message{
			Timestamp:    int64(i),
			ExchangeName: "",
			RoutingKey:   "orders",
			BodySize:     5,
			Body:         []byte("hello"),
		}

		if pubErr := vh.Publish("", "orders", false, msg); pubErr != nil {
			t.Fatalf("Publish() error: %v", pubErr)
		}
	}

	queue, _ := vh.GetQueue("orders")
	queue.Drain()

	count, err := vh.PurgeQueue("orders")
	if err != nil {
		t.Fatalf("PurgeQueue() error: %v", err)
	}

	if count != 2 {
		t.Errorf("PurgeQueue() = %d, want 2", count)
	}

	queue, _ = vh.GetQueue("orders")
	if queue.Len() != 0 {
		t.Errorf("queue length after purge = %d, want 0", queue.Len())
	}
}

func TestVHost_PurgeQueue_NotFound(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.PurgeQueue("nonexistent")
	if err == nil {
		t.Error("expected error purging nonexistent queue, got nil")
	}
}

// --- Binding + Publish Tests ---

func TestVHost_BindQueue_DirectPublish(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	if err := vh.BindQueue("orders", "amq.direct", "order.created", nil); err != nil {
		t.Fatalf("BindQueue() error: %v", err)
	}

	msg := &storage.Message{
		Timestamp:    1,
		ExchangeName: "amq.direct",
		RoutingKey:   "order.created",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	if err := vh.Publish("amq.direct", "order.created", false, msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	queue, _ := vh.GetQueue("orders")
	queue.Drain()
	if queue.Len() != 1 {
		t.Errorf("queue length = %d, want 1", queue.Len())
	}
}

func TestVHost_FanoutPublish_AllBoundQueues(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	queueNames := []string{"q1", "q2", "q3"}
	for _, name := range queueNames {
		if _, err := vh.DeclareQueue(name, false, false, false, nil); err != nil {
			t.Fatalf("DeclareQueue(%q) error: %v", name, err)
		}

		if err := vh.BindQueue(name, "amq.fanout", "", nil); err != nil {
			t.Fatalf("BindQueue(%q) error: %v", name, err)
		}
	}

	msg := &storage.Message{
		Timestamp:    1,
		ExchangeName: "amq.fanout",
		RoutingKey:   "ignored",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	if err := vh.Publish("amq.fanout", "ignored", false, msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	for _, name := range queueNames {
		q, _ := vh.GetQueue(name)
		q.Drain()
		if q.Len() != 1 {
			t.Errorf("queue %q length = %d, want 1", name, q.Len())
		}
	}
}

func TestVHost_TopicPublish_Wildcard(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("all-orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	if err := vh.BindQueue("all-orders", "amq.topic", "order.#", nil); err != nil {
		t.Fatalf("BindQueue() error: %v", err)
	}

	msg := &storage.Message{
		Timestamp:    1,
		ExchangeName: "amq.topic",
		RoutingKey:   "order.created.us",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	if err := vh.Publish("amq.topic", "order.created.us", false, msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	queue, _ := vh.GetQueue("all-orders")
	queue.Drain()
	if queue.Len() != 1 {
		t.Errorf("queue length = %d, want 1", queue.Len())
	}
}

func TestVHost_DefaultExchange_RoutesByQueueName(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("inbox", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	msg := &storage.Message{
		Timestamp:    1,
		ExchangeName: "",
		RoutingKey:   "inbox",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	if err := vh.Publish("", "inbox", false, msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	queue, _ := vh.GetQueue("inbox")
	queue.Drain()
	if queue.Len() != 1 {
		t.Errorf("queue length = %d, want 1", queue.Len())
	}
}

func TestVHost_Publish_NonexistentExchange(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	msg := &storage.Message{
		Timestamp:    1,
		ExchangeName: "nonexistent",
		RoutingKey:   "key",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	err := vh.Publish("nonexistent", "key", false, msg)
	if err == nil {
		t.Error("expected error publishing to nonexistent exchange, got nil")
	}
}

func TestVHost_UnbindQueue(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	if err := vh.BindQueue("orders", "amq.direct", "order.created", nil); err != nil {
		t.Fatalf("BindQueue() error: %v", err)
	}

	if err := vh.UnbindQueue("orders", "amq.direct", "order.created", nil); err != nil {
		t.Fatalf("UnbindQueue() error: %v", err)
	}

	msg := &storage.Message{
		Timestamp:    1,
		ExchangeName: "amq.direct",
		RoutingKey:   "order.created",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	if err := vh.Publish("amq.direct", "order.created", false, msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	queue, _ := vh.GetQueue("orders")
	queue.Drain()
	if queue.Len() != 0 {
		t.Errorf("queue length after unbind = %d, want 0", queue.Len())
	}
}

func TestVHost_BindQueue_NonexistentQueue(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	err := vh.BindQueue("nonexistent", "amq.direct", "key", nil)
	if err == nil {
		t.Error("expected error binding nonexistent queue, got nil")
	}
}

func TestVHost_BindQueue_NonexistentExchange(t *testing.T) {
	t.Parallel()

	vh := newTestVHost(t)

	_, err := vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	err = vh.BindQueue("orders", "nonexistent", "key", nil)
	if err == nil {
		t.Error("expected error binding to nonexistent exchange, got nil")
	}
}

// --- Close Tests ---

func TestVHost_Close(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	vh, err := NewVHost("close-test", dataDir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}

	_, err = vh.DeclareQueue("orders", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	if err := vh.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// Publishing after close should fail.
	msg := &storage.Message{
		Timestamp:    1,
		ExchangeName: "",
		RoutingKey:   "orders",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	err = vh.Publish("", "orders", false, msg)
	if err == nil {
		t.Error("expected error publishing to closed vhost, got nil")
	}
}
