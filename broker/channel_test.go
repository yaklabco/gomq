package broker

import (
	"sync"
	"testing"
	"time"

	"github.com/yaklabco/gomq/amqp"
)

// methodCollector captures methods sent by the channel for assertions.
type methodCollector struct {
	mu      sync.Mutex
	methods []amqp.Method
	headers []*amqp.HeaderFrame
	bodies  [][]byte
}

func (mc *methodCollector) sendMethod(_ uint16, m amqp.Method) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.methods = append(mc.methods, m)
	return nil
}

func (mc *methodCollector) sendContent(ch uint16, classID uint16, props *amqp.Properties, body []byte) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.headers = append(mc.headers, &amqp.HeaderFrame{
		Channel:    ch,
		ClassID:    classID,
		BodySize:   uint64(len(body)),
		Properties: *props,
	})
	mc.bodies = append(mc.bodies, body)
	return nil
}

func (mc *methodCollector) sendDelivery(_ uint16, method amqp.Method, classID uint16, props *amqp.Properties, body []byte) error {
	if err := mc.sendMethod(0, method); err != nil {
		return err
	}
	return mc.sendContent(0, classID, props, body)
}

func (mc *methodCollector) lastMethod() amqp.Method {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if len(mc.methods) == 0 {
		return nil
	}
	return mc.methods[len(mc.methods)-1]
}

func newTestChannel(t *testing.T) (*Channel, *methodCollector) {
	t.Helper()
	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	t.Cleanup(func() { _ = vh.Close() })

	mc := &methodCollector{}
	ch := NewChannel(1, vh, mc.sendMethod, mc.sendContent, mc.sendDelivery)
	return ch, mc
}

func TestChannel_ExchangeDeclare(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	err := ch.HandleMethod(&amqp.ExchangeDeclare{
		Exchange: "test-ex",
		Type:     "direct",
		Durable:  true,
	})
	if err != nil {
		t.Fatalf("HandleMethod(ExchangeDeclare) error: %v", err)
	}

	last := mc.lastMethod()
	if last == nil {
		t.Fatal("no response sent")
	}
	if _, ok := last.(*amqp.ExchangeDeclareOk); !ok {
		t.Errorf("response type = %T, want *ExchangeDeclareOk", last)
	}
}

func TestChannel_QueueDeclare(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	err := ch.HandleMethod(&amqp.QueueDeclare{
		Queue: "test-q",
	})
	if err != nil {
		t.Fatalf("HandleMethod(QueueDeclare) error: %v", err)
	}

	last := mc.lastMethod()
	if last == nil {
		t.Fatal("no response sent")
	}
	declOk, ok := last.(*amqp.QueueDeclareOk)
	if !ok {
		t.Fatalf("response type = %T, want *QueueDeclareOk", last)
	}
	if declOk.Queue != "test-q" {
		t.Errorf("Queue = %q, want %q", declOk.Queue, "test-q")
	}
}

func TestChannel_PublishAndConsume(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	// Declare queue.
	if err := ch.HandleMethod(&amqp.QueueDeclare{Queue: "pc-q"}); err != nil {
		t.Fatalf("QueueDeclare error: %v", err)
	}

	// Start consumer.
	if err := ch.HandleMethod(&amqp.BasicConsume{
		Queue:       "pc-q",
		ConsumerTag: "ctag-1",
		NoAck:       true,
	}); err != nil {
		t.Fatalf("BasicConsume error: %v", err)
	}

	// Publish a message through the default exchange.
	if err := ch.HandleMethod(&amqp.BasicPublish{
		Exchange:   "",
		RoutingKey: "pc-q",
	}); err != nil {
		t.Fatalf("BasicPublish error: %v", err)
	}
	if err := ch.HandleHeader(&amqp.HeaderFrame{
		Channel:  1,
		ClassID:  amqp.ClassBasic,
		BodySize: 5,
	}); err != nil {
		t.Fatalf("HandleHeader error: %v", err)
	}
	if err := ch.HandleBody(&amqp.BodyFrame{
		Channel: 1,
		Body:    []byte("hello"),
	}); err != nil {
		t.Fatalf("HandleBody error: %v", err)
	}

	// Wait for consumer delivery.
	deadline := time.After(2 * time.Second)
	for {
		mc.mu.Lock()
		found := false
		for _, m := range mc.methods {
			if _, ok := m.(*amqp.BasicDeliver); ok {
				found = true
				break
			}
		}
		mc.mu.Unlock()
		if found {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for BasicDeliver")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Verify the delivered message.
	mc.mu.Lock()
	defer mc.mu.Unlock()
	var deliver *amqp.BasicDeliver
	for _, m := range mc.methods {
		if d, ok := m.(*amqp.BasicDeliver); ok {
			deliver = d
			break
		}
	}
	if deliver == nil {
		t.Fatal("no BasicDeliver found")
	}
	if deliver.ConsumerTag != "ctag-1" {
		t.Errorf("ConsumerTag = %q, want %q", deliver.ConsumerTag, "ctag-1")
	}
	if len(mc.bodies) == 0 {
		t.Fatal("no body delivered")
	}
	if string(mc.bodies[len(mc.bodies)-1]) != "hello" {
		t.Errorf("body = %q, want %q", mc.bodies[len(mc.bodies)-1], "hello")
	}
}

func TestChannel_BasicGet(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	// Declare queue and publish a message.
	if err := ch.HandleMethod(&amqp.QueueDeclare{Queue: "get-q"}); err != nil {
		t.Fatalf("QueueDeclare error: %v", err)
	}
	if err := ch.HandleMethod(&amqp.BasicPublish{Exchange: "", RoutingKey: "get-q"}); err != nil {
		t.Fatalf("BasicPublish error: %v", err)
	}
	if err := ch.HandleHeader(&amqp.HeaderFrame{
		Channel: 1, ClassID: amqp.ClassBasic, BodySize: 3,
	}); err != nil {
		t.Fatalf("HandleHeader error: %v", err)
	}
	if err := ch.HandleBody(&amqp.BodyFrame{Channel: 1, Body: []byte("abc")}); err != nil {
		t.Fatalf("HandleBody error: %v", err)
	}

	// Basic.Get.
	if err := ch.HandleMethod(&amqp.BasicGet{Queue: "get-q", NoAck: true}); err != nil {
		t.Fatalf("BasicGet error: %v", err)
	}

	// Should have gotten GetOk.
	found := false
	mc.mu.Lock()
	for _, m := range mc.methods {
		if _, ok := m.(*amqp.BasicGetOk); ok {
			found = true
			break
		}
	}
	mc.mu.Unlock()
	if !found {
		t.Error("expected BasicGetOk response")
	}
}

func TestChannel_BasicGetEmpty(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	if err := ch.HandleMethod(&amqp.QueueDeclare{Queue: "empty-q"}); err != nil {
		t.Fatalf("QueueDeclare error: %v", err)
	}
	if err := ch.HandleMethod(&amqp.BasicGet{Queue: "empty-q", NoAck: true}); err != nil {
		t.Fatalf("BasicGet error: %v", err)
	}

	last := mc.lastMethod()
	if _, ok := last.(*amqp.BasicGetEmpty); !ok {
		t.Errorf("response type = %T, want *BasicGetEmpty", last)
	}
}

func TestChannel_Ack(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	// Declare queue, publish, get with noAck=false.
	if err := ch.HandleMethod(&amqp.QueueDeclare{Queue: "ack-q"}); err != nil {
		t.Fatalf("QueueDeclare error: %v", err)
	}
	if err := ch.HandleMethod(&amqp.BasicPublish{Exchange: "", RoutingKey: "ack-q"}); err != nil {
		t.Fatalf("BasicPublish error: %v", err)
	}
	if err := ch.HandleHeader(&amqp.HeaderFrame{
		Channel: 1, ClassID: amqp.ClassBasic, BodySize: 3,
	}); err != nil {
		t.Fatalf("HandleHeader error: %v", err)
	}
	if err := ch.HandleBody(&amqp.BodyFrame{Channel: 1, Body: []byte("ack")}); err != nil {
		t.Fatalf("HandleBody error: %v", err)
	}

	// Get without auto-ack.
	if err := ch.HandleMethod(&amqp.BasicGet{Queue: "ack-q", NoAck: false}); err != nil {
		t.Fatalf("BasicGet error: %v", err)
	}

	// Find the delivery tag from GetOk.
	mc.mu.Lock()
	var deliveryTag uint64
	for _, m := range mc.methods {
		if g, ok := m.(*amqp.BasicGetOk); ok {
			deliveryTag = g.DeliveryTag
			break
		}
	}
	mc.mu.Unlock()

	if deliveryTag == 0 {
		t.Fatal("no delivery tag found")
	}

	// Verify unacked list is populated.
	ch.mu.Lock()
	if len(ch.unacked) != 1 {
		t.Errorf("unacked count = %d, want 1", len(ch.unacked))
	}
	ch.mu.Unlock()

	// Ack.
	if err := ch.HandleMethod(&amqp.BasicAck{DeliveryTag: deliveryTag}); err != nil {
		t.Fatalf("BasicAck error: %v", err)
	}

	// Verify unacked cleared.
	ch.mu.Lock()
	if len(ch.unacked) != 0 {
		t.Errorf("unacked count after ack = %d, want 0", len(ch.unacked))
	}
	ch.mu.Unlock()
}

func TestChannel_ConfirmMode(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	// Enable confirm mode.
	if err := ch.HandleMethod(&amqp.ConfirmSelect{}); err != nil {
		t.Fatalf("ConfirmSelect error: %v", err)
	}

	last := mc.lastMethod()
	if _, ok := last.(*amqp.ConfirmSelectOk); !ok {
		t.Errorf("response = %T, want *ConfirmSelectOk", last)
	}

	// Declare queue and publish.
	if err := ch.HandleMethod(&amqp.QueueDeclare{Queue: "confirm-q"}); err != nil {
		t.Fatalf("QueueDeclare error: %v", err)
	}
	if err := ch.HandleMethod(&amqp.BasicPublish{Exchange: "", RoutingKey: "confirm-q"}); err != nil {
		t.Fatalf("BasicPublish error: %v", err)
	}
	if err := ch.HandleHeader(&amqp.HeaderFrame{
		Channel: 1, ClassID: amqp.ClassBasic, BodySize: 4,
	}); err != nil {
		t.Fatalf("HandleHeader error: %v", err)
	}
	if err := ch.HandleBody(&amqp.BodyFrame{Channel: 1, Body: []byte("conf")}); err != nil {
		t.Fatalf("HandleBody error: %v", err)
	}

	// Should have received a BasicAck with delivery tag 1.
	mc.mu.Lock()
	var confirmAck *amqp.BasicAck
	for _, m := range mc.methods {
		if a, ok := m.(*amqp.BasicAck); ok {
			confirmAck = a
			break
		}
	}
	mc.mu.Unlock()
	if confirmAck == nil {
		t.Fatal("no BasicAck confirm received")
	}
	if confirmAck.DeliveryTag != 1 {
		t.Errorf("confirm DeliveryTag = %d, want 1", confirmAck.DeliveryTag)
	}
}

func TestChannel_QueueBind(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	// Declare exchange and queue.
	if err := ch.HandleMethod(&amqp.ExchangeDeclare{Exchange: "bind-ex", Type: "direct"}); err != nil {
		t.Fatalf("ExchangeDeclare error: %v", err)
	}
	if err := ch.HandleMethod(&amqp.QueueDeclare{Queue: "bind-q"}); err != nil {
		t.Fatalf("QueueDeclare error: %v", err)
	}

	// Bind.
	if err := ch.HandleMethod(&amqp.QueueBind{
		Queue:      "bind-q",
		Exchange:   "bind-ex",
		RoutingKey: "key",
	}); err != nil {
		t.Fatalf("QueueBind error: %v", err)
	}

	last := mc.lastMethod()
	if _, ok := last.(*amqp.QueueBindOk); !ok {
		t.Errorf("response = %T, want *QueueBindOk", last)
	}

	// Publish through the exchange and verify it reaches the queue.
	if err := ch.HandleMethod(&amqp.BasicPublish{Exchange: "bind-ex", RoutingKey: "key"}); err != nil {
		t.Fatalf("BasicPublish error: %v", err)
	}
	if err := ch.HandleHeader(&amqp.HeaderFrame{
		Channel: 1, ClassID: amqp.ClassBasic, BodySize: 4,
	}); err != nil {
		t.Fatalf("HandleHeader error: %v", err)
	}
	if err := ch.HandleBody(&amqp.BodyFrame{Channel: 1, Body: []byte("bind")}); err != nil {
		t.Fatalf("HandleBody error: %v", err)
	}

	// Get from queue.
	if err := ch.HandleMethod(&amqp.BasicGet{Queue: "bind-q", NoAck: true}); err != nil {
		t.Fatalf("BasicGet error: %v", err)
	}

	found := false
	mc.mu.Lock()
	for _, m := range mc.methods {
		if _, ok := m.(*amqp.BasicGetOk); ok {
			found = true
			break
		}
	}
	mc.mu.Unlock()
	if !found {
		t.Error("expected BasicGetOk after publishing through bound exchange")
	}
}

func TestChannel_BasicQos(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	if err := ch.HandleMethod(&amqp.BasicQos{PrefetchCount: 10}); err != nil {
		t.Fatalf("BasicQos error: %v", err)
	}

	last := mc.lastMethod()
	if _, ok := last.(*amqp.BasicQosOk); !ok {
		t.Errorf("response = %T, want *BasicQosOk", last)
	}

	if ch.prefetchCount != 10 {
		t.Errorf("prefetchCount = %d, want 10", ch.prefetchCount)
	}
}

func TestChannel_BasicCancel(t *testing.T) {
	t.Parallel()
	ch, mc := newTestChannel(t)

	if err := ch.HandleMethod(&amqp.QueueDeclare{Queue: "cancel-q"}); err != nil {
		t.Fatalf("QueueDeclare error: %v", err)
	}
	if err := ch.HandleMethod(&amqp.BasicConsume{
		Queue: "cancel-q", ConsumerTag: "cancel-tag", NoAck: true,
	}); err != nil {
		t.Fatalf("BasicConsume error: %v", err)
	}
	if err := ch.HandleMethod(&amqp.BasicCancel{ConsumerTag: "cancel-tag"}); err != nil {
		t.Fatalf("BasicCancel error: %v", err)
	}

	mc.mu.Lock()
	var cancelOk *amqp.BasicCancelOk
	for _, m := range mc.methods {
		if c, ok := m.(*amqp.BasicCancelOk); ok {
			cancelOk = c
			break
		}
	}
	mc.mu.Unlock()
	if cancelOk == nil {
		t.Fatal("no BasicCancelOk received")
	}
	if cancelOk.ConsumerTag != "cancel-tag" {
		t.Errorf("ConsumerTag = %q, want %q", cancelOk.ConsumerTag, "cancel-tag")
	}

	ch.mu.Lock()
	if len(ch.consumers) != 0 {
		t.Errorf("consumers count = %d after cancel, want 0", len(ch.consumers))
	}
	ch.mu.Unlock()
}
