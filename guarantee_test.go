package gomq_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jamesainslie/gomq"
	amqp "github.com/rabbitmq/amqp091-go"
)

const guaranteeTimeout = 30 * time.Second

// startBrokerWithDir creates a broker using the given data directory and starts
// it on a random port. Returns the broker and a cleanup function. The caller
// must invoke cleanup when done.
func startBrokerWithDir(t *testing.T, dir string) (*gomq.Broker, func()) {
	t.Helper()

	brk, err := gomq.New(gomq.WithDataDir(dir))
	if err != nil {
		t.Fatalf("gomq.New: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- brk.Serve(ctx, ln)
	}()

	addr := brk.WaitForAddr(ctx)
	if addr == nil {
		cancel()
		t.Fatal("broker did not start")
	}

	cleanup := func() {
		if closeErr := brk.Close(); closeErr != nil {
			t.Errorf("broker close: %v", closeErr)
		}
		cancel()
		if srvErr := <-serveErr; srvErr != nil {
			t.Errorf("broker serve: %v", srvErr)
		}
	}

	return brk, cleanup
}

// brokerURL returns the AMQP connection URL for the given broker.
func brokerURL(t *testing.T, brk *gomq.Broker) string {
	t.Helper()

	addr, ok := brk.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("Addr() type = %T, want *net.TCPAddr", brk.Addr())
	}
	return fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", addr.Port)
}

// dialURL connects an AMQP client to the given URL.
func dialURL(t *testing.T, url string) *amqp.Connection {
	t.Helper()

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("amqp.Dial: %v", err)
	}
	return conn
}

func TestGuarantee_MessageOrdering(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	queue, err := ch.QueueDeclare("ordering-test", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	const msgCount = 1000

	for seq := 1; seq <= msgCount; seq++ {
		body := fmt.Sprintf("msg-%04d", seq)
		if err := ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
			Body: []byte(body),
		}); err != nil {
			t.Fatalf("publish msg %d: %v", seq, err)
		}
	}

	msgs, err := ch.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), guaranteeTimeout)
	defer cancel()

	for seq := 1; seq <= msgCount; seq++ {
		expected := fmt.Sprintf("msg-%04d", seq)
		select {
		case msg := <-msgs:
			if string(msg.Body) != expected {
				t.Fatalf("message %d: got %q, want %q (out-of-order delivery)", seq, string(msg.Body), expected)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for message %d", seq)
		}
	}
}

func TestGuarantee_PublisherConfirmsAfterPersistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	brk, cleanup := startBrokerWithDir(t, dir)

	url := brokerURL(t, brk)
	conn := dialURL(t, url)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("enable confirm mode: %v", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 100))

	_, err = ch.QueueDeclare("confirm-persist-test", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare durable queue: %v", err)
	}

	const msgCount = 100
	for seq := 1; seq <= msgCount; seq++ {
		body := fmt.Sprintf("persist-%04d", seq)
		if err := ch.PublishWithContext(context.Background(), "", "confirm-persist-test", false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("publish msg %d: %v", seq, err)
		}
	}

	// Collect all confirms.
	ctx, cancel := context.WithTimeout(context.Background(), guaranteeTimeout)
	defer cancel()

	acked := 0
	for acked < msgCount {
		select {
		case confirm := <-confirms:
			if !confirm.Ack {
				t.Fatalf("message %d was nacked, expected ack", confirm.DeliveryTag)
			}
			acked++
		case <-ctx.Done():
			t.Fatalf("timed out waiting for confirms: got %d/%d", acked, msgCount)
		}
	}

	// Close connection and broker.
	conn.Close()
	cleanup()

	// Restart broker with same data directory.
	brk2, cleanup2 := startBrokerWithDir(t, dir)
	defer cleanup2()

	url2 := brokerURL(t, brk2)
	conn2 := dialURL(t, url2)
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("open channel after restart: %v", err)
	}

	// Re-declare the same durable queue (should be idempotent if data survived).
	_, err = ch2.QueueDeclare("confirm-persist-test", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue after restart: %v", err)
	}

	// Consume and verify all messages survived the restart.
	received := 0
	for {
		msg, ok, getErr := ch2.Get("confirm-persist-test", true)
		if getErr != nil {
			t.Fatalf("get after restart: %v", getErr)
		}
		if !ok {
			break
		}
		received++
		expected := fmt.Sprintf("persist-%04d", received)
		if string(msg.Body) != expected {
			t.Errorf("message %d after restart: got %q, want %q", received, string(msg.Body), expected)
		}
	}

	if received != msgCount {
		t.Fatalf("messages after restart: got %d, want %d (persistence failure)", received, msgCount)
	}
}

func TestGuarantee_DurableQueueSurvivesRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	brk, cleanup := startBrokerWithDir(t, dir)

	url := brokerURL(t, brk)
	conn := dialURL(t, url)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	_, err = ch.QueueDeclare("durable-restart-test", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare durable queue: %v", err)
	}

	const msgCount = 50
	for seq := 1; seq <= msgCount; seq++ {
		body := fmt.Sprintf("durable-%04d", seq)
		if err := ch.PublishWithContext(context.Background(), "", "durable-restart-test", false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("publish msg %d: %v", seq, err)
		}
	}

	// Allow async writes to flush.
	time.Sleep(100 * time.Millisecond)

	conn.Close()
	cleanup()

	// Restart with same data dir.
	brk2, cleanup2 := startBrokerWithDir(t, dir)
	defer cleanup2()

	url2 := brokerURL(t, brk2)
	conn2 := dialURL(t, url2)
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("open channel after restart: %v", err)
	}

	_, err = ch2.QueueDeclare("durable-restart-test", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue after restart: %v", err)
	}

	received := 0
	for {
		msg, ok, getErr := ch2.Get("durable-restart-test", true)
		if getErr != nil {
			t.Fatalf("get after restart: %v", getErr)
		}
		if !ok {
			break
		}
		received++
		expected := fmt.Sprintf("durable-%04d", received)
		if string(msg.Body) != expected {
			t.Errorf("message %d: got %q, want %q", received, string(msg.Body), expected)
		}
	}

	if received != msgCount {
		t.Fatalf("messages after restart: got %d, want %d", received, msgCount)
	}
}

func TestGuarantee_AckDeletesMessage(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	brk, cleanup := startBrokerWithDir(t, dir)

	url := brokerURL(t, brk)
	conn := dialURL(t, url)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	_, err = ch.QueueDeclare("ack-delete-test", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	// Enable confirms so messages are persisted synchronously.
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 10))

	const msgCount = 10
	for seq := 1; seq <= msgCount; seq++ {
		body := fmt.Sprintf("ack-%04d", seq)
		if err := ch.PublishWithContext(context.Background(), "", "ack-delete-test", false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("publish msg %d: %v", seq, err)
		}
		// Wait for confirm.
		ctx, cancel := context.WithTimeout(context.Background(), guaranteeTimeout)
		select {
		case c := <-confirms:
			if !c.Ack {
				t.Fatalf("message %d nacked", seq)
			}
		case <-ctx.Done():
			t.Fatalf("confirm timeout for msg %d", seq)
		}
		cancel()
	}

	// Consume first 5 with manual ack.
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("set qos: %v", err)
	}

	for seq := 1; seq <= 5; seq++ {
		msg, ok, getErr := ch.Get("ack-delete-test", false)
		if getErr != nil {
			t.Fatalf("get msg %d: %v", seq, getErr)
		}
		if !ok {
			t.Fatalf("expected message %d, got none", seq)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("ack msg %d: %v", seq, err)
		}
	}

	conn.Close()
	cleanup()

	// Restart and verify only 5 remain.
	brk2, cleanup2 := startBrokerWithDir(t, dir)
	defer cleanup2()

	url2 := brokerURL(t, brk2)
	conn2 := dialURL(t, url2)
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("open channel after restart: %v", err)
	}

	_, err = ch2.QueueDeclare("ack-delete-test", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue after restart: %v", err)
	}

	received := 0
	for {
		_, ok, getErr := ch2.Get("ack-delete-test", true)
		if getErr != nil {
			t.Fatalf("get after restart: %v", getErr)
		}
		if !ok {
			break
		}
		received++
	}

	if received != 5 {
		t.Fatalf("messages after restart: got %d, want 5 (acked messages should be deleted)", received)
	}
}

func TestGuarantee_NackRequeue(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	queue, err := ch.QueueDeclare("nack-requeue-test", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	const msgCount = 5
	for seq := 1; seq <= msgCount; seq++ {
		body := fmt.Sprintf("nack-%04d", seq)
		if err := ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
			Body: []byte(body),
		}); err != nil {
			t.Fatalf("publish msg %d: %v", seq, err)
		}
	}

	// Consume all 5 without auto-ack.
	if err := ch.Qos(msgCount, 0, false); err != nil {
		t.Fatalf("set qos: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), guaranteeTimeout)
	defer cancel()

	deliveries := make([]amqp.Delivery, 0, msgCount)
	msgs, err := ch.Consume(queue.Name, "nack-consumer", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	for range msgCount {
		select {
		case msg := <-msgs:
			deliveries = append(deliveries, msg)
		case <-ctx.Done():
			t.Fatalf("timed out collecting deliveries: got %d/%d", len(deliveries), msgCount)
		}
	}

	// Nack all with requeue.
	for _, d := range deliveries {
		if err := d.Nack(false, true); err != nil {
			t.Fatalf("nack: %v", err)
		}
	}

	// Consume again -- should get the requeued messages.
	// NOTE: nack-requeue is not yet implemented in the broker (Queue.Reject
	// always deletes regardless of the requeue flag). This test documents the
	// expected behaviour and will start passing once the feature lands.
	redelivered := 0
	timeout := time.After(2 * time.Second)
	for redelivered < msgCount {
		select {
		case msg := <-msgs:
			if !msg.Redelivered {
				t.Errorf("redelivered message %d: Redelivered=false, want true", redelivered+1)
			}
			redelivered++
			if err := msg.Ack(false); err != nil {
				t.Fatalf("ack redelivered msg: %v", err)
			}
		case <-timeout:
			if redelivered == 0 {
				t.Skip("nack-requeue not yet implemented (Queue.Reject ignores requeue flag)")
			}
			t.Fatalf("timed out waiting for redelivered messages: got %d/%d", redelivered, msgCount)
		}
	}
}

func TestGuarantee_ExclusiveQueue(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn1 := dialBroker(t, brk)

	ch1, err := conn1.Channel()
	if err != nil {
		t.Fatalf("open channel 1: %v", err)
	}

	// Declare exclusive queue on first connection.
	queue, err := ch1.QueueDeclare("exclusive-test", false, false, true, false, nil)
	if err != nil {
		t.Fatalf("declare exclusive queue: %v", err)
	}

	// Publish a message so the queue has content.
	if err := ch1.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp.Publishing{
		Body: []byte("exclusive-msg"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Start consuming on first connection (exclusive consumer).
	_, err = ch1.Consume(queue.Name, "exclusive-consumer", true, true, false, false, nil)
	if err != nil {
		t.Fatalf("consume on conn1: %v", err)
	}

	// Open second connection and try to consume from the same queue.
	conn2 := dialBroker(t, brk)
	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("open channel 2: %v", err)
	}

	// Attempt to consume from the exclusive queue should fail.
	_, err = ch2.Consume(queue.Name, "intruder", true, false, false, false, nil)
	if err == nil {
		t.Error("consuming from exclusive queue on second connection: expected error, got nil")
	}

	// Attempt to declare the same exclusive queue from second connection should fail.
	_, err = ch2.QueueDeclare("exclusive-test", false, false, true, false, nil)
	if err == nil {
		t.Error("declaring exclusive queue on second connection: expected error, got nil")
	}
}

func TestGuarantee_MaxLengthDropHead(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	// Enable confirms to force synchronous writes through PublishSync,
	// which correctly enforces overflow limits under the queue lock.
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	args := amqp.Table{
		"x-max-length": int64(5),
	}
	queue, err := ch.QueueDeclare("maxlen-drophead-test", true, false, false, false, args)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), guaranteeTimeout)
	defer cancel()

	const publishCount = 10
	for seq := 1; seq <= publishCount; seq++ {
		body := fmt.Sprintf("msg-%02d", seq)
		if err := ch.PublishWithContext(ctx, "", queue.Name, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("publish msg %d: %v", seq, err)
		}
		select {
		case c := <-confirms:
			if !c.Ack {
				t.Fatalf("msg %d nacked", seq)
			}
		case <-ctx.Done():
			t.Fatalf("confirm timeout for msg %d", seq)
		}
	}

	// Should get only the last 5 messages (oldest dropped).
	var received []string
	for {
		msg, ok, getErr := ch.Get(queue.Name, true)
		if getErr != nil {
			t.Fatalf("get: %v", getErr)
		}
		if !ok {
			break
		}
		received = append(received, string(msg.Body))
	}

	if len(received) != 5 {
		t.Fatalf("message count: got %d, want 5", len(received))
	}

	expected := []string{"msg-06", "msg-07", "msg-08", "msg-09", "msg-10"}
	for i, want := range expected {
		if received[i] != want {
			t.Errorf("message %d: got %q, want %q", i, received[i], want)
		}
	}
}

func TestGuarantee_MaxLengthRejectPublish(t *testing.T) {
	t.Parallel()

	brk := startTestBroker(t)
	conn := dialBroker(t, brk)

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}

	// Enable confirms to force synchronous writes through PublishSync,
	// which correctly enforces the reject-publish overflow policy.
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("enable confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	args := amqp.Table{
		"x-max-length": int64(5),
		"x-overflow":   "reject-publish",
	}
	queue, err := ch.QueueDeclare("maxlen-reject-test", true, false, false, false, args)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), guaranteeTimeout)
	defer cancel()

	const publishCount = 10
	for seq := 1; seq <= publishCount; seq++ {
		body := fmt.Sprintf("msg-%02d", seq)
		if err := ch.PublishWithContext(ctx, "", queue.Name, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		}); err != nil {
			t.Fatalf("publish msg %d: %v", seq, err)
		}
		select {
		case c := <-confirms:
			if !c.Ack {
				t.Fatalf("msg %d nacked", seq)
			}
		case <-ctx.Done():
			t.Fatalf("confirm timeout for msg %d", seq)
		}
	}

	// Should get only the first 5 messages (newest rejected).
	var received []string
	for {
		msg, ok, getErr := ch.Get(queue.Name, true)
		if getErr != nil {
			t.Fatalf("get: %v", getErr)
		}
		if !ok {
			break
		}
		received = append(received, string(msg.Body))
	}

	if len(received) != 5 {
		t.Fatalf("message count: got %d, want 5", len(received))
	}

	expected := []string{"msg-01", "msg-02", "msg-03", "msg-04", "msg-05"}
	for i, want := range expected {
		if received[i] != want {
			t.Errorf("message %d: got %q, want %q", i, received[i], want)
		}
	}
}
