package shovel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// mockSource is a test double for the Source interface.
type mockSource struct {
	mu         sync.Mutex
	connected  bool
	closed     bool
	acked      []uint64
	deliveries chan amqp.Delivery
	connectErr error
	consumeErr error
	ackErr     error
}

func newMockSource() *mockSource {
	return &mockSource{
		deliveries: make(chan amqp.Delivery, 10),
	}
}

func (m *mockSource) Connect(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected = true
	return nil
}

func (m *mockSource) Consume() (<-chan amqp.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.consumeErr != nil {
		return nil, m.consumeErr
	}
	return m.deliveries, nil
}

func (m *mockSource) Ack(tag uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ackErr != nil {
		return m.ackErr
	}
	m.acked = append(m.acked, tag)
	return nil
}

func (m *mockSource) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockSource) isConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

func (m *mockSource) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockSource) ackedTags() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]uint64, len(m.acked))
	copy(result, m.acked)
	return result
}

// mockDestination is a test double for the Destination interface.
type mockDestination struct {
	mu         sync.Mutex
	connected  bool
	closed     bool
	published  []amqp.Publishing
	connectErr error
	publishErr error
}

func newMockDestination() *mockDestination {
	return &mockDestination{}
}

func (m *mockDestination) Connect(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected = true
	return nil
}

func (m *mockDestination) Publish(msg amqp.Publishing) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, msg)
	return nil
}

func (m *mockDestination) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockDestination) isConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

func (m *mockDestination) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockDestination) publishedMessages() []amqp.Publishing {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]amqp.Publishing, len(m.published))
	copy(result, m.published)
	return result
}

// waitForStatus polls the shovel's status until it matches want or
// the timeout expires.
func waitForStatus(t *testing.T, shvl *Shovel, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for shvl.Status() != want {
		select {
		case <-deadline:
			t.Fatalf("shovel did not reach status %q within %v, got %q", want, timeout, shvl.Status())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// waitForCondition polls until the condition returns true or timeout.
func waitForCondition(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("condition not met within %v: %s", timeout, msg)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestNewShovel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		ackMode      AckMode
		prefetch     int
		wantAckMode  AckMode
		wantPrefetch int
	}{
		{
			name:         "explicit values",
			ackMode:      AckOnPublish,
			prefetch:     500,
			wantAckMode:  AckOnPublish,
			wantPrefetch: 500,
		},
		{
			name:         "defaults applied for zero values",
			ackMode:      "",
			prefetch:     0,
			wantAckMode:  DefaultAckMode,
			wantPrefetch: DefaultPrefetch,
		},
		{
			name:         "negative prefetch uses default",
			ackMode:      AckNoAck,
			prefetch:     -1,
			wantAckMode:  AckNoAck,
			wantPrefetch: DefaultPrefetch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			src := newMockSource()
			dst := newMockDestination()

			shvl := NewShovel("test", src, dst, tt.ackMode, tt.prefetch)

			if shvl.AckMode != tt.wantAckMode {
				t.Errorf("AckMode = %q, want %q", shvl.AckMode, tt.wantAckMode)
			}
			if shvl.Prefetch != tt.wantPrefetch {
				t.Errorf("Prefetch = %d, want %d", shvl.Prefetch, tt.wantPrefetch)
			}
			if shvl.Status() != StatusStopped {
				t.Errorf("Status() = %q, want %q", shvl.Status(), StatusStopped)
			}
		})
	}
}

func TestShovelStartStop(t *testing.T) {
	t.Parallel()

	src := newMockSource()
	dst := newMockDestination()
	shvl := NewShovel("test-start-stop", src, dst, AckOnPublish, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := shvl.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	// Wait for the shovel to reach running status (source and dest connected).
	waitForStatus(t, shvl, StatusRunning, 2*time.Second)

	if !src.isConnected() {
		t.Error("source not connected after Start")
	}
	if !dst.isConnected() {
		t.Error("destination not connected after Start")
	}

	// Double start should error.
	if err := shvl.Start(ctx); err == nil {
		t.Error("expected error on double Start, got nil")
	}

	if err := shvl.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}

	if shvl.Status() != StatusStopped {
		t.Errorf("Status() after Stop = %q, want %q", shvl.Status(), StatusStopped)
	}

	if !src.isClosed() {
		t.Error("source not closed after Stop")
	}
	if !dst.isClosed() {
		t.Error("destination not closed after Stop")
	}
}

func TestShovelForwardMessages(t *testing.T) {
	t.Parallel()

	src := newMockSource()
	dst := newMockDestination()
	shvl := NewShovel("test-forward", src, dst, AckOnPublish, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := shvl.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	waitForStatus(t, shvl, StatusRunning, 2*time.Second)

	// Send messages through the source.
	for i := range 3 {
		src.deliveries <- amqp.Delivery{
			DeliveryTag: uint64(i + 1),
			Body:        []byte(fmt.Sprintf("msg-%d", i)),
			ContentType: "text/plain",
		}
	}

	waitForCondition(t, func() bool {
		return len(dst.publishedMessages()) >= 3
	}, 2*time.Second, "expected 3 published messages")

	msgs := dst.publishedMessages()
	for i, msg := range msgs {
		want := fmt.Sprintf("msg-%d", i)
		if string(msg.Body) != want {
			t.Errorf("message %d body = %q, want %q", i, msg.Body, want)
		}
	}

	// Check acks.
	acked := src.ackedTags()
	if len(acked) != 3 {
		t.Fatalf("expected 3 acks, got %d", len(acked))
	}
	for i, tag := range acked {
		if tag != uint64(i+1) {
			t.Errorf("ack %d tag = %d, want %d", i, tag, i+1)
		}
	}

	if err := shvl.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}
}

func TestShovelNoAckMode(t *testing.T) {
	t.Parallel()

	src := newMockSource()
	dst := newMockDestination()
	shvl := NewShovel("test-noack", src, dst, AckNoAck, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := shvl.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	waitForStatus(t, shvl, StatusRunning, 2*time.Second)

	src.deliveries <- amqp.Delivery{
		DeliveryTag: 1,
		Body:        []byte("no-ack-msg"),
	}

	waitForCondition(t, func() bool {
		return len(dst.publishedMessages()) >= 1
	}, 2*time.Second, "expected 1 published message")

	// No-ack mode should not ack.
	acked := src.ackedTags()
	if len(acked) != 0 {
		t.Errorf("expected 0 acks in no-ack mode, got %d", len(acked))
	}

	if err := shvl.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}
}

func TestShovelReconnectOnError(t *testing.T) {
	t.Parallel()

	src := newMockSource()
	dst := newMockDestination()

	// Override connect to fail once then succeed.
	failOnce := &failOnceSource{
		inner:     src,
		failCount: 1,
	}

	shvl := NewShovel("test-reconnect", failOnce, dst, AckOnPublish, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := shvl.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	waitForStatus(t, shvl, StatusRunning, 15*time.Second)

	if err := shvl.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}
}

// failOnceSource wraps a source and fails the first N connect attempts.
type failOnceSource struct {
	inner     *mockSource
	failCount int
	mu        sync.Mutex
	attempts  int
}

func (f *failOnceSource) Connect(ctx context.Context) error {
	f.mu.Lock()
	f.attempts++
	shouldFail := f.attempts <= f.failCount
	f.mu.Unlock()

	if shouldFail {
		return errors.New("simulated connection failure")
	}

	// Clear the error on the inner source.
	f.inner.mu.Lock()
	f.inner.connectErr = nil
	f.inner.mu.Unlock()

	return f.inner.Connect(ctx)
}

func (f *failOnceSource) Consume() (<-chan amqp.Delivery, error) {
	return f.inner.Consume()
}

func (f *failOnceSource) Ack(tag uint64) error {
	return f.inner.Ack(tag)
}

func (f *failOnceSource) Close() error {
	return f.inner.Close()
}

func TestShovelStopIdempotent(t *testing.T) {
	t.Parallel()

	src := newMockSource()
	dst := newMockDestination()
	shvl := NewShovel("test-stop-idle", src, dst, AckOnPublish, 10)

	// Stop without start should not error.
	if err := shvl.Stop(); err != nil {
		t.Fatalf("Stop() without Start error: %v", err)
	}

	// Double stop should not error.
	if err := shvl.Stop(); err != nil {
		t.Fatalf("second Stop() error: %v", err)
	}
}

func TestShovelContextCancellation(t *testing.T) {
	t.Parallel()

	src := newMockSource()
	dst := newMockDestination()
	shvl := NewShovel("test-ctx-cancel", src, dst, AckOnPublish, 10)

	ctx, cancel := context.WithCancel(context.Background())

	if err := shvl.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	waitForStatus(t, shvl, StatusRunning, 2*time.Second)

	// Cancel context should stop the shovel.
	cancel()

	waitForStatus(t, shvl, StatusStopped, 2*time.Second)
}
