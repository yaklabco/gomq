package shovel

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestFederationDestinationHopTracking(t *testing.T) {
	t.Parallel()

	inner := newMockDestination()
	mockDest := &mockFederationDest{mock: inner, maxHops: 2}

	tests := []struct {
		name      string
		headers   amqp.Table
		wantDrop  bool
		wantHops  int
		wantCount int // expected published count after this test
	}{
		{
			name:     "no headers adds hop count",
			headers:  nil,
			wantDrop: false,
			wantHops: 1,
		},
		{
			name:     "zero hops increments to 1",
			headers:  amqp.Table{HeaderFederationHops: int32(0)},
			wantDrop: false,
			wantHops: 1,
		},
		{
			name:     "one hop increments to 2",
			headers:  amqp.Table{HeaderFederationHops: int32(1)},
			wantDrop: false,
			wantHops: 2,
		},
		{
			name:     "at max hops drops message",
			headers:  amqp.Table{HeaderFederationHops: int32(2)},
			wantDrop: true,
		},
		{
			name:     "over max hops drops message",
			headers:  amqp.Table{HeaderFederationHops: int32(5)},
			wantDrop: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msg := amqp.Publishing{
				Body:    []byte("test"),
				Headers: tt.headers,
			}

			published, dropped := mockDest.publish(msg)

			if tt.wantDrop {
				if !dropped {
					t.Error("expected message to be dropped")
				}
				if published {
					t.Error("dropped message should not be published")
				}
			} else {
				if dropped {
					t.Error("expected message to be published, not dropped")
				}
				if !published {
					t.Error("expected message to be published")
				}
			}
		})
	}
}

// mockFederationDest simulates the federation destination logic without
// requiring a real AMQP connection.
type mockFederationDest struct {
	mock    *mockDestination
	maxHops int
}

func (m *mockFederationDest) publish(msg amqp.Publishing) (bool, bool) {
	hops := extractHops(msg.Headers)
	if hops >= m.maxHops {
		return false, true
	}

	if msg.Headers == nil {
		msg.Headers = make(amqp.Table)
	}
	msg.Headers[HeaderFederationHops] = int32(hops + 1) //nolint:gosec // hops bounded by maxHops

	if err := m.mock.Publish(msg); err != nil {
		return false, false
	}
	return true, false
}

func TestExtractHops(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		headers amqp.Table
		want    int
	}{
		{name: "nil headers", headers: nil, want: 0},
		{name: "empty headers", headers: amqp.Table{}, want: 0},
		{name: "missing key", headers: amqp.Table{"other": "val"}, want: 0},
		{name: "int32 value", headers: amqp.Table{HeaderFederationHops: int32(3)}, want: 3},
		{name: "int64 value", headers: amqp.Table{HeaderFederationHops: int64(5)}, want: 5},
		{name: "int value", headers: amqp.Table{HeaderFederationHops: 7}, want: 7},
		{name: "string value", headers: amqp.Table{HeaderFederationHops: "bad"}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractHops(tt.headers)
			if got != tt.want {
				t.Errorf("extractHops() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNewFederationLink(t *testing.T) {
	t.Parallel()

	upstream := &FederationUpstream{
		Name:     "test-upstream",
		URI:      "amqp://remote:5672",
		Exchange: "events",
		MaxHops:  2,
		Prefetch: 100,
	}

	link := NewFederationLink(upstream, "amqp://localhost:5672", "events")

	if link.Name != "federation-test-upstream" {
		t.Errorf("Name = %q, want %q", link.Name, "federation-test-upstream")
	}
	if link.AckMode != AckOnPublish {
		t.Errorf("AckMode = %q, want %q", link.AckMode, AckOnPublish)
	}
	if link.Prefetch != 100 {
		t.Errorf("Prefetch = %d, want 100", link.Prefetch)
	}
}

func TestNewFederationLinkDefaults(t *testing.T) {
	t.Parallel()

	upstream := &FederationUpstream{
		Name: "default-upstream",
		URI:  "amqp://remote:5672",
	}

	link := NewFederationLink(upstream, "amqp://localhost:5672", "")

	if upstream.MaxHops != DefaultMaxHops {
		t.Errorf("MaxHops = %d, want %d", upstream.MaxHops, DefaultMaxHops)
	}
	if upstream.Prefetch != DefaultPrefetch {
		t.Errorf("Prefetch = %d, want %d", upstream.Prefetch, DefaultPrefetch)
	}
	_ = link
}

func TestFederationLoopPrevention(t *testing.T) {
	t.Parallel()

	// Use a mock approach to test loop prevention.
	// Message with 0 hops should pass, with 1 hop should be dropped.
	inner := newMockDestination()
	mockDest := &mockFederationDest{mock: inner, maxHops: 1}

	// First forward: no hops yet, should pass.
	msg1 := amqp.Publishing{Body: []byte("first"), Headers: nil}
	published, dropped := mockDest.publish(msg1)
	if !published || dropped {
		t.Error("message with 0 hops should be forwarded")
	}

	// Second forward: message already has 1 hop, should be dropped.
	msg2 := amqp.Publishing{
		Body:    []byte("loop"),
		Headers: amqp.Table{HeaderFederationHops: int32(1)},
	}
	published, dropped = mockDest.publish(msg2)
	if published || !dropped {
		t.Error("message at max hops should be dropped")
	}
}

func TestFederationConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     FederationConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: FederationConfig{
				Name: "test",
				URI:  "amqp://remote:5672",
			},
			wantErr: false,
		},
		{
			name: "missing name",
			cfg: FederationConfig{
				URI: "amqp://remote:5672",
			},
			wantErr: true,
		},
		{
			name: "missing URI",
			cfg: FederationConfig{
				Name: "test",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestScrubURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		uri  string
		want string
	}{
		{
			name: "with credentials",
			uri:  "amqp://user:pass@host:5672/vhost",
			want: "amqp://***@host:5672/vhost",
		},
		{
			name: "amqps with credentials",
			uri:  "amqps://user:pass@host:5672/vhost",
			want: "amqps://***@host:5672/vhost",
		},
		{
			name: "no credentials",
			uri:  "amqp://host:5672/vhost",
			want: "amqp://host:5672/vhost",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ScrubURI(tt.uri)
			if got != tt.want {
				t.Errorf("ScrubURI() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFederationLinkIntegration(t *testing.T) {
	t.Parallel()

	// Test that a federation link works end-to-end with mock source/dest.
	src := newMockSource()
	inner := newMockDestination()
	mockFedDest := &testFedDestination{
		mock:    inner,
		maxHops: 2,
	}

	shvl := NewShovel("federation-test", src, mockFedDest, AckOnPublish, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := shvl.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	waitForStatus(t, shvl, StatusRunning, 2*time.Second)

	// Send a message without hop header.
	src.deliveries <- amqp.Delivery{
		DeliveryTag: 1,
		Body:        []byte("federated-msg"),
	}

	waitForCondition(t, func() bool {
		return len(inner.publishedMessages()) >= 1
	}, 2*time.Second, "expected 1 published message")

	msgs := inner.publishedMessages()
	hops := extractHops(msgs[0].Headers)
	if hops != 1 {
		t.Errorf("hops after federation = %d, want 1", hops)
	}

	// Send a message that should be dropped (at max hops).
	src.deliveries <- amqp.Delivery{
		DeliveryTag: 2,
		Body:        []byte("should-drop"),
		Headers:     amqp.Table{HeaderFederationHops: int32(2)},
	}

	// Give it time to process, then verify it wasn't published.
	time.Sleep(100 * time.Millisecond)
	if len(inner.publishedMessages()) != 1 {
		t.Errorf("expected 1 published message (dropped one at max hops), got %d",
			len(inner.publishedMessages()))
	}

	if err := shvl.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}
}

// testFedDestination is a test double that implements Destination with
// federation hop tracking, backed by a mockDestination.
type testFedDestination struct {
	mock    *mockDestination
	maxHops int
}

func (d *testFedDestination) Connect(ctx context.Context) error {
	return d.mock.Connect(ctx)
}

func (d *testFedDestination) Publish(msg amqp.Publishing) error {
	hops := extractHops(msg.Headers)
	if hops >= d.maxHops {
		return nil // silently drop
	}

	if msg.Headers == nil {
		msg.Headers = make(amqp.Table)
	}
	msg.Headers[HeaderFederationHops] = int32(hops + 1) //nolint:gosec // hops bounded by maxHops

	return d.mock.Publish(msg)
}

func (d *testFedDestination) Close() error {
	return d.mock.Close()
}
