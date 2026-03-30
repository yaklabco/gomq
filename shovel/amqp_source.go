package shovel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPSource consumes messages from an AMQP broker queue.
type AMQPSource struct {
	URI      string
	Queue    string
	Prefetch int

	mu         sync.Mutex
	conn       *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
}

// NewAMQPSource creates an AMQP source that consumes from the named queue.
func NewAMQPSource(uri, queue string, prefetch int) *AMQPSource {
	if prefetch <= 0 {
		prefetch = DefaultPrefetch
	}
	return &AMQPSource{
		URI:      uri,
		Queue:    queue,
		Prefetch: prefetch,
	}
}

// Connect establishes a connection and channel to the AMQP broker.
func (s *AMQPSource) Connect(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := amqp.Dial(s.URI)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.URI, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close() //nolint:errcheck // cleanup on failure
		return fmt.Errorf("open channel: %w", err)
	}

	if err := ch.Qos(s.Prefetch, 0, false); err != nil {
		_ = ch.Close()   //nolint:errcheck // cleanup on failure
		_ = conn.Close() //nolint:errcheck // cleanup on failure
		return fmt.Errorf("set qos: %w", err)
	}

	s.conn = conn
	s.channel = ch
	return nil
}

// Consume starts consuming from the configured queue and returns the
// delivery channel.
func (s *AMQPSource) Consume() (<-chan amqp.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.channel == nil {
		return nil, errors.New("source not connected")
	}

	deliveries, err := s.channel.Consume(
		s.Queue,
		"",    // consumer tag (auto-generated)
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("consume queue %q: %w", s.Queue, err)
	}

	s.deliveries = deliveries
	return deliveries, nil
}

// Ack acknowledges a message by delivery tag.
func (s *AMQPSource) Ack(tag uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.channel == nil {
		return errors.New("source not connected")
	}
	return s.channel.Ack(tag, false)
}

// Close shuts down the channel and connection.
func (s *AMQPSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	if s.channel != nil {
		if err := s.channel.Close(); err != nil {
			errs = append(errs, err)
		}
		s.channel = nil
	}
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		s.conn = nil
	}
	return errors.Join(errs...)
}
