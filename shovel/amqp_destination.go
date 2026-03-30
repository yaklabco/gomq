package shovel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPDestination publishes messages to an AMQP broker exchange.
type AMQPDestination struct {
	URI        string
	Exchange   string
	RoutingKey string

	mu       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	confirms chan amqp.Confirmation
}

// NewAMQPDestination creates a destination that publishes to the given
// exchange with the specified routing key.
func NewAMQPDestination(uri, exchange, routingKey string) *AMQPDestination {
	return &AMQPDestination{
		URI:        uri,
		Exchange:   exchange,
		RoutingKey: routingKey,
	}
}

// Connect establishes a connection and channel to the AMQP broker
// with publisher confirms enabled.
func (d *AMQPDestination) Connect(_ context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	conn, err := amqp.Dial(d.URI)
	if err != nil {
		return fmt.Errorf("dial %s: %w", d.URI, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close() //nolint:errcheck // cleanup on failure
		return fmt.Errorf("open channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()   //nolint:errcheck // cleanup on failure
		_ = conn.Close() //nolint:errcheck // cleanup on failure
		return fmt.Errorf("enable confirms: %w", err)
	}

	d.conn = conn
	d.channel = ch
	d.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	return nil
}

// Publish sends a message to the configured exchange.
func (d *AMQPDestination) Publish(msg amqp.Publishing) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.channel == nil {
		return errors.New("destination not connected")
	}

	if err := d.channel.Publish(d.Exchange, d.RoutingKey, false, false, msg); err != nil {
		return fmt.Errorf("publish to exchange %q: %w", d.Exchange, err)
	}

	return nil
}

// Close shuts down the channel and connection.
func (d *AMQPDestination) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var errs []error
	if d.channel != nil {
		if err := d.channel.Close(); err != nil {
			errs = append(errs, err)
		}
		d.channel = nil
	}
	if d.conn != nil {
		if err := d.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		d.conn = nil
	}
	return errors.Join(errs...)
}
