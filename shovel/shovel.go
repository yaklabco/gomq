// Package shovel provides cross-broker message forwarding via shovels
// and federation links. A shovel consumes messages from a source and
// publishes them to a destination, supporting AMQP and HTTP endpoints.
package shovel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AckMode controls when source messages are acknowledged.
type AckMode string

const (
	// AckOnConfirm acknowledges after the destination confirms receipt.
	AckOnConfirm AckMode = "on-confirm"
	// AckOnPublish acknowledges immediately after publishing.
	AckOnPublish AckMode = "on-publish"
	// AckNoAck disables acknowledgments (no-ack consumer).
	AckNoAck AckMode = "no-ack"
)

// Default shovel configuration values.
const (
	DefaultPrefetch       = 1000
	DefaultAckMode        = AckOnConfirm
	DefaultReconnectDelay = 5 * time.Second

	maxReconnectDelay = 60 * time.Second
	backoffMultiplier = 2
)

// Status values for a shovel.
const (
	StatusRunning = "running"
	StatusStopped = "stopped"
	StatusError   = "error"
)

// Source consumes messages from an upstream endpoint.
type Source interface {
	Connect(ctx context.Context) error
	Consume() (<-chan amqp.Delivery, error)
	Ack(tag uint64) error
	Close() error
}

// Destination publishes messages to a downstream endpoint.
type Destination interface {
	Connect(ctx context.Context) error
	Publish(msg amqp.Publishing) error
	Close() error
}

// Shovel consumes from a source and publishes to a destination,
// enabling cross-broker message forwarding.
type Shovel struct {
	Name        string
	Source      Source
	Destination Destination
	AckMode     AckMode
	Prefetch    int

	mu     sync.Mutex
	status string
	cancel context.CancelFunc
	done   chan struct{}
}

// NewShovel creates a shovel with the given configuration.
func NewShovel(name string, src Source, dst Destination, ackMode AckMode, prefetch int) *Shovel {
	if prefetch <= 0 {
		prefetch = DefaultPrefetch
	}
	if ackMode == "" {
		ackMode = DefaultAckMode
	}
	return &Shovel{
		Name:        name,
		Source:      src,
		Destination: dst,
		AckMode:     ackMode,
		Prefetch:    prefetch,
		status:      StatusStopped,
	}
}

// Start begins the shovel loop in a background goroutine. The shovel
// runs until Stop is called or the parent context is cancelled.
func (s *Shovel) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		return fmt.Errorf("shovel %q: already running", s.Name)
	}

	sCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.status = "starting"

	go s.run(sCtx)

	return nil
}

// Stop signals the shovel to stop and waits for it to finish.
func (s *Shovel) Stop() error {
	s.mu.Lock()
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()

	if cancel == nil {
		return nil
	}

	cancel()

	if done != nil {
		<-done
	}

	s.mu.Lock()
	s.cancel = nil
	s.status = StatusStopped
	s.mu.Unlock()

	return nil
}

// Status returns the current shovel status.
func (s *Shovel) Status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// run is the main shovel loop with reconnect logic.
func (s *Shovel) run(ctx context.Context) {
	defer close(s.done)
	defer s.cleanup()
	defer s.setStatus(StatusStopped)

	delay := DefaultReconnectDelay
	for {
		if ctx.Err() != nil {
			return
		}

		err := s.forward(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}

		s.setStatus(StatusError)

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		delay *= backoffMultiplier
		if delay > maxReconnectDelay {
			delay = maxReconnectDelay
		}
	}
}

// forward connects to source and destination, then forwards messages
// until an error occurs or the context is cancelled.
func (s *Shovel) forward(ctx context.Context) error {
	if err := s.Source.Connect(ctx); err != nil {
		return fmt.Errorf("connect source: %w", err)
	}

	if err := s.Destination.Connect(ctx); err != nil {
		return fmt.Errorf("connect destination: %w", err)
	}

	deliveries, err := s.Source.Consume()
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	s.setStatus(StatusRunning)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}

			pub := amqp.Publishing{
				Headers:         msg.Headers,
				ContentType:     msg.ContentType,
				ContentEncoding: msg.ContentEncoding,
				DeliveryMode:    msg.DeliveryMode,
				Priority:        msg.Priority,
				CorrelationId:   msg.CorrelationId,
				ReplyTo:         msg.ReplyTo,
				Expiration:      msg.Expiration,
				MessageId:       msg.MessageId,
				Timestamp:       msg.Timestamp,
				Type:            msg.Type,
				UserId:          msg.UserId,
				AppId:           msg.AppId,
				Body:            msg.Body,
			}

			if pubErr := s.Destination.Publish(pub); pubErr != nil {
				return fmt.Errorf("publish: %w", pubErr)
			}

			if s.AckMode == AckOnPublish || s.AckMode == AckOnConfirm {
				if ackErr := s.Source.Ack(msg.DeliveryTag); ackErr != nil {
					return fmt.Errorf("ack: %w", ackErr)
				}
			}
		}
	}
}

func (s *Shovel) setStatus(status string) {
	s.mu.Lock()
	s.status = status
	s.mu.Unlock()
}

func (s *Shovel) cleanup() {
	_ = s.Source.Close()      //nolint:errcheck // best-effort cleanup
	_ = s.Destination.Close() //nolint:errcheck // best-effort cleanup
}
