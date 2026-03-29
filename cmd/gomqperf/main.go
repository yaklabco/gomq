// Command gomqperf benchmarks GoMQ by running publishers and consumers
// against a broker. Inspired by LavinMQ's lavinmqperf and RabbitMQ's
// perf-test.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jamesainslie/gomq"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultMsgSize      = 16
	defaultCount        = 100_000
	defaultPrefetch     = 100
	latencySampleRate   = 100
	minMsgSize          = 8 // minimum for embedded timestamp
	publishPollInterval = 10 * time.Millisecond
	drainTimeout        = 10 * time.Second
	drainPollInterval   = 50 * time.Millisecond
)

type config struct {
	uri        string
	publishers int
	consumers  int
	queue      string
	exchange   string
	routingKey string
	size       int
	count      int
	confirm    bool
	autoAck    bool
	prefetch   int
	interval   int
	embedded   bool
	dataDir    string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.uri, "uri", "amqp://guest:guest@127.0.0.1:5672/", "AMQP URI")
	flag.IntVar(&cfg.publishers, "publishers", 1, "Number of publisher goroutines")
	flag.IntVar(&cfg.consumers, "consumers", 1, "Number of consumer goroutines")
	flag.StringVar(&cfg.queue, "queue", "perf-test", "Queue name")
	flag.StringVar(&cfg.exchange, "exchange", "", "Exchange name")
	flag.StringVar(&cfg.routingKey, "routing-key", "perf-test", "Routing key")
	flag.IntVar(&cfg.size, "size", defaultMsgSize, "Message body size in bytes")
	flag.IntVar(&cfg.count, "count", defaultCount, "Total messages to publish (0=unlimited)")
	flag.BoolVar(&cfg.confirm, "confirm", false, "Enable publisher confirms")
	flag.BoolVar(&cfg.autoAck, "autoack", false, "Consumer auto-ack")
	flag.IntVar(&cfg.prefetch, "prefetch", defaultPrefetch, "Consumer prefetch count")
	flag.IntVar(&cfg.interval, "interval", 1, "Stats reporting interval in seconds")
	flag.BoolVar(&cfg.embedded, "embedded", false, "Start an embedded GoMQ broker")
	flag.StringVar(&cfg.dataDir, "data-dir", "", "Data directory for embedded broker (default temp dir)")
	flag.Parse()

	if cfg.size < minMsgSize {
		cfg.size = minMsgSize
	}

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	uri := cfg.uri
	if cfg.embedded {
		addr, cleanup, err := startEmbeddedBroker(ctx, cfg.dataDir)
		if err != nil {
			return fmt.Errorf("start embedded broker: %w", err)
		}
		defer cleanup()
		uri = fmt.Sprintf("amqp://guest:guest@%s/", addr)
		fmt.Fprintf(os.Stderr, "embedded broker listening on %s\n", addr)
	}

	if err := declareQueue(uri, cfg.queue); err != nil {
		return err
	}

	stats := NewStatsCollector(latencySampleRate)

	var wg sync.WaitGroup

	// Start consumers first so they're ready to receive.
	for i := range cfg.consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runConsumer(ctx, uri, cfg, stats, i); err != nil {
				fmt.Fprintf(os.Stderr, "consumer %d: %v\n", i, err)
			}
		}()
	}

	// pubCtx is cancelled when all publishers should stop.
	pubCtx, pubCancel := context.WithCancel(ctx)
	defer pubCancel()

	for i := range cfg.publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runPublisher(pubCtx, uri, cfg, stats, i); err != nil && pubCtx.Err() == nil {
				fmt.Fprintf(os.Stderr, "publisher %d: %v\n", i, err)
			}
		}()
	}

	// Stats ticker.
	ticker := time.NewTicker(time.Duration(cfg.interval) * time.Second)
	defer ticker.Stop()

	// Wait for publishing to complete or context cancellation.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if cfg.count > 0 {
			for stats.Published() < int64(cfg.count) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(publishPollInterval):
				}
			}
			pubCancel()
		} else {
			<-ctx.Done()
		}
	}()

	fmt.Fprintln(os.Stderr)

loop:
	for {
		select {
		case <-ticker.C:
			fmt.Fprintln(os.Stderr, stats.Report())
		case <-done:
			break loop
		case <-ctx.Done():
			break loop
		}
	}

	// Wait for consumers to drain with a deadline.
	if cfg.count > 0 {
		target := stats.Published()
		deadline := time.After(drainTimeout)
		for stats.Consumed() < target {
			select {
			case <-deadline:
				fmt.Fprintf(os.Stderr, "timed out waiting for consumers (got %d/%d)\n",
					stats.Consumed(), target)
				goto summary
			case <-time.After(drainPollInterval):
			}
		}
	}

summary:
	// Print final stats interval and summary.
	fmt.Fprintln(os.Stderr, stats.Report())
	fmt.Fprintln(os.Stderr)
	fmt.Fprint(os.Stderr, stats.Summary(cfg.size, cfg.publishers, cfg.consumers))

	stop() // cancel context so consumers exit
	wg.Wait()
	return nil
}

// declareQueue opens a temporary connection and declares the test queue.
func declareQueue(uri, queue string) error {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open setup channel: %w", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(queue, false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	return nil
}

func startEmbeddedBroker(ctx context.Context, dataDir string) (string, func(), error) {
	var err error
	if dataDir == "" {
		dataDir, err = os.MkdirTemp("", "gomqperf-*")
		if err != nil {
			return "", nil, fmt.Errorf("create temp dir: %w", err)
		}
	}

	brk, err := gomq.New(
		gomq.WithDataDir(dataDir),
		gomq.WithAMQPPort(0),
		gomq.WithBind("127.0.0.1"),
	)
	if err != nil {
		return "", nil, fmt.Errorf("create broker: %w", err)
	}

	brokerCtx, brokerCancel := context.WithCancel(ctx)

	go func() {
		if serveErr := brk.ListenAndServe(brokerCtx); serveErr != nil && brokerCtx.Err() == nil {
			fmt.Fprintf(os.Stderr, "broker: %v\n", serveErr)
		}
	}()

	netAddr := brk.WaitForAddr(ctx)
	if netAddr == nil {
		brokerCancel()
		return "", nil, errors.New("broker did not start")
	}

	cleanup := func() {
		brokerCancel()
		_ = brk.Close() // best-effort shutdown; nothing actionable on error
	}
	return netAddr.String(), cleanup, nil
}

func runPublisher(ctx context.Context, uri string, cfg config, stats *StatsCollector, id int) error {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return fmt.Errorf("publisher %d dial: %w", id, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("publisher %d channel: %w", id, err)
	}
	defer ch.Close()

	var confirms chan amqp.Confirmation
	if cfg.confirm {
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("publisher %d enable confirms: %w", id, err)
		}
		confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	body := make([]byte, cfg.size)

	for {
		if ctx.Err() != nil {
			return nil
		}
		if cfg.count > 0 && stats.Published() >= int64(cfg.count) {
			return nil
		}

		// Embed timestamp in first 8 bytes.
		binary.LittleEndian.PutUint64(body[:minMsgSize], uint64(time.Now().UnixNano()))

		err := ch.PublishWithContext(ctx, cfg.exchange, cfg.routingKey, false, false, amqp.Publishing{
			Body: body,
		})
		if err != nil {
			return fmt.Errorf("publisher %d publish: %w", id, err)
		}

		if cfg.confirm {
			select {
			case <-ctx.Done():
				return nil
			case c := <-confirms:
				if !c.Ack {
					return fmt.Errorf("publisher %d: message nacked", id)
				}
			}
		}

		stats.RecordPublish()
	}
}

func runConsumer(ctx context.Context, uri string, cfg config, stats *StatsCollector, id int) error {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return fmt.Errorf("consumer %d dial: %w", id, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("consumer %d channel: %w", id, err)
	}
	defer ch.Close()

	if err := ch.Qos(cfg.prefetch, 0, false); err != nil {
		return fmt.Errorf("consumer %d set qos: %w", id, err)
	}

	msgs, err := ch.Consume(cfg.queue, fmt.Sprintf("gomqperf-%d", id), cfg.autoAck, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consumer %d consume: %w", id, err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			var latencyNs int64
			if len(msg.Body) >= minMsgSize {
				sent := int64(binary.LittleEndian.Uint64(msg.Body[:minMsgSize]))
				latencyNs = time.Now().UnixNano() - sent
			}
			if !cfg.autoAck {
				if err := msg.Ack(false); err != nil {
					return fmt.Errorf("consumer %d ack: %w", id, err)
				}
			}
			stats.RecordConsume(latencyNs)
		}
	}
}
