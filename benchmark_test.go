package gomq_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	perfMsgSize     = 16
	perfPrefetch    = 1000
	minTimestampLen = 8
)

// TestPerfGate_1P1C measures single-publisher, single-consumer throughput.
func TestPerfGate_1P1C(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf gate in short mode")
	}
	// Performance tests must not run in parallel -- resource contention
	// produces unreliable measurements.

	const (
		msgCount  = 100_000
		gateRate  = 30_000.0
		queueName = "perf-1p1c"
	)

	brk := startTestBroker(t)
	url := brokerURL(t, brk)

	// Set up queue.
	setupConn := dialURL(t, url)
	setupCh, err := setupConn.Channel()
	if err != nil {
		t.Fatalf("open setup channel: %v", err)
	}
	_, err = setupCh.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	setupConn.Close()

	// Consumer connection.
	conConn := dialURL(t, url)
	defer conConn.Close()
	conCh, err := conConn.Channel()
	if err != nil {
		t.Fatalf("open consumer channel: %v", err)
	}
	if err := conCh.Qos(perfPrefetch, 0, false); err != nil {
		t.Fatalf("set qos: %v", err)
	}
	msgs, err := conCh.Consume(queueName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	// Publisher connection.
	pubConn := dialURL(t, url)
	defer pubConn.Close()
	pubCh, err := pubConn.Channel()
	if err != nil {
		t.Fatalf("open publisher channel: %v", err)
	}

	body := make([]byte, perfMsgSize)

	start := time.Now()

	// Publish all messages.
	ctx := context.Background()
	for i := range msgCount {
		binary.LittleEndian.PutUint64(body[:minTimestampLen], uint64(i))
		if err := pubCh.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
			Body: body,
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Consume all messages.
	consumed := 0
	deadline := time.After(30 * time.Second)
	for consumed < msgCount {
		select {
		case <-msgs:
			consumed++
		case <-deadline:
			t.Fatalf("timed out consuming: got %d/%d", consumed, msgCount)
		}
	}

	elapsed := time.Since(start)
	msgsPerSec := float64(msgCount) / elapsed.Seconds()

	t.Logf("throughput: %.0f msg/s (gate: %.0f msg/s, elapsed: %s)", msgsPerSec, gateRate, elapsed)

	if msgsPerSec < gateRate {
		t.Fatalf("throughput %.0f msg/s below gate %.0f msg/s", msgsPerSec, gateRate)
	}
}

// TestPerfGate_4P4C measures throughput with four publishers and four consumers.
func TestPerfGate_4P4C(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf gate in short mode")
	}
	const (
		msgCount   = 200_000
		publishers = 4
		consumers  = 4
		gateRate   = 30_000.0
		queueName  = "perf-4p4c"
	)

	brk := startTestBroker(t)
	url := brokerURL(t, brk)

	// Set up queue.
	setupConn := dialURL(t, url)
	setupCh, err := setupConn.Channel()
	if err != nil {
		t.Fatalf("open setup channel: %v", err)
	}
	_, err = setupCh.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	setupConn.Close()

	// Start consumers.
	var consumed atomic.Int64
	var consWg sync.WaitGroup
	consCtx, consCancel := context.WithCancel(context.Background())
	defer consCancel()

	for ci := range consumers {
		consWg.Add(1)
		go func(id int) {
			defer consWg.Done()
			conn := dialURL(t, url)
			defer conn.Close()

			ch, chErr := conn.Channel()
			if chErr != nil {
				t.Errorf("consumer %d channel: %v", id, chErr)
				return
			}
			if qErr := ch.Qos(perfPrefetch, 0, false); qErr != nil {
				t.Errorf("consumer %d qos: %v", id, qErr)
				return
			}
			msgsCh, cErr := ch.Consume(queueName, fmt.Sprintf("con-%d", id), true, false, false, false, nil)
			if cErr != nil {
				t.Errorf("consumer %d consume: %v", id, cErr)
				return
			}

			for {
				select {
				case _, ok := <-msgsCh:
					if !ok {
						return
					}
					consumed.Add(1)
				case <-consCtx.Done():
					return
				}
			}
		}(ci)
	}

	start := time.Now()

	// Start publishers.
	perPublisher := msgCount / publishers
	var pubWg sync.WaitGroup
	for pi := range publishers {
		pubWg.Add(1)
		go func(id int) {
			defer pubWg.Done()
			conn := dialURL(t, url)
			defer conn.Close()

			ch, chErr := conn.Channel()
			if chErr != nil {
				t.Errorf("publisher %d channel: %v", id, chErr)
				return
			}

			body := make([]byte, perfMsgSize)
			ctx := context.Background()
			for j := range perPublisher {
				binary.LittleEndian.PutUint64(body[:minTimestampLen], uint64(j))
				if pErr := ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
					Body: body,
				}); pErr != nil {
					t.Errorf("publisher %d publish %d: %v", id, j, pErr)
					return
				}
			}
		}(pi)
	}

	pubWg.Wait()

	// Wait for all messages to be consumed.
	deadline := time.After(30 * time.Second)
	for consumed.Load() < msgCount {
		select {
		case <-deadline:
			t.Fatalf("timed out consuming: got %d/%d", consumed.Load(), msgCount)
		case <-time.After(10 * time.Millisecond):
		}
	}

	elapsed := time.Since(start)
	consCancel()
	consWg.Wait()

	msgsPerSec := float64(msgCount) / elapsed.Seconds()

	t.Logf("throughput: %.0f msg/s (gate: %.0f msg/s, elapsed: %s)", msgsPerSec, gateRate, elapsed)

	if msgsPerSec < gateRate {
		t.Fatalf("throughput %.0f msg/s below gate %.0f msg/s", msgsPerSec, gateRate)
	}
}

// TestPerfGate_Latency measures end-to-end p99 latency.
func TestPerfGate_Latency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf gate in short mode")
	}
	const (
		msgCount  = 10_000
		gateP99   = 200 * time.Millisecond
		queueName = "perf-latency"
	)

	brk := startTestBroker(t)
	url := brokerURL(t, brk)

	// Set up queue.
	setupConn := dialURL(t, url)
	setupCh, err := setupConn.Channel()
	if err != nil {
		t.Fatalf("open setup channel: %v", err)
	}
	_, err = setupCh.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	setupConn.Close()

	// Consumer connection.
	conConn := dialURL(t, url)
	defer conConn.Close()
	conCh, err := conConn.Channel()
	if err != nil {
		t.Fatalf("open consumer channel: %v", err)
	}
	if err := conCh.Qos(perfPrefetch, 0, false); err != nil {
		t.Fatalf("set qos: %v", err)
	}
	msgs, err := conCh.Consume(queueName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	// Publisher connection.
	pubConn := dialURL(t, url)
	defer pubConn.Close()
	pubCh, err := pubConn.Channel()
	if err != nil {
		t.Fatalf("open publisher channel: %v", err)
	}

	latencies := make([]time.Duration, 0, msgCount)
	body := make([]byte, perfMsgSize)

	// Publish and consume, recording latency for each message.
	var consWg sync.WaitGroup
	consWg.Add(1)
	go func() {
		defer consWg.Done()
		deadline := time.After(30 * time.Second)
		for len(latencies) < msgCount {
			select {
			case msg := <-msgs:
				if len(msg.Body) >= minTimestampLen {
					sentNs := int64(binary.LittleEndian.Uint64(msg.Body[:minTimestampLen]))
					latencies = append(latencies, time.Duration(time.Now().UnixNano()-sentNs))
				}
			case <-deadline:
				t.Errorf("timed out consuming: got %d/%d", len(latencies), msgCount)
				return
			}
		}
	}()

	ctx := context.Background()
	for range msgCount {
		binary.LittleEndian.PutUint64(body[:minTimestampLen], uint64(time.Now().UnixNano()))
		if err := pubCh.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
			Body: body,
		}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	consWg.Wait()

	if len(latencies) < msgCount {
		t.Fatalf("collected only %d/%d latency samples", len(latencies), msgCount)
	}

	// Compute p99.
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99Idx := int(math.Ceil(float64(len(latencies))*0.99)) - 1
	if p99Idx < 0 {
		p99Idx = 0
	}
	p99 := latencies[p99Idx]

	median := latencies[len(latencies)/2]
	t.Logf("latency: median=%s p99=%s (gate: p99 < %s)", median, p99, gateP99)

	if p99 > gateP99 {
		t.Fatalf("p99 latency %s exceeds gate %s", p99, gateP99)
	}
}
