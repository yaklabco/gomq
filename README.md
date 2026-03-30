# GoMQ

A high-performance AMQP 0-9-1 message broker written in Go, inspired by [LavinMQ](https://github.com/cloudamqp/lavinmq). GoMQ is designed to be used as a **standalone broker** or **embedded as a Go library**.

GoMQ implements the same mmap-based storage architecture as LavinMQ, ported from Crystal to idiomatic Go with goroutine-per-consumer delivery, zero-copy reads from memory-mapped segments, and a hand-written AMQP 0-9-1 frame codec.

> **Status:** GoMQ is under active development. The core AMQP protocol, storage engine, and all LavinMQ features are implemented. Performance tuning for production workloads is ongoing.

## Features

### Protocols
- **AMQP 0-9-1** — full protocol support with all standard methods
- **MQTT 3.1.1** — bridged to AMQP via `amq.topic` exchange, QoS 0/1/2, retained messages, LWT
- **AMQPS** — TLS/SSL with configurable cert/key

### Exchange Types
- **direct** — exact routing key match
- **fanout** — broadcast to all bound queues
- **topic** — wildcard matching (`*` one word, `#` zero or more)
- **headers** — match on message headers (`x-match: all` or `any`)
- **x-consistent-hash** — consistent hashing with weighted virtual nodes

### Queue Types
- **Standard** — FIFO with mmap-backed persistence
- **Priority** — `x-max-priority` with per-priority stores, highest-first delivery
- **Stream** — append-only log with per-consumer offset tracking (`x-queue-type=stream`)

### Queue Features
- Dead letter exchange (`x-dead-letter-exchange`, `x-dead-letter-routing-key`)
- Message TTL (`x-message-ttl`) and per-message expiration
- Queue expiry (`x-expires`)
- Max length (`x-max-length`, `x-max-length-bytes`) with `drop-head` or `reject-publish` overflow
- Delivery limits (`x-delivery-limit`) with dead-letter on exceed
- Consumer priority (`x-priority`)
- Single active consumer (`x-single-active-consumer`)
- Exclusive and auto-delete queues

### Broker Features
- Publisher confirms
- `basic.return` for mandatory unroutable messages
- `connection.blocked` / `connection.unblocked` flow control
- Nack/reject with requeue
- Per-consumer prefetch (QoS)
- Transaction support (`tx.select`, `tx.commit`, `tx.rollback`)

### Management & Operations
- **HTTP Management API** — RabbitMQ-compatible REST API on port 15672
- **Prometheus metrics** at `/metrics`
- **Policy system** — regex pattern matching with priority-based resolution
- **Vhost limits** — max connections, max queues
- **Definition export/import** — backup and restore via `/api/definitions`
- **INI config file** — LavinMQ-compatible `[main]`, `[amqp]`, `[mgmt]`, `[mqtt]`, `[clustering]` sections

### Clustering
- Leader-follower replication with file-level sync over TCP
- CRC32 checksums with full sync on follower rejoin

### Shovels & Federation
- Cross-broker message forwarding (AMQP source, AMQP + HTTP destinations)
- Federation links with `x-federation-hops` loop prevention
- Three ack modes: `on-confirm`, `on-publish`, `no-ack`

## Quick Start

### Standalone Binary

```bash
# Install
go install github.com/yaklabco/gomq/cmd/gomq@latest

# Run with defaults (AMQP on :5672, HTTP API on :15672)
gomq --data-dir /tmp/gomq-data

# Run with all options
gomq \
  --data-dir /var/lib/gomq \
  --bind 0.0.0.0 --port 5672 \
  --http-bind 0.0.0.0 --http-port 15672 \
  --mqtt-port 1883 \
  --tls-cert server.crt --tls-key server.key --amqps-port 5671
```

### Docker

```bash
docker run -p 5672:5672 -p 15672:15672 ghcr.io/yaklabco/gomq:latest
```

### Embeddable Library

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"

    "github.com/yaklabco/gomq"
)

func main() {
    brk, err := gomq.New(
        gomq.WithDataDir("/tmp/gomq-data"),
        gomq.WithBind("0.0.0.0"),
        gomq.WithAMQPPort(5672),
        gomq.WithHTTPPort(15672),
        gomq.WithMQTTPort(1883),
    )
    if err != nil {
        log.Fatal(err)
    }

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    if err := brk.ListenAndServe(ctx); err != nil {
        log.Fatal(err)
    }
    brk.Close()
}
```

Any standard AMQP 0-9-1 client library works out of the box:

```go
// Using github.com/rabbitmq/amqp091-go
conn, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
ch, _ := conn.Channel()
ch.QueueDeclare("my-queue", true, false, false, false, nil)
ch.PublishWithContext(ctx, "", "my-queue", false, false, amqp.Publishing{
    Body: []byte("hello gomq"),
})
```

## Architecture

GoMQ faithfully ports LavinMQ's architecture to Go:

```mermaid
graph TB
    subgraph broker["GoMQ Broker"]
        subgraph protocols["Protocol Listeners"]
            amqp["AMQP<br/>:5672"]
            amqps["AMQPS<br/>:5671"]
            mqtt["MQTT<br/>:1883"]
            http["HTTP API<br/>:15672"]
        end

        subgraph vhost["VHost ('/')"]
            exchanges["Exchanges<br/>direct, fanout, topic,<br/>headers, x-consistent-hash"]
            queues["Queues<br/>standard, priority, stream"]
            bindings["Bindings"]
            policies["Policies"]
        end

        subgraph storage["Storage Engine (mmap)"]
            segments["Segment files<br/>msgs.XXXXXXXXXX (8 MiB each)"]
            acks["Ack files<br/>acks.XXXXXXXXXX"]
            gc["Automatic segment<br/>rotation & GC"]
        end

        amqp & amqps & mqtt & http --> vhost
        vhost --> storage
    end
```

### Storage Engine

Messages are persisted to memory-mapped segment files using `syscall.Mmap` with `MAP_SHARED`. Publishing is a `copy()` into the mapped region — no `write()` syscalls on the hot path. On graceful shutdown, files are truncated to their logical size. On crash, segments are recovered by scanning for valid messages.

### Concurrency Model

- **Goroutine per connection** — each AMQP connection runs its own read loop
- **Goroutine per consumer** — delivery loop with prefetch enforcement and priority yielding
- **Channel-based queue inbox** — non-durable queues use an async buffered channel for lock-free publishing
- **Synchronous persistence** — durable queues and publisher confirms write directly under lock

## Configuration

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | | Path to INI config file |
| `--data-dir` | `/var/lib/gomq` | Persistent storage directory |
| `--bind` | `127.0.0.1` | AMQP bind address |
| `--port` | `5672` | AMQP listen port |
| `--http-bind` | `127.0.0.1` | Management API bind address |
| `--http-port` | `15672` | Management API port (-1 to disable) |
| `--mqtt-bind` | `127.0.0.1` | MQTT bind address |
| `--mqtt-port` | `1883` | MQTT listen port (-1 to disable) |
| `--tls-cert` | | TLS certificate file |
| `--tls-key` | | TLS private key file |
| `--amqps-port` | `-1` | AMQPS listen port (-1 to disable) |
| `--cluster` | `false` | Enable clustering |
| `--cluster-bind` | `127.0.0.1` | Cluster replication bind address |
| `--cluster-port` | `5679` | Cluster replication port |
| `--cluster-password` | | Cluster replication password |
| `--cluster-leader` | | Leader address (follower mode) |

### INI Config File

```ini
[main]
data_dir = /var/lib/gomq

[amqp]
bind = 0.0.0.0
port = 5672
heartbeat = 300

[mgmt]
bind = 0.0.0.0
port = 15672

[mqtt]
bind = 0.0.0.0
port = 1883

[clustering]
enabled = true
bind = 0.0.0.0
port = 5679
password = secret
```

## Embeddable API

```go
// Options
gomq.WithDataDir(dir string)
gomq.WithBind(addr string)
gomq.WithAMQPPort(port int)
gomq.WithHTTPPort(port int)        // -1 to disable
gomq.WithHTTPBind(addr string)
gomq.WithMQTTPort(port int)        // -1 to disable
gomq.WithMQTTBind(addr string)
gomq.WithTLS(certFile, keyFile string)
gomq.WithAMQPSPort(port int)
gomq.WithCluster(enabled bool)
gomq.WithClusterBind(addr string)
gomq.WithClusterPort(port int)
gomq.WithClusterPassword(pw string)
gomq.WithClusterLeaderURI(uri string)

// Lifecycle
brk, err := gomq.New(opts...)
brk.ListenAndServe(ctx)            // bind and serve all protocols
brk.Serve(ctx, listener)           // serve on existing listener
brk.Addr() net.Addr                // AMQP listener address
brk.HTTPAddr() net.Addr            // HTTP API address
brk.MQTTAddr() net.Addr            // MQTT listener address
brk.WaitForAddr(ctx) net.Addr      // block until listening
brk.Close()                        // graceful shutdown
```

## Management API

RabbitMQ-compatible HTTP API with Basic Auth (default: `guest`/`guest`):

```bash
# Overview
curl -u guest:guest http://localhost:15672/api/overview

# List queues
curl -u guest:guest http://localhost:15672/api/queues

# Declare exchange
curl -u guest:guest -X PUT http://localhost:15672/api/exchanges/%2F/my-exchange \
  -H 'Content-Type: application/json' \
  -d '{"type":"direct","durable":true}'

# Prometheus metrics
curl http://localhost:15672/metrics

# Health check
curl http://localhost:15672/api/healthchecks/node

# Export definitions
curl -u guest:guest http://localhost:15672/api/definitions > backup.json
```

## Performance

Benchmarked on a **32-core AMD EPYC-Milan** (251 GB RAM, Linux 6.17) using `gomqperf` with 16-byte messages, auto-ack, and prefetch 1000. All tests run over TCP loopback (not embedded).

| Scenario | GoMQ | Notes |
|----------|------|-------|
| 1 publisher, 1 consumer | **30,900 msg/s** | 500K messages, p99 latency 3.1s |
| 4 publishers, 4 consumers | **85,600 msg/s** | 1M messages, p99 latency 718ms |
| 1 publisher, 1 consumer (macOS, embedded) | **47,600 msg/s** | 500K messages, p99 latency 72ms |
| 4 publishers, 4 consumers (macOS, embedded) | **73,600 msg/s** | 1M messages |

For context, RabbitMQ 4.1 on the same hardware achieves ~35,000 msg/s (1P/1C) and ~90,000 msg/s (4P/4C). LavinMQ (Crystal) reports 800,000 msg/s on a 2-vCPU ARM instance — GoMQ's architecture is the same but Go's goroutine scheduler has higher overhead than Crystal's cooperative fibers. Performance optimization is ongoing.

> **Test environment:** AMD EPYC-Milan, 32 cores, 251 GB RAM, Linux 6.17.0, ext4, loopback TCP.
> Benchmarks are end-to-end (publish → route → persist → deliver → consume) with latency measured via embedded nanosecond timestamps.

## Benchmarking

GoMQ includes `gomqperf`, a built-in load testing tool:

```bash
go install github.com/yaklabco/gomq/cmd/gomqperf@latest

# Basic throughput test
gomqperf -uri amqp://guest:guest@localhost:5672/ \
  -publishers 4 -consumers 4 -size 16 -count 1000000 -autoack

# With publisher confirms
gomqperf -uri amqp://guest:guest@localhost:5672/ \
  -publishers 1 -consumers 1 -size 256 -count 100000 -confirm

# Embedded mode (no external broker needed)
gomqperf -embedded -publishers 4 -consumers 4 -count 500000 -autoack
```

## Project Structure

| Package | Purpose | LOC |
|---------|---------|-----|
| `gomq` | Embeddable library API | 230 |
| `broker` | Server, connection, channel, consumer, vhost, exchanges, queue | 7,500+ |
| `amqp` | AMQP 0-9-1 frame codec | 4,900 |
| `storage` | MFile (mmap), MessageStore, SegmentPosition | 2,000 |
| `mqtt` | MQTT 3.1.1 codec and broker | 2,200+ |
| `http` | Management API and Prometheus metrics | 1,500+ |
| `auth` | SHA256 passwords, user store, permissions | 850 |
| `cluster` | Leader-follower replication | 800+ |
| `shovel` | Shovels and federation | 1,200+ |
| `config` | Configuration with defaults | 250 |
| `cmd/gomq` | Standalone binary | 60 |
| `cmd/gomqperf` | Benchmark tool | 500 |

**~31,000 lines of Go**, 574 tests with race detector, CI/CD with GitHub Actions.

## Testing

```bash
# Run all tests
stave test:all

# Run correctness guarantee tests
go test -run TestGuarantee ./...

# Run performance gates
go test -run TestPerfGate ./...

# Run integration tests (starts embedded broker, uses amqp091-go client)
go test -run TestIntegration ./...
```

## Building

```bash
# Prerequisites: Go 1.24+, stave, golangci-lint, gotestsum

# Full pipeline (lint + test + build)
stave

# Just build
stave build

# Cross-compile
GOOS=linux GOARCH=amd64 go build -o gomq ./cmd/gomq/
```

## Releasing

Releases are automated via GitHub Actions:

1. Update `CHANGELOG.md` with changes under `[Unreleased]`
2. Merge to main
3. Trigger the Release workflow (Actions → Release → Run workflow)
4. goreleaser tags, builds cross-platform binaries, and publishes Docker image to `ghcr.io`

## License

MIT

## Acknowledgments

GoMQ is a faithful Go port of [LavinMQ](https://github.com/cloudamqp/lavinmq) by [CloudAMQP](https://www.cloudamqp.com/). The mmap-based storage architecture, segment file format, and many design decisions are directly inspired by their work.
