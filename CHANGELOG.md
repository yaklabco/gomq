# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Performance benchmarks in README (32-core AMD EPYC-Milan: 30.9K msg/s 1P/1C, 85.6K msg/s 4P/4C)

### Fixed

- Publisher disconnects under sustained high-throughput load due to blocking inbox send stalling heartbeat processing
- Flaky consumer priority test skipped in CI (goroutine scheduling is non-deterministic on shared runners)

## [0.2.0] - 2026-03-30

### Added

#### AMQP Protocol Hardening

- Nack/reject with requeue support (messages redelivered with Redelivered flag)
- Dead letter exchange support (`x-dead-letter-exchange`, `x-dead-letter-routing-key`)
- `x-death` header on dead-lettered messages with queue, reason, and routing keys
- Queue-level message TTL enforcement (`x-message-ttl`)
- Per-message TTL via AMQP `expiration` property
- Queue expiry timer (`x-expires`) marks unused queues for deletion
- Consumer priority via `x-priority` argument on `basic.consume`
- Single active consumer mode via `x-single-active-consumer` queue argument
- `basic.return` for mandatory messages with no route (reply code 312)
- `connection.blocked`/`connection.unblocked` flow control with disk space monitoring
- Delivery limits via `x-delivery-limit` queue argument with dead-letter on exceed
- Headers and expiration persistence in message store binary format

#### Advanced Queue Types

- Priority queue support via `x-max-priority` queue argument (1-255 levels, per-priority stores, highest-first delivery)
- Stream queue type via `x-queue-type=stream` (append-only log, per-consumer offsets, supports first/last/next/numeric offset specifiers)
- Consistent-hash exchange type (`x-consistent-hash`) with CRC32 hash ring and weighted virtual nodes

#### HTTP Management API

- RabbitMQ-compatible REST API on port 15672 with HTTP Basic Auth
- Endpoints for overview, connections, channels, exchanges, queues, bindings, users, vhosts, permissions, and definitions
- Prometheus metrics at `/metrics` (queue depths, message rates, connection counts)
- Health checks at `/api/healthchecks/node` and `/api/aliveness-test/{vhost}`
- Definition export/import via `GET/POST /api/definitions`
- Policy management via `/api/policies/{vhost}/{name}`
- Vhost limits via `/api/vhost-limits/{vhost}`
- Shovel management via `/api/shovels/{vhost}/{name}`
- Federation link management via `/api/federation-links/{vhost}/{name}`

#### TLS, Security & Configuration

- TLS/AMQPS support with `--tls-cert`, `--tls-key`, `--amqps-port` flags
- Policy system with regex pattern matching, priority-based resolution, and automatic reapplication
- Per-vhost connection and queue limits (`max-connections`, `max-queues`)
- INI config file support (`--config` flag) with `[main]`, `[amqp]`, `[mgmt]`, `[mqtt]`, `[clustering]` sections

#### MQTT 3.1.1

- Full MQTT 3.1.1 packet codec (all 14 control packet types)
- MQTT broker bridging to AMQP via `amq.topic` exchange with topic conversion (`/` → `.`, `+` → `*`, `#` → `#`)
- QoS 0 (at-most-once), QoS 1 (at-least-once), QoS 2 (exactly-once) delivery
- Retained messages, Last Will and Testament, clean/persistent sessions
- Per-client AMQP queues (`mqtt.<clientID>`), wildcard subscriptions, keep-alive
- Cross-protocol: AMQP publish delivered to MQTT subscribers and vice versa

#### Clustering

- Leader-follower replication with file-level sync over TCP
- CRC32 file checksums with directory scanning and diff computation
- Replication protocol (`GOMQR` header) with password authentication and action streaming
- Full sync via `FileIndex.Diff` for detecting missing or changed files

#### Shovels & Federation

- Shovel engine for cross-broker message forwarding (AMQP source, AMQP + HTTP destinations)
- Three ack modes: `on-confirm`, `on-publish`, `no-ack` with exponential backoff reconnect
- Federation links as specialized shovels with `x-federation-hops` hop tracking and loop prevention
- Shovel store with JSON persistence surviving broker restarts

#### Performance

- Zero-copy consumer delivery from mmap to socket via `ShiftFunc`/`ReadFunc`
- Batched network writes (method + header + body in single flush)
- Buffer pooling for message serialization, publish body accumulation, and frame encoding
- Event-driven consumer prefetch signaling (replaced 5ms polling)
- Method object caching in AMQP frame reader
- TCP_CORK for write batching on Linux
- 64KB buffered I/O for reader and writer

#### Testing & CI

- Correctness guarantee tests (ordering, confirms, durability, ack, requeue, exclusive, overflow)
- Performance regression gates (throughput and latency thresholds)
- CI pipeline with GitHub Actions (lint, test, build)
- Changelog enforcement on pull requests
- Release automation with goreleaser and Docker

### Fixed

- Queue lock deadlock: delivery callback no longer holds `q.mu` during socket writes
- Crash recovery: `loadStats` resizes segments to last valid position after scanning
- Frame size validation prevents DoS via oversized frame headers
- `Len()` includes requeued and inbox-buffered messages
- AMQP wire type mismatch: `x-max-length` and other integer arguments now handled correctly from all client libraries

## [0.1.0] - 2026-03-29

### Added

- AMQP 0-9-1 broker with TCP server, connection, channel, consumer lifecycle
- Vhost with exchange/queue management and routing (direct, fanout, topic, headers)
- Queue with message store, configurable limits, and consumer signaling
- Memory-mapped segment storage engine with crash recovery
- AMQP 0-9-1 frame codec with all method types
- User authentication with SHA256 passwords and per-vhost permissions
- Publisher confirms and transaction support
- Embeddable Go library API with functional options
- Standalone `gomq` binary with signal handling
- `gomqperf` performance testing tool

[Unreleased]: https://github.com/jamesainslie/gomq/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/jamesainslie/gomq/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesainslie/gomq/releases/tag/v0.1.0
