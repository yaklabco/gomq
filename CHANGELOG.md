# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- HTTP management API (`/api/`) with RabbitMQ-compatible endpoints
- Endpoints for overview, connections, channels, exchanges, queues, bindings, users, vhosts, permissions, and definitions
- Prometheus metrics at `/api/metrics` and `/metrics` (queue depths, message rates, connection counts)
- Health checks at `/api/healthchecks/node` and `/api/aliveness-test/{vhost}`
- Definition export/import via `GET/POST /api/definitions`
- HTTP Basic Auth against the user store on all management endpoints
- `--http-port` and `--http-bind` CLI flags for the management API (default 127.0.0.1:15672)
- `WithHTTPPort` and `WithHTTPBind` options for the embeddable broker API
- Broker accessor methods for connections, vhosts, exchanges, queues, channels, and bindings
- Consumer priority via `x-priority` argument on `basic.consume`
- Single active consumer mode via `x-single-active-consumer` queue argument
- `basic.return` for mandatory messages with no route (reply code 312)
- `connection.blocked`/`connection.unblocked` flow control with disk space monitoring
- Delivery limits via `x-delivery-limit` queue argument with dead-letter on exceed
- CI pipeline with GitHub Actions (lint, test, build)
- Changelog enforcement on pull requests
- Release automation with goreleaser
- Docker image published to ghcr.io
- Cross-platform binaries (linux/darwin × amd64/arm64)
- Nack/reject with requeue support (messages redelivered with Redelivered flag)
- Dead letter exchange support (x-dead-letter-exchange, x-dead-letter-routing-key)
- x-death header on dead-lettered messages with queue, reason, and routing keys
- Queue-level message TTL enforcement (x-message-ttl)
- Per-message TTL via AMQP expiration property
- Queue expiry timer (x-expires) marks unused queues for deletion
- Headers and expiration persistence in message store binary format

## [0.1.0] - 2026-03-29

### Added

- AMQP 0-9-1 broker with TCP server, connection, channel, consumer lifecycle
- Vhost with exchange/queue management and routing (direct, fanout, topic, headers)
- Queue with message store, configurable limits, and consumer signaling
- Embeddable API, integration tests, and standalone binary
- gomqperf performance testing tool
- Hot-path optimizations with correctness guarantees preserved

[Unreleased]: https://github.com/jamesainslie/gomq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jamesainslie/gomq/releases/tag/v0.1.0
