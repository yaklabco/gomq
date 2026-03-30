# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Leader-follower clustering with file-level replication over TCP
- `cluster` package: `FileIndex` (CRC32 checksums), `Action` encode/decode, `ReplicationServer`, `Follower`, `Election`
- Replication protocol with `GOMQR` header, password authentication, and action streaming
- Single-leader mode: leader broadcasts append/replace/delete actions to connected followers
- Full sync support via `FileIndex.Diff` for detecting missing or changed files
- `[clustering]` INI config section with `enabled`, `bind`, `port`, `password`, and `leader_uri` keys
- `--cluster`, `--cluster-bind`, `--cluster-port`, `--cluster-password`, `--cluster-leader` CLI flags
- `WithCluster`, `WithClusterBind`, `WithClusterPort`, `WithClusterPassword`, `WithClusterLeaderURI` embeddable API options
- MQTT 3.1.1 protocol support with full packet codec (all 14 control packet types)
- MQTT broker bridging to AMQP via `amq.topic` exchange with topic conversion (`/` to `.`, `+` to `*`)
- MQTT QoS 0, QoS 1 (PUBACK), and QoS 2 (PUBREC/PUBREL/PUBCOMP) message delivery
- MQTT retained messages with delivery to new subscribers
- MQTT Last Will and Testament (LWT) on ungraceful disconnect
- MQTT clean and persistent sessions with per-client AMQP queues (`mqtt.<clientID>`)
- MQTT wildcard subscription matching (`+` single-level, `#` multi-level)
- MQTT keep-alive with PINGREQ/PINGRESP
- MQTT authentication via the AMQP user store (username/password)
- `WithMQTTPort` and `WithMQTTBind` options for the embeddable broker API
- `--mqtt-port` and `--mqtt-bind` CLI flags (default 127.0.0.1:1883, -1 to disable)
- `[mqtt]` INI config section with `bind` and `port` keys
- Cross-protocol integration: AMQP publish delivered to MQTT subscribers and vice versa
- TLS/AMQPS support with configurable cert/key and `--tls-cert`, `--tls-key`, `--amqps-port` flags
- `WithTLS` and `WithAMQPSPort` options for the embeddable broker API
- Policy system with regex pattern matching, priority-based resolution, and automatic reapplication
- HTTP API for policies: `GET/PUT/DELETE /api/policies/{vhost}/{name}`
- Per-vhost connection and queue limits (`max-connections`, `max-queues`)
- HTTP API for vhost limits: `GET/PUT/DELETE /api/vhost-limits/{vhost}`
- INI config file support (`--config` flag) with LavinMQ-compatible `[main]`, `[amqp]`, `[mgmt]` sections
- Priority queue support via `x-max-priority` queue argument (1-255 levels, per-priority stores, highest-first delivery)
- Stream queue type via `x-queue-type=stream` (append-only log, per-consumer offsets, ack is no-op, supports first/last/next/numeric offset specifiers)
- Consistent-hash exchange type (`x-consistent-hash`) with CRC32 hash ring and weighted virtual nodes
- Message priority field (`Priority uint8`) in storage binary format and AMQP property mapping
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
