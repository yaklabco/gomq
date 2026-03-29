# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- CI pipeline with GitHub Actions (lint, test, build)
- Changelog enforcement on pull requests
- Release automation with goreleaser
- Docker image published to ghcr.io
- Cross-platform binaries (linux/darwin × amd64/arm64)

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
