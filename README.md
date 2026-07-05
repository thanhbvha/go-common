# go-common

A collection of production-ready, framework-agnostic Go packages for building backend services.

[![Go Reference](https://pkg.go.dev/badge/github.com/thanhbvha/go-common.svg)](https://pkg.go.dev/github.com/thanhbvha/go-common)

## Packages

| Package | Description |
|---|---|
| `logger` | Async structured logger (log/slog) with optional lumberjack file rotation |
| `config` | Environment variable and configuration file loader (Viper wrapper) |
| `xerrors` | Structured error handling with HTTP status codes and string codes |
| `redis` | Redis client wrapper (single / cluster / sentinel) with health-check |
| `nats` | NATS client wrapper — Core Pub/Sub, JetStream streams/consumers, KV Store |
| `queue` | Durable Redis Streams job queue — delays, retries, DLQ, reclaim |
| `queue_nats` | NATS JetStream job queue — feature parity with Redis queue |
| `websocket` | Clustered, framework-agnostic real-time WebSocket server (Fiber / Gin / Echo) |
| `telemetry` | OpenTelemetry integration for distributed tracing and metrics via OTLP |
| `db` | Database abstraction (GORM & MongoDB) with auto-instrumented telemetry |
| `web` | Web core utilities (standard response, validator, Fiber middlewares) |
| `utils` | Go Generics toolset (slice, maps, str) and Graceful Shutdown manager |

## Requirements

- Go 1.22+
- Redis 6.x+ (Redis 6.2+ recommended for `XAUTOCLAIM` support)
- NATS Server 2.10+ with JetStream enabled (`-js` flag or `jetstream: enabled` in config)

---

## Installation

```bash
go get github.com/thanhbvha/go-common
```

---

## Documentation

Each module contains its own detailed `README.md` with usage examples and API references. Please navigate to the respective package directory to learn more:

- [`logger`](./logger/README.md)
- [`config`](./config/README.md)
- [`xerrors`](./xerrors/README.md)
- [`redis`](./redis/README.md)
- [`nats`](./nats/README.md)
- [`queue`](./queue/README.md)
- [`queue_nats`](./queue_nats/README.md)
- [`websocket`](./websocket/README.md)
- [`telemetry`](./telemetry/README.md)
- [`db`](./db/README.md)
- [`web`](./web/README.md)
- [`utils`](./utils/README.md)

---

## License

MIT
