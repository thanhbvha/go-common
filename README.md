# go-common

Composable, framework-agnostic Go packages for backend services. The v2 candidate hardens lifecycle, security, data access and delivery contracts; adoption evidence is tracked per module.

Start with the [reference backend](./examples/production), [v2 API guide](./docs/V2.md), and [production validation matrix](./docs/PRODUCTION.md).

[![Go Reference](https://pkg.go.dev/badge/github.com/thanhbvha/go-common/v2.svg)](https://pkg.go.dev/github.com/thanhbvha/go-common/v2)

## Packages

| Package | Description |
|---|---|
| `foundation` | Supervised component lifecycle with startup rollback, runtime failure handling, graceful shutdown, and health state |
| `logger` | Structured slog runtime; synchronous default, optional bounded async logging and file rotation |
| `config` | Environment variable and configuration file loader (Viper wrapper) |
| `xerrors` | Structured error handling with HTTP status codes and string codes |
| `recovery` | Framework-independent structured panic capture with net/http / Fiber / Gin / Echo middleware adapters |
| `redis` | Redis client wrapper (single / cluster / sentinel) with health-check |
| `nats` | NATS client wrapper — Core Pub/Sub, JetStream streams/consumers, KV Store |
| `kafka` | Apache Kafka client wrapper (franz-go) — Producer, Consumer, Admin, TLS/SASL |
| `eventbus` | Transport-neutral, at-least-once task and event delivery over Memory, Redis Streams, NATS JetStream, and Kafka |
| `websocket` | Clustered, framework-agnostic real-time WebSocket server (net/http / Fiber / Gin / Echo) |
| `telemetry` | OpenTelemetry integration for distributed tracing and metrics via OTLP |
| `db` | Database abstraction (GORM & MongoDB) with auto-instrumented telemetry |
| `web` | Web core utilities (standard responsproductione, validator, net/http / Fiber / Gin / Echo middleware adapters) |
| `graphql` | Framework-agnostic GraphQL engine with Generic DataLoader & Telemetry |
| `utils` | Go Generics and cryptography helpers; includes legacy standalone graceful shutdown for applications not using `foundation` |
| `cache` | Independent namespaced Redis/local caches and session-owned Redis leases |
| `auth` | JWT/encrypted JWT policy, key rotation, revocation and atomic refresh sessions |
| `httpclient` | Resilient REST Client with Circuit Breaker (gobreaker) and Auto-Retry |
| `ratelimit` | Distributed Rate Limiter (Fixed Window) using Redis Lua scripting |
| `cron` | Job scheduler with renewable Redis leases, slot deduplication, and graceful shutdown |
| `health` | Kubernetes-ready health checks (`/live`, `/ready`) with concurrent Checkers and HTTP adapters |
| `grpc` | Pre-configured gRPC server with unified Interceptors (Recovery, Logging, Auth) |

## Requirements

- Go 1.26.10+
- Redis 6.2+ APIs; current disposable integration fixtures use Redis 7
- NATS Server 2.10+ with JetStream enabled (`-js` flag or `jetstream: enabled` in config)
- Apache Kafka 3.9.1 is the validated fixture; other broker versions require application validation

---

## Installation

```bash
go get github.com/thanhbvha/go-common/v2
```

---

## Documentation

Each module contains its own detailed `README.md` with usage examples and API references. Please navigate to the respective package directory to learn more:

- [`foundation`](./foundation/README.md) — [runnable lifecycle examples](./examples/foundation/README.md)
- [`logger`](./logger/README.md)
- [`config`](./config/README.md)
- [`xerrors`](./xerrors/README.md)
- [`recovery`](./recovery/README.md)
- [`redis`](./redis/README.md)
- [`nats`](./nats/README.md)
- [`kafka`](./kafka/README.md)
- [`eventbus`](./eventbus/README.md) — [runnable examples](./examples/eventbus/README.md)
- [`websocket`](./websocket/README.md)
- [`telemetry`](./telemetry/README.md)
- [`db`](./db/README.md)
- [`web`](./web/README.md)
- [`graphql`](./graphql/README.md)
- [`utils`](./utils/README.md)
- [`cache`](./cache/README.md)
- [`auth`](./auth/README.md)
- [`httpclient`](./httpclient/README.md)
- [`ratelimit`](./ratelimit/README.md)
- [`cron`](./cron/README.md)
- [`health`](./health/README.md)
- [`grpc`](./grpc/README.md)

## HTTP support matrix

| Capability | Core | `net/http` | Fiber | Gin | Echo |
|---|---|---|---|---|---|
| Authentication | `auth` managers | Middleware | Middleware | Middleware | Middleware |
| Request logging / ID | `logger`, `ctxkey` | Middleware | Middleware | Middleware | Middleware |
| Health probes | `health.Checker` | Handler | Handler | Handler | Handler |
| Rate limiting | `ratelimit.Limiter` | Middleware | Middleware | Middleware | Middleware |
| Responses / recovery / telemetry | `web` primitives | Adapter | Adapter | Adapter | Adapter |
| GraphQL | `http.Handler` | Direct | Adapter | Adapter | Adapter |
| WebSocket | `core.Conn` | Adapter | Adapter | Adapter | Adapter |

## Lifecycle Selection

For new services, use `foundation` as the single owner of startup, OS signals,
runtime supervision, readiness, and graceful shutdown.

- Register dependencies with `foundation.Resource` or `foundation.Component`.
- Register long-running jobs with `foundation.Worker`.
- Register net/http, Fiber, or gRPC servers with the adapters under
  `foundation/adapter`.
- Finish `main` with `app.Run(ctx)`; do not start server goroutines manually.
- Do **not** combine a Foundation app with `utils/graceful`. Two lifecycle
  coordinators can handle the same signal, close a resource twice, or violate
  dependency shutdown order.
- Use `utils/graceful` for small standalone processes that need a focused,
  one-shot shutdown coordinator without Foundation's runtime supervision.
- If another process or host owns OS signals, create Foundation with
  `foundation.WithoutSignals()` and control it through the parent context or
  `app.Shutdown(ctx)`.

Agents and MCP integrations should read the
[`foundation` lifecycle contract](./foundation/README.md) and the
[`foundation` examples](./examples/foundation/README.md) before generating a
service bootstrap.

---

## 🤖 AI / MCP Integration (Cursor, Cline, VSCode)

An optional **Model Context Protocol (MCP)** server allows AI coding assistants to fetch source code, architectural rules, and boilerplate from the root packages and `examples/` directory.

Review generated integrations against the current v2 APIs and runnable examples. Tool access does not replace application validation.

### Setup for Cursor IDE
1. Open Cursor Settings -> Features -> MCP
2. Click **+ Add New MCP Server**
3. Configure as follows:
   - **Type**: `command`
   - **Command**: `npx`
   - **Args**: `-y go-common-mcp-server@latest`

### Setup for VSCode (Cline)
Add this to your `cline_mcp_settings.json`:
```json
{
  "mcpServers": {
    "go-common": {
      "command": "npx",
      "args": ["-y", "go-common-mcp-server@latest"]
    }
  }
}
```

---

## License

This project is free and open-source software licensed under the [MIT License](LICENSE).
