# Foundation Examples

This directory contains four runnable examples for the supervised Foundation
API. All examples handle `SIGINT` and `SIGTERM`; press `Ctrl+C` to observe the
graceful shutdown sequence.

| Demo | Purpose | Address |
| --- | --- | --- |
| `api` | Minimal net/http API with lifecycle health | `:8080` |
| `fiber` | Fiber API using the dedicated supervised adapter | `:8080` |
| `worker` | Context-aware supervised background worker | none |
| `full` | Logger, optional telemetry, dependency health, and HTTP | `:8080` |

## Integration Rules

These examples use Foundation as the only lifecycle owner. They intentionally
do not import `utils/graceful`.

For MCP, AI-assisted integration, or manual project bootstrapping:

1. Create one `foundation.App`.
2. Register resources in dependency order: telemetry/database/cache first,
   workers next, and network servers last.
3. Let Foundation adapters own listeners and server shutdown.
4. Call `app.Run(ctx)` on the main goroutine.
5. Do not add `graceful.NewShutdown`, `graceful.Register`, `graceful.Wait`, a
   second signal channel, or a detached `Listen`/`Serve` goroutine.

Correct:

```go
app, err := foundation.New(foundation.Config{
    Name:            "orders-api",
    ShutdownTimeout: 15 * time.Second,
})
if err != nil {
    return err
}

if err := app.Register(foundation.Resource("database", openDatabase, closeDatabase)); err != nil {
    return err
}
if err := app.Register(foundationfiber.Component("fiber-http", ":8080", fiberApp)); err != nil {
    return err
}
return app.Run(context.Background())
```

Do not combine lifecycle systems:

```go
// Incorrect: Foundation already owns signals and graceful shutdown.
shutdown := graceful.NewShutdown(15 * time.Second)
shutdown.Register(func(ctx context.Context) error { return fiberApp.ShutdownWithContext(ctx) })
go fiberApp.Listen(":8080")
return app.Run(context.Background())
```

When a parent process already owns signals, pass
`foundation.WithoutSignals()` to `foundation.New`, propagate the parent
context to `Run`, and use `app.Shutdown(ctx)` only for programmatic shutdown.

## Minimal API

```bash
go run ./examples/foundation -demo=api
```

In another terminal:

```bash
curl -i http://localhost:8080/ready
curl -i http://localhost:8080/
```

The HTTP adapter binds the port before the app becomes ready. Starting a second
copy returns a startup error and rolls back resources instead of logging the
error from an unmanaged goroutine.

## Fiber API

```bash
go run ./examples/foundation -demo=fiber
```

Try the API and lifecycle probes:

```bash
curl -i http://localhost:8080/
curl -i http://localhost:8080/ready
curl -i http://localhost:8080/live
```

The example configures panic recovery, request IDs, a safe global error handler,
Fiber read/write/idle timeouts, and a 1 MiB body limit. The dedicated adapter
owns the listener, reports bind failures before readiness becomes healthy,
supervises `Listener`, and gracefully shuts Fiber down when the application
context is canceled.

## Worker

```bash
go run ./examples/foundation -demo=worker
```

The heartbeat worker receives the application context. Returning unexpectedly
from the worker is treated as an application failure; cancellation during
normal shutdown is not.

## Full Service

Run locally without an OpenTelemetry collector:

```bash
go run ./examples/foundation -demo=full
```

To export traces and metrics, provide an OTLP gRPC endpoint:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT='localhost:4317'
go run ./examples/foundation -demo=full
```

The full example demonstrates:

- injecting `go-common/logger` through `foundation.WithLogger`;
- initializing and shutting down telemetry as one resource;
- connecting and closing a dependency as another resource;
- adding dependency readiness alongside `foundation.lifecycle`;
- registering the HTTP server last so it drains first during LIFO shutdown.

## Framework Recipes

### Fiber

```go
fiberApp := fiber.New(fiber.Config{ReadTimeout: 10 * time.Second})
fiberApp.Get("/ready", fiberhealth.ReadyHandler(app.Health()))
app.Register(foundationfiber.Component("fiber", ":8080", fiberApp))
```

See [`fiber.go`](fiber.go) for a complete runnable example. Keep
`AllowUnsafeTimeouts()` reserved for deployments where a trusted upstream
enforces an equivalent connection timeout.

### Gin Or Echo

Gin and Echo implement `http.Handler`, so use the net/http adapter and retain
normal `http.Server` timeout configuration:

```go
server := &http.Server{
    Addr:              ":8080",
    Handler:           router, // *gin.Engine or *echo.Echo
    ReadHeaderTimeout: 5 * time.Second,
}
app.Register(foundationhttp.Component("http", server))
```

### gRPC

```go
grpcServer := grpc.NewServer()
app.Register(foundationgrpc.Component("grpc", ":50051", grpcServer))
```

Production gRPC servers should add transport credentials and explicit maximum
send/receive message sizes. For HTTP, keep handler and read-header timeouts
explicit; use `AllowUnsafeTimeouts()` only when a trusted upstream enforces an
equivalent limit.

See the [Foundation README](../../foundation/README.md) for the lifecycle
contract, error handling, component helpers, and migration guide.
