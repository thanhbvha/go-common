# Foundation Examples

This directory contains five runnable examples for the supervised Foundation
API. All examples handle `SIGINT` and `SIGTERM`; press `Ctrl+C` to observe the
graceful shutdown sequence.

| Demo | Purpose | Address |
| --- | --- | --- |
| `api` | Minimal net/http API with lifecycle health | `:8080` |
| `fiber` | Fiber API using the dedicated supervised adapter | `:8080` |
| `worker` | Context-aware supervised background worker | none |
| `full` | Logger, optional telemetry, dependency health, and HTTP | `:8080` |
| `lifecycle` | Dependency graph, worker, HTTP readiness, and failure diagnostics | `127.0.0.1:8080` |

## Integration Rules

These examples use Foundation as the only lifecycle owner. They intentionally
do not import `utils/graceful`.

For MCP, AI-assisted integration, or manual project bootstrapping:

1. Create one `foundation.App`.
2. Declare `DependsOn` for resources, workers, and servers. Registration may be
   in any order; without dependencies, registration order is preserved.
3. Let Foundation adapters own listeners and server shutdown.
4. Call `app.Run(ctx)` on the main goroutine.
5. Let Foundation handle signals and shutdown; keep blocking server work in
   adapter components and background work in `foundation.Worker`.

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
from the worker is treated as an application failure. After cancellation,
returning nil or only the context's cancellation error is normal. Flush errors
and panics during draining are included in the result from `Run` and `Shutdown`.

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

## Dependency Lifecycle And Timeout Diagnostics

[`lifecycle.go`](lifecycle.go) is a complete service using an in-memory database,
a supervised worker, and a net/http server. No external services or credentials
are required. The server binds to loopback by default.

Run all commands from the repository root:

```bash
go run ./examples/foundation -demo=lifecycle
```

In another terminal:

```bash
curl -i http://127.0.0.1:8080/ready
curl -i http://127.0.0.1:8080/live
curl -i http://127.0.0.1:8080/
```

`/` returns a worker counter. `/ready` combines lifecycle readiness with database
readiness. `/live` is the liveness probe. Stop with Ctrl+C, or run a finite demo:

```bash
go run ./examples/foundation -demo=lifecycle -addr=127.0.0.1:8081 -stop-after=3s
```

Components are deliberately registered as `http`, `worker`, `database`:

```text
http DependsOn worker; worker DependsOn database
startup:  database -> worker -> http
shutdown: http.Stop + runner -> worker.Stop + runner -> database.Stop
```

Initialization is sequential. Runners start concurrently after initialization;
`DependsOn` does not guarantee a runner has reached application-specific readiness.
Shutdown cancels all runners first, then executes Stop and waits for each runner
in reverse startup order. The test in `lifecycle_test.go` also demonstrates calling
`app.Shutdown(ctx)` and observing the same terminal result from `Run`.

Try each failure independently (each command intentionally exits nonzero):

```bash
go run ./examples/foundation -demo=lifecycle -scenario=startup-timeout
go run ./examples/foundation -demo=lifecycle -scenario=stop-timeout -stop-after=2s
go run ./examples/foundation -demo=lifecycle -scenario=runner-timeout -stop-after=2s
go run ./examples/foundation -demo=lifecycle -scenario=runtime-failure
go run ./examples/foundation -demo=lifecycle -scenario=missing
go run ./examples/foundation -demo=lifecycle -scenario=cycle
```

| Scenario | Expected behavior |
| --- | --- |
| `normal` | Worker processes until cancellation; HTTP drains, worker finishes, database closes |
| `startup-timeout` | Worker initialization exceeds the 1s startup budget; database rolls back; HTTP never starts |
| `stop-timeout` | Worker Stop exhausts the 1s shutdown budget; database remains open |
| `runner-timeout` | Worker deliberately takes 1.5s to drain; runner wait exceeds the 1s budget; database remains open |
| `runtime-failure` | Worker returns an error; Foundation shuts down and retains the failure |
| `missing` | Graph validation rejects an unregistered dependency before any callback |
| `cycle` | Graph validation rejects database -> http -> worker -> database before any callback |

`main.go` uses `errors.As` to print the timeout component, phase, operation,
elapsed time, configured budget, and pending components. The original deadline
still matches `errors.Is(err, context.DeadlineExceeded)`. For shutdown timeouts,
pending is normally `[worker database]`; completed HTTP shutdown is excluded.
Go cannot forcibly stop a callback. The delayed runner is intentionally bounded,
but the example process exits after the terminal result, without waiting for it.
This is a diagnostic exercise; production callbacks should honor cancellation.

`-scenario`, `-addr`, and `-stop-after` apply only to `-demo=lifecycle`.
The automatic cancellation timer starts immediately before `Run`; leave it off
for the startup-timeout exercise so it does not cancel startup first.

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

For HTTPS with net/http, Gin, or Echo, set `server.TLSConfig` with certificates
or a certificate callback before registering the component. A non-nil TLS
configuration enables HTTPS; an empty configuration fails startup. See the
[HTTPS configuration](../../foundation/README.md#https) for certificate loading
and callback support.

See the [Foundation README](../../foundation/README.md) for the lifecycle
contract, error handling, component helpers, and configuration.
