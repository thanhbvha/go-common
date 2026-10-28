# Foundation

`foundation` is a lightweight, framework-agnostic application supervisor. It
starts resources in dependency order, supervises long-running services, exposes
lifecycle readiness, and performs bounded LIFO shutdown.

> **Breaking v1 change:** the former `NewApp`, `OnStart`, `OnShutdown`, and
> parameterless `Run` API was removed intentionally. Follow the migration table
> below when upgrading.

## Why Use It

- Startup failures roll back resources that already started.
- HTTP servers and workers are supervised; runtime failures stop the process
  instead of disappearing in detached goroutines.
- `SIGINT`, `SIGTERM`, parent-context cancellation, and programmatic shutdown
  share one lifecycle path.
- Startup and shutdown have total deadlines, including callbacks that ignore
  context cancellation.
- Panics become component errors with stack traces.
- Readiness is down before startup completes and before draining begins.
- Core code does not initialize config, databases, Redis, or telemetry
  implicitly; dependencies remain explicit and testable.

## Lifecycle Ownership

Foundation is the lifecycle and graceful-shutdown owner for an application that
uses it. It handles `SIGINT`/`SIGTERM` by default, cancels the application
context, drains registered components in reverse dependency order, and returns
startup, runtime, or shutdown failures to `main`.

Do not create a `utils/graceful.Shutdown` beside a Foundation app and do not
register the same server or dependency in both systems. Combining two lifecycle
coordinators can cause duplicate signal handling, double shutdown, lost cleanup
errors, and resources closing in the wrong order.

Use exactly one of these models:

| Application type | Lifecycle owner |
| --- | --- |
| New service with managed resources, servers, or workers | `foundation.App` |
| Legacy/minimal application not using Foundation | `utils/graceful` |
| Foundation embedded in a process that already owns signals | `foundation.App` with `WithoutSignals()` |

Foundation does not import or delegate to `utils/graceful`; its lifecycle is
implemented directly because it also supervises startup rollback, runtime
failure, readiness, panic recovery, and dependency-safe teardown.

### Rules For MCP And Code Generators

When generating a Foundation bootstrap:

1. Create one `foundation.App` and check the error from `foundation.New`.
2. Register dependencies first and servers last so LIFO shutdown drains traffic
   before closing dependencies.
3. Put blocking work in `Component.Run` or `foundation.Worker`; never launch an
   unmanaged goroutine from `Component.Start`.
4. Use a framework adapter for net/http, Fiber, or gRPC instead of calling
   `Listen`, `Serve`, or `GracefulStop` directly from `main`.
5. Return `app.Run(ctx)` from the bootstrap and propagate its error to the
   process owner.
6. Never generate `graceful.NewShutdown`, `graceful.Register`, or
   `graceful.Wait` in the same application.

## Lifecycle

```text
Created -> Starting -> Running -> Stopping -> Stopped
              |           |          |
              +-----------+----------+-----> Failed

Start: registration order, sequential
Run:   concurrent and supervised
Stop:  reverse registration order, sequential
```

`Start`, `Run`, and `Stop` are optional, but each component must provide at
least one callback. Components may only be registered before `Run` begins, and
an `App` may only run once.

## Quick Start

```go
app, err := foundation.New(foundation.Config{
    Name:            "orders-api",
    Version:         "v1.0.0",
    StartupTimeout:  30 * time.Second,
    ShutdownTimeout: 30 * time.Second,
})
if err != nil {
    return err
}

mux := http.NewServeMux()
mux.Handle("GET /ready", healthhttp.ReadyHandler(app.Health()))
mux.HandleFunc("GET /orders", listOrders)

server := &http.Server{
    Addr:              ":8080",
    Handler:           mux,
    ReadHeaderTimeout: 5 * time.Second,
}
if err := app.Register(foundationhttp.Component("http", server)); err != nil {
    return err
}

return app.Run(context.Background())
```

The net/http adapter also supports Gin and Echo because both engines implement
`http.Handler`:

```go
server := &http.Server{Addr: ":8080", Handler: ginEngine}
// or: Handler: echoApp
app.Register(foundationhttp.Component("http", server))
```

Fiber and gRPC use dedicated adapters:

```go
fiberApp := fiber.New(fiber.Config{ReadTimeout: 10 * time.Second})
app.Register(foundationfiber.Component("fiber", ":8080", fiberApp))
app.Register(foundationgrpc.Component("grpc", ":50051", grpcServer))
```

Every server adapter binds its listener during `Start`, so an invalid or busy
address fails startup before readiness becomes healthy. Rollback also closes
the owned listener even when `Run` never begins.

The net/http adapter rejects a nil handler and requires an effective
`ReadHeaderTimeout` or `ReadTimeout`. The Fiber adapter requires a positive
`ReadTimeout`. If an upstream proxy enforces an equivalent limit, applications
may opt out explicitly with the adapter's `AllowUnsafeTimeouts()` option.

## Components

### Resource

Use a resource for a dependency with initialization and cleanup:

```go
app.Register(foundation.Resource(
    "redis",
    func(ctx context.Context) error { return redisClient.Connect(ctx) },
    func(context.Context) error { return redisClient.Close() },
))
```

If a later component fails startup, Redis is closed automatically during
rollback.

### Worker

`Worker` functions should block until their context is canceled. Returning
while the app is still running—whether with an error or `nil`—is treated as a
runtime failure.

```go
app.Register(foundation.Worker("consumer", func(ctx context.Context) error {
    return consumer.Consume(ctx)
}))
```

### Generic component

Use `Component` when one unit needs all three phases:

```go
app.Register(foundation.Component{
    Name:  "service",
    Start: service.Connect,
    Run:   service.Serve,
    Stop:  service.Shutdown,
})
```

`Closer(name, io.Closer)` is available for shutdown-only resources.

## Health And Readiness

`App.Health()` returns a normal `health.Checker` and automatically includes the
reserved readiness check `foundation.lifecycle`:

- `created`, `starting`, `stopping`, `stopped`, `failed`: down
- `running`: OK

Dependency checks can be added normally:

```go
app.Health().AddReadinessCheck("postgres", health.GORMChecker(db))
app.Health().AddReadinessCheck("redis", health.RedisChecker(redisClient))
```

Do not register another check named `foundation.lifecycle`.

## Logging And Options

Foundation uses `log/slog.Default()` unless a compatible logger is supplied:

```go
app, err := foundation.New(cfg,
    foundation.WithLogger(serviceLogger),
    foundation.WithHealthChecker(checker),
)
```

`WithSignals` replaces the default `SIGINT`/`SIGTERM` set.
`WithoutSignals` is intended for tests and applications embedded in another
process; cancellation and `Shutdown` remain available.

Application names, versions, and component names are limited to 128 bytes and
must not contain control characters, preventing forged multiline log entries.

## Errors And Programmatic Shutdown

Errors support `errors.Is` and `errors.As`. `ComponentError` reports the
component name and phase (`start`, `run`, or `stop`); `PanicError` retains the
recovered value and stack.

```go
var componentErr *foundation.ComponentError
if errors.As(err, &componentErr) {
    logger.Error("component failed",
        "component", componentErr.Component,
        "phase", componentErr.Phase,
        "error", componentErr.Err,
    )
}
```

`Shutdown(ctx)` is safe to call concurrently. All callers wait for the same
terminal result, bounded by their own waiting contexts.

Foundation logs raw component errors by design. Do not include credentials,
tokens, connection strings, or sensitive payloads in returned errors. Apply
redaction before returning an error when its contents are not safe for logs.

## Security And Timeout Boundaries

- Lifecycle callbacks are trusted in-process code. Go cannot forcibly terminate
  a goroutine that ignores context cancellation.
- If a timed-out `Start` later succeeds, Foundation performs best-effort cleanup
  with a fresh shutdown context.
- If a dependent component does not stop before the global shutdown deadline,
  Foundation does not close earlier dependencies underneath it. `Run` returns a
  fatal error so the process owner can terminate the process safely.
- Configure TLS either on the server or at a trusted reverse proxy. The adapters
  do not add encryption automatically.
- Configure gRPC transport credentials and send/receive message size limits when
  constructing `grpc.Server`.
- `http.Server.Shutdown` does not close hijacked connections such as WebSockets.
  Register the WebSocket manager as its own Foundation component so it can drain
  connections explicitly.
- Health error details remain disabled by default. Only enable them for a
  protected diagnostic endpoint.

## Migration From The Removed API

| Removed API | Replacement |
| --- | --- |
| `foundation.NewApp(cfg)` | `foundation.New(cfg)` and handle the returned error |
| `app.OnStart(fn)` | `app.Register(foundation.Resource(...))` or `Component.Start` |
| Manually starting a goroutine in `OnStart` | `Component.Run` or `foundation.Worker` |
| `app.OnShutdown(fn)` | `Component.Stop`, paired with the owning component |
| `app.Run()` | `app.Run(context.Background())` |
| Detached server goroutine | A framework adapter under `foundation/adapter/...` |

### Migration From `utils/graceful`

| Standalone graceful code | Foundation replacement |
| --- | --- |
| `graceful.NewShutdown(timeout)` | `foundation.New(Config{ShutdownTimeout: timeout})` |
| `shutdown.Register(startedResource.Close)` | `foundation.Resource(name, start, stop)` |
| Registering `server.Shutdown` | Register the matching `foundation/adapter` component |
| `shutdown.Wait()` | `app.Run(context.Background())` |
| `shutdown.Execute()` | Cancel the parent context or call `app.Shutdown(ctx)` while `Run` is active |
| Package-level `graceful.Register/Wait` | No global equivalent; register components on one explicit `App` |

After migration, remove every `utils/graceful` import from that service. Do not
keep it as a second fallback shutdown path.

Shutdown ownership now stays beside startup/runtime ownership. Do not recreate
the old detached-goroutine pattern inside `Start`, because Foundation cannot
supervise errors from work it does not own.

Runnable net/http API, Fiber API, worker, and full-stack examples are in
[`examples/foundation`](../examples/foundation).
