# Foundation

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

`foundation` supervises application startup, long-running components, readiness,
and graceful shutdown. Dependencies are wired explicitly; the core does not load
configuration or initialize databases, brokers, or telemetry.

## Quick Start

Use `foundation/adapter/nethttp` for net/http, Gin, and Echo servers:

```go
package main

import (
    "context"
    "log"
    "net/http"
    "time"

    "github.com/thanhbvha/go-common/v2/foundation"
    foundationhttp "github.com/thanhbvha/go-common/v2/foundation/adapter/nethttp"
    healthhttp "github.com/thanhbvha/go-common/v2/health/handler/nethttp"
)

func run(ctx context.Context) error {
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
    mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
    })
    server := &http.Server{
        Addr:              ":8080",
        Handler:           mux,
        ReadHeaderTimeout: 5 * time.Second,
    }
    if err := app.Register(foundationhttp.Component("http", server)); err != nil {
        return err
    }
    return app.Run(ctx)
}

func main() {
    if err := run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

Runnable [examples](../examples/foundation) cover net/http, Fiber, a worker, and
a service with logging, telemetry, and dependency health.
The [lifecycle example](../examples/foundation/README.md#dependency-lifecycle-and-timeout-diagnostics)
also covers explicit dependencies, readiness, graceful shutdown, and seven
normal/failure scenarios without external services.

## Lifecycle Contract

```text
Created -> Starting -> Running -> Stopping -> Stopped
              |           |          |
              +-----------+----------+-----> Failed
```

Create an app with `New`, register components with `Register`, and call `Run(ctx)`
once. Registration closes when `Run` begins. Component names must be unique.

| Callback | Execution | Responsibility |
| --- | --- | --- |
| `Start(ctx)` | Sequential, dependency order | Initialize and return; respect the shared startup deadline |
| `Run(ctx)` | Concurrent, supervised | Block while serving or working; drain when the application context is canceled |
| `Stop(ctx)` | Sequential, reverse startup order | Release resources within the shared shutdown deadline |

Each component needs at least one callback. Without `DependsOn`, registration
order is preserved. Declare dependencies by component name to allow registration
in any order. Among components eligible to start, registration order breaks ties.
Shutdown invokes a component's `Stop`, waits for its runner to
finish, and then moves to the preceding dependency. Startup failure rolls back
components whose initialization completed successfully. A failing `Start` must
clean up any partial initialization before returning its error.

```go
component := foundation.Component{
    Name: "consumer",
    DependsOn: []string{"database", "broker"},
    Start: consumer.Connect,
    Run: consumer.Consume,
    Stop: consumer.Close,
}
// Register component, database, and broker in any order before Run.
```

`Register` normalizes and copies dependency names and rejects empty, invalid,
self, or duplicate references. `Run` validates missing references and cycles
before calling any component, returning `ErrInvalidDependency` with `StateFailed`
on an invalid graph. Startup rollback follows reverse successful startup order.
Dependencies guarantee initialization order; they do not wait for a runner's
readiness. All `Run` callbacks still execute concurrently after startup.
Adapters return ordinary components; set `DependsOn` on the returned component
before registering it.

Foundation owns OS signals and shutdown for the application. It handles
`SIGINT` and `SIGTERM` by default. Use one lifecycle coordinator and let adapters
own their listeners. Put blocking work in `Component.Run` or `Worker`; return
initialization work from `Start` without launching unmanaged server goroutines.

## Components And Helpers

`Component` pairs initialization, runtime work, and cleanup:

```go
if err := app.Register(foundation.Component{
    Name:  "consumer",
    Start: consumer.Connect,
    Run:   consumer.Consume,
    Stop:  consumer.Close,
}); err != nil {
    return err
}
```

| Helper | Callbacks | Use |
| --- | --- | --- |
| `Resource(name, start, stop)` | `Start`, `Stop` | Database, cache, telemetry, or another initialized dependency |
| `Worker(name, run)` | `Run` | Context-aware background work |
| `Closer(name, closer)` | `Stop` | An already acquired `io.Closer` |

A worker returning before application cancellation is a runtime failure, even
when it returns `nil` (`ErrUnexpectedExit`). During cancellation, `nil` and
errors consisting only of the application context's cancellation/deadline error
are normal exits. Other errors, including flush failures, joined cancellation
and failure errors, and panics, are retained in the terminal result.

## Server Adapters

| Adapter | Constructor | Server configuration |
| --- | --- | --- |
| net/http, Gin, Echo | `nethttp.Component(name, server, options...)` | Explicit address, non-nil handler, effective header read timeout |
| Fiber | `fiber.Component(name, address, app, options...)` | Non-nil app with a positive `ReadTimeout` |
| gRPC | `grpc.Component(name, address, server)` | Non-nil server; configure transport credentials and message limits when constructing it |

Adapters live under `foundation/adapter`. Gin and Echo engines implement
`http.Handler` and can be assigned to `http.Server.Handler`. Fiber uses its
dedicated adapter.

All adapters bind their TCP listener during `Start`. An invalid or occupied
address fails startup before readiness becomes healthy. Rollback closes the
owned listener even if `Run` has not begun. The net/http adapter updates
`server.Addr` to the bound address, including the assigned port when using `:0`.

For net/http, set `ReadHeaderTimeout > 0`, or leave it at zero and set
`ReadTimeout > 0`. Fiber requires `ReadTimeout > 0`. The HTTP and Fiber adapters
accept `AllowUnsafeTimeouts()` only as an explicit opt-out when an upstream
proxy enforces an equivalent limit or a documented deployment requirement
provides equivalent protection.

### HTTPS

The net/http adapter serves HTTP when `server.TLSConfig` is nil. A non-nil
`TLSConfig` enables HTTPS through `http.Server.ServeTLS`; it must provide
`Certificates`, `GetCertificate`, or `GetConfigForClient`. An empty TLS
configuration is rejected during startup, before binding the listener.

Load deployment-provided certificate files before registering the server:

```go
certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
if err != nil {
    return err
}
server.TLSConfig = &tls.Config{
    MinVersion:   tls.VersionTLS12,
    Certificates: []tls.Certificate{certificate},
}
if err := app.Register(foundationhttp.Component("https", server)); err != nil {
    return err
}
```

Import `crypto/tls` for this configuration. Certificate callbacks follow the
standard library contract; invalid certificates or callback errors fail TLS
handshakes. Configure the server before starting the app. If TLS terminates at
a trusted reverse proxy, the internal HTTP server can keep `TLSConfig` nil.
The Fiber adapter serves a plain TCP listener; configure TLS at a trusted proxy
or supply a custom component for a TLS listener. gRPC transport security is
configured on `grpc.Server`.

## Configuration And Options

| Configuration | Behavior |
| --- | --- |
| `Name` | Required application name |
| `Version` | Optional application version, used by the default health checker |
| `StartupTimeout` | Total startup deadline; zero defaults to 30 seconds |
| `ShutdownTimeout` | Total shutdown deadline; zero defaults to 30 seconds |

Negative timeouts are rejected. Names, versions, and component names are trimmed,
limited to 128 bytes, and must not contain Unicode control/format characters.

| Option | Behavior |
| --- | --- |
| `WithLogger(logger)` | Supply the structured `Info`/`Error` logging contract; defaults to `slog.Default()` |
| `WithHealthChecker(checker)` | Supply the application's dependency health checker |
| `WithSignals(signals...)` | Replace the default signal set with a non-empty set |
| `WithoutSignals()` | Disable signal handling for tests or an embedding process |

`New` returns an error for nil options, loggers, health checkers, or signals.
When embedding Foundation, pass the owning process's context to `Run` and use
`WithoutSignals()`.

## Health And Readiness

`app.State()` reports the lifecycle state. `app.Health()` returns a
`health.Checker` containing the reserved readiness check `foundation.lifecycle`.
It reports ready only in `StateRunning`; dependency checks may still report down.
Readiness is down during initialization, draining, and terminal states.

```go
import (
    redischecker "github.com/thanhbvha/go-common/v2/health/checker/redis"
    sqlchecker "github.com/thanhbvha/go-common/v2/health/checker/sql"
)

app.Health().AddReadinessCheck("postgres", sqlchecker.New(sqlPool))
app.Health().AddReadinessCheck("redis", redischecker.New(redisClient))
```

`sqlPool` implements `PingContext(context.Context) error` (for GORM, obtain the
underlying `*sql.DB` first). Integration checkers remain outside the Foundation
and health cores so applications import only the adapters they use.

Reserve `foundation.lifecycle` for the app. Health error details are disabled by
default; expose details only on protected diagnostic endpoints.

## Errors And Programmatic Shutdown

`Run(ctx)` blocks until parent cancellation, an OS signal, `Shutdown(ctx)`, or a
component failure. Errors from completed runners and shutdown callbacks are
joined with the initiating failure. `ComponentError` identifies the component
and phase (`PhaseStart`, `PhaseRun`, or `PhaseStop`). `PanicError` preserves the
recovered value and stack. Use `errors.Is` and `errors.As` to inspect failures.

Deadline failures also wrap `*TimeoutError`. It records `Component`, `Phase`,
`Operation` (start callback, stop callback, runner wait, or a phase boundary),
`Elapsed` for the entire phase, its configured `Budget`, and `Pending` component
names in execution order, including the current component. During shutdown these
are components not confirmed fully stopped; earlier dependencies remain open
once the shared deadline expires. `errors.Is(err, context.DeadlineExceeded)`
continues to work. A callback's own deadline error retains that error as well;
`Budget` is the phase budget, not a per-callback deadline.

```go
var timeout *foundation.TimeoutError
if errors.As(err, &timeout) {
    log.Printf("phase=%s component=%s operation=%s elapsed=%s budget=%s pending=%q",
        timeout.Phase, timeout.Component, timeout.Operation,
        timeout.Elapsed, timeout.Budget, timeout.Pending)
}
```

Cancellation of the owning `Run` context still requests normal shutdown.
The waiting deadline passed to `Shutdown` only bounds that caller's wait and
returns its context error; it does not provide lifecycle timeout diagnostics.

`app.Shutdown(ctx)` requests shutdown and waits for the terminal result. It is
safe for concurrent callers; each caller has its own waiting deadline. A caller's
expired waiting context does not change the app's configured shutdown deadline.
After termination, callers receive the same retained result as `Run`: nil for
`StateStopped`, or the joined failure for `StateFailed`. Calling shutdown before
`Run` returns `ErrNotRunning`; running an app again returns `ErrAlreadyRun`.

## Resource And Security Boundaries

- Callback deadlines bound Foundation's waiting time. Go cannot forcibly stop a
  goroutine that ignores cancellation. If startup times out but later succeeds,
  Foundation attempts cleanup with a fresh shutdown context.
- If a dependent component cannot stop within the total shutdown deadline,
  earlier dependencies remain open. The returned error lets the process owner
  decide how to terminate. Errors from runners still executing after this
  deadline cannot be included in an already returned result.
- Register WebSocket managers separately: `http.Server.Shutdown` does not drain
  hijacked connections.
- Component errors are logged as returned. Redact credentials, tokens, connection
  strings, and sensitive payloads before returning an error.

### gRPC wrapper shutdown

For `github.com/thanhbvha/go-common/v2/grpc.Server`, use the adapter's
`ComponentWithShutdown(name, address, srv.Server, srv.Shutdown)`. It binds the
listener in Start, serves in Run and invokes the supplied callback in Stop. This
preserves wrapper health transitions, configured drain deadlines and forced-stop
errors. `Component(name, address, nativeServer)` remains available for native gRPC
servers. Do not start a second serving/signal loop for the same server.

The runnable [gRPC foundation example](../examples/grpc/README.md#foundation-lifecycle-variant)
shows dependency ordering, readiness, YAML/env configuration and graceful/forced drain.
