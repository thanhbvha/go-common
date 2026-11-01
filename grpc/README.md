# gRPC

Reusable gRPC client/server wrappers with unary and streaming recovery, error
mapping, metadata-free request logging, optional OpenTelemetry, and health checks.

## Production server

Load certificates from deployment-managed files or a secret provider. Configure
`ServerConfig.Credentials` with `credentials.NewTLS` (or `NewServerTLSFromFile`).
For mTLS, configure `ClientAuth: tls.RequireAndVerifyClientCert` and `ClientCAs`.
A nil server credential retains plaintext support for explicitly secured proxy
networks; the wrapper does not create certificates or enforce network isolation.

```go
cfg := common_grpc.DefaultServerConfig
cfg.Credentials = serverCredentials
cfg.ShutdownTimeout = 20 * time.Second
srv := common_grpc.NewServer(cfg,
    grpc.MaxRecvMsgSize(4 << 20),
    grpc.MaxSendMsgSize(4 << 20),
    grpc.MaxConcurrentStreams(128),
    grpc.MaxHeaderListSize(16 << 10),
    grpc.ConnectionTimeout(10 * time.Second),
    grpc.ChainUnaryInterceptor(authUnary),
    grpc.ChainStreamInterceptor(authStream),
)
// Register protobuf services before serving.
// Health starts SERVING for compatibility; control readiness explicitly:
srv.Health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
// Initialize application dependencies, then set SERVING.

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
lis, err := net.Listen("tcp", ":50051")
if err != nil { return err }
return srv.ServeContext(ctx, lis)
```

The snippet belongs in a function returning `error`; `authUnary`, `authStream`,
and `serverCredentials` are application-provided. Authenticate and authorize both
RPC types, including health/reflection according to deployment policy. Authentication
is not enabled automatically. Use **Chain** interceptor options so wrapper recovery
and error mapping enclose your interceptors. Native `UnaryInterceptor` and
`StreamInterceptor` run outside the wrapper chain and own their recovery policy.
Other native options are appended after wrapper options and may override them.

## Lifecycle and graceful shutdown

The gRPC module owns protocol-specific draining; `foundation` owns application
startup/shutdown order and signals when coordinating several services. A separate
generic gRPC graceful coordinator is unnecessary. No gRPC entry point uses the
deprecated process-global `utils/graceful.Register` or `Wait` API.

| API | Listener | Signal handling |
| --- | --- | --- |
| `Serve()` | Binds `Address` or `:Port` | Owns SIGINT/SIGTERM for a standalone process |
| `ListenAndServe(ctx)` | Binds with the caller context | Caller owns signals |
| `ServeContext(ctx, listener)` | Takes ownership of an existing listener | Caller owns signals |
| `Shutdown(ctx)` | Drains the server | No signal handling |

`Shutdown(ctx)` marks health NOT_SERVING, starts one shared graceful drain and
requests native `Stop` if the caller deadline or configured `ShutdownTimeout`
(default 30s) expires. Concurrent/repeated calls share the drain and at most one
forced stop. The first forced-shutdown error is retained for later calls and for
the serving result. Native listener closure alone does not mean draining is done;
`ServeContext` waits for the wrapper's terminal shutdown result.

Native stopping runs asynchronously so a handler ignoring cancellation, or
`grpc.WaitForHandlers(true)`, cannot make the wrapper wait beyond its deadline.
Forced shutdown returns a context error; it does **not** guarantee every application
goroutine has exited or native `Stop` has already returned. Handlers and spawned
goroutines must honor cancellation; Go cannot forcibly terminate them. Long-lived
streams need an application-defined termination/cancellation policy, otherwise the
drain reaches its deadline. Use `Shutdown`, rather than the embedded native
`Stop`/`GracefulStop`, when health and terminal-result handling are required.

**Migration:** `Serve()` now owns signal handling and bounded draining; it no longer
registers a callback in the old global graceful registry. Code that called
`graceful.Wait` to coordinate multiple services should move to `foundation`, or
call `Shutdown` from an explicit instance coordinator. Prefer context-based APIs
in embedded applications/tests; install only one application signal owner.

For `foundation`, keep blocking serving in `Component.Run`. The adapter can bind
in Start, serve in Run and invoke the wrapper shutdown in Stop:

```go
// grpcadapter: github.com/thanhbvha/go-common/foundation/adapter/grpc
// app: an initialized foundation.App; srv: the wrapper created above
err := app.Register(grpcadapter.ComponentWithShutdown(
    "grpc", ":50051", srv.Server, srv.Shutdown,
))
if err != nil { return err }
return app.Run(context.Background()) // foundation owns SIGINT/SIGTERM and LIFO stop
```

`grpcadapter.Component` still supports native `*grpc.Server` with its existing
shutdown policy. Use `ComponentWithShutdown` for wrapper health/error semantics.
Do not also call `srv.Serve()`/`ListenAndServe`/`ServeContext` when the adapter owns
serving. Configure `ShutdownTimeout` within the application's total shutdown
budget and register dependencies before the server so they stop after its drain.

Reflection is disabled unless `EnableReflection` is true. Telemetry needs an
application-configured OpenTelemetry provider/exporter. Zero-value `ServerConfig`
disables optional middleware; copy `DefaultServerConfig` to enable recovery,
logging, telemetry and health.

## Client and migration

```go
conn, err := common_grpc.NewClient(common_grpc.ClientConfig{
    Target: "dns:///orders.example.com:443",
    EnableTelemetry: true,
    UnaryTimeout: 5 * time.Second,
})
if err != nil { return err }
defer conn.Close()
// Reuse conn and generated clients across requests.
```

**Behavior changes:** clients now default to verified TLS with system CA roots and
TLS >= 1.2; old plaintext development/proxy connections must set `Insecure: true`.
Set `Credentials` for a private CA or mTLS. Conflicting `Insecure` and `Credentials`,
blank targets, and negative timeouts return configuration errors. Additional native
`grpc.DialOption` values support credentials, interceptors, call limits and resolver
configuration. They are trusted overrides, not a security sandbox.

`NewClient` is lazy: success does not establish connectivity or validate the remote
certificate. RPCs surface network/handshake failures. `UnaryTimeout` caps unary RPCs
and preserves shorter caller deadlines; zero requires the caller to set deadlines.
Set a context deadline/cancel function for streaming calls. Retries and wait-for-ready
are application choices: only retry operations safe to repeat, with a bounded budget.

## Error and log policy

Both unary and streaming RPCs preserve explicit gRPC statuses/details and map
`xerrors` to gRPC codes. Context cancellation/deadline errors are mapped correctly.
HTTP 422 maps to InvalidArgument and 413 to ResourceExhausted. Any non-nil error
with a success HTTP status maps to Internal rather than silently succeeding.
Unknown errors and 5xx `xerrors` expose a generic message. Explicit gRPC statuses
and 4xx `xerrors.Message` are public contracts: keep sensitive data out of them.

Logs include method, latency and status, without payloads, metadata, raw errors or
panic values. Recovery logs a stack trace and returns Internal. Protect access to
stack traces and telemetry. Panics produce a recovery event rather than a normal
completion log.

## Resource tuning and verification

Size, concurrency, keepalive and connection policies use native gRPC options;
choose limits for the real payload and workload. Concurrent-stream limits are per
connection, not a global rate limit. Add application admission/rate limits where
needed. Coordinate keepalive with clients/load balancers rather than aggressive
PING intervals. Benchmark throughput and p95/p99 latency under representative load
before selecting buffer/window sizes or disabling logging/tracing.

```sh
go test ./grpc/... ./examples/grpc/...
go test -race -count=3 ./grpc/...
go vet ./grpc/... ./examples/grpc/...
```

Tests cover verified TLS, rejection of plaintext by a default client, opt-in
reflection, application readiness, unary/stream panic and error behavior, message
limits, unary deadlines, concurrent/repeated shutdown, retained forced-shutdown
errors, and bounded return even when a handler ignores cancellation. See the
[server/client example](../examples/grpc/README.md) for runnable plaintext,
TLS/mTLS, streaming, auth, health and deadline scenarios. Its development
certificates and shared-token identity are not production security configuration.

References: [authentication](https://grpc.io/docs/guides/auth/),
[deadlines](https://grpc.io/docs/guides/deadlines/),
[graceful shutdown](https://grpc.io/docs/guides/server-graceful-stop/),
[keepalive](https://grpc.io/docs/guides/keepalive/).
