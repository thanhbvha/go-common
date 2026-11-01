# gRPC server/client example

Separate commands demonstrate `go-common/grpc` with unary/streaming RPCs, auth,
TLS/mTLS, YAML/environment configuration and bounded graceful shutdown. Run all
commands from the repository root; this example shares the root `go.mod`.

```text
cmd/server/              standalone server; caller-owned signal context
cmd/foundation/          foundation-owned signals and application lifecycle
cmd/client/              selectable client scenarios
cmd/certgen/             development localhost certificate generator
internal/demo/config.go  typed options, flags, token and telemetry lifecycle
internal/demo/config_loader.go config.Load, env binding and CLI precedence
internal/demo/server.go  server limits, auth, readiness and standalone serving
internal/demo/foundation.go dependency ordering and foundation adapter wiring
internal/demo/client.go  reusable connection and RPC scenarios
internal/demo/service.go validation and cancellation-aware unary/stream handlers
internal/demo/auth.go    unary/stream bearer authentication
internal/demo/transport.go TLS/mTLS and loopback-only plaintext
internal/demo/*_test.go  config, RPC and lifecycle integration tests
internal/devcert/        short-lived development certificate generation
proto/echo.proto         schema; *.pb.go files are generated
server.yaml/client.yaml  non-secret loopback templates
```

## Ready-to-run private local configuration

This workspace has `server.local.yaml` and `client.local.yaml` containing the same
random development token. These files are Git-ignored and are not committed
repository templates. Run in two terminals:

```powershell
go run ./examples/grpc/cmd/server -config examples/grpc/server.local.yaml
```

```powershell
go run ./examples/grpc/cmd/client -config examples/grpc/client.local.yaml -scenario basic
```

Use `-scenario errors`, `health`, `watch`, or `deadline -timeout 100ms` for the other
flows. The local server config enables demonstration error/panic endpoints. Stop
with Ctrl+C. No environment token is needed for these private files. A nonempty
`GRPC_DEMO_TOKEN` overrides the YAML token; clear a stale value in both terminals
if it differs.

For a fresh checkout, copy the sample YAML files to `*.local.yaml` and put the same
cryptographically random secret (at least 32 bytes, printable ASCII without spaces)
in their `token` fields. Never commit private configuration files.

## Environment token and plaintext quick start

The committed templates have an empty `token`. Set the same random development
secret in both terminals, for example interactively in PowerShell 7:

```powershell
$env:GRPC_DEMO_TOKEN = Read-Host 'Shared development secret (32+ characters)' -MaskInput
```

Then run server and client separately:

```powershell
go run ./examples/grpc/cmd/server -insecure -demo-errors
go run ./examples/grpc/cmd/client -insecure -scenario basic
```

Plaintext requires explicit `-insecure` and a loopback `host:port`; remote/wildcard
plaintext addresses are rejected. Defaults are server `127.0.0.1:50051` and client
`localhost:50051`. Use built binaries when a supervisor must signal the actual
server process rather than the `go run` wrapper.

## Configuration: YAML or environment

Both servers share the same `ParseServer` and YAML/env schema. The client has its
own schema. The loader uses repository `config.Load(config.DefaultOptions(), ...)`
with `AutomaticEnv` enabled, plus explicit Viper field binding for keys absent from
YAML and environment-only deployments.

Precedence: **explicit CLI flags > process environment > YAML > defaults**.
Nonempty `GRPC_DEMO_TOKEN` is a special shared override above command-specific
`GRPC_SERVER_TOKEN` / `GRPC_CLIENT_TOKEN` and YAML `token`. There is no CLI token
flag. Both sides must resolve to the same token.

No file is loaded automatically. Use `-config` or `GRPC_SERVER_CONFIG` /
`GRPC_CLIENT_CONFIG`; `-config=` explicitly selects environment/defaults only.
Selected missing/malformed files fail startup. `-h` does not load a file. Typed
configuration errors fail even if a later CLI override was supplied; raw values
are not included in configuration errors. No hot reload is enabled.

With an environment token set, the shipped templates work as follows:

```powershell
go run ./examples/grpc/cmd/server -config examples/grpc/server.yaml -demo-errors
go run ./examples/grpc/cmd/client -config examples/grpc/client.yaml
```

Environment-only setup, in the respective server/client terminal:

```powershell
$env:GRPC_SERVER_ADDR = '127.0.0.1:50051'
$env:GRPC_SERVER_INSECURE = 'true'
$env:GRPC_SERVER_DEMO_ERRORS = 'true'
go run ./examples/grpc/cmd/server
```

```powershell
$env:GRPC_CLIENT_TARGET = 'localhost:50051'
$env:GRPC_CLIENT_INSECURE = 'true'
$env:GRPC_CLIENT_SCENARIO = 'deadline'
$env:GRPC_CLIENT_TIMEOUT = '100ms'
go run ./examples/grpc/cmd/client
```

Clear plaintext environment settings before TLS scenarios, or override them with
`-insecure=false`. Relative certificate paths resolve against the working directory,
not the YAML directory. The templates explicitly select local plaintext.

| YAML key | Server env | Client env |
| --- | --- | --- |
| `addr` / `target` | `GRPC_SERVER_ADDR` | `GRPC_CLIENT_TARGET` |
| `insecure` | `GRPC_SERVER_INSECURE` | `GRPC_CLIENT_INSECURE` |
| `token` (private YAML) | `GRPC_SERVER_TOKEN` | `GRPC_CLIENT_TOKEN` |
| `cert`, `key`, `ca` | `GRPC_SERVER_CERT`, `_KEY`, `_CA` | `GRPC_CLIENT_CERT`, `_KEY`, `_CA` |
| `server_name` (client only) | - | `GRPC_CLIENT_SERVER_NAME` |
| `reflection` | `GRPC_SERVER_REFLECTION` | - |
| `demo_errors` | `GRPC_SERVER_DEMO_ERRORS` | - |
| `shutdown_timeout` | `GRPC_SERVER_SHUTDOWN_TIMEOUT` | - |
| `ready_delay` | `GRPC_SERVER_READY_DELAY` | - |
| `startup_timeout` (foundation) | `GRPC_SERVER_STARTUP_TIMEOUT` | - |
| `app_shutdown_timeout` (foundation) | `GRPC_SERVER_APP_SHUTDOWN_TIMEOUT` | - |
| `scenario`, `message`, `timeout` | - | `GRPC_CLIENT_SCENARIO`, `_MESSAGE`, `_TIMEOUT` |
| `otlp_endpoint` | `GRPC_SERVER_OTLP_ENDPOINT` | `GRPC_CLIENT_OTLP_ENDPOINT` |
| `otlp_insecure` | `GRPC_SERVER_OTLP_INSECURE` | `GRPC_CLIENT_OTLP_INSECURE` |

Keys are flat snake_case; flags are kebab-case. Durations accept `250ms`, `3s`,
`1m`. Explicit `-reflection=false`, `-ready-delay=0s` and `-otlp-endpoint=` override
file/env values; empty environment strings clear string settings. Keep tokens in
Git-ignored `*.local.yaml`, and use deployment-managed secrets in production.

## Foundation lifecycle variant

Stop the standalone server before starting this alternative on the same address.
The existing client, TLS/mTLS options, token and scenarios are reused:

```powershell
go run ./examples/grpc/cmd/foundation -config examples/grpc/server.local.yaml
```

```powershell
go run ./examples/grpc/cmd/client -config examples/grpc/client.local.yaml -scenario basic
go run ./examples/grpc/cmd/client -config examples/grpc/client.local.yaml -scenario health
```

In another checkout, use `server.yaml` with an environment token or prepare a
private YAML token as above. To simulate dependency initialization:

```powershell
go run ./examples/grpc/cmd/foundation -config examples/grpc/server.local.yaml -ready-delay 5s -startup-timeout 15s -shutdown-timeout 10s -app-shutdown-timeout 30s
```

Only `foundation.App` owns SIGINT/SIGTERM. Its command passes `context.Background()`;
it does not add another signal loop or call `srv.Serve()` / `ServeContext()` next
to the adapter.

| Stage | Order | Behavior |
| --- | --- | --- |
| Start | telemetry -> dependency -> gRPC | Exporter setup, dependency initialization, listener bind and SERVING |
| Run | gRPC adapter | Native serving under foundation supervision |
| Stop | gRPC -> dependency -> telemetry | NOT_SERVING, RPC drain, dependency closure after runner exit, then exporter flush |

The dependency is a simulation, not a real database/broker. Replace its Start/Stop
with connection initialization/cleanup. Start returns after initialization; only
the adapter's Run blocks while serving.

Foundation finishes dependency initialization **before binding the listener**.
Health starts NOT_SERVING internally and becomes SERVING after successful startup.
A client connecting during initialization may receive Unavailable. The standalone
variant instead listens during `ready_delay`, so its `watch` can observe live
NOT_SERVING -> SERVING transitions.

The registration is explicit:

```go
grpcadapter.ComponentWithShutdown("grpc", o.Address, srv.Server, srv.Shutdown)
```

Logs show dependency readiness, gRPC drain start/end (`forced=true` on timeout),
dependency closure and telemetry closure. Ctrl+C starts shutdown. An active RPC
can finish while its dependency remains open.

`shutdown_timeout` is the RPC drain budget (default 10s). `app_shutdown_timeout`
is the shared budget for all stop callbacks/runners (default 30s), and must exceed
the drain budget to leave time for dependency/telemetry cleanup. `startup_timeout`
bounds startup callbacks (default 30s). Corresponding CLI flags are
`-shutdown-timeout`, `-app-shutdown-timeout` and `-startup-timeout`.

Startup failure rolls back initialized components. For example,
`-ready-delay 2s -startup-timeout 100ms` exits nonzero before opening the listener.
Forced draining returns a nonzero lifecycle result; cancellation with a successful
drain exits zero. Exporter flush uses foundation's remaining shutdown context.
Handlers must honor cancellation; Go cannot forcibly terminate a goroutine.

Integration tests verify dependency ordering/readiness, availability while a real
RPC drains, forced-shutdown error retention, bind-failure rollback and startup
deadlines. They require no external database/broker.

## TLS and mTLS

Generate short-lived localhost certificates without OpenSSL:

```powershell
go run ./examples/grpc/cmd/certgen
```

Default output `examples/grpc/certs/` is Git-ignored. The generator refuses an
existing directory; use a fresh `-out` directory for renewal and keep alternative
output outside Git. Certificates expire after 24 hours, cover localhost/loopback,
and are development-only. The CA private key stays in memory. Key files request
mode 0600 on Unix; set appropriate ACLs on Windows. `certgen` uses only its `-out`
flag, not the server/client config loader.

With the same environment token in both terminals, use TLS:

```powershell
go run ./examples/grpc/cmd/server -cert examples/grpc/certs/server.pem -key examples/grpc/certs/server-key.pem -demo-errors
```

```powershell
go run ./examples/grpc/cmd/client -ca examples/grpc/certs/ca.pem -scenario basic
```

Replace `cmd/server` with `cmd/foundation` for the foundation variant. Without
`-ca`, clients trust system roots. `-server-name` can override the verification
name; certificate verification is never disabled. No plaintext fallback occurs.

For mTLS, add `-ca examples/grpc/certs/ca.pem` to the server and present the client
certificate as well:

```powershell
go run ./examples/grpc/cmd/client -ca examples/grpc/certs/ca.pem -cert examples/grpc/certs/client.pem -key examples/grpc/certs/client-key.pem -scenario basic
```

mTLS still requires the bearer token. Both health and reflection are authenticated;
probes need credentials or a separately secured endpoint. The shared token is a
single-principal demo: production needs identity, expiry/revocation and method/tenant
authorization, plus managed PKI/rotation.

## Scenarios and limits

| Scenario | Behavior | Expected result |
| --- | --- | --- |
| `basic` | Unary Echo, then EchoStream | Unary success; three stream messages and EOF |
| `errors` | Validation, business error, panic, post-panic Echo, invalid stream | InvalidArgument, InvalidArgument, Internal, success, InvalidArgument |
| `deadline` | Configured unary timeout, then shorter caller deadline | DeadlineExceeded for both |
| `health` | Health.Check for `echo.EchoService` | SERVING; nonzero exit when not ready |
| `watch` | Health.Watch with a finite caller context | Status updates; ends after `-timeout` |

`errors` requires server `-demo-errors`; deliberate error/panic endpoints are off
by default. `deadline` requires `-timeout` below 5s because simulated work is capped
at 5s. Unexpected results exit nonzero. The client reuses/closes one connection;
`NewClient` is lazy and connectivity/TLS are verified by RPCs. Unary timeout is
connection configuration; streaming calls own a separate finite context.

Server limits demonstrate native gRPC options: 64 KiB messages, 16 KiB headers,
128 concurrent streams per connection, 10s handshake timeout. Handlers require a
nonblank message <= 1024 characters, delay_ms <= 5000, count <= 10 (zero means three
stream messages). Add global admission/rate limits for your real workload.

Logs exclude payloads, tokens, metadata and raw transport error text. Recovery
logs a stack trace; restrict log access. Recovery covers the handler goroutine,
not arbitrary application goroutines. Unary and stream auth use Chain interceptors
inside wrapper recovery/error mapping. Reflection is off unless `-reflection`.

Standalone uses `signal.NotifyContext` plus `ServeContext`, with no global
`utils/graceful` callbacks. Library `Serve()` owns signals; `ListenAndServe(ctx)`
uses the caller context. See [gRPC lifecycle](../../grpc/README.md#lifecycle-and-graceful-shutdown)
for migration from deprecated graceful APIs and forced-stop limitations.

## OpenTelemetry

Export is disabled unless `-otlp-endpoint` is provided. Start a collector separately
and configure both commands, for example with a local plaintext collector:

```powershell
go run ./examples/grpc/cmd/server -insecure -otlp-endpoint localhost:4317 -otlp-insecure
go run ./examples/grpc/cmd/client -insecure -otlp-endpoint localhost:4317 -otlp-insecure -scenario basic
```

The foundation command accepts the same options. `-otlp-insecure` controls only
collector transport; `-insecure` controls application RPCs. Collector TLS otherwise
uses system roots. Endpoints are host:port without embedded credentials. Trace
sampling is 10%; metrics use the telemetry module's interval. Standalone/client
flush with a fresh 5s context, while foundation uses its shared shutdown budget.
A live collector is needed to verify export; it is not part of these tests.

## Verification and generation

```powershell
go run ./examples/grpc/cmd/server -h
go run ./examples/grpc/cmd/foundation -h
go run ./examples/grpc/cmd/client -h
go test ./examples/grpc/... ./grpc/... ./foundation/adapter/grpc/...
go test -race -count=3 ./examples/grpc/internal/...
go vet ./examples/grpc/... ./grpc/...
```

Tests cover plaintext/TLS/mTLS, all scenarios, wrong-token rejection on
unary/stream/health, missing mTLS client certificates, input/config validation,
YAML/env/CLI precedence, env-only keys, zero-value overrides, safe config errors,
YAML tokens, certificate overwrite protection and both server lifecycles.

After editing the schema, regenerate committed outputs with protoc and the Go
plugins on PATH:

```powershell
protoc --proto_path=examples/grpc --go_out=examples/grpc --go_opt=paths=source_relative --go-grpc_out=examples/grpc --go-grpc_opt=paths=source_relative proto/echo.proto
```

The old single-main example has been replaced with separate commands. Library API
details: [grpc/README.md](../../grpc/README.md).
