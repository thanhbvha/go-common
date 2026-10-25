# EventBus examples

These examples use the current explicit EventBus API and the completed Memory,
Redis, NATS JetStream, and Kafka transports. Start with the all-in-one program;
then use the separate producer, worker, subscriber, and administrator programs
to see process boundaries.

All payloads and addresses are synthetic local-development defaults. Do not put
credentials in command-line flags, URLs, logs, message headers, or payloads.

## Quickstart

Memory needs no external service:

```powershell
go run ./examples/eventbus --driver memory
```

Memory is bounded and process-local. It is suitable for this all-in-one example,
tests, and local development, but all queued, delayed, dedupe, and DLQ state is
lost when the process exits. It does not connect separate producer/worker
processes and is not a durable substitute for Redis, NATS, or Kafka.

The quickstart declares a task and an event, starts one task handler and two
independent event subscriber groups, then demonstrates:

- typed payload decoding and validation;
- ACK, deterministic retry, permanent failure to DLQ, and intentional skip;
- `WithDelay` and `WithRunAt`;
- headers and trace injection;
- ordering keys;
- deduplication when advertised by the selected transport;
- health checks, queue statistics, metrics, tracing, and graceful drain.

Kafka does not advertise deduplication, so that scenario is skipped explicitly.
The default wait is derived from each driver's delay precision. Override it with
`--wait`, for example `--wait 12s` when a broker is under load.

## Broker setup

The examples connect to existing services; they do not start or destroy broker
containers. Local plaintext defaults are for development only.

```powershell
go run ./examples/eventbus --driver redis --redis-addr 127.0.0.1:6379
go run ./examples/eventbus --driver nats --nats-urls nats://127.0.0.1:4222
go run ./examples/eventbus --driver kafka --kafka-brokers 127.0.0.1:9092
```

Configuration can also come from environment variables:

| Variable | Purpose |
| --- | --- |
| `EVENTBUS_DRIVER` | `memory`, `redis`, `nats`, or `kafka` |
| `EVENTBUS_NAMESPACE` | Isolates all example topology names |
| `EVENTBUS_REDIS_ADDR` | Redis `host:port` |
| `EVENTBUS_REDIS_PASSWORD` | Redis authentication secret |
| `EVENTBUS_NATS_URLS` | Comma-separated NATS URLs |
| `EVENTBUS_NATS_CREDS_FILE` | NATS credentials file path |
| `EVENTBUS_NATS_NKEY_FILE` | NATS NKey seed file path |
| `EVENTBUS_KAFKA_BROKERS` | Comma-separated Kafka brokers |
| `EVENTBUS_KAFKA_TLS` | Enable TLS with a minimum of TLS 1.2 |
| `EVENTBUS_KAFKA_SASL_MECHANISM` | `plain`, `scram-sha-256`, `scram-sha-512`, or `oauthbearer` |
| `EVENTBUS_KAFKA_SASL_USERNAME` | Kafka SASL username |
| `EVENTBUS_KAFKA_SASL_PASSWORD` | Kafka SASL password |
| `EVENTBUS_KAFKA_SASL_TOKEN` | Static OAuth bearer token |

The Kafka example requires TLS whenever SASL is enabled. Production deployments
must also configure trusted CA roots and, when needed, client certificates in
application-owned `tls.Config`; the example intentionally does not invent or
disable certificate verification.

## Separate processes

Memory is process-local, so use Redis, NATS, or Kafka for these commands. Keep
the driver and namespace identical in every terminal.

Start two task workers to demonstrate competing consumption:

```powershell
go run ./examples/eventbus/worker --driver redis --namespace demo_workers
go run ./examples/eventbus/worker --driver redis --namespace demo_workers
```

Start two independent fan-out subscriber groups:

```powershell
go run ./examples/eventbus/subscriber --driver redis --namespace demo_workers --group audit
go run ./examples/eventbus/subscriber --driver redis --namespace demo_workers --group notification
```

Publish one task and one event:

```powershell
go run ./examples/eventbus/producer --driver redis --namespace demo_workers --mode ack
```

Valid task modes are `ack`, `retry`, `permanent`, `skip`, and `slow`. Retry fails
only on attempt one, making the example deterministic. Run two subscriber
instances with the same `--group` to demonstrate competing instances within one
fan-out group.

Workers and subscribers wait for `Ctrl+C` or `SIGTERM`, stop intake, drain
accepted deliveries for up to 15 seconds, stop transport schedulers, and finally
close the application-owned broker client.

## Statistics and DLQ administration

Create a permanent failure first, then inspect it:

```powershell
go run ./examples/eventbus/producer --driver redis --namespace demo_admin --mode permanent
go run ./examples/eventbus/worker --driver redis --namespace demo_admin
go run ./examples/eventbus/admin --driver redis --namespace demo_admin --action stats
go run ./examples/eventbus/admin --driver redis --namespace demo_admin --action dlq-count
go run ./examples/eventbus/admin --driver redis --namespace demo_admin --action dlq-list --limit 20
```

Use the printed `DeadLetter.ID`, not an application payload ID, for requeue:

```powershell
go run ./examples/eventbus/admin --driver redis --namespace demo_admin --action dlq-requeue --ids '<dead-letter-id>'
```

Purge is deliberately guarded:

```powershell
go run ./examples/eventbus/admin --driver redis --namespace demo_admin --action dlq-purge --confirm-purge
```

The administrator prefers cursor pagination when a driver advertises it. Kafka
DLQ purge is logical: it advances an administration cursor, while physical log
records remain until broker retention removes them. Kafka requeue requires
complete per-partition prefixes and administrator operations must be serialized
across processes.

## OpenTelemetry and operations dashboard

With an OTLP gRPC collector listening locally, run the complete metrics/tracing
adapter example:

```powershell
go run ./examples/eventbus/observability --driver memory --otel-endpoint 127.0.0.1:4317
```

It demonstrates the canonical OTel instruments and W3C trace propagation while
using batch enqueue, pause, topology drain, resume, a local rate limit,
ordering-key concurrency policy, and payload-size limit. Import
`eventbus/dashboards/grafana-eventbus.json` after confirming your collector's
Prometheus name translation.

## Capability notes

| Behavior | Memory | Redis | NATS | Kafka |
| --- | --- | --- | --- | --- |
| Durable across process restart | No | Yes, deployment-dependent | Yes, JetStream storage-dependent | Yes, broker-dependent |
| Delay precision | 1 ms | 1 s | 1 s | 5 s |
| Dedupe | Test-only | Yes | Yes | No |
| Ordering guarantee | No | No | No | Within a source partition |
| DLQ admin and statistics | Test-only | Yes | Yes | Yes |

All transports are at-least-once. Handlers must be idempotent. Redis can settle
retry/DLQ atomically in one cluster slot; NATS and Kafka use confirmed
publish-before-source-settlement and may duplicate after an ambiguous response.
Kafka retry can reorder relative to later source records even when the ordering
key keeps records on one partition. Portable pause/resume controls stop only
local process intake; they are not broker-wide driver capabilities.

The shared helper applies driver-specific topology options only to that driver:
Redis stream bounds and blocking, NATS ACK/fetch bounds, and Kafka partitions,
replication, retention, fetch wait, and bounded delay sweeps.

## Verification

The default verification is broker-free:

```powershell
go test ./examples/eventbus/...
go vet ./examples/eventbus/...
go test ./eventbus/...
go vet ./eventbus/...
```

For broker-backed manual runs, use a unique namespace. Do not use `FLUSHDB`,
delete shared streams/topics, or purge a DLQ without reviewing the selected
namespace and supplying the explicit confirmation flag.

## Standardized benchmarks

`./examples/eventbus/benchmark` runs the Phase 6 payload, concurrency, immediate,
delayed, and retry matrix against any configured driver and emits machine-readable
JSON. `run-matrix.ps1` executes all 36 combinations. Durable preload data is not
automatically deleted; use only disposable infrastructure for large backlogs.
