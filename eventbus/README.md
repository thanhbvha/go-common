# EventBus v2

EventBus provides competing-consumer tasks and events with independent subscriber groups through one Go API. Applications explicitly declare topologies, attach handlers, inject a transport, and manage startup and drain.

Use the module path `github.com/thanhbvha/go-common/v2/eventbus`. The repository's `go.mod` is authoritative for the Go toolchain (currently Go 1.26.10). Module v2, broker resource naming v2, and wire envelope v3 are different version boundaries.

- [Runnable examples](../examples/eventbus/README.md): local walkthrough and separate broker-backed processes.
- [Detailed API and driver reference](reference/README.md): constructors, options, capabilities, observability, migration and custom transports.
- [Production adoption matrix](../docs/PRODUCTION.md): operational maturity and deployment gates.

## Start here

From the repository root:

```powershell
go run ./examples/eventbus/v2
```

This finite, broker-free walkthrough demonstrates explicit declarations, typed validation, task ACK/retry/skip/DLQ, two event groups, publication identity and dedupe, batch enqueue, delay and run-at, concurrency keys, pause/drain/resume, DLQ repair/requeue, statistics and shutdown. It checks completion rather than sleeping for an assumed processing duration.

Expected output:

```text
v2 complete: batch=5 duplicate=true task_calls=8 event_groups=2 dlq=0
```

Memory is bounded and process-local. All messages, delayed work, dedupe reservations and DLQ records disappear when the process exits. Use a broker-backed driver for separate processes or durable delivery.

## Minimal application

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "os"
    "time"

    "github.com/thanhbvha/go-common/v2/eventbus"
    "github.com/thanhbvha/go-common/v2/eventbus/driver/memory"
)

type Report struct { Account string `json:"account"` }

func run() (result error) {
    cfg := eventbus.DefaultConfig()
    cfg.Namespace = "reports_v2"
    bus, err := eventbus.Open(cfg, eventbus.WithTransport(memory.New()))
    if err != nil { return err }
    defer func() {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        result = errors.Join(result, bus.Close(ctx))
    }()

    if err := bus.DeclareTask("report.generate", eventbus.WithMaxAttempts(3)); err != nil { return err }
    done := make(chan struct{}, 1)
    if err := bus.HandleTask("report.generate", eventbus.Handle(func(ctx context.Context, job eventbus.Job, payload Report) error {
        if payload.Account == "" { return eventbus.MarkPermanent(errors.New("account required")) }
        if err := ctx.Err(); err != nil { return err }
        // Perform an idempotent operation using job.ID() or a business operation ID.
        select { case done <- struct{}{}: default: }
        return nil
    })); err != nil { return err }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := bus.Start(ctx); err != nil { return err }
    if err := bus.Enqueue(ctx, "report.generate", Report{Account: "demo"}); err != nil { return err }
    select {
    case <-done: return nil // Deferred Close drains the accepted delivery's settlement.
    case <-ctx.Done(): return ctx.Err()
    }
}

func main() {
    if err := run(); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
```

`Handle[T]` decodes JSON; handlers still validate payloads, authorization, ownership and tenancy before side effects. A successful decode is not a trusted business request.

## Tasks and events

| Intent | Declaration | Handler | Publication |
| --- | --- | --- | --- |
| One logical worker group; instances compete | `DeclareTask` | `HandleTask` | `Enqueue`, `EnqueueWithResult` |
| Independent delivery for each named group | `DeclareEvent` | `Subscribe(event, group, handler)` | `Publish`, `PublishWithResult` |

Declare everything before `Start`. Producer-only applications declare the same topology and call `Start` to provision resources, without registering local handlers. Equal repeated declarations are accepted; conflicting declarations, duplicate local handlers and task/event name collisions are errors. Declaration changes after startup are rejected.

For events, two instances using the same subscriber name compete within that group. Different names receive independent deliveries. Provision subscriber groups before publishing if every event must reach them; retention and driver configuration govern historical delivery. Memory needs subscribers provisioned in the same process.

Redis, NATS and Kafka event declarations **require** their driver's `WithEventLayoutTargeted()` option in every producer and subscriber process. For example:

```go
// import redisdriver "github.com/thanhbvha/go-common/v2/eventbus/driver/redis"
if err := bus.DeclareEvent("report.requested", redisdriver.WithEventLayoutTargeted()); err != nil {
    return err
}
if err := bus.Subscribe("report.requested", "audit", auditHandler); err != nil { return err }
if err := bus.Subscribe("report.requested", "notification", notificationHandler); err != nil { return err }
return bus.Publish(ctx, "report.requested", payload)
```

Use `natsdriver.WithEventLayoutTargeted()` or `kafkadriver.WithEventLayoutTargeted()` for those drivers. `Subscribe` retains implicit declaration for compatibility; explicit `DeclareEvent` makes provisioning options reviewable.

## Delivery, failure and supervision

Delivery is **at least once**. Handler execution and external side effects are not atomic with broker settlement. Idempotency is required even when publication dedupe is enabled.

| Handler result | Requested terminal decision |
| --- | --- |
| `nil` or `MarkSkip(reason)` | ACK |
| Ordinary error | Retry using `RetryPolicy`, then DLQ at the attempt limit |
| `MarkRetryAfter(err, delay)` | Retry after the requested delay, within the attempt limit |
| `MarkPermanent(err)` | DLQ immediately |
| Handler panic | Recovered as a retryable handler failure |

`Job.Attempt()` starts at 1. `MaxAttempts` includes the first execution. Broker redelivery can increase the effective attempt count; it is not an exactly-once execution budget.

Consumer sessions reconnect with bounded exponential backoff, including after `nil` or session-local cancellation. Replacement sessions wait for accepted workers and heartbeats from the old session to finish. Intake uses fixed worker pools and bounded queues.

Settlement succeeds only after the driver confirms its terminal operation. Completion metrics are recorded only on success. A failed settlement or unavailable lease ends the session so cleanup can recover unsettled sources and intake capacity. The engine does not blindly repeat an uncertain terminal operation on the same delivery: the broker may already have committed it. Redelivery may repeat application work.

Redis retry/DLQ transfers use same-slot atomic Lua. NATS and Kafka confirm destination publication before acknowledging/committing the source; interruption between those steps can duplicate transfers. Broker retention and capacity must preserve the source until settlement.

Infrastructure panics in consumer, worker, heartbeat or settlement paths cancel the session and degrade bus health. Repeated panic sessions stop the affected consumer at the storm limit. `Ping` reports `ErrBusDegraded`; a successful reconnect does not clear the sticky degraded state. Logger/metrics/tracer callbacks have isolated panic boundaries.

## Configuration and admission controls

Start with `DefaultConfig()` and override application-specific values:

| Setting | Default | Meaning |
| --- | --- | --- |
| `Namespace` | `eventbus` | Broker resource isolation |
| `ProcessTimeout` | 30 seconds | Cooperative handler timeout |
| `Concurrency` | 10 | Workers per task or subscriber |
| `Prefetch` | 20 | Buffered deliveries, excluding active workers |
| `MaxAttempts` | 5 | Total permitted attempts |
| `MaxPayloadBytes` | 1 MiB | Complete serialized envelope limit |
| `MaxTopologies`, `MaxSubscribers` | 0 | Optional startup plan limits; zero means unlimited |

Timeout cancellation cannot forcibly terminate a handler that ignores its context. Honor cancellation in blocking operations.

Topology overrides include `WithConcurrency`, `WithPrefetch`, `WithMaxAttempts`, `WithMaxPayloadBytes`, `WithRateLimit`, `WithConcurrencyKeyLimit` and `WithDelayDisabled`. Wrong-driver options are rejected. Rate limits and keyed admission apply within one process, not across a cluster.

`WithConcurrencyKey` is independent of `WithBrokerOrderingKey` (`WithOrderingKey` is its alias). Broker ordering is driver-specific; Kafka preserves source-partition order, while retry can reorder work. Use the explicit concurrency key with `WithConcurrencyKeyLimit` for local serialization. Rate/key waits keep supported leases alive and precede the handler timeout.

## Publication options and identity

- `WithDelay(duration)` or `WithRunAt(time)` schedules publication; do not combine them. Driver precision and scheduler health affect dispatch time.
- `WithHeader(key, value)` carries application/trace metadata. Never put credentials or sensitive payloads in headers or logs.
- `WithMessageID(id)` sets logical identity; identity alone is not a dedupe reservation.
- `WithDedupeKey(key)` and `WithDedupeTTL(ttl)` request topology-scoped publication dedupe where supported.
- `EnqueueWithResult` and `PublishWithResult` return `PublicationResult{ID, Duplicate}`. A confirmed duplicate returns the original accepted ID. Ambiguous reservations return errors rather than claiming success.
- `EnqueueBatchWithResult` returns the accepted prefix. Batches are bounded by `MaxBatchJobs` (1,000), ordered and non-atomic. Inspect `BatchError.Index` and accepted results before retrying a failed batch.

Publication errors may be ambiguous: a broker can accept a message before the response is lost. Dedupe reduces duplicate publications; it does not replace durable handler idempotency.

## Transport selection and ownership

Prefer `Open(cfg, WithTransport(tr))` with a dedicated transport instance per bus. Registry construction is available but depends on imported drivers and configured default clients; see the [reference](reference/README.md#opening-a-bus).

| Driver | Storage and settlement | Main operational limits |
| --- | --- | --- |
| Memory | Process-local; atomic local transfer | Bounded, non-durable; cannot connect separate processes |
| Redis Streams | Persistence-dependent; atomic retry/DLQ transfer | Redis 6.2+, same-slot keys, capacity and retention configuration |
| NATS JetStream | Storage-dependent; publish before ACK | JetStream required; provision durable consumers and targeted event layout |
| Kafka | Broker-dependent; publish before source offset commit | Partition ordering; no advertised publication dedupe; transactional settlement unavailable |

Read driver guarantees through `eventbus.CapabilityProvider` and normalized `transport.Capabilities`, rather than assuming every backend supports every option. `transport.ErrNotSupported` is an actionable capability failure. See [driver setup and options](reference/README.md#driver-setup).

The bus closes its transport during shutdown. Application-owned injected broker clients must be closed **after** draining the bus; the existing [runtime example](../examples/eventbus/internal/demo/runtime.go) demonstrates this ownership order and environment-based credentials.

## Lifecycle and operations

1. Open the bus; declare tasks/events and register local handlers.
2. Call `Start(startupCtx)` to provision resources and launch consumers. The context bounds startup only.
3. Publish and monitor `Ping`, topology/subscriber stats and scheduler health.
4. Call `Drain(shutdownCtx)` (or `Close`/`Stop`) with a fresh grace deadline. Intake stops while accepted work settles; expiry cancels workers and leaves uncertain sources unsettled where possible. Shutdown closes the transport.

Shutdown is terminal. The first caller owns the cleanup deadline; later callers join that cleanup with their own waiting contexts. Register EventBus startup in a short `foundation.Component.Start` callback; blocking servers/workers belong in `Component.Run` or adapters.

`Pause(ctx, name)` stops local intake. `DrainTopology` additionally waits for accepted work without closing the bus; `Resume` restarts it. These are process-local controls, not broker-wide maintenance locks.

Use `TaskRef(name)` or `EventRef(name)` for `TopologyStats` and DLQ operations; use `SubscriberStats` for an event group. Unknown lag/age measurements are distinguished by their `Known` flags.

DLQ workflow: count/list with cursor pagination, fix the underlying cause, then requeue selected **DeadLetter.ID** values. Do not use application message IDs. Requeue preserves the target subscriber for events. Purge is destructive and should be protected by application authorization. Kafka purge is logical, and requeue requires complete per-partition prefixes with administrators serialized across processes. See the [admin CLI](../examples/eventbus/README.md#statistics-and-dlq-administration).

`Config.Logger`, `Metrics` and `Tracer` accept framework-independent adapters. Optional runtime metrics cover reconnects, saturation, settlement, queue age, handler failures and shutdown. The [observability example](../examples/eventbus/observability/main.go) connects OpenTelemetry and the [Grafana dashboard](dashboards/grafana-eventbus.json).

## Migration and compatibility

- Update imports to `/v2`; use explicit `WithTransport` wiring rather than global client defaults.
- Declare topologies before attaching handlers and starting. Keep namespace, naming version and driver topology options consistent across all processes.
- Newly written envelopes are v3, with explicit topology kind and 1-indexed attempts. Legacy decoding is compatibility behavior, not a reason to hand-author envelopes.
- Broker resource names use collision-safe v2 encoding. `WithLegacyResourceNames()` is an explicit migration option, not an automatic rename or data migration. Plan consumption/drain of existing resources before switching.
- Targeted event layouts require explicit driver markers. Do not mix old broadcast-retry consumers with the targeted layout in one deployment namespace.
- Existing Kafka layout-marker topics with an incorrect partition count require an intentional migration; see the [example namespace notes](../examples/eventbus/README.md#quickstart).

## Verification

Run from the repository root:

```powershell
go run ./examples/eventbus/v2
go test ./eventbus/... ./examples/eventbus/...
go vet ./eventbus/... ./examples/eventbus/...
```

Untagged Redis tests use local test sockets; NATS/Kafka live paths need their integration suites. Check each integration test's broker prerequisites before enabling `-tags=integration`. See the [reference](reference/README.md) and repository `AGENTS.md` for additional commands.
