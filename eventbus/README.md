# EventBus

`eventbus` is a transport-neutral, at-least-once task and event delivery engine
for Go services. It provides one public API over bounded in-memory delivery,
Redis Streams, NATS JetStream, and Kafka while keeping each transport's real
durability, ordering, settlement, scheduling, and administration guarantees
explicit.

The package supports two different topology kinds:

- **Tasks** use competing consumers. Each delivery attempt is handled by one
  member of one worker group.
- **Events** use named fan-out subscriber groups. Every named subscriber group
  receives and settles its own logical delivery independently.

EventBus is not an exactly-once transaction coordinator. Every handler must be
idempotent because duplicate delivery is valid after crashes, lease expiry,
consumer rebalance, network ambiguity, or settlement retry.

## Features

- Explicit task and event topology declaration.
- Producer-only, consumer-only, and combined process layouts.
- Bounded worker pools and bounded per-consumer prefetch queues.
- At-least-once ACK, retry, and dead-letter settlement.
- Constant, exponential, and handler-selected retry delays.
- Delayed publication with transport-specific durable schedulers.
- Typed payload handlers and middleware.
- Handler timeout, panic recovery, lease heartbeat, and poison-message guards.
- Publication IDs and confirmed deduplication where supported.
- Broker ordering keys and separate process-local concurrency keys.
- Process-local rate limiting and keyed concurrency control.
- Non-atomic bounded batch publication with accepted-prefix results.
- Graceful bus drain and topology-level pause, drain, and resume.
- Statistics, DLQ administration, scheduler health, metrics, logging, and
  tracing hooks.
- Versioned, collision-safe broker resource names.
- Memory, Redis Streams, NATS JetStream, and Kafka drivers.

## Delivery Contract

The core contract is **at least once**:

- A durable driver reports publication success only after the broker confirms
  acceptance.
- A publication error means acceptance failed or is ambiguous. The caller must
  not assume that no message was accepted.
- A delivery has one terminal decision: ACK, retry, or dead letter.
- Retry and DLQ destinations are made durable before the source is acknowledged
  or committed.
- Failed settlement leaves the source eligible for redelivery where the broker
  permits it.
- Dedupe narrows producer duplicate windows but does not make handler side
  effects exactly once.
- Global ordering is not guaranteed. Ordering exists only within a scope a
  driver explicitly advertises.

## Architecture

```text
application
    |
    | DeclareTask / DeclareEvent / HandleTask / Subscribe
    v
eventbus.Bus
    |
    +-- encode and validate envelope
    +-- inject trace propagation
    +-- enforce payload and dedupe policy
    +-- run bounded consumer sessions and worker pools
    +-- classify handler outcome
    +-- request one ACK / retry / DLQ settlement
    |
    v
transport.Transport
    |
    +-- memory
    +-- Redis Streams
    +-- NATS JetStream
    +-- Kafka
```

The wiring path is:

```text
eventbus.Open
    -> explicit WithTransport, or registry lookup by Config.Driver
    -> engine
    -> eventbus/transport contract
    -> selected driver
```

Prefer explicit transport injection for deployment-oriented wiring. It makes
client ownership clear, avoids process-global default initialization order, and
allows multiple isolated buses in one process. EventBus can be adopted directly
for greenfield services. Production approval still depends on the selected
broker configuration and each deployment's capacity, fault, security, rollback,
and soak evidence.

## Installation

```bash
go get github.com/thanhbvha/go-common
```

The module uses the Go version declared in the repository's `go.mod`.

## Quick Start

The memory driver needs no external service and is suitable for tests and local
development. It is process-local and loses all queued, delayed, dedupe, and DLQ
state when the process exits.

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/thanhbvha/go-common/eventbus"
    "github.com/thanhbvha/go-common/eventbus/driver/memory"
)

type SendEmail struct {
    UserID string `json:"user_id"`
    To     string `json:"to"`
}

func sendEmail(context.Context, SendEmail) error {
    return nil
}

func run(ctx context.Context) error {
    cfg := eventbus.DefaultConfig()
    cfg.Namespace = "accounts"
    cfg.Concurrency = 8
    cfg.Prefetch = 16
    cfg.MaxAttempts = 5
    cfg.ProcessTimeout = 15 * time.Second

    bus, err := eventbus.Open(cfg, eventbus.WithTransport(memory.New()))
    if err != nil {
        return err
    }
    defer func() {
        drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()
        _ = bus.Drain(drainCtx)
    }()

    if err := bus.DeclareTask("send_email",
        eventbus.WithConcurrency(4),
        eventbus.WithPrefetch(8),
        eventbus.WithMaxAttempts(3),
    ); err != nil {
        return err
    }

    handler := eventbus.Handle(func(ctx context.Context, job eventbus.Job, payload SendEmail) error {
        if payload.UserID == "" || payload.To == "" {
            return eventbus.MarkPermanent(errors.New("invalid email request"))
        }
        return sendEmail(ctx, payload)
    })
    if err := bus.HandleTask("send_email", handler); err != nil {
        return err
    }

    if err := bus.Start(ctx); err != nil {
        return err
    }

    result, err := bus.EnqueueWithResult(ctx, "send_email", SendEmail{
        UserID: "user-123",
        To:     "demo@example.invalid",
    },
        eventbus.WithMessageID("email:user-123:welcome:v1"),
        eventbus.WithDedupeKey("welcome:user-123"),
        eventbus.WithDedupeTTL(24*time.Hour),
        eventbus.WithHeader("correlation-id", "example-correlation"),
    )
    if err != nil {
        return err
    }

    _ = result.ID
    _ = result.Duplicate
    return nil
}

func main() {
    if err := run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

Payload decoding is not business validation. Validate required fields,
authorization, tenancy, and ownership inside the handler before performing side
effects.

## Tasks And Events

### Task topology

A task has one logical worker group. Multiple service instances compete for the
same work:

```go
if err := bus.DeclareTask("invoice.generate"); err != nil {
    return err
}
if err := bus.HandleTask("invoice.generate", invoiceHandler); err != nil {
    return err
}
```

Producer-only processes declare the same task but do not attach a handler:

```go
if err := bus.DeclareTask("invoice.generate"); err != nil {
    return err
}
if err := bus.Start(ctx); err != nil { // provisions broker resources
    return err
}
if err := bus.Enqueue(ctx, "invoice.generate", payload); err != nil {
    return err
}
```

Calling `Start` in a producer-only process is recommended because it validates
the complete declaration plan and provisions required broker resources. A
declared topology may be published before `Start` only when the required broker
resources already exist; relying on that is fragile during fresh deployment.

### Event topology

An event is fan-out across named subscriber groups. Every group owns independent
delivery, retry, and DLQ state:

```go
options := []eventbus.EventOption{}
switch driverName {
case "redis":
    options = append(options, redisdriver.WithEventLayoutTargeted())
case "nats":
    options = append(options, natsdriver.WithEventLayoutTargeted())
case "kafka":
    options = append(options, kafkadriver.WithEventLayoutTargeted())
}

if err := bus.DeclareEvent("order.created", options...); err != nil {
    return err
}
if err := bus.Subscribe("order.created", "receipt", receiptHandler); err != nil {
    return err
}
if err := bus.Subscribe("order.created", "audit", auditHandler); err != nil {
    return err
}
```

Two instances using subscriber name `receipt` compete within the same logical
subscriber group. A `receipt` failure does not affect the `audit` group.

Memory events require at least one subscriber group provisioned in the same
process. A producer-only Memory bus cannot preserve an event for a future or
remote subscriber and returns `transport.ErrNotSupported`. Broker-backed
drivers support producer and subscriber processes separated in time or by
process, subject to their retention and provisioning configuration.

The built-in Redis, NATS, and Kafka drivers require their explicit
`WithEventLayoutTargeted` option for event topologies. This marker prevents an
older broadcast-retry layout from silently mixing with subscriber-targeted
retry and DLQ behavior during rolling deployments.

`Subscribe` can still implicitly declare an event for compatibility. New code
should always call `DeclareEvent` first so provisioning options are explicit and
consistent in every process.

### Declaration rules

- Declare topologies and attach handlers before `Start`.
- Equal repeated declarations are idempotent.
- Conflicting declarations return `ErrConflictingDeclaration`.
- One logical name cannot be both a task and an event in the same bus.
- Duplicate local task handlers and subscriber names are errors.
- Driver-specific options for the wrong transport are rejected.
- Declarations are frozen after startup begins.

## Opening A Bus

### Explicit transport injection

Explicit injection is the recommended production path:

```go
cfg := eventbus.DefaultConfig()
cfg.Namespace = "billing"

bus, err := eventbus.Open(cfg, eventbus.WithTransport(tr))
```

The bus owns the injected transport lifecycle and closes it during `Drain`,
`Stop`, or `Close`. Use a dedicated transport instance per bus. The built-in
Redis, NATS, and caller-owned Kafka constructors do not close the injected
application broker client, so close that client only after the bus has drained.

### Registry construction

Drivers register factories from `init`. Registry construction requires importing
the selected driver and configuring its package-level default client first:

```go
import _ "github.com/thanhbvha/go-common/eventbus/driver/memory"

cfg := eventbus.DefaultConfig()
cfg.Driver = "memory"
bus, err := eventbus.Open(cfg)
```

If a driver was not imported, `Open` returns an unknown-driver error. Redis and
NATS registry factories currently call their panic-on-missing default accessors,
so failing to configure those defaults can panic instead of returning a normal
startup error. Kafka uses a non-panicking lookup. Explicit injection avoids this
failure mode and is preferable for broker-backed applications.

## Driver Setup

### Memory

```go
import "github.com/thanhbvha/go-common/eventbus/driver/memory"

tr := memory.New()
bus, err := eventbus.Open(cfg, eventbus.WithTransport(tr))
```

Memory defaults to bounded capacities of 1,000 ready plus in-flight entries per
task or event subscriber group, 1,000 delayed entries per topology, and 1,000
DLQ records per topology. Capacity rejection is atomic and returns
`transport.ErrCapacityExceeded`; it does not silently drop or partially fan out
an event.

Use memory only when process-local loss is acceptable.

### Redis Streams

```go
import (
    commonredis "github.com/thanhbvha/go-common/redis"
    redisdriver "github.com/thanhbvha/go-common/eventbus/driver/redis"
)

clientCfg := commonredis.DefaultConfig()
clientCfg.Host = "redis.internal"
clientCfg.Port = 6379
client := commonredis.New(clientCfg)
if err := client.Connect(ctx); err != nil {
    return err
}

tr := redisdriver.New(client.Native())
bus, err := eventbus.Open(cfg, eventbus.WithTransport(tr))
// Drain bus before client.Close().
```

Redis requires Redis 6.2 or newer for `XAUTOCLAIM`. For durable operation use an
appropriate persistence and replication configuration and `maxmemory-policy
noeviction`. Ready streams are not lossy-trimmed; configure explicit ready,
delayed, and DLQ capacity bounds and monitor `ErrCapacityExceeded`.

Useful Redis topology options include:

```go
redisdriver.WithBatchSize(32)
redisdriver.WithBlock(time.Second)
redisdriver.WithAckWait(2 * time.Minute)
redisdriver.WithDelayBatchSize(200)
redisdriver.WithMaxReadyEntries(1_000_000)
redisdriver.WithMaxDelayedEntries(100_000)
redisdriver.WithMaxDLQEntries(50_000)
redisdriver.WithRetentionMinAge(time.Hour)
```

### NATS JetStream

```go
import (
    commonnats "github.com/thanhbvha/go-common/nats"
    natsdriver "github.com/thanhbvha/go-common/eventbus/driver/nats"
)

clientCfg := commonnats.DefaultConfig()
clientCfg.URLs = []string{"nats://nats.internal:4222"}
client := commonnats.New(clientCfg)
if err := client.Connect(ctx); err != nil {
    return err
}

tr := natsdriver.New(client.JetStream())
bus, err := eventbus.Open(cfg, eventbus.WithTransport(tr))
// Drain bus before client.Close().
```

Useful NATS topology options include:

```go
natsdriver.WithReplicas(3)
natsdriver.WithMaxAge(7 * 24 * time.Hour)
natsdriver.WithAckWait(2 * time.Minute)
natsdriver.WithBatchSize(32)
natsdriver.WithFetchTimeout(2 * time.Second)
natsdriver.WithDelayBatchSize(200)
natsdriver.WithDelayLease(30 * time.Second)
natsdriver.WithDuplicateWindow(2 * time.Minute)
```

JetStream stream, consumer, KV, replication, and storage policies determine the
real durability of the deployment.

### Kafka

```go
import (
    commonkafka "github.com/thanhbvha/go-common/kafka"
    kafkadriver "github.com/thanhbvha/go-common/eventbus/driver/kafka"
)

clientCfg := commonkafka.DefaultConfig()
clientCfg.Brokers = []string{"kafka.internal:9092"}
client := commonkafka.New(clientCfg)
if err := client.Connect(ctx); err != nil {
    return err
}

tr, err := kafkadriver.NewWithClient(client)
if err != nil {
    return err
}
bus, err := eventbus.Open(cfg, eventbus.WithTransport(tr))
// Drain bus before client.Close().
```

Use `kafkadriver.NewWithConfig` when the transport should own its producer.
EventBus requires broker-acknowledged production and rejects `acks=0` and
unsupported externally transactional producer configurations.

Useful Kafka topology options include:

```go
kafkadriver.WithPartitions(12)
kafkadriver.WithReplicationFactor(3)
kafkadriver.WithRetention(7 * 24 * time.Hour)
kafkadriver.WithFetchMaxWait(500 * time.Millisecond)
kafkadriver.WithFetchMinBytes(1)
kafkadriver.WithFetchMaxPartitionBytes(4 * 1024 * 1024)
kafkadriver.WithMaxBufferedRecords(1_000)
kafkadriver.WithDelayBatchSize(200)
```

Kafka preserves source ordering only within one physical topic partition.
Retries and delayed transfers use separate topics and may reorder relative to
later source records.

## Configuration

Start from `DefaultConfig()` and override measured workload requirements:

| Field | Default | Meaning |
| --- | ---: | --- |
| `Namespace` | `eventbus` | Isolates broker resources between applications or environments. |
| `Driver` | empty | Registry driver name; unnecessary with `WithTransport`. |
| `ProcessTimeout` | `30s` | Handler execution deadline after admission waits complete. |
| `Concurrency` | `10` | Fixed worker count per local task or subscriber. |
| `Prefetch` | `20` | Deliveries buffered per local task or subscriber, excluding running handlers. |
| `MaxAttempts` | `5` | Total handler executions, including the first attempt. |
| `MaxPayloadBytes` | `1 MiB` | Limit for the complete serialized EventBus envelope. |
| `MaxTopologies` | `0` | Optional startup declaration bound; zero is unlimited. |
| `MaxSubscribers` | `0` | Optional startup subscriber bound; zero is unlimited. |
| `RetryPolicy` | exponential | Backoff from `1s`, factor `2`, capped at `1h`, with jitter. |
| `Logger` | no-op | Operational logger. |
| `Metrics` | no-op | Metrics hooks. |
| `Tracer` | no-op | Tracing and propagation hooks. |

New code should explicitly declare topologies and use `MaxAttempts`
terminology.

Capacity per local consumer session is approximately:

```text
running handlers = Concurrency
queued deliveries = Prefetch
broker in-flight bound ~= Concurrency + Prefetch
```

Transport libraries may have additional byte-level buffers. Size all limits
from payload distribution, handler latency, broker round trips, outage backlog,
and available memory rather than using defaults as production capacity claims.

## Topology Options

Portable options can be set on a task, event, or allowed subscriber override:

| Option | Scope | Behavior |
| --- | --- | --- |
| `WithConcurrency(n)` | local runtime | Overrides worker count. |
| `WithPrefetch(n)` | local runtime | Overrides bounded intake queue size. |
| `WithMaxAttempts(n)` | envelope/runtime | Overrides total handler attempts. |
| `WithMaxPayloadBytes(n)` | publication/runtime | Overrides complete envelope limit. |
| `WithRateLimit(rate, burst)` | local runtime | Token bucket for handler starts in this process. |
| `WithConcurrencyKeyLimit(n)` | local runtime | Limits simultaneous handlers sharing a nonempty concurrency key. |
| `WithDelayDisabled()` | provisioning/publication | Declares an immediate-only topology and omits delay infrastructure where supported. |

Provisioning options belong on `DeclareTask` or `DeclareEvent`. Subscriber
options are intentionally restricted to runtime fetch, admission, retry, and
payload controls; a subscriber cannot silently change topology identity or
broker resource layout.

## Typed Handlers And Middleware

Use `Handle[T]` to decode JSON into a strongly typed payload:

```go
handler := eventbus.Handle(func(ctx context.Context, job eventbus.Job, payload OrderCreated) error {
    if err := payload.Validate(); err != nil {
        return eventbus.MarkPermanent(err)
    }
    return applyOrder(ctx, job.ID(), payload)
}, auditMiddleware)
```

Middleware wraps in declaration order. The engine already recovers panics, so
`Recoverer` is mainly useful when invoking a handler outside the engine.

`Job` exposes:

- `ID()` for stable logical identity.
- `Type()` and `Kind()` for route metadata.
- `Attempt()` and `MaxAttempts()` for retry-aware behavior.
- `Decode(&value)` for manual payload decoding.
- `Header(key)` for propagation and synthetic correlation metadata.

The handler context carries the processing deadline and tracing context.
Handlers must pass it to database, HTTP, RPC, and other blocking calls. Go
cannot forcibly terminate a handler that ignores cancellation.

## Handler Outcomes

Handlers return ordinary errors:

| Handler result | Settlement |
| --- | --- |
| `nil` | ACK |
| `MarkSkip(reason)` | ACK, intentionally skipped |
| ordinary non-nil error | Retry, unless attempts are exhausted |
| `MarkRetryAfter(err, delay)` | Retry after the requested normalized delay |
| `MarkPermanent(err)` | Immediate DLQ |
| panic | Recovered and treated as retryable |
| processing deadline | Retryable timeout |
| invalid or oversized envelope | DLQ before user handler execution |

`MaxAttempts` counts total handler executions, not retries. With
`MaxAttempts = 3`, the handler can execute at attempts 1, 2, and 3; failure on
attempt 3 goes to DLQ without a fourth execution.

Retry delays are normalized to the range from immediate retry through 30 days.
A panicking custom retry policy is isolated and becomes an immediate retry with
a fixed error classification.

### Panic isolation

Handler and middleware panics are recovered inside the worker, classified as
`ErrHandlerPanic`, and follow the same retry and final-attempt DLQ rules as other
retryable failures. The worker remains available for later deliveries.

Panic boundaries also protect EventBus-owned consumer, heartbeat, settlement,
shutdown, observability, and built-in driver goroutines. Infrastructure panics
never reuse the handler retry classification:

- consumer and worker panics cancel the current session and leave uncertain
  sources unsettled for broker redelivery;
- heartbeat panic cancels processing and the owning consumer session, then
  suppresses settlement;
- settlement panic is not automatically repeated because broker outcome may be
  ambiguous;
- logger, metrics, and tracer panic is isolated from delivery behavior;
- repeated consumer panics stop the affected consumer and make `Ping` return an
  error matching `ErrBusDegraded`;
- synchronous transport panics return an error matching `ErrRecoveredPanic`.

Recovered diagnostics contain fixed classifications and bounded source
locations. Panic values, payloads, headers, and raw handler errors are not
stored. Goroutines owned internally by third-party broker clients remain outside
EventBus recovery boundaries.

## Scheduling

Schedule a task or event with one of:

```go
eventbus.WithDelay(30 * time.Second)
eventbus.WithRunAt(time.Now().Add(30 * time.Second))
```

Do not set both options on one publication. Durable drivers persist scheduled
work before returning success. Delivery is not earlier than the requested time,
subject to the driver's precision; later delivery is allowed during backlog or
outage.

| Driver | Reported precision | Scheduler model |
| --- | ---: | --- |
| Memory | `1ms` | One bounded process-local heap scheduler |
| Redis | `1s` | Bounded per-topology ZSET scheduler |
| NATS | `1s` | Bounded per-topology durable KV scheduler |
| Kafka | `5s` | Three durable per-topology delay tiers (`5s`, `1m`, `5m`) |

Use `WithDelayDisabled` for topologies that will never publish or retry into the
future. Future publication and delayed retry are then rejected before broker
I/O.

## Publication Identity And Deduplication

Use result-returning publication methods when callers need stable reconciliation:

```go
result, err := bus.EnqueueWithResult(ctx, "payment.capture", payload,
    eventbus.WithMessageID(commandID),
    eventbus.WithDedupeKey(commandID),
    eventbus.WithDedupeTTL(24*time.Hour),
)
if err != nil {
    return err
}
if result.Duplicate {
    // result.ID is the original confirmed publication ID on built-in
    // dedupe-capable drivers.
}
```

`WithMessageID` sets logical identity but is not duplicate suppression by
itself. Combine it with `WithDedupeKey` when retrying the same business
publication.

Dedupe scope is namespace, topology kind, topology name, and dedupe key. Memory,
Redis, and NATS expose confirmed dedupe. Kafka does not. Requesting dedupe on an
unsupported transport returns `ErrDedupeUnavailable`. An unconfirmed or
cleanup-ambiguous reservation returns `ErrDedupeAmbiguous`; never treat it as a
successful duplicate.

The default dedupe TTL is 24 hours when a key is supplied without an explicit
TTL.

## Ordering And Concurrency Keys

The APIs deliberately separate broker ordering from process-local admission:

```go
eventbus.WithBrokerOrderingKey("account:123")
eventbus.WithConcurrencyKey("account:123")
```

- `WithBrokerOrderingKey` requires a driver that advertises broker ordering.
  Kafka supports partition-scoped ordering; Memory, Redis, and NATS reject it.
- `WithConcurrencyKey` works in the core and is enforced only inside one local
  task or subscriber session when `WithConcurrencyKeyLimit` is configured.
- `WithOrderingKey` is a compatibility helper that sets both keys.

Broker ordering does not provide exactly-once side effects. Kafka retry and
delay topics are separate physical logs, so retries may reorder relative to
later source records.

## Batch Publication

`EnqueueBatch` and `EnqueueBatchWithResult` accept at most 1,000 tasks:

```go
result, err := bus.EnqueueBatchWithResult(ctx, "search.index", []eventbus.BatchJob{
    {Payload: first, Options: []eventbus.JobOption{eventbus.WithDedupeKey("first")}},
    {Payload: second, Options: []eventbus.JobOption{eventbus.WithDedupeKey("second")}},
})
```

Batch publication is ordered, bounded, and intentionally non-atomic. On error:

- earlier items may already be durable;
- `result.Results` contains the accepted prefix;
- `BatchError.Index` identifies the first failed item;
- later items are not attempted.

Retry the failed suffix with stable message IDs and dedupe keys when duplicate
publication is unacceptable.

## Lifecycle And Shutdown

The lifecycle is:

```text
new -> starting -> running -> stopping -> stopped
```

`Start`:

- validates optional topology/subscriber planning limits;
- provisions every declared topology;
- provisions durable consumer identities where supported;
- starts fixed worker pools only for locally attached handlers/subscribers;
- can be retried after provisioning failure;
- returns `ErrAlreadyStarted` during startup or after successful start;
- returns `ErrBusClosed` after shutdown begins.

The context passed to `Start` bounds startup and provisioning only. Runtime
consumer sessions use bus-owned contexts.

`Drain`, `Stop`, and `Close` share one terminal shutdown path:

1. Stop local intake and consumer reconnect loops.
2. Let accepted deliveries finish handling and settlement.
3. If the first shutdown deadline expires, cancel workers and leave unfinished
   durable sources unsettled for redelivery.
4. Stop transport-owned schedulers and close the transport.

The first shutdown caller establishes the cleanup deadline. Concurrent later
callers share the same cleanup but can bound their own wait. `Close` also
releases a producer-only or never-started transport.

Recommended shutdown:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := bus.Drain(ctx); err != nil {
    return err
}
return brokerClient.Close()
```

## Pause, Drain, And Resume

Operational controls affect local consumers only:

```go
if err := bus.Pause(ctx, "invoice.generate"); err != nil {
    return err
}
if err := bus.DrainTopology(ctx, "invoice.generate"); err != nil {
    return err
}
if err := bus.Resume(ctx, "invoice.generate"); err != nil {
    return err
}
```

- `Pause` stops new local intake; accepted deliveries continue settling.
- `DrainTopology` pauses and waits for accepted local work to finish.
- `Resume` starts fresh local consumer sessions.
- For an event, the operation applies to every local subscriber group for that
  event.
- Producers, other application instances, and broker-wide intake continue.

Pause every affected instance or use broker-native controls when a global stop
is required.

## Statistics And DLQ Administration

Administration is exposed through `Bus` without retaining a concrete driver:

```go
taskStats, err := bus.TopologyStats(ctx, eventbus.TaskRef("invoice.generate"))
eventStats, err := bus.TopologyStats(ctx, eventbus.EventRef("order.created"))
auditStats, err := bus.SubscriberStats(ctx, "order.created", "audit")

page, err := bus.ListDeadLetters(ctx, eventbus.TaskRef("invoice.generate"), "", 100)
count, err := bus.CountDeadLetters(ctx, eventbus.TaskRef("invoice.generate"))
err = bus.RequeueDeadLetters(ctx, eventbus.TaskRef("invoice.generate"), []string{page.Records[0].ID})
err = bus.PurgeDeadLetters(ctx, eventbus.TaskRef("invoice.generate"))
```

`TopologyStats` fields are delivery obligations:

| Field | Meaning |
| --- | --- |
| `Pending` | Ready work not yet handed to a consumer. |
| `Delayed` | Scheduled work not yet ready. |
| `InFlight` | Delivered but unsettled work where measurable. |
| `DLQ` | Dead-letter records. |
| `Lag` | `Pending + InFlight`; excludes delayed and DLQ. |

Check `LagKnown` and `OldestAgeKnown`; unknown is not zero. Statistics are
near-point-in-time and differ by broker. Kafka reports all main lag as pending
because committed offsets cannot distinguish fetched from unfetched records.

Use `DeadLetter.ID`, not the application message ID, for requeue. Redis and NATS
support deletion-stable cursor pages. Kafka administration is append-only and
logical: purge advances an administrative cursor rather than deleting physical
log records, and requeue accepts constrained per-partition prefixes.

Unsupported optional operations return an error matching `ErrNotSupported`.

## Observability

Set `Config.Logger`, `Config.Metrics`, and `Config.Tracer` before `Open`.
Implementations are called synchronously from concurrent producers and workers,
so they must be concurrency-safe, bounded, and nonblocking.
EventBus isolates a panic from these hooks, but it cannot prevent a slow or
blocking implementation from reducing producer or worker throughput.

Core metrics hooks report:

- publication success, error, and confirmed duplicate;
- deliveries entering processing;
- handler duration;
- confirmed ACK, retry, and DLQ completion.

The optional `RuntimeMetrics` extension reports panic/timeout failures,
settlement latency and outcome, consume reconnects, intake saturation, sampled
queue age, and explicit stats observations.

Tracing operations are:

```text
eventbus.publish
eventbus.process
eventbus.settle.ack
eventbus.settle.retry
eventbus.settle.dlq
```

Implement `TraceInjector` to inject propagation headers and `TraceRecorder` for
sanitized fixed outcome events. The core does not select an observability vendor
or start a metrics server.

Durable drivers expose payload-free scheduler status through
`transport.SchedulerHealthReader`:

```go
if reader, ok := tr.(transport.SchedulerHealthReader); ok {
    statuses := reader.SchedulerHealth(topology)
    _ = statuses
}
```

## Driver Capability Matrix

| Capability | Memory | Redis | NATS JetStream | Kafka |
| --- | --- | --- | --- | --- |
| Durable across process restart | No | Deployment-dependent | JetStream storage-dependent | Broker/config-dependent |
| Tasks | Process-local | Yes | Yes | Yes |
| Event subscriber groups | Process-local | Yes | Yes | Yes |
| Delayed delivery | Process-local | Durable | Durable | Durable tiered scheduler |
| Delay precision | `1ms` | `1s` | `1s` | `5s` |
| Confirmed dedupe | Process-local | Yes | Yes | No |
| Broker ordering | No | No | No | Source partition |
| Retry/DLQ settlement | Process-local | Atomic same-slot Lua | Publish before source ACK | Publish before source commit |
| Lease heartbeat | No | Redis ownership renewal | JetStream `InProgress` | Offset/rebalance model |
| DLQ admin | Process-local | Yes, cursor pages | Yes, cursor pages | Yes, logical append-only cursor |
| Statistics | Process-local | Yes | Yes | Yes |
| Pause/resume | Local bus | Local bus | Local bus | Local bus |

No built-in driver claims exactly-once handler side effects. Broker durability
also depends on deployment configuration such as Redis persistence/replication,
JetStream storage/replicas, or Kafka acknowledgements/replication. Memory is not
production-durable. Redis, NATS JetStream, and Kafka implement the documented
durable contract and are available for greenfield deployment, but the library
does not certify an application's broker topology, infrastructure, capacity, or
operational readiness. Validate those conditions before production rollout.

## Resource Naming

Memory, NATS, and Kafka use a versioned injective `eb2` encoding of namespace,
topology kind, and logical name. Consumer groups additionally encode subscriber
identity. This prevents dots, underscores, slashes, Unicode, and words such as
`event` from aliasing another topology.

Redis uses a separate injective, cluster-slot-safe key layout.

## Security And Safety

- Never put credentials, tokens, personal data, or secrets in topology names,
  message headers, payload logs, metric labels, or tracing attributes.
- Headers are persisted and propagated. Use only bounded, synthetic correlation
  values and standard trace propagation fields.
- Validate payload content and authorization inside handlers; JSON decoding is
  not validation.
- Make handler side effects idempotent using a stable business key or message
  ID.
- Keep `MaxPayloadBytes` below the selected broker's record limit. The EventBus
  limit applies to the complete encoded envelope, including metadata and trace
  headers.
- Topology names, IDs, keys, headers, attempts, timestamps, and routes are
  validated before handler invocation.
- Oversized or malformed deliveries go to DLQ with fixed reason codes where
  settlement succeeds.
- Raw handler error text and panic values are not stored in logs, traces, or DLQ
  metadata. Fixed classifications are used instead.
- Panic stack logs contain bounded source locations without argument values.
- Protect DLQ list, requeue, purge, statistics, and broker administration behind
  application authorization.
- Use TLS and broker authentication in production. Plaintext local examples are
  not deployment defaults.

## Error Handling

Use `errors.Is` for public sentinel errors:

```go
switch {
case errors.Is(err, eventbus.ErrUnknownJobType):
    // Declare the topology or correct the name.
case errors.Is(err, eventbus.ErrDedupeAmbiguous):
    // Reconcile by stable message/business identity; do not assume failure.
case errors.Is(err, eventbus.ErrNotSupported):
    // Capability is unavailable on the selected driver.
case errors.Is(err, transport.ErrCapacityExceeded):
    // Apply backpressure and alert on broker capacity planning.
}
```

Important errors include:

- `ErrUnknownJobType`
- `ErrAlreadyStarted`
- `ErrBusClosed`
- `ErrBusNotRunning`
- `ErrTopologyKindConflict`
- `ErrConflictingDeclaration`
- `ErrDuplicateHandler`
- `ErrDuplicateSubscriber`
- `ErrUndeclaredTopology`
- `ErrInvalidConfig`
- `ErrInvalidName`
- `ErrInvalidOption`
- `ErrPayloadTooLarge`
- `ErrDedupeUnavailable`
- `ErrDedupeAmbiguous`
- `transport.ErrCapacityExceeded`
- `transport.ErrLeaseLost`

## Custom Transports

A custom driver implements the minimal `transport.Transport` contract:

```go
type Transport interface {
    Producer
    Consumer
    Provisioner
    HealthChecker
    Closer

    Name() string
    Capabilities() Capabilities
}
```

Required behavior includes:

- synchronous durable publication confirmation for durable drivers;
- synchronous consume callbacks with bounded backpressure support;
- joined session-owned goroutines before `Consume` returns;
- exactly one successful `Delivery.Settle` decision;
- source redelivery eligibility after failed settlement;
- topology provisioning and health checks;
- complete transport cleanup within the caller context.

Optional features are exposed only through interfaces such as:

- `SubscriptionProvisioner`
- `PublicationDeduperWithResult`
- `StatsReader` and `SubscriptionStatsReader`
- `DLQAdmin` and `CursorDLQAdmin`
- `SchedulerHealthReader`
- `TopologyValidator`

Do not implement unsupported features as success-returning stubs. Return
`transport.ErrNotSupported` or omit the optional interface.

Drivers may register a factory for `Config.Driver` lookup:

```go
func init() {
    registry.Register("example", func() (transport.Transport, error) {
        return newExampleTransport(), nil
    })
}
```

Use the reusable suites under `eventbus/transport/contracttest` and
`eventbus/contracttest` before advertising a capability.
