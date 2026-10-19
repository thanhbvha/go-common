# EventBus migration guide

This guide covers migration from `queue`, `queue_kafka`, and `queue_nats`, and
from the pre-refactor EventBus API. EventBus provides at-least-once delivery:
handlers must be idempotent and may receive a message more than once.

## Phase 6.7 resource-name migration

Memory, NATS, and Kafka now derive broker resources from a versioned `eb2`
encoding of the complete `(namespace, topology kind, logical name)` tuple.
Consumer groups additionally encode the subscriber name. The encoding is
injective for supported inputs: dots, underscores, slashes, the word `event`,
and Unicode cannot shift component boundaries or alias another topology.

This intentionally changes physical topic, stream, bucket, and consumer-group
names. EventBus does not silently alias, copy, drain, or delete v1 resources.
For each topology, either:

1. stop old producers, drain and account for ready/delayed/DLQ work, then deploy
   all v2 producers and consumers together; or
2. temporarily declare the topology with `eventbus.WithLegacyResourceNames()`
   while performing an explicitly supervised migration.

The legacy option preserves the old separator-joined names and their collision
risk. Never use it for two identities that collide, and do not assume legacy
and v2 deployments share a backlog. Keep old resources through the rollback
window, then remove them with broker-specific administration. Redis data keys
already use their separate injective v1 layout and are not renamed by this
change, although process-local scheduler identity now follows the selected
topology name.

## Phase 6.7 dedupe publication state

Built-in dedupe drivers now store an owned `reserved` state before publication
and change it to `published` only after the transport confirms durable
acceptance. Only `published` is a successful duplicate. A live unconfirmed
reservation returns `ErrDedupeAmbiguous`; it is never translated into success.
Publish-failure cleanup and post-publish confirmation use a five-second context
independent of caller cancellation. If either result is ambiguous, the publish
returns an error matching `ErrDedupeAmbiguous` so callers and telemetry can
alert or reconcile it. Reservation expiry remains the recovery bound.

Custom transports that expose only the legacy `Deduper`/`TopologyDeduper`
interfaces still compile, but a reservation hit returns `ErrDedupeAmbiguous`
because those interfaces cannot prove publication. Implement
`transport.PublicationDeduper` to restore confirmed duplicate suppression.

## Phase 6.7 Memory parity boundary

The Memory driver is now bounded and passes the shared transport scenarios that
apply to an in-process transport. Ready capacity counts queued plus in-flight
entries per task/subscriber group; event publication first validates every
declared group, then fan-outs atomically. Delayed work uses one joined heap/timer
scheduler instead of one goroutine per message. Retry and DLQ requeue preserve
the source event subscriber rather than broadcasting a second copy.
The concrete Memory transport's `SchedulerStatus` reports retained delayed
count, last run/success/error, and retry count for local tests and diagnostics.

Defaults are 1,000 ready entries per group, 1,000 delayed entries per topology,
and 1,000 DLQ records per topology. Override them with positive integer options:

```go
eventbus.WithOption("memory.max_ready_entries", 500)
eventbus.WithOption("memory.max_delayed_entries", 200)
eventbus.WithOption("memory.max_dlq_entries", 100)
```

Saturation returns `transport.ErrCapacityExceeded`; it does not drop or partially
fan out a message. Consumer-session cancellation redelivers unsettled entries as
long as the process remains alive. `Close` joins the scheduler and rejects new
publication, consumption, provisioning, and dedupe mutations; read-only stats,
DLQ list, and DLQ count remain available for final inspection.

Publishing a Memory event with no process-local provisioned subscriber returns
`transport.ErrNotSupported`; unlike a durable broker, Memory cannot preserve it
for a subscriber that may appear in another process or after restart. Heartbeat
also returns `ErrNotSupported` because there is no broker lease to extend.

These semantics do not make Memory durable. Process termination loses every
queue, delayed entry, dedupe reservation, and DLQ record. There is no restart
recovery, cross-process fan-out, distributed ordering, durable lease, or broker
availability guarantee. Use Memory for tests, local development, or workloads
whose process-local loss is explicitly accepted; use Redis, NATS, or Kafka for
durable deployment.

## Canonical API

Import the selected driver for its registry side effect, configure the bus,
declare every topology, attach local handlers, and then start consumption:

```go
import (
    "context"

    "github.com/thanhbvha/go-common/eventbus"
    _ "github.com/thanhbvha/go-common/eventbus/driver/kafka"
)

cfg := eventbus.DefaultConfig()
cfg.Driver = "kafka"
cfg.Namespace = "billing"

bus, err := eventbus.Open(cfg)
if err != nil {
    return err
}
defer bus.Stop(context.Background())

if err := bus.DeclareTask("send_email", eventbus.WithMaxAttempts(5)); err != nil {
    return err
}
if err := bus.HandleTask("send_email", func(ctx context.Context, job eventbus.Job) error {
    var payload struct{ To string `json:"to"` }
    if err := job.Decode(&payload); err != nil {
        return eventbus.MarkPermanent(err)
    }
    return sendEmail(ctx, payload.To)
}); err != nil {
    return err
}
if err := bus.Start(ctx); err != nil {
    return err
}
```

A producer-only process still declares its topology, but does not attach a
handler or call `Start`:

```go
if err := bus.DeclareTask("send_email"); err != nil {
    return err
}
if err := bus.Enqueue(ctx, "send_email", payload); err != nil {
    return err
}
```

Declare fan-out events before subscribing or publishing. Each subscriber name
identifies an independent consumer group:

```go
if err := bus.DeclareEvent("order_created"); err != nil {
    return err
}
if err := bus.Subscribe("order_created", "send_receipt", receiptHandler); err != nil {
    return err
}
if err := bus.Publish(ctx, "order_created", order); err != nil {
    return err
}
```

Handlers return ordinary `error` values. Use `MarkPermanent` for an immediate
DLQ decision, `MarkRetryAfter` for an explicit retry time, and `MarkSkip` for an
intentional acknowledgement. Returning any other non-nil error applies the
configured retry policy.

The core recovers panics from handlers and their middleware by default. A panic
becomes `ErrHandlerPanic`, follows the normal retry policy, and goes directly to
DLQ on the final permitted attempt. Worker goroutines remain available for later
deliveries. Panicking with a `PermanentError` does not classify it as permanent;
return `MarkPermanent(err)` explicitly for that outcome. Existing `Recoverer`
middleware remains supported and can also protect handlers invoked outside the
engine. An outer middleware may classify its returned `ErrHandlerPanic`.

Recovery logs at most 32 stack frames / 8 KiB of function names and source
locations, without argument values. The original panic value is not formatted,
logged, or stored in retry/DLQ error metadata. Recovery covers the handler's
calling goroutine; application-created goroutines need their own recovery.

## Compatibility adapters

Metrics/tracing now run throughout publish, processing, and settlement. Existing
`Metrics` and `Tracer` implementations still compile; extended hooks use optional
interfaces. `IncEnqueue` also receives `duplicate` for a skipped dedupe publish.
See [OBSERVABILITY.md](OBSERVABILITY.md) for hook semantics, trace injection,
stats availability flags, and concurrency requirements.

The following APIs remain available so existing applications can migrate
incrementally. New code should use the canonical replacement.

| Legacy API | Replacement | Compatibility behavior |
| --- | --- | --- |
| `Register(name, handler, opts...)` | `DeclareTask`, then `HandleTask` | Adapter performs both operations and preserves sentinel error identity. |
| `PublishEvent(ctx, name, payload, opts...)` | `Publish` | Adapter delegates directly to `Publish`. |
| `Subscribe` on an undeclared name | `DeclareEvent`, then `Subscribe` | Implicit event declaration remains temporarily supported. |
| `Config.AutoRegister` | Explicit declaration | Still supported, but deprecated. |
| `Config.MaxDeliveries` | `Config.MaxAttempts` | Legacy value is normalized to total attempts. |
| `WithMaxDeliveries(n)` | `WithMaxAttempts(n)` | Alias with total-attempt semantics. |
| `Job.GetAttempt()` | `Job.Attempt()` | Delegating adapter. |
| `Job.MaxRetry()` | `Job.MaxAttempts()` | Delegating adapter. |
| `Codec.Encode(...)` | `Codec.EncodeTopology(...)` | Encodes a task topology for compatibility. |

Continue to classify public failures with `errors.Is`; adapters preserve the
existing sentinels:

```go
if err := bus.PublishEvent(ctx, "missing", payload); errors.Is(err, eventbus.ErrUnknownJobType) {
    // Declare the event or correct the topology name.
}
```

## Breaking changes without adapters

- `Bus` now includes `Pause`, `Resume`, and `DrainTopology`. Implementations and
  mocks must add these methods. The controls are process-local: pause stops new
  intake for all local task/subscriber sessions of the named topology, topology
  drain waits for their accepted work to settle, and resume reconnects them.
  Publication and other application instances continue normally.
- `Bus.EnqueueBatch` accepts at most 1,000 `BatchJob` values. It preserves input
  order but is intentionally non-atomic: `BatchError.Index` identifies the first
  failed item, earlier items may already be durable, and later items are not
  attempted. Retry the failed suffix with idempotency or dedupe keys.
- `WithRateLimit(rate, burst)` and `WithOrderingKeyConcurrency(limit)` add
  process-local handler admission controls per task or subscriber. They require
  positive values. The rate is starts per second, the burst is the initial and
  maximum token capacity, and an empty ordering key bypasses keyed concurrency.
  Deployments needing a global quota must coordinate it outside EventBus; these
  options intentionally do not claim cross-instance ordering or rate limiting.
- Serialized envelopes are now limited to 1 MiB by default. Set
  `Config.MaxPayloadBytes` or the positive per-topology
  `WithMaxPayloadBytes` override when migrating known larger records, while
  keeping the value within the selected broker's limits. The measured bytes
  include EventBus metadata and injected trace headers, not only the user JSON.
  Oversized publishes return `ErrPayloadTooLarge` before dedupe reservation;
  oversized deliveries are sent to DLQ as `payload_too_large` before decode.

- Kafka EventBus main and delay consumers now own synchronous source commits;
  `ConsumerCommitInterval=0` is accepted and no longer controls their
  settlement. The standalone Kafka wrapper retains its configured behavior for
  consumers created directly by callers. Producer configuration now honors
  `ProducerIdempotent=false`; enabling it requires `ProducerAcks=-1`, and ACK
  values outside -1/0/1 are rejected. `kafka.New` snapshots its input broker
  slice and TLS config so later caller mutation cannot split producer and
  internal-consumer configuration. Nested TLS objects/callback state follow
  `tls.Config.Clone` semantics and must remain immutable.
- Kafka main consumers now hand only one unsettled record per partition to the
  engine at a time. Use multiple partitions and sufficient `Concurrency` for
  parallel handling. Each poll accepts at most `Concurrency + Prefetch` records;
  a slow record delays the next batch, and a settlement failure ends the session
  for supervisor reconnect. Direct transport consumers must settle accepted
  deliveries before receiving their partition's successor. Negative subscription
  bounds and integer overflow are rejected; omitted bounds default to 10/20.
  Cancellation stops intake while accepted handlers retain their settlement
  coordinator through the existing drain barrier. Main-consumer auto-commit is
  disabled: settlement waits for a synchronous, contiguous offset commit, and a
  higher completed offset never commits past a lower unsettled fetched record.
  During rebalance, new intake stops, accepted handlers drain under their normal
  deadlines, undispatched fetched records are rewound, and revoked/lost
  partitions are fenced before ownership is released. Kafka now advertises
  source-partition ordering; source ordering still does not imply retry-chain
  ordering.
- Kafka EventBus applications should prefer
  `kafkadriver.NewWithConfig(kafka.Config)` for an owned transport or
  `kafkadriver.NewWithClient(*kafka.Client)` for a connected caller-owned
  client. Both paths propagate the complete broker/security/timeout snapshot to
  internal main and delay consumers. The old `New(*kgo.Client, brokers)` remains
  as a deprecated adapter and now clones the native client's effective options;
  the broker slice is only an explicit seed override. Registry construction no
  longer calls the panic-on-missing `kafka.Default`; a missing or disconnected
  default returns an ordinary startup error. SASL protocols now reject a
  missing mechanism or credentials, and OAuthBearer requires a static token
  because token-URL exchange is not implemented.
- Kafka delayed delivery now uses durable `5s`, `1m`, and `5m` topic tiers with
  one bounded periodic worker per tier instead of a timer per record. Configure
  `kafka.delay_batch_size` from 1–1000 (default 100). Main, DLQ, and every delay
  topic must have the configured `kafka.partitions` count; startup rejects an
  existing mismatch before scheduling begins. The scheduler preserves envelope
  bytes, broker headers, and ordering keys, publishes before committing, and can
  duplicate after an ambiguous publish/commit boundary. Retention must cover
  maximum delay plus outage time. See the Kafka
  [scheduler guide](driver/kafka/SCHEDULER.md).
  New writes carry the deadline in a private transport header, allowing
  `PublishAt` to preserve arbitrary payload bytes; the scheduler still reads
  the previous envelope-embedded deadline during rolling upgrades.
- Kafka now advertises `transport.DLQAdmin` and `transport.StatsReader`. New DLQ
  entries are versioned wrappers containing failure and source Kafka metadata;
  older raw entries are not administrable through the new API. `DeadLetter.ID`
  is `<partition>:<offset>`. Because Kafka logs are append-only, requeue only
  accepts complete per-partition prefixes and advances a logical admin cursor
  after confirmed publication; purge advances that cursor without deleting
  physical bytes. Response ambiguity can duplicate requeues, and operators must
  serialize admin mutations across processes. Stats use end minus committed
  offsets; all main lag is `Pending` because Kafka offsets cannot expose the
  in-flight split. See the Kafka [admin guide](driver/kafka/ADMIN.md).
- EventBus jobs may now set `WithDedupeTTL`; the duration is persisted in the
  v3 envelope as `dedupe_ttl_ns`. Omitting it retains the former 24-hour
  default. A positive TTL without `WithDedupeKey`, or a negative TTL, is now an
  invalid option.
- NATS dedupe reservations now use the durable, topology-scoped
  `eventbus_dedupe_v2` KV bucket with per-reservation expiry and CAS. The old
  global `eventbus_dedupe` bucket is not migrated or read; during rollout, a
  key reserved only by an old process may be accepted once by a new process.
  Drain mixed-version producers when that duplicate window is unacceptable,
  then remove the legacy bucket according to your broker retention policy.
  NATS ordering keys no longer populate `Nats-Msg-Id`; distinct message IDs
  sharing an ordering key are no longer collapsed. Consumers restore the
  logical ID from `X-EventBus-Message-Id`, falling back to `Nats-Msg-Id` for
  records produced before this change.
- NATS delayed storage now uses collision-free `v2.<second>.<run-at>.<uuid>`
  KV keys and wrapped scheduling records instead of raw payloads under
  `<run-at>_<message-id>`. The scheduler migrates valid legacy entries with a
  create-before-delete transfer, preserving payload and logical ID. Do not
  delete the topology delay bucket during rollout. `nats.delay_batch_size`
  controls successful publications per pass (default 100, range 1–1000), and
  `nats.delay_lease` controls multi-instance claim recovery (default 30s).
  Delayed transfer remains at-least-once: a crash after broker publish but
  before KV deletion can redeliver after the lease and broker duplicate window.
  See [driver/nats/SCHEDULER.md](driver/nats/SCHEDULER.md).
- NATS deliveries now send `InProgress` at one third of consumer `AckWait`,
  including a renewal before handler execution. Set `nats.ack_wait` high enough
  for broker round trips; heartbeat failure cancels handling and deliberately
  leaves the source unsettled. Retry/DLQ now use source-sequence-derived
  transfer IDs and wait for durable destination acknowledgement before source
  ACK/Term. Transfer identity includes the durable consumer, keeping event
  subscriber outcomes isolated. DLQ uses terminal acknowledgement after transfer, so records that
  previously relied on ordinary ACK redelivery behavior will no longer be
  redelivered after successful dead-letter publication. The guarantee remains
  at-least-once, not atomic or exactly-once.
- NATS now advertises `transport.DLQAdmin`, `transport.CursorDLQAdmin`, and
  `transport.StatsReader`. New DLQ entries are versioned wrappers that preserve
  the original envelope plus failure and JetStream source metadata; DLQ entries
  written by the earlier raw-payload format cannot be listed or requeued by the
  new admin API and should be drained before upgrading. Use `DeadLetter.ID`
  (the DLQ stream sequence), not the logical message ID, for requeue. Requeue is
  publish-before-delete and duplicate-prone rather than atomic. Stats represent
  per-consumer delivery obligations, so event lag can exceed retained message
  count, while delayed and DLQ values remain separate.
- NATS topology startup now reconciles existing main/DLQ stream configuration
  and durable pull consumers. Compatible changes to EventBus-owned stream
  fields, `AckWait`, and `MaxAckPending` are applied; incompatible storage or
  consumer modes fail startup instead of being silently accepted. Consumer
  `MaxAckPending` is now the effective topology `concurrency + prefetch`, not a
  hard-coded 20,000. Review capacity and redelivery behavior before rollout.
- Redis heartbeat now renews only the delivery's current consumer ownership;
  lease loss returns `transport.ErrLeaseLost`. The engine renews before execution
  and cancels processing on renewal failure, leaving the source unsettled.
  Configure `redis.ack_wait` above prefetch/batch waiting time plus operational
  margin. Renewal does not fence external handler side effects; keep handlers
  idempotent. See the Redis key-layout guide for the full lease policy.
- Redis reclaim now requires Redis 6.2+ (`XAUTOCLAIM`) and returns command errors
  through the consume supervisor. Reclaim preserves stream identity/delivery
  history; `redis.block` is capped at two seconds for reclaim progress. See
  [KEY_LAYOUT.md](driver/redis/KEY_LAYOUT.md#reclaim) for idle/heartbeat policy.
- Redis EventBus now uses the versioned, encoded topology key layout in
  [driver/redis/KEY_LAYOUT.md](driver/redis/KEY_LAYOUT.md). Stream/delayed/DLQ/
  metadata/dedupe keys share a cluster hash tag and dedupe is topology-scoped.
  Drain/migrate old keys before switching; existing backlogs and consumer groups
  are not automatically discovered by the new driver.
- Redis `redis.delay_batch_size` is now bounded to 1–1000 (default 100).
  Neither immediate nor delayed publication trims the ready stream. The former
  `redis.maxlen`/`WithMaxLen` behavior could remove accepted work ahead of an
  offline task worker or slow event subscriber and is now rejected during
  topology validation. Redis now provisions every intended `workers` or
  `subscriber:*` group into a durable manifest before consumer startup. A
  transport-owned coordinator trims only IDs strictly below the conservative
  watermark of every active/retiring group, with manifest-epoch fencing across
  instances. Configure `redis.max_ready_entries`,
  `redis.max_delayed_entries`, and `redis.max_dlq_entries`; saturation returns
  `transport.ErrCapacityExceeded` before acceptance or source settlement.
  `RetireSubscription` is explicit: safe retirement requires no pending/unread
  work through its barrier, while forced retirement reports the work abandoned.
  `RetentionReport` exposes capacity, watermark, trim/error/conflict counters,
  blocking groups, and readiness. Keep `maxmemory-policy noeviction`; count
  limits do not replace Redis memory, persistence, or replication planning. See
  [RETENTION.md](driver/redis/RETENTION.md).
- Redis retry/DLQ now use same-slot Lua settlement. Stale sources return
  `transport.ErrDeliverySettled` without publishing another copy. DLQ settlement
  no longer applies `redis.dlq_maxlen`; records remain until explicitly managed.
  Retry deadlines already in the past still pass through the delayed sweeper.
- Redis now advertises `transport.DLQAdmin` and
  `transport.CursorDLQAdmin`. Prefer `ListPage` with its opaque Redis stream-ID
  cursor when requeue can happen during a scan; positional `List` is retained
  for compatibility. Requeue identifiers are `DeadLetter.ID` admin record IDs,
  not envelope `DeadLetter.Message.ID` values. Requeue is a bounded same-slot
  Lua transfer to the ready stream and is idempotent for repeated IDs. Purge is
  topology-scoped and irreversible. See the Redis key-layout guide for limits
  and durability details.
- Redis now advertises `transport.StatsReader`. `Pending` counts ready delivery
  obligations, `InFlight` counts pending-entry-list records, and `Lag` is their
  sum; delayed and DLQ counts remain separate. Event values aggregate all
  `subscriber:*` groups, so they can exceed physical stream length. Task streams
  without a `workers` group are treated as entirely ready. Reads are
  near-point-in-time rather than atomic; Redis 6.2-compatible servers may scan
  the ready backlog in cancellable pages when Redis 7 lag metadata is absent.
  See the Redis key-layout guide for full semantics and polling cost.
- Successful envelope decoding now makes its ID, type, ordering key, dedupe key,
  and headers authoritative over missing or stale broker metadata. Retry uses
  these restored fields. Custom delivery implementations should use
  `Decision.Source`, when non-nil, for DLQ source records; nil means decoding
  failed or a caller settled directly, so retain the raw delivery message.
  Source payload bytes remain unchanged. Never mutate the supplied metadata.
- Decoded v3 envelopes are now validated against the active consumer route
  before tracing or handler invocation. ID, exact topology kind/name, attempts,
  creation/schedule timestamps, payload JSON, dedupe metadata, ordering/dedupe
  key lengths, and bounded control-free headers are required. Invalid records
  go directly to DLQ with a fixed reason such as `topology_type_mismatch` or
  `invalid_headers`; the engine no longer overwrites a mismatched envelope kind.
  Legacy v2 decode compatibility remains, including records that predate the v3
  creation timestamp requirement.
- Handler error text is no longer copied to logs, traces, or DLQ records.
  `PermanentError`, `RetryAfterError`, and `SkipError` return fixed public
  messages and are nil-safe. `transport.Decision.ErrorCode` carries a fixed
  lower-case classification; direct transport callers that omit or provide an
  invalid code receive `delivery_error`. DLQ reason/error codes are limited to
  64 bytes and `[a-z0-9_.-]`, and every built-in driver rejects an encoded DLQ
  record above 2 MiB without acknowledging/removing the source. This is an
  intentional metadata compatibility change: applications must not parse raw
  handler text from `DeadLetter.LastError`.
- Caller-provided retry policies and error wrapper traversal are panic-isolated.
  Negative retry delays clamp to immediate retry; delays above 30 days clamp to
  30 days. A panicking policy produces an immediate retry classified as
  `retry_policy_panic`, preserving the at-least-once source rather than crashing
  a worker. `MarkRetryAfter` is normalized by the same bounds.
- `Bus` now includes `Drain(ctx)`. Implementations and mocks must add it.
  `Drain`, `Stop`, and `Close` share one terminal shutdown: stop intake, finish
  accepted/prefetched deliveries and settlement within the first caller's grace
  deadline, then close the transport. In-flight handler contexts and heartbeats
  remain active during that window (their normal processing timeout still
  applies). Deadline expiry cancels workers and settlement; unfinished sources
  remain unacknowledged for redelivery by durable brokers. Handlers must observe
  cancellation: Go cannot terminate a handler that ignores its context. A late
  handler result after force cancellation is not settled by the engine.
  Memory transport remains non-durable and does not guarantee redelivery.
  Custom consumers that own per-session settlement clients must invoke
  `Subscription.WaitForHandlers`, when non-nil, after stopping callbacks and
  before closing those clients. Kafka uses this barrier to keep its offset
  settlement client alive while draining. Brokers may still redeliver after
  lease expiry or group changes; handlers must remain idempotent.
- `Bus` now includes `Close(ctx)`. Implementations and mocks of `Bus` must add
  this method; `Stop(ctx)` remains available and shares the same shutdown path.
  Closing a producer-only or never-started bus now releases its transport too.
  `Start` can be retried after provisioning failure, returns `ErrAlreadyStarted`
  during startup/running, and returns `ErrBusClosed` after shutdown begins.
  Concurrent shutdown calls share one transport close and its result. The first
  caller supplies the cleanup deadline; later callers bound only their own wait.
- Custom transport implementations must implement the new single-use
  `Delivery.Settle` and `Delivery.Heartbeat` contract. The old Ack/Retry/DLQ
  methods cannot coexist safely because they allow contradictory settlements.
- New v3 envelopes use 1-based `attempt` and `max_attempts`. The codec upgrades
  v2 envelopes automatically, but code that reads raw envelope JSON must be
  updated.
- Statistics, DLQ administration, deduplication, and pause/resume are optional
  transport capabilities. A custom transport must implement the corresponding
  optional interface or callers receive `ErrNotSupported`.
- Public administration no longer requires retaining or type-asserting a
  concrete transport. Use `TopologyStats(TaskRef|EventRef)`, `SubscriberStats`,
  `ListDeadLetters`, `CountDeadLetters`, `RequeueDeadLetters`, and
  `PurgeDeadLetters`. The old `Stats(name)` remains a task-only compatibility
  adapter. Admin operations require a topology declared on that Bus and preserve
  `ErrNotSupported` when the selected driver lacks the optional capability.
- Kafka startup now reconciles replication and explicitly requested topic
  configs for main, DLQ, delay, and DLQ-admin-lock topics. Any cleanup policy
  containing `compact` is rejected because repeated ordering keys could delete
  accepted queue records. Startup never changes partition counts. Operators may
  call the Kafka driver's explicit `ExpandPartitions`; it expands main, DLQ, and
  all delay topics, rejects decreases, reports per-topic partial success, and
  requires an ordering-key remapping plan. Kafka DLQ requeue/purge acquire a
  broker-backed single-operator lease rather than relying only on a local mutex.
- Strict validation now rejects blank or padded names, nil handlers/options,
  invalid numeric values, conflicting schedule options, and driver options
  unsupported by the selected transport. Values that were previously ignored
  can now fail during declaration or publishing.
- Handlers return `error`. Older documentation that mentioned `Outcome`,
  `Success`, `Error`, or `Permanent` described APIs that do not exist; use the
  error helpers listed above.

## Migrating legacy queue applications

Kafka P5.5 uses synchronous publish-before-commit for retry/DLQ. Destination
failure leaves the source uncommitted; a crash or ambiguous broker response can
create duplicate destination records. Retain idempotent handlers. EventBus now
rejects `ProducerAcks=0` with `ErrNotSupported`; use the default `-1` (preferred)
or `1` with its weaker replication guarantee. The deprecated raw constructor
reports this error during validation/publish/consume. Raw transactional
producers are also rejected: transaction settlement is unavailable in this
version, with no automatic downgrade. See the Kafka
[settlement guide](driver/kafka/SETTLEMENT.md) for failure windows and scope.
Delayed retries use the bounded tier scheduler described in the Kafka
[scheduler guide](driver/kafka/SCHEDULER.md); drain inconsistent old related
topics before enabling it.

1. Make every handler idempotent using a stable business key or message ID.
2. Map legacy queue names to explicitly declared task names and move retry
   counts to total-attempt semantics.
3. Deploy EventBus consumers in shadow mode without real side effects and
   compare decode results, attempts, latency, failures, and DLQ outcomes.
4. Switch producers to EventBus behind a rollback flag. Avoid naive dual-write:
   partial publish failures can produce divergent queues.
5. Drain the legacy queues while their old consumers remain active.
6. Enable EventBus side effects, stop legacy consumers, and retain rollback
   capability through the agreed soak period.

Do not run old and new consumers with the same real side effects unless both
paths share a proven idempotency mechanism. Driver-specific topology options
must only be supplied when that driver is selected.
