# EventBus transport capability matrix

Status: Phase 6.7 capability contract. Update this document whenever a driver
capability changes. A capability may be advertised by code only after its
contract tests pass. The production-readiness row remains `No` until Phase 7
migration, rollback, soak, and deprecation gates pass.

Legend:

- **Yes**: implemented and suitable for the current contract.
- **Partial**: primitive exists, but the EventBus contract is not yet met.
- **No**: unsupported or intentionally disabled.
- **Test only**: behavior exists but is non-durable and intended for tests.

## Current implementation audit

| Capability | Memory | Redis | NATS JetStream | Kafka |
|---|---|---|---|---|
| Durable acceptance | No | Yes | Yes | Yes |
| Work-queue delivery | Test only | Yes | Yes | Yes |
| Event fan-out groups | Test only | Yes | Yes | Yes |
| Delayed delivery | Test only, bounded process-local scheduler | Yes, bounded ZSET scheduler | Yes, bounded KV scheduler | Yes, durable bounded tier sweeps |
| Reported delay precision | 1 ms | 1 s | 1 s | 5 s |
| Scheduler health | Process-local snapshot | Yes | Yes | Yes, per tier |
| Ordering | No | No | No | Yes, within a source partition |
| Dedupe | Test only | Yes, confirmed publication state | Yes, confirmed publication state | No |
| Heartbeat/lease extension | No, returns only context cancellation | Yes, ownership-checked idle renewal (P3.5) | Yes (`InProgress`) | Not applicable in the same model |
| Poison delivery count | Process-local | Yes, XPENDING count preserved on direct reclaim | Yes | Envelope-derived only |
| Atomic retry settlement | No | Yes, same-slot Lua (P3.3) | No | No |
| Atomic DLQ settlement | No | Yes, same-slot Lua (P3.3) | No | No |
| Confirmed publish-before-commit retry/DLQ | N/A | Atomic Lua | Yes | Yes, synchronous fallback (P5.5) |
| DLQ write | Test only | Yes | Yes | Yes |
| DLQ list/requeue/purge/count | Test only | Yes, cursor + atomic requeue (P3.6) | Yes, cursor + publish-before-delete requeue (P4.5) | Yes, logical append-only cursor (P5.7) |
| Queue stats | Test only | Yes | Yes | Yes |
| Pause/resume | Process-local Bus control | Process-local Bus control | Process-local Bus control | Process-local Bus control |
| Payload size enforcement | Yes, core | Yes, core | Yes, core | Yes, core |
| Production readiness | No | No | No | No |

`Partial` is not an advertised guarantee. It records implementation work that
can be reused after the relevant phase satisfies contract tests.

## Current code-level advertised capabilities

Required broker operations are exposed by `transport.Transport`. Optional
operations are advertised only by implementing `transport.Deduper`,
`transport.PublicationDeduper`,
`transport.DLQAdmin`, `transport.CursorDLQAdmin`, `transport.StatsReader`, or
`transport.Pauser`. Redis, NATS, and Kafka additionally implement
`transport.SchedulerHealthReader`; unsupported drivers no longer provide
success-returning methods.

| Driver | Deduper | DLQAdmin | StatsReader | Pauser |
|---|---:|---:|---:|---:|
| Memory | yes | yes | yes | no |
| Redis | yes | yes | yes | no |
| NATS | yes | yes | yes | no |
| Kafka | no | yes | yes | no |

Memory, Redis, and NATS implement `PublicationDeduper`. A reservation is not a
successful duplicate until the driver records broker-confirmed publication.
Unconfirmed or cleanup-ambiguous state returns `ErrDedupeAmbiguous`; cleanup and
confirmation are bounded to five seconds. The older dedupe interfaces remain
source-compatible but cannot prove duplicate success.

Memory is a bounded process-local semantic reference, not a broker. Each task
group and declared event subscriber has a queue whose ready plus in-flight count
defaults to 1,000; delayed and DLQ stores also default to 1,000 entries per
topology. `memory.max_ready_entries`, `memory.max_delayed_entries`, and
`memory.max_dlq_entries` accept positive integer overrides through
`eventbus.WithOption`. Capacity failures are atomic and return
`ErrCapacityExceeded`; event publication never partially fans out. One joined
heap/timer scheduler owns all delayed work; `SchedulerStatus` exposes pending
count, last run/success/error, and retry count without payloads. Event groups are provisioned during
`Start`, retry and DLQ requeue return only to their source subscriber, and
session cancellation redelivers unsettled messages in process. All state,
dedupe, delays, and DLQ records disappear on process termination; Memory has no
cross-process coordination, persistence, broker ordering, or distributed lease.
An event with no process-local provisioned subscriber returns `ErrNotSupported`
instead of pretending a successful fan-out. Delivery heartbeat is likewise
unsupported rather than a successful no-op.

Memory, NATS, and Kafka physical resources use the injective `eb2` tuple naming
layout by default. `WithLegacyResourceNames` is an explicit migration mode and
retains the old collision risk; there is no automatic alias between layouts.
Redis keeps its independent injective, cluster-slot-safe key layout. Redis ready
streams are never trimmed during immediate or delayed publication, and the old
`redis.maxlen` option is rejected because it violates durable acceptance.
Redis instead provisions a durable intended-group manifest before consumer
startup and runs one joined, epoch-fenced compactor per transport. It trims only
bounded acknowledged prefixes below every active group's safe watermark.
Atomic ready/delayed/DLQ limits return `ErrCapacityExceeded`; an offline group
therefore causes visible backpressure rather than loss or unbounded entry growth.
Safe/forced retirement and payload-free `RetentionReport` are available through
the Redis-specific `RetentionAdmin` extension. Production deployment still
requires `maxmemory-policy noeviction` and appropriate persistence/replication.

Transport-level `Pauser` remains unadvertised. P6.1 instead provides portable
local intake controls on `Bus`: `Pause` cancels active consumer sessions while
allowing accepted work to settle, `DrainTopology` waits for that settlement,
and `Resume` starts fresh sessions. These controls affect only consumers in the
current process; they do not pause other instances or broker publication.

P6.2 bounds `EnqueueBatch` to 1,000 ordered, individually confirmed task
publications and reports the first partial failure. Consumer sessions remain
bounded by `Concurrency + Prefetch`: Redis caps both fresh reads and reclaim
claims to that capacity, NATS caps pull batches, and Kafka already caps poll
records. Settlement remains per-delivery and broker-confirmed; no driver claims
an atomic multi-message batch.

P6.3 adds process-local admission policies to each task or subscriber.
`WithRateLimit` applies a token bucket to handler starts, and
`WithOrderingKeyConcurrency` limits simultaneous handlers that share the same
nonempty ordering key. Different keys remain independent and empty keys bypass
the keyed limit. Policy waits retain the delivery lease, do not consume the
handler processing timeout, and are canceled during shutdown. These controls do
not coordinate quotas between processes and do not promote a driver's broker
ordering guarantee.

P6.4 enforces a 1 MiB default limit on the complete serialized envelope for
every driver. `Config.MaxPayloadBytes` sets the global limit and
`WithMaxPayloadBytes` overrides it per topology. Producers check the final wire
bytes after trace propagation but before dedupe reservation or broker publish.
Consumers check raw wire length before JSON decoding; oversized records are
settled to DLQ with reason `payload_too_large`. The core limit is independent of
and may need to be lower than a broker's own record-size limit.

P6.7.4 validates every decoded v3 envelope against the active task/event route
before invoking a handler. Built-in transports receive fixed, bounded DLQ
reason/error codes rather than raw caller error text, and independently enforce
a 2 MiB final encoded DLQ-record limit so hostile metadata cannot create an
unsettleable poison loop. Error rendering/wrapper traversal and custom retry
policies are panic-isolated; retry delays are normalized to zero through 30
days. These rules apply equally to direct transport settlement, preventing a
caller from bypassing core metadata safety.

The compact `transport.Capabilities` descriptor still exposes delay precision
and ordering for fast publish-path checks:

| Driver | DelayPrecision | Ordering |
|---|---:|---:|
| Memory | 1 ms | false |
| Redis | 1 s | false |
| NATS | 1 s | false |
| Kafka | 5 s | true |

Kafka P5.2 serializes delivery within each topic/partition through successful
settlement while allowing different partition heads into the engine worker pool
concurrently. Main-consumer polls are bounded to `Concurrency + Prefetch` records
and a whole batch settles before the next poll. Direct transport callers default
to concurrency 10 plus prefetch 20. Franz-go's additional internal fetch buffering
is byte-bounded by its fetch settings, not by this application record limit.
A slow partition can therefore delay the next batch for all partitions. A failed
settlement ends the consume session without dispatching later records from that
partition. Accepted handlers can still settle during graceful drain.

Kafka P5.3 disables auto-commit for main consumers and routes every handler
settlement through a session-owned offset coordinator. The coordinator tracks
the fetched order independently for each topic/partition, synchronously commits
only the highest consecutively settled record, and does not report settlement
success until Kafka confirms the commit. A higher completed offset waits while
any lower fetched offset remains unsettled; compacted offset gaps are valid.
Commit failure reopens settlement and never advances the coordinator. Delay
consumers also use synchronous manual commits under the P5.6 scheduler.

Kafka P5.4 gates group callbacks with `BlockRebalanceOnPoll`. A blocked callback
stops dispatching new records while already accepted handlers drain under the
engine's processing and settlement deadlines. The driver rewinds each retained
partition to its first polled-but-undispatched record before `AllowRebalance`,
preventing local fetch positions from skipping work. Revoked and lost partitions
are fenced in the coordinator, so stale deliveries cannot commit after ownership
changes. Main-consumer shutdown also keeps the coordinator alive through the
final revoke callback. Kafka therefore advertises source-partition ordering.
Retries can still reorder relative to later source records, as specified by
DS-52; this is not arbitrary side-effect or retry-chain ordering.

Kafka P5.5 explicitly selects synchronous publish-before-commit settlement.
Retry/DLQ production must be broker-confirmed before the contiguous coordinator
can commit the source; failed or ambiguous publication never requests a commit.
Failed/ambiguous commits and process crashes can duplicate the destination.
`acks=0` and externally transactional producers are rejected. Transactions are
not implemented or advertised; see [SETTLEMENT.md](driver/kafka/SETTLEMENT.md)
for the fallback choice, crash windows, and broker durability prerequisites.
The Kafka scheduler uses three durable delay topics and one worker per
tier/topology. Every worker polls on the five-second precision cadence and each
sweep reads at most `kafka.delay_batch_size` records (default 100, range
1–1000). A far-future partition head is broker-confirmed to the tail of the same
tier before its source commit, so a later near deadline is not indefinitely
blocked by append order. Lower-tier/main publication follows the same
publish-before-commit rule. Ambiguous commits can duplicate but cannot
intentionally lose work. The five-second precision assumes healthy scheduler
workers and at most one bounded poll ahead of the record; larger backlogs add
bounded sweep intervals and synchronous transfer time. Ordering keys and
headers are preserved, and startup rejects unequal partition counts across
main, DLQ, and delay topics. See [SCHEDULER.md](driver/kafka/SCHEDULER.md).

Redis, NATS, and Kafka expose payload-free scheduler status through
`transport.SchedulerHealthReader`. Snapshots include worker identity, poll and
success times, bounded last error and operation, monotonic poll/dispatch/
transfer/commit/retry counters, consecutive failures, and degraded state.
Redis `Close` now joins every topology delay sweeper and its retention worker
within the caller deadline; NATS and Kafka retain the same joined lifecycle.

Kafka P5.7 stores versioned DLQ wrappers with failure time/reason/error and
source topic/partition/offset/timestamp. `DLQAdmin` uses a dedicated consumer
group offset as a logical cursor because Kafka cannot remove arbitrary records:
requeue is confirmed publish-before-cursor-commit and accepts only complete
per-partition prefixes, while purge advances the cursor without physically
deleting log bytes. Both operations can duplicate after an ambiguous boundary
and must be serialized across administrator processes. `StatsReader` calculates
main and scheduler lag from end offsets and committed group offsets. Kafka
cannot derive the fetched/uncommitted split from those offsets, so all main lag
is reported as `Pending` and `InFlight` remains zero. See
[ADMIN.md](driver/kafka/ADMIN.md). P5.8 runs Kafka through the complete shared
contract; the dedupe scenario is capability-gated because Kafka does not
implement `TopologyDeduper`.

P6.7.5 exposes task/event topology stats, per-event-subscriber stats, and DLQ
list/count/requeue/purge through `Bus`, preserving optional-interface
`ErrNotSupported` without requiring concrete transport access. Kafka validates
replication and reconciles explicitly requested configs for all related topics,
rejects compacted queue logs, keeps ordinary startup partition-stable, and
provides explicit partition expansion with partial-result reporting. Kafka DLQ
mutations are fenced across processes through a one-partition consumer-group
operator lease.

All delivery implementations expose one terminal `Settle(Decision)` operation
guarded by the shared single-use state machine. A failed settlement reopens the
guard and leaves the source eligible for redelivery. This core guarantee does
not imply broker-atomic retry or DLQ settlement; those guarantees remain as
shown in the matrix and require the driver-specific transport phases.

Redis P3.3 uses a same-slot pending-check/destination-write/XACK Lua script.
Miniredis tests verify destination failures, missing source groups, retry after
failure, and concurrent stale deliveries. Live cluster crash/failover validation
remains P3.8; Lua isolation does not imply rollback or exactly-once effects.

Redis P3.6 implements `DLQAdmin` and deletion-stable `CursorDLQAdmin` pages
using stream IDs as opaque cursors. Requeue validates all selected records,
then atomically writes their original envelope bytes to the ready stream and
deletes the DLQ entries in one same-slot Lua invocation. Missing IDs are no-ops,
making response-ambiguity retries idempotent. Page and requeue batches are
bounded to 1,000 records.

Redis P3.7 implements `StatsReader` from the topology stream, consumer groups,
delayed sorted set, and DLQ stream. `Pending` is ready work not yet delivered;
`InFlight` is the Redis pending-entry-list total; `Lag` is their sum and excludes
delayed and DLQ records. Event stats aggregate every `subscriber:*` group, so a
single stream entry can contribute once per subscriber. A task stream without a
`workers` group is reported as entirely ready because its first consumer starts
at stream ID `0`. Counts are a near-point-in-time multi-command snapshot rather
than an atomic transaction. Redis 7 group lag is constant-time when available;
older servers use cancellable, 1,000-entry `XRANGE` pages over the ready backlog.

NATS P4.1 reconciles existing main/DLQ streams instead of treating an existing
name as successful provisioning. EventBus-owned subjects, storage, retention,
maximum age, and replica count are compared and compatible changes are applied
while unrelated stream limits are preserved. Durable pull consumers similarly
reconcile `AckWait` and set `MaxAckPending` to the effective core
`concurrency + prefetch` bound. Incompatible stream or consumer modes fail
startup explicitly; creation races use typed JetStream errors and re-inspection.

NATS P4.2 separates the EventBus logical message ID from ordering metadata.
Immediate publishes use the logical ID as JetStream's `Nats-Msg-Id`; scheduled
retry records use a schedule-specific broker ID so a retry inside JetStream's
duplicate window is not discarded. The logical ID travels in
`X-EventBus-Message-Id` and is restored on consume and DLQ publication.
Deduplication reservations are topology-scoped and stored durably with a
per-key expiry timestamp plus KV revision CAS. `WithDedupeTTL` selects the
reservation lifetime; zero retains the 24-hour compatibility default. Only a
confirmed live reservation returns duplicate: malformed state, KV errors, and
connection failures remain observable errors.

NATS P4.3 stores each delayed acceptance as an independent v2 KV record and
uses a durable second-slot cursor instead of retaining the bucket in an
in-process heap. Each pass publishes at most `nats.delay_batch_size` records
(default 100, maximum 1000). Multiple instances acquire records through KV
revision CAS with `nats.delay_lease` ownership (default 30 seconds), publish
with a stable schedule-specific broker ID, and delete only the claimed
revision. A process failure can therefore delay or duplicate delivery but does
not intentionally remove an unpublished record. Cursor recovery streams KV
metadata with bounded buffering and skips long empty downtime. See
[driver/nats/SCHEDULER.md](driver/nats/SCHEDULER.md) for layout and failure
semantics.

NATS P4.4 exposes each delivery as a lease with heartbeat cadence derived from
one third of the configured `AckWait`. The engine renews once before handler
execution and sends context-bounded JetStream `InProgress` requests throughout
long-running work. Retry settlement first creates an idempotent transfer marker
and delayed record keyed from the source stream/consumer/sequence; DLQ settlement first
receives a synchronous publish acknowledgement. Only then is the source ACKed
for retry or synchronously terminated for DLQ. Repeating settlement after an
ambiguous boundary reuses the retry transfer and stable broker message ID.
These are publish-before-ACK guarantees, not atomic transactions: duplicates
remain possible, while a destination failure leaves the source unsettled.

NATS P4.5 wraps every DLQ envelope with failure reason, error, attempts,
failure time, source stream, durable consumer, and source stream sequence.
`DLQAdmin` and deletion-stable `CursorDLQAdmin` use JetStream stream sequences
as administrative record IDs. Requeue waits for the main-stream publish
acknowledgement before deleting the DLQ record and uses a stable broker message
ID to constrain response-ambiguity duplicates. This is loss-safe
publish-before-delete, not an atomic transfer; duplicates remain possible after
the JetStream duplicate window. Lists and requeue batches are bounded to 1,000
records. `StatsReader` derives ready and in-flight obligations from consumer
`NumPending` and `NumAckPending`, counts scheduled KV records and DLQ stream
records separately, and treats a task stream without its durable consumer as
entirely ready. Event values sum all subscriber consumers and may exceed the
physical stream length. Reads are near-point-in-time; `OldestAge` is reported
only when it can be established without pretending retained acknowledged
history is outstanding work.

NATS P4.6 runs the reusable transport contract against an embedded,
file-backed JetStream server. Additional broker-level regressions cover
`AckWait` redelivery and attempt increments, `MaxAckPending` saturation and
resume, process-visible disconnect/reconnect with the same durable store,
multi-instance delay CAS, heartbeat renewal, and logical-ID duplicate publish
suppression. Together with the core runtime suite, these tests establish the
advertised NATS capabilities; they do not add an ordering or exactly-once
guarantee.

Kafka P5.1 introduces one immutable native-client factory for producer,
consumer, delay-consumer, and administrative roles. Explicit
`NewWithConfig`/`NewWithClient` construction carries broker addresses, client
ID, TLS, SASL, dial/request/produce/fetch/session timeouts, retries, and
producer durability settings into every role. The registry compatibility path
uses a non-panicking default lookup. The deprecated raw `New` adapter snapshots
the supplied franz-go client's effective options rather than rebuilding
consumers from broker addresses. This wiring work does not yet promote Kafka's
ordering, settlement, delay, DLQ administration, or statistics capabilities;
those remain assigned to P5.2-P5.8.

## Structured capability declarations

Optional Go interfaces remain authoritative for whether an operation exists.
`transport.Capabilities` now describes its guarantee and coordination scope.
Older custom transports that populate only `DelayPrecision` and `Ordering` are
normalized for compatibility.

```go
type DurabilityGuarantee uint8
type OrderingScope uint8
type SettlementGuarantee uint8

type Capabilities struct {
    Durability DurabilityGuarantee
    Delay      DelayCapabilities
    Order      OrderingCapabilities
    Settlement SettlementCapabilities
    Dedupe     DedupeGuarantee
    Heartbeat  CapabilityScope
    Admin      AdminCapabilities
    Scheduler  SchedulerCapabilities
    Payload    PayloadCapabilities

    // Deprecated compatibility mirrors.
    DelayPrecision time.Duration
    Ordering       bool
}
```

Implemented enum semantics include:

- Durability: none, process-local, or broker durable.
- Ordering: none, partition, or ordering-key scope, with process/broker
  coordination declared separately.
- Settlement: none, process-local, publish-before-source, atomic, or
  transactional, independently for retry and DLQ.
- Delay provisioning: unavailable, shared scheduler, per-topology scheduler, or
  tiered per-topology scheduler. Delay is topology-optional for built-in drivers.

Avoid a single `ExactlyOnce` flag. Broker settlement cannot make arbitrary
handler side effects exactly once.

## Promotion requirements

Before changing a matrix cell from Partial/No to Yes:

1. Implement the capability without success-returning stubs.
2. Add driver-specific tests for failure windows.
3. Pass the shared transport contract test where applicable.
4. Update driver documentation with guarantees and limitations.
5. Update this matrix in the same change.

## Expected target by driver

### Memory

- Remains explicitly non-durable and process-local.
- Passes every applicable shared transport contract scenario for routing,
  settlement/redelivery, delay, dedupe, DLQ administration, and statistics.
- Uses bounded atomic fan-out queues and one bounded delayed scheduler; it never
  returns success after dropping or partially accepting work.
- Is intended for tests, local development, and explicitly accepted ephemeral
  workloads, not as a substitute for Redis, NATS, or Kafka.

### Redis

- Broker-durable work queue and event groups.
- Cluster-safe atomic delay, retry, and DLQ settlement via Lua.
- Dedupe with caller TTL and topology-scoped keys.
- DLQ admin and stream/group stats.
- Heartbeat support only if a real lease-extension mechanism is implemented;
  otherwise reclaim policy must make the limitation explicit.

### NATS JetStream

- Broker-durable work queue and event groups.
- Durable KV scheduler for large delayed backlogs.
- Publish-before-ACK settlement constrained by message dedupe.
- Real `InProgress` heartbeat.
- DLQ admin and stream/consumer stats.

### Kafka

- Broker-durable work queue and subscriber groups.
- Partition-scoped ordering with a contiguous offset coordinator.
- Transactional retry/DLQ when configured; synchronous publish-before-commit
  fallback otherwise.
- Durable bounded delay scheduling.
- Lag stats and documented append-only DLQ administration limits.
