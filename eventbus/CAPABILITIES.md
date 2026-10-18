# EventBus transport capability matrix

Status: baseline audit for Phase P0.4. Update this document whenever a driver
capability changes. A capability may be advertised by code only after its
contract tests pass.

Legend:

- **Yes**: implemented and suitable for the current contract.
- **Partial**: primitive exists, but the EventBus contract is not yet met.
- **No**: unsupported or intentionally disabled.
- **Test only**: behavior exists but is non-durable and intended for tests.

## Current implementation audit

| Capability | Memory | Redis | NATS JetStream | Kafka |
|---|---|---|---|---|
| Durable acceptance | No | Yes | Yes | Yes |
| Work-queue delivery | Test only | Yes | Yes | Partial |
| Event fan-out groups | Test only | Yes | Yes | Partial |
| Delayed delivery | Test only, process-local | Partial, ZSET sweeper | Partial, KV sweeper | Partial, durable bounded tier sweeps (P5.6) |
| Reported delay precision | 1 ms | 1 s | 1 s | 5 s |
| Ordering | No | No | No | Yes, within a source partition |
| Dedupe | Test only | Partial | Partial | No |
| Heartbeat/lease extension | No-op | Yes, ownership-checked idle renewal (P3.5) | Yes (`InProgress`) | Not applicable in the same model |
| Poison delivery count | Process-local | Yes, XPENDING count preserved on direct reclaim | Yes | Envelope-derived only |
| Atomic retry settlement | No | Yes, same-slot Lua (P3.3) | No | No |
| Atomic DLQ settlement | No | Yes, same-slot Lua (P3.3) | No | No |
| Confirmed publish-before-commit retry/DLQ | N/A | Atomic Lua | Yes | Yes, synchronous fallback (P5.5) |
| DLQ write | Test only | Yes | Yes | Yes |
| DLQ list/requeue/purge/count | Test only | Yes, cursor + atomic requeue (P3.6) | Yes, cursor + publish-before-delete requeue (P4.5) | Yes, logical append-only cursor (P5.7) |
| Queue stats | Test only | Yes | Yes | Yes |
| Pause/resume | No | No | No | No |
| Payload size enforcement | No | No | No | No |
| Production readiness | No | No | No | No |

`Partial` is not an advertised guarantee. It records implementation work that
can be reused after the relevant phase satisfies contract tests.

## Current code-level advertised capabilities

Required broker operations are exposed by `transport.Transport`. Optional
operations are advertised only by implementing `transport.Deduper`,
`transport.DLQAdmin`, `transport.CursorDLQAdmin`, `transport.StatsReader`, or
`transport.Pauser`; unsupported drivers no longer provide success-returning
methods.

| Driver | Deduper | DLQAdmin | StatsReader | Pauser |
|---|---:|---:|---:|---:|
| Memory | yes | yes | yes | no |
| Redis | yes | yes | yes | no |
| NATS | yes | yes | yes | no |
| Kafka | no | yes | yes | no |

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
The P5.6 scheduler uses three durable delay topics and exactly one periodic
worker per tier/topology. Each sweep polls at most `kafka.delay_batch_size`
(default 100, range 1–1000); records that still belong in their current tier are
rewound instead of retained by process-local timers. Lower-tier/main publication
is broker-confirmed before a synchronous source commit. Publish/commit ambiguity
can duplicate but cannot intentionally lose work. Ordering keys and headers are
preserved, and startup rejects unequal partition counts across main, DLQ, and
delay topics. See [SCHEDULER.md](driver/kafka/SCHEDULER.md).

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

## Target capability declarations

Optional Go interfaces answer whether an operation exists. Later transport
phases should enrich the descriptive capability metadata with guarantee levels
similar to:

```go
type Durability uint8
type OrderingScope uint8
type SettlementGuarantee uint8

type Capabilities struct {
    Durability            Durability
    Delay                 bool
    DelayPrecision        time.Duration
    Ordering              OrderingScope
    Dedupe                bool
    Heartbeat             bool
    AtomicRetrySettlement SettlementGuarantee
    AtomicDLQSettlement   SettlementGuarantee
    DLQAdmin              bool
    Stats                 bool
    PauseResume           bool
    MaxPayloadBytes       int64
}
```

Suggested enum semantics:

- Durability: `NonDurable`, `BrokerDurable`.
- Ordering: `None`, `Partition`, `OrderingKey`.
- Settlement: `None`, `PublishBeforeAck`, `Atomic`, `Transactional`.

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

- Remains explicitly non-durable.
- Implements routing and settlement state-machine behavior for fast tests.
- Does not silently drop on full buffers; returns a typed capacity error.

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
