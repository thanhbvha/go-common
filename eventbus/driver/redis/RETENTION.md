# Redis ready-stream retention design

Status: implemented by P6.7.1R. Production-scale 100k/1m evidence remains a
separate P6.7.6 release gate.

This design replaces unsafe publish-time `MAXLEN` trimming with bounded,
consumer-group-aware retention. It targets Redis 6.2 and later. Redis 8.2
consumer-group-aware deletion may be added as an optimized path, but it is not
the correctness baseline.

## Problem and invariants

`XACK` removes a delivery from one consumer group's pending-entry list; it does
not remove the entry from the stream. The current driver therefore preserves
accepted work, including work owed to an offline event subscriber, but the ready
stream can grow without bound.

Retention must preserve these invariants:

1. A successful publication remains observable by every subscriber group that
   was durably provisioned before that publication.
2. An entry that is unread or pending in any active relevant group is never
   trimmed.
3. Missing, inconsistent, or concurrently changing group metadata blocks
   trimming; it never makes trimming more aggressive.
4. Storage pressure produces observable backpressure, not silent deletion.
5. Ordinary shutdown, deployment downtime, or a temporarily absent consumer
   does not retire its durable group.
6. Retention work is bounded per pass and does not add a broker command to every
   acknowledgement.

Redis documents that stream entries remain until explicitly deleted and that
consumer groups have independent cursors and pending-entry lists. `XTRIM MINID`
removes only entries below an ID threshold and is available in Redis 6.2. See
the Redis documentation for
[`XTRIM`](https://redis.io/docs/latest/commands/xtrim/),
[`XINFO GROUPS`](https://redis.io/docs/latest/commands/xinfo-groups/), and
[`XPENDING`](https://redis.io/docs/latest/commands/xpending/).

## Durable subscription provisioning

Retention cannot infer intended subscribers only from `XINFO GROUPS`. Today the
Redis group is created inside `Consume`, leaving a race in which a declared but
not-yet-running subscriber is invisible to a compactor.

Add an optional transport contract such as:

```go
type SubscriptionProvisioner interface {
    EnsureSubscription(context.Context, Subscription) error
}
```

`engine.Start` must perform these steps in order:

1. provision every declared topology;
2. provision the task `workers` group and every declared event subscriber
   group;
3. commit the engine to running;
4. launch consumer sessions.

For Redis, subscription provisioning atomically creates/reconciles the stream
group and registers it in a topology-scoped durable group manifest. Consumer
startup verifies the registration but does not become the first authority that
introduces the group. Direct transport callers must explicitly provision a
subscription, or `Consume` must use the same idempotent provisioning operation
before reading.

The manifest uses keys in the topology's existing Redis Cluster hash slot and
contains:

- the active task/event group names;
- a monotonically increasing manifest epoch;
- lifecycle state (`active` or `retiring`);
- registration time and layout version for diagnostics.

Task topology provisioning registers `workers`. Event groups remain registered
while their deployment is offline. Normal `Stop` or `Close` never unregisters a
group. A new event subscriber is guaranteed only publications accepted after
its provisioning succeeds; it may consume older entries that are still
retained, but that history is not a guarantee.

Group retirement is an explicit administrative operation. The safe path
requires zero pending and zero unread lag before removing the group and bumping
the manifest epoch. A forced retirement is destructive, must require an
explicit force flag, and must report the discarded pending/unread counts. An
EventBus-looking group found in Redis but absent from the manifest, or a
manifest group missing from Redis, blocks compaction until reconciled.

Existing deployments without a manifest are migration-blocked: the driver may
report candidates, but must not infer ownership and start trimming
automatically. Operators first reconcile the existing durable groups into the
manifest. This makes enabling retention on an old stream fail safe.

## Safe watermark

For every active relevant group, inspect `last-delivered-id` with `XINFO GROUPS`
and the oldest pending ID with `XPENDING` summary.

The conservative protected ID for a group is:

- its oldest pending ID when its pending list is nonempty;
- otherwise its `last-delivered-id`.

The topology safe watermark is the minimum protected ID across all active
groups. `XTRIM MINID` may remove entries strictly lower than this watermark;
the watermark entry itself is retained. This deliberately keeps at least one
extra acknowledged entry for the limiting group rather than risk crossing its
cursor.

A group at `0-0`, a missing group, a group with unavailable state, an empty
event manifest, or an unreconciled EventBus group produces a blocked result and
no trim. An optional minimum retained age can lower the watermark further, but
can never advance it beyond group progress.

Cursor progress and acknowledgements only move the safe boundary forward.
Group addition, retirement, or recreation changes the manifest epoch. A trim
pass reads an epoch with the group snapshot and applies its candidate through a
same-slot broker operation that rejects a changed epoch. Out-of-band
`XGROUP SETID`, group deletion, or recreation is unsupported for EventBus-owned
groups and should be prevented with Redis ACLs; detected drift blocks trimming.

## Bounded compaction

Use one transport-owned retention coordinator rather than one goroutine per
topology. It schedules registered topologies fairly and joins during
`Drain`/`Close`. Across application instances, a short topology-scoped lease
reduces duplicate work, but correctness does not depend on lease exclusivity.

Each pass:

1. reads the manifest epoch and active groups;
2. pipelines group state inspection;
3. computes the conservative watermark;
4. checks that the manifest epoch is unchanged;
5. atomically scans at most `batch + 1` eligible IDs and runs exact
   `XTRIM MINID <threshold>` for at most `batch` entries;
6. records the result and reschedules while eligible entries remain.

The exact threshold avoids Redis macro-node approximation indefinitely retaining
a small acknowledged backlog. The bounded scan chooses either the safe watermark
or the first untrimmed candidate, so one pass removes no more than the configured
batch. Group-count and per-cycle topology work are also bounded. P6.7.6 may tune
the defaults but must not weaken these safety properties.

Redis 8.2's `ACKED` reference policy or `XACKDEL` may be feature-detected and
benchmarked as a later fast path. It cannot replace durable group provisioning,
and mixed-version behavior must pass the same contract tests before activation.

## Capacity and backpressure

Safe retention cannot bound a stream while an active subscriber remains
offline forever. Every production topology therefore needs an admission limit
in addition to compaction.

Introduce new options with names that cannot be confused with lossy trimming:

- `redis.max_ready_entries`;
- `redis.max_delayed_entries`;
- `redis.max_dlq_entries`;
- `redis.retention_interval`;
- `redis.retention_batch_size`;
- `redis.retention_min_age`.

The ready-entry limit is checked with `XADD` in one atomic same-slot operation.
Delayed dispatch and DLQ settlement apply their corresponding limit before
removing or acknowledging the source. At capacity they leave the durable source
unchanged and return a stable typed capacity error containing only topology,
resource, current count, and limit. No payload or credentials are logged.

Capacity is a count bound, not a byte guarantee. Production guidance must also
require Redis `maxmemory` with `maxmemory-policy noeviction`, persistence and
replication appropriate to the durability claim, plus alerts before the broker
limit. An explicit unbounded mode may remain for compatibility, but it must be
machine-readable, documented as non-production, and never advertised as a
bounded-resource capability.

When ready capacity is reached because acknowledged entries await a scheduled
compaction pass, the driver signals the coordinator and returns retryable
backpressure; it does not perform an unbounded synchronous trim in the publish
path. Hysteresis and batch sizing should keep this transient window small.

## Observability

Expose per-topology metrics and structured health for:

- physical stream length and configured capacity;
- safe watermark and last successfully trimmed ID;
- entries trimmed per pass and cumulative total;
- pass duration, errors, retries, and last success time;
- active/retiring/missing/unmanaged group counts;
- the group and reason currently blocking retention;
- oldest retained, unread, and pending ages;
- ready, delayed, and DLQ capacity rejections;
- manifest epoch conflicts and lease contention.

Retention failure degrades readiness before capacity is exhausted. It must not
make liveness fail solely because a subscriber is intentionally offline. Logs
are rate-limited and contain broker IDs/counts only, never message payloads.

## Failure semantics

- A read/inspection/epoch error performs no trim and is observable.
- A timeout or cancellation stops the bounded pass without changing the
  publication result that originally created the records.
- An ambiguous trim response is safe to retry because trimming below the same
  watermark is idempotent.
- A stale watermark remains conservative while supported group cursors advance;
  any manifest change rejects it before application.
- Compactor crashes do not affect delivery. Another instance may acquire the
  optimization lease and resume.
- Capacity rejection happens before source acknowledgement or removal, so it
  creates backpressure without silent loss.

## Implementation sequence

1. Add the subscription-provisioning contract and provision all groups before
   consumers start.
2. Add the durable manifest, reconciliation report, and explicit safe/forced
   retirement operation.
3. Implement watermark calculation as a pure, property-tested component.
4. Implement the epoch-checked, bounded Redis 6.2 compactor and its single
   transport-owned scheduler.
5. Apply atomic admission limits to immediate publish, delayed movement,
   retry, DLQ, and requeue paths.
6. Add metrics, health degradation, examples, migration guidance, and operator
   alerts.
7. Benchmark and freeze defaults; optionally evaluate the Redis 8.2 fast path.

## Verification and acceptance

Deterministic tests must cover task and event topologies with fast, slow,
offline, pending, missing, newly registered, retiring, and forced-retired
groups. Include manifest-epoch races, concurrent ACK/publish/compaction,
capacity races, cancellation, ambiguous responses, wrong Redis types, scheduler
shutdown, and direct transport use.

Live standalone and Redis Cluster tests must prove:

- no unread or pending entry is removed for any active group;
- all eligible acknowledged entries are eventually reclaimed;
- adding a group during a pass invalidates the stale candidate;
- an offline group blocks trimming and eventually causes typed backpressure at
  the configured bound rather than memory growth or message loss;
- delayed/retry/DLQ capacity failure leaves the source recoverable;
- multiple instances compact safely without `CROSSSLOT` or duplicate-work
  storms;
- restart/failover preserves the manifest, group obligations, and progress.

P6.7.6 must run 100,000- and 1,000,000-message cases with multiple event groups
and record publish/ACK latency, compaction throughput, Redis CPU, memory high
watermark and post-compaction plateau. Phase 6.7 cannot claim bounded Redis
resource use until these gates pass.
