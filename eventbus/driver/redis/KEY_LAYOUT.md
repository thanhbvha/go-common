# Redis EventBus key layout v1

The canonical prefix is:

```text
eventbus:v1:{<base64url(namespace)>:<numeric-kind>:<base64url(name)>}
```

Encoding uses unpadded URL-safe base64 independently for each string. Kind is
`0` for tasks and `1` for events. Empty namespace is encoded as an empty field.
Only the library adds braces and colon delimiters: names containing periods,
colons, Unicode, or braces cannot alter tuple boundaries or inject a hash tag.
Different topology identities may hash to the same Redis slot (normal hash
collisions), but never use the same key prefix.

| Resource | Suffix |
| --- | --- |
| Main stream | `:stream` |
| Delayed sorted set | `:delayed` |
| Subscriber retry stream | `:retry:<base64url(subscriber-group)>:stream` |
| Subscriber retry delayed set | `:retry:<base64url(subscriber-group)>:delayed` |
| Dead-letter stream | `:dlq` |
| Topology metadata hash | `:metadata` |
| Subscriber scheduler index | `:group-index` |
| Dedupe reservation | `:dedupe:<base64url(user-key)>` |

Every key above shares the same Redis Cluster hash tag. Future atomic delayed
dispatch and settlement scripts can operate on those keys without CROSSSLOT.
`EnsureTopology` persists `layout_version`, `namespace`, `kind`, and `name` in
the metadata hash before starting the topology scheduler. Event topologies also
persist the immutable `event_layout=targeted-v1` migration fence. Scheduler
identity is the canonical prefix, not the delimiter-joined logical display name.

Consumer groups belong to their stream: task group is `workers`, subscriber
groups are `subscriber:<subscriber-name>`. Event subscriptions provision the
same group on the shared event stream and their private retry stream. Dedupe scope is namespace + kind +
name + user key. Engine publications use `transport.PublicationDeduper`: values
are owned `reserved` records until broker acceptance is confirmed, then become
`published`; abort/confirm scripts compare the reservation ID before mutation.
A live reserved record is ambiguous, not a successful duplicate. Existing
`Deduper` direct calls keep the old unscoped `eventbus:dedupe:<key>` behavior
for compatibility; they cannot prove publication. The engine's 24-hour default
TTL remains unchanged.

## Migration from the old layout

This is a physical storage change. Old keys were `{<Topology.Name()>}:stream`,
`:delayed`, and `:dlq`, with unscoped dedupe keys. The new driver does not read,
rename, delete, or automatically copy them. The old consumer group state also
does not transfer automatically.

Before switching the driver version:

1. Stop old producers and let the old consumers/schedulers drain ready, delayed,
   and pending work. Account for and export/requeue DLQ records explicitly.
2. Check the old backlog and pending entries before retiring old consumers.
3. Switch producers and consumers together to v1. Reconcile any intentionally
   migrated records by stable envelope ID and use idempotent handlers.
4. Keep old keys intact for the chosen rollback window. New reservations are
   separate from legacy dedupe keys; do not expect old reservations to suppress
   v1 publication.

If draining is impractical, design and verify a durable migration separately.
Do not use RENAME across old/new keys in a cluster: their slots may differ.
Do not run mixed old/new producers and assume they share a queue. Existing
`queue`, `queue_nats`, and `queue_kafka` modules retain their own storage APIs.

## Targeted event retry migration fence

Subscriber-private event retry lanes require an explicit declaration with
`redis.WithEventLayoutTargeted()`. It sets the Redis-specific topology option
`redis.event_layout=targeted-v1`. Drivers predating the targeted layout reject
that unknown option during `ValidateTopology`, so they cannot join the same
declared event topology and silently broadcast retries.

`EnsureTopology` atomically verifies the canonical metadata identity and the
existing event marker before writing `event_layout=targeted-v1`. It never
replaces a different marker. Event publication, delayed publication, consume,
subscription provisioning, retry/DLQ settlement, delayed dispatch, capacity
reconciliation, retention, statistics, and DLQ administration fail closed when
the marker is absent or different. Task topologies do not use this option or
marker and retain their existing behavior.

For an existing event topology, deploy in this order:

1. Stop all old Redis EventBus processes for that topology. Confirm no old
   producer, consumer, scheduler, or administration process remains.
2. Upgrade every process and add `redis.WithEventLayoutTargeted()` to each event
   declaration. Do not add it to task declarations.
3. Run normal topology startup so `EnsureTopology` establishes the marker before
   publication, subscription provisioning, or administration begins.
4. Treat a missing or mismatched marker as an incomplete or conflicting
   migration. Do not manually overwrite a different marker; isolate or drain the
   old topology and resolve its storage semantics first.

Removing the option after migration is not a rollback: new drivers reject the
event declaration, while old drivers remain incompatible with private retry
lanes. Roll back only by stopping the targeted deployment and restoring a fully
isolated pre-migration topology and backlog.

Default tests verify exact hash-tag equality, tuple separation and scoped
dedupe using miniredis; they do not claim a live Redis Cluster integration run.

## Delayed dispatch

`PublishAt` stores the serialized envelope unchanged as a sorted-set member,
scored by Unix milliseconds. A once-per-second sweeper runs a single Lua move
with `redis.delay_batch_size` entries (default 100, allowed range 1–1000).
Each tick performs at most one batch; large backlogs can be later than their
deadline. Lua serializes concurrent instances on the broker; no distributed
lease or process-local ownership map is needed.

The script validates source/destination types before writing, selects only due
members, writes each exact envelope to the stream, then removes that member.
Redis Lua is isolated but does not roll back runtime errors: a failure never
removes a member before its destination write. Duplicates remain allowed after
network/failover ambiguity. Durability still depends on Redis persistence and
replication configuration; this is not an exactly-once guarantee.

Neither delayed dispatch nor immediate publication applies `redis.maxlen`:
trimming a ready stream can discard accepted work before every task/event group
observes it. `redis.maxlen` and `WithMaxLen` are retained only as compiling
compatibility entry points and are rejected by topology validation. Operators
must provision sufficient storage, use a no-eviction policy for queue keys, and
perform retention only after accounting for every relevant group.
P6.7.1R implements the mandatory durable group manifest, epoch-fenced safe
watermark, bounded transport-owned compactor, explicit subscriber retirement,
and atomic ready/delayed/DLQ admission limits described in
[RETENTION.md](RETENTION.md). An offline active group intentionally blocks
compaction; publication then fails with `transport.ErrCapacityExceeded` at the
configured bound instead of deleting owed work. Production Redis must use
`maxmemory-policy noeviction`; the count limits are not byte limits.
Identical serialized members retain Redis sorted-set upsert semantics; normal
engine envelopes contain distinct IDs. Scheduling/retry does not regenerate IDs
or rewrite headers, attempts, ordering/dedupe keys, or payload bytes.

## Retry and DLQ settlement

One same-slot Lua script checks the source stream/group and pending entry,
validates destination type, writes retry to the delayed sorted set or DLQ to
the DLQ stream, then XACKs the source. An already-settled/stale delivery returns
`transport.ErrDeliverySettled` without another destination write. Retry always
uses delayed storage, including elapsed deadlines; the sweeper moves it later.
DLQ stores payload, reason, error, attempts, failed_at (Unix milliseconds),
source_stream, source_group, and source_id. Original envelope bytes are retained.

Destination write errors leave the source pending. Lua has no rollback: if a
later ACK unexpectedly fails, a destination copy may exist alongside a pending
source, allowing duplicates but not silent loss. Redis durability/replication
settings still govern crash/failover guarantees. Missing pending state cannot
prove which external actor acknowledged a record; it is an explicit stale
settlement error, not a success receipt. Task retry uses topology delayed
storage. Event retry uses the source subscriber's private delayed set and ready
stream. The metadata hash stores topology-wide ready and delayed usage counters;
admission, retry transfer, dispatch, requeue, private ACK, retirement, and
retention update them in the same Lua operation as their data mutation. The
bounded sweeper uses cursor-based manifest pages and rotates across shared and
private lanes. ACK and
transfer from a private retry stream also XDEL the private source after XACK.
Scripts never trim DLQ records;
`redis.dlq_maxlen` is retained as a compatibility option but no longer trims on
settlement. Admin retention is separate work.

## Reclaim

Consume scans pending work with Redis 6.2+ `XAUTOCLAIM` on startup and at the
smaller of 30 seconds or `redis.ack_wait` after a full scan. Reclaim and fresh
intake share one permit budget of `Concurrency + Prefetch`; one broker entry is
acquired per permit across the shared and private streams, so there is no
fetched-but-undelivered excess. Each reclaim command claims at most one entry;
cursor progress is retained across empty pages. Fresh intake runs between
pages; read blocking is capped at two seconds so a larger `redis.block` cannot
starve reclaim. The same synchronous callback handles new and claimed work,
propagating worker-pool backpressure without a separate reclaim goroutine.

Claimed entries retain their stream ID and payload and are never republished or
ACKed by the reclaimer. Every dispatch reads its pending delivery count from
`XPENDING`, preserving broker history for the core poison guard. History lookup
or claim errors terminate the session for observable supervisor retry; unsupported
XAUTOCLAIM is not silently ignored (Redis older than 6.2 is not supported).
Malformed payload fields reach the core decode-error path rather than being
silently dropped. Cancellation leaves claimed but unhandled entries pending.

`redis.ack_wait` remains the minimum reclaim idle (default five minutes).
Redis heartbeat uses a same-key Lua ownership check followed by XCLAIM JUSTID
with the existing RETRYCOUNT, resetting idle without changing delivery history.
The engine renews before execution and at most every ack_wait/3 during handling.
Any renewal error cancels the handler and leaves the source unsettled; missing
or changed ownership returns ErrLeaseLost and never claims it back. ACK and
retry/DLQ settlement also reject changed ownership for consumer-bound deliveries.
Heartbeat stops and joins before settlement. Normal processing timeout still
applies, and heartbeat remains active during graceful drain.

Buffered/fetched entries are not renewed until a worker starts them. Configure
ack_wait above the maximum prefetch/batch queueing time plus renewal/network
margin. Avoid short leases under scheduler pauses. Cross-process partitions and
uncooperative handlers can still cause overlapping side effects: handlers must
be idempotent; lease ownership does not fence external application writes.

## DLQ administration

The DLQ is an append-only Redis stream until an explicit requeue or purge.
`transport.CursorDLQAdmin.ListPage` returns oldest-first pages and uses the last
returned Redis stream ID as an opaque cursor. Because subsequent pages use an
exclusive stream-ID range, deleting or requeueing older entries does not shift
the traversal. An empty cursor starts from the oldest record; an empty next
cursor means the scan is complete. Page size is limited to 1,000 records.

Each returned `DeadLetter.ID` is the DLQ stream record ID and is the identifier
accepted by `Requeue`; it is distinct from `DeadLetter.Message.ID`, which is the
application envelope ID. Records expose reason, error, attempts, failure time,
source stream, source group, and source stream ID. Envelope metadata is decoded
best-effort for display while the exact original bytes remain in `Message.Payload`.
Malformed mandatory DLQ metadata causes an explicit list error rather than a
partial or fabricated record.

Requeue accepts at most 1,000 record IDs. A same-slot Lua operation validates
the DLQ and ready-stream types and every selected payload before mutation, then
XADDs the original envelope to the task/shared ready stream, or for events to
the recorded source subscriber's private retry stream, and XDELs its DLQ record.
The event destination's manifest state is checked inside that same Lua script,
so retirement cannot race requeue into recreating a deleted private lane.
Missing/already-requeued IDs are successful no-ops, so retrying after an
ambiguous response cannot publish that DLQ record again. Lua isolation is not
rollback across Redis crash/failover; durability still depends on Redis
persistence and replication. Purge deletes the complete topology-scoped DLQ
stream and is intentionally irreversible.

`DLQAdmin.List(offset, limit)` remains as a positional compatibility API and
walks bounded cursor pages internally. Use `CursorDLQAdmin` for operational
scans that can overlap requeue or purge activity.

## Statistics

`transport.StatsReader` combines the main stream, consumer groups, topology and
subscriber-private delayed sets/streams, and DLQ stream. The meanings are operational obligations, not just
physical entries:

- `Pending` is ready work not yet delivered to a consumer.
- `InFlight` is delivered but unsettled work in Redis pending-entry lists.
- `Lag` is `Pending + InFlight`; delayed and DLQ entries are excluded.
- `Delayed` and `DLQ` are physical topology-scoped record counts.
- `OldestAge` covers ready and in-flight work, using the later of envelope
  creation/schedule time and falling back to the stream ID timestamp.

Task statistics inspect the `workers` group. Before that group exists, every
retained task stream entry is reported ready because the first consumer creates
the group at ID `0`. Event statistics sum every `subscriber:*` group; therefore
one event contributes once for each subscriber that still owes processing.
Unrelated groups are ignored, and an event with no subscriber groups has no
delivery obligations even if its stream retains published entries.

The result is a near-point-in-time multi-command snapshot and may span concurrent
publishes or settlements. Redis 7 `XINFO GROUPS` lag is used when its entries-read
metadata is available. Redis 6.2 and compatible implementations fall back to
cancellable `XRANGE` scans in pages of 1,000, which are exact but O(ready backlog).
Storage type errors, unavailable group lag, and count overflow return errors
instead of fabricated zeros. `OldestAgeKnown` is false when an outstanding
pending record no longer has a readable source stream entry.
