# Kafka DLQ administration and statistics

The Kafka EventBus driver implements `transport.DLQAdmin` and
`transport.StatsReader`. Both APIs return near-point-in-time broker state; they
do not create an atomic snapshot across topics, partitions, or consumer groups.

## DLQ record format

New DLQ entries are versioned JSON wrappers around the original normalized
`transport.Message`. The wrapper records the fixed failure reason/error code,
attempt count, failure time, and the source Kafka topic, partition, offset, and
record timestamp. Event entries additionally retain the source subscriber group.
`DeadLetter.ID` is the stable DLQ coordinate
`<partition>:<offset>`; it is not the logical message ID.

Older raw-payload DLQ entries cannot be listed or requeued by this API. Drain
or separately archive them before upgrading.

## Append-only administration model

Kafka cannot delete one arbitrary record from a partition. The driver therefore
uses a topology-specific consumer-group offset as a logical administration
cursor:

- `List` scans unrequeued records from the cursor and orders them by partition,
  then offset. Kafka has no total order across partitions, so positional pages
  can shift while records are concurrently appended or administered.
- `Requeue` accepts at most 1,000 IDs. IDs must include the complete logical
  prefix of each selected partition. The driver synchronously publishes every
  original task message to the main topic, or an event message to the source
  subscriber's private ready topic, then advances that partition's cursor.
  Event records written before source-group metadata existed fail closed and
  cannot be requeued because broadcasting them would violate subscriber
  isolation.
  A publish failure leaves the cursor unchanged. A crash, response loss, or
  cursor-commit failure after publication can duplicate the requeued message.
- `Purge` advances every partition cursor to a snapshot of its end offset. It is
  a logical purge: physical bytes remain until Kafka retention removes them.
- `Count` is the sum of end offset minus logical cursor for every partition.
  This is exact for the required delete-retention DLQ topic. Compaction creates
  offset holes and makes offset-based counts an upper bound; do not configure a
  compacted cleanup policy for EventBus DLQ topics.

Requeue and purge are serialized within one transport instance and acquire a
broker-backed consumer-group lease on the topology's one-partition
`.dlq-admin-lock` topic. A competing process receives a busy error instead of
running concurrently. This does not make requeue atomic: publish-before-cursor-
commit ambiguity can still duplicate records.

## Topology reconciliation and expansion

Startup reconciles every main, DLQ, ordinary delay, targeted event-delay, event
layout marker, and admin-lock topic. Event topologies must explicitly use
`kafka.WithEventLayoutTargeted()`, which sets
`kafka.event_layout=targeted-v1`; older drivers reject this option, which makes
mixed-version startup fail closed instead of allowing old instances to broadcast
targeted retries. `EnsureSubscription` provisions each event subscriber's
private ready topic plus a durable, non-expiring broker registry marker. It validates
the configured replication factor on every partition, rejects any cleanup
policy containing `compact`, and explicitly alters caller-owned topic configs
such as `retention.ms` when they differ. Immutable/unauthorized mismatches fail
startup. Ordinary startup validates partition equality but never changes the
partition count.

`ExpandPartitions` is an explicit driver administration operation for main,
DLQ, all delay topics, and broker-registered private ready topics. Registry marker
topics remain one partition. Expansion rejects decreases, updates topics in stable order,
and returns `PartitionExpansionResult`; `PartitionExpansionError` includes the
successful prefix if Kafka reports partial success. Expansion changes the
ordering-key hash-to-partition mapping. Drain producers/consumers or accept an
ordering transition before invoking it.

## Statistics

Main and delay lag are calculated from Kafka end offsets and committed consumer
group offsets, clamped to the current log start after retention. An absent
commit starts at the log start. Task stats use the default task group; event
stats aggregate all discovered subscriber groups for the topology. This event
discovery requires cluster group-list permission. `SubscriptionStats` reads one
explicit event subscriber group without aggregating its peers.

Kafka group offsets do not distinguish fetched-but-uncommitted records from
records that have not been fetched. Consequently `Pending` contains all main
topic plus private-ready lag, `InFlight` is zero, and `Lag = Pending`. `Delayed`
in topology stats includes ordinary and targeted scheduler-group lag. Kafka has
no per-header index for shared targeted tiers, so subscriber stats report
`Delayed=0` as unknown rather than attributing the entire shared backlog to every
subscriber. `DLQ`
uses the logical DLQ count above.
`OldestAge` is unknown while lag is nonzero.
