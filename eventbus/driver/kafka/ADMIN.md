# Kafka DLQ administration and statistics

The Kafka EventBus driver implements `transport.DLQAdmin` and
`transport.StatsReader`. Both APIs return near-point-in-time broker state; they
do not create an atomic snapshot across topics, partitions, or consumer groups.

## DLQ record format

New DLQ entries are versioned JSON wrappers around the original normalized
`transport.Message`. The wrapper records the failure reason, error text,
attempt count, failure time, and the source Kafka topic, partition, offset, and
record timestamp. `DeadLetter.ID` is the stable DLQ coordinate
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
  original message to the main topic, then advances that partition's cursor.
  A publish failure leaves the cursor unchanged. A crash, response loss, or
  cursor-commit failure after publication can duplicate the requeued message.
- `Purge` advances every partition cursor to a snapshot of its end offset. It is
  a logical purge: physical bytes remain until Kafka retention removes them.
- `Count` is the sum of end offset minus logical cursor for every partition.
  This is exact for the required delete-retention DLQ topic. Compaction creates
  offset holes and makes offset-based counts an upper bound; do not configure a
  compacted cleanup policy for EventBus DLQ topics.

Requeue and purge are serialized within one transport instance. Kafka group
offset commits do not offer compare-and-swap, so operators must serialize these
operations across processes. Concurrent administrators can cause duplicate
requeues or cursor races; they cannot make the driver claim an atomic transfer.

## Statistics

Main and delay lag are calculated from Kafka end offsets and committed consumer
group offsets, clamped to the current log start after retention. An absent
commit starts at the log start. Task stats use the default task group; event
stats aggregate all discovered subscriber groups for the topology. This event
discovery requires cluster group-list permission.

Kafka group offsets do not distinguish fetched-but-uncommitted records from
records that have not been fetched. Consequently `Pending` contains all main
topic lag, `InFlight` is zero, and `Lag = Pending`. `Delayed` is the sum of lag
for the three durable scheduler groups. `DLQ` uses the logical DLQ count above.
`OldestAge` is unknown while lag is nonzero.
