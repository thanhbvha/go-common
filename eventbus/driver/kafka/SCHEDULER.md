# Kafka delayed scheduler

The Kafka EventBus driver stores delayed envelopes in three durable tier topics:
`5s`, `1m`, and `5m`. Publication selects the largest tier no greater than the
remaining delay. One worker per tier polls every five seconds and reads at
most `kafka.delay_batch_size` records (default 100, range 1–1000). The scheduler
therefore uses three tickers per topology and never creates a timer, goroutine,
or in-memory heap entry for each delayed record. Backlog beyond the current
batch remains in Kafka.

Each tier has its own consumer group. Multiple EventBus instances join the same
tier groups, so Kafka assigns every source partition to at most one active
scheduler member. A sweep uses `BlockRebalanceOnPoll`, completes its bounded
batch, rewinds retained offsets, and then permits rebalance. Scheduler clients
disable auto-commit.

For each source partition, records are examined in offset order:

1. If a record still belongs in its current tier, the scheduler republishes it
   to the tail of that same durable tier and commits the source only after Kafka
   confirms the copy. This rotation lets a later record with an earlier deadline
   progress instead of being blocked by append order.
2. If the deadline is closer, the exact envelope, ordering key, and transport
   headers are synchronously published to the lower tier.
3. If the deadline has elapsed, the same data is synchronously published to the
   main topic.
4. Only after Kafka confirms destination publication is the source offset
   committed synchronously.

The transport records the deadline in its private `X-EventBus-Run-At` header,
so `PublishAt` treats the message payload as opaque bytes. The scheduler removes
that header before publishing to the main topic and preserves it while moving a
record between tiers. Records written by older versions remain readable through
the legacy envelope deadline fallback.

A publish failure never requests a source commit. A commit failure rewinds the
source, so rotation or transfer retry can duplicate a confirmed destination
record but cannot silently discard the source. This is the same loss-safe,
duplicate-prone publish-before-commit boundary documented in
[SETTLEMENT.md](SETTLEMENT.md).
Records with neither a valid scheduling header nor a legacy envelope deadline,
and records from unexpected source topics, remain uncommitted and block only
their source partition.

The main, DLQ, and all delay-tier topics must have the same positive partition
count. Provisioning waits for newly created topic metadata to become visible,
then rejects any existing mismatch before starting workers. Kafka cannot reduce
an existing topic's partition count; migrate or recreate inconsistent related
topics before rollout. Equal counts plus the preserved key keep a retry on the
same derived partition as it moves between related topics when all producers use
compatible key partitioning.

The reported precision is five seconds when the tier group is healthy and the
number of records ahead of the deadline is within one bounded poll. Larger
backlogs add one or more five-second sweeps plus synchronous produce/commit
time; broker load, group rebalance, and destination failures add further delay.
The batch limit intentionally trades peak drain rate for bounded application
work. Raise `kafka.delay_batch_size` only after accounting for produce/commit
latency and the consumer group's maximum poll interval. Retention on every delay
tier must cover the longest scheduled delay plus outage and recovery time.

`transport.SchedulerHealthReader` exposes one payload-free status per tier with
poll, dispatch, transfer, commit, retry, last-success, bounded last-error, and
degraded state. Alert on `Degraded`, increasing retries, stale `LastSuccess`, and
oldest delayed age.

`Close` cancels and joins all tier workers within the caller's context. Because
the scheduler owns source commits manually, `ConsumerCommitInterval=0` is now
valid for EventBus construction; this setting no longer controls main or delay
consumer settlement.
