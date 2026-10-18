# Kafka delayed scheduler

The Kafka EventBus driver stores delayed envelopes in three durable tier topics:
`5s`, `1m`, and `5m`. Publication selects the largest tier no greater than the
remaining delay. One worker per tier wakes at that tier's interval and polls at
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

1. If a record still belongs in its current tier, its source offset is rewound
   and later records in that partition are left for a future sweep.
2. If the deadline is closer, the exact envelope, ordering key, and transport
   headers are synchronously published to the lower tier.
3. If the deadline has elapsed, the same data is synchronously published to the
   main topic.
4. Only after Kafka confirms destination publication is the source offset
   committed synchronously.

A publish failure never requests a source commit. A commit failure rewinds the
source, so retry can duplicate a confirmed destination record but cannot
silently discard the source. This is the same loss-safe, duplicate-prone
publish-before-commit boundary documented in [SETTLEMENT.md](SETTLEMENT.md).
Malformed envelopes and unexpected source topics remain uncommitted and block
only their source partition; Kafka DLQ wrapping and administration are P5.7.

The main, DLQ, and all delay-tier topics must have the same positive partition
count. Provisioning waits for newly created topic metadata to become visible,
then rejects any existing mismatch before starting workers. Kafka cannot reduce
an existing topic's partition count; migrate or recreate inconsistent related
topics before rollout. Equal counts plus the preserved key keep a retry on the
same derived partition as it moves between related topics when all producers use
compatible key partitioning.

The reported precision is five seconds. A record may arrive later under broker
load, group rebalance, a bounded backlog, or a destination failure. The batch
limit intentionally trades peak drain rate for bounded application work; raise
`kafka.delay_batch_size` only after accounting for produce/commit latency and
the consumer group's maximum poll interval. Retention on every delay tier must
cover the longest scheduled delay plus outage and recovery time.

`Close` cancels and joins all tier workers within the caller's context. Because
the scheduler owns source commits manually, `ConsumerCommitInterval=0` is now
valid for EventBus construction; this setting no longer controls main or delay
consumer settlement.
