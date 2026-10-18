# Kafka retry and DLQ settlement

P5.5 selects the **synchronous publish-before-commit fallback**. Every retry
and DLQ transfer waits for `ProduceSync` to receive broker confirmation before
requesting a source commit from the session's contiguous offset coordinator.
The coordinator remains the sole owner of source commits, including ACKs.
Settlement succeeds only after Kafka confirms that commit.

| Failure boundary | Source offset | Recovery |
|---|---|---|
| Destination publish fails | Not committed | Source can be delivered again |
| Destination write succeeds but its response is lost | Not committed | Redelivery can duplicate the destination |
| Destination confirmed, process stops or commit fails | Not intentionally advanced | Redelivery can duplicate the destination |
| Commit succeeds but its response is lost | May already be committed | Settlement reports failure; repeating it can duplicate the destination |
| Both responses confirmed | Committed through the safe contiguous prefix | Settlement succeeds |

No higher completed offset can bypass a lower failed transfer. Failed
settlement reopens the delivery guard; repeated successful settlement is
rejected without further broker work. The engine ends a failed consume session
and reconnects so uncommitted sources can be redelivered. Retry keeps the
incremented envelope bytes, logical ID, headers, and ordering key; a future
deadline selects a delay-tier topic. DLQ uses `Decision.Source` when present,
otherwise retaining the raw delivery. DLQ wrappers/admin remain P5.7 work.

This is at-least-once delivery. Producer idempotence limits retries within a
producer session; it does not deduplicate a second application-level transfer
after a crash or ambiguous response. Handlers must be idempotent. Retry chains
can reorder relative to later source records (DS-52).

## Configuration and durability

Use `NewWithConfig` or `NewWithClient` with the normal Kafka configuration.
There is no transaction mode to enable in this version. `ProducerAcks=0` is
rejected with `transport.ErrNotSupported`, because it cannot confirm durable
destination acceptance. The deprecated raw `New` constructor cannot return an
error, so invalid producer configuration is reported by topology validation,
publish, and consume before work is accepted. A raw producer with
`kgo.TransactionalID` is also rejected rather than silently using a transaction
that the driver cannot atomically settle.

Prefer the default `ProducerAcks=-1` and `ProducerIdempotent=true`, and configure
topic replication, `min.insync.replicas`, retention, and broker durability for
the required failure tolerance. Leader-only acknowledgements (`acks=1`) are
accepted but provide weaker durability if the leader fails before replication.
No client-side settlement protocol can recover data discarded by broker
retention or lost beyond the configured replication guarantee.

## Why transactions are not selected yet

The transport has one shared producer and independent consumers per topology
and subscriber group. Beginning a transaction on that shared producer cannot
safely pair one session's outputs with its group generation and safe offset
prefix while other sessions publish concurrently. Transactional support needs
session-owned transactional producers, fenced transactional identities, abort
and response-ambiguity recovery, and `read_committed` consumers (including delay
consumers). It must be implemented and fault-tested as one protocol before it
can be offered. The fallback is an explicit P5.5 choice, not an automatic
downgrade after a transaction fails. Neither atomic retry nor atomic DLQ
settlement is advertised.

P5.6 applies the same confirmed publish-before-commit boundary while moving
records between bounded delay tiers and the main topic. See
[SCHEDULER.md](SCHEDULER.md) for its resource, ordering, and timing behavior.
