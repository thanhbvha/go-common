# EventBus test suites

The default test suite is hermetic and must not require Redis, NATS, Kafka, or
Docker:

```powershell
go test ./eventbus/...
```

The Memory package runs the same reusable transport contract as durable drivers
for every applicable in-process capability. Its focused tests additionally cover
atomic capacity saturation, publish-before-consume event groups, concurrent
publication, scheduler ordering/capacity/close, settlement redelivery,
subscriber-scoped retry and DLQ requeue, topology-scoped dedupe, complete stats,
and post-close behavior. Durable restart and multi-process scenarios are
capability-inapplicable rather than simulated.

Tests that exercise a broker or an implementation spike use the `integration`
build tag:

```powershell
go test -tags=integration ./eventbus/...
```

Integration prerequisites:

- Kafka must be reachable at `localhost:9092` for the current Kafka driver and
  Kafka spike tests. Tests skip when the driver test cannot reach Kafka, while
  the low-level Kafka spike reports setup failures.
  `go test -tags=integration ./eventbus/driver/kafka -count=1` includes a unique
  two-partition topic test. Default Kafka tests use deterministic `synctest`
  coverage for partition serialization, cross-partition concurrency through the
  engine, bounded polling, cancellation, and settlement failure. The tagged
  `TestKafkaOffsetCoordinator_ContiguousBrokerCommit` reads real consumer-group
  offsets and proves that a higher completed record cannot advance the broker
  commit past a lower unsettled record. The tagged
  `TestKafkaRebalanceDrainsAndTransfersPartitions` forces a two-member partition
  transfer while work is in flight and verifies drain-before-release plus
  loss-free rewind of polled-but-undispatched records.
  `TestKafkaSettlementBrokerFaultWindows` injects failures before destination
  production, after its broker response, before source commit, and after its
  broker response. It checks real group offsets and destination log counts for
  both retry and DLQ, including the fallback's expected duplicate windows.
  Default `TestKafkaSettlement*` and `TestKafkaFailedTransferBlocksHigherCommit`
  tests cover confirmation barriers, cancellation, failed transfer fencing,
  metadata/routing, single-use settlement, and unsafe producer rejection.
  Scheduler tests cover tier selection, bounded polling, shutdown joining,
  publish/commit failures, malformed envelopes, exact ordering-key/header
  transfer, payload-free health recovery, and a far-future record preceding a
  near-future deadline in the same partition. Tagged scheduler tests reject related-topic
  partition mismatches and exercise a live five-record burst with a two-record
  sweep bound while verifying stable ordering-key partitioning.
  P5.7 default tests cover versioned failure/source metadata, append-only logical
  cursors, prefix-only requeue, logical purge, offset-derived task/event/delay
  lag, and group filtering. The tagged admin test verifies real broker offsets,
  DLQ list/count/requeue/purge, and post-requeue task lag.
  P5.8 adds `TestKafkaTransportContract`, which runs every applicable shared
  transport scenario against the live broker (dedupe is skipped because Kafka
  does not advertise it). TLS/SASL construction tests cover producer, grouped
  and manual consumers, scheduler consumers, and the direct DLQ reader through
  both supported factories and the deprecated raw-client adapter.
- NATS driver and spike tests start an in-process JetStream server and do not
  require a separately installed NATS server. The suite includes the shared
  transport contract plus restart/reconnect, `AckWait`, heartbeat,
  `MaxAckPending`, duplicate-publish, multi-instance scheduler, and scheduler
  health failure/recovery coverage. Event topology tests also require the typed
  `nats.WithEventLayoutTargeted()` declaration option and verify that a missing
  or conflicting broker layout marker fences publish, provisioning, consume,
  scheduler, private-lane, statistics, cleanup, and DLQ requeue paths.
- Redis driver unit/contract/fault tests use `miniredis` and remain part of the
  hermetic default suite. Deterministic tests also prove scheduler health
  failure/recovery and that `Close` waits for owned delayed sweepers or returns
  the caller deadline. Live single-node and Redis Cluster contracts are
  integration-tagged and require explicit addresses. A local Redis 7 fixture is
  provided in `driver/redis/testdata/compose.yaml`:

```powershell
docker compose -f eventbus/driver/redis/testdata/compose.yaml up -d --wait
$env:EVENTBUS_REDIS_ADDR='127.0.0.1:16379'
$env:EVENTBUS_REDIS_CLUSTER_ADDRS='127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002'
go test -tags=integration ./eventbus/driver/redis -run 'ContractIntegration|RetentionIntegration' -count=1
docker compose -f eventbus/driver/redis/testdata/compose.yaml down -v
```

The tests use unique namespaces and delete only their own keys. They never call
`FLUSHDB`. An unset address skips the corresponding live suite.

The shared scenario-to-test mapping and Redis fault-window coverage are recorded
in [CONTRACT_TESTS.md](CONTRACT_TESTS.md).

P6.7.4 core tests inject wrong topology kind/name, empty or oversized IDs,
invalid timestamps, dedupe fields, keys, payloads, and headers before handler
invocation. Hostile errors whose `Error`/`Unwrap` methods panic, panicking retry
policies, and negative/oversized retry delays must remain non-panicking and
produce fixed classifications. Memory, Redis, NATS, and Kafka DLQ tests verify
metadata sanitization and the final encoded-record size bound; NATS coverage is
integration-tagged because it reuses the embedded JetStream delivery fixture.

P6.7.5 core tests exercise kind-explicit task/event and subscriber statistics
plus Bus-level DLQ administration without concrete transport assertions. Kafka
unit tests cover unsafe compaction rejection, replication/config reconciliation,
partition decreases, and deterministic partial expansion reporting. The live
Kafka topology-admin test verifies retention reconciliation, expansion of main/
DLQ/all delay topics, and cross-process exclusion by the DLQ operator lease.

P6.7.6 uses two reusable suites. `transport/contracttest` covers driver-boundary
settlement, retry/DLQ transfer and administration, delay, fan-out/isolation,
dedupe, stats, scheduler health, and malformed payloads. `eventbus/contracttest`
covers exact retry bounds, retry-after, permanent failure, panic, timeout,
malformed/wrong-route envelopes, graceful drain, and capability-gated forced
redelivery through the public Bus. Memory and Redis/miniredis run by default;
embedded NATS, live Redis, and live Kafka registrations are integration-tagged.
Subscriber retry and DLQ-requeue isolation pass Memory, Redis, NATS, and Kafka.
Redis, NATS, and Kafka pass the shared forced-redelivery case; Memory is
durability-inapplicable. The embedded NATS harness uses a short broker AckWait
and waits beyond it before starting the replacement consumer; that focused Bus
suite passed ten consecutive runs on 2026-10-20.

The transport suite also cancels retry and DLQ settlement before destination
confirmation, verifies no destination state was accepted, closes the failed
session, and proves the source redelivers and can settle successfully. This
shared case passes Memory, Redis, NATS, and Kafka. Post-commit response-loss
ambiguity is exercised through one capability-gated shared scenario with
driver adapters for each broker boundary. Redis proves its atomic transfer
committed one destination and removed the pending source before response loss.
NATS proves stable transfer identity keeps one destination while the same source
delivery remains pending until settlement retry. Kafka proves an ambiguous
append followed by source redelivery can create the documented duplicate
destination, then commits the source without loss. Memory is inapplicable
because it has no broker response boundary.

The transport suite forces one unsettled delivery to lose its consumer session,
then receives the same logical message again and ACKs it. It compares the second
delivery with the first delivery's exposed ID, type, payload, ordering key, and
correlation header, requires a valid broker attempt, and requires final topology
stats to return to zero. Memory restores session-owned in-flight state, Redis
reclaims a stale pending entry, NATS waits beyond the configured AckWait, and
Kafka replays the uncommitted group offset. Kafka may retain attempt `1` because
the envelope itself was not retried; this does not weaken the at-least-once
redelivery guarantee. All four drivers pass. Handlers must remain idempotent.

The public Bus suite also forces five consecutive real-driver consume sessions
to end and reconnect under supervisor control. It asserts that no two sessions
overlap, samples goroutines only after each driver's `Consume` has returned and
joined session-owned work, bounds first-to-last goroutine growth, then publishes,
handles, settles, and drains normally through the recovered session. Redis,
NATS, and Kafka pass this shared case; Memory is durability-inapplicable. Broker
process restart loops remain production fault/soak evidence rather than part of
this deterministic contract scenario.

The transport suite runs one shared multi-instance delayed-scheduler ownership
scenario. Redis forces the first instance's destination write to fail, verifies
the delayed source remains durable, and lets the second instance atomically move
it. NATS writes a live KV lease owned by the first instance, proves the successor
cannot dispatch it before expiry, then reclaims and dispatches exactly once.
Kafka lets the first scheduler group member fetch and exit without transfer or
commit, then verifies a successor member receives the same source offset,
transfers it, and commits. The suite consumes and ACKs every allowed ready copy
and requires final stats to be zero. Redis, NATS, and Kafka pass; Memory is
distributed-ownership-inapplicable.

The capability-gated ordering scenario publishes eight messages with one
ordering key, consumes with concurrency and prefetch greater than one, and
requires each payload and key to arrive in publication order with no later
same-key delivery admitted before the current delivery settles. Kafka passes
this scenario; Memory, Redis, and NATS skip it because they do not advertise
ordering. Kafka's guarantee remains scoped to one physical topic-partition:
main, delay-tier, and subscriber-private topics are independent logs.

Production-scale backlog and soak runs are not Go tests. Use the disposable
fixture and guarded orchestrator documented in `PRODUCTION_SCALE.md`; never run
the destructive scale matrix against shared developer brokers.

On Ubuntu, validate the native Bash tooling without starting Docker workloads:

```bash
./eventbus/testdata/benchmark/test-scripts.sh
```

When running in the restricted Windows workspace, use a workspace-local build
cache:

```powershell
$env:GOCACHE='D:\Go\go-common\.gocache-review'
go test ./eventbus/...
```

Do not add sleeps or external broker connections to the default suite. Prefer
fake clocks, channels, in-memory transports, and deterministic test servers.

## Core race checks on Windows

With Scoop `mingw` installed (verified with GCC 16.2.0 / runtime v14), enable
CGO for the current terminal and run the core and memory transport suites:

```powershell
$env:Path = "$env:USERPROFILE\scoop\apps\mingw\current\bin;$env:Path"
$env:CGO_ENABLED = '1'
$env:CC = 'gcc'
$env:GOCACHE = 'D:\Go\go-common\.gocache-review'
go test -race ./eventbus ./eventbus/driver/memory -count=1
```

These settings do not change persisted `go env` defaults. The Phase 2 race exit
check passed on 2026-10-17; no external brokers are required by this command.
