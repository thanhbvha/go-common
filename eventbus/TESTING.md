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
  P5.6 default tests cover tier selection, bounded polling, shutdown joining,
  offset rewind, publish/commit failures, malformed envelopes, and exact
  ordering-key/header transfer. Tagged scheduler tests reject related-topic
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
  `MaxAckPending`, duplicate-publish, and multi-instance scheduler coverage.
- Redis driver unit/contract/fault tests use `miniredis` and remain part of the
  hermetic default suite. Live single-node and Redis Cluster contracts are
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
