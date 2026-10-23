# EventBus Performance Optimization Backlog

Updated: 2026-10-23

## Purpose

This document records performance work that remains after P6.7.6. The items are
based on the accepted current-schema production matrix at 1 KiB payloads and
concurrency 100, plus the Phase 6 micro-benchmark baseline.

Performance work must not remove or weaken:

- payload and option validation before broker I/O;
- bounded admission and worker concurrency;
- durable publish confirmation and settlement ordering;
- retry, duplicate, DLQ, and final-obligation accounting;
- lease/ownership fencing, rebalance safety, and graceful drain;
- metrics, traces, topology isolation, or project-scoped cleanup.

Use `P6.7.6_MATRIX_REVIEW.md`, `BENCHMARKS.md`, and
`PERFORMANCE_BASELINE_ADR.md` as the evidence sources. Re-run like-for-like
benchmarks with the same Go version, broker digest, payload, concurrency, and
sampling mode before claiming an improvement.

## Priority Summary

| Priority | Area | Why |
| --- | --- | --- |
| P0 | Kafka delayed scheduler | Stable ceiling near 205-209 msg/s at both 100k and 1m |
| P0 | NATS delayed scheduler and stats | 1m delayed throughput is nonlinear and exact stats perturb the workload |
| P1 | Kafka preload client working set | Immediate/delayed 1m peaks reached roughly 490-537 MB before draining |
| P1 | EventBus core allocations | Enqueue and EngineDispatch remain 84% and 124% above the Phase 0 latency baseline |
| P1 | Redis retry50 path | Roughly 660 msg/s at 1m, materially below Redis immediate/retry10 |
| P1 | Memory cloning/state allocations | Hardening increased round-trip allocation shape to 1,333 B/op and 20 allocs/op |
| P2 | Broker stats cost | Redis group scans, NATS KV enumeration, and Kafka multi-group lag reads can perturb workloads |
| P2 | Topology/resource footprint | Kafka creates main, DLQ, and three delay topics per topology; durable drivers need capacity tuning |

## Shared Core

Implementation status (2026-10-23):

- The engine now builds an envelope once and serializes it once after trace
  injection; the public codec API retains its existing build-and-encode behavior.
- Header cloning preserves nil, trace-event attribute maps are created only when
  a recorder exists, and the no-policy release callback is shared.
- Non-lease deliveries no longer allocate a heartbeat context, channel,
  goroutine, and ticker. Redis/NATS lease deliveries retain the full renewal and
  lease-loss path.
- Settlement reuses one normalized source snapshot for the span carrier and uses
  fixed span names.
- Three-run results on the current tree: Enqueue 898-1,095 ns/op, 576 B/op and 6
  allocs/op; EngineDispatch 18,809-21,569 ns/op, about 5,212 B/op and 60
  allocs/op. Enqueue improved from 11 allocations and EngineDispatch improved
  from the current pre-change 85 allocations. Codec allocation counts remained
  unchanged. Core/Memory unit, race, and vet gates pass.

### Current evidence

The accepted Phase 6 micro-benchmark baseline is:

| Benchmark | Baseline | Allocation shape |
| --- | ---: | ---: |
| CodecEncode | 1,527 ns/op | 592 B/op, 6 allocs/op |
| CodecDecode | 485.4 ns/op | 512 B/op, 4 allocs/op |
| Enqueue | 1,600 ns/op | 1,201 B/op, 11 allocs/op |
| EngineDispatch | 13,745 ns/op | 4,335 B/op, 47 allocs/op |
| MemoryTransportRoundTrip | 1,231 ns/op | 200 B/op, 2 allocs/op |

Enqueue and EngineDispatch regressions were accepted because they bought required
correctness and observability. They remain valid optimization targets.

### P1: Reduce envelope construction and copy allocations

Investigate:

- redundant envelope materialization after trace injection;
- repeated payload/header copying between job options, codec, transport message,
  normalized delivery, retry, and DLQ paths;
- intermediate maps for headers when no mutation is needed;
- repeated JSON encoder/decoder setup that can be safely cached by concrete type.

Prefer immutable ownership transfer or copy-on-write boundaries over removing
copies globally. Caller-provided payload/header data must not become mutable
transport state.

Acceptance:

- Enqueue and EngineDispatch median latency or allocation improves by at least
  15% on three like-for-like runs.
- Codec benchmarks do not regress more than 10%.
- Metadata isolation, trace propagation, retry/DLQ preservation, and race tests
  continue to pass.

### P1: Reduce per-delivery context, timer, and span bookkeeping

Investigate allocation and timer ownership around:

- processing timeout contexts;
- lease heartbeat contexts;
- handler/process/settlement spans;
- result channels and per-delivery closures;
- retry decision normalization.

Do not pool contexts, timers, records, or message buffers unless ownership and
zeroing are explicit and race-tested.

Acceptance:

- EngineDispatch allocation count decreases without changing timeout, panic,
  cancellation, lease-loss, or confirmed-settlement behavior.
- `go test -race` for core and Memory remains clean on a supported CGO host.

### P2: Decouple resource sampling from topology stats

Implementation status (2026-10-23): complete. Process heap/goroutine samples
and broker topology samples now run in separate goroutines with independent
timestamps. Resource records retain type `sample`; topology records use
`topology_sample`. A shared encoder mutex preserves valid JSONL output without
allowing slow broker stats to block the high-frequency peak sampler.

The production harness currently mixes fast process sampling with broker stats
that can be materially slower. Keep high-frequency heap/goroutine sampling on a
separate cadence from topology calls so benchmark instrumentation does not hide
short peaks or reduce broker throughput.

Acceptance:

- Samples record separate resource and topology timestamps/cadences.
- Disabling topology sampling does not disable process or broker-volume samples.
- Throughput difference with topology sampling enabled versus disabled is
  quantified for delayed 1m workloads.

## Memory Driver

Implementation status (2026-10-23):

- External messages are cloned once at immediate/delayed ingress, then shared as
  immutable internal snapshots across fan-out, ready, delayed, in-flight, retry,
  and DLQ requeue state.
- `Delivery.Message()` and DLQ `List` remain defensive copy-on-exposure
  boundaries; mutation-isolation regressions cover repeated reads, subscribers,
  delayed caller input, and DLQ administration.
- Subscriber group order is cached and sorted, one-element ready queues retain
  backing storage, and delayed dispatch is bounded to 128 records per lock pass
  with immediate continuation signaling.
- Three-run round-trip results are 4,686-5,492 ns/op, 1,136 B/op and 18
  allocs/op, down from approximately 1,333 B/op and 20 allocs/op. This is a 15%
  byte reduction, below the aspirational 20% threshold; the two remaining
  payload copies are the required external-ingress and public-exposure ownership
  boundaries and were not removed. Unit, race, and vet gates pass.

### Current evidence

Memory is intended for tests, development, and process-local workloads rather
than durable production capacity. The P6.7.2 hardening smoke result was:

| Benchmark | Result |
| --- | ---: |
| EngineDispatch | 14,859 ns/op, 6,390 B/op, 69 allocs/op |
| MemoryTransportRoundTrip | 3,042 ns/op, 1,333 B/op, 20 allocs/op |

The cost comes from atomic fan-out checks, queued/in-flight redelivery state,
metadata isolation, bounded delayed scheduling, and source-scoped retry/DLQ.

### P1: Reduce `cloneMessage` amplification

`cloneMessage` copies payloads and header maps at enqueue, dispatch, delivery
exposure, requeue, and DLQ boundaries. Audit which boundaries truly require a
deep copy and introduce an internal immutable message representation if it can
preserve caller and subscriber isolation.

Do not expose shared mutable payload or header storage through
`Delivery.Message()`.

Acceptance:

- Memory round-trip B/op and allocs/op improve by at least 20%.
- Multi-subscriber fan-out mutation tests prove one handler cannot alter another
  handler's message.
- Retry, DLQ, metadata isolation, and race tests remain clean.

### P2: Reduce global mutex hold time

Profile contention around the transport-wide mutex during:

- fan-out capacity preflight;
- ready queue insertion;
- delayed heap movement;
- stats and DLQ operations.

Potential approaches include per-topology state locks or preparing immutable
copies before entering the critical section. Preserve atomic all-or-nothing
fan-out capacity checks.

Acceptance:

- Add parallel publish/consume benchmarks at 1, 10, and 100 goroutines.
- Throughput scales without partial fan-out, lost wakeups, or race failures.

### P2: Batch delayed wakeups

The joined heap/timer scheduler is already preferable to one timer per message.
Measure whether repeated scheduler signals and one-by-one ready queue movement
cause contention when many messages share the same deadline. Move due records in
bounded batches while retaining capacity checks and deterministic ordering.

## Redis Driver

Implementation status (2026-10-23):

- `redis.batch_size` is now applied to fresh `XREADGROUP` intake. Batch permits
  are reserved non-blockingly up to the shared concurrency+prefetch bound, and
  every delivery owns exactly one release token.
- Successful capacity reconciliation is cached process-locally, eliminating the
  normal preflight `HMGET`; all Lua scripts retain their fail-closed capacity
  checks. Only provably pre-write capacity-state errors on publish are retried;
  settlement errors are never replayed automatically.
- Event delayed lanes use a bounded fair-share quantum capped at 16 instead of
  one record per lane, while retaining the 16-lane scan and global batch bounds.
- Exact stats use the atomically maintained aggregate delayed counter when ready,
  read the subscriber manifest once, and pipeline legacy per-lane fallback
  counts in bounded chunks.
- Redis unit, race, vet, and integration-tag compilation gates pass. The 1m
  retry50 broker rerun remains required before claiming the documented 20%
  throughput target.

Measured starting profiles after this refactor:

- Low latency: batch 1-4, block 50-250 ms, delay batch 100-250.
- Retry-heavy throughput: batch 12-100, block 250 ms-1 s, delay batch 1,000.
- Large delayed backlog: delay batch 1,000 with capacities sized above the
  measured burst and exact stats cadence verified separately.

### Current evidence

Current-schema 1m throughput was approximately:

| Workload | Throughput |
| --- | ---: |
| Immediate/preload | 4,693 msg/s |
| Delayed/preload | 1,106 msg/s |
| Retry10 | 3,245 msg/s |
| Retry50 | 660 msg/s |

Post-drain heap remained below 6.7 MB and no retained backlog owner was found.
Redis has the strongest delayed throughput among the durable drivers in the
current matrix, but retry-heavy workloads are the main performance gap.

### P1: Reduce retry50 round trips and script invocations

Profile the retry path across consume, settlement Lua, delayed insertion,
scheduler transfer, and re-consume. The retry50 result suggests broker command
and scheduling overhead dominates when half the messages retry.

Investigate:

- larger safe `redis.batch_size` for fast handlers;
- pipelined settlement where source ownership and destination durability remain
  atomic per message;
- reducing repeated capacity-counter initialization checks after topology
  readiness is established;
- avoiding repeated metadata/type lookups on hot paths;
- grouped scheduler movement without weakening per-record loss safety.

Do not replace atomic settlement scripts with non-atomic command sequences.

Acceptance:

- Improve retry50 1m throughput by at least 20% from roughly 660 msg/s.
- Preserve exact 1.5m invocation accounting, zero final obligations, ownership
  fencing, and pre/post-command ambiguity tests.

### P1: Tune event delayed-lane fairness

Task delayed movement can use a batch up to 1,000, but event subscriber retry
lanes currently move one record per visited lane for fairness. With many
subscribers, lane scans and round trips can dominate.

Investigate an adaptive per-lane quantum that considers active lane count,
remaining global batch, and oldest due age. Preserve fairness so one subscriber
cannot monopolize a scheduler pass.

Acceptance:

- Add 10, 100, and 1,000 subscriber retry-lane benchmarks.
- Improve aggregate delayed throughput without starving the slowest lane.
- Keep maximum per-pass work bounded.

### P2: Bound and cache stats work

Redis stats pipeline basic counts, but event topology stats also inspect group
manifests, retry delayed sets, consumer groups, and oldest records. For many
subscriber groups this can become O(groups) broker work.

Investigate:

- short-lived cached group manifests keyed by topology revision;
- bounded parallel/pipelined delayed counts;
- a lightweight stats mode that excludes oldest age;
- separate expensive administrative stats from operational lag gauges.

Acceptance:

- Stats latency and Redis command count are benchmarked at 1, 100, and 1,000
  groups.
- Stats polling does not reduce message throughput by more than 5% at the
  documented operational cadence.

### P2: Document production option profiles

Provide measured presets rather than one universal default for:

- `redis.batch_size`;
- `redis.block`;
- `redis.delay_batch_size`;
- retention interval and batch size;
- ready, delayed, and DLQ capacities.

Presets must distinguish low-latency, high-throughput, and large delayed-backlog
workloads.

## NATS JetStream Driver

Implementation status (2026-10-23):

- Added the opt-in, mixed-version-fenced `indexed-v3` delayed layout. Its keys
  index both due second and targeted retry lane while retaining record-level CAS
  ownership and lease recovery.
- Indexed scheduler passes no longer perform the full legacy migration snapshot.
  Existing v2 topologies retain the compatibility/migration path.
- Successful indexed dispatch purges only the exact claimed key revisions after
  confirmed destination publication, avoiding KV delete-marker accumulation and
  protecting newer recovery revisions.
- Topology and subscriber delayed counts use filtered KV backing-stream state
  rather than downloading and decoding every delayed record.
- Private retry consumer stats run with bounded concurrency instead of serial
  broker requests.
- Indexed event retry lanes use versioned v3 resources and WorkQueue retention;
  existing v2 lanes retain Limits retention.
- The production benchmark enables indexed-v3. Unit, full embedded-JetStream
  integration, race, vet, and integration-tag compilation gates pass. The
  100k/1m production reruns remain required before recording throughput, stats
  overhead, and allocated-volume improvements.

### Current evidence

Current-schema 1m throughput was approximately:

| Workload | Throughput |
| --- | ---: |
| Immediate/preload | 28,465 msg/s |
| Delayed/preload | 231 msg/s |
| Retry10 | 6,969 msg/s |
| Retry50 | 1,733 msg/s |

NATS is strongest for immediate and retry10 workloads. Delayed 1m throughput is
nonlinear: the 100k delayed run reached roughly 1,067 msg/s, while 1m reached
roughly 231 msg/s. Exact periodic delayed stats also perturb the workload.

### P0: Replace full KV delayed scans with an indexed due structure

The scheduler stores delayed records in KV and walks bounded ephemeral pull
snapshots by second slot. Large live key sets still impose storage and scan cost.

Evaluate a design that avoids enumerating the full delayed KV population:

- per-slot subjects or streams with a bounded slot index;
- a compact due-slot index updated atomically with delayed publication;
- bucketed streams/consumers whose broker sequence is the cursor;
- sharded scheduler ownership by slot range when one scheduler cannot meet the
  target rate.

The design must preserve durable independent records, CAS ownership, recovery
after owner failure, collision-free IDs, and no callback watchers.

Acceptance:

- Delayed 1m throughput improves by at least 2x from roughly 231 msg/s.
- The 1m/100k throughput ratio is at least 0.7 under the same broker resources.
- Post-drain heap remains below 16 MB and no blocked KV callback exists.
- Multi-instance owner-failure and concurrent-write regressions pass.

### P0: Add cheap delayed counters for operational stats

Current exact delayed counts enumerate live KV keys. Add broker-maintained or
atomically updated counters per topology/slot so normal stats polling is O(1) or
O(active slots), reserving full reconciliation for explicit administration.

Counter updates must tolerate publish, dispatch, retry, migration, crash, and
recovery ambiguity. Include a reconciliation command/test that detects and
repairs drift without deleting work.

Acceptance:

- Periodic topology sampling can remain enabled during delayed 1m runs.
- Stats overhead reduces throughput by less than 5% at a five-second cadence.
- Final exact count and reconciled counter agree after injected failures.

### P1: Increase consumer batch efficiency

The default `nats.batch_size` is 1. Benchmark larger fetch batches with fast
handlers and bounded prefetch. Avoid creating excessive per-batch slices,
contexts, or ACK flushes.

Acceptance:

- Publish/consume benchmarks cover batch sizes 1, 10, 100, and the effective
  prefetch bound.
- Immediate throughput improves without increasing duplicate rate, ACK latency,
  or shutdown time beyond agreed limits.

### P1: Reduce retry storage and disk amplification

Retry-heavy NATS cells used the largest allocated broker volumes in the matrix,
including roughly 3.07 GB for retry50 1m. Profile stream copies, KV delayed
records, duplicate windows, and retention interactions.

Investigate shorter evidence-backed retention, compaction-safe metadata, and
removal of redundant durable representations only after confirmed transfer.

Acceptance:

- Reduce peak allocated volume for retry50 1m by at least 20%.
- Preserve restart recovery, dedupe window behavior, DLQ data, and exact retry
  accounting.

### P2: Bound subscriber retry stats

Subscriber stats snapshot the lane registry and then call `ConsumerInfo` for each
distinct durable. Add bounded concurrency, caching, or an aggregate counter for
large fan-out topologies.

## Kafka Driver

Implementation status (2026-10-23):

- Batched delayed destination production and per-partition contiguous source
  watermark commits are implemented. Produce and commit ambiguity rewinds the
  earliest unsafe source offset without committing past a failure.
- Full/due batches use bounded fast follow-up polling; idle and error paths keep
  the five-second cadence and sustained activity yields periodically.
- Consumer fetch min/max, per-partition max, and buffered record/byte limits are
  exposed as validated topology options and apply to main and delay consumers.
- The production benchmark bounds Kafka consumer records/bytes and allocates
  retry-attempt tracking only for retry workloads.
- Exact stats batch start/end offset snapshots and fetch committed offsets once
  per group.
- `WithDelayDisabled` lets immediate-only topologies omit delay topics and
  scheduler clients while rejecting future publish/retry before broker I/O.
- Unit, race, vet, and integration-tag compilation gates pass. The 100k/1m
  broker-backed acceptance reruns remain required before recording measured
  throughput and peak-heap improvements.

### Current evidence

Current-schema 1m throughput was approximately:

| Workload | Throughput |
| --- | ---: |
| Immediate/preload | 4,097 msg/s |
| Delayed/preload | 209 msg/s |
| Retry10 | 5,298 msg/s |
| Retry50 | 3,823 msg/s |

Delayed throughput remains near 205-209 msg/s at both 100k and 1m. Immediate and
delayed preload runs peaked around 490-537 MB Go heap before returning to roughly
7.2-7.5 MB after drain.

### P0: Batch delayed destination production and source commits

The delay scheduler wakes every five seconds, polls at most 1,000 records, then
processes records in partition order with one confirmed produce followed by one
synchronous source commit per record. This serial confirmation boundary is the
primary candidate for the stable delayed ceiling.

Implement bounded partition batches that:

- classify destination topics first;
- produce a bounded batch with per-record confirmation;
- commit the highest contiguous confirmed source offsets per partition;
- rewind from the first ambiguous or failed source offset;
- preserve partition ordering and publish-before-commit duplicate semantics.

Do not commit past a failed or ambiguous transfer. Kafka transactions remain out
of scope unless the entire documented settlement model is redesigned.

Acceptance:

- Delayed throughput improves by at least 2x from roughly 209 msg/s.
- Fault tests prove no source offset commits past failed/ambiguous destination
  publication.
- Duplicate behavior remains explained and exactly accounted.
- Rebalance tests still prove drain/rewind before partition transfer.

### P0: Make scheduler cadence adaptive

A fixed five-second tick adds avoidable latency and limits responsiveness when a
large due backlog exists. Use a fast follow-up poll while the previous pass found
due work, then back off to the idle interval when no due records are available.

Bound continuous work so delay tiers do not starve shutdown, metadata refresh,
or other topologies.

Acceptance:

- p50 delayed ready-to-dispatch latency decreases materially for bursty due
  workloads.
- Idle broker request rate does not increase beyond an agreed threshold.
- Scheduler cancellation and close remain bounded.

### P1: Reduce preload verifier and client working set

The large preload heap is not retained after drain, but it affects capacity.
Separate benchmark-verifier memory from driver/client memory with profiles and
reduce both where possible.

Investigate:

- completion tracking representation and latency sample retention;
- franz-go fetch batch and buffered-byte settings;
- prefetch relative to concurrency and payload size;
- copying during decode, normalization, retry, and completion verification;
- streaming percentile estimation for production-scale benchmarks.

Acceptance:

- Reduce 1m immediate/delayed peak Go heap by at least 25% from roughly
  490-537 MB.
- Preserve exact duplicate detection for finite evidence runs, or document a
  separate low-memory benchmark mode that is not used for correctness claims.
- Post-drain heap and final accounting remain unchanged.

### P1: Reduce topic and client footprint per topology

Each topology provisions main, DLQ, and three delay-tier topics; targeted event
layouts add more resources. Each delay tier owns a consumer client and lifecycle
goroutines. With many topologies, topic partitions, metadata, connections, and
goroutines can dominate.

Evaluate:

- sharing delay-tier scheduler clients across compatible topologies;
- multiplexing topology records while preserving routing and ownership;
- making delay infrastructure opt-in for topologies that never delay or retry;
- reducing default partition count while exposing measured scaling guidance.

Acceptance:

- Measure clients, goroutines, topics, partitions, metadata bytes, and startup
  time at 1, 10, 100, and 1,000 topologies.
- A topology without delay/retry does not pay for unused scheduler resources.
- Naming, isolation, settlement, and cleanup tests remain unchanged.

### P2: Reduce stats request amplification

Kafka stats request lag for every relevant main, retry-ready, and delay consumer
group, then count DLQ records. This can create many metadata/list-offset requests
for event topologies.

Investigate bounded parallel requests, short-lived metadata caching, and a
lightweight lag-only mode. Do not fabricate fetched-versus-pending precision that
Kafka cannot provide.

### P2: Tune fetch and partition settings with measured profiles

Benchmark combinations of:

- partitions relative to worker concurrency;
- `kafka.fetch_max_wait`;
- `kafka.fetch_max_bytes`;
- EventBus prefetch;
- franz-go buffered fetch bytes and records.

Publish presets for low latency, balanced throughput, and high throughput. Do
not assume partitions equal concurrency is optimal for every payload or handler.

## Measurement Plan

Every optimization PR should include:

1. Focused micro-benchmarks with at least three runs.
2. A broker-backed 100k comparison for the affected workload.
3. A 1m comparison for P0 items or any change to scheduler/storage behavior.
4. Throughput, p50/p95/p99, CPU, launcher RSS, Go peak/post-drain heap,
   goroutines, broker memory, allocated volume, lag, duplicates, DLQ, and cleanup.
5. Exact result and retry/handler accounting.
6. Heap and goroutine profile review.
7. Fault/restart/rebalance regressions appropriate to the changed driver.
8. An ADR update if the accepted baseline regresses by more than 20%.

## Recommended Execution Order

1. Kafka delayed batch production/contiguous commits and adaptive cadence.
2. NATS delayed index/counter redesign.
3. Core envelope and delivery allocation reduction.
4. Kafka preload working-set reduction.
5. Redis retry50 round-trip reduction.
6. Memory copy and lock optimization.
7. Cross-driver lightweight stats and benchmark sampling separation.
8. Driver-specific operational tuning presets.
