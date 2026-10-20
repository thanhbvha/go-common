# EventBus benchmark baseline

This file records performance baselines before the refactor. These numbers are
not release targets. Compare changes on the same machine with `benchstat` or an
equivalent statistical comparison; results from different machines are not
directly comparable.

## Baseline: 2026-10-15

Environment:

- OS: Windows
- Architecture: amd64
- CPU: 11th Gen Intel Core i7-11800H @ 2.30 GHz
- Go package: `github.com/thanhbvha/go-common/eventbus`
- Parallel benchmark workers reported by Go: 16

Command:

```powershell
$env:GOCACHE='D:\Go\go-common\.gocache-review'
go test ./eventbus -run '^$' -bench 'Benchmark(Codec|Enqueue|EngineDispatch|MemoryTransport)' -benchmem -count=3
```

Median of three runs:

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| CodecEncode | 1,573.0 | 560 | 6 | Envelope v2 plus JSON payload encoding |
| CodecDecode | 495.1 | 496 | 4 | 208-byte envelope; 420.15 MB/s on median run |
| Enqueue | 870.1 | 544 | 6 | Encode plus synchronous sink transport publish |
| EngineDispatch | 6,130.0 | 2,194-2,208 | 25 | Sequential enqueue-to-handler signal through memory transport |
| MemoryTransportRoundTrip | 1,182.0 | 144 | 1 | Publish, receive delivery, and ACK |

Raw ranges:

- CodecEncode: 1,506-1,754 ns/op.
- CodecDecode: 459.2-513.7 ns/op.
- Enqueue: 853.3-987.5 ns/op.
- EngineDispatch: 5,534-6,186 ns/op.
- MemoryTransportRoundTrip: 1,140-1,262 ns/op.

## Benchmark rules

- Benchmarks must not synchronize with fixed sleeps.
- Broker benchmarks belong to the integration/benchmark environment, not the
  hermetic default suite.
- Record payload size, concurrency, success/retry mix, and backlog for all
  transport comparisons.
- Do not optimize solely for `ns/op`; settlement correctness and bounded
  resource use remain release gates.

## Phase 6 standardized runner: 2026-10-18

Run one end-to-end case against any driver:

```powershell
$env:GOCACHE="$PWD\.gocache-review"
go run ./examples/eventbus/benchmark --driver kafka --namespace phase6_isolated `
  --payload-kb 10 --concurrency 10 --workload retry10 --messages 1000
```

Valid payload sizes are 1/10/100 KiB, concurrency levels are 1/10/100, and
workloads are `immediate`, `delayed`, `retry10`, and `retry50`. The runner emits
JSON with throughput, p50/p95/p99 end-to-end latency, allocation bytes/count,
sampled peak heap, Go CPU seconds, GC cycles, and successful duplicate count.
Broker publication errors are retried with bounded exponential delay until the
whole-run context expires; a response-ambiguity duplicate remains observable.

Run all 36 cases for one driver with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  ./examples/eventbus/benchmark/run-matrix.ps1 -Driver kafka -Messages 1000
```

Use `--preload` with 100,000 or 1,000,000 messages only on disposable isolated
broker infrastructure. It pauses local intake, loads the backlog, drains the
paused session, then times resume-to-completion. Redis automatically raises its
benchmark stream bound to the requested message count. Durable benchmark
topologies/data are intentionally not deleted by this repository tool.

### Functional matrix results

All four live local drivers completed 36 cases (144 total) with 20 messages per
case, covering every required payload/concurrency/workload combination. This is
a functional and resource-bound smoke matrix, not a capacity claim.

| Driver | Throughput range msg/s | Maximum p99 | Maximum sampled heap | Maximum CPU/case | Duplicates |
| --- | ---: | ---: | ---: | ---: | ---: |
| Memory | 1,658.65–38,461.54 | 2.97 ms | 9,534,104 B | 0.0344 s | 0 |
| Redis | 9.90–2,664.54 | 2,007.00 ms | 7,706,984 B | 0.1050 s | 0 |
| NATS | 9.20–1,552.36 | 2,133.28 ms | 15,155,912 B | 0.1756 s | 0 |
| Kafka | 1.94–19.32 | 9,815.47 ms | 10,435,680 B | 0.2415 s | 0 |

The slow minima are delay-precision/scheduler cases and the tiny sample size
magnifies setup cost. A separate representative 1 KiB/immediate/concurrency-10
run with 100 messages produced:

| Driver | Throughput msg/s | p50 | p95 | p99 | B/message | allocs/message | Duplicates |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 48,475.45 | <0.1 ms | 0.52 ms | 0.52 ms | 25,646.88 | 64.61 | 0 |
| Redis | 3,491.74 | 2.71 ms | 4.18 ms | 4.26 ms | 32,331.28 | 142.77 | 0 |
| NATS | 1,783.38 | 1.05 ms | 1.69 ms | 2.57 ms | 41,025.36 | 169.91 | 0 |
| Kafka | 92.99 | 806.52 ms | 832.20 ms | 841.83 ms | 47,040.24 | 273.43 | 0 |

Large persistent backlog runs were not executed on the shared local services:
Memory has intentional default 1,000-entry ready, delayed, and DLQ bounds for
its process-local test/development role, while Redis/NATS/Kafka lack a
safe benchmark topology deletion API and would retain substantial user-visible
data. The runner supports both target sizes for disposable infrastructure; this
environment therefore does not satisfy the plan's “where infrastructure
permits” condition. No destructive broker cleanup was attempted.

The three-run core micro-benchmark results and accepted regressions are recorded
in [PERFORMANCE_BASELINE_ADR.md](PERFORMANCE_BASELINE_ADR.md).

P6.7.2 changes Memory from a channel benchmark stub into a bounded semantic
reference transport with atomic fan-out, queued/in-flight redelivery, and one
joined delayed scheduler. A Linux smoke run and the accepted correctness cost
are recorded in the ADR addendum; use a same-host three-run comparison before
promoting those smoke values to a new statistical baseline.

## P6.7.6 production-scale tooling

The benchmark runner now uses a fixed latency histogram, a bounded completion
notification channel, and one bit per sequence for duplicate detection. It can
write periodic heap, goroutine, throughput, and topology-stat samples without
retaining per-message timestamps. `PRODUCTION_SCALE.md` documents the pinned,
profile-scoped Redis/NATS/Kafka fixture and cleanup-guarded PowerShell runner.
Completion now pauses every benchmark consumer, waits for accepted handlers and
settlement, reads final topology statistics while the transport is still open,
and rejects residual pending, delayed, in-flight, DLQ, or lag obligations.
Native Bash runner/matrix/validation scripts provide the same guarded workflow
on Ubuntu. Both native runners sample named-volume data usage from the broker
data directory. Periodic benchmark and broker samples carry explicit `preload`
or `measurement` phases; preload runs reset Go heap, CPU, elapsed-time, and peak
baselines immediately before resume. Acceptance fails if measurement samples or
numeric named-volume samples are missing, and preload runs also require preload
samples.

One disposable Redis 7.4.1 run completed on 2026-10-19 with 100,000 immediate
1 KiB messages, concurrency 100, and a preloaded backlog:

- 100,000 unique completions, zero observed duplicates, and final lag zero.
- Drain throughput 1,527.67 messages/second.
- p50/p95/p99 acceptance-to-completion latency 57.91/67.16/67.16 seconds; this
  includes paused preload time, while throughput starts at resume.
- Peak Go heap 6,175,912 bytes, peak benchmark RSS about 74.8 MB, and peak
  goroutines 108.
- Redis container memory peaked around 159 MB in the sampled series and fell to
  about 117 MB near completion; the isolated container, network, and named
  volume were removed by project-scoped cleanup.

That historical run predates the stricter final-settlement result schema and is
therefore useful harness evidence, not sufficient acceptance evidence. Artifacts are intentionally ignored because they contain machine-local paths
and high-volume samples. This single matrix cell validates the harness, not the
P6.7.6 release gate. The 1,000,000-message runs, remaining workloads/drivers,
fault/restart/rebalance cases, and sustained soak remain open.

On 2026-10-20 the corrected Ubuntu runner accepted a new disposable Redis
7.4.1 immediate/preload harness cell with the same 100,000-message, 1 KiB,
concurrency-100 dimensions:

- 100,000 unique completions, zero observed duplicates, and all final topology
  counters zero.
- Measurement-only drain throughput 4,354.52 messages/second over 22.96 seconds.
- 14 preload samples, 23 measurement samples, and 18 numeric volume samples.
  Their 83,082,531-byte maximum predates allocated-block sampling and is an
  apparent-size historical value, not accepted allocated-disk evidence.
- Peak measurement-phase Go heap was 4,052,176 bytes and peak goroutines were
  107.
- `execution_status` was `completed`, `acceptance_status` was `passed`, and
  project-scoped container/volume cleanup was verified.

Artifacts are under the ignored local path
`/tmp/thothan/p676-production-evidence/20261020T043520168Z-redis-immediate-100000-phase-volume-rerun-07cfaf87/`.
This remains one harness cell, not the required matrix or soak evidence.

The Redis 1,000,000-message immediate/preload cell was accepted on 2026-10-20
with 1 KiB payloads and concurrency 100:

- 1,000,000 unique completions, zero observed duplicates, and all final topology
  counters zero.
- Measurement-only drain throughput 4,451.03 messages/second over 224.67 seconds.
- 29 preload samples, 45 measurement samples, and 61 broker samples. Every
  broker sample had a valid phase and numeric named-volume measurement.
- Maximum pending/lag was the intended 1,000,000-message backlog. The recorded
  443,364,033-byte maximum and 324,948,455-byte last Redis values predate
  allocated-block sampling and remain apparent-size historical values.
- Peak measurement-phase Go heap was 4,665,016 bytes and peak goroutines were
  107.
- Execution and acceptance passed, and project-scoped cleanup was verified.

Artifacts are under the ignored local path
`/tmp/thothan/p676-production-evidence/20261020T045307169Z-redis-immediate-1000000-redis-1m-final-ee39105d/`.
This closes only the Redis immediate/preload 1m cell. Other Redis workloads,
NATS/Kafka cells, fault runs, and sustained soak remain open.

The NATS 100,000-message delayed/preload cell was accepted on 2026-10-20 with
1 KiB payloads, concurrency 100, a requested 5-second delay, and an explicitly
recorded `nats_delay_batch_size` of 1,000 (the driver's validated maximum):

- 100,000 unique completions, zero observed duplicates, and all final topology
  counters zero.
- Measurement throughput 1,127.80 messages/second over 88.67 seconds.
- The first measurement sample had 14,000 completed and 86,000 delayed; sampled
  maxima were 88,000 delayed, 12,000 pending, and one in-flight obligation,
  demonstrating scheduler transfer rather than immediate bypass.
- 9 preload samples, 45 measurement samples, and 35 fully phase-labeled numeric
  JetStream volume samples. The 396,737,438-byte maximum/final value predates
  allocated-block sampling and remains an apparent-size historical value.
- Peak high-frequency measurement heap was 97,473,328 bytes, peak goroutines
  were 117, and project-scoped cleanup was verified.

An earlier diagnostic run inherited the demo batch size of 100 and required
987.67 seconds, about 101.25 messages/second. It exposed a benchmark-configuration
ceiling rather than a correctness failure. The accepted run overrides only the
benchmark topology, records the effective batch in both manifest and result, and
does not change the library or demo default. Artifacts are under
`/tmp/thothan/p676-production-evidence/20261020T053743021Z-nats-delayed-100000-nats-100k-delayed-tuned-0fa837b0/`.
This closes only the NATS delayed/preload 100k cell.

The Kafka 100,000-message `retry50` cell was accepted on 2026-10-20 with 1 KiB
payloads, concurrency 100, 100 partitions, and a second worker joining after
five seconds:

- 100,000 unique completions, zero duplicates, and all final topology counters
  zero. The retry mix produced exactly 150,000 handler invocations.
- The primary worker handled 82,968 invocations and the second handled 67,032.
  The second joined when only 11,542 messages were produced and 10,643 completed,
  proving active group participation rather than only bus startup.
- Throughput was 4,131.56 unique completions/second over 24.20 seconds;
  p50/p95/p99 latency was 1.17/324.45/1,061.22 milliseconds.
- Peak measurement heap was 7,829,568 bytes, peak goroutines were 302, and the
  maximum sampled pending/lag obligation was 898.
- 11 broker samples used `du_allocated_1k_blocks`; maximum allocated Kafka data
  usage was 104,665,088 bytes. Cleanup was verified.

The fixture healthcheck now uses local TCP readiness because a metadata-aware
client followed the host-advertised listener from inside the container and timed
out despite a started broker. Startup failures retain scoped broker logs.
Rebalance acceptance requires nonzero second-worker handler participation. An
intermediate run also exposed sparse Kafka segment apparent sizes; volume
evidence now uses allocated `du -sk` blocks and records the method explicitly.
Final artifacts are under
`/tmp/thothan/p676-production-evidence/20261020T055834112Z-kafka-retry50-100000-kafka-100k-retry50-rebalance-allocated-0ef6ce26/`.
This closes only the Kafka retry50/second-worker 100k cell.

The Redis 100,000-message `retry50` broker-restart cell was accepted on
2026-10-20 with 1 KiB payloads, concurrency 100, a five-second restart, and an
explicitly recorded `redis_delay_batch_size` of 1,000:

- The restart completed during active work at 10,799 produced, 9,899 completed,
  and lag 900.
- The run completed 100,000 unique messages and exactly 150,000 handler
  invocations with zero duplicates and all final topology counters zero.
- Throughput was 1,722.95 unique completions/second over 58.04 seconds;
  p50/p95/p99 latency was 505.99/1,010.09/1,061.22 milliseconds.
- Peak heap was 4,088,872 bytes, peak goroutines were 107, and the maximum
  sampled pending/lag obligation was 900.
- 19 broker samples used `du_allocated_1k_blocks`; maximum allocated Redis data
  usage was 74,121,216 bytes. Cleanup was verified.

An earlier correctness-pass run inherited the demo delay batch of 100 and took
501.07 seconds at 199.57 messages/second. The accepted rerun uses a benchmark-
only validated-maximum override, now recorded and fail-closed in the result and
runner manifest. Final artifacts are under
`/tmp/thothan/p676-production-evidence/20261020T070313896Z-redis-retry50-100000-redis-100k-retry50-restart-delaybatch1000-8a263c7f/`.
This closes one Redis broker-restart fault cell, not the remaining fault matrix
or sustained soak.

The NATS 100,000-message `retry50` broker-restart cell was accepted on
2026-10-20 with 1 KiB payloads, concurrency 100, a five-second restart, and
`nats_delay_batch_size=1000`:

- The restart completed during active work at 10,799 produced, 9,899 completed,
  and lag 2.
- The run completed 100,000 unique messages and exactly 150,000 handler
  invocations. All 50,000 expected retry candidates executed their synthetic
  attempt-1 failure; none was first observed after attempt one.
- There were zero duplicates and all final topology counters were zero.
- Throughput was 1,733.06 unique completions/second over 57.70 seconds;
  p50/p95/p99 latency was 63.60/1,230.66/1,427.15 milliseconds.
- Peak heap was 38,894,840 bytes, peak goroutines were 124, and sampled maxima
  were 900 delayed and three pending/in-flight/lag.
- 19 broker samples used `du_allocated_1k_blocks`; maximum allocated JetStream
  data was 431,722,496 bytes. Cleanup was verified.

The first restart run exposed an unbounded JetStream publish request: a publish
started near the outage retained the whole 30-minute run deadline and prevented
the producer from retrying. Benchmark enqueue attempts are now individually
bounded to five seconds while the retry loop remains under the parent run
deadline. A subsequent diagnostic completed with 149,999 invocations because
one retry candidate was first delivered with broker attempt two after restart.
The benchmark now records expected retries, executed synthetic failures, and
retry candidates first seen after attempt one; runners fail unless that retry
partition and total handler accounting are exact. Final artifacts are under
`/tmp/thothan/p676-production-evidence/20261020T080451700Z-nats-retry50-100000-nats-100k-retry50-restart-accounted-398c2045/`.
This closes one NATS broker-restart fault cell, not the remaining Kafka fault
cell or sustained soak.

The Kafka 100,000-message `retry50` broker-restart cell was accepted on
2026-10-20 with 1 KiB payloads, concurrency 100, 100 partitions, and a restart
requested after five seconds:

- The runner waited for observed active work, then restarted at 1,573 produced,
  682 completed, and lag 902. The scoped restart completed successfully.
- The run completed 100,000 unique messages. All 50,000 unique retry candidates
  first failed at envelope attempt one; restart replay produced 50,010 synthetic
  failure invocations in total.
- Four duplicate completions were observed inside the documented at-least-once
  restart window. Total handler accounting was exact at 150,014 invocations.
- All final pending, delayed, in-flight, DLQ, and lag counters were zero.
- Throughput was 2,140.44 unique completions/second over 46.72 seconds;
  p50/p95/p99 latency was 229.63/481.61/1,655.01 milliseconds.
- Peak heap was 8,809,672 bytes, peak goroutines were 154, and maximum sampled
  pending/lag was 906.
- 17 broker samples used `du_allocated_1k_blocks`; maximum allocated Kafka data
  was 112,594,944 bytes. Cleanup was verified.

An initial run restarted Kafka while the benchmark was still provisioning and
had produced no work. Restart scheduling now treats the configured duration as
an earliest time and waits for a benchmark sample with `produced > 0` plus active
production or backlog. A diagnostic run then exposed the distinction between
unique retry candidates and repeated attempt-one failure invocations during
Kafka replay. Result and runner acceptance now record both values separately.
Final artifacts are under
`/tmp/thothan/p676-production-evidence/20261020T081623285Z-kafka-retry50-100000-kafka-100k-retry50-restart-final-01159a0a/`.
This closes the selected Kafka broker-restart fault cell, not sustained soak or
the remaining production matrix.

A two-hour Kafka sustained restart/reconnect/rebalance soak was accepted on
2026-10-20 with `retry10`, 1 KiB payloads, target rate 100 messages/second,
concurrency 100, and 100 partitions. A second worker joined after 15 minutes and
the broker restarted after 30, 60, and 90 minutes:

- 711,450 messages were actually produced and all 711,450 completed uniquely.
  Duration mode intentionally does not catch up rate slots missed while an
  enqueue is blocked by broker downtime, so 720,000 is an upper pacing bound,
  not the accepted count.
- All 71,145 expected retry candidates failed exactly once at attempt one.
  There were zero duplicate completions and total handler accounting was exact
  at 782,595 invocations.
- The second worker joined at 89,976 produced/89,975 completed and handled
  341,959 invocations; the primary handled 440,636.
- Three scoped broker restarts completed during active production at 176,966,
  356,531, and 531,880 produced messages. All final topology counters were zero.
- Throughput was 98.81 unique completions/second over 7,200.02 seconds;
  p50/p95/p99 latency was 1.29/2.83/4.21 milliseconds.
- Average Go heap by 30-minute window was 9.52, 10.16, 10.04, and 9.93 MB;
  window maxima were 12.74, 12.88, 13.01, and 13.12 MB. Final sampled heap was
  8.73 MB. This is a bounded plateau rather than monotonic growth.
- After the second worker joined, average goroutines stayed near 282-289 per
  window with maxima 290-292; final post-drain goroutines were 69. The benchmark
  high-frequency peak was 307.
- Launcher RSS window averages rose slowly from 31.3 MB to 35.8 MB, with a
  36.5 MB maximum. Kafka container memory repeatedly reset on broker restart and
  otherwise plateaued around 1 GiB.
- 231 allocated-volume samples peaked/finalized at 355,037,184 bytes. Maximum
  sampled lag was 900 and final lag was zero. Cleanup was verified.

The runner now supports fail-closed repeated restart cadence through
`--broker-restart-every`, records `broker_restart_count`, and requires every
scheduled action to occur during observed active work. Final artifacts are under
`/tmp/thothan/p676-production-evidence/20261020T083539506Z-kafka-retry10-soak-7200.0s-100-kafka-2h-retry10-restart-rebalance-soak-e9397c7d/`.
This closes one qualifying multi-hour sustained Kafka fault/rebalance soak. It
does not close the remaining 100k/1m production matrix cells.
