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
This remains historical harness evidence. Its apparent-size disk claim is
superseded by the allocated-volume rerun below.

The final Redis 100,000-message immediate/preload allocated-volume rerun was
accepted on 2026-10-22 with 1 KiB payloads, concurrency 100, and
`redis_delay_batch_size=1000`:

- 100,000 unique messages completed with exactly 100,000 handler invocations,
  zero duplicates, and all final pending, delayed, in-flight, DLQ, and lag
  counters zero.
- The staged backlog reached exactly 100,000 pending/lag before resume. Drain
  throughput was 4,569.01 messages/second over 21.89 seconds.
- Three preload and five measurement topology samples were available. Peak heap
  was 4,722,304 bytes, peak goroutines were 107, post-drain heap was 2,785,328
  bytes, and five expected lifecycle goroutines remained after drain.
- Six broker samples used `du_allocated_1k_blocks`; allocated Redis data peaked
  at 56,315,904 bytes. Cleanup was verified.

The post-drain heap and goroutine profiles showed no retained backlog owner or
blocked goroutine. Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T043021568Z-redis-immediate-100000-redis-100k-immediate-preload-allocated-final-d13838dd/`.
This closes the Redis immediate/preload 100k matrix cell. The canonical matrix is
now 17/24 complete; seven 100k workload cells remain open before
payload/concurrency expansion.

The Redis 100,000-message delayed/preload cell was accepted on 2026-10-22 with
1 KiB payloads, concurrency 100, a five-second requested delay, and
`redis_delay_batch_size=1000`:

- 100,000 unique messages completed with exactly 100,000 handler invocations,
  zero duplicates, and all final pending, delayed, in-flight, DLQ, and lag
  counters zero.
- Preload ended with 92,000 delayed and 8,000 pending/lag. During measurement,
  delayed obligations fell from 87,000 to 2,000 in the periodic samples while
  completions rose from 13,000 to 98,000, proving scheduler transfer rather than
  immediate bypass.
- Drain throughput was 1,075.03 messages/second over 93.02 seconds. The
  49.94/90.32/94.89-second p50/p95/p99 includes staged delayed-backlog age.
- Peak heap was 4,786,624 bytes, peak goroutines were 107, post-drain heap was
  2,833,200 bytes, and five expected lifecycle goroutines remained.
- Eighteen broker samples used `du_allocated_1k_blocks`; allocated Redis data
  peaked at 85,630,976 bytes. Cleanup was verified.

Post-drain profiles showed no retained delayed backlog owner or blocked stack.
Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T043755913Z-redis-delayed-100000-redis-100k-delayed-preload-allocated-final-55e5a2dc/`.
The canonical matrix is now 18/24 complete; six 100k workload cells remain open.

The Redis 100,000-message `retry10` cell was accepted on 2026-10-22 with
1 KiB payloads, concurrency 100, and `redis_delay_batch_size=1000`:

- 100,000 unique messages completed with all 10,000 expected retry candidates
  failing exactly once at attempt one. Total handler accounting was exact at
  110,000 invocations.
- Zero duplicates were observed and all final pending, delayed, in-flight, DLQ,
  and lag counters were zero.
- Throughput was 4,315.18 unique completions/second over 23.17 seconds;
  p50/p95/p99 latency was 63.60/647.68/1,010.09 milliseconds. Sampled pending
  and lag remained below 629.
- Peak heap was 4,508,648 bytes, peak goroutines were 107, post-drain heap was
  2,635,776 bytes, and five expected lifecycle goroutines remained.
- Four broker samples used `du_allocated_1k_blocks`; allocated Redis data peaked
  at 59,424,768 bytes. Cleanup was verified.

Post-drain profiles showed no retained retry backlog owner or blocked stack.
Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T044504112Z-redis-retry10-100000-redis-100k-retry10-allocated-final-34bfce33/`.
Redis now has all four canonical workloads closed at both 100k and 1m. The
canonical matrix is 19/24 complete; five NATS/Kafka 100k cells remain open.

The NATS 100,000-message immediate/preload cell was accepted on 2026-10-22 with
1 KiB payloads, concurrency 100, and `nats_delay_batch_size=1000`:

- 100,000 unique messages completed with exactly 100,000 handler invocations,
  zero duplicates, and all final pending, delayed, in-flight, DLQ, and lag
  counters zero.
- Preload reached exactly 100,000 pending/lag. Four measurement topology samples
  showed pending/lag falling from 77,080 to 28,408 before final drain.
- Measurement throughput was 25,427.32 messages/second over 3.93 seconds. Staged
  p50/p95/p99 latency was 9.32/12.53/13.17 seconds.
- Peak heap was 5,804,128 bytes, peak goroutines were 115, post-drain heap was
  3,123,424 bytes, and eight expected NATS lifecycle goroutines remained.
- Eight broker samples used `du_allocated_1k_blocks`, including two measurement-
  phase samples. Allocated JetStream data peaked at 152,023,040 bytes. Cleanup
  was verified.

Post-drain profiles contained no blocked KV watcher callback. An earlier run with
five-second sampling completed correctness but drained before the broker sampler
recorded a measurement-phase volume sample. It exposed a Bash validator bug where
a later successful preload assertion overwrote the failed measurement-volume
assertion. Every evidence assertion now returns fail-closed, and Bash/PowerShell
self-tests reject missing measurement broker evidence. The earlier artifact is
diagnostic only. Accepted artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T045001546Z-nats-immediate-100000-nats-100k-immediate-preload-measurement-volume-final-46b01d88/`.
The canonical matrix is now 20/24 complete; four NATS/Kafka 100k cells remain.

The NATS 100,000-message `retry10` cell was accepted on 2026-10-22 with
1 KiB payloads, concurrency 100, and `nats_delay_batch_size=1000`:

- 100,000 unique messages completed with all 10,000 expected retry candidates
  failing exactly once at attempt one. Total handler accounting was exact at
  110,000 invocations.
- Zero duplicates were observed and all final pending, delayed, in-flight, DLQ,
  and lag counters were zero.
- Throughput was 6,571.82 unique completions/second over 15.22 seconds;
  p50/p95/p99 latency was 0.37/714.91/961.43 milliseconds. Periodic delayed
  obligations stayed between 673 and 731 during steady production, while sampled
  lag remained at or below five.
- Peak heap was 10,395,232 bytes, peak goroutines were 113, post-drain heap was
  2,994,912 bytes, and eight expected NATS lifecycle goroutines remained.
- Eight measurement-phase broker samples used `du_allocated_1k_blocks`;
  allocated JetStream data peaked at 207,540,224 bytes. Cleanup was verified.

Post-drain profiles showed no retained retry backlog owner or blocked KV watcher.
Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T045339836Z-nats-retry10-100000-nats-100k-retry10-allocated-final-2048d938/`.
NATS now has all four canonical workloads closed at both 100k and 1m. The
canonical matrix is 21/24 complete; three Kafka 100k cells remain open.

The Kafka 100,000-message immediate/preload cell was accepted on 2026-10-22
with 1 KiB payloads, concurrency/partitions 100, and
`kafka_delay_batch_size=1000` recorded for harness identity:

- 100,000 unique messages completed with exactly 100,000 handler invocations,
  zero duplicates, and all final pending, delayed, in-flight, DLQ, and lag
  counters zero.
- Measurement-only drain throughput was 4,848.85 messages/second over 20.62
  seconds. p50/p95/p99 was 23.81/39.01/43.06 seconds because preload latency
  includes staged-backlog age.
- All 46 topology samples were available: 25 preload and 21 measurement
  samples. Peak heap was 286,731,512 bytes and returned to 3,417,848 bytes after
  drain, showing a backlog-proportional working set rather than retained growth.
- Peak goroutines were 148 and 36 expected Kafka client, consumer, scheduler,
  and health-check goroutines remained before transport close. Post-drain
  profiles showed no blocked application channel or unbounded worker set.
- All 26 broker samples used `du_allocated_1k_blocks`; allocated Kafka data
  peaked at 77,778,944 bytes. Cleanup was verified.

The first cold-fixture attempt failed before workload execution because creating
the main, DLQ, and three delay topics with 100 partitions exceeded the prior
fixed 20-second startup deadline. The benchmark runner now allows Kafka two
minutes for topology provisioning while other drivers retain 20 seconds; this
does not change the reusable library or demo defaults. Accepted artifacts are
under
`/tmp/thothan/p676-production-evidence/20261022T051247081Z-kafka-immediate-100000-kafka-100k-immediate-preload-startup-fixed-final-1d9d4f4f/`.
The diagnostic startup artifact is under
`/tmp/thothan/p676-production-evidence/20261022T050926209Z-kafka-immediate-100000-kafka-100k-immediate-preload-allocated-final-072597a1/`.
The canonical matrix is 22/24 complete; Kafka delayed and retry10 at 100k remain.

The Kafka 100,000-message delayed/preload cell was accepted on 2026-10-22 with
1 KiB payloads, concurrency/partitions 100, a five-second delay, and
`kafka_delay_batch_size=1000`:

- 100,000 unique messages completed with exactly 100,000 handler invocations,
  zero duplicates, and all final pending, delayed, in-flight, DLQ, and lag
  counters zero.
- Measurement-only throughput was 204.75 messages/second over 488.41 seconds.
  p50/p95/p99 was 254.74/484.04/508.54 seconds because latency includes delayed
  scheduler and staged-backlog wait.
- Preload ended at 97,182 delayed plus 2,818 pending/lag. The first measurement
  sample completed those 2,818 ready records while 97,182 remained delayed; the
  final sample reached 100,000 completions with no obligations. All 511 topology
  samples were available.
- Measurement heap averaged 126.48 MB, peaked at 180.76 MB, and ended at 7.50
  MB; the high-frequency peak was 180.87 MB and post-drain heap was 3.54 MB.
  Quarter averages were 113.63/118.44/136.93/136.84 MB while average delayed
  work fell from 85,084 to 12,533. Post-drain profiling showed no retained
  backlog owner.
- Peak goroutines were 153 and 35 expected Kafka client, consumer, scheduler,
  and health-check goroutines remained before transport close. There was no
  blocked application channel or unbounded worker set.
- All 241 broker samples used `du_allocated_1k_blocks`; allocated Kafka data
  peaked/finalized at 156,930,048 bytes. Maximum sampled container memory was
  about 1,032 MiB. Broker logs contained no runtime, controller, or storage
  errors, and cleanup was verified.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T052034500Z-kafka-delayed-100000-kafka-100k-delayed-preload-final-b365233f/`.
The 204.75 msg/s result closely matches the accepted Kafka 1m delayed result of
206.98 msg/s, confirming a stable scheduler throughput ceiling that remains a
production tuning concern. The canonical matrix is 23/24 complete; only Kafka
retry10 at 100k remains.

The Kafka 100,000-message `retry10` cell was accepted on 2026-10-22 with 1 KiB
payloads, concurrency/partitions 100, and `kafka_delay_batch_size=1000`:

- 100,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- All 10,000 expected retry candidates failed exactly once at attempt one. No
  candidate was first observed after attempt one, and handler accounting was
  exactly 110,000 invocations.
- Throughput was 5,157.48 unique completions/second over 19.39 seconds;
  p50/p95/p99 latency was 1.49/198.02/481.61 milliseconds.
- All 20 topology samples were available. Maximum sampled pending/lag was 898;
  final lag was zero.
- Sampled heap averaged 5.02 MB, peaked at 7.61 MB, and ended at 4.46 MB;
  high-frequency peak was 7.74 MB and post-drain heap was 3.42 MB. Quarter
  averages were 5.85/4.58/5.07/4.56 MB, showing a bounded plateau.
- Peak goroutines were 155, the final sampled value was 34, and 33 expected
  Kafka lifecycle goroutines remained before transport close. Post-drain
  profiles showed no retained retry owner, blocked application channel, or
  unbounded worker set.
- All 14 broker samples used `du_allocated_1k_blocks`; allocated Kafka data
  peaked/finalized at 82,849,792 bytes. Maximum sampled container memory was
  about 986.6 MiB. Broker logs were clean and cleanup was verified.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T054124064Z-kafka-retry10-100000-kafka-100k-retry10-final-35219fac/`.
This closes the final canonical cell. The 24-cell Redis/NATS/Kafka x
immediate/delayed/retry10/retry50 x 100k/1m matrix is complete at the canonical
1 KiB/concurrency-100 dimension. The subsequent full cross-artifact accounting,
volume, cleanup, resource, heap-profile, and goroutine-profile review is recorded
in `P6.7.6_MATRIX_REVIEW.md` and passed after replacing legacy profile-less
artifacts with current-schema reruns. PowerShell/Windows execution remains an
optional cross-platform tooling compatibility check.

The fixture images were then pinned to immutable Redis, NATS, and Kafka digests.
Registry pulls, simultaneous healthy startup, runtime versions, container image
IDs, Compose rendering, and project-scoped teardown all passed. This closed the
final defined P6.7.6 gate; broader dimensions and Windows tooling execution are
optional follow-ups. The pinned digests are Redis `c1e88455...`, NATS
`73b0ba5f...`, and Kafka `c89f315c...` as recorded in `PRODUCTION_SCALE.md`.

The final Redis 1,000,000-message immediate/preload allocated-volume rerun was
accepted on 2026-10-20 with 1 KiB payloads and concurrency 100:

- 1,000,000 unique completions, zero observed duplicates, and all final topology
  counters zero.
- Measurement-only drain throughput was 4,712.62 messages/second over 212.20
  seconds.
- 26 preload samples, 43 measurement samples, and 57 broker samples had valid
  phases and numeric allocated-volume measurements.
- Maximum pending/lag was the intended 1,000,000-message backlog. Allocated
  Redis data measured by `du_allocated_1k_blocks` peaked at 559,230,976 bytes;
  preload ended at 424,026,112 bytes and the last measurement was 415,813,632
  bytes.
- Measurement sampled heap averaged 9,140,063 bytes, peaked at 11,941,968 bytes,
  and ended at 9,435,104 bytes. High-frequency peak heap was 12,391,568 bytes
  and peak goroutines were 107.
- p50/p95/p99 was 180.30/219.67/219.67 seconds because preload latency includes
  time spent waiting in the staged backlog; it is backlog age, not live-flow
  handler latency.
- Execution and acceptance passed, and project-scoped cleanup was verified.

Artifacts are under the ignored local path
`/tmp/thothan/p676-production-evidence/20261020T105513919Z-redis-immediate-1000000-redis-1m-immediate-preload-allocated-2ce085c7/`.
The earlier accepted artifact remains historical performance evidence but its
apparent-size volume values are superseded by this allocated-block rerun.
This closes only the Redis immediate/preload 1m cell. Other Redis workloads and
the remaining NATS/Kafka matrix cells remain open.

The Redis 1,000,000-message delayed/preload cell was accepted on 2026-10-21
with 1 KiB payloads, concurrency 100, a requested five-second delay, and
`redis_delay_batch_size=1000`:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- Measurement drain took 908.34 seconds at 1,100.91 messages/second. Periodic
  topology sampling remained enabled and all 202 samples had available stats.
- Sampled maxima were 908,000 delayed and 92,000 pending/lag, demonstrating the
  scheduler transfer during drain rather than immediate-delivery bypass.
- Measurement sampled heap averaged 9.22 MB, peaked at 12.08 MB, and ended at
  10.80 MB; the high-frequency peak was 12.43 MB. Peak goroutines were 107 and
  the final sampled value was six.
- 164 allocated-volume samples peaked at 884,355,072 bytes and ended at
  230,834,176 bytes. Cleanup was verified.
- p50/p95/p99 was 460.72/875.40/919.71 seconds and represents scheduler plus
  staged-backlog age, not live handler latency.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261021T070920961Z-redis-delayed-1000000-redis-1m-delayed-preload-allocated-7b48cf4d/`.
This closes the Redis delayed/preload 1m matrix cell, not the remaining Redis
retry workloads or the full production matrix.

The Redis 1,000,000-message `retry50` cell was accepted on 2026-10-21 with
1 KiB payloads, concurrency 100, and `redis_delay_batch_size=1000`:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- All 500,000 retry candidates failed exactly once at attempt one, producing an
  exactly accounted 1,500,000 handler invocations.
- Throughput was 657.87 unique completions/second over 1,520.07 seconds;
  p50/p95/p99 latency was 340.87/1,010.09/27,613.29 milliseconds.
- All 305 periodic topology samples were available. Sampled maxima were 899
  pending, 900 delayed, three in-flight, and 901 lag.
- Sampled heap averaged 9.57 MB, peaked at 12.60 MB, and ended at 10.74 MB; the
  high-frequency peak was 12.64 MB. Peak goroutines were 107 and the final
  sampled value was six.
- 248 allocated-volume samples peaked at 549,867,520 bytes and ended at
  359,661,568 bytes. Cleanup was verified.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261021T080001972Z-redis-retry50-1000000-redis-1m-retry50-df712b70/`.
This closes the Redis retry50 1m matrix cell, not Redis retry10 or the full
production matrix.

The Redis 1,000,000-message `retry10` cell was accepted on 2026-10-21 with
1 KiB payloads, concurrency 100, and `redis_delay_batch_size=1000`:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- All 100,000 retry candidates failed exactly once at attempt one, producing an
  exactly accounted 1,100,000 handler invocations.
- Throughput was 3,256.32 unique completions/second over 307.09 seconds;
  p50/p95/p99 latency was 126.97/751.10/1,114.93 milliseconds.
- All 62 periodic topology samples were available. Sampled maxima were 898
  pending, 217 delayed, three in-flight, and 900 lag.
- Sampled heap averaged 8.99 MB, peaked at 11.42 MB, and ended at 6.89 MB; the
  high-frequency peak was 12.23 MB. Four equal-window averages declined from
  9.30/9.39 MB to 8.72/8.55 MB. Peak goroutines were 107 and the final sampled
  value was six.
- 51 allocated-volume samples peaked/finalized at 470,380,544 bytes. Cleanup was
  verified.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261021T091703321Z-redis-retry10-1000000-redis-1m-retry10-dba56d28/`.
This completes the immediate, delayed, retry10, and retry50 Redis 1m workload
set at the canonical 1 KiB/concurrency-100 dimension. It does not close Redis
100k, payload/concurrency expansion, or the full production matrix.

The final NATS 100,000-message delayed/preload allocated-volume rerun was
accepted on 2026-10-20 with 1 KiB payloads, concurrency 100, a requested
5-second delay, and an explicitly recorded `nats_delay_batch_size` of 1,000:

- 100,000 unique completions, zero observed duplicates, and all final topology
  counters zero.
- Measurement throughput was 1,144.94 messages/second over 87.34 seconds.
- The first measurement sample had 15,000 completed and 85,000 delayed; sampled
  maxima were 87,708 delayed during preload, 85,000 delayed during measurement,
  12,357 pending during preload, and one in-flight obligation,
  demonstrating scheduler transfer rather than immediate bypass.
- 10 preload samples, 44 measurement samples, and 34 fully phase-labeled numeric
  broker samples were recorded. Allocated JetStream data measured by
  `du_allocated_1k_blocks` peaked/finalized at 391,983,104 bytes.
- Measurement sampled heap averaged 35,543,654 bytes, peaked at 62,493,768 bytes,
  and ended at 34,328,856 bytes. High-frequency peak heap was 118,494,192 bytes,
  peak goroutines were 121, and project-scoped cleanup was verified.
- p50/p95/p99 was 49.94/85.97/90.32 seconds because preload latency includes
  scheduler and staged-backlog wait time rather than only live handler latency.

An earlier diagnostic run inherited the demo batch size of 100 and required
987.67 seconds, about 101.25 messages/second. It exposed a benchmark-configuration
ceiling rather than a correctness failure. The accepted run overrides only the
benchmark topology, records the effective batch in both manifest and result, and
does not change the library or demo default. Final artifacts are under
`/tmp/thothan/p676-production-evidence/20261020T110642726Z-nats-delayed-100000-nats-100k-delayed-preload-allocated-733ca13b/`.
The earlier accepted artifact remains historical performance evidence but its
apparent-size volume claim is superseded by this allocated-block rerun.
This closes only the NATS delayed/preload 100k cell.

The NATS 1,000,000-message delayed/preload cell was accepted on 2026-10-21 with
1 KiB payloads, concurrency 100, a five-second requested delay, and
`nats_delay_batch_size=1000`:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- Measurement drain took 2,925.38 seconds at 341.84 messages/second. This is
  materially slower than the 100k cell and records nonlinear scheduler/storage
  cost at the 1m backlog rather than claiming linear scale.
- Periodic topology sampling was intentionally disabled while resource and broker
  sampling remained active. Exact NATS delayed stats enumerate live KV keys; at
  1m backlog, repeated scans distorted the scheduler workload. Final exact stats
  after drain remained mandatory and passed.
- 11 preload and 98 measurement resource samples were recorded. Measurement
  sampled heap averaged 155.82 MB, peaked at 307.33 MB, and ended at 186.94 MB;
  high-frequency peak was 386.82 MB. Peak goroutines were 163 and the final
  sampled value was 61.
- Measurement heap averages across four equal windows were 152.3, 136.7, 155.5,
  and 178.6 MB. The profile is GC-variable with a higher final window, but not a
  monotonic per-window rise; it should remain a tuning concern for larger cells.
- Launcher RSS averaged 239.8 MB during measurement, peaked at 366.6 MB, and
  ended at 289.9 MB. Final NATS container memory was about 1.259 GiB.
- 105 allocated-volume samples peaked at 2,641,719,296 bytes and ended at
  2,192,113,664 bytes. Cleanup was verified.
- p50/p95/p99 was 1,299.46/2,863.31/3,008.24 seconds and represents scheduler
  plus staged-backlog age, not live handler latency.

Two diagnostic runs with periodic exact topology scans are not acceptance
evidence. They demonstrated that client-side enumeration of up to 1m KV keys
caused unavailable samples, increased heap/CPU, and reduced throughput. The
benchmark now supports an explicit recorded resource-only periodic mode without
relaxing final exact stats. Final artifacts are under
`/tmp/thothan/p676-production-evidence/20261021T043246685Z-nats-delayed-1000000-nats-1m-delayed-preload-resource-only-final-1e7ece30/`.
This closes the NATS delayed/preload 1m matrix cell with the observed nonlinear
performance limitation documented.

The NATS 1,000,000-message immediate/preload cell was accepted on 2026-10-22
with 1 KiB payloads, concurrency 100, and `nats_delay_batch_size=1000` recorded
for harness identity:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- Measurement-only drain throughput was 32,322.30 messages/second over 30.94
  seconds. p50/p95/p99 was 74.13/121.46/127.61 seconds because preload latency
  includes staged-backlog age rather than only live handler latency.
- All 32 topology samples were available. Preload ended with 1,000,000 pending/
  lag; the first measurement sample had 159,894 completed, 840,028 pending,
  31 in-flight, and 840,059 lag.
- Measurement sampled heap averaged 11.36 MB, peaked at 13.53 MB, and ended at
  9.40 MB; the high-frequency peak was 13.53 MB. Quarter-window averages were
  9.92/11.38/12.52/10.90 MB, showing a bounded profile.
- Peak goroutines were 120 and the final sampled value was nine. Final NATS
  container memory was about 404.7 MiB.
- 26 allocated-volume samples peaked/finalized at 1,520,283,648 bytes. Cleanup
  was verified.

The fast drain produced seven measurement resource samples and four measurement
broker samples; phase validation, periodic topology stats, final exact stats, and
allocated-volume requirements all passed. Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T022746582Z-nats-immediate-1000000-nats-1m-immediate-preload-1617f76a/`.
This closes the NATS immediate/preload 1m cell, not retry10/retry50 or the full
production matrix.

The NATS 1,000,000-message `retry10` cell was accepted on 2026-10-22 with
1 KiB payloads, concurrency 100, and `nats_delay_batch_size=1000`:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- All 100,000 retry candidates failed exactly once at attempt one, producing an
  exactly accounted 1,100,000 handler invocations.
- Throughput was 6,717.80 unique completions/second over 148.86 seconds;
  p50/p95/p99 latency was 0.36/714.91/1,010.09 milliseconds.
- Twenty-nine of 30 periodic topology samples were available. One transient stats
  read at 45 seconds was unavailable; subsequent samples and mandatory final exact
  stats passed. Sampled maxima were one pending, 742 delayed, five in-flight, and
  five lag.
- Sampled heap averaged 33.80 MB, peaked at 71.70 MB, and ended at 49.54 MB; the
  high-frequency peak was 76.08 MB. Five equal time-window averages rose from
  10.45 to 55.33 MB during the continuous high-allocation finite run. This proves
  a bounded peak for this cell, not a standalone long-soak plateau claim.
- Peak goroutines were 128 and the final sampled value was 24. Final NATS
  container memory was about 1.21 GiB.
- 25 allocated-volume samples peaked/finalized at 1,844,441,088 bytes. Cleanup
  was verified.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T023734743Z-nats-retry10-1000000-nats-1m-retry10-e59b4629/`.
This closes the NATS retry10 1m cell with its finite-run resource caveat recorded,
not retry50 or the full production matrix.

The NATS 1,000,000-message `retry50` cell was accepted on 2026-10-22 with
1 KiB payloads, concurrency 100, and `nats_delay_batch_size=1000`:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- All 500,000 retry candidates failed exactly once at attempt one, producing an
  exactly accounted 1,500,000 handler invocations.
- Throughput was 1,732.52 unique completions/second over 577.19 seconds;
  p50/p95/p99 latency was 22.55/1,292.95/2,711.71 milliseconds.
- All 58 periodic topology samples were available. Peak heap was 23,705,176
  bytes and peak goroutines were 113. After final drain and synchronous GC, heap
  was 6,852,560 bytes and eight goroutines remained.
- The post-drain goroutine profile contained no blocked KV watcher callback.
  Allocated JetStream data peaked at 3,065,737,216 bytes across 53 numeric
  broker/volume samples. Cleanup was verified.

Earlier retry50 diagnostics completed message accounting but retained 102-120
goroutines and roughly 263-293 MB after drain. Profiles traced the retention to
NATS KV watcher callbacks blocked at `kv.go:1082`. NATS delayed scheduling and
statistics now use bounded ephemeral pull snapshots instead of callback watchers,
including cursor recovery, slot dispatch, legacy migration, topology counts, and
subscriber retry-lane inspection. A 100k diagnostic after the fix retained only
3,079,416 bytes and eight goroutines, with no blocked watcher stack. Final 1m
artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T035005800Z-nats-retry50-1000000-nats-1m-retry50-no-watchers-final-8db14bd9/`.
This closes all four NATS 1m workloads only at the canonical
1 KiB/concurrency-100 dimension, not the full production matrix.

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

The Kafka 1,000,000-message `retry10` cell was accepted on 2026-10-21 with
1 KiB payloads, concurrency 100, and 100 partitions:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- All 100,000 unique retry candidates failed exactly once at attempt one, for an
  exactly accounted 1,100,000 handler invocations.
- Throughput was 4,772.28 unique completions/second over 209.54 seconds;
  p50/p95/p99 latency was 1.35/4.88/126.97 milliseconds.
- All 42 periodic topology samples were available. Maximum sampled pending/lag
  was 295 and final lag was zero.
- Sampled heap averaged 10.30 MB, peaked at 13.71 MB, and ended at 10.17 MB;
  high-frequency peak was 14.97 MB. Four equal-window heap averages were 10.71,
  9.45, 10.42, and 10.67 MB, showing a bounded plateau.
- Peak goroutines were 155 and the final sampled value was 34. Launcher RSS
  averaged 33.6 MB, peaked at 37.2 MB, and ended at 37.2 MB.
- 36 allocated-volume samples peaked/finalized at 565,137,408 bytes. Final Kafka
  container memory was about 1.083 GiB and cleanup was verified.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261021T065611580Z-kafka-retry10-1000000-kafka-1m-retry10-b39b2ed2/`.
This closes the Kafka retry10 1m matrix cell.

The Kafka 1,000,000-message delayed/preload cell was accepted on 2026-10-21 with
1 KiB payloads, concurrency/partitions 100, a requested five-second delay, and
`kafka_delay_batch_size=1000`:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- Measurement drain took 4,831.38 seconds at 206.98 messages/second. This is a
  material production tuning concern, not a claim of linear delayed-backlog scale.
- All 1,008 periodic topology samples were available. The first measurement
  sample had 28,620 completed, 960,238 delayed, and 11,134 pending/lag; sampled
  maxima were 961,238 delayed and 38,762 pending/lag, demonstrating durable tier
  transfer rather than immediate-delivery bypass.
- Measurement sampled heap averaged 334.20 MB, peaked at 452.22 MB, and ended at
  8.25 MB; the high-frequency peak was 499.37 MB. Quarter averages were
  328.14/350.92/343.65/314.08 MB while delayed backlog declined, and quarter
  maxima remained near 442-452 MB. This is backlog-proportional working set, not
  retained post-drain growth.
- Peak goroutines were 154 and the final sampled value was 36. Kafka container
  memory ended around 1.228 GiB.
- 819 allocated-volume samples peaked at 885,276,672 bytes and ended at
  848,666,624 bytes. Cleanup was verified.
- p50/p95/p99 was 2,469.08/4,691.47/4,928.94 seconds and represents scheduler
  plus staged-backlog age, not live handler latency.

The benchmark now overrides and records Kafka's validated maximum delay batch of
1,000 for production evidence while leaving the reusable library/demo default at
100. Both orchestrators validate the recorded value fail-closed. Artifacts are
under
`/tmp/thothan/p676-production-evidence/20261021T095244081Z-kafka-delayed-1000000-kafka-1m-delayed-preload-bcfaa9da/`.
This closes the Kafka delayed/preload 1m matrix cell with its nonlinear scheduler
cost documented, not the remaining Kafka or full production matrix.

The Kafka 1,000,000-message `retry50` cell was accepted on 2026-10-22 with
1 KiB payloads, concurrency/partitions 100, and
`kafka_delay_batch_size=1000`:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- All 500,000 retry candidates failed exactly once at attempt one, producing an
  exactly accounted 1,500,000 handler invocations.
- Throughput was 4,124.16 unique completions/second over 242.47 seconds;
  p50/p95/p99 latency was 208.04/415.30/531.60 milliseconds.
- All 49 periodic topology samples were available. Maximum sampled pending/lag
  was 909 and final lag was zero.
- Sampled heap averaged 12.20 MB, peaked at 15.67 MB, and ended at 8.48 MB; the
  high-frequency peak was 16.32 MB. Four equal-window averages were
  12.46/11.62/11.80/12.88 MB, showing a bounded plateau.
- Peak goroutines were 161 and the final sampled value was 34. Final Kafka
  container memory was about 1,017 MiB.
- 42 allocated-volume samples peaked/finalized at 734,973,952 bytes. Cleanup was
  verified.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T005504565Z-kafka-retry50-1000000-kafka-1m-retry50-7c9299ec/`.
This closes both Kafka 1m retry workloads at the canonical 1 KiB/concurrency-100
dimension, not Kafka immediate, remaining 100k cells, or the full matrix.

The Kafka 1,000,000-message immediate/preload cell was accepted on 2026-10-22
with 1 KiB payloads, concurrency/partitions 100, and
`kafka_delay_batch_size=1000` recorded for harness identity:

- 1,000,000 unique messages completed with zero duplicates and all final pending,
  delayed, in-flight, DLQ, and lag counters zero.
- Measurement-only drain throughput was 4,177.66 messages/second over 239.37
  seconds. p50/p95/p99 was 242.47/326.08/342.59 seconds because preload latency
  includes staged-backlog age rather than only live handler latency.
- All 99 topology samples were available. Preload ended with 1,000,000 pending/
  lag and 8.38 MB heap; the first measurement sample had 17,710 completed and
  982,277 pending/lag.
- Measurement sampled heap averaged 330.31 MB, peaked at 442.72 MB, and ended at
  32.38 MB; the high-frequency peak was 536.87 MB. Quarter averages were
  337.18/357.93/367.64/258.50 MB while average pending declined from 862,154 to
  116,698. Quarter maxima stayed near 399-443 MB. This is a backlog-proportional
  verifier/consumer working set, not retained post-drain growth.
- Peak goroutines were 150 and the final sampled value was 34. Final Kafka
  container memory was about 1,008 MiB.
- 82 allocated-volume samples peaked at 570,462,208 bytes and ended at
  505,024,512 bytes. Cleanup was verified.

Artifacts are under
`/tmp/thothan/p676-production-evidence/20261022T020915669Z-kafka-immediate-1000000-kafka-1m-immediate-preload-9211c10c/`.
This completes all four Kafka 1m workloads at the canonical
1 KiB/concurrency-100 dimension. Remaining Kafka 100k and payload/concurrency
dimensions keep the production matrix open.

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
