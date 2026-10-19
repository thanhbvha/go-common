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

Artifacts are intentionally ignored because they contain machine-local paths
and high-volume samples. This single matrix cell validates the harness, not the
P6.7.6 release gate. The 1,000,000-message runs, remaining workloads/drivers,
fault/restart/rebalance cases, and sustained soak remain open.
