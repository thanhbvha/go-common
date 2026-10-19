# ADR-002: Accept the Phase 6 EventBus performance baseline

- Status: Accepted
- Date: 2026-10-18
- Applies to: EventBus core and transport benchmark gates

## Context

The Phase 0 baseline predates the v3 envelope, strict validation, trace
propagation, source normalization, bounded worker pools, lease heartbeat and
settlement instrumentation, topology controls, admission policies, and payload
limits. The Phase 6 exit criterion requires an ADR when a benchmark exceeds the
agreed 20% regression threshold.

On the same Windows/amd64 i7-11800H host and Go toolchain, the median of three
runs changed as follows:

| Benchmark | Phase 0 ns/op | Phase 6 ns/op | Change | Phase 6 B/op, allocs/op |
| --- | ---: | ---: | ---: | ---: |
| CodecEncode | 1,573 | 1,527 | -2.9% | 592, 6 |
| CodecDecode | 495.1 | 485.4 | -2.0% | 512, 4 |
| Enqueue | 870.1 | 1,600 | +83.9% | 1,201, 11 |
| EngineDispatch | 6,130 | 13,745 | +124.2% | 4,335, 47 |
| MemoryTransportRoundTrip | 1,182 | 1,231 | +4.1% | 200, 2 |

## Decision

Accept the Enqueue and EngineDispatch regressions and the Memory transport's
small absolute allocation increase. They purchase required correctness and
operability guarantees; removing those checks or hooks from the default path is
not an acceptable optimization. Codec throughput did not regress, and the raw
transport remains within the latency threshold.

The figures above become the Phase 6 micro-benchmark baseline. On the same
class of environment, a median latency or allocation regression above 20% from
this baseline requires investigation and either remediation or another ADR.
Use at least three runs and compare like-for-like Go versions and host load.

## Consequences

Enqueue and dispatch allocation reduction is worthwhile future work, especially
avoiding redundant envelope materialization after trace injection and reducing
per-delivery context/span bookkeeping. Any optimization must retain payload
validation before broker I/O, propagation, lease safety, bounded concurrency,
confirmed settlement, and observability behavior.

## P6.7.2 Memory hardening addendum

P6.7.2 replaces Memory's channel fast path with atomic multi-subscriber capacity
checks, queued/in-flight redelivery state, metadata isolation, a bounded heap
scheduler, scheduler health, and source-scoped retry/DLQ transfer. A Linux/amd64
i7-12700F smoke run with Go 1.26.0 and `-benchtime=100x` measured:

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| EngineDispatch | 14,859 | 6,390 | 69 |
| MemoryTransportRoundTrip | 3,042 | 1,333 | 20 |

These figures are not directly comparable to the Windows Phase 6 baseline, but
the allocation shape confirms an intentional implementation cost. Accept it for
P6.7.2 because the previous fast path returned success after drops, could lose
pre-consume events, and created one timer goroutine per delayed message. Future
optimization may reduce cloning and state allocations, but must retain atomic
fan-out, bounded admission, redelivery, subscriber isolation, and joined
scheduler behavior.
