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
