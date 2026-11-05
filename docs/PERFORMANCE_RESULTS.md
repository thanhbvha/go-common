# Performance evidence collected on 2026-11-04

Audited on 2026-11-05 after the chat usage limit interrupted progress reporting. The already-running process completed all measurements; no long benchmark was repeated during this audit. SHA-256 inventories and expected sample counts were verified before finishing documentation and CI configuration.

## Provenance and completed runs

Parent revision: `a18386e1c53bdc51ae397e7e9db803b9ae9c0b9f` on `v2-production`, with uncommitted implementation changes. This is local working-tree evidence, not an executed GitHub Actions run or release attestation. Final documentation/CI edits follow the captured source snapshot; the measurement harness remains unchanged.

Host: 12th Gen Intel(R) Core(TM) i7-12700F, Linux/amd64, Go 1.26.10. CPU governor: `powersave`. Cgroup CPU/memory limits were unavailable to the collector. Full environment details, image identities and command records remain in each run directory.

| Artifact under `artifacts/performance/` | Result |
|---|---|
| `core-baseline-final/` | 34 cases × 2 CPU settings × 10 repetitions = 680 Go samples |
| `integration-baseline-verified/` | 20 PostgreSQL cursor + 2,240 Redis registry Go samples; 70 timed load scenario results |
| `core-cpu-profile/` | Eight package CPU profiles before the runtime optimization |
| `integration-cpu-profile/` | Seven workloads, 30 profile files including live/post-drain heap and allocation snapshots, with matching binaries and verified `pprof -top` reports |
| `health-ab-final/` | Ten alternating AB/BA pairs; 20 samples per side across CPU 1 and 4 |
| `health-cpu-after/`, `health-memory-before/`, `health-memory-after/` | Focused before/after evidence; profiler timings excluded from comparisons |
| `health-mutex-check/`, `health-block-check/` | Profile collection/readback checks |
| `final-checks/` | Build, vet, module integrity, boundaries, full race suite, integration-code vet and whitespace checks passed after the runtime change |
| `resume-audit.json` | Saved checksum/inventory audit of the completed runs |

Measurement source fingerprint: `b5606678fadee646a09ca786b4a63f4dfc04946f4762ec5e6c5310731304a254`.

Harness fingerprint: `212cd787bb81d0cbdb643b4927513a799e32db430fe2cfc3aa640e57e0bd7d58`.

Manifest SHA-256: `0b6c9cd9bf220ef247122f0698fe8b23af8ef4167d4d1d54ddf76a8ca5095686`.

The images and benchstat revision are locked in [the manifest](../benchmarks/manifest.json). Raw logs/binaries stay in ignored `artifacts/`; preserve or archive that directory with any shared report. `health-ab-final/baseline-source.tar.gz` and `candidate-source.tar.gz` reproduce the exact recorded source fingerprints. The unchanged temporary baseline worktree was archived and removed after validation. Failed development attempts remain clearly marked and are excluded from these results. In particular, the earlier PostgreSQL socket-readiness race was fixed by waiting on TCP; the full verified baseline uses that fix.

## Profile-guided readiness optimization

Only the runtime path for a single health dependency changed. `executeCheck` still isolates the callback and retains timeout, busy-check and panic-sanitization behavior. A single dependency now avoids the additional coordinator goroutine/channel. The multi-dependency path, public API and configuration defaults remain unchanged.

The pre-change CPU profile showed scheduler/locking work and the coordinator wrapper; the allocation profile attributed about 58% of allocated space to `Checker.run`. This identified a concrete removable overhead. The final comparison uses identical harnesses and alternating unprofiled executions, rather than comparing profiler timings.

| Case | Before median ns/op | After median ns/op | Change | benchstat p |
|---|---:|---:|---:|---|
| `BenchmarkReadiness` | 3,338 | 2,390 | -28.40% | 0.007 |
| `BenchmarkReadiness-4` | 3,001 | 2,096 | -30.16% | <0.001 |

Both CPU configurations reduced bytes from **1,336 to 1,088 B/op** (18.56%) and allocations from **14 to 10 allocs/op** (28.57%). Timing intervals are wide on this desktop host; the allocation reduction was constant across samples. These results establish a benefit for one-check readiness, not an overall HTTP or service throughput improvement.

A/B source fingerprints: base `73843a19e5a15e0230e2584228c275e2f12fe3b8624598ce191e382f4d96961a`; candidate `b5606678fadee646a09ca786b4a63f4dfc04946f4762ec5e6c5310731304a254`. The original source, updated source and diff are saved under `artifacts/performance/readiness-source/`.

Regression checks include repeated race-enabled health, Foundation and reference-service tests; canceled probes across readiness/liveness/combined APIs; uncooperative callbacks; repeated busy checks; panic/error privacy and configuration concurrency.

## Real-workload baseline

Each scenario below has ten independent repetitions, 5 s warmup and 30 s measurement, with fresh owned infrastructure per repetition. Server/client processes use `GOMAXPROCS=4`. Values are medians of run summaries; the p95/p99 columns are medians of per-run histogram percentiles, not percentiles pooled across requests. All accepted runs report zero errors, missing deliveries and detected duplicates, with successful client drain and fixture cleanup. EventBus additionally verifies broker drain. The original WebSocket backlog fields were unmeasured default zeros; its client receipt checks do not establish broker settlement. Harness v2 corrects that gap with observed stats and a broker-drain barrier; see the [2026-11-05 WebSocket follow-up](WEBSOCKET_PERFORMANCE.md).

| Scenario | Completed operations/s | p95 ms | p99 ms | Server allocated B/operation |
|---|---:|---:|---:|---:|
| `http-1` | 2,193.15 | 1.135 | 1.482 | 10,729 |
| `http-16` | 28,793.73 | 1.095 | 1.537 | 10,705 |
| `websocket-fanout` | 100.00 | 7.899 | 9.081 | 18,934 |
| `websocket-message` | 300.69 | 406.285 | 416.257 | 52,224 |
| `eventbus-redis` | 6,572.09 | 3.240 | 4.550 | 19,102 |
| `eventbus-nats` | 19,079.56 | 0.859 | 1.297 | 18,896 |
| `eventbus-kafka` | 3,379.21 | 7.528 | 10.191 | 25,419 |

WebSocket fanout counts completed room sends to ten recipients, not individual deliveries. The business-message workload targeted up to 1,000 operations/s across 100 clients but achieved about 301/s under the closed-loop policy, with p95 around 406 ms. That observation is retained for follow-up profiling; no latency SLO or saturation-capacity target was established in this change.

EventBus latency includes a fixed-size local pipe acknowledgement after handler completion, before broker settlement. PostgreSQL cursor is measured separately: `/v1/me` performs authentication/Redis work and no SQL query. These local plaintext, single-node fixtures exclude TLS, multi-node routing and production backend/application costs. Broker numbers are workload baselines, not a general broker ranking. See [workload semantics](PERFORMANCE.md).

`integration-comparison-check/` compares the accepted integration dataset against itself to exercise the real-load report/95% bootstrap path without rerunning workloads. It is a comparator wiring check, not independent A/A timing evidence or an optimization claim. The separate `aa-runner-check/` performed independent short Go A/A runs to exercise AB/BA orchestration.

## Reproduce and validate

```bash
python3 benchmarks/performance.py run --suite core --mode baseline
python3 benchmarks/performance.py run --suite integration --mode baseline
python3 benchmarks/performance.py profile --suite integration --cpu 4
python3 benchmarks/performance.py ab --base-source /path/to/base --workload health --count 10
python3 -m unittest discover -s .github/scripts -p 'test_*.py'
```

Use two checkouts with identical final harness files for A/B. To recreate the recorded source states, create checkouts at the parent revision and overlay the matching source archives. A simpler new experiment can use current harness files on both sides and replace only the runtime health implementation with the saved pre-change source on the base side. Do not substitute the old historical microbenchmark snapshots: several benchmark harnesses now verify completed work or use bounded fixtures and intentionally need a new baseline.

All repository Go gates were already recorded after the runtime optimization. The resumed audit reuses those valid results and reruns only checks appropriate to finishing the Python/workflow/documentation work. CI is configured for PR smoke and weekly/manual measurements, with separate registry CPU lanes to keep a hosted-runner command below the timeout. GitHub Actions itself has not been executed from this local task.

## WebSocket follow-up on 2026-11-05

[The Redis intake optimization](WEBSOCKET_PERFORMANCE.md) uses new baselines on
both sides with `performance-v2`. Ten alternating pairs show mean business
throughput 300.59 to 999.98 operations/s and mean run p95 404.33 to 69.36 ms.
The candidate reaches the configured 1,000/s load ceiling; this is not a maximum
capacity measurement. Default fanout p95/p99 difference intervals include zero.
The change costs about 0.9% more allocated bytes/business operation at default
load. Six equal-work pairs do not establish a retained/peak heap regression; the
report includes a small low-load fanout median increase and all confidence
intervals rather than claiming every metric improves.

The v1 latency/receipt samples above remain historical evidence. Their WebSocket
zero backlog fields must not be interpreted as measured broker stats or compared
directly against the new method.
