# Redis session registry benchmark — 2026-10-30

Measured the current working tree, including the session lifecycle refactor, with
Go 1.26.0 (cross-compiled linux/amd64, CGO disabled), Redis 7.4.11
(`redis:7-alpine`), Intel Core i7-11800H @ 2.30 GHz, and `GOMAXPROCS=4`.
The Go binary and Redis ran in the same disposable Docker Desktop Linux container,
using TCP loopback. Redis RDB snapshots and AOF were disabled. No container CPU or
memory limits were requested. No application workload used this Redis instance.
Base commit: `554923968b2eeedc9ae5f7dc685fa7c1524b50bf`, with the uncommitted
session lifecycle changes present in the working tree. Register/Resolve use the
existing HGETALL/Lua scan implementation.

Harness: [registry_benchmark_test.go](../routing/redis/registry_benchmark_test.go).
[Raw output](redis-registry-20261030-linux.txt) contains all 204 measured runs:
68 cases, 3 repeats per case, 1,000 operations per repeat (204,000 operations).
Every case passed result-count/error validation. No production implementation was
changed for this measurement.

## Method and interpretation

- One tenant/user hash contains N synthetic session records with identical ID
  lengths, generation lengths and field layout; live records have a one-hour lease.
  Setup seeds encoded records directly, excluding sequential registration costs.
- `expired=50` restores exactly `floor(N/2)` expired records before **every**
  measured operation. The last session (the hit/replacement target) stays live.
  Thus N=1 has zero expired records. Live-fixture setup, expired-fixture resets,
  and cleanup are outside the Go benchmark timer and allocation accounting.
- `RegisterNew` uses a capacity of N+1 and resets its N-record fixture before
  each operation. `RegisterCapacity` uses capacity N and checks rejection with
  N live records. All other operations use the default limit of 1,000.
- `Parallel` uses Go `RunParallel` with four workers, a pool of four connections,
  one shared user hash, and no fixture resets. It measures direct registry calls;
  Gateway lifecycle serialization is not part of this harness.
- Every metric below is the **median of three run-level metrics**. Percentiles
  are nearest-rank percentiles within each 1,000-operation run, then medians of
  those three percentiles; they are not percentiles pooled across 3,000 samples.
- Mean latency is Go `ns/op` converted to microseconds. Parallel mean is elapsed
  time divided by completed operations, so use p50/p95/p99 for per-request latency.
  Throughput excludes fixture setup/reset and is not sustained end-to-end throughput
  for reset-dependent cases. Client result validation and latency sampling are timed.
- `Redis EVAL` is delta `INFO commandstats` execution microseconds divided by
  EVAL calls. Each case asserts exactly one EVAL per measured operation. It isolates
  server command service time, rather than TCP round trips or Go JSON decoding.
  It is not an independently sampled process CPU utilization measurement.
- `HGETControl` performs raw HGET only. It omits expiry validation, pruning and Go
  JSON decoding, so its results are a diagnostic baseline, not an equivalent API
  or an achieved speedup for a future Resolve implementation.
- Linux provides the per-call monotonic timing used here. The harness omits custom
  percentiles on Windows because this Go toolchain's `time.Now` granularity is
  coarser than the QPC clock used internally by the Go benchmark timer.
- Three short repeats are a local microbenchmark, without TLS, business traffic,
  production network RTT, mixed-user fairness or a long saturation/soak test.
  Cases run in fixed order; CPU frequency/thermal state and VM scheduling were
  not controlled. Read individual runs before interpreting noisy tails.

## Sequential, all sessions live

All latency columns are microseconds. `B/op` and allocations cover the Go client,
not Redis's Lua allocations or server memory.

| Sessions | Operation | Mean us | p50 us | p95 us | p99 us | ops/s | Redis EVAL us | B/op | allocs/op |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | ResolveHit | 76.30 | 71.52 | 115.80 | 157.00 | 13107.0 | 11.11 | 1936 | 35 |
| 1 | ResolveMiss | 74.17 | 71.03 | 110.30 | 147.40 | 13483.0 | 10.84 | 840 | 18 |
| 1 | ResolveAll | 79.25 | 73.86 | 112.70 | 167.30 | 12618.0 | 11.45 | 1920 | 34 |
| 1 | RegisterReplace | 82.33 | 76.45 | 120.80 | 162.30 | 12145.0 | 16.52 | 1368 | 23 |
| 1 | RegisterNew | 85.73 | 79.01 | 123.60 | 163.70 | 11664.0 | 17.51 | 1364 | 22 |
| 1 | RegisterCapacity | 79.77 | 72.48 | 118.10 | 165.60 | 12536.0 | 13.50 | 1360 | 22 |
| 1 | HGETControl | 57.51 | 57.49 | 76.45 | 99.03 | 17387.0 | — | 472 | 8 |
| 10 | ResolveHit | 111.19 | 105.20 | 148.10 | 199.20 | 8994.0 | 38.58 | 1936 | 35 |
| 10 | ResolveMiss | 102.31 | 96.36 | 148.60 | 183.60 | 9774.0 | 38.34 | 840 | 18 |
| 10 | ResolveAll | 147.19 | 135.30 | 202.50 | 284.10 | 6794.0 | 42.66 | 11784 | 160 |
| 10 | RegisterReplace | 120.05 | 112.80 | 171.00 | 201.30 | 8330.0 | 44.08 | 1368 | 23 |
| 10 | RegisterNew | 124.45 | 117.00 | 159.40 | 197.80 | 8035.0 | 46.04 | 1364 | 22 |
| 10 | RegisterCapacity | 109.26 | 105.10 | 140.30 | 191.10 | 9153.0 | 40.25 | 1361 | 22 |
| 10 | HGETControl | 56.85 | 55.43 | 73.28 | 107.80 | 17591.0 | — | 472 | 8 |
| 100 | ResolveHit | 460.68 | 433.50 | 618.90 | 784.80 | 2171.0 | 344.40 | 1936 | 35 |
| 100 | ResolveMiss | 426.48 | 422.00 | 484.90 | 618.60 | 2345.0 | 319.70 | 840 | 18 |
| 100 | ResolveAll | 703.78 | 684.90 | 805.70 | 1173.00 | 1421.0 | 340.10 | 110296 | 1420 |
| 100 | RegisterReplace | 415.46 | 414.30 | 472.20 | 551.70 | 2407.0 | 312.70 | 1368 | 23 |
| 100 | RegisterNew | 409.61 | 403.10 | 466.20 | 599.40 | 2441.0 | 309.80 | 1367 | 22 |
| 100 | RegisterCapacity | 400.80 | 393.90 | 462.80 | 580.30 | 2495.0 | 306.90 | 1361 | 22 |
| 100 | HGETControl | 57.49 | 56.68 | 80.44 | 105.40 | 17394.0 | — | 472 | 8 |
| 1000 | ResolveHit | 3144.12 | 3086.00 | 3526.00 | 3956.00 | 318.1 | 2988.00 | 1936 | 35 |
| 1000 | ResolveMiss | 3032.36 | 2994.00 | 3271.00 | 3849.00 | 329.8 | 2903.00 | 840 | 18 |
| 1000 | ResolveAll | 5862.58 | 5735.00 | 6622.00 | 7146.00 | 170.6 | 3211.00 | 1092477 | 14020 |
| 1000 | RegisterReplace | 3032.95 | 2979.00 | 3316.00 | 4171.00 | 329.7 | 2895.00 | 1369 | 23 |
| 1000 | RegisterNew | 3134.53 | 3022.00 | 3598.00 | 5599.00 | 319.0 | 2990.00 | 1451 | 23 |
| 1000 | RegisterCapacity | 3100.12 | 3051.00 | 3438.00 | 3848.00 | 322.6 | 2950.00 | 1368 | 23 |
| 1000 | HGETControl | 85.63 | 72.53 | 175.60 | 307.00 | 11678.0 | — | 472 | 8 |

## Four concurrent workers, all sessions live

| Sessions | Operation | Mean us | p50 us | p95 us | p99 us | ops/s | Redis EVAL us | B/op | allocs/op |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | ResolveHitParallel | 30.09 | 115.10 | 168.50 | 195.70 | 33228.0 | 9.54 | 1938 | 35 |
| 1 | ResolveMissParallel | 28.82 | 110.60 | 168.00 | 195.00 | 34698.0 | 8.80 | 842 | 18 |
| 1 | ResolveAllParallel | 31.25 | 120.50 | 173.90 | 210.70 | 32003.0 | 9.52 | 1920 | 34 |
| 1 | RegisterReplaceParallel | 36.51 | 145.60 | 202.20 | 267.00 | 27387.0 | 13.23 | 1370 | 23 |
| 1 | HGETControlParallel | 26.00 | 95.31 | 180.30 | 296.30 | 38464.0 | — | 473 | 8 |
| 10 | ResolveHitParallel | 51.65 | 194.80 | 273.00 | 391.10 | 19362.0 | 36.69 | 1937 | 35 |
| 10 | ResolveMissParallel | 50.17 | 192.20 | 268.80 | 332.60 | 19933.0 | 35.64 | 840 | 18 |
| 10 | ResolveAllParallel | 55.74 | 211.70 | 307.80 | 385.60 | 17941.0 | 39.36 | 11784 | 160 |
| 10 | RegisterReplaceParallel | 53.92 | 210.50 | 257.00 | 370.80 | 18547.0 | 39.68 | 1370 | 23 |
| 10 | HGETControlParallel | 22.57 | 85.65 | 118.30 | 149.00 | 44316.0 | — | 472 | 8 |
| 100 | ResolveHitParallel | 326.07 | 1288.00 | 1445.00 | 1619.00 | 3067.0 | 309.10 | 1936 | 35 |
| 100 | ResolveMissParallel | 317.07 | 1262.00 | 1365.00 | 1532.00 | 3154.0 | 300.80 | 840 | 18 |
| 100 | ResolveAllParallel | 345.42 | 1358.00 | 1746.00 | 1958.00 | 2895.0 | 323.30 | 110297 | 1420 |
| 100 | RegisterReplaceParallel | 312.52 | 1241.00 | 1343.00 | 1510.00 | 3200.0 | 296.80 | 1369 | 23 |
| 100 | HGETControlParallel | 22.77 | 87.90 | 122.10 | 164.40 | 43914.0 | — | 472 | 8 |
| 1000 | ResolveHitParallel | 2936.60 | 11554.00 | 12748.00 | 13312.00 | 340.5 | 2907.00 | 1936 | 35 |
| 1000 | ResolveMissParallel | 2992.52 | 11692.00 | 12957.00 | 17168.00 | 334.2 | 2960.00 | 840 | 18 |
| 1000 | ResolveAllParallel | 4223.93 | 16475.00 | 18981.00 | 28518.00 | 236.7 | 3240.00 | 1092485 | 14020 |
| 1000 | RegisterReplaceParallel | 2858.27 | 11396.00 | 11937.00 | 12292.00 | 349.9 | 2841.00 | 1370 | 23 |
| 1000 | HGETControlParallel | 22.25 | 86.42 | 117.80 | 141.00 | 44947.0 | — | 472 | 8 |

## Sequential pruning, half the sessions expired

| Sessions | Operation | Mean us | p50 us | p95 us | p99 us | ops/s | Redis EVAL us | B/op | allocs/op |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | ResolveHit | 85.98 | 78.15 | 126.20 | 168.70 | 11631.0 | 12.83 | 1936 | 35 |
| 1 | ResolveMiss | 75.55 | 71.38 | 108.20 | 142.70 | 13236.0 | 11.22 | 848 | 18 |
| 1 | ResolveAll | 85.42 | 78.59 | 122.60 | 160.70 | 11707.0 | 12.58 | 1920 | 34 |
| 1 | RegisterReplace | 88.06 | 80.15 | 123.70 | 156.80 | 11356.0 | 17.01 | 1378 | 23 |
| 1 | RegisterNew | 86.14 | 78.27 | 125.40 | 165.80 | 11608.0 | 17.55 | 1362 | 22 |
| 10 | ResolveHit | 125.63 | 117.80 | 160.30 | 192.00 | 7960.0 | 46.59 | 1936 | 35 |
| 10 | ResolveMiss | 119.82 | 111.40 | 155.90 | 190.60 | 8346.0 | 47.01 | 848 | 18 |
| 10 | ResolveAll | 140.09 | 130.30 | 180.40 | 255.00 | 7138.0 | 50.26 | 6304 | 90 |
| 10 | RegisterReplace | 131.58 | 125.60 | 164.50 | 236.00 | 7600.0 | 55.38 | 1380 | 23 |
| 10 | RegisterNew | 127.00 | 120.80 | 161.20 | 199.00 | 7874.0 | 51.96 | 1364 | 22 |
| 100 | ResolveHit | 464.68 | 459.30 | 515.00 | 578.40 | 2152.0 | 356.90 | 1936 | 35 |
| 100 | ResolveMiss | 453.73 | 447.10 | 502.70 | 612.60 | 2204.0 | 354.40 | 848 | 18 |
| 100 | ResolveAll | 605.93 | 592.70 | 684.40 | 900.00 | 1650.0 | 374.30 | 55568 | 720 |
| 100 | RegisterReplace | 464.86 | 457.10 | 526.60 | 659.40 | 2151.0 | 362.00 | 1384 | 23 |
| 100 | RegisterNew | 460.68 | 453.50 | 518.30 | 645.80 | 2171.0 | 359.50 | 1368 | 22 |
| 1000 | ResolveHit | 3505.87 | 3466.00 | 3787.00 | 4176.00 | 285.2 | 3366.00 | 1943 | 35 |
| 1000 | ResolveMiss | 3595.95 | 3537.00 | 4004.00 | 4522.00 | 278.1 | 3455.00 | 848 | 18 |
| 1000 | ResolveAll | 4930.88 | 4789.00 | 5597.00 | 6900.00 | 202.8 | 3535.00 | 550754 | 7020 |
| 1000 | RegisterReplace | 3477.27 | 3442.00 | 3732.00 | 4052.00 | 287.6 | 3338.00 | 1455 | 23 |
| 1000 | RegisterNew | 3484.46 | 3438.00 | 3759.00 | 4182.00 | 287.0 | 3344.00 | 1459 | 23 |

## Assessment

At 1,000 live sessions, resolving one session averages 3.14 ms, including 2.99 ms
of Redis EVAL service time, versus 76.30 us including 11.11 us of EVAL at one
session. Missing-session lookups also scan the full hash. Go allocations for a hit
stay at about 1.9 KB / 35 allocations while Redis service time rises sharply,
pointing to server-side scan/decode work as the dominant cost in this workload.

Four concurrent lookup workers increase 1,000-session hit throughput only from
318.1 to 340.5 ops/s, while median run-level p99 increases from 3.96 to 13.31 ms.
This supports the concern that a hot large-user hash can saturate the Redis command
execution path. It does not establish a production SLA or global capacity limit.

Resolving all 1,000 live sessions averages 5.86 ms and allocates approximately
1.04 MiB / 14,020 Go allocations per operation. Part of that cost is unavoidable
when returning all sessions; an expiry index will not make the output size constant.

Replacing a session averages 3.03 ms at 1,000 sessions. Capacity rejection still
averages 3.10 ms, confirming that rejection also pays for the scan. With 500
expired records restored each time, a hit averages 3.51 ms and replacement 3.48 ms.
Pruning therefore adds work rather than avoiding the initial full traversal.

Prioritize a measured HGET-based single-session lookup with its existing expiry
and generation contracts preserved. Consider a separate sorted-set expiry index
for Register/pruning only after comparing full-workload reads, registrations and
renewals: the new index adds writes and memory to every renewal. Hash/index updates,
removals and stale-generation protection must remain atomic, including Redis
Cluster key-slot compatibility. No such optimization was applied in this task.

## Reproduce

Use a disposable Linux Redis with INFO commandstats permission, no competing EVAL
workload, and enough time to finish all cases. Never point this harness at a
production Redis. It owns UUID-prefixed hashes, deletes only those hashes, sets
one-hour TTLs, and does not run FLUSHDB/FLUSHALL or reset commandstats.

On Linux with Redis on loopback:

```bash
WS_REGISTRY_BENCH_ADDR=127.0.0.1:6379 GOMAXPROCS=4 go test \
  -run '^$' -bench '^BenchmarkRegistryHash$' -benchtime=1000x -count=3 \
  -benchmem -timeout=15m ./websocket/routing/redis
```

For the Windows-hosted run, cross-compile the package test binary with
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c`, copy it into the disposable Redis
container, and run it with these arguments (quote each dotted flag in PowerShell):

```text
-test.run=^$ -test.bench=^BenchmarkRegistryHash$ -test.benchtime=1000x
-test.count=3 -test.benchmem -test.timeout=15m
```

Pass the address with `WS_REGISTRY_BENCH_ADDR`; if needed, credentials come only
from `WS_REGISTRY_BENCH_PASSWORD`. Run without the address to skip the opt-in
benchmark. Standard package tests do not require the benchmark container.
