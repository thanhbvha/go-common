# Redis Register/Renew workload measurements — 2026-10-30

Measured the current working tree with Go 1.26.0 (linux/amd64, CGO disabled), Redis
7.4.11 (`redis:7-alpine`), Intel Core i7-11800H @ 2.30 GHz, and `GOMAXPROCS=4`.
The cross-compiled Go test binary and Redis ran in the same disposable Docker
Desktop Linux container over TCP loopback. Redis persistence was disabled and no
container CPU/memory limits were requested. No competing application used Redis.
Base commit: `554923968b2eeedc9ae5f7dc685fa7c1524b50bf`, with the existing uncommitted
session lifecycle refactor. Production Registry/Gateway implementation is unchanged
by this measurement task.

Harness: [register_renew_benchmark_test.go](../routing/redis/register_renew_benchmark_test.go).
[Raw output](redis-register-renew-20261030-linux.txt) contains **44 profiles × 3
repeats × 1,000 primary iterations**, or 132 measured runs. A full-batch primary
iteration performs N EVAL commands; the background-renew profile performs additional
renewal calls. Thus primary iterations are not a count of Redis commands or all API
calls. The [earlier lookup/Register benchmark](redis-registry-20261030.md) remains
available separately.

## Method

- Every case owns one UUID-prefixed tenant/user hash with N live records, seeded
  outside the timer and deleted afterward, with a one-hour TTL as a fallback. There
  is no FLUSHDB/FLUSHALL or commandstats reset. No setup/reset occurs in timed loops.
- Each session uses a 36-character UUID generation. Every RegisterChurn operation
  generates a fresh UUID and replaces an existing session; cardinality stays N and
  the default capacity stays 1,000. This differs from the earlier benchmark's fixed
  16-character generation and includes generation allocation and publication
  coordination; the two Register results are not a controlled before/after comparison.
- RenewOne rotates over individual sessions. RenewAllBatch calls the real RenewBatch
  for all N sessions, using its existing pipelines of at most 256 commands. Batch
  results are checked individually. Single-session renewal and Register each issue
  one EVAL; a full renewal issues N EVALs.
- RenewAllBatchStale10 installs newer Redis generations for `floor(N/10)` sessions
  while retaining old generations in the renewal snapshot. Every batch must report
  exactly that stale count and successfully process the other sessions. N=1 has no
  stale fixture. Expiry/pruning is not part of these workloads.
- Parallel profiles use four Go RunParallel workers and a connection pool of four.
  MixedRegister1/10/50Percent use exactly 10/100/500 registrations per 1,000 primary
  operations; the remaining operations are single-session renewals. A deterministic
  permutation spreads each percentage across every block of 100 operations. These
  are proportions of API calls, not reconnect percentages per user per second.
- RegisterWithBatchRenew uses **three registration workers plus one continuously
  renewing full-batch worker**. The timer includes waiting for its final batch after
  registrations finish. It is a closed-loop saturation test, with no 10-second sleep
  or fixed target arrival rate. Reported registration throughput includes that drain.
- Register operations for the same session are serialized by a per-session harness
  mutex to publish the last confirmed generation in order. Different sessions can
  register concurrently. A short RWMutex protects local snapshots; Redis I/O runs
  outside it. The latency includes this coordination. This models registry-level
  concurrent writers/snapshots, not Gateway's full lifecycle serialization or sockets.
- Mixed renewal snapshots can legitimately become stale during a replacement. Those
  ErrStale results are counted, while any other error fails the run. High stale rates
  for one rapidly replaced session are expected fencing, not missing data. Every run
  verifies final session cardinality; churn/mixed runs also verify every final Redis
  generation equals the last confirmed registration.
- EVAL counts must match registration calls plus session-renewal attempts exactly.
  `Redis EVAL us` is aggregate INFO commandstats service time per EVAL command, not
  per full batch and not process CPU utilization. Mixed values combine both scripts.
- Tables show medians of three run-level metrics. Per-method percentiles use nearest
  rank within each run, then the median of those three percentiles, not a pooled CDF.
  Single-call mixed profiles have only 10 registration samples at 1% and 100 at 10%,
  so their high registration percentiles are noisy. Background batch sample counts
  are shown explicitly; at N=1,000 only 14 batches finish per repeat, making p99 the
  maximum observed batch rather than a stable production-tail estimate.
- `ns/op` for parallel profiles is aggregate elapsed time / primary iterations, not
  per-request latency. In the background profile primary iterations are registrations
  only; their B/op includes background work divided by those registrations. Use
  per-method latency/rates when comparing profiles. Renewal rates count **attempted**
  sessions including stale results; successful renewals are lower by stale-percent.
- Client validation, snapshots, UUID generation and latency sampling are included in
  timed work. Linux monotonic clocks provide per-call percentiles; on Windows the
  harness omits those custom percentiles because of the toolchain's coarser clock.
- These short, fixed-order local runs exclude TLS, production RTT, room renewal,
  heartbeat scheduling, mixed-user fairness and long soak tests. CPU thermal state
  and VM scheduling are uncontrolled. They are not production sizing or SLA claims.

## Isolated sequential workloads

Latency columns are microseconds. A full-batch row's mean/pXX describe the entire
N-session API call; EVAL time is per session command. B/op is Go client allocation.

| N | Profile | Mean us | p50 us | p95 us | p99 us | Register/s | Renew attempts/s | EVAL us/call | Stale % | B/op | allocs/op |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | RenewOne | 78.64 | 73.30 | 124.90 | 165.00 | 0.0 | 12717.0 | 13.360 | 0.000 | 1792 | 30 |
| 1 | RenewAllBatch | 77.00 | 72.59 | 117.60 | 152.60 | 0.0 | 12987.0 | 13.010 | 0.000 | 1792 | 30 |
| 1 | RenewAllBatchStale10 | 79.94 | 74.03 | 120.70 | 163.90 | 0.0 | 12509.0 | 13.700 | 0.000 | 1793 | 30 |
| 1 | RegisterChurn | 80.52 | 76.20 | 122.30 | 156.20 | 12419.0 | 0.0 | 15.760 | 0.000 | 1593 | 26 |
| 10 | RenewOne | 78.00 | 73.30 | 121.90 | 158.90 | 0.0 | 12820.0 | 13.420 | 0.000 | 1792 | 30 |
| 10 | RenewAllBatch | 214.75 | 199.90 | 278.70 | 402.30 | 0.0 | 46566.0 | 9.066 | 0.000 | 17089 | 215 |
| 10 | RenewAllBatchStale10 | 206.68 | 196.70 | 266.10 | 373.30 | 0.0 | 48383.0 | 8.619 | 10.000 | 17087 | 215 |
| 10 | RegisterChurn | 118.30 | 110.10 | 159.40 | 193.70 | 8453.0 | 0.0 | 44.250 | 0.000 | 1592 | 26 |
| 100 | RenewOne | 77.56 | 73.30 | 110.40 | 151.70 | 0.0 | 12893.0 | 13.410 | 0.000 | 1792 | 30 |
| 100 | RenewAllBatch | 1405.97 | 1376.00 | 1691.00 | 1989.00 | 0.0 | 71125.0 | 8.342 | 0.000 | 169000 | 2018 |
| 100 | RenewAllBatchStale10 | 1405.49 | 1362.00 | 1730.00 | 2046.00 | 0.0 | 71149.0 | 8.232 | 10.000 | 169002 | 2018 |
| 100 | RegisterChurn | 443.06 | 434.90 | 493.90 | 556.50 | 2257.0 | 0.0 | 335.700 | 0.000 | 1592 | 26 |
| 1000 | RenewOne | 80.34 | 73.17 | 120.90 | 172.10 | 0.0 | 12446.0 | 14.310 | 0.000 | 1794 | 30 |
| 1000 | RenewAllBatch | 13429.40 | 13314.00 | 14563.00 | 16671.00 | 0.0 | 74463.0 | 8.224 | 0.000 | 1677932 | 20075 |
| 1000 | RenewAllBatchStale10 | 13319.19 | 13133.00 | 14547.00 | 16951.00 | 0.0 | 75080.0 | 8.056 | 10.000 | 1677945 | 20076 |
| 1000 | RegisterChurn | 3307.90 | 3221.00 | 3941.00 | 4539.00 | 302.3 | 0.0 | 3151.000 | 0.000 | 1594 | 26 |

## Four workers, isolated workloads

| N | Profile | Mean us | p50 us | p95 us | p99 us | Register/s | Renew attempts/s | EVAL us/call | Stale % | B/op | allocs/op |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | RenewOneParallel | 32.76 | 127.70 | 180.10 | 202.90 | 0.0 | 30526.0 | 11.330 | 0.000 | 1795 | 30 |
| 1 | RenewAllBatchParallel | 31.10 | 121.30 | 180.20 | 201.90 | 0.0 | 32152.0 | 11.150 | 0.000 | 1794 | 30 |
| 1 | RegisterChurnParallel | 98.83 | 103.60 | 1285.00 | 1361.00 | 10118.0 | 0.0 | 17.600 | 0.000 | 1595 | 26 |
| 10 | RenewOneParallel | 32.64 | 126.60 | 190.40 | 212.10 | 0.0 | 30639.0 | 11.650 | 0.000 | 1793 | 30 |
| 10 | RenewAllBatchParallel | 116.60 | 454.30 | 582.60 | 729.10 | 0.0 | 85766.0 | 8.210 | 0.000 | 17088 | 215 |
| 10 | RegisterChurnParallel | 54.13 | 212.90 | 254.90 | 324.90 | 18473.0 | 0.0 | 40.460 | 0.000 | 1594 | 26 |
| 100 | RenewOneParallel | 32.99 | 127.30 | 193.40 | 235.00 | 0.0 | 30308.0 | 11.800 | 0.000 | 1793 | 30 |
| 100 | RenewAllBatchParallel | 1066.05 | 4217.00 | 4729.00 | 5202.00 | 0.0 | 93804.0 | 8.175 | 0.000 | 169024 | 2018 |
| 100 | RegisterChurnParallel | 343.67 | 1325.00 | 1595.00 | 2480.00 | 2910.0 | 0.0 | 325.400 | 0.000 | 1593 | 26 |
| 1000 | RenewOneParallel | 32.72 | 126.10 | 191.60 | 236.50 | 0.0 | 30566.0 | 12.090 | 0.000 | 1795 | 30 |
| 1000 | RenewAllBatchParallel | 10645.38 | 42254.00 | 45538.00 | 52818.00 | 0.0 | 93937.0 | 8.186 | 0.000 | 1678575 | 20080 |
| 1000 | RegisterChurnParallel | 3053.58 | 12069.00 | 13063.00 | 14487.00 | 327.5 | 0.0 | 3026.000 | 0.000 | 1594 | 26 |

## Four workers, mixed single-session API calls

Separate method latencies are microseconds. Every row has exactly 1,000 primary
API calls per repeat. Register and Renew latency samples are kept separately.

| N | Register share | Register/s | Renew attempts/s | Register p50 us | Register p99 us | Renew p50 us | Renew p95 us | Renew p99 us | Stale % |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1% | 299.9 | 29692.0 | 131.10 | 185.30 | 129.80 | 191.50 | 230.20 | 2.929 |
| 1 | 10% | 3126.0 | 28133.0 | 128.20 | 206.20 | 124.00 | 189.30 | 210.70 | 33.110 |
| 1 | 50% | 8259.0 | 8259.0 | 153.90 | 1263.00 | 85.55 | 136.20 | 164.60 | 49.400 |
| 10 | 1% | 313.1 | 31000.0 | 155.70 | 227.50 | 122.70 | 191.10 | 227.20 | 0.000 |
| 10 | 10% | 2794.0 | 25145.0 | 163.50 | 229.00 | 137.00 | 203.20 | 235.80 | 0.000 |
| 10 | 50% | 11081.0 | 11081.0 | 180.90 | 308.70 | 160.30 | 262.10 | 299.60 | 0.000 |
| 100 | 1% | 265.9 | 26326.0 | 499.90 | 559.10 | 133.20 | 214.90 | 507.10 | 0.000 |
| 100 | 10% | 1364.0 | 12276.0 | 512.30 | 616.60 | 170.90 | 557.90 | 589.60 | 0.000 |
| 100 | 50% | 2536.0 | 2536.0 | 848.80 | 1237.00 | 703.20 | 1111.00 | 1199.00 | 0.000 |
| 1000 | 1% | 144.1 | 14268.0 | 3380.00 | 3630.00 | 133.20 | 344.70 | 3405.00 | 0.000 |
| 1000 | 10% | 271.8 | 2446.0 | 3293.00 | 3730.00 | 277.90 | 3433.00 | 3605.00 | 0.000 |
| 1000 | 50% | 309.7 | 309.7 | 6581.00 | 10291.00 | 6203.00 | 9400.00 | 9994.00 | 0.000 |

## Three registration workers plus one full-batch renewal worker

Latencies here are **milliseconds**. Each repeat completes 1,000 registrations
plus as many renewal batches as overlap. Batch counts below are medians per repeat.

| N | Register/s | Batch calls/s | Renew attempts/s | Register p50 ms | Register p99 ms | Batch p50 ms | Batch p99 ms | Batches/repeat | Stale % |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 9423.0 | 10074.00 | 10074.0 | 0.114 | 1.380 | 0.096 | 0.172 | 1064 | 96.980 |
| 10 | 10868.0 | 3597.00 | 35974.0 | 0.267 | 0.486 | 0.268 | 0.486 | 331 | 30.430 |
| 100 | 2178.0 | 239.50 | 23954.0 | 1.432 | 1.739 | 4.145 | 4.800 | 111 | 5.306 |
| 1000 | 306.5 | 4.29 | 4290.0 | 9.689 | 11.441 | 246.823 | 262.503 | 14 | 3.643 |

## Assessment

Single-session renewal is nearly independent of user cardinality in this run:
78.64 us at N=1 and 80.34 us at N=1,000, with 13.36–14.31 us of EVAL service time.
Registration churn rises from 80.52 us to 3.31 ms; at N=1,000 its EVAL averages
3,151 us versus 14.31 us for a single renewal. The existing HGETALL/pruning path
in Register remains the expensive per-session action.

Renewing all 1,000 sessions takes 13.43 ms on average (p99 16.67 ms), or 74,463
session attempts/s, with 8.224 us EVAL service time per session. Four parallel batch
callers reach about 93,937 attempts/s but p99 rises to 52.82 ms per full batch.
The batch still performs 1,000 Lua commands and allocates about 1.60 MiB / 20,075
Go allocations per API call. With 10% stale inputs, mean batch time is 13.32 ms;
partial failures preserve processing across all 256-command chunks.

Registration load delays renewals. At N=1,000, four single-session workers show
renewal p99 of 0.237 ms with no registration, 3.405 ms with 1% registration,
3.605 ms with 10%, and 9.994 ms with 50%. Compare within this experiment: the
parallel pure-renew profile is the relevant baseline, not the sequential run.
The 1% registration profile contains only ten registration samples per repeat.

The saturated three-register/one-batch profile is more severe: at N=1,000 it
completes 306.5 registrations/s and 4.29 renewal batches/s. Median renewal batch
latency rises from 13.31 ms in isolation to 246.82 ms, while registration p50 is
9.69 ms. All three repeats complete 14 batches, so the reported 262.50 ms batch
p99 is an observed maximum from small samples. Final session generations remain
correct; about 3.64% of renewal attempts are fenced as stale during replacement.
This is evidence of contention under sustained reconnect saturation, not evidence
that normal 10-second lease scheduling will always have this latency.

For context, renewing one user's 1,000 sessions every 10 seconds requires 100
renewal EVALs/s on average. The isolated measured EVAL cost would sum to roughly
0.82 ms of server command service per second for that user; this arithmetic
excludes other users, arrivals and queuing and is not measured CPU utilization.
The benchmark deliberately renews continuously to expose contention.

An expiry sorted set would add maintenance to this frequent, currently inexpensive
renewal path. These measurements support optimizing registration scan/pruning and
single-session lookups while preserving O(1)-like per-session renewal. They do not
prove a sorted-set design is faster overall: benchmark a candidate with atomic
hash/index updates, generation fencing, memory overhead and identical workload
mixes before adopting it. Gateway lock wait and room renewal need separate
end-to-end measurements if they become operational bottlenecks.

## Validation and reproduction

All 132 measured repeats passed EVAL-count, batch-result, cardinality and applicable
final-generation checks. Package routing tests and vet passed. A separate Windows
race-detector run passed MixedRegister10Percent and RegisterWithBatchRenew with
N=10, 100 primary iterations; those race-run timings are not performance results.

On Linux, point only at a disposable Redis with INFO commandstats permission and
no competing EVAL workload:

```bash
WS_REGISTRY_BENCH_ADDR=127.0.0.1:6379 GOMAXPROCS=4 go test \
  -run '^$' -bench '^BenchmarkRegistryRegisterRenew$' -benchtime=1000x \
  -count=3 -benchmem -timeout=15m ./websocket/routing/redis
```

For Windows/Docker, compile with `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c`,
copy the package test binary into the disposable Redis container, then execute
with the same address/GOMAXPROCS environment and these quoted dotted flags:

```text
-test.run=^$ -test.bench=^BenchmarkRegistryRegisterRenew$ -test.benchtime=1000x
-test.count=3 -test.benchmem -test.timeout=15m
```

Without WS_REGISTRY_BENCH_ADDR, this opt-in benchmark skips. Optional credentials
come only from WS_REGISTRY_BENCH_PASSWORD. Existing lookup benchmark output is
preserved independently.
