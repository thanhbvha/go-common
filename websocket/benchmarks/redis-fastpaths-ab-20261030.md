# Registry fast paths: implementation and A/B measurements — 2026-10-30

Single-session resolution now reads only the requested field. Registration directly
replaces a live session or admits a new session below the physical hash limit;
expiry scanning is needed only for a new admission at capacity. The Redis key/JSON
format, renewal scripts and generation-fenced removal stay compatible with the
previous implementation. No expiry sorted set, extra renewal write or migration
is introduced.

A fresh A/B run covers **112 cases per version, three repeats of 1,000 primary
iterations per case: 672 measured repeats total**. Both binaries use the same
benchmark harness and fixtures, Go 1.26.0 (linux/amd64, CGO disabled), Intel Core
i7-11800H @ 2.30 GHz, GOMAXPROCS=4 and the same disposable Redis 7.4.11 container
with TCP loopback and persistence disabled. No application or test workload ran
concurrently with the timed benchmarks. The baseline ran first, then the candidate;
case order, CPU frequency and VM scheduling were not randomized or controlled.

Baseline includes the prior uncommitted lifecycle refactor at base commit
`554923968b2eeedc9ae5f7dc685fa7c1524b50bf`. It was preserved as a Linux binary before
editing the two Lua scripts. Candidate adds those fast paths and regression tests.

Binary SHA-256 fingerprints:

- Before: `F399A82B5CAB009507924C61B85C6F90D30DE3BE6A89C7211214BACD9B5A98EB`
- After: `78AA91BB09B0F50C9C4C1B0DB1E809829604C1B9A02B462EDD6A7F22470ED417`

[Before raw output](redis-fastpaths-before-20261030-linux.txt) ·
[After raw output](redis-fastpaths-after-20261030-linux.txt).
Harnesses and detailed workload definitions:
[lookup/Register](../routing/redis/registry_benchmark_test.go),
[Register/Renew](../routing/redis/register_renew_benchmark_test.go),
[lookup baseline methodology](redis-registry-20261030.md),
[Register/Renew baseline methodology](redis-register-renew-20261030.md).

## Implementation and correctness

- Targeted Resolve uses atomic HGET + lease validation and removes only an expired
  target. It no longer builds an N-entry Lua table or decodes unrelated records.
  Missing-session lookup returns an empty list. User-wide Resolve retains its scan.
- Register inspects the target's lease. A live target can be replaced without
  consuming a new slot. An expired same-ID field is removed and goes through new
  admission, preserving quota semantics even when a limit has been lowered.
- New admission uses HLEN first. At capacity, the original full expiry pruning and
  capacity check run in the same Lua invocation before writing. Concurrent admission
  cannot exceed the limit. Hash TTL extension still preserves longer-lived sessions.
- Renew, batch settlement, room semantics and generation checks use the existing
  implementation. Old renewal/removal cannot overwrite or remove a replacement.
- Pruning is now demand-driven. Unrelated expired metadata may remain until a
  user-wide resolution, capacity admission, targeted cleanup/removal or hash TTL.
  Such records are not advertised as live sessions. This can retain more physical
  hash metadata than eager pruning; client B/op below does not measure Redis memory.
  New admissions remain physically bounded by the configured limit. Existing
  over-limit hashes after a config reduction cannot grow through admission.
- The unchanged format supports old/new writers sharing a namespace. A secondary
  expiry index was not selected: older writers would not maintain it, and its
  consistency/rebuild policy and write cost would need additional validation.

## Main comparisons: 1,000 sessions/user

Values are **medians of three run-level mean latencies**, in microseconds. The
ratio is before / after; small differences in unchanged paths are not claimed as
algorithmic improvements. RenewAllBatch includes all 1,000 sessions in one API call.
RegisterNew has one slot of headroom (capacity N+1), exactly as in the baseline.

| Case | Before us | After us | Before / after |
|---|---:|---:|---:|
| Resolve one session | 3063.00 | 76.02 | 40.29x |
| Resolve missing session | 3104.90 | 67.49 | 46.00x |
| Register replacement | 3139.23 | 80.78 | 38.86x |
| Register new, headroom available | 3125.39 | 109.48 | 28.55x |
| Register with a fresh UUID generation | 3253.84 | 78.45 | 41.48x |
| Renew one session | 78.03 | 75.98 | 1.03x |
| Renew all 1,000 sessions | 13697.86 | 13626.33 | 1.01x |
| Resolve entire user | 5867.46 | 5806.22 | 1.01x |
| Register rejected at capacity | 2992.51 | 2753.39 | 1.09x |

At N=1,000, one-session Resolve EVAL service time falls from 2,923 us to 9.774 us;
Register replacement EVAL falls from 2,992 us to 14.15 us. Their Go allocation
counts are unchanged at 35 and 23 respectively. This is removal of Redis scan/
decode work, rather than a change to the Go result shape.

RenewAllBatch averages 13.70 ms before and 13.63 ms after, with 8.362 versus 8.293
us of EVAL service time per session. No new index maintenance is on this hot path.
ResolveAll and capacity rejection still scan and remain O(N); their small measured
differences are within the limitations of this short local experiment.

## Scaling of targeted lookup and replacement

All entries are means in microseconds, summarized as medians across three repeats.

| Sessions | Resolve hit before | Resolve hit after | Replacement before | Replacement after | RenewOne before | RenewOne after |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 76.87 | 81.45 | 83.14 | 83.53 | 77.09 | 77.86 |
| 10 | 117.58 | 76.10 | 116.37 | 77.04 | 82.61 | 78.28 |
| 100 | 420.52 | 75.56 | 410.58 | 110.72 | 77.68 | 78.77 |
| 1000 | 3063.00 | 76.02 | 3139.23 | 80.78 | 78.03 | 75.98 |

## Contention: single-session mixed workloads at N=1,000

Four workers share one user hash. Register share is a fraction of API calls;
other calls renew one session. Percentiles are per-method, in milliseconds.
Renewal rates below include stale attempts, as in the original harness.

| Register share | Renew p99 before ms | Renew p99 after ms | Register/s before | Register/s after | Renew attempts/s before | Renew attempts/s after |
|---:|---:|---:|---:|---:|---:|---:|
| 1% | 3.539 | 0.269 | 139.0 | 285.4 | 13761.0 | 28259.0 |
| 10% | 3.770 | 0.301 | 270.4 | 2667.0 | 2434.0 | 24001.0 |
| 50% | 9.672 | 0.336 | 314.8 | 14030.0 | 314.8 | 14030.0 |

## Contention: three registration workers plus a full-batch renewal worker

These are closed-loop saturation tests, not renewal once every ten seconds.
The batch worker continues until the 1,000 registrations finish and its last batch
completes. Faster registration therefore produces fewer sampled batches, rather
than a fixed before/after batch count. Latencies below are **p50 milliseconds**.

| Sessions | Register/s before | Register/s after | Batch p50 before ms | Batch p50 after ms | Batches per repeat before | Batches per repeat after |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8991.0 | 9749.0 | 0.099 | 0.094 | 1081 | 1063 |
| 10 | 10478.0 | 16014.0 | 0.274 | 0.241 | 323 | 257 |
| 100 | 2240.0 | 8090.0 | 4.007 | 1.514 | 111 | 80 |
| 1000 | 300.8 | 7178.0 | 249.173 | 15.127 | 14 | 9 |

At N=1,000, batch p50 falls from 249.17 ms to 15.13 ms while registration throughput
rises from 300.8/s to 7,178/s. Final generations/cardinality remain correct. Stale
snapshots remain expected under concurrent replacement (3.65% before, 6.056%
after in this profile). Their change is not a generation-safety failure: the
candidate replaces sessions much faster while renewal snapshots are in flight.
Batch-tail precision is limited by roughly 14 sampled batches before and 9 after
per repeat; do not use their p99 as production SLA estimates.

## Complete A/B matrix

Every value is a median across three repeats. All latency columns are microseconds.
For parallel profiles mean = total elapsed / primary iterations, not per-request
latency. For RegisterWithBatchRenew, primary iterations and API percentiles describe
registrations; background renewal latency is in the contention table/raw output.
The lookup and workload harnesses use different generation fixtures (16-character
fixed generations versus fresh UUIDs). Compare each row to itself across versions.

| Case | Mean before us | Mean after us | Ratio | API p99 before us | API p99 after us |
|---|---:|---:|---:|---:|---:|
| Workload/sessions=1/RenewOne | 77.09 | 77.86 | 0.99x | 158.90 | 148.10 |
| Workload/sessions=1/RenewOneParallel | 32.25 | 31.04 | 1.04x | 225.00 | 224.20 |
| Workload/sessions=1/RenewAllBatch | 78.99 | 78.64 | 1.00x | 138.30 | 150.90 |
| Workload/sessions=1/RenewAllBatchParallel | 32.37 | 32.27 | 1.00x | 244.40 | 211.80 |
| Workload/sessions=1/RenewAllBatchStale10 | 81.99 | 80.94 | 1.01x | 144.90 | 145.30 |
| Workload/sessions=1/RegisterChurn | 85.53 | 80.64 | 1.06x | 154.40 | 156.80 |
| Workload/sessions=1/RegisterChurnParallel | 101.48 | 95.18 | 1.07x | 1373.00 | 1336.00 |
| Workload/sessions=1/MixedRegister1Percent | 32.97 | 31.53 | 1.05x | 228.70 | 218.80 |
| Workload/sessions=1/MixedRegister10Percent | 32.34 | 31.36 | 1.03x | 216.60 | 205.10 |
| Workload/sessions=1/MixedRegister50Percent | 59.95 | 59.23 | 1.01x | 1139.00 | 1218.00 |
| Workload/sessions=1/RegisterWithBatchRenew | 111.22 | 102.57 | 1.08x | 1383.00 | 1386.00 |
| Workload/sessions=10/RenewOne | 82.61 | 78.28 | 1.06x | 148.10 | 146.50 |
| Workload/sessions=10/RenewOneParallel | 32.14 | 32.49 | 0.99x | 212.20 | 215.40 |
| Workload/sessions=10/RenewAllBatch | 208.47 | 207.66 | 1.00x | 376.00 | 327.80 |
| Workload/sessions=10/RenewAllBatchParallel | 121.21 | 116.34 | 1.04x | 812.90 | 744.60 |
| Workload/sessions=10/RenewAllBatchStale10 | 219.61 | 207.42 | 1.06x | 343.50 | 419.60 |
| Workload/sessions=10/RegisterChurn | 124.10 | 81.79 | 1.52x | 195.00 | 165.90 |
| Workload/sessions=10/RegisterChurnParallel | 54.08 | 33.51 | 1.61x | 318.70 | 213.00 |
| Workload/sessions=10/MixedRegister1Percent | 32.80 | 32.03 | 1.02x | 223.30 | 218.00 |
| Workload/sessions=10/MixedRegister10Percent | 36.28 | 32.72 | 1.11x | 240.50 | 217.70 |
| Workload/sessions=10/MixedRegister50Percent | 44.28 | 33.21 | 1.33x | 306.80 | 229.20 |
| Workload/sessions=10/RegisterWithBatchRenew | 95.43 | 62.45 | 1.53x | 514.50 | 306.00 |
| Workload/sessions=100/RenewOne | 77.68 | 78.77 | 0.99x | 143.80 | 155.20 |
| Workload/sessions=100/RenewOneParallel | 32.69 | 33.07 | 0.99x | 227.10 | 212.10 |
| Workload/sessions=100/RenewAllBatch | 1411.33 | 1392.79 | 1.01x | 1997.00 | 1916.00 |
| Workload/sessions=100/RenewAllBatchParallel | 1048.80 | 1066.11 | 0.98x | 4947.00 | 5116.00 |
| Workload/sessions=100/RenewAllBatchStale10 | 1305.81 | 1333.56 | 0.98x | 1814.00 | 1807.00 |
| Workload/sessions=100/RegisterChurn | 412.44 | 78.31 | 5.27x | 623.20 | 149.20 |
| Workload/sessions=100/RegisterChurnParallel | 318.12 | 33.45 | 9.51x | 1527.00 | 215.80 |
| Workload/sessions=100/MixedRegister1Percent | 36.55 | 32.06 | 1.14x | 519.90 | 220.00 |
| Workload/sessions=100/MixedRegister10Percent | 71.58 | 32.78 | 2.18x | 583.50 | 216.70 |
| Workload/sessions=100/MixedRegister50Percent | 188.36 | 33.81 | 5.57x | 1217.00 | 216.20 |
| Workload/sessions=100/RegisterWithBatchRenew | 446.42 | 123.62 | 3.61x | 1658.00 | 653.00 |
| Workload/sessions=1000/RenewOne | 78.03 | 75.98 | 1.03x | 176.80 | 166.80 |
| Workload/sessions=1000/RenewOneParallel | 34.18 | 35.09 | 0.97x | 300.70 | 456.70 |
| Workload/sessions=1000/RenewAllBatch | 13697.86 | 13626.33 | 1.01x | 18446.00 | 16829.00 |
| Workload/sessions=1000/RenewAllBatchParallel | 10737.34 | 10600.03 | 1.01x | 61344.00 | 50966.00 |
| Workload/sessions=1000/RenewAllBatchStale10 | 13453.70 | 13481.70 | 1.00x | 20389.00 | 17428.00 |
| Workload/sessions=1000/RegisterChurn | 3253.84 | 78.45 | 41.48x | 4990.00 | 160.40 |
| Workload/sessions=1000/RegisterChurnParallel | 3064.19 | 36.69 | 83.53x | 13926.00 | 368.90 |
| Workload/sessions=1000/MixedRegister1Percent | 71.94 | 35.03 | 2.05x | 3592.00 | 268.60 |
| Workload/sessions=1000/MixedRegister10Percent | 369.76 | 37.50 | 9.86x | 3807.00 | 302.50 |
| Workload/sessions=1000/MixedRegister50Percent | 1588.48 | 35.64 | 44.57x | 9912.00 | 340.60 |
| Workload/sessions=1000/RegisterWithBatchRenew | 3324.84 | 139.31 | 23.87x | 11450.00 | 928.50 |
| Hash/sessions=1/expired=0/ResolveHit | 76.87 | 81.45 | 0.94x | 146.30 | 152.40 |
| Hash/sessions=1/expired=0/ResolveHitParallel | 30.56 | 30.48 | 1.00x | 202.80 | 204.40 |
| Hash/sessions=1/expired=0/ResolveMiss | 73.59 | 68.20 | 1.08x | 122.60 | 131.20 |
| Hash/sessions=1/expired=0/ResolveMissParallel | 28.97 | 26.13 | 1.11x | 191.60 | 166.00 |
| Hash/sessions=1/expired=0/ResolveAll | 78.99 | 77.82 | 1.02x | 130.40 | 159.20 |
| Hash/sessions=1/expired=0/ResolveAllParallel | 30.90 | 33.53 | 0.92x | 201.50 | 246.10 |
| Hash/sessions=1/expired=0/RegisterReplace | 83.14 | 83.53 | 1.00x | 151.00 | 163.10 |
| Hash/sessions=1/expired=0/RegisterReplaceParallel | 34.73 | 33.43 | 1.04x | 235.00 | 214.90 |
| Hash/sessions=1/expired=0/RegisterNew | 89.45 | 78.61 | 1.14x | 149.80 | 138.50 |
| Hash/sessions=1/expired=0/RegisterCapacity | 78.26 | 77.80 | 1.01x | 157.90 | 157.10 |
| Hash/sessions=1/expired=0/HGETControl | 58.55 | 58.11 | 1.01x | 116.90 | 113.40 |
| Hash/sessions=1/expired=0/HGETControlParallel | 22.63 | 22.61 | 1.00x | 132.80 | 142.60 |
| Hash/sessions=1/expired=50/ResolveHit | 81.86 | 81.09 | 1.01x | 171.50 | 154.70 |
| Hash/sessions=1/expired=50/ResolveMiss | 84.27 | 67.90 | 1.24x | 192.80 | 131.20 |
| Hash/sessions=1/expired=50/ResolveAll | 80.80 | 82.96 | 0.97x | 157.50 | 164.70 |
| Hash/sessions=1/expired=50/RegisterReplace | 84.82 | 82.19 | 1.03x | 156.40 | 160.20 |
| Hash/sessions=1/expired=50/RegisterNew | 84.44 | 94.25 | 0.90x | 153.60 | 202.70 |
| Hash/sessions=10/expired=0/ResolveHit | 117.58 | 76.10 | 1.55x | 202.90 | 149.80 |
| Hash/sessions=10/expired=0/ResolveHitParallel | 51.76 | 29.96 | 1.73x | 396.30 | 220.50 |
| Hash/sessions=10/expired=0/ResolveMiss | 99.81 | 68.05 | 1.47x | 182.20 | 150.50 |
| Hash/sessions=10/expired=0/ResolveMissParallel | 49.78 | 25.72 | 1.94x | 325.50 | 159.00 |
| Hash/sessions=10/expired=0/ResolveAll | 143.64 | 140.91 | 1.02x | 239.90 | 261.40 |
| Hash/sessions=10/expired=0/ResolveAllParallel | 55.44 | 56.53 | 0.98x | 407.20 | 435.50 |
| Hash/sessions=10/expired=0/RegisterReplace | 116.37 | 77.04 | 1.51x | 200.10 | 153.30 |
| Hash/sessions=10/expired=0/RegisterReplaceParallel | 57.64 | 33.30 | 1.73x | 388.80 | 224.90 |
| Hash/sessions=10/expired=0/RegisterNew | 125.17 | 78.04 | 1.60x | 212.60 | 144.80 |
| Hash/sessions=10/expired=0/RegisterCapacity | 108.70 | 107.43 | 1.01x | 193.70 | 191.80 |
| Hash/sessions=10/expired=0/HGETControl | 59.07 | 56.84 | 1.04x | 105.10 | 107.00 |
| Hash/sessions=10/expired=0/HGETControlParallel | 22.03 | 23.04 | 0.96x | 132.80 | 162.30 |
| Hash/sessions=10/expired=50/ResolveHit | 126.76 | 82.59 | 1.53x | 213.80 | 154.00 |
| Hash/sessions=10/expired=50/ResolveMiss | 119.58 | 68.64 | 1.74x | 196.30 | 128.60 |
| Hash/sessions=10/expired=50/ResolveAll | 137.64 | 136.25 | 1.01x | 228.30 | 221.30 |
| Hash/sessions=10/expired=50/RegisterReplace | 127.69 | 81.47 | 1.57x | 206.10 | 146.90 |
| Hash/sessions=10/expired=50/RegisterNew | 128.23 | 78.45 | 1.63x | 191.70 | 143.50 |
| Hash/sessions=100/expired=0/ResolveHit | 420.52 | 75.56 | 5.57x | 592.90 | 140.20 |
| Hash/sessions=100/expired=0/ResolveHitParallel | 310.89 | 29.81 | 10.43x | 1491.00 | 213.80 |
| Hash/sessions=100/expired=0/ResolveMiss | 415.27 | 65.98 | 6.29x | 520.90 | 133.40 |
| Hash/sessions=100/expired=0/ResolveMissParallel | 308.41 | 25.68 | 12.01x | 1488.00 | 170.80 |
| Hash/sessions=100/expired=0/ResolveAll | 706.76 | 692.77 | 1.02x | 1103.00 | 1106.00 |
| Hash/sessions=100/expired=0/ResolveAllParallel | 356.70 | 360.64 | 0.99x | 2084.00 | 2420.00 |
| Hash/sessions=100/expired=0/RegisterReplace | 410.58 | 110.72 | 3.71x | 573.50 | 267.10 |
| Hash/sessions=100/expired=0/RegisterReplaceParallel | 312.43 | 33.53 | 9.32x | 1510.00 | 217.00 |
| Hash/sessions=100/expired=0/RegisterNew | 422.61 | 83.55 | 5.06x | 548.40 | 163.20 |
| Hash/sessions=100/expired=0/RegisterCapacity | 409.78 | 386.10 | 1.06x | 570.50 | 558.10 |
| Hash/sessions=100/expired=0/HGETControl | 61.68 | 58.52 | 1.05x | 135.80 | 99.84 |
| Hash/sessions=100/expired=0/HGETControlParallel | 22.97 | 22.99 | 1.00x | 149.20 | 142.70 |
| Hash/sessions=100/expired=50/ResolveHit | 469.28 | 86.87 | 5.40x | 595.00 | 163.40 |
| Hash/sessions=100/expired=50/ResolveMiss | 458.14 | 70.68 | 6.48x | 597.00 | 131.80 |
| Hash/sessions=100/expired=50/ResolveAll | 602.46 | 593.47 | 1.02x | 902.20 | 890.00 |
| Hash/sessions=100/expired=50/RegisterReplace | 463.98 | 83.08 | 5.58x | 623.70 | 162.30 |
| Hash/sessions=100/expired=50/RegisterNew | 468.80 | 79.77 | 5.88x | 659.80 | 168.20 |
| Hash/sessions=1000/expired=0/ResolveHit | 3063.00 | 76.02 | 40.29x | 3628.00 | 159.60 |
| Hash/sessions=1000/expired=0/ResolveHitParallel | 2902.14 | 29.33 | 98.94x | 12582.00 | 200.00 |
| Hash/sessions=1000/expired=0/ResolveMiss | 3104.90 | 67.49 | 46.00x | 5088.00 | 119.80 |
| Hash/sessions=1000/expired=0/ResolveMissParallel | 2899.78 | 24.30 | 119.33x | 12577.00 | 158.20 |
| Hash/sessions=1000/expired=0/ResolveAll | 5867.46 | 5806.22 | 1.01x | 8462.00 | 7678.00 |
| Hash/sessions=1000/expired=0/ResolveAllParallel | 4112.47 | 4199.22 | 0.98x | 18831.00 | 21081.00 |
| Hash/sessions=1000/expired=0/RegisterReplace | 3139.23 | 80.78 | 38.86x | 4778.00 | 144.50 |
| Hash/sessions=1000/expired=0/RegisterReplaceParallel | 2883.58 | 33.48 | 86.14x | 17579.00 | 215.90 |
| Hash/sessions=1000/expired=0/RegisterNew | 3125.39 | 109.48 | 28.55x | 4591.00 | 196.50 |
| Hash/sessions=1000/expired=0/RegisterCapacity | 2992.51 | 2753.39 | 1.09x | 4568.00 | 3423.00 |
| Hash/sessions=1000/expired=0/HGETControl | 91.39 | 57.55 | 1.59x | 225.20 | 111.50 |
| Hash/sessions=1000/expired=0/HGETControlParallel | 23.91 | 23.04 | 1.04x | 174.30 | 145.30 |
| Hash/sessions=1000/expired=50/ResolveHit | 3894.49 | 108.51 | 35.89x | 6805.00 | 208.20 |
| Hash/sessions=1000/expired=50/ResolveMiss | 3715.12 | 87.51 | 42.45x | 4642.00 | 163.60 |
| Hash/sessions=1000/expired=50/ResolveAll | 4971.20 | 5074.15 | 0.98x | 6715.00 | 7986.00 |
| Hash/sessions=1000/expired=50/RegisterReplace | 3498.91 | 113.78 | 30.75x | 3953.00 | 179.50 |
| Hash/sessions=1000/expired=50/RegisterNew | 3508.11 | 105.52 | 33.25x | 4792.00 | 185.90 |

## Verification and scope

- go test ./... and go vet ./... passed.
- The expanded WebSocket/routing race check passed with -race -count=10.
- TestRealRedisRoutingContract passed against a real Redis server, using the
  cross-compiled integration test binary inside the disposable Linux container.
- New regression tests exercise old-format hashes, expired same-ID quota checks,
  targeted expiry cleanup, tenant isolation, delayed-generation fencing and
  concurrent admission after pruning.
- Every one of the 672 A/B benchmark repeats passed its result/error/count checks;
  workload repeats also verify final cardinality and applicable generations.
- There is no schema migration or additional Redis key. Renewal, room operations,
  EventBus settlement and client-delivery guarantees are outside this source change.

These measurements use local TCP, disabled persistence, fixed case order and no
production RTT/TLS, other tenants or sustained arrival-rate/soak testing. Server
EVAL service time is from commandstats, not sampled CPU utilization. Go allocation
metrics do not include Redis memory. Fixture reset is outside timing in expired/
new-admission hash cases, so their rates are not sustained end-to-end throughput.
Small regressions/improvements in low-N/control cases and noisy tails must not be
interpreted as statistically established effects.

Re-run either binary on a disposable Redis with INFO commandstats permission and
no competing EVAL workload, using WS_REGISTRY_BENCH_ADDR and GOMAXPROCS=4:

```text
-test.run=^$ -test.bench=^(BenchmarkRegistryHash|BenchmarkRegistryRegisterRenew)$
-test.benchtime=1000x -test.count=3 -test.benchmem -test.timeout=20m
```

On Linux, build the current package test binary with `go test -c
./websocket/routing/redis`; the Windows-hosted run cross-compiled with GOOS=linux,
GOARCH=amd64 and CGO_ENABLED=0, then copied it into the disposable Redis container.
The preserved baseline binary fingerprint above identifies the pre-edit working
snapshot; baseline raw files are retained independently of the earlier reports.
