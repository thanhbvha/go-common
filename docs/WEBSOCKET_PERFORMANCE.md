# WebSocket business performance — 2026-11-05

The Redis EventBus consumer previously blocked on an empty private retry stream
between batches from the shared notification stream. In the reference WebSocket
business flow, that limited completed delivery to about 300 operations/s and
produced p95 latency around 404 ms. Fair nonblocking intake now reaches the
workload's configured 1,000 operations/s with mean run p95 around 69 ms.

Public library APIs, driver defaults, retry isolation, leases and settlement are
unchanged. This is a latency/throughput optimization with a small allocation
cost; it does not improve every metric or establish production capacity.

## Implementation

`eventbus/driver/redis/intake.go` polls the main and private retry streams in
rotation, with each ownership-acquiring `XREADGROUP` limited to the already
reserved aggregate concurrency + prefetch budget. A ready lane never waits for
another lane's blocking timeout. This also works with a one-delivery budget.

If both lanes are empty, the reader snapshots their tail IDs, checks both lanes
again, and uses read-only `XREAD` to wait for new data on either lane. Appends
between the snapshot, recheck and waiter cannot be missed. This wake-up does not
create pending ownership; the next budgeted single-lane poll acquires work.
Verified empty cursors are reused across idle timeouts, so steady idle issues
one blocking command per configured interval rather than repeatedly polling.
Task consumers retain their single-stream blocking path.

Only `last-generated-id` is decoded from raw XINFO fields, supporting RESP2 field
pairs and RESP3 maps while ignoring newer Redis metadata. This was verified
against pinned Redis 7.4.7 and local Redis Cluster 8.10.2; the pinned go-redis
version's typed XINFO decoder rejected the newer server's `idmp-duration` field.

Custom Redis ACLs for event consumers must permit **XREAD and XINFO STREAM**.
Both lanes retain their shared cluster hash tag. No background reader or
unbounded queue was introduced.

## Measurement method and provenance

Artifacts are under ignored `artifacts/performance/`:

| Artifact | Evidence |
|---|---|
| `ws-business-investigation/` | Original command/notification/delivery correlation, block profile and timeout experiment |
| `ws-v2-ab-final/` | Ten alternating AB/BA pairs, 40 scenario results, comparison JSON and per-metric CI95 |
| `ws-v2-matched-load/` | Six alternating pairs at equal offered/completed work, 24 scenario results, sealed build overlays |
| `ws-v2-cpu-after/` | Eight CPU/allocation/live-heap/drained-heap profiles, matching server binary and pprof readback |
| `ws-v2-final-checks/` | Fourteen verification commands, including full race/build/vet, live single/cluster contracts and smoke |
| `ws-optimization-source/` | Exact source archives, old consumer source, negative-control regression evidence and final audit |

Primary measurements use `performance-v2`, 100 clients, 10 operations/s/client
for business, and ten room senders each sending at 10/s to ten recipients for
fanout. Each scenario warms for 5 s and measures for 30 s, without race,
profiling or MONITOR. Each repetition owns a fresh Redis fixture and server;
message and fanout scenarios share that server. Two checkouts have identical
harnesses and differ only in the production consumer call site in `redis.go`;
the baseline does not call the new private intake helper.

Host: i7-12700F, Linux/amd64, Go 1.26.10, GOMAXPROCS=4 for server and client,
`powersave` governor. Redis image and tools remain pinned in the manifest.
Full source/environment fingerprints and execution order are in the run records.
The recorded candidate source fingerprint is
`24ad87da30c8e97c0018dd21c4e1346c5f7d3a8dae1904cf84defcd9531a07b5`;
harness fingerprint is
`ce1ccb33ece0f6138520a2dc6a3981dd5ff8edaf7d7ea70f8ea4d52967da398c`.
Final documentation and a transport-entry regression were added afterwards;
runtime and measurement code remain as archived.

Values below are arithmetic means of independent run summaries. A latency cell
is the mean of each run's histogram percentile, not a pooled request percentile.
CI95 uses 10,000 bootstrap resamples of independent runs, with the interval for
candidate minus baseline. Intervals are per metric, not a simultaneous guarantee
across all exploratory comparisons.

## Primary A/B results

| Metric | Before | After | Difference CI95 |
|---|---:|---:|---:|
| Business completed operations/s | 300.59 | 999.98 | +698.16 to +700.65 |
| Business p50 | 318.80 ms | 35.80 ms | -283.70 to -282.35 ms |
| Business p95 | 404.33 ms | 69.36 ms | -337.40 to -332.28 ms |
| Business p99 | 418.29 ms | 91.60 ms | -330.13 to -322.78 ms |
| Fanout completed room sends/s | 100.00 | 100.00 | -0.0010 to +0.0009 |
| Fanout p95 | 7.931 ms | 8.056 ms | -0.039 to +0.296 ms |
| Fanout p99 | 9.298 ms | 9.928 ms | -0.205 to +1.698 ms |

Business throughput increased 3.33× and p95 fell 82.85% in this workload. The
candidate reaches the offered-load ceiling, so **1,000/s is not a measured
maximum capacity**. No statistically clear default fanout p95/p99 regression
was established; uncertainty still permits small changes.

All measured operations passed exact-content receipt checks. Warmup plus
measurement receipt totals match actual sends, including ten receipts per room
send. Task/subscriber snapshots have known lag and zero pending, in-flight,
delayed and dead-letter work before the client quiet window and shutdown.

## Memory and smaller tradeoffs

At default load, business allocation increases from **52,247 to 52,725 B/op**
(+0.915%) and **822.8 to 833.2 allocations/op**. Fanout allocation rises from
18,980 to 19,565 B/room-send (+3.09%) in the shared-server sequence. These are
measured costs, not allocation improvements.

Absolute retained/peak heap is higher in the default candidate run, which also
processes about 3.3× as many business messages. The existing gateway dedupe cache
retains message IDs with unchanged capacity 100,000 and TTL one minute. The
post-change sampled retained-heap profile attributes about 4.16 MiB directly to
`deliveryCache.remember`; decoded strings also remain retained. This is evidence
of volume-dependent retained state, not a claim that every byte is explained by
the cache.

To control that confound, a separate versioned build overlay sets pacing to
2 operations/s/client on both sides, without editing either checkout. Six pairs
warm for 2 s and measure for 10 s. Every run completes exactly 2,000 business
operations and 200 room sends; receipt totals including warmup also match.
Overlay sources, SHA-256 identity and build commands are retained in each sample.
These results are not mixed with the default experiment.

| Equal-work metric | Before | After | Difference CI95 |
|---|---:|---:|---:|
| Business heap after drain | 8.404 MB | 8.450 MB | -0.320 to +0.387 MB |
| Business peak heap | 16.913 MB | 16.599 MB | -0.743 to +0.080 MB |
| Fanout heap after drain | 11.529 MB | 11.437 MB | -0.488 to +0.294 MB |
| Fanout peak heap | 16.702 MB | 16.689 MB | -0.104 to +0.084 MB |
| Business allocated B/op | 52,617 | 53,270 | +629 to +678 B/op |
| Business p95 | 350.28 ms | 66.21 ms | -298.23 to -269.07 ms |

MB here is 1,000,000 bytes. Equal-work heap intervals include zero; business
allocation still increases about 1.24%. The low-load fanout p50 increases from
4.495 to 4.922 ms (difference CI95 +0.029 to +0.799 ms), while its p95/p99
intervals include zero. Treat this small exploratory median change and the
allocation cost as tradeoffs, rather than claiming every latency/memory measure
is unchanged. The default fanout p50 interval includes zero.

The retained change is justified by the large, reproducible primary latency and
throughput benefit, bounded intake state, equivalent equal-work retained heap,
and preserved correctness. This is not a memory optimization. Re-evaluate the
small allocation/low-load median costs if an application's SLO makes them material.

## Profiles, verification and remaining work

The post-change CPU profile has 29.88 CPU-seconds sampled over 30.21 wall-seconds
while serving approximately 1,000 business operations/s. Syscalls account for
about 28.9% flat and allocation about 8.2% cumulative CPU samples. Raw CPU totals
must not be compared to the old profile without accounting for different
completed work. Profiles are excluded from all baseline comparisons.

New tests cover empty-main/ready-retry and the reverse direction, fairness,
aggregate budgets 1/2/3/30, wake-up append races, read-only ownership, idle cursor
reuse, metadata compatibility, cancellation/errors and the broker-drain barrier.
A regression through `Consume` passes on the candidate and fails on the original
consumer in a test-overlay negative control, without a timing threshold.

Full race, build, vet, boundary and module-integrity checks passed. Live tests
exercise Redis single/cluster wake-up, retention and transport contracts, Redis
WebSocket bridge/routing, and example Redis/Redis-to-NATS paths. The existing
transport contract suite explicitly skips eight optional capability cases in
total (four each for single and cluster): post-commit ambiguous retry/DLQ fault
boundaries, deterministic scheduler-owner recovery and advertised ordering.
Those capabilities were not asserted by this integration harness; fixture
availability and all newly added integration cases were required and passed.

The unchanged limits of the experiment remain: local plaintext single-node
business delivery, synthetic lightweight handler, bounded closed-loop clients,
short runs and no production DB work. Redis Cluster testing verifies compatibility
and contracts, not clustered throughput. Next capacity work needs an arrival-rate
ramp with explicit SLOs, realistic payload/business costs, TLS/multi-node fixtures
and a soak long enough to observe cache steady state. No further runtime hotspot
was changed speculatively.

Reproduce the main comparison with matching archived source checkouts:

```bash
python3 benchmarks/performance.py ab --suite integration --workload websocket \
  --base-source /path/to/baseline --candidate-source /path/to/candidate \
  --count 10 --cpu 4 --warmup 5 --duration 30
python3 benchmarks/performance.py profile --suite integration --workload websocket \
  --profile-kind cpu --cpu 4
```

The exact secondary overlay experiment is retained as
`artifacts/performance/ws-optimization-source/matched_load.py`. Preserve the
ignored artifact directory when sharing binaries, raw samples and source archives.
