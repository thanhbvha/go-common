# Reproducible performance measurements

The versioned manifest in `benchmarks/manifest.json` describes the workloads.
`benchmarks/performance.py` runs them sequentially, retains raw evidence, profiles
one server/package at a time, and compares compatible baselines. Run from a stable
checkout on an otherwise idle Linux host. Results describe these workloads and
this machine, not deployment capacity or an application SLO.

See [the first verified evidence and optimization results](PERFORMANCE_RESULTS.md)
for the completed local suite and alternating readiness comparison. See the
[WebSocket business follow-up](WEBSOCKET_PERFORMANCE.md) for the ten-pair Redis
intake comparison, equal-work memory control and measured tradeoffs.

The current manifest is `performance-v2`. It adds observed WebSocket broker
drain and delivery counts; v1's WebSocket backlog fields were unmeasured default
zeros. Recollect both A/B sides with v2 rather than mixing the two methods.

## Run

Requirements: the Go version in `go.mod`, Python 3.10+, and Docker for integration
workloads. Images are pinned by digest. The optional comparison tool is installed
to ignored `artifacts/performance-tools/` at the exact revision in the manifest;
it does not change `go.mod` or `go.sum`.

```bash
# Fast correctness check; one iteration, not a performance baseline.
python3 benchmarks/performance.py run --suite core --mode smoke

# Complete baseline, including disposable infrastructure.
python3 benchmarks/performance.py run --suite all --mode baseline

# Select workloads, or one exact benchmark leaf listed in the manifest.
python3 benchmarks/performance.py run --mode baseline --workload health
python3 benchmarks/performance.py run --mode baseline --workload graphql --benchmark BenchmarkDataLoaderCached

# Short integration check while developing the harness.
python3 benchmarks/performance.py run --suite integration --mode smoke --duration 1 --warmup 0.2
```

Default Go baselines use `-run '^$' -benchmem -benchtime=1s -count=10 -cpu=1,4
-p=1`, without the race detector. Redis registry baselines use `1000x` per case;
the existing percentile sampler retains one sample per iteration, so the runner
requires an iteration bound. Smoke uses `1x`. Each workload's exact expected
benchmark inventory is checked, including repeat counts and CPU suffixes. New or
renamed benchmark cases must update the manifest.

Load workloads use 5 seconds of warmup, 30 seconds of measurement and at most
15 seconds of drain, repeated in ten independent client/server processes with
fresh containers per repetition. For HTTP and WebSocket, the two scenarios in a
repetition share the server process; each scenario has a new client process and
its own warmup. The runner rejects missing fixtures, skipped cases, failed
commands, incomplete result sets and source changes during measurement.
Load server and client processes use `GOMAXPROCS=4`; `--cpu` selects CPU settings
for Go microbenchmarks rather than changing the load processes.

`--count`, `--cpu`, `--benchtime`, `--duration`, `--warmup`, `--timeout`, and
`--output` support focused experiments. Overrides are recorded; they produce a
different experiment and must match on both sides of a comparison. Put outputs
under ignored `artifacts/` or outside the checkout. Leave `GOFLAGS` unset; build
or race overrides must not silently change a benchmark.

## What each workload measures

| Workload | Measurement and fixed configuration |
|---|---|
| Core | JWT validation; logger admission; readiness; request-scoped DataLoader batching/cache; EventBus codec, enqueue and memory dispatch; WebSocket queue fanout, construction and gateway coordination |
| PostgreSQL cursor | 1,000 rows, page size 50, pool size 2; real SQL and bounded concurrent callers |
| Redis registry | All existing lookup/register/renew cases and 1/10/100/1,000-session cardinalities; isolated Redis command statistics |
| HTTP | `/v1/me`, JWT verification/revocation and quota checks against real Redis, concurrency 1 and 16, quota 1,000,000,000; every response body and status checked |
| EventBus | Redis, NATS JetStream and Kafka; 1 KiB data field plus JSON/envelope overhead; 10 workers and 10 bounded in-flight producer operations; immediate delivery and full queue drain |
| WebSocket message | 100 sockets; `business` through Redis EventBus worker/router back to the authenticated client; exact reply content checked |
| WebSocket fanout | 100 sockets in groups of 10; one outstanding send per room, every recipient's content checked; latency ends after the last group member receives |

`/v1/me` does not query SQL. PostgreSQL is exercised by the separate cursor
workload and by readiness during server startup. These are local plaintext,
single-node fixtures with no OTLP exporter. Their costs must not be presented as
TLS, multi-node or production database results.

HTTP and EventBus use a closed-loop saturated producer. WebSocket is paced at
10 operations/second per worker (100 workers for message, 10 room workers for
fanout), below the example's unchanged per-connection protection limits. Fanout
throughput counts completed room sends; multiply by 10 for recipient deliveries.
A worker never starts a second operation before its previous operation completes.
These latency percentiles omit externally queued arrivals and therefore cannot
establish an open-loop arrival-rate SLO. Achieved throughput and drain are retained.

EventBus reuses the example's runtime, topology options, quiet observers and drain
validation. Its client waits for a fixed 8-byte local pipe acknowledgement after
the worker handler completes; latency includes that acknowledgement cost and
ends before broker settlement. Final broker stats separately verify settlement
and zero pending, delayed, in-flight, DLQ and lag. The producer and worker use
separate processes, so worker profiles exclude producer encode/publish work.

WebSocket duration mode reuses the existing loadtest connection/authentication,
reader and protocol paths. It waits for all three roles to start. Clients remain
connected while the server verifies task and subscriber stats with known lag,
zero pending/in-flight/delayed/DLQ, then observe another 100 ms for late replies.
`broker-stats.json` retains the observed task/subscriber snapshots;
`backlog_measured=true` and `broker_drain_seconds` identify the settlement check.
Client `drain_seconds` still measures outstanding operations after the load window,
separately from the subsequent broker barrier. `sent_operations`,
`expected_receipts` and `received` count actual sends and validated receipts across
warmup plus measurement; throughput and latency cover measurement only. The
comparator rejects missing counts or unmeasured WebSocket backlog. Server heap
after drain is sampled after this barrier, quiet window and client shutdown.
Original finite/churn/slow-reader modes and the
EventBus production-scale matrix remain available for larger payloads,
delayed/retry loads, TLS, churn and slow readers; they are not silently mixed
into this baseline.

## Profiles and optimization

```bash
# Focus one exact case and one GOMAXPROCS setting.
python3 benchmarks/performance.py profile --workload health --benchmark BenchmarkReadiness --cpu 4 --benchtime 10s --profile-kind cpu
python3 benchmarks/performance.py profile --workload health --benchmark BenchmarkReadiness --cpu 4 --benchtime 3s --profile-kind memory

# Profile the actual server/worker; the client runs separately.
python3 benchmarks/performance.py profile --suite integration --workload http --cpu 4 --profile-kind cpu
python3 benchmarks/performance.py profile --suite integration --workload eventbus-redis --cpu 4 --profile-kind memory
```

`cpu`, `memory`, `mutex` and `block` are separate runs. Each profile retains the
matching `benchmark.test` or `server.test` binary and a verified `pprof -top`
report. Load profiles also retain live and post-drain heap snapshots. Memory
profiling records every allocation and is deliberately excluded from timing
comparisons. Server and client process heap/allocation/goroutine metrics are
separate; peaks are sampled every 100 ms, not exact maxima or RSS. Server totals
include the fixture's server-side roles and the lightweight sampler.

Inspect a saved profile with its matching binary:

```bash
go tool pprof -http=127.0.0.1:8082 /path/to/server.test /path/to/cpu.pprof
# Allocation and retained-heap views of Go memory profiles:
go tool pprof -sample_index=alloc_space -top /path/to/benchmark.test /path/to/alloc.pprof
go tool pprof -sample_index=inuse_space -top /path/to/benchmark.test /path/to/alloc.pprof
```

Rank hotspots by contribution to a relevant workload, then change at most one or
two at a time. Keep the same harness on both source revisions: harness changes
create new baselines, not runtime speedups. Hold payload, batch window, queue and
pool sizes, concurrency, images, compiler and runtime environment constant.

## Compare and keep evidence

For alternating A/B measurement, prepare two existing checkouts with identical
harness files and the intended runtime difference. The runner never checks out,
commits, resets or modifies source files in either checkout:

```bash
python3 benchmarks/performance.py ab --base-source /path/to/base --candidate-source /path/to/candidate --workload health
```

This runs ten AB/BA pairs, validates source stability and compatible environments,
retains every independent run, aggregates matching collections and runs the
comparison. Both Go and load workloads can be selected. Do not alter either
checkout while it runs. `experiment.json` records the exact order.

For independently collected compatible baseline directories:

```bash
python3 benchmarks/performance.py compare --base artifacts/performance/base --candidate artifacts/performance/candidate
```

The runner records revision, dirty state, full source and harness fingerprints,
Go/compiler settings, OS/CPU, CPU affinity, cgroup limits, memory, governor and
pinned image identities. SHA-256 inventories detect missing/changed raw files.
They are corruption checks, not signatures or an attestation of trusted CI.

Comparison rejects smoke/profile runs, fewer than six repetitions, changed or
missing artifacts, different case inventories, environment, images, configuration
or harness. CPU/time and allocation comparisons use pinned `benchstat`.
Load comparisons bootstrap independent run-level summaries with 10,000 resamples
and a 95% percentile interval for candidate minus base. Requests within one run
are not independent repetitions. Reports include latency, throughput and server
memory/allocation/goroutine comparisons; statistical significance alone is not a
production benefit or a guarantee across many simultaneous hypothesis tests.

For optimization acceptance, alternate base/candidate execution on the same host
in one measurement session. Compare matching complete collections, review the
profile, and require improvement in the selected metric without clear regression
in latency, throughput, memory, correctness or lifecycle. Do not compare numbers
from different hosted CI machines or automatically promote the latest artifact
to a performance gate. If improvement is not established, retain the evidence and
drop the runtime change.

CI runs core smoke on PR/push. A weekly or manual workflow retains full baseline
artifacts; a manual profile selects a workload and profile kind. The former
WebSocket-only benchmark job is consolidated into this workflow. Timing noise
never blocks a PR in this initial version. Baselines run one manifest workload
at a time. CI measures registry CPU 1 and CPU 4 in separate runs, each with ten
samples per case, to stay within the per-command time budget on hosted runners.
Compare matching CPU configurations; the combined local registry run and these
individual CI runs intentionally have different configurations. Artifacts are under
`artifacts/performance/<run-id>/`, with `run.json`, raw streams, command exit
records, fixture cleanup records, metrics and `report.md`.

## Harness verification

```bash
python3 -m unittest discover -s .github/scripts -p 'test_performance.py'
go test -race ./internal/perftest ./examples/websocket/loadtest
```

Tests cover invalid/missing samples, failed commands, timeout/interruption,
owned-resource cleanup, artifact corruption, incompatible comparisons, bounded
histograms and load phase failures. Run package contracts/integration tests for
any optimized adapter and repository build, vet, boundaries and full race tests
before accepting an optimization.
