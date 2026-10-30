# WebSocket refactor measurements

Measured locally on 2026-10-30 using Go 1.26.3, Linux amd64, Intel i7-12700F.
Baseline: commit `b75269c`, exported to a temporary directory. Both revisions use
`GOMAXPROCS=4`; five runs, 10,000 iterations per run. Values below are medians.
The benchmark suite and live processes ran separately to avoid load interference.

## Comparable microbenchmarks

| Benchmark | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| BenchmarkConnectionSend | 65.17 | 62.77 | 48 | 48 | 1 | 1 |
| BenchmarkDataStructureMemory | 1547 | 905.7 | 8245 | 7657 | 18 | 11 |

The connection-construction benchmark allocates about 7.1% fewer bytes and drops
from 18 to 11 allocations. Connections share their shard's immutable config;
configuration normalization is no longer repeated per connection. These figures
exclude real socket buffers, TLS, kernel resources, registry metadata and active
handler work. Send allocation count is unchanged; small timing differences are
not evidence of a production throughput improvement.

Event-dispatch timing is not compared: the old harness busy-spins while waiting,
whereas the corrected harness yields. Old fan-out timing is also not comparable:
its consumers could be disconnected under overload, silently changing audience.

## Corrected fan-out harness

Each iteration verifies and drains every recipient. It includes queue drain,
keeps the audience fixed, and cleans up connections/executors. Five runs of
1,000 iterations, with `GOMAXPROCS=4`:

| Benchmark | Recipients | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|---:|
| BenchmarkSendToRoom | 100 | 8760 | 2128 | 6 |
| BenchmarkBroadcast100 | 100 | 9735 | 923 | 2 |
| BenchmarkBroadcast1000 | 1000 | 62076 | 8320 | 3 |

## Real broker enqueue sampling

A two-gateway example test used real Redis and an embedded real NATS JetStream
server, separate worker/router roles, real WebSocket clients, and 100 direct
routing samples to a connected user. One local smoke run (no race detector):

| Targeted transport | p95 | p99 | Samples/second |
|---|---:|---:|---:|
| Redis Pub/Sub + registry | 243.673 µs | 265.752 µs | 7,862.6 |
| NATS Core + Redis registry | 529.206 µs | 675.430 µs | 5,538.8 |

These are smoke measurements, not a broker ranking or capacity claim. They use
one serial producer, two small clients, no TLS, local broker processes, and
include registry lookup plus gateway enqueue acknowledgement. The sampling
path excludes business-task persistence and client-receipt acknowledgement.
Rerun with production-like payloads, concurrency, slow consumers, TLS, node
count and reconnect churn before sizing a deployment.

The contract suite verifies that user delivery contacts only nodes holding
sessions. Rooms fan out once per hosting node and return compact recipient
counts, so broker result size does not grow with room population. Redis session
and room lease refreshes use pipelines of at most 256 operations; destination
fan-out has at most 32 workers and transport has at most 1,024 pending requests.
Manager stats expose inbound/executor queue-wait buckets as `eventQueueLatency`.

## Reproduce

```bash
GOMAXPROCS=4 go test -run '^$' -bench '^(BenchmarkConnectionSend|BenchmarkDataStructureMemory)$' -benchmem -benchtime=10000x -count=5 ./websocket/core
GOMAXPROCS=4 go test -run '^$' -bench '^(BenchmarkSendToRoom|BenchmarkBroadcast100|BenchmarkBroadcast1000)$' -benchmem -benchtime=1000x -count=5 ./websocket/core
go test -tags=integration -count=1 -v ./examples/websocket/cluster
```

For CPU/heap profiles, add `-cpuprofile /tmp/ws-cpu.pprof
-memprofile /tmp/ws-heap.pprof -o /tmp/ws-core.test` to a focused benchmark run;
inspect using `go tool pprof`. For sustained socket load, use
`examples/websocket/loadtest`; for enqueue sampling use the cluster example's
`-role load`. Keep connection count, recipients, drop count, goroutine count,
RSS, payload size, broker traffic and queue occupancy alongside latency.
