# Gateway coordination and drain verification

Measured 2026-11-03 on Windows amd64, Go 1.27.3, Intel i7-11800H.
Baseline: `e322e85` gateway source, loaded through a temporary Go overlay.
Both revisions ran the same `BenchmarkGatewayDelivery` harness, sequentially,
with `-cpu=4 -benchtime=500ms -count=3`. No other test/benchmark job ran during
this final comparison. Values below are medians of three runs.

The harness uses immutable payloads, unique message IDs and nonblocking no-op
enqueues. Every call verifies the accepted recipient count. It excludes Redis,
NATS, actual connection queues, socket writes, TLS and client receipt. Parallel
single-recipient calls deliberately contend for the same session; parallel room
calls share the same 1,000-session audience. These are coordination measurements,
not deployment capacity or end-to-end latency guarantees.

| Workload | Baseline ns/op | Current ns/op | Baseline B/op | Current B/op | Baseline allocs/op | Current allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| Single session, serial | 1,801 | 1,650 | 896 | 877 | 20 | 15 |
| Same session, parallel | 2,403 | 3,818 | 920 | 1,123 | 20 | 16 |
| Room 1,000, serial | 1,320,060 | 388,453 | 1,032,262 | 173,649 | 15,026 | 19 |
| Room 1,000, parallel | 1,327,853 | 172,533 | 1,033,167 | 165,234 | 15,026 | 18 |

Room fan-out is about 3.4x faster serially and 7.7x faster with parallel callers
in this harness, with about 83–84% fewer allocated bytes. Snapshotting recipients
outside enqueue, typed incarnation keys and striped bounded dedupe remove most
per-recipient JSON allocations and allow unrelated recipients to make progress.

The parallel hot-session case is about 59% slower. All calls still serialize on
one session to preserve enqueue/dedupe ordering; cancellable gate waiting and
shutdown tracking add coordination overhead. This workload is a measured tradeoff,
not an improvement claim. Include hot users, slow clients and reconnect churn in
service-specific sizing tests. Dedupe capacity is divided among at most 64 stripes;
hash skew may evict entries before aggregate capacity is reached.

[Raw before/after output](gateway-drain-20261103-windows.txt).

```powershell
go test -run '^$' -bench '^BenchmarkGatewayDelivery$' -benchmem -benchtime=500ms -count=3 -cpu=4 ./websocket/routing
```

Regression coverage includes canceled session-gate waits, independent cleanup
concurrency and its configured limit, renewal/remove fencing, busy and expired
sessions, legacy partial-renewal ownership checks, concurrent dedupe and rejected
enqueue retries, replacement generations, gateway close versus a late remote
registration commit, FIFO outbound drain, forced-close counters and NATS callback
joining. Redis contract tests run both compatibility and indexed room readers;
targeted cases cover mixed leases, removing a node's last member, old-generation
cleanup and node-only resolution.

The full repository tests and vet checks pass. Race coverage runs ten times over
core, pools, pub/sub, adapters, routing and the EventBus bridge. Redis tests use
miniredis; the NATS transport tests start a real embedded NATS server. Tagged live
broker tests and production-like room-index performance were not run: no disposable
Redis was selected and the local Docker daemon was unavailable. The optional node
index adds writes and metadata; follow the [rollout guidance](../routing/README.md#room-lookup-and-node-index-rollout)
and measure against a disposable real Redis before enabling indexed reads on an
existing namespace.
