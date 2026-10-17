# EventBus benchmark baseline

This file records performance baselines before the refactor. These numbers are
not release targets. Compare changes on the same machine with `benchstat` or an
equivalent statistical comparison; results from different machines are not
directly comparable.

## Baseline: 2026-10-15

Environment:

- OS: Windows
- Architecture: amd64
- CPU: 11th Gen Intel Core i7-11800H @ 2.30 GHz
- Go package: `github.com/thanhbvha/go-common/eventbus`
- Parallel benchmark workers reported by Go: 16

Command:

```powershell
$env:GOCACHE='D:\Go\go-common\.gocache-review'
go test ./eventbus -run '^$' -bench 'Benchmark(Codec|Enqueue|EngineDispatch|MemoryTransport)' -benchmem -count=3
```

Median of three runs:

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| CodecEncode | 1,573.0 | 560 | 6 | Envelope v2 plus JSON payload encoding |
| CodecDecode | 495.1 | 496 | 4 | 208-byte envelope; 420.15 MB/s on median run |
| Enqueue | 870.1 | 544 | 6 | Encode plus synchronous sink transport publish |
| EngineDispatch | 6,130.0 | 2,194-2,208 | 25 | Sequential enqueue-to-handler signal through memory transport |
| MemoryTransportRoundTrip | 1,182.0 | 144 | 1 | Publish, receive delivery, and ACK |

Raw ranges:

- CodecEncode: 1,506-1,754 ns/op.
- CodecDecode: 459.2-513.7 ns/op.
- Enqueue: 853.3-987.5 ns/op.
- EngineDispatch: 5,534-6,186 ns/op.
- MemoryTransportRoundTrip: 1,140-1,262 ns/op.

## Benchmark rules

- Benchmarks must not synchronize with fixed sleeps.
- Broker benchmarks belong to the integration/benchmark environment, not the
  hermetic default suite.
- Record payload size, concurrency, success/retry mix, and backlog for all
  transport comparisons.
- Do not optimize solely for `ns/op`; settlement correctness and bounded
  resource use remain release gates.
