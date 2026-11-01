# DataLoader benchmark baseline

Measured on 2026-11-01, Windows/amd64, Go 1.27.3,
Intel Core i7-11800H, with the same in-memory map fetch function and
`MaxBatch: 100`. Command: `go test ./graphql -run '^$' -bench . -benchmem -benchtime=10x -count=1`.

The baseline was captured before replacing the original implementation.
The new constructor adds request context and configuration validation.

| Unique keys per LoadAll | Before ns/op | After ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op | Backend calls/op (both) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16,289,700 | 1,503,490 | 1,003 | 1,422 | 18 | 23 | 1 |
| 100 | 16,570,260 | 46,990 | 44,484 | 24,659 | 291 | 153 | 1 |
| 1,000 | 17,302,070 | 309,340 | 383,732 | 298,676 | 2,544 | 1,279 | 10 |

The default window changed from 16 ms to 1 ms. Full batches now dispatch
immediately. Those deliberate behavior changes explain much of the latency
difference; this is not an isolated comparison of scheduler CPU cost.
A single-key miss adds a few allocations for bounded request-scoped behavior.
Backend latency, database contention, authorization and network costs are absent.

Additional new-implementation samples:

| Scenario | ns/op | B/op | allocs/op | Backend calls/op |
| --- | ---: | ---: | ---: | ---: |
| Primed cache hit | 70 | 0 | 0 | 0 |
| 1,000 input keys, 10 unique | 1,563,630 | 35,745 | 45 | 1 |
| Parallel isolated requests, 4 input keys / 3 unique | 160,680 | 2,189 | 32 | 1 |

The separate resource benchmark uses 1,000 unique keys and a simulated 1 ms
backend delay. It sampled a peak of 20 additional goroutines during backend
execution (4,650,640 ns/op). This includes batch timers and runtime noise,
is not a universal bound, and was not measured for the old implementation.
The wrapper enforces at most four concurrently executing backend batches;
unit tests also verify that limit and deduplication under concurrent callers.

These are short, ten-iteration development measurements, not statistically
significant production capacity estimates or latency percentiles. Repeat with
`-count=3` or more on an otherwise idle host and representative backend load
before setting SLOs. Future comparisons should hold Wait and batch sizes constant
when measuring scheduler overhead independently.

Functional gates include request/query limits, error privacy, pagination,
request isolation, metric labels and balanced in-flight counts, all four adapter
contexts, panic/cancellation behavior and race checks. Go 1.26 compatibility
must be checked separately from performance measurements on Go 1.27.
