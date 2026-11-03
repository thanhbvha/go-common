# HTTP client example

Run `go run ./examples/httpclient` to see a local flaky server recover through
bounded GET retries. `RunCircuitBreakerExample` shows HTTP failures counted by
the breaker, followed by an open-circuit error before another network request.

The v2 constructor returns `(*Client, error)`. Check construction errors, reuse a
client per downstream service, and pass request contexts into `Execute`.
HTTP status codes remain responses with nil Go error; inspect them explicitly.
POST and streaming bodies are not automatically retried. Unsafe methods require
an idempotency guarantee plus `WithIdempotentRetry` and a replayable body.

See [module contracts](../../httpclient/README.md) for transport injection,
response limits, tracing and total operation deadlines.
