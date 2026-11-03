# HTTP client (v2)

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

A checked Resty client with bounded retry, circuit breaking, response limits and
OpenTelemetry transport instrumentation. Clients reuse their connection pool.

```go
cfg := httpclient.DefaultConfig("https://orders.example.com")
cfg.Timeout = 5 * time.Second
client, err := httpclient.NewClient(cfg)
if err != nil { return err }
defer client.CloseIdleConnections()
resp, err := client.Execute(ctx, func(request *resty.Request) (*resty.Response, error) {
    return request.SetHeader("Accept", "application/json").Get("/orders")
})
if err != nil { return err }
// Inspect resp.StatusCode(): HTTP 4xx/5xx remain responses with nil error.
```

`Timeout` bounds the whole Execute operation, including retries, waits and body
reads; shorter caller deadlines win. Zero uses 10 seconds. Buffered responses
are capped by `MaxResponseBodyBytes` (default 4 MiB). Streaming RawBody consumers
must close the body; their request context remains live until EOF/Close/deadline.

Retries default to GET, HEAD and OPTIONS on transport errors or HTTP 5xx.
`MaxRetries` counts retries after the initial attempt. Waits use bounded Resty
backoff and honor context cancellation. Parsing/body-limit errors are not retried.
Reader, multipart and form bodies are not retried. Open raw streaming responses
are returned without retry so no discarded stream is leaked. Buffered byte/string/JSON
bodies can be replayed; applications must not mutate them during execution.

For an operation whose server explicitly supports idempotency, use
`httpclient.WithIdempotentRetry(ctx)` and a stable application idempotency key.
This opts unsafe methods into retry, while preserving the body replay guard.
Without this opt-in, POST/PATCH/PUT/DELETE are attempted once.

Circuit breakers observe the final logical operation, count transport failures
and 5xx, and exclude caller cancellation. An open breaker returns gobreaker
errors before sending a request. Enabling it never changes HTTP status handling
into a new application error contract.

Inject `Transport`, `TracerProvider` and `Propagator` for custom TLS/proxies,
per-application telemetry and tests. Transport spans finish on network failure,
EOF, body close or request cancellation; parsed Resty error models are never
asserted as Go errors. Spans omit URL path/query and raw error text. Default Resty retry logging is
silent to prevent raw URL disclosure; applications own diagnostic logging and
can explicitly configure a trusted logger through Native.

`Native()` exposes trusted Resty configuration. Configure it before concurrent
use. Replacing transports/retry hooks/limits can bypass these guarantees.
When configured, BaseURL must be an HTTP(S) origin with an optional path prefix; credentials,
query strings (including an empty `?`), fragments, and opaque URLs are rejected.
For example, `https://example.com/api/v1/` plus `/orders` targets
`https://example.com/api/v1/orders`. Constructor validation also rejects malformed
URLs, negative timeouts/limits, invalid retry bounds and invalid breaker thresholds.

```sh
go test -race ./httpclient/...
```
