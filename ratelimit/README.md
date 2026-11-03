# Rate limiting (v2)

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

Atomic Redis fixed-window request quotas, with adapters for net/http, Fiber,
Gin and Echo. A window expires `Window` after the first request for its key.
Traffic across a window boundary can reach twice the configured limit; choose
an algorithm at the application edge if a strict sliding-window bound is needed.

```go
limiter, err := ratelimit.NewRedisLimiter(redisClient, "orders:api")
if err != nil { return err }
cfg := ratelimit.MiddlewareConfig{
    Policy: ratelimit.Config{Limit: 100, Window: 1500*time.Millisecond},
    FailurePolicy: ratelimit.FailClosed,
    OnError: func(ctx context.Context, err error) { /* record an outage metric */ },
}
limit, err := nethttplimit.Middleware(limiter, cfg, nil)
if err != nil { return err }
handler := limit(mux)
```

`NewRedisLimiter` accepts `redis.UniversalClient` for standalone, Sentinel and
Cluster. The client remains owned by the application. Namespaces/prefixes must
be distinct for unrelated quota policies. `Config.Validate` requires a positive
limit and a positive window expressed in whole milliseconds; there is no
seconds truncation. The same validation runs when calling `Allow` directly.

HTTP middleware construction requires an explicit failure policy:

- `FailClosed`: backend failure or missing quota result returns HTTP 503.
- `FailOpen`: bypasses failed checks; use when application availability outweighs
  quota enforcement. `OnError` still observes the failure.

Quota exhaustion returns 429 and `Retry-After`, with `X-RateLimit-Limit`,
`X-RateLimit-Remaining` and `X-RateLimit-Reset`. Empty generated keys follow the
selected failure policy. Error observers must be fast and concurrency-safe.

All four adapters expose `Middleware(limiter, MiddlewareConfig, KeyGenerator)`
and return `(handler, error)`; check errors during bootstrap. The net/http
resolver uses the direct peer address and ignores forwarding headers. Configure
trusted proxies before using the framework IP resolvers, or provide a key
function based on authenticated user/API key. Place identity quotas after auth.

```sh
go test -race ./ratelimit/...
```

See [runnable examples](../examples/ratelimit/README.md).
