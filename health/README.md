# Health

Concurrent, bounded health probes with a standard-library-only core. HTTP adapters live
under `health/handler/{nethttp,gin,echo,fiber}`.

```go
checker := health.New().WithTimeout(500 * time.Millisecond)
checker.AddReadinessCheck("database", sqlchecker.New(sqlPool))
checker.AddReadinessCheck("redis", redischecker.New(redisClient))
mux.Handle("GET /live", healthhttp.LiveHandler(checker))
mux.Handle("GET /ready", healthhttp.ReadyHandler(checker))
```

Import SQL and Redis integrations from `health/checker/sql` and `health/checker/redis`.
`sqlchecker.New` accepts a narrow `PingContext` interface, including `*sql.DB`; obtain
that pool from GORM with `db.DB()`. Integration dependencies are not imported by core
`health` or `foundation`. `HTTPChecker` and `CustomChecker` remain available in core.

Readiness checks dependencies; liveness should describe local process health so an
external outage does not cause a restart loop. Foundation adds its own lifecycle
readiness check automatically. Prefer Foundation's checker in a Foundation application.

Timeout defaults to five seconds. Each registered callback has at most one execution
in flight. If a callback ignores cancellation, the probe times out and subsequent
probes report `ErrCheckBusy` until that callback returns. Go cannot forcibly terminate
an arbitrary callback; checkers must still honor context. This bounds runaway executions
without claiming to stop uncooperative code.

Raw errors are hidden by default. `WithErrorDetails(true)` is for trusted diagnostics.
Panics are converted to `ErrCheckPanic`, without exposing their value or stack.
Configuration methods are concurrency-safe. Nonpositive timeouts, empty/duplicate names,
and nil callbacks panic as programmer configuration errors.

See [production validation](../docs/PRODUCTION.md).
