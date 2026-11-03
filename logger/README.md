# Logger

Structured logging with `log/slog`, optional file rotation, and an explicitly owned runtime.
Synchronous logging is the default. Enable async mode only after measuring the workload.

```go
runtime := logger.New(logger.DefaultOptions())
log := runtime.Logger()                  // *slog.Logger
worker := log.With("component", "worker") // fields only; no lifecycle ownership
worker.InfoContext(ctx, "started")
// Stop producers first, then drain accepted records and close owned files.
if err := runtime.Shutdown(shutdownCtx); err != nil { return err }
```

`logger.SetDefault(runtime)` is optional process bootstrap configuration. `logger.With`
returns a native slog logger and never creates hidden workers. `Close()` is a five-second
convenience; use `Shutdown(ctx)` to observe errors. Repeated/concurrent shutdown shares
one drain. After shutdown, the handler returns `ErrClosed` and accepts no new records.
Slog's convenience methods do not return handler errors.

For async mode set `Options.Async=true`; queue capacity defaults to 1024 and workers to
one. `OverflowSync` writes on the caller when full; `OverflowDrop` reports `ErrDropped`
through `OnError`. All logging methods follow the configured mode, including the
`*Async` convenience methods. A queue does not guarantee ordering across workers.

`OnError` must be prompt and concurrency-safe; it must not invoke shutdown on the same
runtime. Values in async `slog.Any` attributes must remain immutable until drained.
Use scalar attributes or pre-serialize mutable payloads before logging. A caller's
shutdown deadline bounds waiting; cleanup continues, so a blocked writer is never
closed underneath an active write. Supplied `Writer` values are borrowed and never closed.

File rotation is opt-in through `Options.File`. HTTP adapters remain in
`logger/middleware/{nethttp,gin,echo,fiber}`. Request IDs use `utils/ctxkey`.
See [validation and maturity](../docs/PRODUCTION.md) and [v2 API changes](../docs/V2.md).
