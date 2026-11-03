# Logger Module

## Overview
The `logger` module is a structured logging runtime based on the standard
`log/slog` package. It is synchronous by default, with optional bounded async
admission, Lumberjack file rotation, and HTTP middleware for standard
`net/http`, Fiber, Gin, and Echo.

## Key Features
- `logger.Info()`, `logger.Error()`, etc. for standard logging.
- `logger.InfoAsync()` uses the runtime's configured mode; the `Async` suffix is
  retained for API compatibility and does not itself make a synchronous runtime
  non-blocking.
- Built-in JSON vs Text formatting.

## 🚨 Best Practices for AI/Developers
- **Initialization**: Call `logger.SetDefault(l)` early in the `main()` function so that sub-packages can just call `logger.Info()` without passing the logger instance around.
- **Context Logging**: Prefer `logger.InfoWithContext(ctx, "message", "key", "val")` inside request handlers. It appends the shared request ID when present; add trace identifiers explicitly when your application policy permits them.
- **Async mode**: Set `Options.Async=true`, select a bounded queue and choose the documented overflow policy explicitly. The default overflow policy writes synchronously when the queue is full.
- **Graceful Shutdown**: Register the root runtime's `Shutdown(ctx)` with the application lifecycle after producers. Child sloggers add fields only and do not own shutdown.
