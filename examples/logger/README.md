# Logger Module

## Overview
The `logger` module is a high-performance, structured logging library based on `golang.org/x/slog`. It natively supports asynchronous writing (to reduce I/O blocking), automatic log rotation (via Lumberjack), and seamless integration with OpenTelemetry and web frameworks like Fiber, Gin, and Echo.

## Key Features
- `logger.Info()`, `logger.Error()`, etc. for standard logging.
- `logger.InfoAsync()` for zero-blocking logging (great for high-throughput WebSocket servers or APIs).
- Built-in JSON vs Text formatting.

## 🚨 Best Practices for AI/Developers
- **Initialization**: Call `logger.SetDefault(l)` early in the `main()` function so that sub-packages can just call `logger.Info()` without passing the logger instance around.
- **Context Logging**: Always prefer `logger.InfoWithContext(ctx, "message", "key", "val")` over `logger.Info` when inside HTTP/gRPC handlers. This automatically extracts the `TraceID` from OpenTelemetry and `RequestID` from the framework context and injects them into the log line.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** call `defer logger.Close()` after initialization. If you use asynchronous logging (`InfoAsync`), the messages are queued in a channel. If you exit without calling `Close()`, the queued messages in memory will be permanently lost.
