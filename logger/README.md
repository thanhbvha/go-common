# logger

Async structured logger with request middleware for standard `net/http`, Fiber,
Gin, and Echo. Framework adapters live in isolated subpackages so applications
only import the stack they use.

```go
import "github.com/thanhbvha/go-common/logger"

// Create with defaults (stdout, INFO level).
log := logger.New(logger.DefaultOptions())
logger.SetDefault(log)
defer logger.Close()

// Synchronous
logger.Info("server started", "port", 8080)
logger.Error("handler error", "err", err)

// Async (non-blocking, worker pool flushes in background)
logger.InfoAsync("request processed", "latency_ms", 12)
logger.WarnAsync("cache miss", "key", key)

// With file rotation (lumberjack)
log := logger.New(logger.Options{
    Level:  slog.LevelDebug,
    StdOut: true,
    File: &logger.FileOptions{
        Path:       "/var/log/myapp/app.log",
        MaxSizeMB:  50,
        MaxBackups: 7,
        MaxAgeDays: 30,
        Compress:   true,
    },
})

// Attach request-id via context
ctx = ctxkey.SetRequestID(ctx, requestID)
log.InfoWithContext(ctx, "request received", "method", r.Method)
```

HTTP middleware import paths:

- `logger/middleware/nethttp`
- `logger/middleware/fiber`
- `logger/middleware/gin`
- `logger/middleware/echo`

### Key types

| Symbol | Description |
|---|---|
| `Options` | Top-level config (level, stdout, file, workers, buffer) |
| `FileOptions` | lumberjack rotation config (optional — nil = no file) |
| `Logger` | Struct — `Info/Error/Warn/Debug` sync; `*Async` async variants |
| `SetDefault(l)` | Register as process-wide default |
| `Default()` | Retrieve default (nil if not set) |
| `Close()` | Flush & shut down default logger |
