# go-common

A collection of production-ready, framework-agnostic Go packages for building backend services.

[![Go Reference](https://pkg.go.dev/badge/github.com/thanhbvha/go-common.svg)](https://pkg.go.dev/github.com/thanhbvha/go-common)

## Packages

| Package | Description |
|---|---|
| `logger` | Async structured logger (log/slog) with optional lumberjack file rotation |
| `redis` | Redis client wrapper (single / cluster / sentinel) with health-check |
| `queue` | Durable Redis Streams job queue — delays, retries, DLQ, reclaim |

## Requirements

- Go 1.22+
- Redis 6.x+ (Redis 6.2+ recommended for `XAUTOCLAIM` support)

---

## Installation

```bash
go get github.com/thanhbvha/go-common
```

---

## logger

Zero-dependency async logger. Fiber (or any HTTP framework) adapter lives in a separate repo — no framework lock-in.

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
ctx = context.WithValue(ctx, logger.ContextKeyRequestID, requestID)
log.InfoWithContext(ctx, "request received", "method", r.Method)
```

### Key types

| Symbol | Description |
|---|---|
| `Options` | Top-level config (level, stdout, file, workers, buffer) |
| `FileOptions` | lumberjack rotation config (optional — nil = no file) |
| `Logger` | Struct — `Info/Error/Warn/Debug` sync; `*Async` async variants |
| `SetDefault(l)` | Register as process-wide default |
| `Default()` | Retrieve default (nil if not set) |
| `Close()` | Flush & shut down default logger |

---

## redis

```go
import "github.com/thanhbvha/go-common/redis"

// Standalone
cfg := redis.DefaultConfig()
cfg.Host     = "redis.internal"
cfg.Password = os.Getenv("REDIS_PASSWORD")
cfg.Prefix   = "myapp:"
cfg.Logger   = log // any logger.Logger-compatible value

client := redis.New(cfg)
if err := client.Connect(ctx); err != nil {
    log.Fatal(err)
}
redis.SetDefault(client)
defer redis.Close()

// Cluster
cfg.Mode         = redis.ModeCluster
cfg.ClusterAddrs = []string{"node1:6379", "node2:6379", "node3:6379"}

// Sentinel
cfg.Mode           = redis.ModeSentinel
cfg.SentinelAddrs  = []string{"sentinel1:26379"}
cfg.MasterName     = "mymaster"

// Usage
client.Set(ctx, client.BuildKey("session:abc"), data, time.Hour)
val, _  := client.Get(ctx, client.BuildKey("session:abc"))
version, _ := client.ServerVersion(ctx) // "7.2.4"
```

### Key types

| Symbol | Description |
|---|---|
| `Config` | All connection, pool, timeout, retry options |
| `ConnectionMode` | `ModeSingle` \| `ModeCluster` \| `ModeSentinel` |
| `Client` | Wraps `redis.UniversalClient`, thread-safe |
| `New(cfg)` | Allocate (does not connect) |
| `MustConnect(ctx, cfg)` | Allocate + connect, panic on error |
| `SetDefault(c)` | Register process-wide default |
| `Default()` | Retrieve default (panics if unset) |

---

## queue

Backed by Redis Streams. Supports per-type worker pools, delayed jobs, automatic retry, dead-letter queue, and stuck-job reclaiming.

```go
import "github.com/thanhbvha/go-common/queue"

cfg := queue.DefaultConfig()
cfg.Logger = log

q := queue.New(rdb, cfg) // rdb satisfies queue.RedisStreamer

// Register types (before Start)
q.RegisterJobType("email", queue.JobTypeOptions{
    Concurrency: 4,
    MaxRetry:    5,
    BatchSize:   10,
})

// Register handlers (before Start)
q.RegisterHandler("email", func(job queue.Job) error {
    // job.Data holds your payload
    return sendEmail(job.Data)
})

q.Start(ctx)
defer q.Stop()

// Immediate
q.Enqueue(ctx, "email", payload)

// Delayed
q.EnqueueDelayed(ctx, "email", payload, 30*time.Second)

// Unique / deduplicated (window: 5 min)
q.EnqueueUnique(ctx, "email", "welcome-user-42", payload, 5*time.Minute)

// Options
q.Enqueue(ctx, "email", payload,
    queue.WithMaxRetry(10),
    queue.WithDelay(5),
)
```

### Architecture

```
Enqueue ──► Redis Stream (per job type)
              │
              ▼
         Worker pool ──► handler()
              │               │
         success            error
              │               │
             ACK          retry (re-enqueue)
                               │
                          max retries exceeded
                               │
                              DLQ stream
```

| Symbol | Description |
|---|---|
| `Queue` | Central struct — owns workers, reclaimer, dispatcher |
| `Config` | All tuneable parameters with `DefaultConfig()` helper |
| `JobTypeOptions` | Per-type concurrency, retry, stream key, group override |
| `Job` | Payload envelope (ID, Type, Data, Retry, Delay, …) |
| `JobHandler` | `func(Job) error` — return non-nil to trigger retry |
| `RedisStreamer` | Interface — any Redis client satisfying it works |

---

## Full example

See [`example/main.go`](example/main.go) for a wired-up binary.

```bash
REDIS_HOST=localhost go run ./example/main.go
```

---

## License

MIT
