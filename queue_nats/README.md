# queue_nats

A robust, durable job queue backed by **NATS JetStream**. This module provides feature parity with the Redis-backed `queue` module, including per-type worker pools, delayed jobs, automatic retries, dead-letter queue (DLQ) routing, and unique job deduplication.

By leveraging NATS JetStream, `queue_nats` automatically handles message retention, durable consumer tracking, and redelivery mechanisms (`AckWait`) natively on the server side, eliminating the need for client-side reclaim loops.

### Quick Start

```go
import "github.com/thanhbvha/go-common/queue_nats"
import "github.com/thanhbvha/go-common/nats"

// 1. Initialize NATS Streamer
natsClient := nats.MustConnect(ctx, nats.DefaultConfig())

// 2. Initialize Queue
cfg := queue_nats.DefaultConfig()
cfg.Logger = log

q := queue_nats.New(natsClient, cfg)

// 3. Register Job Types (Defines Concurrency, Stream names, etc.)
q.RegisterJobType("email", queue_nats.JobTypeOptions{
    Concurrency: 4,
    MaxRetry:    5,
    BatchSize:   10,
})

// 4. Register Handlers
q.RegisterHandler("email", func(job queue_nats.Job) error {
    // Process payload
    return sendEmail(job.Data) 
    // Return non-nil error to trigger automatic retry (up to MaxRetry times)
})

// 5. Start processing (Creates streams and consumers in JetStream)
q.Start(ctx)
defer q.Stop()

// 6. Enqueue Jobs

// Immediate execution
q.Enqueue(ctx, "email", payload)

// Delayed execution (runs after 30 seconds)
q.EnqueueDelayed(ctx, "email", payload, 30*time.Second)

// Unique / deduplicated job (window: 5 minutes via NATS KV Store)
q.EnqueueUnique(ctx, "email", "welcome-user-42", payload, 5*time.Minute)

// With Custom Options
q.Enqueue(ctx, "email", payload,
    queue_nats.WithMaxRetry(10),
    queue_nats.WithDelay(5),
)
```

### Architecture

```
Enqueue ──► NATS JetStream (subject: queue.<job_type>)
              │
              ▼
    Durable Consumer (Push/Pull)
              │
         Worker pool ──► handler()
              │               │
         success            error
              │               │
             ACK          NAK / Timeout
                          (JetStream auto-redelivers)
                               │
                          max retries exceeded
                               │
                          DLQ stream (Term)
```

### Key Differences vs Redis Queue

1. **No Reclaim Loop:** NATS JetStream inherently tracks unacknowledged messages. If a worker crashes before `Ack()`, JetStream automatically redelivers the message after `AckWait` expires.
2. **Deduplication:** The `EnqueueUnique` function utilizes the NATS **KV Store** (`KVCreate2` for optimistic locking) instead of Redis SETNX.
3. **Delayed Jobs:** Delayed jobs are pushed to a central `queue_delayed` stream. A single background dispatcher sleeps and pushes them to the target subject once their `RunAt` time matures.

### Key Types

| Symbol | Description |
|---|---|
| `Queue` | Central manager — owns NATS Streamer, dispatchers, and worker routines |
| `Config` | Tuneable parameters (AckWait, DLQ details) with `DefaultConfig()` |
| `JobTypeOptions` | Per-type concurrency, retry limit, stream configuration |
| `Job` | Payload envelope (ID, Type, Data, Retry, Delay, MaxAge) |
| `JobHandler` | `func(Job) error` — business logic processor |
| `NATSStreamer` | Interface wrapping `go-common/nats` capabilities |
