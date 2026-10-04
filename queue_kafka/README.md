# queue_kafka

A durable, Apache Kafka-backed job queue for `go-common`. Provides feature
parity with the sibling `queue` (Redis Streams) and `queue_nats` (NATS
JetStream) packages, with Kafka-specific additions such as partition key
routing and ordered processing.

## Features

- **Per-type worker pools** — configurable concurrency per job type
- **Automatic retry** — failed jobs are re-routed to a dedicated retry topic
- **Dead-letter queue (DLQ)** — exhausted jobs land in a configurable DLQ topic
- **Delayed execution** — `EnqueueDelayed` / `WithDelay` for future scheduling
- **Deduplication** — `EnqueueUnique` leverages Kafka message keys + idempotent producer
- **Partition routing** — `EnqueueWithKey` / `WithPartitionKey` for ordered processing
- **Topic auto-provisioning** — topics are created automatically on `Start`
- **Graceful shutdown** — in-flight jobs complete before the process exits

## Quick start

```go
// 1. Connect Kafka
kc := kafka.MustConnect(ctx, kafka.DefaultConfig())

// 2. Create queue
q := queue_kafka.New(kc, queue_kafka.DefaultConfig())

// 3. Register tasks via registry
registry.Register("send-email", queue_kafka.JobTypeOptions{
    Concurrency: 4,
    MaxRetry:    5,
}, func(job queue_kafka.Job) error {
    // process job.Data
    return nil
})

// Apply registered tasks to the queue
registry.ApplyToQueue(q)

// 4. Start (creates topics, launches goroutines)
q.Start(ctx)
defer q.Stop()

// 5. Enqueue
q.Enqueue(ctx, "send-email", EmailPayload{To: "alice@example.com"})
```

## Enqueue variants

| Method | Description |
|--------|-------------|
| `Enqueue` | Immediate processing |
| `EnqueueDelayed` | Deliver after a duration |
| `EnqueueUnique` | Drop duplicates by key |
| `EnqueueWithKey` | Explicit Kafka partition key for ordered processing |

## Retry and DLQ flow

```
job fails → retry < MaxRetry → published to <prefix>.<type>.retry topic
job fails → retry >= MaxRetry → published to queue.dlq topic
```

Both topics are auto-created on `Start` with the configured retention.

## Configuration reference

| Field | Default | Description |
|-------|---------|-------------|
| `TopicPrefix` | `"queue"` | Prefix for all auto-generated topic names |
| `DefaultGroup` | `"workers"` | Consumer group prefix |
| `DefaultPartitions` | `4` | Partitions for auto-created topics |
| `DefaultReplicationFactor` | `1` | Replication factor (set ≥ 3 for production) |
| `DefaultRetentionMs` | `15 days` | Message retention for job topics |
| `DefaultMaxRetry` | `3` | Retries before DLQ |
| `DefaultWorkerCount` | `5` | Workers when no types are registered |
| `DelayedTopic` | `"queue.delayed"` | Topic for delayed jobs |
| `DLQTopic` | `"queue.dlq"` | Dead-letter queue topic |
| `DLQRetentionMs` | `30 days` | DLQ message retention |
| `ProcessTimeout` | `5m` | Per-job handler timeout |
| `ShutdownTimeout` | `10s` | Graceful shutdown wait |

## Topic naming convention

| Job type | Main topic | Retry topic |
|----------|-----------|-------------|
| `send-email` | `queue.send-email` | `queue.send-email.retry` |
| `push-notify` | `queue.push-notify` | `queue.push-notify.retry` |
| *(default)* | `queue.default` | — |
| *(delayed)* | `queue.delayed` | — |
| *(DLQ)* | `queue.dlq` | — |
