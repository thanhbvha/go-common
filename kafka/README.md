# kafka

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

An Apache Kafka client toolkit for `go-common`, built on top of
[`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) (pure-Go, CGO-free).

## Features

- **Full security support** — TLS/mTLS, SASL PLAIN, SCRAM-SHA-256/512, OAuthBearer
- **Idempotent producer** — broker producer retry deduplication enabled by default; application handlers still require idempotency
- **Flexible producer** — sync (`Produce`), batch (`ProduceBatch`), async (`ProduceAsync`), JSON helper (`ProduceJSON`)
- **Consumer groups** — independent bounded subscriptions, synchronous commits, stop on first error
- **Topic admin** — create, delete, describe, list topics; manage retention
- **Global default** — `SetDefault` / `Default` pattern for zero-argument use across packages
- **Structured logging** — pluggable `Logger` interface (compatible with `go-common/logger`)
- **Graceful shutdown** — `Flush` before `Close` to avoid data loss

- **Native client factory** — role-specific clients inherit one immutable
  broker, TLS/SASL, client-ID, timeout, retry, and batching configuration

## Quick start

```go
cfg := kafka.DefaultConfig()
cfg.Brokers = []string{"kafka:9092"}
cfg.ConsumerGroup = "my-service"

client := kafka.MustConnect(ctx, cfg)
kafka.SetDefault(client)
defer kafka.Close()

// Produce
result, err := client.ProduceJSON(ctx, "user-events", userID, event)

// Consume
err = client.Subscribe(ctx, []string{"user-events"}, func(ctx context.Context, r kafka.ConsumeRecord) error {
    // process r.Value
    return nil
})
```

## Security (SASL + TLS)

```go
cfg := kafka.DefaultConfig()
cfg.Brokers             = []string{"kafka:9093"}
cfg.SecurityProtocol    = kafka.SASLSSL          // TLS + SASL
cfg.SASLMechanism       = kafka.SASLSCRAMSHA256  // recommended
cfg.SASLUsername        = os.Getenv("KAFKA_USER")
cfg.SASLPassword        = os.Getenv("KAFKA_PASSWORD")
```

`SASLOAuthBearer` currently supports a static `SASLToken`. A configured token
URL without a static token is rejected; automatic token-endpoint exchange is
not implemented.

Use `NewNativeClientFactory` when a framework integration needs separate
producer and consumer instances without duplicating security options:

```go
factory, err := kafka.NewNativeClientFactory(cfg)
producer, err := factory.NewProducer()
consumer, err := factory.NewConsumer(
    kgo.ConsumerGroup("workers"),
    kgo.ConsumeTopics("jobs"),
)
```

Integrations that coordinate offsets themselves should use
`NewManualConsumer`; it preserves the same connection, security, session, and
fetch settings while disabling auto-commit. The caller must commit with
franz-go's synchronous commit APIs only after processing succeeds.

## Configuration reference

| Field | Default | Description |
|-------|---------|-------------|
| `Brokers` | `["localhost:9092"]` | Broker addresses |
| `ClientID` | `"go-common-kafka"` | Client identifier |
| `SecurityProtocol` | `Plaintext` | `Plaintext` / `SSL` / `SASLPlaintext` / `SASLSSL` |
| `SASLMechanism` | `SASLNone` | `SASLPlain` / `SASLSCRAMSHA256` / `SASLSCRAMSHA512` / `SASLOAuthBearer` |
| `ProducerAcks` | `-1` | `-1`=all replicas, `1`=leader, `0`=none |
| `ProducerIdempotent` | `true` | Idempotent produce; requires `ProducerAcks=-1`; `false` disables it |
| `ProducerCompression` | `"snappy"` | `none` / `gzip` / `snappy` / `lz4` / `zstd` |
| `ConsumerGroup` | `""` | Consumer group ID (required for Subscribe) |
| `ConsumerAutoOffsetReset` | `"earliest"` | `"earliest"` / `"latest"` |
| `MaxConnRetries` | `3` | Connection attempts before error |

The EventBus Kafka adapter always uses manual, synchronous commits for main and
delay consumers independently of the wrapper subscriptions. Its delayed scheduler
uses three durable tier topics and a bounded per-sweep batch.

## Admin operations

```go
// Create topic
err = client.CreateTopic(ctx, "my-topic", 4, 1, map[string]string{
    "retention.ms": "604800000", // 7 days
})

// Ensure topic exists (idempotent)
err = client.EnsureTopic(ctx, "my-topic", 4, 1, nil)

// Describe topic
info, err := client.DescribeTopic(ctx, "my-topic")

// List topics
topics, err := client.ListTopics(ctx)

// Get consumer group offsets
offsets, err := client.ConsumerGroupOffsets(ctx, "my-group", "my-topic")
```

## v2 consumer and lifecycle contract

Each `Subscribe` or `SubscribeBatch` call owns a separate native consumer with
manual commits and rebalance fencing. `Subscribe` stops at the first handler
error and synchronously commits only the successful prefix of its bounded poll.
`SubscribeBatch` commits only successful whole batches. Poll or commit errors
stop intake. Cancellation returns `ctx.Err()`. Restart the subscription to resume
from committed offsets; duplicate processing after crashes or ambiguous commits
is expected, so handlers must be idempotent and honor context cancellation.

`Client` itself owns the producer/admin connection. Transient health failures do
not replace this connection or block native reconnect behavior. `Close` is
terminal and cancels its active subscription contexts; use a new client for a new
lifecycle. The caller must join its subscription goroutines before closing other
handler dependencies. Call `Flush(ctx)` before `Close` when confirmed publication completion
is required. A canceled `ProduceBatch` returns a stable snapshot; zero entries
mean the result was not observed, and broker acceptance can remain ambiguous.

v2 removes `ConsumerCommitInterval`, `CommitOffsets`, `SeekToOffset`,
`SeekToTime`, `PauseTopics`, and `ResumeTopics` from the convenience wrapper.
Use `NativeClientFactory.NewManualConsumer` for explicit franz-go offset and
assignment control. Both factory consumer constructors default to manual commits.
Use `eventbus` for automatic retries, DLQ, scheduling, and bounded parallel
processing. The EventBus Kafka driver has its own offset coordinator and does
not use these convenience subscription loops.
