# kafka

A production-ready Apache Kafka client for `go-common`, built on top of
[`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) (pure-Go, CGO-free).

## Features

- **Full security support** — TLS/mTLS, SASL PLAIN, SCRAM-SHA-256/512, OAuthBearer
- **Idempotent producer** — exactly-once write guarantees enabled by default
- **Flexible producer** — sync (`Produce`), batch (`ProduceBatch`), async (`ProduceAsync`), JSON helper (`ProduceJSON`)
- **Consumer groups** — auto offset management, seek to offset/timestamp, pause/resume
- **Topic admin** — create, delete, describe, list topics; manage retention
- **Global default** — `SetDefault` / `Default` pattern for zero-argument use across packages
- **Structured logging** — pluggable `Logger` interface (compatible with `go-common/logger`)
- **Graceful shutdown** — `Flush` before `Close` to avoid data loss

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

## Configuration reference

| Field | Default | Description |
|-------|---------|-------------|
| `Brokers` | `["localhost:9092"]` | Broker addresses |
| `ClientID` | `"go-common-kafka"` | Client identifier |
| `SecurityProtocol` | `Plaintext` | `Plaintext` / `SSL` / `SASLPlaintext` / `SASLSSL` |
| `SASLMechanism` | `SASLNone` | `SASLPlain` / `SASLSCRAMSHA256` / `SASLSCRAMSHA512` / `SASLOAuthBearer` |
| `ProducerAcks` | `-1` | `-1`=all replicas, `1`=leader, `0`=none |
| `ProducerIdempotent` | `true` | Exactly-once produce semantics |
| `ProducerCompression` | `"snappy"` | `none` / `gzip` / `snappy` / `lz4` / `zstd` |
| `ConsumerGroup` | `""` | Consumer group ID (required for Subscribe) |
| `ConsumerAutoOffsetReset` | `"earliest"` | `"earliest"` / `"latest"` |
| `ConsumerCommitInterval` | `1s` | Auto-commit interval; `0` = manual commit |
| `MaxConnRetries` | `3` | Connection attempts before error |

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
