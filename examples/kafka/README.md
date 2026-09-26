# Kafka Module

## Overview
The `kafka` module provides a robust, high-performance Apache Kafka client built on top of `github.com/twmb/franz-go`. It offers simplified abstractions for producing and consuming messages, topic administration, and graceful shutdown, without relying on CGO dependencies (unlike `confluent-kafka-go`).

## Key Structs & Configs
- `kafka.Config`: Central configuration for the client.
  - `Brokers`: List of Kafka broker addresses (e.g., `["localhost:9092"]`).
  - `ClientID`: Identifier for the client.
  - `ConsumerGroup`: The consumer group ID for this instance.
  - `Logger`: Uses the `go-common/logger` interface for unified logging.
- `kafka.Client`: The primary client wrapper managing producers, consumers, and admin operations.

## 🚨 Best Practices for AI/Developers
- **Client Initialization**: Always initialize the logger (`logger.SetDefault`) **before** calling `kafka.MustConnect(ctx, config)`.
- **Graceful Shutdown (CRITICAL)**: Intercept OS signals (SIGINT, SIGTERM) and wait on the context (`<-ctx.Done()`). Always `defer client.Close()` to ensure in-flight messages are flushed and consumer group offsets are committed.
- **Consumer Behavior**: Use `Subscribe` or `SubscribeGroup` to consume messages. Be aware that the underlying `franz-go` client polls records for *all* topics subscribed via a single client. For fully isolated processing and retry semantics, prefer using the `queue_kafka` module.
- **Producing**: Use `ProduceJSON` for easy serialization or `Produce` for raw byte arrays. Both are asynchronous and thread-safe.
