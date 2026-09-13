# NATS & JetStream Module

## Overview
The `nats` module provides a battle-tested wrapper around `nats.go`. It supports standard Core NATS (Pub/Sub) and advanced JetStream features including Consumer Groups, Key-Value Stores, and structured JSON messaging.

## Covered Features
This example file contains 5 independent functions demonstrating different NATS features:
- `RunPubSubExample()`: Basic Publish and Subscribe (Core NATS).
- `RunJetStreamExample()`: Persistent messaging (JetStream).
- `RunConsumerGroupExample()`: Load balancing messages across multiple workers.
- `RunKVStoreExample()`: Distributed Key-Value store using JetStream.
- `RunJSONHelpersExample()`: Utilities for automatically marshaling/unmarshaling JSON payloads.

## 🚨 Best Practices for AI/Developers
- **Initialization**: Initialize using `cfg := nats.DefaultConfig()` and `client := nats.MustConnect(ctx, cfg)`.
- **JetStream context**: All JetStream functions (like `AddStream`, `JSPublish`, `PullSubscribe`) are natively attached to the `nats.Client` wrapper. You do not need to call `.JetStream()` manually.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** defer `client.Close()` in `main.go`. This safely drains active subscriptions and flushes pending outgoing messages to the server before the application exits.
