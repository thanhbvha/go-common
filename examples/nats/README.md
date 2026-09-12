# NATS & JetStream Module

## Overview
The `nats` module provides a battle-tested wrapper around `nats.go`. It supports standard Core NATS (Pub/Sub) and advanced JetStream features including Consumer Groups, Key-Value Stores, and structured JSON messaging.

## Directory Structure
- `01_pubsub`: Basic Publish and Subscribe (Core NATS).
- `02_jetstream`: Persistent messaging (JetStream).
- `03_consumer_group`: Load balancing messages across multiple workers.
- `04_kv_store`: Distributed Key-Value store using JetStream.
- `05_json_helpers`: Utilities for automatically marshaling/unmarshaling JSON payloads.

## 🚨 Best Practices for AI/Developers
- **Initialization**: Initialize using `nats.New(cfg)` and then `err := client.Connect(ctx)`. Set it globally using `nats.SetDefault(client)`.
- **JetStream context**: To use JetStream features, obtain the context via `js := client.JetStream()`. Do not re-initialize JetStream manually.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** defer `client.Close()` in `main.go`. This safely drains active subscriptions and flushes pending outgoing messages to the server before the application exits.
