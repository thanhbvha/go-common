# WebSocket Module

## Overview
The `websocket` module is a high-performance, clustered WebSocket engine. It is designed to scale horizontally out-of-the-box by leveraging a distributed Pub/Sub backend (Redis or NATS). It avoids the common "single-node bottleneck" problem of traditional WebSocket implementations.

## Architecture
- `core.Manager`: The central brain orchestrating shards.
- `core.Shard`: Sharded connection pools to reduce lock contention on highly concurrent connections.
- `core.Connection`: Wraps the raw WebSocket connection.
- `adapter/*`: Pluggable adapters to easily attach the engine to standard web frameworks (`Fiber`, `Gin`, `Echo`).

## 🚨 Best Practices for AI/Developers
- **Clustered Mode**: To run in clustered mode, you MUST initialize the global `redis` or `nats` connection pool BEFORE starting the WebSocket server. The engine will auto-detect them and use them as the Pub/Sub bus. If neither is available, it silently falls back to a standalone (in-memory) mode.
- **Event Handlers**: Business logic should be implemented by calling `core.RegisterHandler("event_type", func(...))`. DO NOT try to parse raw bytes manually.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** call `core.GetGlobalManager().Shutdown()` during your application's graceful shutdown sequence. This cascades the shutdown signal safely through all Shards to disconnect clients cleanly, preventing memory leaks and circular dependencies.
