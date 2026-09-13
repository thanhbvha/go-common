# WebSocket Module

## Overview
The `websocket` module is a high-performance, clustered WebSocket engine. It is designed to scale horizontally out-of-the-box by leveraging a distributed Pub/Sub backend (Redis or NATS). It avoids the common "single-node bottleneck" problem of traditional WebSocket implementations.

## Architecture
- `core.Manager`: The central brain orchestrating shards.
- `core.Shard`: Sharded connection pools to reduce lock contention on highly concurrent connections.
- `core.Connection`: Wraps the raw WebSocket connection.
- `adapter/*`: Pluggable adapters to easily attach the engine to standard web frameworks (`Fiber`, `Gin`, `Echo`).

## Covered Examples
The `main.go` file contains an extremely comprehensive example divided into two parts:
1. `SetupCore()`: Bootstraps the framework-agnostic shared infrastructure (Logger, Redis connection, and registering the `chat_message` business event handler).
2. Framework specific launchers: You can choose to start the server using either `RunFiberExample()`, `RunGinExample()`, or `RunEchoExample()`. (Uncomment only one at a time since they all bind to port 8080).

**Client Test Script**: A premium interactive test client is provided in the `client/` directory. You can use it to connect to the server and send chat messages interactively.
Run it via: `go run client/main.go -interactive=true -user=AI_TESTER`

## 🚨 Best Practices for AI/Developers
- **Clustered Mode**: To run in clustered mode, you MUST initialize the global `redis` or `nats` connection pool BEFORE starting the WebSocket server. The engine will auto-detect them and use them as the Pub/Sub bus. If neither is available, it silently falls back to a standalone (in-memory) mode.
- **Event Handlers**: Business logic should be implemented by calling `core.RegisterHandler("event_type", func(...))`. DO NOT try to parse raw bytes manually.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** call `core.GetGlobalManager().Shutdown()` during your application's graceful shutdown sequence. This cascades the shutdown signal safely through all Shards to disconnect clients cleanly, preventing memory leaks and circular dependencies.
