# WebSocket Module

## Overview
The `websocket` module is a high-performance, clustered WebSocket engine. It is designed to scale horizontally out-of-the-box by leveraging a distributed Pub/Sub backend (Redis or NATS). It avoids the common "single-node bottleneck" problem of traditional WebSocket implementations.

## Architecture
- `core.Manager`: The central brain orchestrating shards.
- `core.Shard`: Sharded connection pools to reduce lock contention on highly concurrent connections.
- `core.Connection`: Wraps the raw WebSocket connection.
- `adapter/*`: Pluggable adapters for standard `net/http`, Fiber, Gin, and Echo.

## Covered Examples
The `main.go` file contains an extremely comprehensive example divided into two parts:
1. `SetupCore()`: Bootstraps the framework-agnostic shared infrastructure (Logger, Redis connection, and registering the `chat_message` business event handler).
2. HTTP launchers: choose `RunNetHTTPExample()`, `RunFiberExample()`, `RunGinExample()`, or `RunEchoExample()`. Run only one because they all bind to port 8080.

**Client Test Script**: A premium interactive test client is provided in the `client/` directory. You can use it to connect to the server and send chat messages interactively.
Set a demo token on both server and client, then run them with the same environment:

```powershell
$env:WS_DEMO_TOKEN="replace-with-a-random-demo-token"
go run ./examples/websocket/ -stack=nethttp
go run ./examples/websocket/client/ -interactive=true -user=AI_TESTER
```

Valid server stacks are `nethttp`, `fiber`, `gin`, and `echo`.

## 🚨 Best Practices for AI/Developers
- **Clustered Mode**: To run in clustered mode, you MUST explicitly inject the Pub/Sub backend by calling `pubsub.SetGlobalManager(redispubsub.NewManagerWithClient(redisClient))` BEFORE starting the WebSocket server. If you do not inject a manager, it silently falls back to a standalone (in-memory) mode without requiring any external dependencies.
- **Event Handlers**: Business logic should be implemented by calling `core.RegisterHandler("event_type", func(...))`. DO NOT try to parse raw bytes manually.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** call `core.GetGlobalManager().Shutdown()` during your application's graceful shutdown sequence. This cascades the shutdown signal safely through all Shards to disconnect clients cleanly, preventing memory leaks and circular dependencies.
- **Authentication**: Every adapter rejects upgrades unless an explicit `Authenticate` callback resolves a validated user identity.
- **Origin Policy**: Same-origin is the secure default. Configure an explicit origin allowlist for browser clients hosted on different origins.
- **Panic Recovery**: Every launcher installs the matching project recovery middleware for the HTTP upgrade. Connection goroutine panics are recovered separately and logged with structured stack metadata without exposing diagnostics to clients.
- **Operational Endpoints**: The runnable launchers expose only `/ws` and `/health`. Register stats or shard-management handlers separately behind administrator authentication.
