# Queue NATS Module

## Overview
The `queue_nats` module provides a distributed task queue built natively on top of **NATS JetStream**. It utilizes the same generic worker pool and scheduler mechanism as the Redis queue but leverages JetStream for persistent, at-least-once delivery guarantees.

## Key Structs & Configs
- `queue_nats.Config`: Configuration for the JetStream queue workers.
  - `Concurrency`: Number of concurrent workers per stream (Default: 10).
  - `ShutdownTimeout`: Duration to wait for in-flight jobs to complete during `q.Stop()` before forcing exit.
- `registry`: Central package for registering NATS tasks via `init()` blocks.

## 🚨 Best Practices for AI/Developers
- **Task Registration**: Tasks must be registered in their own packages using `init()`. You MUST use blank imports in `main.go` (e.g., `_ "github.com/.../tasks"`) to trigger these registrations.
- **Graceful Shutdown (CRITICAL)**: Always intercept OS signals (SIGINT, SIGTERM) and call `q.Stop()` in a separate goroutine. This allows the NATS workers to NAK (Negative Acknowledge) or complete messages safely within the `ShutdownTimeout` window.
- **Dependency Loading**: Ensure the NATS client is initialized and `nats.SetDefault(client)` is called **before** instantiating `queue_nats.New(natsClient, config)`.
