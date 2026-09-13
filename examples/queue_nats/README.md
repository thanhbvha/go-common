# Queue NATS Module

## Overview
The `queue_nats` module provides a distributed task queue built natively on top of **NATS JetStream**. It utilizes the same generic worker pool and scheduler mechanism as the Redis queue but leverages JetStream for persistent, at-least-once delivery guarantees.

## Key Structs & Configs
- `queue_nats.Config`: Configuration for the JetStream queue workers.
  - `Concurrency`: Number of concurrent workers per stream (Default: 10).
  - `ShutdownTimeout`: Duration to wait for in-flight jobs to complete during `q.Stop()` before forcing exit.
- `registry`: Central package for registering NATS tasks via `init()` blocks.

## 🚨 Best Practices for AI/Developers
- **Task Registration (IMPORTANT)**: In this specific `main.go` example, the task handler and `init()` function are written directly in the same file so you can see all the code at once. However, in a real project, you MUST structure your tasks in separate packages (e.g. `tasks/email.go`) using `init()` functions to register them, and then use a blank import (`_ "github.com/.../tasks"`) in `main.go` to trigger the registration.
- **Graceful Shutdown (CRITICAL)**: Always intercept OS signals (SIGINT, SIGTERM) and call `q.Stop()` in a separate goroutine. This allows the NATS workers to NAK (Negative Acknowledge) or complete messages safely within the `ShutdownTimeout` window.
- **Dependency Loading**: Ensure the NATS client is initialized and `nats.SetDefault(client)` is called **before** instantiating `queue_nats.New(natsClient, config)`.
