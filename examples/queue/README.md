# Queue (Redis-based) Module

## Overview
The `queue` module provides a distributed task queue built on top of Redis. It relies on the generic worker pool and scheduler mechanism to execute background jobs securely and concurrently.

## Key Structs & Configs
- `queue.Config`: Configurations for the queue workers.
  - `Concurrency`: Number of worker goroutines (Default: 10).
  - `ShutdownTimeout`: Duration to wait for in-flight jobs to complete during `q.Stop()` before forcing exit.
- `registry`: A central package to register tasks (`registry.Register(...)`) via `init()` blocks.

## 🚨 Best Practices for AI/Developers
- **Task Registration**: Tasks should be registered in their own packages using `init()` to keep the codebase modular. You MUST use blank imports in `main.go` (e.g., `_ "github.com/.../tasks"`) to trigger the `init()` functions.
- **Graceful Shutdown (CRITICAL)**: Always intercept OS signals (SIGINT, SIGTERM) and call `q.Stop()` in a separate goroutine to allow the workers to drain safely within the `ShutdownTimeout` window.
- **Dependency Loading**: Ensure Redis is connected and initialized (`gored.SetDefault`) **before** calling `queue.New(rdb, config)`.
