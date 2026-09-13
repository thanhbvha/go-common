# Queue (Redis-based) Module

## Overview
The `queue` module provides a distributed task queue built on top of Redis. It relies on the generic worker pool and scheduler mechanism to execute background jobs securely and concurrently.

## Key Structs & Configs
- `queue.Config`: Configurations for the queue workers.
  - `Concurrency`: Number of worker goroutines (Default: 10).
  - `ShutdownTimeout`: Duration to wait for in-flight jobs to complete during `q.Stop()` before forcing exit.
- `registry`: A central package to register tasks (`registry.Register(...)`).

## 🚨 Best Practices for AI/Developers
- **Task Registration (IMPORTANT)**: In this specific `main.go` example, the task handler and `init()` function are written directly in the same file so you can see all the code at once. However, in a real project, you MUST structure your tasks in separate packages (e.g. `tasks/email.go`) using `init()` functions to register them, and then use a blank import (`_ "github.com/.../tasks"`) in `main.go` to trigger the registration.
- **Graceful Shutdown (CRITICAL)**: Always intercept OS signals (SIGINT, SIGTERM) and call `q.Stop()` in a separate goroutine to allow the workers to drain safely within the `ShutdownTimeout` window.
- **Dependency Loading**: Ensure Redis is connected and initialized (`gored.SetDefault`) **before** calling `queue.New(rdb, config)`.
