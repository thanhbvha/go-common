# Foundation Module

## Overview
The `foundation` module acts as the central lifecycle orchestrator for microservices built with `go-common`. It simplifies building production-ready applications by providing a cohesive wrapper for background routines, health checks, and graceful shutdowns, ensuring the service starts reliably and terminates safely without data loss or dropped requests.

## Key Features
- `foundation.NewApp(config)`: Initializes the application container with timeout configurations.
- **Lifecycle Hooks**: Use `app.OnStart(func)` to register non-blocking servers or background workers. Use `app.OnShutdown(func)` to register cleanup routines (e.g., stopping HTTP servers or closing DB connections). Shutdown hooks execute in LIFO (Last-In-First-Out) order upon receiving interrupt signals.
- **Health Checking**: Automatically integrates with the `health` module. Provides methods like `app.Health().AddReadinessCheck(...)` to easily expose Kubernetes-compatible `/health`, `/ready`, and `/live` endpoints.
- **Execution Orchestration**: `app.Run()` coordinates everything: it triggers `OnStart` hooks, blocks the main thread waiting for an OS signal (`SIGINT`/`SIGTERM`), and sequentially executes `OnShutdown` hooks when the process is killed.

## 🚨 Best Practices for AI/Developers
- **Never Block OnStart**: When registering a server inside `app.OnStart(func)`, **NEVER** run blocking methods (like `srv.Listen()`) directly in the main body of the function. ALWAYS wrap blocking server initializations in a goroutine (`go func() { ... }()`).
- **LIFO Shutdown Strategy**: Register `OnShutdown` hooks immediately after successfully starting a resource in `OnStart`. Because they execute in Last-In-First-Out order, resources are cleaned up safely (e.g., stop the Web Router first, then safely close the Database connection).
- **Graceful Termination**: Avoid using `os.Exit()` or `log.Fatal()` to shut down the server. Always let `app.Run()` catch the termination signal and handle the graceful shutdown naturally.
- **Health Integration**: Expose your health checker endpoints directly from your `app.Health()` instance (e.g., `fiberhealth.Handler(app.Health())`) instead of creating standalone, hardcoded ping endpoints.
