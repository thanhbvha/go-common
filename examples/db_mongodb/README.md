# MongoDB Module

## Overview
The `db/mongodb` module provides a thread-safe, robust wrapper around the official `mongo-driver/v2/mongo`. It handles connection pooling, multiple database instances, and OpenTelemetry instrumentation.

## Key Structs & Configs
- `mongodb.Config`: Contains fields like `URI`, `DBName`, `PingTimeout`, `EnableTelemetry`.
- `mongodb.Manager`: A singleton manager holding all MongoDB database instances.

## 🚨 Best Practices for AI/Developers
- **Initialization**: Always initialize using `mongodb.Init(ctx, configs, "default_db_name")`. The `Init` function is thread-safe and can merge configs if called multiple times, preserving previously added connections.
- **Dynamic Connections**: Use `mongodb.AddConnection(ctx, name, config, setAsDefault)` to add database connections at runtime.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** ensure `defer mongodb.DisconnectAll(ctx)` is called in `main.go` after `mongodb.Init`. This flushes the driver's background sockets and prevents connection leaks.
- **Retrieving Connections**: Use `mongodb.Get("name")` for a specific DB, or `mongodb.Get()` for the default one.
- **Generic Repository**: Use `mongodb.NewRepository[T]()` to quickly set up CRUD and Pagination operations.
