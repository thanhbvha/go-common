# MongoDB Module

## Overview
The `db/mongodb` module provides a thread-safe, robust wrapper around the official `mongo-driver/v2/mongo`. It handles connection pooling, multiple database instances, and OpenTelemetry instrumentation.

## Key Structs & Configs
- `mongodb.Config`: Contains fields like `URI`, `DBName`, `PingTimeout`, `EnableTelemetry`.
- `mongodb.Manager`: A singleton manager holding all MongoDB database instances.

## Generic Repository Operations
The `mongodb.Repository[T]` simplifies standard queries using BSON (`bson.M`):
- `Insert(ctx, &entity)`: Inserts a single document.
- `Paginate(ctx, filter bson.M, req mongodb.PageRequest)`: Returns a paginated `mongodb.PageResponse[T]` based on a `mongodb.PageRequest` (Page, Size) and a BSON filter.
- `Exists(ctx, filter bson.M)`: Quickly checks if any document matches the BSON filter.
- `FindOne(ctx, filter bson.M, &result)`: Fetches a single document.

## 🚨 Best Practices for AI/Developers
- **Initialization**: Always initialize using `mongodb.Init(ctx, configs, "default_db_name")`. The `Init` function is thread-safe and can merge configs if called multiple times, preserving previously added connections.
- **Dynamic Connections**: Use `mongodb.AddConnection(ctx, name, config, setAsDefault)` to add database connections at runtime.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** ensure `defer mongodb.DisconnectAll(ctx)` is called in `main.go` after `mongodb.Init`. This flushes the driver's background sockets and prevents connection leaks.
- **Retrieving Connections**: Use `mongodb.Get("name")` for a specific DB, or `mongodb.Get()` for the default one.
- **Using BSON (CRITICAL)**: Always use `bson.M{"field": "value"}` for filters. Make sure struct fields map to BSON correctly using `` `bson:"field_name"` `` tags.
