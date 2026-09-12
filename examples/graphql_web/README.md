# GraphQL Web Module

## Overview
The `graphql` module integrates `gqlgen` with our core framework, providing a high-performance GraphQL server wrapped within a Fiber adapter. It includes native support for DataLoader (N+1 query resolution) and OpenTelemetry.

## Key Features
- `graphql.NewServer()`: Creates the core GraphQL server (standard `net/http`).
- `graphql/adapter/fiber`: Wraps the core server to run seamlessly on the Fiber framework.
- `graphql.NewDataLoader()`: A generic, robust implementation of the Facebook DataLoader pattern to batch database queries.

## 🚨 Best Practices for AI/Developers
- **DataLoader Injection**: Always inject DataLoaders into the request context inside the Fiber Adapter's `ContextSetup` function using typed context keys (e.g. `ctxkey.SetDataLoader`).
- **Resolvers**: Extract DataLoaders from the context in your GraphQL Resolvers using typed keys, and call `.Load()` instead of calling the database directly.
- **Graceful Shutdown (CRITICAL)**: Always launch `app.Listen` in a goroutine and use `graceful.Wait()` at the end of `main.go`. Do NOT block the main thread with `app.Listen()`, as it will instantly kill ongoing GraphQL queries upon termination.
