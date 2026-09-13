# GraphQL Web Module

## Overview
The `graphql` module integrates `gqlgen` with our core framework, providing a high-performance GraphQL server. Instead of forcing you into a specific framework, it includes native Adapters for **Fiber**, **Gin**, and **Echo**, complete with OpenTelemetry and DataLoader (N+1 query resolution) support out-of-the-box.

## Key Features
- `graphql.NewServer()`: Creates the core GraphQL server (standard `net/http.Handler`).
- `graphql/adapter/*`: Contains wrappers (`fiber`, `gin`, `echo`) to run the core server seamlessly on your preferred web framework.
- `graphql.NewDataLoader()`: A generic, robust implementation of the Facebook DataLoader pattern to batch database queries.

## 🚨 Best Practices for AI/Developers
- **Framework Agnostic Core**: Always initialize the core server using `graphql.NewServer(es, cfg)` first.
- **Adapter Context Injection (CRITICAL)**: When wrapping the server with an adapter (e.g. `fiber_adapter.NewHandler`), you **MUST** use the `ContextSetup` function to inject DataLoaders into the `context.Context` using typed context keys (e.g., `ctxkey.SetDataLoader`). This is because Fiber/Gin/Echo have their own context wrappers (`fiber.Ctx`, `gin.Context`), but GraphQL resolvers only understand standard `context.Context`.
- **Resolvers**: Extract DataLoaders from the context in your GraphQL Resolvers using typed keys, and call `.Load()` instead of calling the database directly.
- **Graceful Shutdown (CRITICAL)**: Always launch your framework's server listen method (e.g., `app.Listen`, `srv.ListenAndServe`) in a goroutine and use `graceful.Wait()` at the end of `main.go`. Do NOT block the main thread with listen, as it will instantly kill ongoing GraphQL queries upon termination.
