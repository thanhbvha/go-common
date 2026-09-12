# Web API Module (Fiber)

## Overview
The `web` module provides standardized middlewares, response formatting, and validators for building HTTP REST APIs using `gofiber/fiber/v2`.

## Key Features
- `web/response`: Standardized JSON response envelopes (`Success`, `Created`, `Error`, `ValidationError`).
- `web/middleware`: Pre-configured middlewares for Recover, ErrorHandler, Telemetry, and Logger.
- `web/validator`: Global struct validator using `go-playground/validator/v10`.
- `utils/graceful`: A robust, centralized graceful shutdown coordinator.

## 🚨 Best Practices for AI/Developers
- **Response Format**: Do not write custom `c.JSON(...)` directly. Always use the `response` package (e.g. `response.Success(c, data)`) to maintain a consistent API contract across all microservices.
- **Middlewares**: Always attach `middleware.Recover()` at the very top of the Fiber app to prevent panics from crashing the whole server.
- **Graceful Shutdown (CRITICAL)**: **NEVER** use `app.Listen(...)` synchronously on the main thread. Always launch it in a goroutine, and use `graceful.Register(app.ShutdownWithContext)` followed by `graceful.Wait()` at the bottom of `main()`. This guarantees that active HTTP requests are finished before the container terminates.
