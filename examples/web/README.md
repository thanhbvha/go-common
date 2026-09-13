# Web API Module (Multi-Framework)

## Overview
The `web` module provides standardized middlewares, response formatting, and validators for building HTTP REST APIs. 
**It fully supports the big three Go frameworks: Fiber, Gin, and Echo!**

## Key Features
- `web/response`: Standardized JSON response envelopes for Fiber (`Success`), Gin (`GinSuccess`), and Echo (`EchoSuccess`).
- `web/middleware`: Pre-configured middlewares for Recover, Global ErrorHandler, and OpenTelemetry instrumentation for all three frameworks.
- `web/validator`: Global struct validator using `go-playground/validator/v10` that works universally across frameworks.
- `utils/graceful`: A robust, centralized graceful shutdown coordinator.

## Covered Examples
This module includes three complete REST API setups, demonstrating the power of framework-agnostic utilities:
1. `RunFiberAPIExample()`: Setup using `gofiber/fiber/v2`
2. `RunGinAPIExample()`: Setup using `gin-gonic/gin`
3. `RunEchoAPIExample()`: Setup using `labstack/echo/v4`

## 🚨 Best Practices for AI/Developers
- **Response Format**: Do not write custom `JSON()` calls directly. Always use the `response` package (e.g., `response.Success` for Fiber, `response.GinSuccess` for Gin, `response.EchoSuccess` for Echo) to maintain a consistent API contract across all microservices.
- **Middlewares**: Always attach the `Recover()` middleware at the very top of your app to prevent panics from crashing the whole server.
- **Error Handling**: Use the provided Global Error Handlers (`middleware.ErrorHandler`, `middleware.GinErrorHandler`, `middleware.EchoErrorHandler`) to automatically unwrap `xerrors` and format them securely for the client.
- **Graceful Shutdown (CRITICAL)**: **NEVER** block the main thread synchronously with `app.Listen()`. Always launch the server in a goroutine, and use `graceful.Register(...)` followed by `graceful.Wait()` at the bottom of `main()`. This guarantees that active HTTP requests finish before the container terminates.
