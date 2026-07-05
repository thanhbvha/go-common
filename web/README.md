# web

Core utilities for building REST APIs. Includes standardized responses, friendly validation, and common Fiber middlewares.

```go
import (
    "github.com/thanhbvha/go-common/web/response"
    "github.com/thanhbvha/go-common/web/validator"
    "github.com/thanhbvha/go-common/web/middleware"
)

app := fiber.New(fiber.Config{
    // Automatically formats xerrors and unwraps panics into standard JSON responses
    ErrorHandler: middleware.ErrorHandler,
})

// Middlewares
app.Use(middleware.Recover())
app.Use(middleware.RequestID())
app.Use(middleware.Telemetry("HTTP Request")) // Creates a span for each request

app.Post("/users", func(c *fiber.Ctx) error {
    var req UserReq
    c.BodyParser(&req)

    // Validate with friendly Vietnamese errors
    if errs := validator.Struct(&req); errs != nil {
        return response.ValidationError(c, errs)
    }

    return response.Success(c, req)
})
```
