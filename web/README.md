# web

Core utilities for building REST APIs. Includes standardized responses, friendly validation, and common Fiber middlewares.

```go
import (
    "github.com/thanhbvha/go-common/logger"
    fiberlog "github.com/thanhbvha/go-common/logger/middleware/fiber"
    fibermw "github.com/thanhbvha/go-common/web/middleware/fiber"
    fiberresp "github.com/thanhbvha/go-common/web/response/fiber"
    "github.com/thanhbvha/go-common/web/validator"
)

app := fiber.New(fiber.Config{
    // Automatically formats xerrors and unwraps panics into standard JSON responses
    ErrorHandler: fibermw.ErrorHandler,
})

// Middlewares
app.Use(fibermw.Recover())
app.Use(fiberlog.RequestIDMiddleware())
app.Use(fiberlog.Middleware())
app.Use(fibermw.Telemetry("HTTP Request")) // Creates a span for each request

app.Post("/users", func(c *fiber.Ctx) error {
    var req UserReq
    c.BodyParser(&req)

    // Validate with friendly Vietnamese errors
    if errs := validator.Struct(&req); errs != nil {
        return fiberresp.ValidationError(c, errs)
    }

    return fiberresp.Success(c, req)
})
```

## Panic recovery

The Fiber, Gin, and Echo `Recover` middleware adapters use the shared
framework-independent `recovery` package. A recovered panic is logged with its
bounded message, type, origin file/line/function, and structured stack. Clients
receive only a generic `INTERNAL_ERROR` response and the current request ID.

Recovery applies only to the request goroutine. Application-created goroutines
must install their own `defer recovery.Catch(...)` boundary.
