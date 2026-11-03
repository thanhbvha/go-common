# web

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

Core utilities for building REST APIs. Includes standardized responses for standard `net/http`, Fiber, Gin, and Echo, friendly validation, and framework middleware.

Standard `net/http` handlers can write the same response envelope without importing a web framework:

```go
import httpresp "github.com/thanhbvha/go-common/v2/web/response/nethttp"

func getUser(w http.ResponseWriter, r *http.Request) {
    user := User{ID: 1}
    if err := httpresp.Success(w, r, user); err != nil {
        // Log the encoding error; the response may already be committed.
    }
}
```

Standard-library middleware composes around an `http.Handler`:

```go
import (
    nethttplog "github.com/thanhbvha/go-common/v2/logger/middleware/nethttp"
    nethttpmw "github.com/thanhbvha/go-common/v2/web/middleware/nethttp"
)

mux := http.NewServeMux()
mux.Handle("/users", nethttpmw.ErrorHandler(func(w http.ResponseWriter, r *http.Request) error {
    return httpresp.Success(w, r, users)
}))

handler := nethttplog.RequestIDMiddleware(
    nethttplog.Middleware(
        nethttpmw.Recover(
            nethttpmw.Telemetry("HTTP Request")(mux),
        ),
    ),
)
```

The framework-specific adapters retain their existing APIs. For example, with Fiber:

```go
import (
    "github.com/thanhbvha/go-common/v2/logger"
    fiberlog "github.com/thanhbvha/go-common/v2/logger/middleware/fiber"
    fibermw "github.com/thanhbvha/go-common/v2/web/middleware/fiber"
    fiberresp "github.com/thanhbvha/go-common/v2/web/response/fiber"
    "github.com/thanhbvha/go-common/v2/web/validator"
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
    if errs, err := validator.Validate(&req); err != nil {
        return err
    } else if errs != nil {
        return fiberresp.ValidationError(c, errs)
    }

    return fiberresp.Success(c, req)
})
```

## Panic recovery

The standard `net/http`, Fiber, Gin, and Echo `Recover` middleware adapters use the shared
framework-independent `recovery` package. A recovered panic is logged with its
type, origin file/line/function, and structured stack; panic messages are omitted. Clients
receive only a generic `INTERNAL_ERROR` response and the current request ID.

Recovery applies only to the request goroutine. Application-created goroutines
must install their own `defer recovery.Catch(...)` boundary.

## Middleware ordering

Use this outer-to-inner order for standard `net/http`; register middleware in
the equivalent request order for Fiber, Gin, and Echo:

1. Request ID
2. Access logger
3. Recovery
4. HTTP telemetry
5. Optional pre-auth direct-peer IP rate limit
6. Authentication
7. Optional authenticated user/API-key rate limit
8. Router or application handler

The logger remains outside recovery so it observes the recovered HTTP 500. A
user-keyed limiter must run after authentication. Forwarded client IP headers
must only be used after the application configures a trusted-proxy boundary.

## v2 contracts

`response.FromError` exposes only the explicit `xerrors.CustomError.Message` for
4xx errors. Wrapped causes, unknown errors, every 5xx and invalid HTTP statuses
are masked. Error response adapters normalize invalid error statuses to 500.

HTTP telemetry uses route templates where available and a fixed operation name
otherwise. It does not export URL paths, query strings, client IPs or raw errors.
For net/http templates, use Go ServeMux request patterns; unmatched routes keep
the fixed name. Applications control additional telemetry attributes explicitly.

Validation uses `validator.Validate(value)` or an immutable instance from
`validator.New(rules)`. Both return `(violations, error)`: nil/scalar inputs and
invalid schema tags return an error instead of panic. Rules are supplied during
construction and must be concurrency-safe. Field names use JSON tags. Mutable
global `CustomRule` and the unchecked `Struct` API were removed.
