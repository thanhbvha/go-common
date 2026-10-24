# Recovery

`recovery` converts a panic into a bounded, structured report without depending
on an HTTP framework, logger implementation, or transport. Framework adapters
for Fiber, Gin, and Echo live under `web/middleware`.

Use panic recovery at process boundaries such as HTTP handlers, RPC handlers,
queue workers, and application-created goroutines. It is a last-resort safety
boundary, not a replacement for returning and handling ordinary errors.

## Captured information

`Report` contains:

- the bounded panic message and concrete panic type;
- the original `error` as `Cause` when the panic value implements `error`;
- the origin file, line, and function;
- up to 32 structured stack frames from a bounded 32 KiB stack snapshot.

Message formatting is protected so an `Error` or `String` method that panics
cannot panic the recovery path again. Messages longer than 4 KiB are truncated.

Go does not expose arbitrary local variables from a recovered stack. Add
business context explicitly to your log record or error. Never include tokens,
passwords, personal data, or other secrets in panic values or log fields.

## Core usage

`Catch` must be deferred in the same goroutine as the code it protects:

```go
func runWorker(ctx context.Context, jobID string) {
	defer recovery.Catch(func(report recovery.Report) {
		logger.ErrorWithContext(ctx, "worker panic recovered",
			slog.String("job_id", jobID),
			slog.String("panic_type", report.Type),
			slog.String("panic_message", report.Message),
			slog.String("file", report.Origin.File),
			slog.Int("line", report.Origin.Line),
			slog.String("function", report.Origin.Function),
			slog.Any("stack", report.Stack),
		)
	})

	processJob(ctx, jobID)
}
```

Passing a nil handler deliberately re-panics instead of silently swallowing the
failure:

```go
defer recovery.Catch(nil)
```

## HTTP middleware

Install request ID and access logging outside recovery, then place recovery
before telemetry, authentication, rate limiting, and routes that it must
protect. The adapters log the server-side report synchronously and return only
a generic HTTP 500 body:

```json
{
  "code": 500,
  "status": "INTERNAL_ERROR",
  "message": "An unexpected internal error occurred",
  "request_id": "..."
}
```

The panic message, stack, source file, and line are never returned to the client.

### Fiber

```go
app := fiber.New()
app.Use(fiberlog.RequestIDMiddleware())
app.Use(fiberlog.Middleware())
app.Use(fibermw.Recover())
```

### Gin

```go
router := gin.New()
router.Use(ginlog.RequestIDMiddleware())
router.Use(ginlog.Middleware())
router.Use(ginmw.Recover())
```

### Echo

```go
server := echo.New()
server.Use(echolog.RequestIDMiddleware())
server.Use(echolog.Middleware())
server.Use(echomw.Recover())
```

## Goroutine boundary

Recovery is goroutine-local. An HTTP middleware cannot catch a panic from a
goroutine created by a handler:

```go
go func() {
	defer recovery.Catch(handleBackgroundPanic)
	runBackgroundJob()
}()
```

Install a recovery boundary at every goroutine entry point that must remain
isolated. Capture plain `context.Context` values before starting the goroutine;
do not retain Fiber, Gin, or Echo request-context objects after the request.

## Examples

- [`examples/recovery`](../examples/recovery) demonstrates HTTP recovery and a
  separate background-goroutine boundary.
- [`examples/web`](../examples/web) shows net/http, Fiber, Gin, and Echo application
  composition with their respective recovery adapters.

Run the dedicated example from the module root:

```powershell
go run ./examples/recovery/
```
