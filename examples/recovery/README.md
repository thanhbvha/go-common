# Recovery example

This runnable example demonstrates two distinct panic boundaries:

1. `fibermw.Recover()` protects the Fiber request goroutine.
2. `recovery.Catch(...)` protects a background goroutine created by a handler.

Start it from the module root:

```powershell
go run ./examples/recovery/
```

Trigger an HTTP-handler panic:

```powershell
Invoke-RestMethod http://127.0.0.1:3000/panic
```

The client receives a generic HTTP 500 response containing a request ID. The
server log contains the synthetic panic message and its origin file, line,
function, and bounded stack.

Trigger a background-goroutine panic:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:3000/background
```

The request returns HTTP 202 while the goroutine's own recovery boundary records
the panic. Without the `defer recovery.Catch(...)` inside that goroutine, HTTP
middleware cannot catch its panic.

Stop the example with `Ctrl+C`; it uses an instance-scoped graceful-shutdown
coordinator to close the server.
