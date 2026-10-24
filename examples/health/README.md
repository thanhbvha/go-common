# Health Example

The same `/health`, `/ready`, and `/live` checks run on standard `net/http`,
Fiber, Gin, and Echo.

```powershell
go run ./examples/health -stack=nethttp
```

Valid stacks are `nethttp`, `fiber`, `gin`, and `echo`; each listens on port
3000. The example intentionally simulates a failed Redis readiness check:

- `GET /live`: HTTP 200
- `GET /ready`: HTTP 503
- `GET /health`: HTTP 503

```powershell
Invoke-WebRequest http://localhost:3000/live
Invoke-WebRequest http://localhost:3000/ready -SkipHttpErrorCheck
```

Checks honor request cancellation and per-check timeout. Raw dependency errors
are hidden by default. Only enable `WithErrorDetails(true)` on a protected
diagnostic endpoint, never on public Kubernetes probes.
