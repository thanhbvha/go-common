# Web API Example

The example exposes the same validated `POST /users` endpoint on standard
`net/http`, Fiber, Gin, and Echo.

```powershell
go run ./examples/web -stack=nethttp
```

Valid stacks are `nethttp`, `fiber`, `gin`, and `echo`; each listens on port
3000 and uses the same response envelope.

```powershell
Invoke-RestMethod -Method Post http://localhost:3000/users -ContentType application/json -Body '{"name":"Alice Smith","email":"alice@example.com"}'
```

Middleware order is request ID, access logger, recovery, telemetry, then the
application handler. The response contains a non-secret demonstration
`reference_id`; it does not generate or return credentials.

All launchers use the struct-based graceful shutdown coordinator. Press Ctrl+C
to stop the selected server and telemetry provider cleanly.
