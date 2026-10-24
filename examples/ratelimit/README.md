# Rate Limit Example

This example runs the same Redis fixed-window limit on standard `net/http`,
Fiber, Gin, and Echo.

## Prerequisite

Start Redis and optionally set its address:

```powershell
$env:REDIS_ADDR="localhost:6379"
go run ./examples/ratelimit -stack=nethttp
```

Valid stacks are `nethttp`, `fiber`, `gin`, and `echo`. Each exposes
`GET http://localhost:3000/api/data` with a limit of five requests per ten
seconds.

```powershell
1..6 | ForEach-Object { Invoke-WebRequest http://localhost:3000/api/data -SkipHttpErrorCheck | Select-Object StatusCode }
```

The sixth request should return HTTP 429. Middleware currently fails open when
Redis is unavailable, but this example verifies Redis during startup so that
the behavior is visible. Decide explicitly whether fail-open is acceptable for
sensitive endpoints such as login, OTP, and password reset.

IP-based limits can group users behind NAT. For authenticated APIs, use a custom
key generator based on a verified user or API-key context, not a caller-supplied
identity header.
