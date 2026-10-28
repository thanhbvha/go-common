# Health Module

The `health` module provides standardized, Kubernetes-ready HTTP health check endpoints (`/live`, `/ready`, `/health`) for microservices. It supports concurrent health checks with timeouts and provides ready-to-use adapters for standard **net/http**, **Fiber**, **Gin**, and **Echo** servers.

## Features

- **Kubernetes Native**: Fully implements standard K8s probes (`liveness` and `readiness`).
- **Concurrent Execution**: Runs all registered dependency checks concurrently using goroutines.
- **Configurable Timeouts**: Prevents your health endpoint from hanging indefinitely if a dependency is stuck.
- **Panic Isolation**: A panicking dependency checker is reported as down without crashing the service or exposing the panic value.
- **Built-in Checkers**: Pre-built checkers for `GORM`, `Redis`, and standard `HTTP` endpoints.
- **Framework Agnostic**: Handlers are isolated in subpackages (`health/handler/nethttp`, `health/handler/fiber`, `health/handler/gin`, `health/handler/echo`) so you only import what you need.

---

## Best Practices

- **Liveness vs Readiness**: 
  - **Liveness** (`/live`): Checks if the application process is running and not deadlocked. Should be extremely fast (e.g., memory checks). **If this fails, Kubernetes restarts the pod.**
  - **Readiness** (`/ready`): Checks if the application is ready to accept HTTP traffic (e.g., verifying DB, Redis, or upstream API connections). **If this fails, Kubernetes stops sending traffic to the pod (but does NOT restart it).**
- Always set a timeout on the Checker (default is 5s) to ensure K8s probes don't time out. The timeout bounds the probe response even when a faulty checker ignores context cancellation.

---

## Quick Start

### 1. Create the Checker

Initialize the checker and configure version and timeouts.

```go
import "github.com/thanhbvha/go-common/health"

h := health.New().
    WithVersion("v1.2.0").
    WithTimeout(2 * time.Second)
```

### 2. Register Checks

Add lightweight checks to liveness and dependency checks to readiness.

```go
// Liveness: Is the process alive?
h.AddLivenessCheck("memory", health.CustomChecker(func(ctx context.Context) error {
    // Return nil if healthy
    return nil
}))

// Readiness: Are dependencies ready?
// Example: GORM Database
h.AddReadinessCheck("database", health.GORMChecker(db)) // db is *gorm.DB

// Example: Redis
h.AddReadinessCheck("redis", health.RedisChecker(rdb)) // rdb is redis.UniversalClient

// Example: Custom Dependency (e.g. MongoDB or External API)
h.AddReadinessCheck("external-api", health.CustomChecker(func(ctx context.Context) error {
    // Perform custom ping
    return nil
}))
```

### 3. Register HTTP Handlers

Attach the health endpoints to your favorite framework.

**For standard `net/http`:**
```go
import "github.com/thanhbvha/go-common/health/handler/nethttp"

mux := http.NewServeMux()
mux.Handle("/health", nethttp.Handler(h))
mux.Handle("/ready", nethttp.ReadyHandler(h))
mux.Handle("/live", nethttp.LiveHandler(h))
```

**For Fiber:**
```go
import "github.com/thanhbvha/go-common/health/handler/fiber"

app.Get("/health", fiberhealth.Handler(h))       // All checks
app.Get("/ready", fiberhealth.ReadyHandler(h))   // Readiness only
app.Get("/live", fiberhealth.LiveHandler(h))     // Liveness only
```

**For Gin:**
```go
import "github.com/thanhbvha/go-common/health/handler/gin"

router.GET("/health", ginhealth.Handler(h))
router.GET("/ready", ginhealth.ReadyHandler(h))
router.GET("/live", ginhealth.LiveHandler(h))
```

**For Echo:**
```go
import "github.com/thanhbvha/go-common/health/handler/echo"

e.GET("/health", echohealth.Handler(h))
e.GET("/ready", echohealth.ReadyHandler(h))
e.GET("/live", echohealth.LiveHandler(h))
```

---

## API Response Format

**Healthy Response (HTTP 200 OK):**
```json
{
  "status": "ok",
  "version": "v1.2.0",
  "checks": {
    "database": { "status": "ok", "duration_ms": 100 },
    "redis": { "status": "ok", "duration_ms": 12 }
  }
}
```

**Unhealthy Response (HTTP 503 Service Unavailable):**
```json
{
  "status": "down",
  "version": "v1.2.0",
  "checks": {
    "database": { "status": "ok", "duration_ms": 100 },
    "redis": {
      "status": "down",
      "duration_ms": 50
    }
  }
}
```

Raw dependency errors are hidden by default because probe endpoints are often
public inside a cluster. For a protected diagnostic endpoint only, opt in with
`health.New().WithErrorDetails(true)`.

## Examples

Check out the full working example in the [`examples/health`](../examples/health) directory.
