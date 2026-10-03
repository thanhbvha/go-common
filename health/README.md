# Health Module

The `health` module provides standardized, Kubernetes-ready HTTP health check endpoints (`/live`, `/ready`, `/health`) for microservices. It supports concurrent health checks with timeouts and provides ready-to-use adapters for **Fiber**, **Gin**, and **Echo**.

## Features

- **Kubernetes Native**: Fully implements standard K8s probes (`liveness` and `readiness`).
- **Concurrent Execution**: Runs all registered dependency checks concurrently using goroutines.
- **Configurable Timeouts**: Prevents your health endpoint from hanging indefinitely if a dependency is stuck.
- **Built-in Checkers**: Pre-built checkers for `GORM`, `Redis`, and standard `HTTP` endpoints.
- **Framework Agnostic**: Includes pre-built HTTP handlers for `Fiber`, `Gin`, and `Echo`.

---

## 🚨 Best Practices for AI/Developers

- **Liveness vs Readiness**: 
  - **Liveness** (`/live`): Checks if the application process is running and not deadlocked. Should be extremely fast (e.g., memory checks). **If this fails, Kubernetes restarts the pod.**
  - **Readiness** (`/ready`): Checks if the application is ready to accept HTTP traffic (e.g., verifying DB, Redis, or upstream API connections). **If this fails, Kubernetes stops sending traffic to the pod (but does NOT restart it).**
- Always set a timeout on the Checker (default is 5s) to ensure K8s probes don't time out.

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

**For Fiber:**
```go
app.Get("/health", health.FiberHandler(h))       // All checks
app.Get("/ready", health.FiberReadyHandler(h))   // Readiness only
app.Get("/live", health.FiberLiveHandler(h))     // Liveness only
```

**For Gin:**
```go
router.GET("/health", health.GinHandler(h))
router.GET("/ready", health.GinReadyHandler(h))
router.GET("/live", health.GinLiveHandler(h))
```

**For Echo:**
```go
e.GET("/health", health.EchoHandler(h))
e.GET("/ready", health.EchoReadyHandler(h))
e.GET("/live", health.EchoLiveHandler(h))
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
      "error": "dial tcp [::1]:6379: connect: connection refused", 
      "duration_ms": 50 
    }
  }
}
```

## Examples

Check out the full working example in the [`examples/health`](../examples/health) directory.
