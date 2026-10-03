# Foundation Module

The `foundation` module acts as the core application lifecycle manager for microservices built with `go-common`. It simplifies building production-ready applications by orchestrating startup routines, health checks, and graceful shutdowns.

## Key Features

- **Lifecycle Management**: Register background jobs, servers, or workers using `OnStart(func)`.
- **Graceful Shutdown**: Automatically handles `SIGINT`/`SIGTERM` signals and triggers cleanup functions registered via `OnShutdown(func)` in LIFO (Last-In-First-Out) order.
- **Health Checks**: Seamlessly integrates with the `health` package to maintain Kubernetes-ready `/health`, `/ready`, and `/live` endpoints.

## Quick Start

```go
package main

import (
	"context"
	"time"

	"github.com/thanhbvha/go-common/foundation"
	"github.com/thanhbvha/go-common/logger"
)

func main() {
	app := foundation.NewApp(foundation.Config{
		Name:            "my-microservice",
		Version:         "v1.0.0",
		ShutdownTimeout: 30 * time.Second,
	})

	app.OnStart(func(ctx context.Context) error {
		logger.Info("Starting component...")
		
		app.OnShutdown(func(ctx context.Context) error {
			logger.Info("Cleaning up component...")
			return nil
		})
		
		return nil
	})

	// Blocks until Ctrl+C, then gracefully shuts down.
	app.Run()
}
```

## Running the Example
Check out a fully working example in the `examples/foundation` directory:
```bash
go run examples/foundation/main.go
```
