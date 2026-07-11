# grpc

Plug-and-play gRPC wrapper for building robust microservices. It comes with built-in middlewares (interceptors) for logging, panic recovery, error translation, and OpenTelemetry tracing.

## Features

This module automatically attaches the following interceptors to your server and client:

- **Recovery**: Safely catches `panic` to prevent server crashes, logs the stack trace securely, and returns an `Internal` error code.
- **Logger**: Automatically logs request details, latency, and status codes. Integrates seamlessly with the `go-common/logger` module.
- **Error Handler**: Automatically translates internal `xerrors` (HTTP status codes) into standard gRPC `codes.Code` so clients can understand the exact failure reason.
- **Telemetry**: Distributed tracing out-of-the-box using OpenTelemetry (`otelgrpc`).
- **Health Check & Reflection**: Built-in support for `grpc_health_v1` and Server Reflection for easier debugging with tools like Postman or grpcurl.

## Usage

### 1. Server Setup

```go
package main

import (
	"context"

	common_grpc "github.com/thanhbvha/go-common/grpc"
	"github.com/thanhbvha/go-common/logger"
)

func main() {
	// Initialize logger
	l := logger.New(logger.Options{StdOut: true})
	logger.SetDefault(l)
	defer logger.Close()

	// Initialize gRPC Server with default config (Port: 50051)
	// This automatically wires up Graceful Shutdown, Recovery, Logging, and Error Handling.
	srv := common_grpc.NewServer(common_grpc.DefaultServerConfig)

	// Register your compiled protobuf service implementation here
	// pb.RegisterYourServiceServer(srv.Server, &yourImplementation{})

	// Start the server (Blocks until termination signal is received)
	if err := srv.Serve(); err != nil {
		logger.Error("Server stopped with error", "err", err)
	}
}
```

### 2. Client Setup

```go
package main

import (
	"context"
	
	common_grpc "github.com/thanhbvha/go-common/grpc"
	"github.com/thanhbvha/go-common/logger"
)

func main() {
	// Connect to the internal gRPC server securely (with telemetry enabled)
	conn, err := common_grpc.NewClient(common_grpc.ClientConfig{
		Target:          "localhost:50051",
		EnableTelemetry: true,
	})
	if err != nil {
		logger.Error("Failed to connect", "err", err)
		return
	}
	defer conn.Close()

	// Initialize your compiled protobuf client here
	// client := pb.NewYourServiceClient(conn)
	
	// Call methods...
}
```

For a complete working example, check out `examples/grpc_simple`.
