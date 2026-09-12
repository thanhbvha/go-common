# gRPC Module

## Overview
The `grpc` module provides production-ready wrappers around standard `google.golang.org/grpc`. It includes built-in middlewares (interceptors) for panic recovery, request logging, error translation, and OpenTelemetry.

## Key Features
- `grpc.NewServer()`: Creates a secure, robust gRPC server.
- **Interceptors**: Automatically configured with `Recovery`, `Logger`, and `ErrorHandler` interceptors.
- `grpc.Dial()`: Creates a resilient gRPC client connection with automatic retries and backoff.

## 🚨 Best Practices for AI/Developers
- **Error Handling**: Do not return standard Go `errors.New` directly from your gRPC service. Always return an `xerrors` (from `go-common/xerrors`). The `ErrorHandler` interceptor will automatically map it to the correct gRPC status code (e.g. `InvalidArgument`, `Internal`).
- **Panics**: Do not fear panics in gRPC handlers. The `Recovery` interceptor will trap them, log the stack trace securely, and return a clean `Internal Error` to the client.
- **Graceful Shutdown (CRITICAL)**: Use `srv.Serve()`. It is a blocking call that automatically intercepts OS signals (SIGINT/SIGTERM) and triggers a graceful shutdown internally. You do not need to use `utils/graceful` separately for the gRPC server.
