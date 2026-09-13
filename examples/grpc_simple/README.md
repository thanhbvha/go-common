# gRPC Module

## Overview
The `grpc` module provides production-ready wrappers around standard `google.golang.org/grpc`. It includes built-in middlewares (interceptors) for panic recovery, request logging, error translation, and OpenTelemetry.

## Key Features
- `grpc.NewServer()`: Creates a secure, robust gRPC server.
- **Interceptors**: Automatically configured with `Recovery`, `Logger`, and `ErrorHandler` interceptors.
- `grpc.NewClient()`: Creates a resilient gRPC client connection with automatic OpenTelemetry integration.

## 🛠️ Generating Protobuf Code
Before writing gRPC code, you **MUST** compile your `.proto` files into Go code using `protoc` (Protocol Buffers Compiler). 
Navigate to the module directory and run:
```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/echo.proto
```
This generates the `echo.pb.go` and `echo_grpc.pb.go` files inside the `proto/` directory.

## 🚨 Best Practices for AI/Developers
- **Running the Example**: This example contains both the Server and the Client in one `main.go` file. To run it, you need to open 2 terminals. In the first terminal, uncomment `RunServer()` and run `go run main.go`. In the second terminal, uncomment `RunClient()` and run `go run main.go`.
- **Error Handling**: Do not return standard Go `errors.New` directly from your gRPC service. Always return an `xerrors` (from `go-common/xerrors`). The `ErrorHandler` interceptor will automatically map it to the correct gRPC status code (e.g. `InvalidArgument`, `Internal`).
- **Panics**: Do not fear panics in gRPC handlers. The `Recovery` interceptor will trap them, log the stack trace securely, and return a clean `Internal Error` to the client.
- **Graceful Shutdown (CRITICAL)**: Use `srv.Serve()`. It is a blocking call that automatically intercepts OS signals (SIGINT/SIGTERM) and triggers a graceful shutdown internally. You do not need to use `utils/graceful` separately for the gRPC server.
