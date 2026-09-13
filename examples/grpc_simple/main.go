package main

import (
	"context"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/examples/grpc_simple/proto"
	common_grpc "github.com/thanhbvha/go-common/grpc"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/xerrors"
)

func main() {
	fmt.Println("=== gRPC Module Examples ===")
	fmt.Println("Open 2 terminals. Run the Server in one, and the Client in the other.")
	fmt.Println("To run: go run main.go (and modify the source to uncomment the desired function)")

	// Uncomment one of the functions below to run:
	RunServer()
	// RunClient()
}

// =====================================================================
// 1. gRPC SERVER
// =====================================================================

// echoServer implements proto.EchoServiceServer
type echoServer struct {
	proto.UnimplementedEchoServiceServer
}

func (s *echoServer) Echo(ctx context.Context, req *proto.EchoRequest) (*proto.EchoResponse, error) {
	logger.InfoWithContext(ctx, "Received Echo request", "message", req.Message)
	return &proto.EchoResponse{Message: "Server says: " + req.Message}, nil
}

func (s *echoServer) EchoError(ctx context.Context, req *proto.EchoRequest) (*proto.EchoResponse, error) {
	logger.WarnWithContext(ctx, "Received EchoError request, throwing xerror")
	// This xerror will be caught by the grpc Error Handler interceptor and
	// translated into a proper grpc status code (InvalidArgument = HTTP 400).
	return nil, xerrors.New("INVALID_REQUEST", "This is an expected error", 400)
}

func (s *echoServer) EchoPanic(ctx context.Context, req *proto.EchoRequest) (*proto.EchoResponse, error) {
	logger.InfoWithContext(ctx, "Received EchoPanic request, triggering panic")
	// This panic will be caught by the grpc Recovery interceptor,
	// logged securely, and a generic Internal Error will be returned to client.
	panic("Oops! Something went terribly wrong.")
}

func RunServer() {
	fmt.Println("--- Starting gRPC Server ---")
	// 1. Initialize Logger
	l := logger.New(logger.Options{StdOut: true, TextFormat: false})
	logger.SetDefault(l)
	defer logger.Close()

	// 2. Initialize gRPC Server Wrapper from go-common/grpc
	// It automatically attaches Recovery, Logger, and Error Translation middlewares
	cfg := common_grpc.DefaultServerConfig
	cfg.EnableTelemetry = false // Disable telemetry for simple example
	srv := common_grpc.NewServer(cfg)

	// 3. Register our Service
	proto.RegisterEchoServiceServer(srv.Server, &echoServer{})

	// 4. Start Server (This blocks and also handles Graceful Shutdown internally)
	// CRITICAL: srv.Serve() blocks the main thread. It automatically traps OS signals
	// and performs a graceful shutdown, giving active RPCs time to finish.
	if err := srv.Serve(); err != nil {
		logger.Error("Server stopped with error", "err", err)
	}
}

// =====================================================================
// 2. gRPC CLIENT
// =====================================================================
func RunClient() {
	fmt.Println("--- Starting gRPC Client ---")
	// 1. Initialize Logger
	l := logger.New(logger.Options{StdOut: true, TextFormat: true})
	logger.SetDefault(l)
	defer logger.Close()

	// 2. Initialize gRPC Client Connection wrapper
	conn, err := common_grpc.NewClient(common_grpc.ClientConfig{
		Target:          "localhost:50051",
		EnableTelemetry: false, // Disable telemetry for simple example
	})
	if err != nil {
		logger.Error("Failed to connect to gRPC server", "err", err)
		return
	}
	defer conn.Close()

	client := proto.NewEchoServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test 1: Successful Echo
	logger.Info("--- Test 1: Normal Echo ---")
	resp, err := client.Echo(ctx, &proto.EchoRequest{Message: "Hello World!"})
	if err != nil {
		logger.Error("Echo failed", "err", err)
	} else {
		logger.Info("Echo success", "response", resp.Message)
	}

	// Test 2: Error Echo
	logger.Info("--- Test 2: Error Echo (Translating xerrors) ---")
	resp, err = client.EchoError(ctx, &proto.EchoRequest{Message: "Trigger Error"})
	if err != nil {
		logger.Info("✅ EXPECTED: EchoError correctly returned an error", "err", err.Error())
	} else {
		logger.Error("❌ FAILED: EchoError should have returned an error, but got success", "response", resp.Message)
	}

	// Test 3: Panic Echo
	logger.Info("--- Test 3: Panic Echo (Recovery Interceptor) ---")
	resp, err = client.EchoPanic(ctx, &proto.EchoRequest{Message: "Trigger Panic"})
	if err != nil {
		logger.Info("✅ EXPECTED: EchoPanic correctly returned an error without crashing the server", "err", err.Error())
	} else {
		logger.Error("❌ FAILED: EchoPanic should have returned an error, but got success", "response", resp.Message)
	}
}
