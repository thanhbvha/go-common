package main

import (
	"context"
	"time"

	"github.com/thanhbvha/go-common/examples/grpc_simple/proto"
	common_grpc "github.com/thanhbvha/go-common/grpc"
	"github.com/thanhbvha/go-common/logger"
)

func main() {
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
