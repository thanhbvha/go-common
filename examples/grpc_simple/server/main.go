package main

import (
	"context"

	"github.com/thanhbvha/go-common/examples/grpc_simple/proto"
	common_grpc "github.com/thanhbvha/go-common/grpc"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/xerrors"
)

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

func main() {
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
	if err := srv.Serve(); err != nil {
		logger.Error("Server stopped with error", "err", err)
	}
}
