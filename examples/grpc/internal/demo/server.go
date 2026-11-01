package demo

import (
	"context"
	"errors"
	"net"
	"time"

	pb "github.com/thanhbvha/go-common/examples/grpc/proto"
	common "github.com/thanhbvha/go-common/grpc"
	"github.com/thanhbvha/go-common/logger"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func newServer(o ServerOptions, token string) (*common.Server, error) {
	creds, err := transport(o.CommonOptions, o.Address, true)
	if err != nil {
		return nil, err
	}
	cfg := common.DefaultServerConfig
	cfg.Credentials = creds
	cfg.EnableTelemetry = o.OTLPEndpoint != ""
	cfg.EnableReflection = o.Reflection
	cfg.ShutdownTimeout = o.ShutdownTimeout
	srv := common.NewServer(cfg,
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.MaxHeaderListSize(16<<10), grpc.MaxConcurrentStreams(128),
		grpc.ConnectionTimeout(10*time.Second),
		grpc.ChainUnaryInterceptor(unaryAuth(token)),
		grpc.ChainStreamInterceptor(streamAuth(token)),
	)
	pb.RegisterEchoServiceServer(srv.Server, &echoService{demoErrors: o.DemoErrors})
	srv.Health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	srv.Health.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
	return srv, nil
}

// RunServer owns transport, readiness, telemetry and shutdown, independently of
// process-global graceful hooks. Business handlers live in service.go.
func RunServer(ctx context.Context, o ServerOptions) (result error) {
	token, err := sharedToken(o.Token)
	if err != nil {
		return err
	}
	srv, err := newServer(o, token)
	if err != nil {
		return err
	}
	// Reuse bounded, idempotent wrapper shutdown for startup failures too.
	defer combineCleanup(&result, func() error { return srv.Shutdown(context.Background()) })
	cleanup, err := initTelemetry(ctx, "grpc-example-server", o.CommonOptions)
	if err != nil {
		return err
	}
	defer combineCleanup(&result, cleanup)
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", o.Address)
	if err != nil {
		return errors.New("gRPC listen failed")
	}
	readyCtx, stopReady := context.WithCancel(ctx)
	defer stopReady()
	go func() {
		if pause(readyCtx, o.ReadyDelay) != nil {
			return
		}
		// Replace this simulation with database/broker initialization as appropriate.
		srv.Health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		srv.Health.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)
		logger.Info("gRPC dependencies ready")
	}()
	logger.Info("gRPC example listening", "address", lis.Addr().String(), "tls", !o.Insecure, "reflection", o.Reflection)
	if err := srv.ServeContext(ctx, lis); err != nil {
		return errors.New("gRPC serving failed or shutdown exceeded its deadline")
	}
	return nil
}
