package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/thanhbvha/go-common/grpc/interceptors"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/utils/graceful"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// ServerConfig defines the configuration for the gRPC Server
type ServerConfig struct {
	Port            int
	EnableTelemetry bool
	EnableLogger    bool
	EnableRecovery  bool
	EnableHealth    bool
}

// DefaultServerConfig provides sensible defaults
var DefaultServerConfig = ServerConfig{
	Port:            50051,
	EnableTelemetry: true,
	EnableLogger:    true,
	EnableRecovery:  true,
	EnableHealth:    true,
}

// Server wraps the native grpc.Server with built-in ecosystem middlewares
type Server struct {
	*grpc.Server
	config ServerConfig
}

// NewServer creates a new gRPC server with the specified configuration and interceptors
func NewServer(cfg ServerConfig) *Server {
	var opts []grpc.ServerOption
	var unaryInterceptors []grpc.UnaryServerInterceptor

	// 1. Recovery Interceptor (First to catch panics in subsequent interceptors)
	if cfg.EnableRecovery {
		unaryInterceptors = append(unaryInterceptors, interceptors.UnaryServerRecovery())
	}

	// 2. Logger Interceptor
	if cfg.EnableLogger {
		unaryInterceptors = append(unaryInterceptors, interceptors.UnaryServerLogger())
	}

	// 3. Error Handler Interceptor (Translates xerrors)
	unaryInterceptors = append(unaryInterceptors, interceptors.UnaryServerErrorHandler())

	// Combine Unary Interceptors
	opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...))

	// 4. Telemetry Interceptor
	if cfg.EnableTelemetry {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	srv := grpc.NewServer(opts...)

	// 5. Enable Health Check Protocol
	if cfg.EnableHealth {
		healthcheck := health.NewServer()
		grpc_health_v1.RegisterHealthServer(srv, healthcheck)
		// Mark server as ready
		healthcheck.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	}

	// 6. Enable Server Reflection for easier debugging (e.g. grpcurl, Postman)
	reflection.Register(srv)

	return &Server{
		Server: srv,
		config: cfg,
	}
}

// Serve starts the gRPC server and automatically registers it with graceful shutdown
func (s *Server) Serve() error {
	addr := fmt.Sprintf(":%d", s.config.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	// Register graceful shutdown
	graceful.Register(func(ctx context.Context) error {
		logger.Info("Shutting down gRPC server gracefully...")
		s.Server.GracefulStop()
		return nil
	})

	logger.Info("gRPC Server is running", "port", s.config.Port)
	return s.Server.Serve(lis)
}
