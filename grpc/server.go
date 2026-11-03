package grpc

import (
	"sync"
	"time"

	"github.com/thanhbvha/go-common/v2/grpc/interceptors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// ServerConfig defines the configuration for the gRPC Server
type ServerConfig struct {
	Port             int
	EnableTelemetry  bool
	EnableLogger     bool
	EnableRecovery   bool
	EnableHealth     bool
	EnableReflection bool
	// Credentials enables TLS/mTLS. Nil allows plaintext for proxy-terminated deployments.
	Credentials credentials.TransportCredentials
	// Address overrides Port, e.g. "127.0.0.1:50051".
	Address string
	// ShutdownTimeout defaults to 30 seconds when non-positive.
	ShutdownTimeout time.Duration
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
	// Health is available when EnableHealth is set. Applications own readiness.
	Health          *health.Server
	shutdownOnce    sync.Once
	forceOnce       sync.Once
	shutdownStarted chan struct{}
	shutdownDone    chan struct{}
	shutdownForced  chan struct{}
	shutdownMu      sync.Mutex
	shutdownErr     error
}

// NewServer creates a server with unary and streaming middleware. Native options
// are appended last. Prefer ChainUnaryInterceptor/ChainStreamInterceptor for
// application interceptors so recovery and error mapping enclose them.
func NewServer(cfg ServerConfig, options ...grpc.ServerOption) *Server {
	var opts []grpc.ServerOption
	var unaryInterceptors []grpc.UnaryServerInterceptor
	var streamInterceptors []grpc.StreamServerInterceptor

	// 1. Recovery Interceptor (First to catch panics in subsequent interceptors)
	if cfg.EnableRecovery {
		unaryInterceptors = append(unaryInterceptors, interceptors.UnaryServerRecovery())
		streamInterceptors = append(streamInterceptors, interceptors.StreamServerRecovery())
	}

	// 2. Logger Interceptor
	if cfg.EnableLogger {
		unaryInterceptors = append(unaryInterceptors, interceptors.UnaryServerLogger())
		streamInterceptors = append(streamInterceptors, interceptors.StreamServerLogger())
	}

	// 3. Error Handler Interceptor (Translates xerrors)
	unaryInterceptors = append(unaryInterceptors, interceptors.UnaryServerErrorHandler())
	streamInterceptors = append(streamInterceptors, interceptors.StreamServerErrorHandler())

	// Combine Unary Interceptors
	opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	opts = append(opts, grpc.ChainStreamInterceptor(streamInterceptors...))
	if cfg.Credentials != nil {
		opts = append(opts, grpc.Creds(cfg.Credentials))
	}

	// 4. Telemetry Interceptor
	if cfg.EnableTelemetry {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	srv := grpc.NewServer(append(opts, options...)...)
	var healthcheck *health.Server

	// 5. Enable Health Check Protocol
	if cfg.EnableHealth {
		healthcheck = health.NewServer()
		grpc_health_v1.RegisterHealthServer(srv, healthcheck)
		// Readiness is application-owned; bootstrap must mark dependencies ready.
		healthcheck.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}

	// 6. Enable Server Reflection for easier debugging (e.g. grpcurl, Postman)
	if cfg.EnableReflection {
		reflection.Register(srv)
	}

	return &Server{
		Server:          srv,
		config:          cfg,
		Health:          healthcheck,
		shutdownStarted: make(chan struct{}),
		shutdownDone:    make(chan struct{}),
		shutdownForced:  make(chan struct{}),
	}
}
