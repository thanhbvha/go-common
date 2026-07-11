package grpc

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientConfig defines the configuration for the gRPC Client
type ClientConfig struct {
	Target          string
	EnableTelemetry bool
}

// NewClient creates a new gRPC client connection to the target server
// It automatically sets up OpenTelemetry.
func NewClient(cfg ClientConfig) (*grpc.ClientConn, error) {
	var appendOpts []grpc.DialOption

	// 1. Insecure by default for internal microservices communication
	// In a real-world scenario with external APIs, you'd want TLS.
	appendOpts = append(appendOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// 2. OpenTelemetry Interceptor
	if cfg.EnableTelemetry {
		appendOpts = append(appendOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}

	// Connect
	return grpc.NewClient(cfg.Target, appendOpts...)
}
