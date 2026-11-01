package grpc

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientConfig defines the configuration for the gRPC Client
type ClientConfig struct {
	Target          string
	EnableTelemetry bool
	// Credentials defaults to TLS with system roots and TLS 1.2 minimum.
	Credentials credentials.TransportCredentials
	// Insecure explicitly enables plaintext for local development or a trusted TLS proxy.
	Insecure bool
	// UnaryTimeout caps unary calls, including calls that have a longer deadline.
	// Zero leaves deadlines to the caller. Streams always use the caller's context.
	UnaryTimeout time.Duration
}

// NewClient creates a lazy client connection with TLS by default and optional
// OpenTelemetry. Native options are appended last and may override configuration.
func NewClient(cfg ClientConfig, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if strings.TrimSpace(cfg.Target) == "" {
		return nil, errors.New("grpc: target is required")
	}
	if cfg.UnaryTimeout < 0 || (cfg.Insecure && cfg.Credentials != nil) {
		return nil, errors.New("grpc: invalid timeout or conflicting transport credentials")
	}
	var appendOpts []grpc.DialOption

	creds := cfg.Credentials
	if cfg.Insecure {
		creds = insecure.NewCredentials()
	} else if creds == nil {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	appendOpts = append(appendOpts, grpc.WithTransportCredentials(creds))
	if cfg.UnaryTimeout > 0 {
		appendOpts = append(appendOpts, grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
			ctx, cancel := context.WithTimeout(ctx, cfg.UnaryTimeout)
			defer cancel()
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}))
	}

	// 2. OpenTelemetry Interceptor
	if cfg.EnableTelemetry {
		appendOpts = append(appendOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}

	// Connect
	return grpc.NewClient(cfg.Target, append(appendOpts, opts...)...)
}
