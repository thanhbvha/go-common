package demo

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// This is a single-principal demo, not a multi-tenant identity or role system.
// Both health/reflection and business RPCs require the token, including streams.
func authenticate(ctx context.Context, token string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 {
		return status.Error(codes.Unauthenticated, "Bearer token required")
	}
	got := sha256.Sum256([]byte(values[0]))
	want := sha256.Sum256([]byte("Bearer " + token))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		return status.Error(codes.Unauthenticated, "Invalid bearer token")
	}
	return nil
}

func unaryAuth(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if err := authenticate(ctx, token); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func streamAuth(token string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if err := authenticate(stream.Context(), token); err != nil {
			return err
		}
		return next(srv, stream)
	}
}

type bearerToken string

func (t bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}
func (bearerToken) RequireTransportSecurity() bool { return true }

// Explicit plaintext credentials are only used after loopback validation.
type localBearerToken struct{ bearerToken }

func (localBearerToken) RequireTransportSecurity() bool { return false }
