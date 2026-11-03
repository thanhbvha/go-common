package interceptors

import (
	"context"
	"runtime/debug"

	"github.com/thanhbvha/go-common/v2/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerRecovery recovers panics on the handler goroutine. Panic values
// are excluded from logs because they can contain credentials or request data.
func UnaryServerRecovery() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if recover() != nil {
				resp = nil
				err = recovered(ctx, info.FullMethod)
			}
		}()
		return handler(ctx, req)
	}
}

// StreamServerRecovery protects streaming handlers on the handler goroutine.
func StreamServerRecovery() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if recover() != nil {
				err = recovered(stream.Context(), info.FullMethod)
			}
		}()
		return handler(srv, stream)
	}
}

func recovered(ctx context.Context, method string) error {
	logger.ErrorWithContext(ctx, "gRPC Panic Recovered", "method", method, "stack", string(debug.Stack()))
	return status.Error(codes.Internal, "Internal server error")
}
