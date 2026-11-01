package interceptors

import (
	"context"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryServerLogger logs method, latency and status without payloads or error text.
func UnaryServerLogger() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		resp, err = handler(ctx, req)
		logRPC(ctx, info.FullMethod, start, err)
		return resp, err
	}
}

// StreamServerLogger logs the total stream lifetime without message contents.
func StreamServerLogger() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, stream)
		logRPC(stream.Context(), info.FullMethod, start, err)
		return err
	}
}

func logRPC(ctx context.Context, method string, start time.Time, err error) {
	fields := []any{"method", method, "latency", time.Since(start).String(), "status_code", status.Code(toStatus(err)).String()}
	if err != nil {
		logger.ErrorWithContext(ctx, "gRPC Request Failed", fields...)
	} else {
		logger.InfoWithContext(ctx, "gRPC Request OK", fields...)
	}
}
