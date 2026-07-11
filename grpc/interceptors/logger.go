package interceptors

import (
	"context"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryServerLogger is an interceptor that logs the gRPC request and response.
func UnaryServerLogger() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		// Execute the handler
		resp, err := handler(ctx, req)

		// Calculate latency
		latency := time.Since(start)
		st, _ := status.FromError(err)

		// Determine log level
		if err != nil {
			logger.ErrorWithContext(ctx, "gRPC Request Failed",
				"method", info.FullMethod,
				"latency", latency.String(),
				"status_code", st.Code().String(),
				"error", err.Error(),
			)
		} else {
			logger.InfoWithContext(ctx, "gRPC Request OK",
				"method", info.FullMethod,
				"latency", latency.String(),
				"status_code", st.Code().String(),
			)
		}

		return resp, err
	}
}
