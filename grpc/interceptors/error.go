package interceptors

import (
	"context"
	"errors"

	"github.com/thanhbvha/go-common/v2/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerErrorHandler maps application errors and hides unclassified failures.
func UnaryServerErrorHandler() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		return resp, toStatus(err)
	}
}

// StreamServerErrorHandler applies the same error policy to streaming RPCs.
func StreamServerErrorHandler() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return toStatus(handler(srv, stream))
	}
}

func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var custom *xerrors.CustomError
	if errors.As(err, &custom) {
		code := httpToGRPCCode(custom.HTTPStatus)
		message := custom.Message
		if custom.HTTPStatus >= 500 || code == codes.Internal {
			message = "Internal server error"
		}
		return status.Error(code, message)
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "Request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "Request deadline exceeded")
	}
	// Explicit client-status errors remain public contracts. Server/transport
	// statuses are rebuilt so their message and details cannot expose causes.
	if grpcStatus, ok := status.FromError(err); ok {
		switch grpcStatus.Code() {
		case codes.Internal, codes.Unknown, codes.DataLoss:
			return status.Error(grpcStatus.Code(), "Internal server error")
		case codes.Unavailable:
			return status.Error(grpcStatus.Code(), "Service unavailable")
		case codes.Unimplemented:
			return status.Error(grpcStatus.Code(), "Operation not implemented")
		case codes.DeadlineExceeded:
			return status.Error(grpcStatus.Code(), "Request deadline exceeded")
		case codes.Canceled:
			return status.Error(grpcStatus.Code(), "Request canceled")
		default:
			return err
		}
	}
	return status.Error(codes.Internal, "Internal server error")
}

func httpToGRPCCode(httpStatus int) codes.Code {
	switch httpStatus {
	case 400, 422:
		return codes.InvalidArgument
	case 401:
		return codes.Unauthenticated
	case 403:
		return codes.PermissionDenied
	case 404:
		return codes.NotFound
	case 408, 504:
		return codes.DeadlineExceeded
	case 409:
		return codes.AlreadyExists
	case 413, 429:
		return codes.ResourceExhausted
	case 501:
		return codes.Unimplemented
	case 503:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}
