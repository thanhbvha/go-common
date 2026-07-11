package interceptors

import (
	"context"
	"errors"

	"github.com/thanhbvha/go-common/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerErrorHandler translates internal xerrors to gRPC status errors.
func UnaryServerErrorHandler() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			var customErr *xerrors.CustomError
			if errors.As(err, &customErr) {
				// Map HTTP status to gRPC codes
				c := httpToGRPCCode(customErr.HTTPStatus)
				st := status.New(c, customErr.Message)
				
				return resp, st.Err()
			}
		}
		return resp, err
	}
}

// httpToGRPCCode converts standard HTTP status codes to gRPC codes.
func httpToGRPCCode(httpStatus int) codes.Code {
	switch httpStatus {
	case 200, 201:
		return codes.OK
	case 400:
		return codes.InvalidArgument
	case 401:
		return codes.Unauthenticated
	case 403:
		return codes.PermissionDenied
	case 404:
		return codes.NotFound
	case 409:
		return codes.AlreadyExists
	case 429:
		return codes.ResourceExhausted
	case 501:
		return codes.Unimplemented
	case 503:
		return codes.Unavailable
	case 504:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}
