package graphql

import (
	"context"
	"errors"

	"github.com/99designs/gqlgen/graphql"
	"github.com/thanhbvha/go-common/v2/logger"
	"github.com/thanhbvha/go-common/v2/utils/ctxkey"
	"github.com/thanhbvha/go-common/v2/xerrors"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.opentelemetry.io/otel/trace"
)

// ErrorPresenter treats xerrors 4xx messages as explicitly public. Other
// resolver errors and all 5xx are masked; causes/extensions never escape.
func ErrorPresenter(ctx context.Context, e error) *gqlerror.Error {
	original := graphql.DefaultErrorPresenter(ctx, e)
	result := &gqlerror.Error{Path: original.Path, Locations: original.Locations}
	code, status, message := "INTERNAL_ERROR", 500, "Internal Server Error"
	var custom *xerrors.CustomError
	switch {
	case errors.Is(e, context.DeadlineExceeded):
		code, status, message = "REQUEST_TIMEOUT", 408, "Request timed out"
	case errors.Is(e, context.Canceled):
		code, status, message = "REQUEST_CANCELLED", 408, "Request cancelled"
	case errors.As(e, &custom):
		if custom.HTTPStatus >= 400 && custom.HTTPStatus < 500 {
			code, status, message = custom.Code, custom.HTTPStatus, custom.Message
		}
	default:
		// Engine codes are only public before resolver execution. Resolvers cannot
		// publish arbitrary gqlerror messages by forging an engine code.
		if executing, _ := ctx.Value(phaseKey{}).(bool); !executing {
			engineCode, _ := original.Extensions["code"].(string)
			switch engineCode {
			case "GRAPHQL_PARSE_FAILED":
				code, status, message = engineCode, 400, "Invalid GraphQL document"
			case "GRAPHQL_VALIDATION_FAILED":
				code, status, message = engineCode, 400, "Invalid GraphQL operation or variables"
			case "COMPLEXITY_LIMIT_EXCEEDED", "QUERY_LIMIT_EXCEEDED":
				code, status, message = engineCode, 422, "Operation exceeds query limits"
			case "UNSUPPORTED_OPERATION":
				code, status, message = engineCode, 400, "Operation not supported"
			default:
				if !graphql.HasOperationContext(ctx) && ctx.Value(stateKey{}) != nil {
					code, status, message = "BAD_REQUEST", 400, "Invalid GraphQL request"
				}
			}
		}
	}
	result.Message = message
	result.Extensions = map[string]any{"code": code, "http_status": status}
	return result
}

// RecoverFunc deliberately omits panic values and causes from logs.
func RecoverFunc(ctx context.Context, _ any) error {
	id, _ := ctxkey.GetRequestID(ctx)
	logger.Error("GraphQL panic recovered", "request_id", id, "trace_id", trace.SpanContextFromContext(ctx).TraceID().String())
	return xerrors.New("INTERNAL_ERROR", "Internal Server Error", 500)
}
