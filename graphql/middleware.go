package graphql

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/thanhbvha/go-common/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

// TelemetryInterceptor automatically creates an OpenTelemetry span for each GraphQL operation
type TelemetryInterceptor struct{}

var _ graphql.ResponseInterceptor = TelemetryInterceptor{}
var _ graphql.HandlerExtension = TelemetryInterceptor{}

func (t TelemetryInterceptor) ExtensionName() string {
	return "OpenTelemetry"
}

func (t TelemetryInterceptor) Validate(schema graphql.ExecutableSchema) error {
	return nil
}

func (t TelemetryInterceptor) InterceptResponse(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
	rc := graphql.GetOperationContext(ctx)

	spanName := rc.OperationName
	if spanName == "" {
		spanName = "GraphQL Operation"
	}

	ctx, span := telemetry.StartSpan(ctx, spanName)
	defer span.End()

	telemetry.SetAttributes(span,
		attribute.String("graphql.operation", rc.OperationName),
		attribute.String("graphql.query", rc.RawQuery),
	)

	// Process next request
	res := next(ctx)

	// Record error into span if one occurs
	if res != nil && len(res.Errors) > 0 {
		telemetry.RecordError(span, res.Errors)
	}

	return res
}
