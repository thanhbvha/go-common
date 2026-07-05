package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// StartSpan starts a new span from the global tracer.
// The caller MUST ensure span.End() is called.
func StartSpan(ctx context.Context, spanName string) (context.Context, trace.Span) {
	tracer := otel.Tracer("go-common/telemetry")
	return tracer.Start(ctx, spanName)
}

// RecordError records an error on the span and sets the span status to Error.
func RecordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// SetAttributes adds attributes to the span for better searching and filtering.
func SetAttributes(span trace.Span, kvs ...attribute.KeyValue) {
	span.SetAttributes(kvs...)
}
