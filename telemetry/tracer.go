package telemetry

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
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

// StartClientSpan starts a new span specifically for outgoing Client requests (e.g., HTTP).
func StartClientSpan(ctx context.Context, spanName string) (context.Context, trace.Span) {
	tracer := otel.Tracer("go-common/telemetry")
	return tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))
}

// InjectHTTPHeaders injects the current trace context into HTTP headers.
// This is required to propagate traces to downstream services.
func InjectHTTPHeaders(ctx context.Context, header http.Header) {
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.HeaderCarrier(header))
}
