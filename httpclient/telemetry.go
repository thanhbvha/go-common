package httpclient

import (
	"github.com/go-resty/resty/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/thanhbvha/go-common/httpclient")

// telemetryOnBeforeRequest starts a trace span and injects the trace context into HTTP headers.
func telemetryOnBeforeRequest(c *resty.Client, req *resty.Request) error {
	ctx := req.Context()

	// Start a span for this outgoing HTTP request
	spanName := "HTTP " + req.Method
	ctx, _ = tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))
	
	// Inject trace context into headers so the downstream service can continue the trace
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))

	// Store the span in the request context so we can close it later
	req.SetContext(ctx)

	return nil
}

// telemetryOnAfterResponse closes the trace span and records attributes.
func telemetryOnAfterResponse(c *resty.Client, resp *resty.Response) error {
	ctx := resp.Request.Context()
	span := trace.SpanFromContext(ctx)
	defer span.End()

	// Record HTTP attributes
	span.SetAttributes(
		attribute.String("http.method", resp.Request.Method),
		attribute.String("http.url", resp.Request.URL),
		attribute.Int("http.status_code", resp.StatusCode()),
		attribute.String("http.duration", resp.Time().String()),
	)

	// If there was a network or system error (not just a 4xx/5xx response)
	if err := resp.Error(); err != nil {
		span.RecordError(err.(error))
	}

	return nil
}
