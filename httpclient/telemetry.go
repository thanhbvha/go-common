package httpclient

import (
	"github.com/go-resty/resty/v2"
	"github.com/thanhbvha/go-common/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// telemetryOnBeforeRequest starts a trace span and injects the trace context into HTTP headers.
func telemetryOnBeforeRequest(c *resty.Client, req *resty.Request) error {
	ctx := req.Context()

	// Start a span for this outgoing HTTP request using our centralized telemetry module
	spanName := "HTTP " + req.Method
	ctx, _ = telemetry.StartClientSpan(ctx, spanName)
	
	// Inject trace context into headers so the downstream service can continue the trace
	telemetry.InjectHTTPHeaders(ctx, req.Header)

	// Store the span in the request context so we can close it later
	req.SetContext(ctx)

	return nil
}

// telemetryOnAfterResponse closes the trace span and records attributes.
func telemetryOnAfterResponse(c *resty.Client, resp *resty.Response) error {
	ctx := resp.Request.Context()
	span := trace.SpanFromContext(ctx)
	defer span.End()

	// Record HTTP attributes using our centralized module
	telemetry.SetAttributes(span, 
		attribute.String("http.method", resp.Request.Method),
		attribute.String("http.url", resp.Request.URL),
		attribute.Int("http.status_code", resp.StatusCode()),
		attribute.String("http.duration", resp.Time().String()),
	)

	// If there was a network or system error (not just a 4xx/5xx response)
	if err := resp.Error(); err != nil {
		telemetry.RecordError(span, err.(error))
	}

	return nil
}
