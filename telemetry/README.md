# telemetry

Production-ready OpenTelemetry integration for distributed tracing and metrics via OTLP.

```go
import "github.com/thanhbvha/go-common/telemetry"

// Initialize Telemetry
tel, err := telemetry.Init(context.Background(), telemetry.Config{
    ServiceName:   "my-service",
    EnableTracing: true,
    EnableMetrics: true,
    Endpoint:      "localhost:4317",
})
defer tel.Shutdown(context.Background())

// Start a trace span
ctx, span := telemetry.StartSpan(ctx, "HandlePayment")
defer span.End()

telemetry.SetAttributes(span, attribute.String("user.id", "123"))

// Metrics
counter := telemetry.MustCounter("requests_total", "Total requests")
counter.Add(ctx, 1)
```

For inbound standard-library HTTP middleware, use
`web/middleware/nethttp.Telemetry`. It extracts upstream `traceparent` headers,
starts a server span, propagates the span through `r.Context()`, and records the
response status. The lower-level `StartServerSpan` and `ExtractHTTPHeaders`
helpers are available for custom transports.
