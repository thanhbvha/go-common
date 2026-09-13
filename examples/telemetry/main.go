package main

import (
	"context"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func main() {
	fmt.Println("=== Telemetry Module Examples ===")
	
	ctx := context.Background()

	// 1. Initialize Telemetry configuration
	cfg := telemetry.Config{
		ServiceName:    "demo-telemetry-service",
		ServiceVersion: "v1.0.0",
		Environment:    "development",
		Endpoint:       "localhost:4317", // Default address of OpenTelemetry Collector or Jaeger/Signoz
		EnableTracing:  true,
		EnableMetrics:  true,
	}

	// 2. Initialize module
	tel, err := telemetry.Init(ctx, cfg)
	if err != nil {
		fmt.Printf("[Error] Failed to initialize telemetry: %v\n", err)
	} else {
		// CRITICAL: Always defer tel.Shutdown(ctx). If you forget this, the background
		// spans might be lost in memory and never sent to the OpenTelemetry Collector upon exit.
		defer tel.Shutdown(ctx)
		fmt.Println("✅ Telemetry initialized successfully!")
	}

	// Uncomment the example you want to run:
	RunTracingExample(ctx)
	// RunMetricsExample(ctx)
}

// =====================================================================
// 1. Tracing Example
// =====================================================================
func RunTracingExample(ctx context.Context) {
	fmt.Println("\n--- 1. Tracing Example ---")
	fmt.Println("Processing request...")
	
	// Initialize a new Trace
	ctx, span := telemetry.StartSpan(ctx, "HandleCheckoutAPI")
	
	// Attach tags (attributes) for easy searching on Dashboard (e.g., find all errors for user_123)
	telemetry.SetAttributes(span, attribute.String("user.id", "user_123"), attribute.String("order.id", "ORD-8888"))
	
	// Simulate logic execution time
	time.Sleep(150 * time.Millisecond) 
	
	// End Trace
	span.End()

	fmt.Printf("Processed successfully! Your TraceID is: %s\n", span.SpanContext().TraceID().String())
	fmt.Println("If you have Jaeger/Signoz installed on port 4317, open the UI and search for this TraceID!")
}

// =====================================================================
// 2. Metrics Example
// =====================================================================
func RunMetricsExample(ctx context.Context) {
	fmt.Println("\n--- 2. Metrics Example ---")
	
	// Initialize a Counter Metric
	requestCounter := telemetry.MustCounter("api_requests_total", "Total API requests")

	// Simulate receiving 5 requests
	for i := 1; i <= 5; i++ {
		// Attach an additional metric to count with specific attributes
		requestCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "success"),
			attribute.String("endpoint", "/api/checkout"),
		))
		fmt.Printf("Incremented api_requests_total counter (Request %d)\n", i)
		time.Sleep(50 * time.Millisecond)
	}
	
	fmt.Println("Metrics recorded successfully! Check your Prometheus/Grafana dashboard.")
}
