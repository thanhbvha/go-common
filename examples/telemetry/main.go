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
		// Ensure all data is flushed before the program exits
		defer tel.Shutdown(ctx)
	}

	fmt.Println("--- Telemetry Example ---")

	// 3. Use Metrics (Counter)
	requestCounter := telemetry.MustCounter("api_requests_total", "Total API requests")

	// 4. Use Tracing (Monitor execution flow)
	fmt.Println("Processing request...")
	
	// Initialize a new Trace
	ctx, span := telemetry.StartSpan(ctx, "HandleCheckoutAPI")
	
	// Attach tags (attributes) for easy searching on Dashboard (e.g., find all errors for user_123)
	telemetry.SetAttributes(span, attribute.String("user.id", "user_123"), attribute.String("order.id", "ORD-8888"))
	
	// Simulate logic execution time
	time.Sleep(150 * time.Millisecond) 
	
	// Attach an additional metric to count
	requestCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success")))
	
	// End Trace
	span.End()

	fmt.Printf("Processed successfully! Your TraceID is: %s\n", span.SpanContext().TraceID().String())
	fmt.Println("If you have Jaeger installed on port 4317, open the UI and search for this TraceID!")
}
